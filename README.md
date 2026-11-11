# Event-Driven Trading System (Memory-Efficient Python Backtester)

A small, event-driven backtesting framework written in Python and designed to run on a machine with **8GB of RAM**. The project is a learning exercise in market microstructure, production-style system architecture, and quantitative evaluation. It is built so that every component can be explained and defended, not just run.

## Purpose

Most beginner backtests load an entire price history into a spreadsheet-style table and compute signals across it in one pass. That approach is easy to write, but it hides two problems:

1. **Lookahead bias.** It is easy to accidentally use future data when every row is visible at once.
2. **Memory limits.** Large datasets (especially 1-minute or tick data) can exceed available RAM.

This project avoids both by replacing the "whole table at once" approach with an **event-driven architecture**: data is streamed in small batches, and each component reacts to discrete events (a new price arrives, a strategy wants to trade, an order is filled), closer to how live trading systems work.

## Design Goals

- **Memory-efficient:** stream Parquet data in small batches with Polars rather than loading full datasets.
- **Realistic:** simulate bid-ask spread, slippage, and commissions rather than assuming perfect fills at the quoted price.
- **Modular:** each stage (data, strategy, execution, portfolio, metrics) is its own component, communicating only through events.
- **Evaluated honestly:** results are reported with risk-adjusted metrics and separate in-sample / out-of-sample performance.

## Architecture

```
DataHandler ──MarketEvent──▶ Strategy ──SignalEvent──▶ Portfolio ──OrderEvent──▶ ExecutionEngine
     ▲                                                     ▲                           │
     │                                                     └────────FillEvent──────────┘
  Parquet file (streamed in batches)
```

| Event | Meaning |
|---|---|
| `MarketEvent` | A new price bar/tick has arrived |
| `SignalEvent` | The strategy wants to buy or sell |
| `OrderEvent` | An instruction to execute a trade |
| `FillEvent` | Confirmation of execution, including fill price and commission |

Events flow through a central `queue.Queue` and are dispatched by an event loop.

## Components

| Component | Responsibility |
|---|---|
| `DataHandler` | Streams historical data from Parquet in small batches, yielding one row at a time and pushing `MarketEvent`s onto the queue |
| `Strategy` | Consumes market data and emits signals. The initial strategy is classic **mean reversion** using a rolling Z-score |
| `ExecutionEngine` | Simulates the broker: applies spread, slippage, and commission to produce realistic fills |
| `Portfolio` | Tracks cash, positions, and equity over time; marks positions to market |
| `metrics` | Computes total return, Sharpe ratio, maximum drawdown, and related statistics |

## Tech Stack

- **Python 3.10+**
- **Polars** for lazy, memory-efficient data handling
- **Parquet** (zstd-compressed) for storage
- **pytest** for unit tests
- **matplotlib** for equity curve and drawdown plots

## Memory Strategy

Running within 8GB is a core constraint, so the project uses these techniques:

- Data is converted once from CSV to compressed Parquet.
- `pl.scan_parquet(...)` (lazy) is used instead of `pl.read_parquet(...)` (eager), and rows are consumed in small batches through `LazyFrame.collect_batches()`.
- Only a fixed-size rolling window (`collections.deque(maxlen=N)`) is kept for strategy calculations, not the full price history.
- Events are lightweight dataclasses with `__slots__`.
- The equity curve is stored as simple numeric tuples rather than full state snapshots.

## Example: Streaming Data

```python
import polars as pl

class DataHandler:
    def __init__(self, path, batch_size=500):
        self._reader = pl.scan_parquet(path).collect_batches(chunk_size=batch_size)
        self._buffer = iter([])

    def __iter__(self):
        return self

    def __next__(self):
        try:
            return next(self._buffer)
        except StopIteration:
            batch = next(self._reader)  # raises StopIteration at end of data
            self._buffer = batch.iter_rows(named=True)
            return next(self._buffer)

for row in DataHandler("AAPL_daily.parquet"):
    print(row)  # {'timestamp': ..., 'open': ..., 'close': ..., ...}
```

Note: `collect_batches` is marked unstable in the Polars documentation, so its behavior may change between versions.

## Evaluation Metrics

- **Total return**
- **Sharpe ratio**, annualized for the data frequency
- **Maximum drawdown** and drawdown duration
- **Win rate and profit factor**
- **In-sample vs. out-of-sample comparison** using a chronological train/test split

## Project Roadmap

| Phase | Focus | Status |
|---|---|---|
| 1 | Data pipeline: CSV to Parquet, streaming `DataHandler` | In progress |
| 2 | Event-driven backbone: event classes, queue, event loop | Planned |
| 3 | Execution engine and portfolio: spread, slippage, commissions | Planned |
| 4 | Mean-reversion strategy, metrics, and validation | Planned |
| Stretch | Second strategy, parameter sensitivity analysis, CLI | Planned |

## Suggested Repository Structure

```
trading_system/
├── config.py
├── data/
│   └── data_handler.py
├── events.py
├── engine.py
├── strategy.py
├── execution.py
├── portfolio.py
├── metrics.py
├── run_backtest.py
├── tests/
├── RESULTS.md
└── requirements.txt
```

## Limitations and Disclaimer

This is an educational project. Backtested results, however carefully simulated, do not guarantee future performance, and the fill model is a simplification of real market behavior. Nothing here is financial advice.

## References

- Larry Harris, *Trading and Exchanges: Market Microstructure for Practitioners*
- Ernest Chan, *Quantitative Trading*
- [Polars documentation](https://docs.pola.rs)
