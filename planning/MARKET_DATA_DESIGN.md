# Market Data Backend — Detailed Design

**Status:** Implemented, tested (73/73 passing), reviewed. This document describes the system as it actually exists in `backend/app/market/`, and prescribes how the not-yet-built parts of the backend (`app/main.py`, portfolio/watchlist routes) should integrate with it.

Everything in this document lives under `backend/app/market/`, with tests in `backend/tests/market/`.

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [File Structure](#2-file-structure)
3. [Data Model — `models.py`](#3-data-model--modelspy)
4. [Price Cache — `cache.py`](#4-price-cache--cachepy)
5. [Abstract Interface — `interface.py`](#5-abstract-interface--interfacepy)
6. [Seed Prices & Ticker Parameters — `seed_prices.py`](#6-seed-prices--ticker-parameters--seed_pricespy)
7. [GBM Simulator — `simulator.py`](#7-gbm-simulator--simulatorpy)
8. [Massive API Client — `massive_client.py`](#8-massive-api-client--massive_clientpy)
9. [Factory — `factory.py`](#9-factory--factorypy)
10. [SSE Streaming Endpoint — `stream.py`](#10-sse-streaming-endpoint--streampy)
11. [Public Package API — `__init__.py`](#11-public-package-api--__init__py)
12. [FastAPI Lifecycle Integration (prescriptive)](#12-fastapi-lifecycle-integration-prescriptive)
13. [Watchlist Coordination](#13-watchlist-coordination)
14. [Testing Strategy](#14-testing-strategy)
15. [Error Handling & Edge Cases](#15-error-handling--edge-cases)
16. [Configuration Summary](#16-configuration-summary)

---

## 1. Architecture Overview

```
MarketDataSource (ABC)
├── SimulatorDataSource  →  GBM simulator (default, no API key needed)
└── MassiveDataSource    →  Polygon.io REST poller (when MASSIVE_API_KEY set)
        │
        ▼
   PriceCache (thread-safe, in-memory, single instance per app)
        │
        ├──→ SSE stream endpoint (GET /api/stream/prices)
        ├──→ Portfolio valuation (not yet built)
        └──→ Trade execution      (not yet built)
```

Both data sources implement the same abstract base class (`MarketDataSource`) and push into the same `PriceCache`. Nothing downstream — SSE streaming, portfolio math, trade validation — needs to know or care which source is active. This is a textbook Strategy pattern: `factory.py` picks the strategy once at startup based on an environment variable, and everything else is written against the interface, not the implementation.

The cache is the seam. Producers (`start()`/background loop) write; consumers (SSE, REST routes) read. There's no direct coupling between a data source and its consumers.

---

## 2. File Structure

```
backend/
  app/
    market/
      __init__.py          # Re-exports: PriceUpdate, PriceCache, MarketDataSource,
                            #   create_market_data_source, create_stream_router
      models.py             # PriceUpdate dataclass
      cache.py               # PriceCache (thread-safe in-memory store)
      interface.py           # MarketDataSource ABC
      seed_prices.py          # SEED_PRICES, TICKER_PARAMS, DEFAULT_PARAMS, CORRELATION_GROUPS
      simulator.py            # GBMSimulator + SimulatorDataSource
      massive_client.py       # MassiveDataSource
      factory.py              # create_market_data_source()
      stream.py               # SSE endpoint (FastAPI router factory)
  tests/
    market/
      test_models.py
      test_cache.py
      test_simulator.py
      test_simulator_source.py
      test_factory.py
      test_massive.py
  market_data_demo.py         # Rich terminal dashboard demo
```

Each file has a single responsibility. `app.market.__init__` re-exports the public surface so the rest of the backend never imports from submodules directly — only `from app.market import ...`.

---

## 3. Data Model — `models.py`

`PriceUpdate` is the only data structure that leaves the market data layer. SSE streaming, portfolio valuation, and trade execution all work exclusively with this type.

```python
"""Data models for market data."""

from __future__ import annotations

import time
from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class PriceUpdate:
    """Immutable snapshot of a single ticker's price at a point in time."""

    ticker: str
    price: float
    previous_price: float
    timestamp: float = field(default_factory=time.time)  # Unix seconds

    @property
    def change(self) -> float:
        """Absolute price change from previous update."""
        return round(self.price - self.previous_price, 4)

    @property
    def change_percent(self) -> float:
        """Percentage change from previous update."""
        if self.previous_price == 0:
            return 0.0
        return round((self.price - self.previous_price) / self.previous_price * 100, 4)

    @property
    def direction(self) -> str:
        """'up', 'down', or 'flat'."""
        if self.price > self.previous_price:
            return "up"
        elif self.price < self.previous_price:
            return "down"
        return "flat"

    def to_dict(self) -> dict:
        """Serialize for JSON / SSE transmission."""
        return {
            "ticker": self.ticker,
            "price": self.price,
            "previous_price": self.previous_price,
            "timestamp": self.timestamp,
            "change": self.change,
            "change_percent": self.change_percent,
            "direction": self.direction,
        }
```

**Design decisions:**

- **`frozen=True`** — price updates are immutable value objects, safe to share across async tasks and threads without copying or locking.
- **`slots=True`** — memory optimization; many of these get created per second across 10+ tickers.
- **Computed properties** (`change`, `change_percent`, `direction`) derive from `price`/`previous_price` so they can never drift out of sync with each other. There's no stored `direction` field that could go stale.
- **`to_dict()`** is the single serialization point used by both the SSE endpoint and any future REST response.

---

## 4. Price Cache — `cache.py`

The cache is the central hub: whichever data source is active writes to it; the SSE endpoint and (eventually) portfolio/trade code read from it.

```python
"""Thread-safe in-memory price cache."""

from __future__ import annotations

import time
from threading import Lock

from .models import PriceUpdate


class PriceCache:
    """Thread-safe in-memory cache of the latest price for each ticker.

    Writers: SimulatorDataSource or MassiveDataSource (one at a time).
    Readers: SSE streaming endpoint, portfolio valuation, trade execution.
    """

    def __init__(self) -> None:
        self._prices: dict[str, PriceUpdate] = {}
        self._lock = Lock()
        self._version: int = 0  # Monotonically increasing; bumped on every update

    def update(self, ticker: str, price: float, timestamp: float | None = None) -> PriceUpdate:
        """Record a new price for a ticker. Returns the created PriceUpdate.

        Automatically computes direction and change from the previous price.
        If this is the first update for the ticker, previous_price == price (direction='flat').
        """
        with self._lock:
            ts = timestamp or time.time()
            prev = self._prices.get(ticker)
            previous_price = prev.price if prev else price

            update = PriceUpdate(
                ticker=ticker,
                price=round(price, 2),
                previous_price=round(previous_price, 2),
                timestamp=ts,
            )
            self._prices[ticker] = update
            self._version += 1
            return update

    def get(self, ticker: str) -> PriceUpdate | None:
        """Get the latest price for a single ticker, or None if unknown."""
        with self._lock:
            return self._prices.get(ticker)

    def get_all(self) -> dict[str, PriceUpdate]:
        """Snapshot of all current prices. Returns a shallow copy."""
        with self._lock:
            return dict(self._prices)

    def get_price(self, ticker: str) -> float | None:
        """Convenience: get just the price float, or None."""
        update = self.get(ticker)
        return update.price if update else None

    def remove(self, ticker: str) -> None:
        """Remove a ticker from the cache (e.g., when removed from watchlist)."""
        with self._lock:
            self._prices.pop(ticker, None)

    @property
    def version(self) -> int:
        """Current version counter. Useful for SSE change detection."""
        return self._version

    def __len__(self) -> int:
        with self._lock:
            return len(self._prices)

    def __contains__(self, ticker: str) -> bool:
        with self._lock:
            return ticker in self._prices
```

**Why a version counter?** The SSE loop polls the cache every ~500ms. Without a version counter it would re-serialize and re-send every ticker's price on every tick even when nothing changed (e.g., between Massive polls, which happen every 15s). Instead:

```python
last_version = -1
while True:
    if price_cache.version != last_version:
        last_version = price_cache.version
        yield format_sse(price_cache.get_all())
    await asyncio.sleep(0.5)
```

**Why `threading.Lock` and not `asyncio.Lock`?** The Massive client's synchronous `get_snapshot_all()` call runs inside `asyncio.to_thread()`, which executes on a real OS thread — an `asyncio.Lock` would not protect against that thread racing with the event loop. `threading.Lock` is correct from both a background thread and the async event loop.

---

## 5. Abstract Interface — `interface.py`

```python
"""Abstract interface for market data sources."""

from __future__ import annotations

from abc import ABC, abstractmethod


class MarketDataSource(ABC):
    """Contract for market data providers.

    Implementations push price updates into a shared PriceCache on their own
    schedule. Downstream code never calls the data source directly for prices —
    it reads from the cache.

    Lifecycle:
        source = create_market_data_source(cache)
        await source.start(["AAPL", "GOOGL", ...])
        # ... app runs ...
        await source.add_ticker("TSLA")
        await source.remove_ticker("GOOGL")
        # ... app shutting down ...
        await source.stop()
    """

    @abstractmethod
    async def start(self, tickers: list[str]) -> None:
        """Begin producing price updates for the given tickers.

        Starts a background task that periodically writes to the PriceCache.
        Must be called exactly once. Calling start() twice is undefined behavior.
        """

    @abstractmethod
    async def stop(self) -> None:
        """Stop the background task and release resources.

        Safe to call multiple times. After stop(), the source will not write
        to the cache again.
        """

    @abstractmethod
    async def add_ticker(self, ticker: str) -> None:
        """Add a ticker to the active set. No-op if already present.

        The next update cycle will include this ticker.
        """

    @abstractmethod
    async def remove_ticker(self, ticker: str) -> None:
        """Remove a ticker from the active set. No-op if not present.

        Also removes the ticker from the PriceCache.
        """

    @abstractmethod
    def get_tickers(self) -> list[str]:
        """Return the current list of actively tracked tickers."""
```

**Why the source writes to the cache instead of returning prices:** This push model decouples timing. The simulator ticks every 500ms; Massive polls every 15s. SSE always reads from the cache on its own 500ms cadence regardless of which source is active or how often it updates. The interface has no "get price" method at all — that's deliberately only on `PriceCache`.

---

## 6. Seed Prices & Ticker Parameters — `seed_prices.py`

Constants only — no logic, no imports beyond the type hints. Shared by the simulator for initial prices, per-ticker GBM parameters, and correlation grouping.

```python
"""Seed prices and per-ticker parameters for the market simulator."""

# Realistic starting prices for the default watchlist (as of project creation)
SEED_PRICES: dict[str, float] = {
    "AAPL": 190.00,
    "GOOGL": 175.00,
    "MSFT": 420.00,
    "AMZN": 185.00,
    "TSLA": 250.00,
    "NVDA": 800.00,
    "META": 500.00,
    "JPM": 195.00,
    "V": 280.00,
    "NFLX": 600.00,
}

# Per-ticker GBM parameters
# sigma: annualized volatility (higher = more price movement)
# mu: annualized drift / expected return
TICKER_PARAMS: dict[str, dict[str, float]] = {
    "AAPL": {"sigma": 0.22, "mu": 0.05},
    "GOOGL": {"sigma": 0.25, "mu": 0.05},
    "MSFT": {"sigma": 0.20, "mu": 0.05},
    "AMZN": {"sigma": 0.28, "mu": 0.05},
    "TSLA": {"sigma": 0.50, "mu": 0.03},  # High volatility
    "NVDA": {"sigma": 0.40, "mu": 0.08},  # High volatility, strong drift
    "META": {"sigma": 0.30, "mu": 0.05},
    "JPM": {"sigma": 0.18, "mu": 0.04},  # Low volatility (bank)
    "V": {"sigma": 0.17, "mu": 0.04},  # Low volatility (payments)
    "NFLX": {"sigma": 0.35, "mu": 0.05},
}

# Default parameters for tickers not in the list above (dynamically added)
DEFAULT_PARAMS: dict[str, float] = {"sigma": 0.25, "mu": 0.05}

# Correlation groups for the simulator's Cholesky decomposition
# Tickers in the same group have higher intra-group correlation
CORRELATION_GROUPS: dict[str, set[str]] = {
    "tech": {"AAPL", "GOOGL", "MSFT", "AMZN", "META", "NVDA", "NFLX"},
    "finance": {"JPM", "V"},
}

# Correlation coefficients
INTRA_TECH_CORR = 0.6  # Tech stocks move together
INTRA_FINANCE_CORR = 0.5  # Finance stocks move together
CROSS_GROUP_CORR = 0.3  # Between sectors / unknown tickers
TSLA_CORR = 0.3  # TSLA does its own thing
```

Tickers added dynamically that aren't in `SEED_PRICES`/`TICKER_PARAMS` fall back to a random seed price between $50–$300 and `DEFAULT_PARAMS`.

---

## 7. GBM Simulator — `simulator.py`

Two classes live here:

- **`GBMSimulator`** — pure math engine, stateful, holds current prices and advances them one step at a time.
- **`SimulatorDataSource`** — the `MarketDataSource` implementation wrapping `GBMSimulator` in an async loop that writes to `PriceCache`.

### 7.1 GBM Math

At each tick, a price evolves as:

```
S(t+dt) = S(t) * exp((mu - sigma^2/2) * dt + sigma * sqrt(dt) * Z)
```

where `mu` is annualized drift, `sigma` is annualized volatility, `dt` is the tick size as a fraction of a trading year, and `Z` is a (correlated) standard normal draw. For 500ms ticks over a 252-day/6.5h trading year:

```
dt = 0.5 / (252 * 6.5 * 3600) ≈ 8.48e-8
```

This tiny `dt` produces sub-cent moves per tick that accumulate naturally into realistic intraday ranges. Because the formula is multiplicative (`exp(...)` is always positive), prices can never go negative.

### 7.2 Correlated Moves via Cholesky Decomposition

Real stocks don't move independently. Given a correlation matrix `C`, computing `L = cholesky(C)` and applying it to independent standard normals `Z_independent` yields correlated draws: `Z_correlated = L @ Z_independent`.

Correlation structure: tech stocks correlate at 0.6 with each other, finance stocks at 0.5, everything else (including TSLA, which trades independently) at 0.3.

### 7.3 Full Implementation

```python
"""GBM-based market simulator."""

from __future__ import annotations

import asyncio
import logging
import math
import random

import numpy as np

from .cache import PriceCache
from .interface import MarketDataSource
from .seed_prices import (
    CORRELATION_GROUPS,
    CROSS_GROUP_CORR,
    DEFAULT_PARAMS,
    INTRA_FINANCE_CORR,
    INTRA_TECH_CORR,
    SEED_PRICES,
    TICKER_PARAMS,
    TSLA_CORR,
)

logger = logging.getLogger(__name__)


class GBMSimulator:
    """Geometric Brownian Motion simulator for correlated stock prices.

    Math:
        S(t+dt) = S(t) * exp((mu - sigma^2/2) * dt + sigma * sqrt(dt) * Z)

    The tiny dt (~8.5e-8 for 500ms ticks over 252 trading days * 6.5h/day)
    produces sub-cent moves per tick that accumulate naturally over time.
    """

    TRADING_SECONDS_PER_YEAR = 252 * 6.5 * 3600  # 5,896,800
    DEFAULT_DT = 0.5 / TRADING_SECONDS_PER_YEAR  # ~8.48e-8

    def __init__(
        self,
        tickers: list[str],
        dt: float = DEFAULT_DT,
        event_probability: float = 0.001,
    ) -> None:
        self._dt = dt
        self._event_prob = event_probability

        self._tickers: list[str] = []
        self._prices: dict[str, float] = {}
        self._params: dict[str, dict[str, float]] = {}
        self._cholesky: np.ndarray | None = None

        for ticker in tickers:
            self._add_ticker_internal(ticker)
        self._rebuild_cholesky()

    def step(self) -> dict[str, float]:
        """Advance all tickers by one time step. Returns {ticker: new_price}.

        This is the hot path — called every 500ms. Keep it fast.
        """
        n = len(self._tickers)
        if n == 0:
            return {}

        z_independent = np.random.standard_normal(n)
        z_correlated = self._cholesky @ z_independent if self._cholesky is not None else z_independent

        result: dict[str, float] = {}
        for i, ticker in enumerate(self._tickers):
            params = self._params[ticker]
            mu = params["mu"]
            sigma = params["sigma"]

            drift = (mu - 0.5 * sigma**2) * self._dt
            diffusion = sigma * math.sqrt(self._dt) * z_correlated[i]
            self._prices[ticker] *= math.exp(drift + diffusion)

            # Random event: ~0.1% chance per tick per ticker
            # With 10 tickers at 2 ticks/sec, expect an event ~every 50 seconds
            if random.random() < self._event_prob:
                shock_magnitude = random.uniform(0.02, 0.05)
                shock_sign = random.choice([-1, 1])
                self._prices[ticker] *= 1 + shock_magnitude * shock_sign
                logger.debug(
                    "Random event on %s: %.1f%% %s",
                    ticker, shock_magnitude * 100, "up" if shock_sign > 0 else "down",
                )

            result[ticker] = round(self._prices[ticker], 2)

        return result

    def add_ticker(self, ticker: str) -> None:
        """Add a ticker to the simulation. Rebuilds the correlation matrix."""
        if ticker in self._prices:
            return
        self._add_ticker_internal(ticker)
        self._rebuild_cholesky()

    def remove_ticker(self, ticker: str) -> None:
        """Remove a ticker from the simulation. Rebuilds the correlation matrix."""
        if ticker not in self._prices:
            return
        self._tickers.remove(ticker)
        del self._prices[ticker]
        del self._params[ticker]
        self._rebuild_cholesky()

    def get_price(self, ticker: str) -> float | None:
        """Current price for a ticker, or None if not tracked."""
        return self._prices.get(ticker)

    def get_tickers(self) -> list[str]:
        """Return the list of currently tracked tickers."""
        return list(self._tickers)

    def _add_ticker_internal(self, ticker: str) -> None:
        """Add a ticker without rebuilding Cholesky (for batch initialization)."""
        if ticker in self._prices:
            return
        self._tickers.append(ticker)
        self._prices[ticker] = SEED_PRICES.get(ticker, random.uniform(50.0, 300.0))
        self._params[ticker] = TICKER_PARAMS.get(ticker, dict(DEFAULT_PARAMS))

    def _rebuild_cholesky(self) -> None:
        """Rebuild the Cholesky decomposition of the ticker correlation matrix.

        Called whenever tickers are added or removed. O(n^2) but n < 50.
        """
        n = len(self._tickers)
        if n <= 1:
            self._cholesky = None
            return

        corr = np.eye(n)
        for i in range(n):
            for j in range(i + 1, n):
                rho = self._pairwise_correlation(self._tickers[i], self._tickers[j])
                corr[i, j] = rho
                corr[j, i] = rho

        self._cholesky = np.linalg.cholesky(corr)

    @staticmethod
    def _pairwise_correlation(t1: str, t2: str) -> float:
        """Determine correlation between two tickers based on sector grouping."""
        tech = CORRELATION_GROUPS["tech"]
        finance = CORRELATION_GROUPS["finance"]

        # TSLA is in the tech set but behaves independently
        if t1 == "TSLA" or t2 == "TSLA":
            return TSLA_CORR
        if t1 in tech and t2 in tech:
            return INTRA_TECH_CORR
        if t1 in finance and t2 in finance:
            return INTRA_FINANCE_CORR
        return CROSS_GROUP_CORR


class SimulatorDataSource(MarketDataSource):
    """MarketDataSource backed by the GBM simulator.

    Runs a background asyncio task that calls GBMSimulator.step() every
    `update_interval` seconds and writes results to the PriceCache.
    """

    def __init__(
        self,
        price_cache: PriceCache,
        update_interval: float = 0.5,
        event_probability: float = 0.001,
    ) -> None:
        self._cache = price_cache
        self._interval = update_interval
        self._event_prob = event_probability
        self._sim: GBMSimulator | None = None
        self._task: asyncio.Task | None = None

    async def start(self, tickers: list[str]) -> None:
        self._sim = GBMSimulator(tickers=tickers, event_probability=self._event_prob)
        # Seed the cache with initial prices so SSE has data immediately
        for ticker in tickers:
            price = self._sim.get_price(ticker)
            if price is not None:
                self._cache.update(ticker=ticker, price=price)
        self._task = asyncio.create_task(self._run_loop(), name="simulator-loop")
        logger.info("Simulator started with %d tickers", len(tickers))

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        logger.info("Simulator stopped")

    async def add_ticker(self, ticker: str) -> None:
        if self._sim:
            self._sim.add_ticker(ticker)
            price = self._sim.get_price(ticker)
            if price is not None:
                self._cache.update(ticker=ticker, price=price)
            logger.info("Simulator: added ticker %s", ticker)

    async def remove_ticker(self, ticker: str) -> None:
        if self._sim:
            self._sim.remove_ticker(ticker)
        self._cache.remove(ticker)
        logger.info("Simulator: removed ticker %s", ticker)

    def get_tickers(self) -> list[str]:
        return self._sim.get_tickers() if self._sim else []

    async def _run_loop(self) -> None:
        """Core loop: step the simulation, write to cache, sleep."""
        while True:
            try:
                if self._sim:
                    prices = self._sim.step()
                    for ticker, price in prices.items():
                        self._cache.update(ticker=ticker, price=price)
            except Exception:
                logger.exception("Simulator step failed")
            await asyncio.sleep(self._interval)
```

**Key behaviors:**

- **Immediate seeding** — `start()` populates the cache with seed prices *before* the loop begins, so the SSE endpoint has data on its very first tick. `add_ticker()` does the same, so a ticker added mid-session has a price right away instead of waiting for the next `step()`.
- **Graceful cancellation** — `stop()` cancels the task and awaits it, swallowing `CancelledError`. Clean under FastAPI lifespan teardown.
- **Exception resilience** — the loop catches exceptions per-step, so one bad tick can't kill the whole feed.
- **`get_tickers()` is public on both classes** — `SimulatorDataSource.get_tickers()` delegates to `GBMSimulator.get_tickers()` rather than reaching into a private attribute.

---

## 8. Massive API Client — `massive_client.py`

Polls the Massive (formerly Polygon.io) REST snapshot endpoint on a configurable interval. The `massive` package's `RESTClient` is synchronous, so calls run inside `asyncio.to_thread()` to avoid blocking the event loop.

```python
"""Massive (Polygon.io) API client for real market data."""

from __future__ import annotations

import asyncio
import logging

from massive import RESTClient
from massive.rest.models import SnapshotMarketType

from .cache import PriceCache
from .interface import MarketDataSource

logger = logging.getLogger(__name__)


class MassiveDataSource(MarketDataSource):
    """MarketDataSource backed by the Massive (Polygon.io) REST API.

    Polls GET /v2/snapshot/locale/us/markets/stocks/tickers for all watched
    tickers in a single API call, then writes results to the PriceCache.

    Rate limits:
      - Free tier: 5 req/min → poll every 15s (default)
      - Paid tiers: higher limits → poll every 2-5s
    """

    def __init__(
        self,
        api_key: str,
        price_cache: PriceCache,
        poll_interval: float = 15.0,
    ) -> None:
        self._api_key = api_key
        self._cache = price_cache
        self._interval = poll_interval
        self._tickers: list[str] = []
        self._task: asyncio.Task | None = None
        self._client: RESTClient | None = None

    async def start(self, tickers: list[str]) -> None:
        self._client = RESTClient(api_key=self._api_key)
        self._tickers = list(tickers)

        # Do an immediate first poll so the cache has data right away
        await self._poll_once()

        self._task = asyncio.create_task(self._poll_loop(), name="massive-poller")
        logger.info(
            "Massive poller started: %d tickers, %.1fs interval",
            len(tickers), self._interval,
        )

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        self._client = None
        logger.info("Massive poller stopped")

    async def add_ticker(self, ticker: str) -> None:
        ticker = ticker.upper().strip()
        if ticker not in self._tickers:
            self._tickers.append(ticker)
            logger.info("Massive: added ticker %s (will appear on next poll)", ticker)

    async def remove_ticker(self, ticker: str) -> None:
        ticker = ticker.upper().strip()
        self._tickers = [t for t in self._tickers if t != ticker]
        self._cache.remove(ticker)
        logger.info("Massive: removed ticker %s", ticker)

    def get_tickers(self) -> list[str]:
        return list(self._tickers)

    async def _poll_loop(self) -> None:
        """Poll on interval. First poll already happened in start()."""
        while True:
            await asyncio.sleep(self._interval)
            await self._poll_once()

    async def _poll_once(self) -> None:
        """Execute one poll cycle: fetch snapshots, update cache."""
        if not self._tickers or not self._client:
            return

        try:
            # The Massive RESTClient is synchronous — run in a thread to
            # avoid blocking the event loop.
            snapshots = await asyncio.to_thread(self._fetch_snapshots)
            processed = 0
            for snap in snapshots:
                try:
                    price = snap.last_trade.price
                    # Massive timestamps are Unix milliseconds → convert to seconds
                    timestamp = snap.last_trade.timestamp / 1000.0
                    self._cache.update(ticker=snap.ticker, price=price, timestamp=timestamp)
                    processed += 1
                except (AttributeError, TypeError) as e:
                    logger.warning("Skipping snapshot for %s: %s", getattr(snap, "ticker", "???"), e)
            logger.debug("Massive poll: updated %d/%d tickers", processed, len(self._tickers))

        except Exception as e:
            logger.error("Massive poll failed: %s", e)
            # Don't re-raise — the loop retries on the next interval.
            # Common failures: 401 (bad key), 429 (rate limit), network errors.

    def _fetch_snapshots(self) -> list:
        """Synchronous call to the Massive REST API. Runs in a thread."""
        return self._client.get_snapshot_all(
            market_type=SnapshotMarketType.STOCKS,
            tickers=self._tickers,
        )
```

### 8.1 Massive API Reference

The `massive` package (`pip install -U massive` / `uv add massive`, Python 3.9+) wraps the REST API formerly known as Polygon.io. Auth is via `RESTClient(api_key=...)` or the `MASSIVE_API_KEY` env var, which the client reads automatically.

The snapshot endpoint used here returns **all requested tickers in a single call** — critical for staying under the free tier's 5 req/min limit:

```python
snapshots = client.get_snapshot_all(
    market_type=SnapshotMarketType.STOCKS,
    tickers=["AAPL", "GOOGL", "MSFT"],
)
for snap in snapshots:
    snap.ticker                  # "AAPL"
    snap.last_trade.price        # 190.50
    snap.last_trade.timestamp    # Unix ms
    snap.day.previous_close      # for day-change calculations later
    snap.day.change_percent
```

Other endpoints available for future features (not currently used by the poller): `get_snapshot_ticker()` for a single-ticker detail view, `get_previous_close_agg()` for previous-day OHLC, `list_aggs()` for historical bars (useful if historical charting is added later).

### 8.2 Error Handling Philosophy

The poller is deliberately resilient — a bad API response should never take down the price feed:

| Error | Behavior |
|-------|----------|
| **401 Unauthorized** | Logged as error. Poller keeps running (user might fix `.env` and restart the container). |
| **429 Rate Limited** | Logged as error. Retried on the next scheduled poll. |
| **Network timeout** | Logged as error. Retried automatically on the next cycle. |
| **Malformed snapshot** | That ticker is skipped with a warning; other tickers in the same batch are still processed. |
| **All tickers fail** | Cache retains last-known prices — SSE keeps streaming stale data, which is better than blank data. |

### 8.3 Why `massive` is a core dependency, not a lazy import

`massive` and `massive.rest.models` are imported at module level. This differs from earlier design drafts that lazy-imported inside `start()` to make the package "optional." In practice `massive>=1.0.0` is declared as a hard dependency in `pyproject.toml` (it's needed for the demo script and installed unconditionally by `uv sync`), so lazy-importing bought no real benefit and only made `unittest.mock.patch("app.market.massive_client.RESTClient")` awkward in tests (the target didn't exist at module scope until first call). Top-level import keeps mocking straightforward and the code simpler; the simulator path still never touches this module at all, since `factory.py` only constructs a `MassiveDataSource` when `MASSIVE_API_KEY` is set.

---

## 9. Factory — `factory.py`

```python
"""Factory for creating market data sources."""

from __future__ import annotations

import logging
import os

from .cache import PriceCache
from .interface import MarketDataSource
from .massive_client import MassiveDataSource
from .simulator import SimulatorDataSource

logger = logging.getLogger(__name__)


def create_market_data_source(price_cache: PriceCache) -> MarketDataSource:
    """Create the appropriate market data source based on environment variables.

    - MASSIVE_API_KEY set and non-empty → MassiveDataSource (real market data)
    - Otherwise → SimulatorDataSource (GBM simulation)

    Returns an unstarted source. Caller must await source.start(tickers).
    """
    api_key = os.environ.get("MASSIVE_API_KEY", "").strip()

    if api_key:
        logger.info("Market data source: Massive API (real data)")
        return MassiveDataSource(api_key=api_key, price_cache=price_cache)
    else:
        logger.info("Market data source: GBM Simulator")
        return SimulatorDataSource(price_cache=price_cache)
```

This is the single decision point in the whole system for "which data source." Everything else — including this document's SSE and lifecycle sections — is written against `MarketDataSource` and never checks `MASSIVE_API_KEY` again.

---

## 10. SSE Streaming Endpoint — `stream.py`

```python
"""SSE streaming endpoint for live price updates."""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncGenerator

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

from .cache import PriceCache

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/stream", tags=["streaming"])


def create_stream_router(price_cache: PriceCache) -> APIRouter:
    """Create the SSE streaming router with a reference to the price cache.

    This factory pattern lets us inject the PriceCache without globals.
    """

    @router.get("/prices")
    async def stream_prices(request: Request) -> StreamingResponse:
        """SSE endpoint for live price updates.

        Streams all tracked ticker prices every ~500ms. The client connects
        with EventSource and receives events in the format:

            data: {"AAPL": {"ticker": "AAPL", "price": 190.50, ...}, ...}

        Includes a retry directive so the browser auto-reconnects on
        disconnection (EventSource built-in behavior).
        """
        return StreamingResponse(
            _generate_events(price_cache, request),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",  # Disable nginx buffering if proxied
            },
        )

    return router


async def _generate_events(
    price_cache: PriceCache,
    request: Request,
    interval: float = 0.5,
) -> AsyncGenerator[str, None]:
    """Async generator that yields SSE-formatted price events.

    Sends all prices every `interval` seconds. Stops when the client
    disconnects (detected via request.is_disconnected()).
    """
    # Tell the client to retry after 1 second if the connection drops
    yield "retry: 1000\n\n"

    last_version = -1
    client_ip = request.client.host if request.client else "unknown"
    logger.info("SSE client connected: %s", client_ip)

    try:
        while True:
            if await request.is_disconnected():
                logger.info("SSE client disconnected: %s", client_ip)
                break

            current_version = price_cache.version
            if current_version != last_version:
                last_version = current_version
                prices = price_cache.get_all()

                if prices:
                    data = {ticker: update.to_dict() for ticker, update in prices.items()}
                    yield f"data: {json.dumps(data)}\n\n"

            await asyncio.sleep(interval)
    except asyncio.CancelledError:
        logger.info("SSE stream cancelled for: %s", client_ip)
```

### 10.1 Wire Format

```
data: {"AAPL":{"ticker":"AAPL","price":190.50,"previous_price":190.42,"timestamp":1707580800.5,"change":0.08,"change_percent":0.042,"direction":"up"},"GOOGL":{...}}

```

Client side:

```javascript
const eventSource = new EventSource('/api/stream/prices');
eventSource.onmessage = (event) => {
    const prices = JSON.parse(event.data);
    // { "AAPL": { ticker, price, previous_price, timestamp, change, change_percent, direction }, ... }
};
```

### 10.2 Why Poll-and-Push Instead of Event-Driven?

The endpoint polls `price_cache.version` on a fixed 500ms interval rather than being notified by the data source directly. This is simpler (no pub/sub plumbing between producer and consumer) and produces evenly-spaced updates, which matters because the frontend accumulates these into sparkline charts — regular spacing keeps that visualization clean regardless of whether the underlying source is a 500ms simulator or a 15s Massive poller.

---

## 11. Public Package API — `__init__.py`

```python
"""Market data subsystem for FinAlly.

Public API:
    PriceUpdate         - Immutable price snapshot dataclass
    PriceCache          - Thread-safe in-memory price store
    MarketDataSource    - Abstract interface for data providers
    create_market_data_source - Factory that selects simulator or Massive
    create_stream_router - FastAPI router factory for SSE endpoint
"""

from .cache import PriceCache
from .factory import create_market_data_source
from .interface import MarketDataSource
from .models import PriceUpdate
from .stream import create_stream_router

__all__ = [
    "PriceUpdate",
    "PriceCache",
    "MarketDataSource",
    "create_market_data_source",
    "create_stream_router",
]
```

Downstream code should always import from `app.market`, never from a submodule:

```python
from app.market import PriceCache, PriceUpdate, MarketDataSource, create_market_data_source, create_stream_router
```

---

## 12. FastAPI Lifecycle Integration (prescriptive)

`app/main.py` does not exist yet — this section is guidance for whoever wires up the FastAPI app, not a description of existing code. The market data system should start and stop with the app via the `lifespan` context manager.

```python
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.market import PriceCache, MarketDataSource, create_market_data_source, create_stream_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage startup and shutdown of background services."""

    # --- STARTUP ---

    price_cache = PriceCache()
    app.state.price_cache = price_cache

    source = create_market_data_source(price_cache)
    app.state.market_source = source

    # Load initial tickers from the SQLite watchlist table (lazy-init if needed)
    initial_tickers = await load_watchlist_tickers()
    await source.start(initial_tickers)

    yield  # App is running

    # --- SHUTDOWN ---
    await source.stop()


app = FastAPI(title="FinAlly", lifespan=lifespan)

# Register once, at app construction time — not inside lifespan, since
# create_stream_router() must not be called more than once per router instance
app.include_router(create_stream_router(app.state.price_cache))
```

Note the router registration happens once, outside `lifespan`, using a `PriceCache` created before the app object exists (or via a small refactor where `PriceCache()` is instantiated at module scope and passed into both `lifespan` and `create_stream_router`). Calling `create_stream_router()` twice registers `/prices` twice on the same module-level `router` object in `stream.py` — a latent footgun worth avoiding by construction rather than by discipline.

### Dependency injection for other routes

```python
from fastapi import APIRouter, Depends, HTTPException, Request

def get_price_cache(request: Request) -> PriceCache:
    return request.app.state.price_cache

def get_market_source(request: Request) -> MarketDataSource:
    return request.app.state.market_source


router = APIRouter(prefix="/api")

@router.post("/portfolio/trade")
async def execute_trade(
    trade: TradeRequest,
    price_cache: PriceCache = Depends(get_price_cache),
):
    current_price = price_cache.get_price(trade.ticker)
    if current_price is None:
        raise HTTPException(400, f"Price not yet available for {trade.ticker}. Try again shortly.")
    # ... execute trade at current_price ...


@router.post("/watchlist")
async def add_to_watchlist(
    payload: WatchlistAdd,
    source: MarketDataSource = Depends(get_market_source),
):
    # ... insert into watchlist table ...
    await source.add_ticker(payload.ticker)
    # ...
```

---

## 13. Watchlist Coordination

When the watchlist changes — via the REST API or the LLM chat's `watchlist_changes` — the market data source must be told, so it tracks the right ticker set.

**Adding a ticker:**

```
POST /api/watchlist {ticker: "PYPL"}
  → INSERT INTO watchlist (SQLite)
  → await source.add_ticker("PYPL")
      Simulator: adds to GBMSimulator, rebuilds Cholesky, seeds cache immediately
      Massive:   appends to internal ticker list, appears on next poll (up to poll_interval delay)
  → Return success
```

**Removing a ticker:**

```
DELETE /api/watchlist/PYPL
  → DELETE FROM watchlist (SQLite)
  → await source.remove_ticker("PYPL")
      Simulator: removes from GBMSimulator, rebuilds Cholesky, removes from cache
      Massive:   removes from ticker list, removes from cache
  → Return success
```

**Edge case — ticker has an open position:** if the user removes a ticker from the watchlist while still holding shares, the data source should keep tracking it so portfolio valuation stays accurate:

```python
@router.delete("/watchlist/{ticker}")
async def remove_from_watchlist(
    ticker: str,
    source: MarketDataSource = Depends(get_market_source),
):
    await db.delete_watchlist_entry(ticker)

    position = await db.get_position(ticker)
    if position is None or position.quantity == 0:
        await source.remove_ticker(ticker)

    return {"status": "ok"}
```

---

## 14. Testing Strategy

**73 tests, all passing**, across 6 modules in `backend/tests/market/`. Overall coverage: 84%.

| Module | Tests | What it covers |
|--------|-------|-----------------|
| `test_models.py` | 11 | `PriceUpdate` properties (`change`, `change_percent`, `direction`, `to_dict()`), edge cases like zero previous price |
| `test_cache.py` | 13 | `PriceCache` update/get/get_all/remove, direction computation, version increments |
| `test_simulator.py` | 17 | GBM math correctness, prices stay positive, add/remove ticker, Cholesky rebuild triggers, unknown-ticker random seeding |
| `test_simulator_source.py` | 10 | `SimulatorDataSource` integration: cache seeded on start, prices change over time, clean stop (including double-stop), add/remove ticker end-to-end |
| `test_factory.py` | 7 | Env var selection logic (`MASSIVE_API_KEY` set vs. unset/blank) |
| `test_massive.py` | 13 | `MassiveDataSource` with `_fetch_snapshots` mocked: successful poll, malformed snapshot skipped, API error doesn't crash the loop |

Per-file coverage: `models.py`, `cache.py`, `interface.py`, `seed_prices.py`, `factory.py` all at 100%; `simulator.py` at 98%; `massive_client.py` at 56% (expected — real HTTP calls aren't exercised, only the parsing/error-handling logic around a mocked `_fetch_snapshots`); `stream.py` at 31% (expected — SSE needs a running ASGI server; would require an `httpx.AsyncClient`-based integration test to raise, and is a reasonable gap to close once `main.py` exists).

### Representative test patterns

```python
# GBM prices never go negative
def test_prices_are_positive(self):
    sim = GBMSimulator(tickers=["AAPL"])
    for _ in range(10_000):
        prices = sim.step()
        assert prices["AAPL"] > 0

# Cache computes direction/change correctly
def test_direction_up(self):
    cache = PriceCache()
    cache.update("AAPL", 190.00)
    update = cache.update("AAPL", 191.00)
    assert update.direction == "up"
    assert update.change == 1.00

# Massive poller degrades gracefully on API failure
async def test_api_error_does_not_crash(self):
    cache = PriceCache()
    source = MassiveDataSource(api_key="test-key", price_cache=cache, poll_interval=60.0)
    source._tickers = ["AAPL"]
    with patch.object(source, "_fetch_snapshots", side_effect=Exception("network error")):
        await source._poll_once()  # Should not raise
    assert cache.get_price("AAPL") is None
```

---

## 15. Error Handling & Edge Cases

**Empty watchlist at startup.** If the database has no watchlist entries, `start([])` is a no-op for both sources — the simulator produces no prices, the Massive poller skips its API call entirely (`_poll_once` returns early when `self._tickers` is empty). SSE sends `retry:` but no `data:` events until a ticker is added, at which point tracking begins immediately (simulator: instant; Massive: next poll).

**Price cache miss during a trade.** If a user tries to trade a ticker with no cached price yet (just added, Massive hasn't polled), the trade route should return a clear 400:

```python
price = price_cache.get_price(ticker)
if price is None:
    raise HTTPException(400, f"Price not yet available for {ticker}. Please wait a moment and try again.")
```

The simulator avoids this case entirely by seeding the cache synchronously in both `start()` and `add_ticker()`. Massive has an unavoidable gap between "ticker added" and "first poll after that."

**Invalid `MASSIVE_API_KEY`.** The first poll fails with 401; the poller logs it and keeps retrying every `poll_interval`. SSE keeps streaming (connection is fine, payload is just empty/stale). The user sees a green connection dot but no data — the fix is correcting the key and restarting the container, not a code change.

**Thread safety under load.** `PriceCache` uses a plain mutex; the critical section is a dict lookup plus assignment. At the project's scale (≤~10-50 tickers, 2 writes/sec, a handful of SSE readers) contention is negligible. If this ever became a bottleneck, a read/write lock would be the fix — not needed here.

**Numeric precision.** GBM's tiny `dt` produces very small per-tick log-returns, but `exp()` is numerically stable and prices are rounded to 2 decimals on every read/write, so there's no precision concern in practice.

---

## 16. Configuration Summary

| Parameter | Location | Default | Description |
|-----------|----------|---------|--------------|
| `MASSIVE_API_KEY` | Environment variable | `""` (empty) | If set and non-empty, use Massive API; otherwise use the simulator |
| `update_interval` | `SimulatorDataSource.__init__` | `0.5` sec | Time between simulator ticks |
| `event_probability` | `GBMSimulator.__init__` | `0.001` | Chance of a random shock event, per ticker, per tick |
| `dt` | `GBMSimulator.__init__` | `~8.48e-8` | GBM time step, as a fraction of a trading year |
| `poll_interval` | `MassiveDataSource.__init__` | `15.0` sec | Time between Massive API polls (free tier: 5 req/min ⇒ 15s minimum safe interval) |
| SSE push interval | `_generate_events()` | `0.5` sec | Time between cache-version checks / pushes to the client |
| SSE retry directive | `_generate_events()` | `1000` ms | Browser `EventSource` reconnection delay after disconnect |

Both data source constructors accept these as keyword arguments, so a paid Massive tier could lower `poll_interval` to 2–5s without any code changes — only the value passed at construction in `factory.py` (or an additional env var, if that flexibility is wanted later) would need to change.
