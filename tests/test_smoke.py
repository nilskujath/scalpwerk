"""
Smoke test: buy on first bar, sell 10 bars later.

Verifies the full pipeline: CSVDatafeedConnector → SimulatedBroker → Strategy → SQLiteRecorder.
"""

from scalpwerk.core import Enums, Orchestrator, StrategyBase, Events
from scalpwerk.indicators import (
    SMA,
    ATR,
    RSI,
    ReverseRSI,
    BollingerUpper,
    BollingerLower,
    BollingerBandwidth,
    BoostedRSI,
    SwingHigh,
    SwingLow,
    BBLowerTurnaround,
    BBUpperTurnaround,
)
from scalpwerk.simulated import CSVDatafeedConnector, SimulatedBroker
from scalpwerk.recorders import SQLiteRecorder

import pathlib

# --- Configuration via subclassing ---

MARKET_DATA = "/Users/practice/Desktop/market_data/MNQ_continuous.csv"
DB_PATH = pathlib.Path("/Users/practice/Desktop/docs/scalpwerk/tests/smoke_test.db")

SYMBOL = "MNQ.v.0"


class SmokeDatafeed(CSVDatafeedConnector):
    CSV_PATH = MARKET_DATA
    START = "2024-01-08"  # a Monday
    STOP = "2024-01-08T00:30:00"  # 30 minutes of 1s bars


class SmokeBroker(SimulatedBroker):
    COMMISSION_PER_UNIT = 0.85
    MINIMUM_COMMISSION_PER_ORDER = 1.00


class SmokeRecorder(SQLiteRecorder):
    DB_PATH = DB_PATH


class SmokeStrategy(StrategyBase):
    SYMBOLS = {SYMBOL}
    RECORD_TYPE = Enums.BarPeriod.OHLCV_1S

    def setup(self) -> None:
        self._bar_count = 0
        self.sma = self.add_indicator(SMA(period=20, bar_field=Enums.BarField.CLOSE))
        self.atr = self.add_indicator(ATR(period=14))
        self.rsi = self.add_indicator(RSI(period=14))

        self.bb_upper = self.add_indicator(BollingerUpper(period=20, num_std=2.0))
        self.bb_lower = self.add_indicator(BollingerLower(period=20, num_std=2.0))
        self.bb_bw = self.add_indicator(BollingerBandwidth(period=20, num_std=2.0))

        self.cbci = self.add_indicator(BoostedRSI())

        self.rev_rsi_ob = self.add_indicator(ReverseRSI(target_rsi=80.0))
        self.rev_rsi_os = self.add_indicator(ReverseRSI(target_rsi=20.0))

        self.swing_high = self.add_indicator(
            SwingHigh(atr_period=14, atr_multiplier=2.0)
        )
        self.swing_low = self.add_indicator(SwingLow(atr_period=14, atr_multiplier=2.0))

        self.bb_lower_turn = self.add_indicator(BBLowerTurnaround(method="pivot"))
        self.bb_upper_turn = self.add_indicator(
            BBUpperTurnaround(method="ma_crossover")
        )

    def on_bar(self, bar: Events.Datafeed.Bar) -> None:
        self._bar_count += 1

        if self._bar_count == 1:
            self.submit_order(
                symbol=SYMBOL,
                order_type=Enums.OrderType.MARKET,
                side=Enums.TradeSide.BUY,
                quantity=1,
            )

        elif self._bar_count == 11:
            self.submit_order(
                symbol=SYMBOL,
                order_type=Enums.OrderType.MARKET,
                side=Enums.TradeSide.SELL,
                quantity=1,
            )


if __name__ == "__main__":
    # Clean up previous run.
    DB_PATH.unlink(missing_ok=True)

    orchestrator = Orchestrator(
        strategy_classes=[SmokeStrategy],
        broker=SmokeBroker,
        datafeed=SmokeDatafeed,
        recorders=[SmokeRecorder],
    )

    print("Running smoke test...")
    orchestrator.run()
    print("Done.")

    # Quick verification: read fills from the database.
    # Schema version is embedded in the filename by SQLiteRecorder.
    import sqlite3

    actual_db = DB_PATH.with_stem(f"{DB_PATH.stem}_v{SmokeRecorder._SCHEMA_VERSION}")
    conn = sqlite3.connect(actual_db)
    conn.row_factory = sqlite3.Row

    fills = conn.execute("SELECT * FROM broker_fill ORDER BY occurred_at_ns").fetchall()
    print(f"\nFills: {len(fills)}")
    for f in fills:
        print(
            f"  {f['side']:>4} {f['filled_quantity']}x "
            f"@ {f['fill_price'] / 1_000_000_000:,.2f}  "
            f"pos={f['position_size']} "
            f"cost_basis={f['position_cost_basis'] / 1_000_000_000:,.2f}"
        )

    bars = conn.execute("SELECT COUNT(*) as n FROM datafeed_bar").fetchone()
    print(f"\nBars recorded: {bars['n']}")

    indicators = conn.execute(
        "SELECT indicator_name, COUNT(*) as n, "
        "MIN(indicator_value) as min_val, MAX(indicator_value) as max_val "
        "FROM strategy_indicator_value GROUP BY indicator_name"
    ).fetchall()
    print(f"\nIndicators ({len(indicators)}):")
    for ind in indicators:
        print(
            f"  {ind['indicator_name']}: {ind['n']} values, "
            f"range [{ind['min_val']:.2f}, {ind['max_val']:.2f}]"
        )

    conn.close()
