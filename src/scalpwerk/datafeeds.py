import csv

from collections.abc import Iterator
from pathlib import Path

from .core import DatafeedConnectorBase, Symbol, PeriodType, Events


class CSVDatafeedConnector(DatafeedConnectorBase):
    def __init__(self, csv_path: Path) -> None:
        self._csv_path = csv_path
        self._subscriptions: frozenset[tuple[Symbol, PeriodType]] = frozenset()
        super().__init__()

    def _subscribe(self, period_type: PeriodType, symbols: frozenset[Symbol]) -> None:
        self._subscriptions |= frozenset((symbol, period_type) for symbol in symbols)

    def _connect(self) -> None:
        for bar in self._iter_bars():
            self._wait_until_system_idle()  # ensure previous bar is fully processed
            self.emit(bar)
        self.emit(Events.System.Shutdown())  # historical data is exhausted, stop system

    def _iter_bars(self) -> Iterator[Events.Datafeed.Bar]:
        with open(self._csv_path) as f:
            header = next(csv.reader(f))
            column_index = {name: position for position, name in enumerate(header)}

            for row in csv.reader(f):
                sym = row[column_index["symbol"]]
                period = PeriodType(int(row[column_index["rtype"]]))
                if (sym, period) not in self._subscriptions:
                    continue
                yield Events.Datafeed.Bar(
                    symbol=sym,
                    period_start=int(row[column_index["ts_event"]]),
                    period_type=period,
                    open=int(row[column_index["open"]]),
                    high=int(row[column_index["high"]]),
                    low=int(row[column_index["low"]]),
                    close=int(row[column_index["close"]]),
                    volume=(
                        int(row[column_index["volume"]])
                        if row[column_index["volume"]]
                        else None
                    ),
                )

    def _disconnect(self) -> None:
        pass
