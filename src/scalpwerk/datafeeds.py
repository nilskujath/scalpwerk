import csv

from collections.abc import Iterator
from pathlib import Path

from .core import DatafeedConnectorBase, Symbol, PeriodType, Events


class CSVDatafeedConnector(DatafeedConnectorBase):
    _DATABENTO_RTYPE_TO_PERIOD: dict[int, PeriodType] = {
        32: PeriodType.SECOND,
        33: PeriodType.MINUTE,
        34: PeriodType.HOUR,
        35: PeriodType.DAY,
    }

    def __init__(self, csv_path: Path) -> None:
        self._csv_path = csv_path
        self._subscriptions: frozenset[tuple[Symbol, PeriodType]] = frozenset()
        super().__init__()

    def _subscribe(self, period_type: PeriodType, symbols: frozenset[Symbol]) -> None:
        self._subscriptions |= frozenset((symbol, period_type) for symbol in symbols)

    def _connect(self) -> None:
        for bar in self._iter_bars():
            self._wait_until_system_idle()
            self.emit(bar)
        self.emit(Events.System.Shutdown())

    def _iter_bars(self) -> Iterator[Events.Datafeed.Bar]:
        with open(self._csv_path) as f:
            header = next(csv.reader(f))
            col = {name: pos for pos, name in enumerate(header)}

            for row in csv.reader(f):
                sym = row[col["symbol"]]
                period = self._DATABENTO_RTYPE_TO_PERIOD.get(int(row[col["rtype"]]))
                if period is None:
                    continue
                if (sym, period) not in self._subscriptions:
                    continue
                yield Events.Datafeed.Bar(
                    symbol=sym,
                    period_start=int(row[col["ts_event"]]),
                    period_type=period,
                    open=int(row[col["open"]]),
                    high=int(row[col["high"]]),
                    low=int(row[col["low"]]),
                    close=int(row[col["close"]]),
                    volume=(int(row[col["volume"]]) if row[col["volume"]] else None),
                )

    def _disconnect(self) -> None:
        pass
