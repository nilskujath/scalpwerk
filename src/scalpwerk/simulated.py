import csv
import threading
import typing
import uuid

from .core import (
    BrokerConnectorBase,
    DatafeedConnectorBase,
    Enums,
    Events,
    Exposure,
    Types,
)


class CSVDatafeedConnector(DatafeedConnectorBase):
    CSV_PATH: str

    _DATABENTO_RTYPE_TO_BAR_PERIOD: dict[int, Enums.BarPeriod] = {
        32: Enums.BarPeriod.OHLCV_1S,
        33: Enums.BarPeriod.OHLCV_1M,
        34: Enums.BarPeriod.OHLCV_1H,
        35: Enums.BarPeriod.OHLCV_1D,
    }

    def __init__(self) -> None:
        self._subscriptions: set[tuple[Enums.BarPeriod, str]] = set()
        self._stop_event = threading.Event()
        self._streaming_thread: threading.Thread | None = None
        self._csv_file: typing.IO[str] | None = None
        self._csv_reader: typing.Any | None = None
        self._column_indices: dict[str, int] = {}

    # Called before connect(). Just collects; streaming starts in _connect().
    def subscribe(
        self,
        symbols: list[str],
        record_type: Enums.BarPeriod,
    ) -> None:
        for symbol in symbols:
            self._subscriptions.add((record_type, symbol))

    def _connect(self) -> None:
        self._csv_file = open(self.CSV_PATH, newline="")
        self._csv_reader = csv.reader(self._csv_file)
        header = next(self._csv_reader)
        self._column_indices = {name: i for i, name in enumerate(header)}

        # Streaming starts here, not in subscribe(). The Orchestrator calls
        # subscribe() before connect(), so all subscriptions are set by now.
        self._stop_event.clear()
        self._streaming_thread = threading.Thread(target=self._stream)
        self._streaming_thread.start()

    def _disconnect(self) -> None:
        self._stop_event.set()
        if self._streaming_thread is not None and self._streaming_thread.is_alive():
            self._streaming_thread.join()
            self._streaming_thread = None
        if self._csv_file is not None:
            self._csv_file.close()
            self._csv_file = None
        self._csv_reader = None

    def _stream(self) -> None:
        try:
            if self._csv_reader is None:
                raise RuntimeError("CSV reader not initialized")

            for row in self._csv_reader:
                if self._stop_event.is_set():
                    break

                symbol = row[self._column_indices["symbol"]]
                record_type = self._DATABENTO_RTYPE_TO_BAR_PERIOD[
                    int(row[self._column_indices["rtype"]])
                ]

                if (record_type, symbol) not in self._subscriptions:
                    continue

                self._emit_event(
                    Events.Datafeed.Bar(
                        occurred_at_ns=Types.UnixNanoseconds(
                            int(row[self._column_indices["ts_event"]])
                        ),
                        symbol=symbol,
                        record_type=record_type,
                        open=Types.ScaledPrice(int(row[self._column_indices["open"]])),
                        high=Types.ScaledPrice(int(row[self._column_indices["high"]])),
                        low=Types.ScaledPrice(int(row[self._column_indices["low"]])),
                        close=Types.ScaledPrice(
                            int(row[self._column_indices["close"]])
                        ),
                        volume=int(row[self._column_indices["volume"]]),
                    )
                )
                self._wait_until_system_idle()
        finally:
            self.trigger_shutdown()


class SimulatedBroker(BrokerConnectorBase):
    def _connect(self) -> None:
        raise NotImplementedError

    def _disconnect(self) -> None:
        raise NotImplementedError

    def _get_exposure_snapshot(
        self,
    ) -> tuple[dict[uuid.UUID, Exposure.WorkingOrder], dict[str, Exposure.Position]]:
        raise NotImplementedError

    def _on_submit_order(self, event: Events.Strategy.SubmitOrder) -> None:
        raise NotImplementedError

    def _on_modify_order(self, event: Events.Strategy.ModifyOrder) -> None:
        raise NotImplementedError

    def _on_cancel_order(self, event: Events.Strategy.CancelOrder) -> None:
        raise NotImplementedError
