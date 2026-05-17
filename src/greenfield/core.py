import csv
import json

from abc import ABC, abstractmethod
from collections import defaultdict, deque
from dataclasses import dataclass, field, replace, asdict
from enum import Enum, auto
from io import TextIOWrapper
from pathlib import Path
from queue import Queue
from threading import Thread
from time import time_ns
from typing import Protocol
from uuid import UUID, uuid4

RUNS_DIR = Path("runs")

PRICE_SCALE_FACTOR = 1_000_000_000

type NanosecondsSinceUnixEpoch = int
type ScaledPrice = int


class PeriodType(Enum):  # values for easy compatibility with Databento schema
    # fmt: off
    SECOND  = 32
    MINUTE  = 33
    HOUR    = 34
    DAY     = 35
    # fmt: on


class TradeSide(Enum):
    # fmt: off
    BUY     = auto()
    SELL    = auto()
    # fmt: on


class TimeInForce(Enum):
    # fmt: off
    DAY     = auto()
    GTC     = auto()
    # fmt: on


@dataclass(frozen=True, kw_only=True)
class WorkingOrder:
    # fmt: off
    symbol:         str
    order_id:       UUID
    trade_side:     TradeSide
    quantity:       int
    filled_qty:     int
    time_in_force:  TimeInForce
    limit_price:    ScaledPrice | None = None
    stop_price:     ScaledPrice | None = None
    # fmt: on


@dataclass(frozen=True, kw_only=True)
class OpenPosition:
    # fmt: off
    symbol:         str
    signed_qty:     int
    cost_basis:     ScaledPrice
    # fmt: on


@dataclass(frozen=True, kw_only=True)
class _EventBase:
    timestamp: NanosecondsSinceUnixEpoch = field(default_factory=lambda: time_ns())


@dataclass(frozen=True, kw_only=True)
class _SystemShutdown(_EventBase):
    pass


class Events:
    class Datafeed:
        @dataclass(frozen=True, kw_only=True)
        class Bar(_EventBase):
            # fmt: off
            symbol:         str
            period_start:   NanosecondsSinceUnixEpoch
            period_type:    PeriodType
            open:           ScaledPrice
            high:           ScaledPrice
            low:            ScaledPrice
            close:          ScaledPrice
            volume:         int | None = None
            # fmt: on

    class Strategy:
        @dataclass(frozen=True, kw_only=True)
        class StreamRequest(_EventBase):
            # fmt: off
            period_type:    PeriodType
            symbols:        frozenset[str]
            # fmt: on

        @dataclass(frozen=True, kw_only=True)
        class IndicatorUpdate(_EventBase):
            # fmt: off
            symbol:         str
            source_event:   "Events.Datafeed.Bar"
            ind_values:     dict[str, float]
            # fmt: on

        @dataclass(frozen=True, kw_only=True)
        class SubmitOrder(_EventBase):
            # fmt: off
            symbol:         str
            order_id:       UUID
            trade_side:     TradeSide
            quantity:       int
            time_in_force:  TimeInForce
            limit_price:    ScaledPrice | None = None
            stop_price:     ScaledPrice | None = None
            # fmt: on

        # We do not modify orders, cancel and resubmit is the way. This significantly
        # reduces the surface area of our system and reduces complexity.
        @dataclass(frozen=True, kw_only=True)
        class CancelOrder(_EventBase):
            # fmt: off
            symbol:         str
            order_id:       UUID
            # fmt: on

    class Broker:
        @dataclass(frozen=True, kw_only=True)
        class BrokerConnected(_EventBase):
            # fmt: off
            working_orders: dict[UUID, WorkingOrder]
            open_positions: dict[str, OpenPosition]
            # fmt: on

        @dataclass(frozen=True, kw_only=True)
        class OrderAccepted(_EventBase):
            # fmt: off
            symbol:         str
            order_id:       UUID
            # fmt: on

        @dataclass(frozen=True, kw_only=True)
        class OrderRejected(_EventBase):
            # fmt: off
            symbol:         str
            order_id:       UUID
            reason:         str
            # fmt: on

        @dataclass(frozen=True, kw_only=True)
        class CancellationAccepted(_EventBase):
            # fmt: off
            symbol:         str
            order_id:       UUID
            # fmt: on

        @dataclass(frozen=True, kw_only=True)
        class CancellationRejected(_EventBase):
            # fmt: off
            symbol:         str
            order_id:       UUID
            reason:         str
            # fmt: on

        @dataclass(frozen=True, kw_only=True)
        class Fill(_EventBase):
            # fmt: off
            # fill information
            symbol:         str
            fill_id:        UUID
            order_id:       UUID
            trade_side:     TradeSide
            filled_qty:     int
            fill_price:     ScaledPrice

            # position state after this fill
            signed_position_size:   int
            position_cost_basis:    ScaledPrice
            # fmt: on

        @dataclass(frozen=True, kw_only=True)
        class OrderExpired(_EventBase):
            # fmt: off
            symbol:         str
            order_id:       UUID
            # fmt: on


class _ComponentLike(Protocol):
    def receive(self, event: _EventBase) -> None: ...


class _EventBus:
    def __init__(self) -> None:
        self._subs: dict[type[_EventBase], set[_ComponentLike]] = defaultdict(set)

    def subscribe(self, component: _ComponentLike, *event_types: type[_EventBase]):
        for event_type in event_types:
            self._subs[event_type].add(component)

    def publish(self, event: _EventBase):
        for component in self._subs[type(event)]:
            component.receive(event)


_system_event_bus = _EventBus()


class _Connectable(ABC):
    @abstractmethod
    def _connect(self) -> None: ...

    @abstractmethod
    def _disconnect(self) -> None: ...


class _ComponentBase(_ComponentLike, ABC):
    SUBSCRIBE_TO: tuple[type[_EventBase], ...] = ()

    def __init__(self, event_bus: _EventBus = _system_event_bus) -> None:
        self._event_bus: _EventBus = event_bus
        self._event_bus.subscribe(self, *self.SUBSCRIBE_TO, _SystemShutdown)
        self._queue: Queue[_EventBase] = Queue()
        self._thread: Thread = Thread(target=self._event_loop, name=type(self).__name__)
        self._thread.start()

    def receive(self, event: _EventBase) -> None:
        self._queue.put(event)

    def emit(self, event: _EventBase) -> None:
        self._event_bus.publish(event)

    def _event_loop(self) -> None:
        while True:
            event = self._queue.get()
            if isinstance(event, _SystemShutdown):
                self._queue.task_done()
                break
            self._on_event(event)
            self._queue.task_done()
        if isinstance(self, _Connectable):
            self._disconnect()

    @abstractmethod
    def _on_event(self, event: _EventBase) -> None: ...


class RecorderBase(_ComponentBase, ABC): ...


class JSONLRecorder(RecorderBase):
    SUBSCRIBE_TO: tuple[type[_EventBase], ...] = tuple(
        cls for cls in _EventBase.__subclasses__() if cls is not _SystemShutdown
    )

    def __init__(self) -> None:
        self._run_id: str = str(uuid4())
        self._path: Path = RUNS_DIR / f"{self._run_id}.jsonl"
        self._jsonl_file: TextIOWrapper | None = None
        super().__init__()  # attributes must exist before starting thread

    def _event_loop(self) -> None:
        RUNS_DIR.mkdir(parents=True, exist_ok=True)
        self._jsonl_file = open(self._path, "a")
        try:
            super()._event_loop()
        finally:
            if self._jsonl_file is not None:
                self._jsonl_file.close()

    def _on_event(self, event: _EventBase) -> None:
        assert self._jsonl_file is not None
        record = {
            "run_id": self._run_id,
            "event_type": type(event).__qualname__,
            "data": asdict(event),
        }
        self._jsonl_file.write(json.dumps(record, default=str) + "\n")
        self._jsonl_file.flush()


class DatafeedConnectorBase(_ComponentBase, _Connectable, ABC):
    SUBSCRIBE_TO: tuple[type[_EventBase], ...] = (
        Events.Strategy.StreamRequest,
        Events.Broker.BrokerConnected,
    )

    @abstractmethod
    def _subscribe(self, period_type: PeriodType, symbols: frozenset[str]) -> None: ...

    def _on_event(self, event: _EventBase) -> None:
        match event:
            case Events.Strategy.StreamRequest() as event:
                self._subscribe(period_type=event.period_type, symbols=event.symbols)
            case Events.Broker.BrokerConnected():
                self._connect()  # connect datafeed only after broker is connected


class CSVDatafeedConnector(DatafeedConnectorBase):
    def __init__(self, csv_path: Path) -> None:
        self._csv_path = csv_path
        self._symbols: frozenset[str] = frozenset()
        super().__init__()

    def _subscribe(self, period_type: PeriodType, symbols: frozenset[str]) -> None:
        self._symbols |= symbols

    def _connect(self) -> None:
        with open(self._csv_path) as f:
            column_names = next(csv.reader(f))
            idx = {name: i for i, name in enumerate(column_names)}
            i_sym, i_ts, i_rt = idx["symbol"], idx["ts_event"], idx["rtype"]
            i_o, i_h, i_l, i_c, i_v = (
                idx["open"],
                idx["high"],
                idx["low"],
                idx["close"],
                idx["volume"],
            )
            for row in csv.reader(f):
                if row[i_sym] not in self._symbols:
                    continue
                self.emit(
                    Events.Datafeed.Bar(
                        symbol=row[i_sym],
                        period_start=int(row[i_ts]),
                        period_type=PeriodType(int(row[i_rt])),
                        open=int(row[i_o]),
                        high=int(row[i_h]),
                        low=int(row[i_l]),
                        close=int(row[i_c]),
                        volume=int(row[i_v]) if row[i_v] else None,
                    )
                )
        self.emit(_SystemShutdown())  # initiate system shutdown at EOF

    def _disconnect(self) -> None:
        pass


class _IndicatorBase(ABC):
    def __init__(self, max_history: int = 100) -> None:
        self._max_history = max(1, int(max_history))
        self._history: dict[str, deque[float]] = {}
        self._input_indicators: dict[str, "_IndicatorBase"] = {}

    @property
    @abstractmethod
    def name(self) -> str: ...  # use f-string to put parameters in indicator name

    @abstractmethod
    def _compute(self, bar: Events.Datafeed.Bar) -> float: ...

    def add_indicator(self, indicator: "_IndicatorBase") -> "_IndicatorBase":
        self._input_indicators[indicator.name] = indicator
        return indicator

    def update(self, bar: Events.Datafeed.Bar) -> None:
        for indicator in self._input_indicators.values():
            indicator.update(bar)
        history = self._history.setdefault(bar.symbol, deque(maxlen=self._max_history))
        history.append(self._compute(bar))

    def latest(self, symbol: str) -> float:
        return self[symbol, -1]

    def get_history(self, symbol: str) -> deque[float] | None:
        return self._history.get(symbol)

    def __getitem__(self, key: tuple[str, int]) -> float:  # `self.sma["ES", -1]`
        symbol, index = key
        try:
            return self._history[symbol][index]
        except (KeyError, IndexError):
            return float("nan")


class Open(_IndicatorBase):
    @property
    def name(self) -> str:
        return "Open"

    def _compute(self, bar: Events.Datafeed.Bar) -> float:
        return bar.open


class High(_IndicatorBase):
    @property
    def name(self) -> str:
        return "High"

    def _compute(self, bar: Events.Datafeed.Bar) -> float:
        return bar.high


class Low(_IndicatorBase):
    @property
    def name(self) -> str:
        return "Low"

    def _compute(self, bar: Events.Datafeed.Bar) -> float:
        return bar.low


class Close(_IndicatorBase):
    @property
    def name(self) -> str:
        return "Close"

    def _compute(self, bar: Events.Datafeed.Bar) -> float:
        return bar.close


class Volume(_IndicatorBase):
    @property
    def name(self) -> str:
        return "Volume"

    def _compute(self, bar: Events.Datafeed.Bar) -> float:
        return float(bar.volume) if bar.volume is not None else float("nan")


class SimpleMovingAverage(_IndicatorBase):
    def __init__(self, window_length: int, source: _IndicatorBase) -> None:
        super().__init__()
        self._window_length: int = window_length
        self._source_indicator: _IndicatorBase = self.add_indicator(source)

    @property
    def name(self) -> str:
        return f"SMA_{self._window_length}_{self._source_indicator.name}"

    def _compute(self, bar: Events.Datafeed.Bar) -> float:
        source_indicator_history: deque[float] | None = (
            self._source_indicator.get_history(bar.symbol)
        )
        if (
            source_indicator_history is None
            or len(source_indicator_history) < self._window_length
        ):
            return float("nan")
        return (
            sum(source_indicator_history[i] for i in range(-self._window_length, 0))
            / self._window_length
        )


class StrategyBase(_ComponentBase):
    SUBSCRIBE_TO: tuple[type[_EventBase], ...] = (
        Events.Datafeed.Bar,
        Events.Broker.BrokerConnected,
        Events.Broker.OrderAccepted,
        Events.Broker.OrderRejected,
        Events.Broker.CancellationAccepted,
        Events.Broker.CancellationRejected,
        Events.Broker.Fill,
        Events.Broker.OrderExpired,
    )

    SYMBOLS: frozenset[str] = frozenset()
    PERIOD_TYPE: PeriodType = PeriodType.SECOND

    def __init__(self, event_bus: _EventBus = _system_event_bus) -> None:
        super().__init__(event_bus)

        self._indicators: dict[str, _IndicatorBase] = {}
        self._current_bar: Events.Datafeed.Bar | None = None

        self._working_orders: dict[UUID, WorkingOrder] = {}
        self._open_positions: dict[str, OpenPosition] = {}

        # In-flight requests awaiting broker acknowledgement.
        self._submitted_orders: dict[UUID, Events.Strategy.SubmitOrder] = {}
        self._submitted_cancellations: dict[UUID, Events.Strategy.CancelOrder] = {}

        self.setup()
        self.emit(
            Events.Strategy.StreamRequest(
                period_type=self.PERIOD_TYPE, symbols=self.SYMBOLS
            )
        )

    @abstractmethod
    def setup(self) -> None: ...

    def add_indicator(self, indicator: _IndicatorBase) -> _IndicatorBase:
        self._indicators[indicator.name] = indicator
        return indicator  # for inline assignment: `self.sma = self.add_indicator(...)`

    @abstractmethod
    def on_bar(self, event: Events.Datafeed.Bar) -> None: ...

    def submit_order(
        self,
        trade_side: TradeSide,
        quantity: int,
        symbol: str | None = None,
        time_in_force: TimeInForce = TimeInForce.GTC,
        limit_price: int | None = None,
        stop_price: int | None = None,
    ) -> UUID:
        if self._current_bar is None:
            raise RuntimeError()
        order_id: UUID = uuid4()
        order_submission_event = Events.Strategy.SubmitOrder(
            symbol=symbol if symbol is not None else self._current_bar.symbol,
            order_id=order_id,
            trade_side=trade_side,
            quantity=quantity,
            time_in_force=time_in_force,
            limit_price=limit_price,
            stop_price=stop_price,
        )
        self._submitted_orders[order_id] = order_submission_event
        self.emit(order_submission_event)
        return order_id

    def submit_cancel(self, order_id: UUID) -> None:
        working_order = self._working_orders[order_id]
        order_cancellation_event = Events.Strategy.CancelOrder(
            symbol=working_order.symbol,
            order_id=working_order.order_id,
        )
        self._submitted_cancellations[working_order.order_id] = order_cancellation_event
        self.emit(order_cancellation_event)

    def _on_event(self, event: _EventBase) -> None:
        # fmt: off
        match event:
            case Events.Datafeed.Bar()                  as event:
                self._on_bar(event)

            case Events.Broker.BrokerConnected()        as event:
                self._working_orders = event.working_orders
                self._open_positions = event.open_positions

            case Events.Broker.OrderAccepted()          as event:
                self._on_order_accepted(event)

            case Events.Broker.OrderRejected()          as event:
                self._submitted_orders.pop(event.order_id)

            case Events.Broker.CancellationAccepted()   as event:
                self._submitted_cancellations.pop(event.order_id)
                self._working_orders.pop(event.order_id)

            case Events.Broker.CancellationRejected()   as event:
                self._submitted_cancellations.pop(event.order_id, None)

            case Events.Broker.Fill()                   as event:
                self._on_fill(event)

            case Events.Broker.OrderExpired()           as event:
                self._working_orders.pop(event.order_id)
                self._submitted_cancellations.pop(event.order_id, None)
        # fmt: on

    def _on_bar(self, event: Events.Datafeed.Bar) -> None:
        if event.symbol not in self.SYMBOLS or event.period_type != self.PERIOD_TYPE:
            return
        self._current_bar = event
        for indicator in self._indicators.values():
            indicator.update(event)
        self.on_bar(event)
        self.emit(
            Events.Strategy.IndicatorUpdate(
                symbol=event.symbol,
                source_event=event,
                ind_values={
                    name: indicator.latest(event.symbol)
                    for name, indicator in self._indicators.items()
                },
            )
        )

    def _on_order_accepted(self, event: Events.Broker.OrderAccepted) -> None:
        order = self._submitted_orders.pop(event.order_id)
        self._working_orders[event.order_id] = WorkingOrder(
            symbol=order.symbol,
            order_id=order.order_id,
            trade_side=order.trade_side,
            quantity=order.quantity,
            filled_qty=0,
            time_in_force=order.time_in_force,
            stop_price=order.stop_price,
            limit_price=order.limit_price,
        )

    def _on_fill(self, event: Events.Broker.Fill) -> None:
        order = self._working_orders[event.order_id]

        if order.quantity - order.filled_qty - event.filled_qty:  # partial fill
            self._working_orders[event.order_id] = replace(
                order, filled_qty=order.filled_qty + event.filled_qty
            )
        else:  # full fill
            self._working_orders.pop(event.order_id)
            self._submitted_cancellations.pop(event.order_id, None)

        # Update position tracking for symbol; fill event carries source of truth
        if event.signed_position_size == 0:
            self._open_positions.pop(event.symbol)
        else:
            self._open_positions[event.symbol] = OpenPosition(
                symbol=event.symbol,
                signed_qty=event.signed_position_size,
                cost_basis=event.position_cost_basis,
            )


class BrokerConnectorBase(_ComponentBase, _Connectable):
    SUBSCRIBE_TO: tuple[type[_EventBase], ...] = (
        Events.Strategy.SubmitOrder,
        Events.Strategy.CancelOrder,
    )

    def __init__(self, event_bus: _EventBus = _system_event_bus) -> None:
        super().__init__(event_bus)
        self._connect()
        working_orders, open_positions = self._exposure_snapshot()
        self.emit(
            Events.Broker.BrokerConnected(
                working_orders=working_orders,
                open_positions=open_positions,
            )
        )

    @abstractmethod
    def _exposure_snapshot(
        self,
    ) -> tuple[dict[UUID, WorkingOrder], dict[str, OpenPosition]]: ...

    def _on_event(self, event: _EventBase) -> None:
        match event:
            case Events.Strategy.SubmitOrder() as event:
                self._on_submit_order(event)
            case Events.Strategy.CancelOrder() as event:
                self._on_cancel_order(event)

    @abstractmethod
    def _on_submit_order(self, event: Events.Strategy.SubmitOrder) -> None: ...

    @abstractmethod
    def _on_cancel_order(self, event: Events.Strategy.CancelOrder) -> None: ...


if __name__ == "__main__":
    recorder = JSONLRecorder()
    datafeed = CSVDatafeedConnector(csv_path=Path(""))

    dummy_strategy = ...
    dummy_broker = ...
