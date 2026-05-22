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
from time import time_ns, strftime, gmtime
from typing import Protocol
from uuid import UUID, uuid4

PRICE_SCALE_FACTOR = 1_000_000_000

type NanosecondsSinceUnixEpoch = int
type ScaledPrice = int
type Symbol = str
type Quantity = int
type SignedQuantity = int  # positive if long, negative if short


class PeriodType(Enum):  # values for easy compatibility with Databento schema
    # fmt: off
    SECOND  = 32
    MINUTE  = 33
    HOUR    = 34
    DAY     = 35
    # fmt: on


class OrderType(Enum):
    # fmt: off
    MARKET      = auto()
    STOP        = auto()
    STOP_LIMIT  = auto()
    LIMIT       = auto()
    # fmt: on


class TradeSide(Enum):
    # fmt: off
    BUY     = auto()
    SELL    = auto()
    # fmt: on


class PositionDirection(Enum):
    # fmt: off
    LONG    = auto()
    SHORT   = auto()
    # fmt: on


class TimeInForce(Enum):
    # fmt: off
    DAY     = auto()
    GTC     = auto()
    # fmt: on


@dataclass(frozen=True, kw_only=True)
class WorkingOrder:
    # fmt: off
    symbol:         Symbol
    order_id:       UUID
    order_type:     OrderType
    trade_side:     TradeSide
    qty:            Quantity
    filled_qty:     Quantity
    time_in_force:  TimeInForce
    limit_price:    ScaledPrice | None = None
    stop_price:     ScaledPrice | None = None
    # fmt: on


@dataclass(frozen=True, kw_only=True)
class OpenPosition:
    # fmt: off
    symbol:         Symbol
    signed_qty:     Quantity
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
            symbol:         Symbol
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
            symbols:        frozenset[Symbol]
            # fmt: on

        @dataclass(frozen=True, kw_only=True)
        class IndicatorUpdate(_EventBase):
            # fmt: off
            symbol:         Symbol
            source_event:   "Events.Datafeed.Bar"
            ind_values:     dict[str, float]
            # fmt: on

        @dataclass(frozen=True, kw_only=True)
        class SubmitOrder(_EventBase):
            # fmt: off
            symbol:         Symbol
            order_id:       UUID
            order_type:     OrderType
            trade_side:     TradeSide
            qty:            Quantity
            time_in_force:  TimeInForce
            limit_price:    ScaledPrice | None = None
            stop_price:     ScaledPrice | None = None
            # fmt: on

        # We do not modify orders, cancel and resubmit is the way. This significantly
        # reduces the surface area of our system and reduces complexity.
        @dataclass(frozen=True, kw_only=True)
        class CancelOrder(_EventBase):
            # fmt: off
            symbol:         Symbol
            order_id:       UUID
            # fmt: on

    class Broker:
        @dataclass(frozen=True, kw_only=True)
        class BrokerConnected(_EventBase):
            # fmt: off
            working_orders: dict[UUID, WorkingOrder]
            open_positions: dict[Symbol, OpenPosition]
            # fmt: on

        @dataclass(frozen=True, kw_only=True)
        class OrderAccepted(_EventBase):
            # fmt: off
            symbol:         Symbol
            order_id:       UUID
            # fmt: on

        @dataclass(frozen=True, kw_only=True)
        class OrderRejected(_EventBase):
            # fmt: off
            symbol:         Symbol
            order_id:       UUID
            reason:         str
            # fmt: on

        @dataclass(frozen=True, kw_only=True)
        class CancellationAccepted(_EventBase):
            # fmt: off
            symbol:         Symbol
            order_id:       UUID
            # fmt: on

        @dataclass(frozen=True, kw_only=True)
        class CancellationRejected(_EventBase):
            # fmt: off
            symbol:         Symbol
            order_id:       UUID
            reason:         str
            # fmt: on

        @dataclass(frozen=True, kw_only=True)
        class Fill(_EventBase):
            # fmt: off
            # fill information
            symbol:         Symbol
            fill_id:        UUID
            order_id:       UUID
            trade_side:     TradeSide
            filled_qty:     Quantity
            fill_price:     ScaledPrice

            # position state after this fill
            signed_position_size:   int
            position_cost_basis:    ScaledPrice | None = None  # if fill flattens pos
            # fmt: on

        @dataclass(frozen=True, kw_only=True)
        class OrderExpired(_EventBase):
            # fmt: off
            symbol:         Symbol
            order_id:       UUID
            # fmt: on


class _ComponentLike(Protocol):
    def receive(self, event: _EventBase) -> None: ...
    @property
    def is_idle(self) -> bool: ...
    def wait_until_idle(self) -> None: ...
    def wait_until_shutdown(self) -> None: ...


class _EventBus:
    def __init__(self) -> None:
        self._subs: dict[type[_EventBase], set[_ComponentLike]] = defaultdict(set)

    def subscribe(self, component: _ComponentLike, *event_types: type[_EventBase]):
        for event_type in event_types:
            self._subs[event_type].add(component)

    def publish(self, event: _EventBase):
        for component in self._subs[type(event)]:
            component.receive(event)

    def wait_until_system_idle(self, exclude: _ComponentLike | None = None) -> None:
        while True:
            all_components = set().union(*self._subs.values())
            if exclude is not None:
                all_components.discard(exclude)
            for component in all_components:
                component.wait_until_idle()
            if all(component.is_idle for component in all_components):
                break


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

    @property
    def is_idle(self) -> bool:
        return self._queue.unfinished_tasks == 0

    def wait_until_idle(self) -> None:
        self._queue.join()

    def _wait_until_system_idle(self) -> None:
        self._event_bus.wait_until_system_idle(exclude=self)

    def wait_until_shutdown(self) -> None:
        self._thread.join()

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


class DatafeedConnectorBase(_ComponentBase, _Connectable, ABC):
    SUBSCRIBE_TO: tuple[type[_EventBase], ...] = (
        Events.Strategy.StreamRequest,
        Events.Broker.BrokerConnected,
    )

    @abstractmethod
    def _subscribe(
        self, period_type: PeriodType, symbols: frozenset[Symbol]
    ) -> None: ...

    def _on_event(self, event: _EventBase) -> None:
        match event:
            case Events.Strategy.StreamRequest() as event:
                self._subscribe(period_type=event.period_type, symbols=event.symbols)
            case Events.Broker.BrokerConnected():
                self._connect()  # connect datafeed only after broker is connected


# TODO It should return int (?)
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

    def latest(self, symbol: Symbol) -> float:
        return self[symbol, -1]

    def get_history(self, symbol: Symbol) -> deque[float] | None:
        return self._history.get(symbol)

    def __getitem__(self, key: tuple[Symbol, int]) -> float:  # `self.sma["ES", -1]`
        symbol, index = key
        try:
            return self._history[symbol][index]
        except (KeyError, IndexError):
            return float("nan")


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

    SYMBOLS: frozenset[Symbol] = frozenset()
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

        self.emit(
            Events.Strategy.StreamRequest(
                period_type=self.PERIOD_TYPE, symbols=self.SYMBOLS
            )
        )

    def add_indicator(self, indicator: _IndicatorBase) -> _IndicatorBase:
        self._indicators[indicator.name] = indicator
        return indicator  # for inline assignment: `self.sma = self.add_indicator(...)`

    @abstractmethod
    def on_bar(self, event: Events.Datafeed.Bar) -> None: ...

    @property
    def position(self) -> SignedQuantity:
        if self._current_bar is None:
            return 0
        position = self._open_positions.get(self._current_bar.symbol)
        return position.signed_qty if position else 0

    @property
    def cost_basis(self) -> ScaledPrice | None:
        if self._current_bar is None:
            return None
        position = self._open_positions.get(self._current_bar.symbol)
        return position.cost_basis if position else None

    @property
    def no_working_orders(self) -> bool:
        if self._current_bar is None:
            return True
        symbol = self._current_bar.symbol
        return not any(
            order.symbol == symbol for order in self._working_orders.values()
        )

    def submit_order(
        self,
        order_type: OrderType,
        trade_side: TradeSide,
        qty: Quantity,
        symbol: Symbol | None = None,
        time_in_force: TimeInForce = TimeInForce.GTC,
        limit_price: ScaledPrice | None = None,
        stop_price: ScaledPrice | None = None,
    ) -> UUID:
        if self._current_bar is None:
            raise RuntimeError()
        order_id: UUID = uuid4()
        order_submission_event = Events.Strategy.SubmitOrder(
            symbol=symbol if symbol is not None else self._current_bar.symbol,
            order_id=order_id,
            order_type=order_type,
            trade_side=trade_side,
            qty=qty,
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
            order_type=order.order_type,
            trade_side=order.trade_side,
            qty=order.qty,
            filled_qty=0,
            time_in_force=order.time_in_force,
            stop_price=order.stop_price,
            limit_price=order.limit_price,
        )

    def _on_fill(self, event: Events.Broker.Fill) -> None:
        order = self._working_orders[event.order_id]

        if order.qty - order.filled_qty - event.filled_qty:  # partial fill
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
            assert event.position_cost_basis is not None
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
    ) -> tuple[dict[UUID, WorkingOrder], dict[Symbol, OpenPosition]]: ...

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


# ——————————————————————————————————————————————————————————————————————————————————————
# CONCRETE IMPLEMENTATIONS
# ——————————————————————————————————————————————————————————————————————————————————————


class JSONLRecorder(RecorderBase):
    SUBSCRIBE_TO: tuple[type[_EventBase], ...] = tuple(
        cls for cls in _EventBase.__subclasses__() if cls is not _SystemShutdown
    )

    def __init__(self, jsonl_path: Path = Path("./runs")) -> None:
        self._run_id: str = f"{strftime('%Y%m%d_%H%M%S', gmtime())}_{uuid4().hex[:6]}"
        self._path: Path = jsonl_path / f"{self._run_id}.jsonl"
        self._jsonl_file: TextIOWrapper | None = None
        super().__init__()  # attributes must exist before starting thread

    @property
    def path(self) -> Path:
        return self._path

    def _event_loop(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
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


class CSVDatafeedConnector(DatafeedConnectorBase):
    def __init__(self, csv_path: Path) -> None:
        self._csv_path = csv_path
        self._symbols: frozenset[Symbol] = frozenset()
        super().__init__()

    def _subscribe(self, period_type: PeriodType, symbols: frozenset[Symbol]) -> None:
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
                self._wait_until_system_idle()
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


class SimulatedBrokerConnector(BrokerConnectorBase):
    SUBSCRIBE_TO: tuple[type[_EventBase], ...] = (
        Events.Strategy.SubmitOrder,
        Events.Strategy.CancelOrder,
        Events.Datafeed.Bar,  # also subscribe to bars for order matching
    )

    # fmt: off
    COMMISSION_PER_UNIT:        float = 0.0
    MIN_COMMISSION_PER_ORDER:   float = 0.0
    # fmt: on

    def __init__(self) -> None:
        self._working_orders: dict[UUID, WorkingOrder] = {}  # incl. market orders
        self._open_entries: dict[Symbol, deque[tuple[SignedQuantity, ScaledPrice]]] = {}
        super().__init__()

    def _connect(self) -> None:
        pass

    def _disconnect(self) -> None:
        pass

    def _exposure_snapshot(
        self,
    ) -> tuple[dict[UUID, WorkingOrder], dict[Symbol, OpenPosition]]:
        return self._working_orders.copy(), {
            symbol: OpenPosition(
                symbol=symbol,
                signed_qty=sum(qty for qty, _ in open_entries),
                cost_basis=(
                    sum(qty * price for qty, price in open_entries)
                    // sum(qty for qty, _ in open_entries)
                ),
            )
            for symbol, open_entries in self._open_entries.items()
        }

    def _on_event(self, event: _EventBase) -> None:
        if isinstance(event, Events.Datafeed.Bar):
            self._on_bar(event)
        else:
            super()._on_event(event)

    def _on_submit_order(self, event: Events.Strategy.SubmitOrder) -> None:
        self._working_orders[event.order_id] = WorkingOrder(
            symbol=event.symbol,
            order_id=event.order_id,
            order_type=event.order_type,
            trade_side=event.trade_side,
            qty=event.qty,
            filled_qty=0,
            time_in_force=event.time_in_force,
            limit_price=event.limit_price,
            stop_price=event.stop_price,
        )
        self.emit(
            Events.Broker.OrderAccepted(
                timestamp=event.timestamp,  # for charting purposes
                symbol=event.symbol,
                order_id=event.order_id,
            )
        )

    def _on_cancel_order(self, event: Events.Strategy.CancelOrder) -> None:
        if event.order_id in self._working_orders:
            del self._working_orders[event.order_id]
            self.emit(
                Events.Broker.CancellationAccepted(
                    timestamp=event.timestamp,
                    symbol=event.symbol,
                    order_id=event.order_id,
                )
            )

    def _on_bar(self, bar: Events.Datafeed.Bar) -> None:
        for order in list(self._working_orders.values()):
            if order.symbol != bar.symbol:
                continue

            # TODO Get rid of indentation via private methods
            match order.order_type:
                case OrderType.MARKET:
                    self._execute_order(order, bar.open, bar.period_start)

                case OrderType.STOP:
                    assert order.stop_price is not None
                    if (
                        order.trade_side is TradeSide.BUY
                        and bar.high >= order.stop_price
                    ):
                        self._execute_order(
                            order, max(order.stop_price, bar.open), bar.period_start
                        )
                    elif (
                        order.trade_side is TradeSide.SELL
                        and bar.low <= order.stop_price
                    ):
                        self._execute_order(
                            order, min(order.stop_price, bar.open), bar.period_start
                        )

                case OrderType.STOP_LIMIT:
                    assert (
                        order.stop_price is not None and order.limit_price is not None
                    )
                    if (
                        order.trade_side is TradeSide.BUY
                        and bar.high >= order.stop_price
                    ):
                        limit_order = replace(
                            order, order_type=OrderType.LIMIT, stop_price=None
                        )
                        self._working_orders[order.order_id] = limit_order
                        if bar.low <= order.limit_price:
                            self._execute_order(
                                limit_order,
                                min(order.limit_price, bar.open),
                                bar.period_start,
                            )
                    elif (
                        order.trade_side is TradeSide.SELL
                        and bar.low <= order.stop_price
                    ):
                        limit_order = replace(
                            order, order_type=OrderType.LIMIT, stop_price=None
                        )
                        self._working_orders[order.order_id] = limit_order
                        if bar.high >= order.limit_price:
                            self._execute_order(
                                limit_order,
                                max(order.limit_price, bar.open),
                                bar.period_start,
                            )

                case OrderType.LIMIT:
                    assert order.limit_price is not None
                    if (
                        order.trade_side is TradeSide.BUY
                        and bar.low <= order.limit_price
                    ):
                        self._execute_order(
                            order, min(order.limit_price, bar.open), bar.period_start
                        )
                    elif (
                        order.trade_side is TradeSide.SELL
                        and bar.high >= order.limit_price
                    ):
                        self._execute_order(
                            order, max(order.limit_price, bar.open), bar.period_start
                        )

    def _execute_order(
        self,
        order: WorkingOrder,
        fill_price: ScaledPrice,
        fill_timestamp: NanosecondsSinceUnixEpoch,
    ) -> None:

        # Remove working order
        del self._working_orders[order.order_id]

        # Reconcile open entry tracking with fill (FIFO method)
        signed_fill_qty: SignedQuantity = (
            order.qty if order.trade_side is TradeSide.BUY else -order.qty
        )
        open_entries = self._open_entries.get(order.symbol, deque())
        open_position_qty: SignedQuantity = sum(qty for qty, _ in open_entries)

        # CASE: no position -> create first entry
        if not open_entries:
            self._open_entries[order.symbol] = deque([(signed_fill_qty, fill_price)])

        # CASE: fill adds to position -> append new entry
        elif open_position_qty * signed_fill_qty > 0:
            open_entries.append((signed_fill_qty, fill_price))

        # CASE: fill exactly closes position -> flatten
        elif order.qty == abs(open_position_qty):
            self._open_entries.pop(order.symbol, None)

        # CASE: fill partially reduces position -> consume in FIFO order
        elif order.qty < abs(open_position_qty):
            qty_to_consume: Quantity = order.qty
            while qty_to_consume > 0:
                entry_qty, entry_price = open_entries[0]
                if qty_to_consume >= abs(entry_qty):
                    qty_to_consume -= abs(entry_qty)
                    self._open_entries[order.symbol].popleft()
                else:
                    self._open_entries[order.symbol][0] = (
                        (
                            entry_qty - qty_to_consume
                            if entry_qty > 0
                            else entry_qty + qty_to_consume
                        ),
                        entry_price,
                    )
                    qty_to_consume = 0

        # CASE: fill exceeds position -> flatten and open new position
        elif order.qty > abs(open_position_qty):
            self._open_entries.pop(order.symbol, None)
            remaining: Quantity = order.qty - abs(open_position_qty)
            remaining_signed: SignedQuantity = (
                remaining if signed_fill_qty > 0 else -remaining
            )
            self._open_entries[order.symbol] = deque([(remaining_signed, fill_price)])

        # Emit fill event
        updated_open_entries = self._open_entries.get(order.symbol, deque())

        self.emit(
            Events.Broker.Fill(
                timestamp=fill_timestamp,
                symbol=order.symbol,
                fill_id=uuid4(),
                order_id=order.order_id,
                trade_side=order.trade_side,
                filled_qty=order.qty,
                fill_price=fill_price,
                signed_position_size=sum(q for q, _ in updated_open_entries),
                position_cost_basis=(
                    sum(q * p for q, p in updated_open_entries)
                    // sum(q for q, _ in updated_open_entries)
                    if updated_open_entries
                    else None
                ),
            )
        )


# ——————————————————————————————————————————————————————————————————————————————————————
# WORKBENCH
# ——————————————————————————————————————————————————————————————————————————————————————


if __name__ == "__main__":

    class DummyStrategy(StrategyBase):
        SYMBOLS: frozenset[Symbol] = frozenset(["MNQM9"])
        PERIOD_TYPE: PeriodType = PeriodType.MINUTE

        def __init__(self) -> None:
            super().__init__()
            self.open: _IndicatorBase = self.add_indicator(Open())
            self.sma = self.add_indicator(SimpleMovingAverage(20, Close()))

        def on_bar(self, event: Events.Datafeed.Bar) -> None:
            if (
                self.open.latest(event.symbol) > self.sma.latest(event.symbol)
                and self.position == 0
                and self.no_working_orders
            ):
                self.submit_order(
                    order_type=OrderType.MARKET, trade_side=TradeSide.BUY, qty=1
                )
            if (
                self.open.latest(event.symbol) < self.sma.latest(event.symbol)
                and self.position == 1
                and self.no_working_orders
            ):
                self.submit_order(
                    order_type=OrderType.MARKET, trade_side=TradeSide.SELL, qty=1
                )

    class Backtest:
        # A fill within a round trip. May originate from a real broker fill,
        # a BrokerConnected snapshot (starting into an existing position),
        # or a split of a position-flipping fill (same fill closes one trade,
        # opens the next). fill_id and order_id are only present for real fills.
        @dataclass(frozen=True, kw_only=True)
        class RoundTripFill:
            # fmt: off
            timestamp:              NanosecondsSinceUnixEpoch
            symbol:                 Symbol
            trade_side:             TradeSide
            filled_qty:             Quantity
            fill_price:             ScaledPrice
            signed_position_size:   int
            position_cost_basis:    ScaledPrice | None = None
            fill_id:                UUID | None = None
            order_id:               UUID | None = None
            # fmt: on

        # TODO integrate duration_bar, mfe, mae -> then update the method to parse
        @dataclass(frozen=True, kw_only=True)
        class RoundTrip:
            fills: tuple["Backtest.RoundTripFill", ...]
            mfe: ScaledPrice = 0
            mae: ScaledPrice = 0
            duration_bars: int = 0

            @property
            def symbol(self) -> Symbol:
                return self.fills[0].symbol

            @property
            def direction(self) -> PositionDirection:
                if self.fills[0].trade_side is TradeSide.BUY:
                    return PositionDirection.LONG
                return PositionDirection.SHORT

        def __init__(
            self, strategy_class: type[StrategyBase], csv_path: Path, jsonl_path: Path
        ) -> None:
            self.strategy_class = strategy_class
            self.csv_path = csv_path
            self.jsonl_path = jsonl_path
            self._recorder: RecorderBase | None = None
            self._round_trip_trades: defaultdict[Symbol, list["Backtest.RoundTrip"]] = (
                defaultdict(list)
            )

        def run(self) -> None:
            self._recorder = JSONLRecorder(jsonl_path=self.jsonl_path)
            datafeed = CSVDatafeedConnector(csv_path=self.csv_path)
            strategy = self.strategy_class()
            broker = SimulatedBrokerConnector()

            broker.wait_until_shutdown()
            datafeed.wait_until_shutdown()
            strategy.wait_until_shutdown()
            self._recorder.wait_until_shutdown()

            self._round_trip_trades = self._parse_round_trip_trades(self._recorder.path)

        # TODO
        def journey(self):
            if self._recorder is None:
                raise RuntimeError()
            for symbol in self._round_trip_trades:
                self._render_trade_journey(symbol)

        # TODO
        def charts(self):
            pass

        def _render_trade_journey(self, symbol: Symbol) -> None:
            pass

        @staticmethod
        def _parse_round_trip_trades(
            jsonl_path: Path,
        ) -> defaultdict[Symbol, list[RoundTrip]]:
            round_trip_trades: defaultdict[Symbol, list["Backtest.RoundTrip"]] = (
                defaultdict(list)
            )
            open_trade: dict[Symbol, list["Backtest.RoundTripFill"]] = {}

            def seed_open_positions(data: dict) -> None:
                for sym, pos in data["open_positions"].items():
                    open_trade[sym] = [
                        Backtest.RoundTripFill(
                            timestamp=data["timestamp"],
                            symbol=sym,
                            trade_side=(
                                TradeSide.BUY
                                if pos["signed_qty"] > 0
                                else TradeSide.SELL
                            ),
                            filled_qty=abs(pos["signed_qty"]),
                            fill_price=pos["cost_basis"],
                            signed_position_size=pos["signed_qty"],
                            position_cost_basis=pos["cost_basis"],
                        )
                    ]

            def process_fill(data: dict) -> None:
                fill = Backtest.RoundTripFill(
                    timestamp=data["timestamp"],
                    symbol=data["symbol"],
                    trade_side=TradeSide[data["trade_side"].split(".")[-1]],
                    filled_qty=data["filled_qty"],
                    fill_price=data["fill_price"],
                    signed_position_size=data["signed_position_size"],
                    position_cost_basis=data["position_cost_basis"],
                    fill_id=UUID(data["fill_id"]),
                    order_id=UUID(data["order_id"]),
                )

                sym = fill.symbol
                prev_pos = (
                    open_trade[sym][-1].signed_position_size if sym in open_trade else 0
                )

                if prev_pos == 0 and fill.signed_position_size != 0:
                    open_trade[sym] = [fill]
                    return

                if fill.signed_position_size == 0:
                    open_trade[sym].append(fill)
                    round_trip_trades[sym].append(
                        Backtest.RoundTrip(fills=tuple(open_trade.pop(sym)))
                    )
                    return

                if prev_pos * fill.signed_position_size < 0:
                    open_trade[sym].append(
                        Backtest.RoundTripFill(
                            timestamp=fill.timestamp,
                            symbol=sym,
                            trade_side=fill.trade_side,
                            filled_qty=abs(prev_pos),
                            fill_price=fill.fill_price,
                            signed_position_size=0,
                            position_cost_basis=None,
                        )
                    )
                    round_trip_trades[sym].append(
                        Backtest.RoundTrip(fills=tuple(open_trade[sym]))
                    )
                    open_trade[sym] = [
                        Backtest.RoundTripFill(
                            timestamp=fill.timestamp,
                            symbol=sym,
                            trade_side=fill.trade_side,
                            filled_qty=abs(fill.signed_position_size),
                            fill_price=fill.fill_price,
                            signed_position_size=fill.signed_position_size,
                            position_cost_basis=fill.position_cost_basis,
                        )
                    ]
                    return

                open_trade[sym].append(fill)

            with open(jsonl_path) as f:
                for line in f:
                    record = json.loads(line)
                    event_type = record["event_type"]

                    if "BrokerConnected" in event_type:
                        seed_open_positions(record["data"])
                    elif "Fill" in event_type:
                        process_fill(record["data"])

            return round_trip_trades

    backtest = Backtest(
        strategy_class=DummyStrategy,
        jsonl_path=Path("./runs"),
        csv_path=Path("./mnq_minute.csv"),
    )
    backtest.run()
