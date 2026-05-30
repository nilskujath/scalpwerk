from abc import ABC, abstractmethod
from collections import defaultdict, deque
from copy import deepcopy
from dataclasses import dataclass, field, replace
from enum import Enum, auto
from queue import Queue
from threading import Thread
from time import time_ns
from typing import Protocol, NamedTuple
from uuid import UUID, uuid4

PRICE_SCALE_FACTOR = 1_000_000_000  # compatible with Databento's conventions

type NanosecondsSinceUnixEpoch = int
type Nanoseconds = int
type ScaledPrice = int  # actual price multiplied by `PRICE_SCALE_FACTOR`
type Symbol = str
type Quantity = int
type SignedQuantity = int  # positive if long, negative if short
type IndicatorName = str
type PlotGroup = int | None  # 0 = on price, -n above, +n below, `None` = don't plot
type Reason = str


class PeriodType(Enum):  # values for easy compatibility with Databento's conventions
    # fmt: off
    SECOND  = 32
    MINUTE  = 33
    HOUR    = 34
    DAY     = 35
    # fmt: on

    @property
    def duration_in_nanoseconds(self) -> Nanoseconds:
        match self:
            case PeriodType.SECOND:
                return 1_000_000_000
            case PeriodType.MINUTE:
                return 60_000_000_000
            case PeriodType.HOUR:
                return 3_600_000_000_000
            case PeriodType.DAY:
                return 86_400_000_000_000

    @property
    def duration_in_days(self) -> float:  # useful for plotting with mpl's date system
        return self.duration_in_nanoseconds / 86_400_000_000_000


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
    signed_qty:     SignedQuantity
    cost_basis:     ScaledPrice
    # fmt: on


class IndicatorReading(NamedTuple):
    # fmt: off
    value:      float
    is_scaled:  bool
    plot_group: PlotGroup
    # fmt: on


@dataclass(frozen=True, kw_only=True)
class EventBase:
    timestamp: NanosecondsSinceUnixEpoch = field(default_factory=lambda: time_ns())


class Events:
    class System:
        @dataclass(frozen=True, kw_only=True)
        class Shutdown(EventBase):
            pass

    class Datafeed:
        @dataclass(frozen=True, kw_only=True)
        class Bar(EventBase):
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
        class StreamRequest(EventBase):
            # fmt: off
            period_type:    PeriodType
            symbols:        frozenset[Symbol]
            # fmt: on

        @dataclass(frozen=True, kw_only=True)
        class IndicatorUpdate(EventBase):
            # fmt: off
            symbol:         Symbol
            source_event:   "Events.Datafeed.Bar"
            ind_values:     dict[IndicatorName, IndicatorReading]
            # fmt: on

        @dataclass(frozen=True, kw_only=True)
        class SubmitOrder(EventBase):
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
        class CancelOrder(EventBase):
            # fmt: off
            symbol:         Symbol
            order_id:       UUID
            # fmt: on

    class Broker:
        @dataclass(frozen=True, kw_only=True)
        class BrokerConnected(EventBase):
            # fmt: off
            working_orders: dict[UUID, WorkingOrder]
            open_positions: dict[Symbol, OpenPosition]
            # fmt: on

        @dataclass(frozen=True, kw_only=True)
        class OrderAccepted(EventBase):
            # fmt: off
            symbol:         Symbol
            order_id:       UUID
            # fmt: on

        @dataclass(frozen=True, kw_only=True)
        class OrderRejected(EventBase):
            # fmt: off
            symbol:         Symbol
            order_id:       UUID
            reason:         Reason
            # fmt: on

        @dataclass(frozen=True, kw_only=True)
        class CancellationAccepted(EventBase):
            # fmt: off
            symbol:         Symbol
            order_id:       UUID
            # fmt: on

        @dataclass(frozen=True, kw_only=True)
        class CancellationRejected(EventBase):
            # fmt: off
            symbol:         Symbol
            order_id:       UUID
            reason:         Reason
            # fmt: on

        @dataclass(frozen=True, kw_only=True)
        class Fill(EventBase):
            # fmt: off
            symbol:         Symbol
            fill_id:        UUID
            order_id:       UUID
            trade_side:     TradeSide
            filled_qty:     Quantity
            fill_price:     ScaledPrice

            # position state after this fill; broker is single source of truth
            signed_position_size:   SignedQuantity
            position_cost_basis:    ScaledPrice | None = None  # None when flat
            # fmt: on

        @dataclass(frozen=True, kw_only=True)
        class OrderExpired(EventBase):
            # fmt: off
            symbol:         Symbol
            order_id:       UUID
            # fmt: on


class _ComponentLike(Protocol):  # protocol to avoid circular dependencies
    def receive(self, event: EventBase) -> None: ...
    @property
    def is_idle(self) -> bool: ...
    def wait_until_idle(self) -> None: ...


class EventBus:
    def __init__(self) -> None:
        self._subs: dict[type[EventBase], set[_ComponentLike]] = defaultdict(set)

    def subscribe(self, component: _ComponentLike, *event_types: type[EventBase]):
        for event_type in event_types:
            self._subs[event_type].add(component)

    def publish(self, event: EventBase):
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


_system_event_bus = EventBus()  # global so standard use case is simplified


class _Connectable(ABC):
    @abstractmethod
    def _connect(self) -> None: ...

    @abstractmethod
    def _disconnect(self) -> None: ...


class _ComponentBase(_ComponentLike, ABC):
    SUBSCRIBE_TO: tuple[type[EventBase], ...] = ()

    def __init__(self, event_bus: EventBus = _system_event_bus) -> None:
        self._event_bus: EventBus = event_bus
        self._event_bus.subscribe(self, *self.SUBSCRIBE_TO, Events.System.Shutdown)
        self._queue: Queue[EventBase] = Queue()
        self._thread: Thread = Thread(target=self._event_loop, name=type(self).__name__)
        self._thread.start()

    def receive(self, event: EventBase) -> None:
        self._queue.put(event)

    def emit(self, event: EventBase) -> None:
        self._event_bus.publish(event)

    def _event_loop(self) -> None:
        while True:
            event = self._queue.get()
            if isinstance(event, Events.System.Shutdown):
                self._on_event(event)
                self._queue.task_done()
                break
            self._on_event(event)
            self._queue.task_done()
        if isinstance(self, _Connectable):
            self._disconnect()

    @abstractmethod
    def _on_event(self, event: EventBase) -> None: ...

    # System pacing utilities

    @property
    def is_idle(self) -> bool:  # non-blocking; `True` if queue is empty
        return self._queue.unfinished_tasks == 0

    def wait_until_idle(self) -> None:  # blocking; blocks until queue is drained
        self._queue.join()

    def wait_until_shutdown(self) -> None:  # blocks until this component is done
        self._thread.join()

    def _wait_until_system_idle(self) -> None:  # block until all other components idle
        self._event_bus.wait_until_system_idle(exclude=self)


class RecorderBase(_ComponentBase, ABC):
    SUBSCRIBE_TO: tuple[type[EventBase], ...] = tuple(EventBase.__subclasses__())


class DatafeedConnectorBase(_ComponentBase, _Connectable, ABC):
    SUBSCRIBE_TO: tuple[type[EventBase], ...] = (
        Events.Strategy.StreamRequest,
        Events.Broker.BrokerConnected,
    )

    @abstractmethod
    def _subscribe(
        self, period_type: PeriodType, symbols: frozenset[Symbol]
    ) -> None: ...

    def _on_event(self, event: EventBase) -> None:
        match event:
            case Events.Strategy.StreamRequest() as event:
                self._subscribe(period_type=event.period_type, symbols=event.symbols)
            case Events.Broker.BrokerConnected():
                self._connect()  # connect datafeed only after broker is connected (!)


class IndicatorBase(ABC):
    IS_OUTPUT_SCALED: bool = True

    def __init__(self, max_history: int = 100) -> None:
        self._max_history = max(1, int(max_history))
        self._history: dict[Symbol, deque[float]] = {}
        self._input_indicators: dict[IndicatorName, "IndicatorBase"] = {}

    @property
    @abstractmethod
    def name(
        self,
    ) -> IndicatorName: ...  # use f-string to put parameter values in indicator name

    @abstractmethod
    def _compute(self, bar: Events.Datafeed.Bar) -> float: ...

    def add_indicator(self, indicator: "IndicatorBase") -> "IndicatorBase":
        indicator = deepcopy(indicator)  # isolate from shared references
        if indicator.name in self._input_indicators:
            raise ValueError(f"duplicate input indicator {indicator.name!r}")
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
    SUBSCRIBE_TO: tuple[type[EventBase], ...] = (
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

    def __init__(self, event_bus: EventBus = _system_event_bus) -> None:
        super().__init__(event_bus)

        self._indicators: dict[IndicatorName, IndicatorBase] = {}
        self._indicator_plot_groups: dict[IndicatorName, PlotGroup] = {}
        self._current_bar: Events.Datafeed.Bar | None = None

        self._working_orders: dict[UUID, WorkingOrder] = {}
        self._open_positions: dict[Symbol, OpenPosition] = {}

        # In-flight requests awaiting broker acknowledgement.
        self._submitted_orders: dict[UUID, Events.Strategy.SubmitOrder] = {}
        self._submitted_cancellations: dict[UUID, Events.Strategy.CancelOrder] = {}

        self.emit(
            Events.Strategy.StreamRequest(
                period_type=self.PERIOD_TYPE, symbols=self.SYMBOLS
            )
        )

    def add_indicator(
        self,
        indicator: IndicatorBase,
        *,
        plot_group: PlotGroup = 0,
    ) -> IndicatorBase:
        indicator = deepcopy(indicator)  # isolate from shared references
        if indicator.name in self._indicators:
            self.emit(Events.System.Shutdown())
            raise ValueError(f"duplicate indicator {indicator.name!r}")
        self._indicators[indicator.name] = indicator
        self._indicator_plot_groups[indicator.name] = plot_group
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

    def _on_event(self, event: EventBase) -> None:
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
                    name: IndicatorReading(
                        value=indicator.latest(event.symbol),
                        is_scaled=indicator.IS_OUTPUT_SCALED,
                        plot_group=self._indicator_plot_groups[name],
                    )
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
    SUBSCRIBE_TO: tuple[type[EventBase], ...] = (
        Events.Strategy.SubmitOrder,
        Events.Strategy.CancelOrder,
    )

    def __init__(self, event_bus: EventBus = _system_event_bus) -> None:
        super().__init__(event_bus)
        self._connect()
        working_orders, open_positions = self._exposure_snapshot()
        self.emit(
            Events.Broker.BrokerConnected(
                working_orders=working_orders,
                open_positions=open_positions,
            )
        )

    # Reflects that we don't know fills if we have an existing position at broker
    @abstractmethod
    def _exposure_snapshot(
        self,
    ) -> tuple[dict[UUID, WorkingOrder], dict[Symbol, OpenPosition]]: ...

    def _on_event(self, event: EventBase) -> None:
        match event:
            case Events.Strategy.SubmitOrder() as event:
                self._on_submit_order(event)
            case Events.Strategy.CancelOrder() as event:
                self._on_cancel_order(event)

    @abstractmethod
    def _on_submit_order(self, event: Events.Strategy.SubmitOrder) -> None: ...

    @abstractmethod
    def _on_cancel_order(self, event: Events.Strategy.CancelOrder) -> None: ...
