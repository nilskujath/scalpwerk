# Author:   Nils Kujath


# ——— Imports ——————————————————————————————————————————————————————————————————————————


# fmt: off
from abc            import ABC, abstractmethod
from collections    import defaultdict, deque
from copy           import deepcopy
from dataclasses    import dataclass, field, replace
from enum           import Enum, auto
from queue          import Queue
from threading      import Thread
from time           import time_ns
from typing         import Protocol, NamedTuple
from uuid           import UUID, uuid4
# fmt: on


# ——— Constants ————————————————————————————————————————————————————————————————————————


PRICE_SCALE_FACTOR = 1_000_000_000  # to convert prices to fixed-point integers


# ——— Enums ————————————————————————————————————————————————————————————————————————————


class PeriodType(Enum):
    # fmt: off
    SECOND      = auto()
    MINUTE      = auto()
    HOUR        = auto()
    DAY         = auto()
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
    BUY         = auto()
    SELL        = auto()
    # fmt: on


class TimeInForce(Enum):
    # fmt: off
    DAY         = auto()
    GTC         = auto()
    # fmt: on


# ——— Type Aliases —————————————————————————————————————————————————————————————————————


# fmt: off
type NsSinceUnixEpoch   = int
type ScaledPrice        = int
type Quantity           = int  # absolute, no indication of trade side/direction
type SignedQuantity     = int  # positive if long/buy, negative if short/sell

type Symbol             = str
type IndicatorName      = str

type OrderId            = UUID
type FillId             = UUID
# fmt: on


# ——— Data Structures ——————————————————————————————————————————————————————————————————


@dataclass(frozen=True, kw_only=True)
class WorkingOrder:
    # fmt: off
    symbol:         Symbol
    order_id:       OrderId
    order_type:     OrderType
    trade_side:     TradeSide
    qty:            Quantity  # total qty of order, not reduced by (partial) fills
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


class IndicatorValue(NamedTuple):  # `NamedTuple` for hot-path performance
    # fmt: off
    value:          float
    is_scaled:      bool
    # fmt: on


# Complex Type Aliases for Data Structures
# fmt: off
type WorkingOrders      = dict[OrderId, WorkingOrder]
type OpenPositions      = dict[Symbol, OpenPosition]
type IndicatorReadings  = dict[IndicatorName, IndicatorValue]
# fmt: on


# ——— Event Messages ———————————————————————————————————————————————————————————————————


@dataclass(frozen=True, kw_only=True)
class EventBase:  # outside events namespace so nested classes can access it
    timestamp: NsSinceUnixEpoch = field(default_factory=lambda: time_ns())


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
            period_start:   NsSinceUnixEpoch
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
            symbols:        frozenset[Symbol]  # `frozenset` enforces deep immutability
            # fmt: on

        @dataclass(frozen=True, kw_only=True)
        class IndicatorUpdate(EventBase):
            # fmt: off
            symbol:         Symbol
            source_event:   "Events.Datafeed.Bar"
            ind_readings:   IndicatorReadings
            # fmt: on

        @dataclass(frozen=True, kw_only=True)
        class SubmitOrder(EventBase):
            # fmt: off
            symbol:         Symbol
            order_id:       OrderId
            order_type:     OrderType
            trade_side:     TradeSide
            qty:            Quantity
            time_in_force:  TimeInForce
            limit_price:    ScaledPrice | None = None
            stop_price:     ScaledPrice | None = None
            # fmt: on

        # We do not modify orders; cancel and resubmit is the way. This significantly
        # reduces the surface area and complexity of our system.
        @dataclass(frozen=True, kw_only=True)
        class CancelOrder(EventBase):
            # fmt: off
            symbol:         Symbol
            order_id:       OrderId
            # fmt: on

    class Broker:
        @dataclass(frozen=True, kw_only=True)
        class BrokerConnected(EventBase):  # carries current exposure; broker is SSOT
            # fmt: off
            working_orders: WorkingOrders
            open_positions: OpenPositions
            # fmt: on

        @dataclass(frozen=True, kw_only=True)
        class OrderAccepted(EventBase):
            # fmt: off
            symbol:         Symbol
            order_id:       OrderId
            # fmt: on

        @dataclass(frozen=True, kw_only=True)
        class OrderRejected(EventBase):
            # fmt: off
            symbol:         Symbol
            order_id:       OrderId
            reason:         str
            # fmt: on

        @dataclass(frozen=True, kw_only=True)
        class CancellationAccepted(EventBase):
            # fmt: off
            symbol:         Symbol
            order_id:       OrderId
            # fmt: on

        @dataclass(frozen=True, kw_only=True)
        class CancellationRejected(EventBase):
            # fmt: off
            symbol:         Symbol
            order_id:       OrderId
            reason:         str
            # fmt: on

        @dataclass(frozen=True, kw_only=True)
        class Fill(EventBase):
            # fmt: off
            symbol:         Symbol
            fill_id:        FillId
            order_id:       OrderId
            trade_side:     TradeSide
            filled_qty:     Quantity
            fill_price:     ScaledPrice

            # Position state after this fill; broker is SSOT. This eliminates the need
            # for computing position sizes and cost bases internally, and avoids having
            # to track instrument-specific commissions and fees for each trading action.
            signed_position_size:   SignedQuantity
            position_cost_basis:    ScaledPrice | None = None  # None when flat
            # fmt: on

        @dataclass(frozen=True, kw_only=True)
        class OrderExpired(EventBase):
            # fmt: off
            symbol:         Symbol
            order_id:       OrderId
            # fmt: on


# ——— Event Bus ————————————————————————————————————————————————————————————————————————


class _ComponentLike(Protocol):  # protocol to avoid circular dependencies
    def receive(self, event: EventBase) -> None: ...

    @property
    def is_idle(self) -> bool: ...  # check if component is currently idle

    def wait_until_idle(self) -> None: ...  # wait until the component is idle


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
        # We need to loop until all components are idle at the same time; this is
        # because draining one component may enqueue events in a previously drained one.
        while True:
            all_components = set().union(*self._subs.values())
            if exclude is not None:
                all_components.discard(exclude)
            for component in all_components:
                component.wait_until_idle()
            if all(component.is_idle for component in all_components):
                break


_system_event_bus = EventBus()  # global so standard use case is simplified


# ——— Component Base ———————————————————————————————————————————————————————————————————


class _Connectable(ABC):  # mixin interface for components with connection lifecycles
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
            self._on_event(event)
            self._queue.task_done()
            if isinstance(event, Events.System.Shutdown):
                break
        if isinstance(self, _Connectable):
            self._disconnect()

    @abstractmethod
    def _on_event(self, event: EventBase) -> None: ...

    # System Pacing Utilities
    @property
    def is_idle(self) -> bool:  # non-blocking; `True` if queue is empty
        return self._queue.unfinished_tasks == 0

    def wait_until_idle(self) -> None:  # blocking; blocks until queue is drained
        self._queue.join()

    def wait_until_shutdown(self) -> None:  # blocks until this component is done
        self._thread.join()

    def _wait_until_system_idle(self) -> None:  # block until all other components idle
        self._event_bus.wait_until_system_idle(exclude=self)


# ——— Recorder Base ————————————————————————————————————————————————————————————————————


class RecorderBase(_ComponentBase, ABC):
    SUBSCRIBE_TO: tuple[type[EventBase], ...] = tuple(EventBase.__subclasses__())


# ——— Datafeed Connector Base ——————————————————————————————————————————————————————————


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


# ——— Indicator Base ———————————————————————————————————————————————————————————————————


class IndicatorBase(ABC):
    IS_SCALED: bool = True  # `True` if output is `ScaledPrice` (e.g., `False` for RSI)

    def __init__(self, max_history: int = 100) -> None:
        self._max_history = max(1, int(max_history))
        self._history: dict[Symbol, deque[float]] = {}
        self._input_indicators: dict[IndicatorName, "IndicatorBase"] = {}

    @property
    @abstractmethod
    def name(self) -> IndicatorName: ...  # use f-string to put parameter values in name

    @abstractmethod
    def _compute(self, bar: Events.Datafeed.Bar) -> float: ...

    def add_indicator(self, indicator: "IndicatorBase") -> "IndicatorBase":
        indicator = deepcopy(indicator)  # so shared sources don't multi-update per bar
        if indicator.name in self._input_indicators:  # avoid silent shadowing
            raise ValueError(f"duplicate input indicator {indicator.name!r}")
        self._input_indicators[indicator.name] = indicator
        return indicator

    def initialize_symbols(self, symbols: frozenset[Symbol]) -> None:
        # Seed deques so `__getitem__` `IndexError` -> warmup (NaN), `KeyError` -> bug.
        for indicator in self._input_indicators.values():
            indicator.initialize_symbols(symbols)
        for symbol in symbols:
            self._history.setdefault(symbol, deque(maxlen=self._max_history))

    def update(self, bar: Events.Datafeed.Bar) -> None:
        for indicator in self._input_indicators.values():
            indicator.update(bar)
        # `initialize_symbols` must have been called via `StrategyBase` before this
        self._history[bar.symbol].append(self._compute(bar))

    def __getitem__(self, key: tuple[Symbol, int]) -> float:  # `self.sma["ES", -1]`
        symbol, index = key
        try:
            return self._history[symbol][index]
        except IndexError:
            return float("nan")  # warmup; not enough bars yet

    def latest(self, symbol: Symbol) -> float:
        return self[symbol, -1]


# ——— Strategy Base ————————————————————————————————————————————————————————————————————


class _ExposureTracker:
    def __init__(self) -> None:
        self.working_orders: WorkingOrders = {}
        self.open_positions: OpenPositions = {}

        # In-flight requests awaiting broker acknowledgement.
        self.submitted_orders: dict[OrderId, Events.Strategy.SubmitOrder] = {}
        self.submitted_cancellations: dict[OrderId, Events.Strategy.CancelOrder] = {}

        self.current_symbol: Symbol | None = None

    def track_order_submission(self, event: Events.Strategy.SubmitOrder) -> None:
        self.submitted_orders[event.order_id] = event

    def track_cancellation_submission(self, event: Events.Strategy.CancelOrder) -> None:
        self.submitted_cancellations[event.order_id] = event

    @property
    def position(self) -> SignedQuantity:
        if self.current_symbol is None:
            raise RuntimeError("no active symbol")
        position = self.open_positions.get(self.current_symbol)
        return position.signed_qty if position else 0

    @property
    def cost_basis(self) -> ScaledPrice | None:
        if self.current_symbol is None:
            raise RuntimeError("no active symbol")
        position = self.open_positions.get(self.current_symbol)
        return position.cost_basis if position else None

    @property
    def no_working_orders(self) -> bool:
        if self.current_symbol is None:
            raise RuntimeError("no active symbol")
        return not any(
            order.symbol == self.current_symbol
            for order in self.working_orders.values()
        )

    def on_event(self, event: EventBase) -> None:
        match event:
            case Events.Broker.BrokerConnected() as event:
                self.working_orders = event.working_orders
                self.open_positions = event.open_positions

            case Events.Broker.OrderAccepted() as event:
                order = self.submitted_orders.pop(event.order_id)
                self.working_orders[event.order_id] = WorkingOrder(
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

            case Events.Broker.OrderRejected() as event:
                self.submitted_orders.pop(event.order_id)

            case Events.Broker.CancellationAccepted() as event:
                self.submitted_cancellations.pop(event.order_id)
                self.working_orders.pop(event.order_id)

            case Events.Broker.CancellationRejected() as event:
                # Pop with `None` since order might have been filled in the meantime.
                self.submitted_cancellations.pop(event.order_id, None)

            case Events.Broker.Fill() as event:
                working_order = self.working_orders[event.order_id]
                if working_order.qty - working_order.filled_qty - event.filled_qty:
                    self.working_orders[event.order_id] = replace(
                        working_order,
                        filled_qty=working_order.filled_qty + event.filled_qty,
                    )
                else:
                    self.working_orders.pop(event.order_id)
                    self.submitted_cancellations.pop(event.order_id, None)

                # Update position tracking for symbol; fill event carries SOT
                if event.signed_position_size == 0:
                    self.open_positions.pop(event.symbol)
                else:
                    assert event.position_cost_basis is not None
                    self.open_positions[event.symbol] = OpenPosition(
                        symbol=event.symbol,
                        signed_qty=event.signed_position_size,
                        cost_basis=event.position_cost_basis,
                    )

            case Events.Broker.OrderExpired() as event:
                self.working_orders.pop(event.order_id)
                # Cancellation might have been in-flight right before order expired
                self.submitted_cancellations.pop(event.order_id, None)


class _IndicatorManager:
    def __init__(self) -> None:
        self._registry: dict[IndicatorName, IndicatorBase] = {}

    def register(self, ind: IndicatorBase, symbols: frozenset[Symbol]) -> IndicatorBase:
        ind = deepcopy(ind)
        if ind.name in self._registry:
            raise ValueError(f"duplicate indicator {ind.name!r}")
        ind.initialize_symbols(symbols)
        self._registry[ind.name] = ind
        return ind

    def update(self, bar: Events.Datafeed.Bar) -> None:
        for indicator in self._registry.values():
            indicator.update(bar)

    def get_readings_for(self, symbol: Symbol) -> IndicatorReadings:
        return {
            name: IndicatorValue(value=ind.latest(symbol), is_scaled=ind.IS_SCALED)
            for name, ind in self._registry.items()
        }


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
        self.exposure = _ExposureTracker()
        self._indicators = _IndicatorManager()
        self.emit(
            Events.Strategy.StreamRequest(
                period_type=self.PERIOD_TYPE, symbols=self.SYMBOLS
            )
        )

    def add_indicator(self, indicator: IndicatorBase) -> IndicatorBase:
        try:
            return self._indicators.register(indicator, self.SYMBOLS)
        except ValueError:
            self.emit(Events.System.Shutdown())
            raise

    @abstractmethod
    def on_bar(self, event: Events.Datafeed.Bar) -> None: ...

    # fmt: off
    def submit_order(
        self,
        order_type:     OrderType,
        trade_side:     TradeSide,
        qty:            Quantity,
        symbol:         Symbol      | None  = None,
        time_in_force:  TimeInForce         = TimeInForce.GTC,
        limit_price:    ScaledPrice | None  = None,
        stop_price:     ScaledPrice | None  = None,
    ) -> OrderId:

        if self.exposure.current_symbol is None:
            raise RuntimeError()

        order_id: OrderId = uuid4()
        event = Events.Strategy.SubmitOrder(
            symbol          =symbol or self.exposure.current_symbol,
            order_id        =order_id,
            order_type      =order_type,
            trade_side      =trade_side,
            qty             =qty,
            time_in_force   =time_in_force,
            limit_price     =limit_price,
            stop_price      =stop_price,
        )
        self.exposure.track_order_submission(event)
        self.emit(event)
        
        return order_id

    def submit_cancel(self, order_id: OrderId) -> None:
        order = self.exposure.working_orders[order_id]
        event = Events.Strategy.CancelOrder(
            symbol  =order.symbol,
            order_id=order.order_id,
        )
        self.exposure.track_cancellation_submission(event)
        self.emit(event)
    # fmt: on

    def _on_event(self, event: EventBase) -> None:
        match event:
            case Events.Datafeed.Bar() as event:
                self._process_bar(event)

            case _:
                self.exposure.on_event(event)

    def _process_bar(self, bar: Events.Datafeed.Bar) -> None:
        if bar.symbol not in self.SYMBOLS or bar.period_type != self.PERIOD_TYPE:
            return

        self.exposure.current_symbol = bar.symbol
        self._indicators.update(bar)
        self.emit(
            Events.Strategy.IndicatorUpdate(
                symbol=bar.symbol,
                source_event=bar,
                ind_readings=self._indicators.get_readings_for(bar.symbol),
            )
        )

        self.on_bar(bar)


# ——— Broker Connector Base ————————————————————————————————————————————————————————————


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

    @abstractmethod
    def _exposure_snapshot(self) -> tuple[WorkingOrders, OpenPositions]: ...

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
