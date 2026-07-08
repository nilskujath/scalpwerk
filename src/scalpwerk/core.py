# fmt: off
import pickle

from abc                import ABC, abstractmethod
from collections        import defaultdict, deque
from collections.abc    import Generator
from contextlib         import contextmanager
from copy               import deepcopy
from dataclasses        import dataclass, field, replace
from enum               import Enum
from io                 import BufferedWriter
from pathlib            import Path
from queue              import Queue
from threading          import Thread
from time               import time_ns
from traceback          import format_exc
from typing             import Protocol
from uuid               import UUID, uuid4
# fmt: on


# ——————————————————————————————————————————————————————————————————————————————————————
# System Infrastructure
# ——————————————————————————————————————————————————————————————————————————————————————


type DequeIndex = int
type MaxHistory = int
type NanosecondsSinceUnixEpoch = int
type PositionSize = int
type Quantity = int
type ScaledPrice = int
type IndicatorValue = float
type IndicatorName = str
type ShutdownReason = str
type Symbol = str


PeriodType = Enum("PeriodType", ["SECOND", "MINUTE", "HOUR", "DAY"])
TradeSide = Enum("TradeSide", ["BUY", "SELL"])


@dataclass(frozen=True, kw_only=True)
class EventMessageBase:
    timestamp: NanosecondsSinceUnixEpoch = field(default_factory=time_ns)


class DomainEvents:
    @dataclass(frozen=True, kw_only=True)
    class NewBar(EventMessageBase):
        # fmt: off
        symbol:         Symbol
        period_start:   NanosecondsSinceUnixEpoch
        period_type:    PeriodType
        open:           ScaledPrice
        high:           ScaledPrice
        low:            ScaledPrice
        close:          ScaledPrice
        volume:         int
        # fmt: on

    @dataclass(frozen=True, kw_only=True)
    class OrderRequest(EventMessageBase):
        # We only allow limit and stop limit orders; market orders and plain stop orders
        # must be implemented as their limit(ed) counterparts with marketable limit
        # prices. More sophisticated order types need to be approximated within the
        # strategy logic as cancel-and-resubmit processes.

        # fmt: off
        symbol:         Symbol
        order_id:       UUID
        trade_side:     TradeSide
        qty:            Quantity
        limit_price:    ScaledPrice
        stop_price:     ScaledPrice | None = None  # becomes stop-limit order if set
        # fmt: on

    @dataclass(frozen=True, kw_only=True)
    class CancellationRequest(EventMessageBase):
        # Note that we do not provide events for order modification; cancel-and-resubmit
        # is the way (this significantly reduces the surface area and complexity of our
        # system).

        # fmt: off
        symbol:         Symbol
        order_id:       UUID
        # fmt: on

    @dataclass(frozen=True, kw_only=True)
    class OrderSubmitted(EventMessageBase):
        # fmt: off
        symbol:         Symbol
        order_id:       UUID
        source_request: "DomainEvents.OrderRequest"

        # If `True`, this event was emitted by a broker connector component on
        # (re-)connect to communicate to the system the working orders that already
        # exist for the account (it does not represent an acceptance of an order request
        # submitted during this run).
        synthetic:      bool = False
        # fmt: on

    @dataclass(frozen=True, kw_only=True)
    class OrderRejected(EventMessageBase):
        # fmt: off
        symbol:         Symbol
        order_id:       UUID
        source_request: "DomainEvents.OrderRequest"
        reason:         str
        # fmt: on

    @dataclass(frozen=True, kw_only=True)
    class OrderCancelled(EventMessageBase):
        # fmt: off
        symbol:         Symbol
        order_id:       UUID
        # fmt: on

    @dataclass(frozen=True, kw_only=True)
    class CancellationRejected(EventMessageBase):
        # fmt: off
        symbol:         Symbol
        order_id:       UUID
        reason:         str
        # fmt: on

    @dataclass(frozen=True, kw_only=True)
    class Fill(EventMessageBase):
        # fmt: off
        symbol:         Symbol
        order_id:       UUID
        trade_side:     TradeSide
        filled_qty:     Quantity
        remaining_qty:  Quantity
        fill_price:     ScaledPrice

        # If `True`, this event was emitted by the broker on (re-)connect to communicate
        # to the system the position state (position size and cost basis for the fill's
        # symbol) that already exists for the account (it does not represent an actual
        # execution).
        synthetic:      bool = False

        # Position state after this fill; broker is single source of truth (SSOT).
        # This eliminates the need for computing position sizes and cost bases
        # internally, and avoids having to track instrument-specific commissions and
        # fees for each trading action.
        signed_position_size:   PositionSize
        position_cost_basis:    ScaledPrice | None = None  # `None` if position is flat
        # fmt: on

    @dataclass(frozen=True, kw_only=True)
    class OrderExpired(EventMessageBase):
        # fmt: off
        symbol:         Symbol
        order_id:       UUID
        # fmt: on


class SystemEvents:
    @dataclass(frozen=True, kw_only=True)
    class Shutdown(EventMessageBase):
        reason: ShutdownReason

    @dataclass(frozen=True, kw_only=True)
    class IndicatorUpdate(EventMessageBase):
        # This event message was designed to be emitted after updating a set of
        # indicators with the next new bar inside a strategy, which makes their readings
        # available to any interested component (e.g., charting).

        # fmt: off
        source_bar:     "DomainEvents.NewBar"
        readings:       dict[IndicatorName, IndicatorValue]
        # fmt: on

    @dataclass(frozen=True, kw_only=True)
    class RoundTripCompleted(EventMessageBase):
        # This event message was designed to capture all events from the first order
        # submission while flat to the fill that returns the position to flat.

        # fmt: off
        symbol:         Symbol
        readings:       tuple["SystemEvents.IndicatorUpdate", ...]  # incl. original bar
        submissions:    tuple[DomainEvents.OrderSubmitted, ...]  # incl. requests
        cancellations:  tuple[DomainEvents.OrderCancelled, ...]
        fills:          tuple[DomainEvents.Fill, ...]
        expiries:       tuple[DomainEvents.OrderExpired, ...]
        # fmt: on


class IndicatorBase(ABC):
    IS_SCALED: bool = True  # `True` if output is in the same scale as the price data

    def __init__(self, max_history: MaxHistory = 100) -> None:
        self._max_history = max(1, max_history)
        self._history: defaultdict[Symbol, deque[IndicatorValue]] = defaultdict(
            lambda: deque(maxlen=self._max_history)
        )
        self._input_indicators: dict[IndicatorName, "IndicatorBase"] = {}

    def __getitem__(self, key: tuple[Symbol, DequeIndex]) -> IndicatorValue:
        symbol, deque_index = key
        try:
            return self._history[symbol][deque_index]
        except IndexError:
            return float("nan")

    @property
    @abstractmethod
    def name(self) -> IndicatorName:
        # The name should uniquely identify the indicator and its configuration, e.g.,
        # `f"Indicator Name ({self.parameter_1}, {self.parameter_2})"`.
        ...

    def add_input(self, indicator: "IndicatorBase") -> "IndicatorBase":
        # Defensive deep copy so that, should the same indicator instance be used as
        # input to multiple parent indicators, it gets an independent copy per parent,
        # thus preventing double updates.
        indicator = deepcopy(indicator)
        # Duplicate names would shadow the previous input, leaving the caller with
        # a stale reference that silently stops receiving updates.
        if indicator.name in self._input_indicators:
            raise ValueError(f"duplicate input indicator {indicator.name!r}")
        self._input_indicators[indicator.name] = indicator
        return indicator

    def update(self, bar: DomainEvents.NewBar) -> None:
        for indicator in self._input_indicators.values():
            indicator.update(bar)
        self._history[bar.symbol].append(self._compute(bar))

    @abstractmethod
    def _compute(self, bar: DomainEvents.NewBar) -> IndicatorValue: ...


class SystemComponentLike(Protocol):  # defined to break a circular dependency
    def receive(self, event: EventMessageBase) -> None: ...

    def wait_until_idle(self) -> None: ...

    @property
    def is_idle(self) -> bool: ...


class EventBus:
    def __init__(self) -> None:
        self._per_eventtype_subscriptions: defaultdict[
            type[EventMessageBase], set[SystemComponentLike]
        ] = defaultdict(set)

    def subscribe(
        self, component: SystemComponentLike, *event_types: type[EventMessageBase]
    ) -> None:
        for event_type in event_types:
            self._per_eventtype_subscriptions[event_type].add(component)

    def publish(self, event: EventMessageBase) -> None:
        for component in self._per_eventtype_subscriptions[type(event)]:
            component.receive(event)


class SystemComponentBase(ABC):
    SUBSCRIBE_TO: tuple[type[EventMessageBase], ...] = ()  # override to receive events

    def __init__(self, event_bus: EventBus) -> None:
        self._event_bus: EventBus = event_bus
        self._queue: Queue[EventMessageBase] = Queue()
        self._thread = Thread(target=self._event_loop, name=type(self).__name__)
        self._event_bus.subscribe(self, *self.SUBSCRIBE_TO, SystemEvents.Shutdown)
        self._thread.start()

    @property
    def is_idle(self) -> bool:
        return self._queue.unfinished_tasks == 0

    def wait_until_idle(self) -> None:
        self._queue.join()

    def receive(self, event: EventMessageBase) -> None:
        # Delivery is fast since the `.receive` (on the calling component's thread) only
        # puts the event in the queue. The heavy lifting is then done by the event
        # handlers on each component's own thread.
        self._queue.put(event)

    def emit(self, event: EventMessageBase) -> None:
        self._event_bus.publish(event)

    def _event_loop(self) -> None:
        while True:
            self._consume(event := self._queue.get())
            if isinstance(event, SystemEvents.Shutdown):
                break
        self._teardown()

    def _consume(self, event: EventMessageBase) -> None:
        try:
            self._on_event(event)
        except Exception:
            self.emit(SystemEvents.Shutdown(reason=format_exc()))
        self._queue.task_done()

    # Override this hook to perform component-specific cleanup actions after the event
    # loop exits.
    def _teardown(self) -> None:
        pass

    # Subclasses must implement their event processing logic here.
    @abstractmethod
    def _on_event(self, event: EventMessageBase) -> None: ...


class ConnectableSystemComponentBase(SystemComponentBase, ABC):
    def __init__(self, event_bus: EventBus) -> None:
        super().__init__(event_bus)
        self._connect()

    @abstractmethod
    def _connect(self) -> None: ...

    @abstractmethod
    def _disconnect(self) -> None: ...

    def _teardown(self) -> None:
        self._disconnect()
        super()._teardown()


class RecorderBase(SystemComponentBase, ABC):
    # This base class exists to establish recorders as a recognized architectural
    # role in the system. Recorders subscribe to every event type and persist them
    # outside the system (e.g., to disk) without emitting anything back onto the bus.

    SUBSCRIBE_TO = tuple(
        member
        for cls in (DomainEvents, SystemEvents)
        for member in vars(cls).values()
        if isinstance(member, type) and issubclass(member, EventMessageBase)
    )


class AggregatorBase(SystemComponentBase, ABC):
    # This base class exists to establish aggregators as a recognized architectural
    # role in the system. Aggregators consume events and emit derived events that
    # communicate higher-level state to other components (e.g., round trip tracking).
    pass


class BrokerConnectorBase(ConnectableSystemComponentBase, ABC):
    SUBSCRIBE_TO: tuple[type[EventMessageBase], ...] = (
        DomainEvents.OrderRequest,
        DomainEvents.CancellationRequest,
    )

    def __init__(self, event_bus: EventBus, symbols: set[Symbol]) -> None:
        self._symbols = symbols
        super().__init__(event_bus)
        self._emit_account_state()

    @abstractmethod
    def _emit_account_state(self) -> None:
        # On (re-)connect, the `._emit_account_state` method should be called to emit a
        # synthetic `DomainEvents.Fill` event for each open position and a synthetic
        # `DomainEvents.OrderSubmitted` event for each working order to restore each
        # strategy's internal position, cost basis, and active order state.
        ...

    def _on_event(self, event: EventMessageBase) -> None:
        if (s := getattr(event, "symbol", None)) is not None and s not in self._symbols:
            return
        match event:
            case DomainEvents.OrderRequest():
                self._on_order_request(event)
            case DomainEvents.CancellationRequest():
                self._on_cancellation_request(event)

    @abstractmethod
    def _on_order_request(self, event: DomainEvents.OrderRequest) -> None: ...

    @abstractmethod
    def _on_cancellation_request(
        self, event: DomainEvents.CancellationRequest
    ) -> None: ...


class StrategyBase(SystemComponentBase):
    # Define a strategy by subclassing, registering indicators in `__init__` (via
    # `register_indicator`), and implementing trading logic in `on_bar`.

    SUBSCRIBE_TO: tuple[type[EventMessageBase], ...] = (
        DomainEvents.NewBar,
        DomainEvents.OrderSubmitted,
        DomainEvents.OrderRejected,
        DomainEvents.OrderCancelled,
        DomainEvents.CancellationRejected,
        DomainEvents.Fill,
        DomainEvents.OrderExpired,
    )

    @contextmanager
    def _bar_context(self, symbol: Symbol) -> Generator[None, None, None]:
        self._current_symbol = symbol
        try:
            yield
        finally:
            del self._current_symbol

    def __init__(self, event_bus: EventBus, symbols: set[Symbol]) -> None:
        # fmt: off
        self._symbols:          set[Symbol] = symbols
        self._indicators:       dict[IndicatorName, IndicatorBase] = {}
        self._pending_orders:   dict[UUID, DomainEvents.OrderRequest] = {}
        self._active_orders:    dict[UUID, DomainEvents.OrderRequest] = {}
        self._pending_cancels:  dict[UUID, DomainEvents.CancellationRequest] = {}
        self._position:         dict[Symbol, PositionSize] = {}
        self._cost_basis:       dict[Symbol, ScaledPrice] = {}
        # fmt: on
        super().__init__(event_bus)

    def position(self, symbol: Symbol | None = None) -> PositionSize:
        return self._position.get(symbol or self._current_symbol, 0)

    def cost_basis(self, symbol: Symbol | None = None) -> ScaledPrice | None:
        return self._cost_basis.get(symbol or self._current_symbol, None)

    def active_orders(
        self, symbol: Symbol | None = None
    ) -> list[DomainEvents.OrderRequest]:
        symbol = symbol or self._current_symbol
        return [o for o in self._active_orders.values() if o.symbol == symbol]

    def pending_orders(
        self, symbol: Symbol | None = None
    ) -> list[DomainEvents.OrderRequest]:
        symbol = symbol or self._current_symbol
        return [po for po in self._pending_orders.values() if po.symbol == symbol]

    def pending_cancels(
        self, symbol: Symbol | None = None
    ) -> list[DomainEvents.CancellationRequest]:
        symbol = symbol or self._current_symbol
        return [c for c in self._pending_cancels.values() if c.symbol == symbol]

    @abstractmethod
    def on_bar(self, event: DomainEvents.NewBar) -> None: ...

    def register_indicator(self, indicator: IndicatorBase) -> IndicatorBase:
        if indicator.name in self._indicators:
            # `SystemEvents.Shutdown` brings down already-running components; the raise
            # crashes the caller with a traceback.
            self.emit(SystemEvents.Shutdown(reason=f"duplicate {indicator.name!r}."))
            raise ValueError(f"duplicate {indicator.name!r}.")
        self._indicators[indicator.name] = indicator
        return indicator

    def submit_order(
        self,
        trade_side: TradeSide,
        qty: Quantity,
        limit_price: ScaledPrice,
        stop_price: ScaledPrice | None = None,
        symbol: Symbol | None = None,  # `None` to use current symbol within `on_bar`
    ) -> UUID:
        event = DomainEvents.OrderRequest(
            symbol=symbol or self._current_symbol,
            order_id=(order_id := uuid4()),
            trade_side=trade_side,
            qty=qty,
            limit_price=limit_price,
            stop_price=stop_price,
        )
        self._pending_orders[order_id] = event
        self.emit(event)
        return order_id

    def submit_cancel(self, order_id: UUID) -> None:
        event = DomainEvents.CancellationRequest(
            symbol=self._active_orders[order_id].symbol,
            order_id=order_id,
        )
        self._pending_cancels[order_id] = event
        self.emit(event)

    def _on_event(self, event: EventMessageBase) -> None:
        # Two strategies on the same bus must not trade the same symbol: they would
        # receive each other's fills and order lifecycle events, causing conflicting
        # state transitions. This is an inherent consequence of the broker-as-SSOT
        # mechanism (cf. DomainEvents.Fill).
        if (s := getattr(event, "symbol", None)) is not None and s not in self._symbols:
            return
        match event:
            case DomainEvents.NewBar():
                self._on_new_bar(event)
            case DomainEvents.OrderSubmitted():
                self._on_order_submitted(event)
            case DomainEvents.OrderRejected():
                self._on_order_rejected(event)
            case DomainEvents.OrderCancelled():
                self._on_order_cancelled(event)
            case DomainEvents.CancellationRejected():
                self._on_cancellation_rejected(event)
            case DomainEvents.Fill():
                self._on_fill(event)
            case DomainEvents.OrderExpired():
                self._on_order_expired(event)

    def _on_new_bar(self, event: DomainEvents.NewBar) -> None:
        with self._bar_context(event.symbol):
            self._update_indicators(event)
            self.on_bar(event)

    def _update_indicators(self, event: DomainEvents.NewBar) -> None:
        readings = {}
        for indicator in self._indicators.values():
            indicator.update(event)
            readings[indicator.name] = indicator[event.symbol, -1]
        self.emit(
            SystemEvents.IndicatorUpdate(
                source_bar=event,
                readings=readings,
            )
        )

    def _on_order_submitted(self, event: DomainEvents.OrderSubmitted) -> None:
        if event.synthetic:
            self._active_orders[event.order_id] = event.source_request
            return
        self._active_orders[event.order_id] = self._pending_orders.pop(event.order_id)

    def _on_order_rejected(self, event: DomainEvents.OrderRejected) -> None:
        self._pending_orders.pop(event.order_id)

    def _on_order_cancelled(self, event: DomainEvents.OrderCancelled) -> None:
        self._pending_cancels.pop(event.order_id)
        self._active_orders.pop(event.order_id)

    def _on_cancellation_rejected(
        self, event: DomainEvents.CancellationRejected
    ) -> None:
        # Defensively pop with `None` as default because a fill or expiration may have
        # already cleared the entry in `_pending_cancels` before this event message was
        # processed.
        self._pending_cancels.pop(event.order_id, None)

    def _on_fill(self, event: DomainEvents.Fill) -> None:
        if event.synthetic:
            self._position[event.symbol] = event.signed_position_size
            assert event.position_cost_basis is not None
            self._cost_basis[event.symbol] = event.position_cost_basis
            return
        # Defensively pop with `None` as default because a cancellation request may not
        # have been in flight.
        self._pending_cancels.pop(event.order_id, None)
        if event.remaining_qty == 0:
            self._active_orders.pop(event.order_id)
        if event.signed_position_size == 0:
            self._position.pop(event.symbol)
            self._cost_basis.pop(event.symbol)
        else:
            self._position[event.symbol] = event.signed_position_size
            assert event.position_cost_basis is not None
            self._cost_basis[event.symbol] = event.position_cost_basis

    def _on_order_expired(self, event: DomainEvents.OrderExpired) -> None:
        self._active_orders.pop(event.order_id)
        # Defensively pop with `None` as default because a cancellation request may not
        # have been in flight.
        self._pending_cancels.pop(event.order_id, None)


class DatafeedConnectorBase(ConnectableSystemComponentBase, ABC):
    def __init__(
        self, event_bus: EventBus, symbols: set[tuple[Symbol, PeriodType]]
    ) -> None:
        self._symbols = symbols
        super().__init__(event_bus)

    def _on_event(self, event: EventMessageBase) -> None:
        pass


# ——————————————————————————————————————————————————————————————————————————————————————
# Aggregators
# ——————————————————————————————————————————————————————————————————————————————————————


# ——————————————————————————————————————————————————————————————————————————————————————
# Recorders
# ——————————————————————————————————————————————————————————————————————————————————————


class PickleRecorder(RecorderBase):
    # Events are persisted as native Python objects via pickle (no conversion code is
    # needed, but the log would break if event dataclass fields are renamed or removed).

    def __init__(self, event_bus: EventBus, output_path: Path) -> None:
        self._output_path = output_path
        self._file: BufferedWriter | None = None
        super().__init__(event_bus)

    def _event_loop(self) -> None:
        self._output_path.parent.mkdir(parents=True, exist_ok=True)
        self._file = open(self._output_path, "wb")
        try:
            super()._event_loop()
        finally:
            self._file.close()

    def _on_event(self, event: EventMessageBase) -> None:
        assert self._file is not None
        pickle.dump(event, self._file)
        self._file.flush()

    @staticmethod
    def replay(event_bus: EventBus, path: Path) -> None:
        with open(path, "rb") as f:
            while True:
                try:
                    event = pickle.load(f)
                except EOFError:
                    break
                if not isinstance(event, SystemEvents.Shutdown):
                    event_bus.publish(event)
        event_bus.publish(SystemEvents.Shutdown(reason="end of replay"))


# ——————————————————————————————————————————————————————————————————————————————————————
# Backtesting Toolset
# ——————————————————————————————————————————————————————————————————————————————————————


class BacktestEventBus(EventBus):
    def publish(self, event: EventMessageBase) -> None:
        if not isinstance(event, DomainEvents.NewBar):
            super().publish(event)
            return

        # In live trading, fills happen during a bar and the new bar event arrives after
        # the bar closes, so strategies naturally see fills before they see the (then
        # closed) bar during which they happened.
        # In backtesting, therefore, simulated broker components must evaluate fills
        # against each bar before strategies receive the bar, which is why we need a
        # two-phased delivery mechanism if we want to use simulated broker components.

        for subscriber in self._per_eventtype_subscriptions[type(event)]:  # Phase 1
            if isinstance(subscriber, BrokerConnectorBase):
                subscriber.receive(event)
        self.wait_until_system_idle()

        for subscriber in self._per_eventtype_subscriptions[type(event)]:  # Phase 2
            if not isinstance(subscriber, BrokerConnectorBase):
                subscriber.receive(event)

    def wait_until_system_idle(
        self, exclude: SystemComponentLike | None = None
    ) -> None:
        # Processing events in one component may cause new events to land in other
        # components' queues, so we loop until all components are simultaneously idle.
        while True:
            all_components = set().union(*self._per_eventtype_subscriptions.values())
            if exclude is not None:
                all_components.discard(exclude)
            for component in all_components:
                component.wait_until_idle()
            if all(component.is_idle for component in all_components):
                break


class SimulatedBrokerConnector(BrokerConnectorBase):
    SUBSCRIBE_TO = BrokerConnectorBase.SUBSCRIBE_TO + (DomainEvents.NewBar,)

    def __init__(self, event_bus: BacktestEventBus, symbols: set[Symbol]) -> None:
        # fmt: off
        self._working_orders:   dict[UUID, DomainEvents.OrderRequest] = {}
        self._position:         dict[Symbol, PositionSize] = {}
        self._cost_basis:       dict[Symbol, ScaledPrice] = {}
        # fmt: on
        super().__init__(event_bus, symbols)

    def _connect(self) -> None:
        pass

    def _disconnect(self) -> None:
        pass

    def _emit_account_state(self) -> None:
        pass

    def _on_event(self, event: EventMessageBase) -> None:
        match event:
            case DomainEvents.NewBar():
                self._on_bar(event)
            case _:
                super()._on_event(event)

    def _on_order_request(self, event: DomainEvents.OrderRequest) -> None:
        # Note that (for simplicity) orders are treated as GTC with no expiration.
        self._working_orders[event.order_id] = event
        self.emit(
            DomainEvents.OrderSubmitted(
                symbol=event.symbol,
                order_id=event.order_id,
                source_request=event,
            )
        )

    def _on_cancellation_request(self, event: DomainEvents.CancellationRequest) -> None:
        # Safe to hard-delete because two-phase bar delivery guarantees the strategy
        # has seen all fills before it can issue a cancel.
        del self._working_orders[event.order_id]
        self.emit(
            DomainEvents.OrderCancelled(
                symbol=event.symbol,
                order_id=event.order_id,
            )
        )

    def _on_bar(self, bar: DomainEvents.NewBar) -> None:
        for order in list(self._working_orders.values()):
            if order.symbol != bar.symbol:
                continue
            if order.stop_price is not None:
                self._try_trigger_stop(order, bar)
            else:
                self._try_fill_limit(order, bar)

    def _try_fill_limit(
        self, order: DomainEvents.OrderRequest, bar: DomainEvents.NewBar
    ) -> None:
        # We conservatively assume no fill if the limit price is just touched.
        if order.trade_side is TradeSide.BUY and bar.low < order.limit_price:
            self._fill(order=order, fill_price=min(order.limit_price, bar.open))
        elif order.trade_side is TradeSide.SELL and bar.high > order.limit_price:
            self._fill(order=order, fill_price=max(order.limit_price, bar.open))

    def _try_trigger_stop(
        self, order: DomainEvents.OrderRequest, bar: DomainEvents.NewBar
    ) -> None:
        assert order.stop_price is not None

        stop_triggered = (
            order.trade_side is TradeSide.BUY and bar.high >= order.stop_price
        ) or (order.trade_side is TradeSide.SELL and bar.low <= order.stop_price)
        if not stop_triggered:
            return

        limit_order = DomainEvents.OrderRequest(
            symbol=order.symbol,
            order_id=order.order_id,
            trade_side=order.trade_side,
            qty=order.qty,
            limit_price=order.limit_price,
            stop_price=None,
        )
        self._working_orders[order.order_id] = limit_order

        # The limit order can either serve as slippage protection beyond the stop level
        # or fish for a pullback after the breakout. We fill slippage protection orders
        # at the stop price (optimistically disregarding slippage for simplicity) and
        # defer pullback orders to the next bar since bar data cannot confirm the
        # retracement occurred after the stop triggered.
        if order.trade_side is TradeSide.BUY and order.limit_price >= order.stop_price:
            self._fill(order=limit_order, fill_price=order.stop_price)
        elif (
            order.trade_side is TradeSide.SELL and order.limit_price <= order.stop_price
        ):
            self._fill(order=limit_order, fill_price=order.stop_price)

    def _fill(self, order: DomainEvents.OrderRequest, fill_price: ScaledPrice) -> None:
        position_size_before_fill = self._position.get(order.symbol, 0)
        signed_fill_qty = order.qty if order.trade_side is TradeSide.BUY else -order.qty

        # We optimistically model all fills as complete for simplicity.
        del self._working_orders[order.order_id]

        updated_position_size: PositionSize = (
            position_size_before_fill + signed_fill_qty
        )

        # Weighted average cost basis, not FIFO. The two methods produce identical total
        # PnL per round trip (flat to flat); they only diverge in per-fill attribution
        # after partial reductions. If a strategy uses cost basis as a decision input
        # with partial reductions, the live broker must use the same accounting method
        # for the backtest to be accurate (since the broker is the SSOT).
        updated_cost_basis: ScaledPrice | None

        # Case: fill opens new position
        if position_size_before_fill == 0:
            updated_cost_basis = fill_price

        # Case: fill adds to position
        elif position_size_before_fill * signed_fill_qty > 0:
            updated_cost_basis = (
                position_size_before_fill * self._cost_basis[order.symbol]
                + signed_fill_qty * fill_price
            ) // updated_position_size

        # Case: fill flips position
        elif position_size_before_fill * updated_position_size < 0:
            updated_cost_basis = fill_price

        # Case: fill flattens existing position
        elif updated_position_size == 0:
            updated_cost_basis = None

        # Case: fill reduces position (but neither flips nor flattens it)
        else:
            updated_cost_basis = self._cost_basis[order.symbol]

        self.emit(
            DomainEvents.Fill(
                symbol=order.symbol,
                order_id=order.order_id,
                trade_side=order.trade_side,
                filled_qty=order.qty,
                remaining_qty=0,
                fill_price=fill_price,
                signed_position_size=updated_position_size,
                position_cost_basis=updated_cost_basis,
            )
        )

        if updated_position_size == 0:
            self._position.pop(order.symbol)
            self._cost_basis.pop(order.symbol)
        else:
            self._position[order.symbol] = updated_position_size
            assert updated_cost_basis is not None
            self._cost_basis[order.symbol] = updated_cost_basis


class SimulatedDatafeed(DatafeedConnectorBase):
    def __init__(
        self,
        event_bus: BacktestEventBus,
        symbols: set[tuple[Symbol, PeriodType]],
        path_to_pkl: Path,
        max_bars: int | None = None,
    ) -> None:
        # Pickle file needs to contain a chronological succession of objects of type
        # `DomainEvents.NewBar`.
        self._path_to_pkl = path_to_pkl
        # Second reference to the same event bus; needed because the parent annotates
        # `_event_bus` as `EventBus`, which does not have `wait_until_system_idle`.
        self._backtest_bus = event_bus
        self._max_bars = max_bars
        super().__init__(event_bus, symbols)

    def _connect(self) -> None:
        bars_emitted = 0
        with open(self._path_to_pkl, "rb") as f:
            while True:
                try:
                    bar = pickle.load(f)
                except EOFError:
                    break
                if (bar.symbol, bar.period_type) in self._symbols:
                    # Replace the stale serialization timestamp with current timestamp.
                    self.emit(replace(bar, timestamp=time_ns()))
                    bars_emitted += 1
                    if self._max_bars is not None and bars_emitted == self._max_bars:
                        break

        self._backtest_bus.wait_until_system_idle(exclude=self)
        self.emit(
            SystemEvents.Shutdown(
                reason=f"end of historical data: ({bars_emitted} bars emitted)"
            )
        )

    def _disconnect(self) -> None:
        pass
