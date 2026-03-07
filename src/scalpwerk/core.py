import threading
import time
import typing
import uuid

from abc import ABC, abstractmethod
from collections import defaultdict, deque
from dataclasses import dataclass
from enum import Enum, auto
from queue import Queue

__all__ = [
    "UnixNanoseconds",
    "ScaledPrice",
    "IndicatorValue",
    "Volume",
    "Quantity",
    "FilledQuantity",
    "PositionSize",
    "Symbol",
    "IndicatorName",
    "InternalOrderId",
    "InternalFillId",
    "BrokerOrderId",
    "BrokerFillId",
    "RunId",
    "Models",
    "Events",
    "EventBus",
    "BrokerBase",
    "DatafeedBase",
    "IndicatorBase",
    "StrategyBase",
    "RunRecorderBase",
]

# ——————————————————————————————————————————————————————————————————————————————————————
# DOMAIN-SPECIFIC TYPE DEFINITIONS
# ——————————————————————————————————————————————————————————————————————————————————————


UnixNanoseconds = typing.NewType("UnixNanoseconds", int)

ScaledPrice = typing.NewType("ScaledPrice", int)
IndicatorValue = typing.NewType("IndicatorValue", float)

Volume = typing.NewType("Volume", int)
Quantity = typing.NewType("Quantity", int)
FilledQuantity = typing.NewType("FilledQuantity", int)
PositionSize = typing.NewType("PositionSize", int)

Symbol = typing.NewType("Symbol", str)
IndicatorName = typing.NewType("IndicatorName", str)

InternalOrderId = typing.NewType("InternalOrderId", uuid.UUID)
InternalFillId = typing.NewType("InternalFillId", uuid.UUID)
BrokerOrderId = typing.NewType("BrokerOrderId", str)
BrokerFillId = typing.NewType("BrokerFillId", str)
RunId = typing.NewType("RunId", str)


# ——————————————————————————————————————————————————————————————————————————————————————
# DOMAIN MODEL DEFINITIONS
# ——————————————————————————————————————————————————————————————————————————————————————


class Models:
    class OrderType(Enum):
        MARKET = auto()
        LIMIT = auto()
        STOP = auto()
        STOP_LIMIT = auto()

    class TradeSide(Enum):
        BUY = auto()
        SELL = auto()

    # The enum values for record types match DataBento's schema values for convenience.
    class RecordType(Enum):
        OHLCV_1S = 32
        OHLCV_1M = 33
        OHLCV_1H = 34
        OHLCV_1D = 35


# ——————————————————————————————————————————————————————————————————————————————————————
# EVENT DEFINITIONS
# ——————————————————————————————————————————————————————————————————————————————————————


# Defined outside `Events` so nested classes within `Events` can inherit from it.
@dataclass(kw_only=True, frozen=True, slots=True)
class _EventBase:
    occurred_at_ns: UnixNanoseconds
    created_at_ns: UnixNanoseconds


class Events:
    class MarketUpdate:
        @dataclass(kw_only=True, frozen=True, slots=True)
        class OHLCV(_EventBase):
            symbol: Symbol
            record_type: typing.Literal[
                Models.RecordType.OHLCV_1S,
                Models.RecordType.OHLCV_1M,
                Models.RecordType.OHLCV_1H,
                Models.RecordType.OHLCV_1D,
            ]
            open: ScaledPrice
            high: ScaledPrice
            low: ScaledPrice
            close: ScaledPrice
            volume: Volume | None = None

    class StrategyUpdate:
        @dataclass(kw_only=True, frozen=True, slots=True)
        class IndicatorUpdate(_EventBase):
            symbol: Symbol
            source_event: "Events.MarketUpdate.OHLCV"
            indicator_values: dict[IndicatorName, IndicatorValue]

    class BrokerRequest:
        @dataclass(kw_only=True, frozen=True, slots=True)
        class _RequestBase(_EventBase):
            internal_order_id: InternalOrderId
            symbol: Symbol

        @dataclass(kw_only=True, frozen=True, slots=True)
        class SubmitOrder(_RequestBase):
            order_type: Models.OrderType
            side: Models.TradeSide
            quantity: Quantity
            limit_price: ScaledPrice | None = None
            stop_price: ScaledPrice | None = None

        @dataclass(kw_only=True, frozen=True, slots=True)
        class ModifyOrder(_RequestBase):
            quantity: Quantity
            limit_price: ScaledPrice | None = None
            stop_price: ScaledPrice | None = None

        @dataclass(kw_only=True, frozen=True, slots=True)
        class CancelOrder(_RequestBase):
            pass

    class BrokerResponse:
        @dataclass(kw_only=True, frozen=True, slots=True)
        class _ResponseBase(_EventBase):
            internal_order_id: InternalOrderId
            # Carried for reconciliation against broker statements. Optional because a
            # rejection may arrive before the broker has assigned an ID.
            broker_order_id: BrokerOrderId | None = None

        @dataclass(kw_only=True, frozen=True, slots=True)
        class CancellationAccepted(_ResponseBase):
            pass

        @dataclass(kw_only=True, frozen=True, slots=True)
        class CancellationRejected(_ResponseBase):
            # Free-form rejection reason as provided by the broker for manual review.
            reason: str

        @dataclass(kw_only=True, frozen=True, slots=True)
        class ModificationAccepted(_ResponseBase):
            pass

        @dataclass(kw_only=True, frozen=True, slots=True)
        class ModificationRejected(_ResponseBase):
            reason: str

        @dataclass(kw_only=True, frozen=True, slots=True)
        class OrderAccepted(_ResponseBase):
            pass

        @dataclass(kw_only=True, frozen=True, slots=True)
        class OrderRejected(_ResponseBase):
            reason: str

        # A single execution against an order. Does not indicate partial vs. full fill;
        # the system must track fill quantities to determine remaining open quantity.
        @dataclass(kw_only=True, frozen=True, slots=True)
        class Fill(_ResponseBase):
            symbol: Symbol
            internal_fill_id: InternalFillId
            broker_fill_id: BrokerFillId | None = None
            side: Models.TradeSide
            filled_quantity: Quantity
            fill_price: ScaledPrice
            exchange: str
            # Combined cost for this fill (broker, exchange, and regulatory fees).
            commission: ScaledPrice | None = None

        @dataclass(kw_only=True, frozen=True, slots=True)
        class OrderExpired(_ResponseBase):
            pass


# ——————————————————————————————————————————————————————————————————————————————————————
# EVENT BUS DEFINITION
# ——————————————————————————————————————————————————————————————————————————————————————


# Defines the subscriber interface that `EventBus` depends on. A protocol is used
# because `_SubscriberBase` is defined after `EventBus`.
class _SubscriberLike(typing.Protocol):
    def wait_until_idle(self) -> None: ...
    @property
    def is_idle(self) -> bool: ...
    def receive(self, event: _EventBase) -> None: ...


class EventBus:
    def __init__(self) -> None:
        # Using a set as the defaultdict's value avoids needing additional logic to
        # guard against duplicate subscriptions in the `add_event_subscription` method.
        self._per_event_subscriptions: defaultdict[
            type[_EventBase], set[_SubscriberLike]
        ] = defaultdict(set)
        # Each subscriber runs in its own thread. The Lock ensures that only one
        # subscriber thread at a time can access `_per_event_subscriptions`.
        self._lock: threading.Lock = threading.Lock()

    def add_event_subscription(
        self, subscriber: _SubscriberLike, event_type: type[_EventBase]
    ) -> None:
        with self._lock:
            self._per_event_subscriptions[event_type].add(subscriber)

    def remove_subscriber(self, subscriber: _SubscriberLike) -> None:
        with self._lock:
            for subscriber_set in self._per_event_subscriptions.values():
                subscriber_set.discard(subscriber)

    def publish_event_to_system(self, event: _EventBase) -> None:
        # Copy under the Lock and iterate outside it so the Lock is held as briefly
        # as possible and concurrent publishes from other threads are not blocked.
        with self._lock:
            subscribers = self._per_event_subscriptions[type(event)].copy()
        for subscriber in subscribers:
            subscriber.receive(event)

    # Backtesting must wait for all subscribers to finish before feeding the next market
    # event, otherwise the strategy and simulated broker would fall out of sync.
    def wait_until_system_idle(self) -> None:
        # A single pass is insufficient because processing can publish new events into
        # already-drained queues, so we loop until a re-check confirms idleness.
        while True:
            # Waiting for the subscribers to be idle needs to be done outside the lock
            # since they may require the lock themselves to publish an event.
            with self._lock:
                subscribers = set().union(*self._per_event_subscriptions.values())
            for subscriber in subscribers:
                subscriber.wait_until_idle()

            with self._lock:
                subscribers = set().union(*self._per_event_subscriptions.values())
            if all(subscriber.is_idle for subscriber in subscribers):
                break


# ——————————————————————————————————————————————————————————————————————————————————————
# GENERAL SYSTEM COMPONENT BASE CLASS DEFINITIONS
# ——————————————————————————————————————————————————————————————————————————————————————


class _ComponentBase(ABC):
    def __init__(self, event_bus: EventBus) -> None:
        # The concrete `EventBus` instance is injected rather than a global instance so
        # multiple independent systems could co-exist within the same runtime.
        self._event_bus: EventBus = event_bus


class _SubscriberBase(_ComponentBase):
    def __init__(self, event_bus: EventBus) -> None:
        super().__init__(event_bus)
        # None acts as a poison pill that tells the event loop to shut down.
        self._queue: Queue[_EventBase | None] = Queue()
        # Thread-safe flag (no Lock needed). Set before starting the thread so receive
        # can accept events immediately; otherwise they'd be silently dropped.
        self._running: threading.Event = threading.Event()
        self._running.set()
        self._thread = threading.Thread(
            target=self._event_loop, name=self.__class__.__name__
        )
        self._thread.start()

    def wait_until_idle(self) -> None:
        if not self._running.is_set():
            return
        self._queue.join()

    # Checks idleness without waiting for the queue to drain (i.e., non-blocking check).
    @property
    def is_idle(self) -> bool:
        if not self._running.is_set():
            return True
        return self._queue.unfinished_tasks == 0

    def receive(self, event: _EventBase) -> None:
        if self._running.is_set():
            self._queue.put(event)

    def shutdown(self):
        if not self._running.is_set():
            return
        self._event_bus.remove_subscriber(self)
        # Clear `_running` before the poison pill so `receive` stops accepting events
        self._running.clear()
        self._queue.put(None)
        # If `shutdown()` is called from within `_on_event`, we're on the subscriber's
        # own thread. A thread cannot join itself, this prevents a runtime error.
        if threading.current_thread() is not self._thread:
            self._thread.join()

    def _subscribe_to_events(self, *event_types: type[_EventBase]):
        for event_type in event_types:
            self._event_bus.add_event_subscription(self, event_type)

    def _event_loop(self):
        try:
            while True:
                event = self._queue.get()
                if event is None:
                    # `task_done()` needs to be called for the poison pill itself,
                    # otherwise `queue.join()` would block indefinitely after shutdown.
                    self._queue.task_done()
                    break
                # If `_on_exception` re-raises, the outer except clears `_running` so
                # the system doesn't deadlock on a dead thread.
                try:
                    self._on_event(event)
                except Exception as exc:
                    self._on_exception(exc)
                finally:
                    self._queue.task_done()
        except Exception:
            self._running.clear()

    @abstractmethod
    def _on_event(self, event: _EventBase):
        pass

    @abstractmethod
    def _on_exception(self, exception: Exception):
        pass


# Parameterizes `_EmitterBase` so each subclass must declare which event types it emits.
_EmittableEventType = typing.TypeVar("_EmittableEventType", bound=_EventBase)


# Subclasses specify their allowed event types, e.g.
# `class DatafeedBase(_EmitterBase[<Events.<NestedNamespace>.SomeEventType>])`.
class _EmitterBase(_ComponentBase, typing.Generic[_EmittableEventType]):
    def _emit_event(self, event: _EmittableEventType) -> None:
        self._event_bus.publish_event_to_system(event)


# Mixin class for components that needs to manage an external connection lifecycle.
class _ExternalComponentMixin(ABC):
    @abstractmethod
    def connect(self) -> None:
        pass

    @abstractmethod
    def disconnect(self) -> None:
        pass


# ——————————————————————————————————————————————————————————————————————————————————————
# CONCRETE BASE CLASS DEFINITIONS
# ——————————————————————————————————————————————————————————————————————————————————————


class BrokerBase(
    _ExternalComponentMixin,
    _SubscriberBase,
    _EmitterBase[
        Events.BrokerResponse.OrderAccepted
        | Events.BrokerResponse.OrderRejected
        | Events.BrokerResponse.ModificationAccepted
        | Events.BrokerResponse.ModificationRejected
        | Events.BrokerResponse.CancellationAccepted
        | Events.BrokerResponse.CancellationRejected
        | Events.BrokerResponse.Fill
        | Events.BrokerResponse.OrderExpired
    ],
):
    def __init__(self, event_bus: EventBus):
        super().__init__(event_bus)

        self._subscribe_to_events(
            Events.BrokerRequest.SubmitOrder,
            Events.BrokerRequest.ModifyOrder,
            Events.BrokerRequest.CancelOrder,
        )

    def _on_event(self, event: _EventBase) -> None:
        match event:
            case Events.BrokerRequest.SubmitOrder() as submit_order_event:
                self._on_submit_order(submit_order_event)
            case Events.BrokerRequest.ModifyOrder() as modify_order_event:
                self._on_modify_order(modify_order_event)
            case Events.BrokerRequest.CancelOrder() as cancel_order_event:
                self._on_cancel_order(cancel_order_event)
            case _:
                return

    @abstractmethod
    def _on_submit_order(self, event: Events.BrokerRequest.SubmitOrder) -> None:
        pass

    @abstractmethod
    def _on_modify_order(self, event: Events.BrokerRequest.ModifyOrder) -> None:
        pass

    @abstractmethod
    def _on_cancel_order(self, event: Events.BrokerRequest.CancelOrder) -> None:
        pass


class DatafeedBase(_ExternalComponentMixin, _EmitterBase[Events.MarketUpdate.OHLCV]):
    def __init__(self, event_bus: EventBus):
        super().__init__(event_bus)

    @abstractmethod
    def subscribe(self, symbols: list[Symbol], record_type: Models.RecordType) -> None:
        pass

    @abstractmethod
    def unsubscribe(
        self, symbols: list[Symbol], record_type: Models.RecordType
    ) -> None:
        pass


# Computes and stores a single scalar value per symbol on each `update` call. Complex
# indicators must be split into separate instances, e.g. `BollingerUpper` et cetera.
class IndicatorBase(ABC):
    def __init__(self, max_history: int = 100) -> None:
        self._max_history = max(1, int(max_history))
        # Per-symbol bounded FIFO buffers. The bounded size ensures memory stays
        # predictable since indicators only need a finite lookback window.
        self._history: dict[Symbol, deque[IndicatorValue]] = {}

    # The name should be defined via an f-string so that instances of the same indicator
    # can be distinguished via their parameters, e.g. SMAs with different window size.
    @property
    @abstractmethod
    def name(self) -> IndicatorName:
        pass

    def update(self, event: Events.MarketUpdate.OHLCV) -> None:
        value = self._compute(event)
        symbol = event.symbol
        if symbol not in self._history:
            self._history[symbol] = deque(maxlen=self._max_history)
        self._history[symbol].append(value)

    @abstractmethod
    def _compute(self, event: Events.MarketUpdate.OHLCV) -> IndicatorValue:
        pass

    def latest(self, symbol: Symbol) -> IndicatorValue:
        return self[symbol, -1]

    # Supports standard negative indexing, e.g. `indicator["AAPL", -2]`.
    def __getitem__(self, key: tuple[Symbol, int]) -> IndicatorValue:
        symbol, index = key
        history = self._history.get(symbol)
        if history is None:
            return IndicatorValue(float("nan"))
        try:
            return history[index]
        except IndexError:
            return IndicatorValue(float("nan"))


@dataclass(frozen=True, slots=True)
class _PendingOrder:
    order: Events.BrokerRequest.SubmitOrder
    filled_quantity: FilledQuantity


class StrategyBase(
    _SubscriberBase,
    _EmitterBase[
        Events.BrokerRequest.SubmitOrder
        | Events.BrokerRequest.ModifyOrder
        | Events.BrokerRequest.CancelOrder
        | Events.StrategyUpdate.IndicatorUpdate
    ],
):
    def __init__(
        self,
        event_bus: EventBus,
        symbols: list[Symbol],
        record_type: Models.RecordType,
    ) -> None:
        super().__init__(event_bus)

        self._symbols: list[Symbol] = symbols
        self._record_type: Models.RecordType = record_type

        self._subscribe_to_events(
            Events.MarketUpdate.OHLCV,
            Events.BrokerResponse.OrderAccepted,
            Events.BrokerResponse.OrderRejected,
            Events.BrokerResponse.ModificationAccepted,
            Events.BrokerResponse.ModificationRejected,
            Events.BrokerResponse.CancellationAccepted,
            Events.BrokerResponse.CancellationRejected,
            Events.BrokerResponse.Fill,
            Events.BrokerResponse.OrderExpired,
        )

        self._current_symbol: Symbol | None = None
        self._current_event_ns: UnixNanoseconds | None = None

        self._indicators: dict[IndicatorName, IndicatorBase] = {}

        # `_submitted*` dicts store in-flight requests awaiting broker acknowledgement.
        # Orders move to `_pending_orders` on acceptance or are removed on rejection.
        self._submitted_orders: dict[
            InternalOrderId, Events.BrokerRequest.SubmitOrder
        ] = {}
        self._submitted_modifications: dict[
            InternalOrderId, Events.BrokerRequest.ModifyOrder
        ] = {}
        self._submitted_cancellations: dict[
            InternalOrderId, Events.BrokerRequest.CancelOrder
        ] = {}

        # Tracks accepted orders and their cumulative filled quantity. Orders leave
        # this dict on full fill, cancellation, or expiry.
        self._pending_orders: dict[InternalOrderId, _PendingOrder] = {}

        self._position_sizes: dict[Symbol, PositionSize] = {}
        self._average_entry_prices: dict[Symbol, ScaledPrice] = {}

        # Must be last so base class state exists before the subclass's `setup()` runs.
        self.setup()

    # Subclasses implement `setup()` instead of `__init__` to avoid `super().__init__`.
    @abstractmethod
    def setup(self) -> None:
        pass

    def add_indicator(self, indicator: IndicatorBase) -> IndicatorBase:
        if indicator.name in self._indicators:
            raise ValueError(
                f"Indicator with name '{indicator.name}' is already registered."
            )
        self._indicators[indicator.name] = indicator
        # Returns indicator for inline assignment: `self.sma = self.add_indicator(...)`.
        return indicator

    def _on_event(self, event: _EventBase) -> None:
        match event:
            case Events.MarketUpdate.OHLCV() as ohlcv_event:
                self._on_market_update(ohlcv_event)
            case Events.BrokerResponse.OrderAccepted() as order_accepted_event:
                self._on_order_accepted(order_accepted_event)
            case Events.BrokerResponse.OrderRejected() as order_rejected_event:
                self._on_order_rejected(order_rejected_event)
            case Events.BrokerResponse.ModificationAccepted() as mod_accepted_event:
                self._on_modification_accepted(mod_accepted_event)
            case Events.BrokerResponse.ModificationRejected() as mod_rejected_event:
                self._on_modification_rejected(mod_rejected_event)
            case Events.BrokerResponse.CancellationAccepted() as cancel_accepted_event:
                self._on_cancellation_accepted(cancel_accepted_event)
            case Events.BrokerResponse.CancellationRejected() as cancel_rejected_event:
                self._on_cancellation_rejected(cancel_rejected_event)
            case Events.BrokerResponse.Fill() as fill_event:
                self._on_fill(fill_event)
            case Events.BrokerResponse.OrderExpired() as order_expired_event:
                self._on_order_expired(order_expired_event)
            case _:
                return

    # Wraps the abstract `on_market_update` with internal plumbing.
    def _on_market_update(self, event: Events.MarketUpdate.OHLCV) -> None:
        self._current_symbol = event.symbol
        self._current_event_ns = event.occurred_at_ns
        for indicator in self._indicators.values():
            indicator.update(event)
        self.on_market_update(event)
        # Emitted after `on_market_update` so strategy logic isn't delayed by emission.
        self._emit_indicator_update(event)

    @abstractmethod
    def on_market_update(self, event: Events.MarketUpdate.OHLCV) -> None:
        pass

    def _emit_indicator_update(self, source_event: Events.MarketUpdate.OHLCV) -> None:
        if not self._indicators:
            return

        assert self._current_symbol is not None
        assert self._current_event_ns is not None

        indicator_values = {
            name: indicator.latest(self._current_symbol)
            for name, indicator in self._indicators.items()
        }

        self._emit_event(
            Events.StrategyUpdate.IndicatorUpdate(
                occurred_at_ns=self._current_event_ns,
                created_at_ns=self._current_event_ns,
                symbol=self._current_symbol,
                source_event=source_event,
                indicator_values=indicator_values,
            )
        )

    # Convenience properties so `on_market_update` can query position state for the
    # current symbol.
    @property
    def position_size(self) -> PositionSize:
        assert self._current_symbol is not None
        return self._position_sizes.get(self._current_symbol, PositionSize(0))

    @property
    def flat(self) -> bool:
        return self.position_size == 0

    @property
    def average_entry_price(self) -> ScaledPrice | None:
        assert self._current_symbol is not None
        return self._average_entry_prices.get(self._current_symbol)

    def submit_order(
        self,
        symbol: Symbol,
        order_type: Models.OrderType,
        side: Models.TradeSide,
        quantity: Quantity,
        limit_price: ScaledPrice | None = None,
        stop_price: ScaledPrice | None = None,
    ) -> InternalOrderId:
        assert self._current_event_ns is not None

        internal_order_id = InternalOrderId(uuid.uuid4())

        event = Events.BrokerRequest.SubmitOrder(
            occurred_at_ns=self._current_event_ns,
            created_at_ns=self._current_event_ns,
            internal_order_id=internal_order_id,
            symbol=symbol,
            order_type=order_type,
            side=side,
            quantity=quantity,
            limit_price=limit_price,
            stop_price=stop_price,
        )

        self._submitted_orders[internal_order_id] = event
        self._emit_event(event)
        return internal_order_id

    # Returns False if the order is not in `_pending_orders` (i.e. not modifiable).
    def submit_modification(
        self,
        internal_order_id: InternalOrderId,
        quantity: Quantity,
        limit_price: ScaledPrice | None = None,
        stop_price: ScaledPrice | None = None,
    ) -> bool:
        assert self._current_event_ns is not None

        pending = self._pending_orders.get(internal_order_id)
        if pending is None:
            return False

        event = Events.BrokerRequest.ModifyOrder(
            occurred_at_ns=self._current_event_ns,
            created_at_ns=self._current_event_ns,
            internal_order_id=internal_order_id,
            symbol=pending.order.symbol,
            quantity=quantity,
            limit_price=limit_price,
            stop_price=stop_price,
        )

        self._submitted_modifications[internal_order_id] = event
        self._emit_event(event)
        return True

    # Returns False if the order is not in `_pending_orders` (i.e. not cancellable).
    def submit_cancellation(self, internal_order_id: InternalOrderId) -> bool:
        assert self._current_event_ns is not None

        pending = self._pending_orders.get(internal_order_id)
        if pending is None:
            return False

        event = Events.BrokerRequest.CancelOrder(
            occurred_at_ns=self._current_event_ns,
            created_at_ns=self._current_event_ns,
            internal_order_id=internal_order_id,
            symbol=pending.order.symbol,
        )

        self._submitted_cancellations[internal_order_id] = event
        self._emit_event(event)
        return True

    # Broker implementations must emit OrderAccepted before any Fill for the same order,
    # otherwise fills would arrive before `_pending_orders` has the entry to update.
    def _on_order_accepted(self, event: Events.BrokerResponse.OrderAccepted) -> None:
        order = self._submitted_orders.pop(event.internal_order_id, None)
        if order is not None:
            self._pending_orders[event.internal_order_id] = _PendingOrder(
                order, FilledQuantity(0)
            )

    def _on_order_rejected(self, event: Events.BrokerResponse.OrderRejected) -> None:
        self._submitted_orders.pop(event.internal_order_id, None)

    def _on_modification_accepted(
        self, event: Events.BrokerResponse.ModificationAccepted
    ) -> None:
        modification = self._submitted_modifications.pop(event.internal_order_id, None)
        if modification is not None:
            pending = self._pending_orders.get(event.internal_order_id)
            if pending is not None:
                updated_order = Events.BrokerRequest.SubmitOrder(
                    occurred_at_ns=pending.order.occurred_at_ns,
                    created_at_ns=pending.order.created_at_ns,
                    internal_order_id=pending.order.internal_order_id,
                    symbol=pending.order.symbol,
                    order_type=pending.order.order_type,
                    side=pending.order.side,
                    quantity=modification.quantity,
                    limit_price=modification.limit_price,
                    stop_price=modification.stop_price,
                )
                # Check necessary because a quantity reduction may retroactively fully
                # fill the order if fills arrived while the modification was in-flight.
                if pending.filled_quantity >= modification.quantity:
                    self._pending_orders.pop(event.internal_order_id)
                else:
                    self._pending_orders[event.internal_order_id] = _PendingOrder(
                        updated_order,
                        pending.filled_quantity,
                    )

    def _on_modification_rejected(
        self, event: Events.BrokerResponse.ModificationRejected
    ) -> None:
        self._submitted_modifications.pop(event.internal_order_id, None)

    def _on_cancellation_accepted(
        self, event: Events.BrokerResponse.CancellationAccepted
    ) -> None:
        self._submitted_cancellations.pop(event.internal_order_id, None)
        self._pending_orders.pop(event.internal_order_id, None)
        # In-flight modifications will not get a response after cancellation.
        self._submitted_modifications.pop(event.internal_order_id, None)

    def _on_cancellation_rejected(
        self, event: Events.BrokerResponse.CancellationRejected
    ) -> None:
        self._submitted_cancellations.pop(event.internal_order_id, None)

    def _on_fill(self, event: Events.BrokerResponse.Fill) -> None:
        self._update_position_size_and_avg_entry_price(event)
        self._update_pending_orders(event)

    def _update_pending_orders(self, event: Events.BrokerResponse.Fill) -> None:
        pending = self._pending_orders.get(event.internal_order_id)
        # The order may already be gone (prior fill, cancellation, or expiry).
        if pending is None:
            return
        new_filled_quantity = FilledQuantity(
            pending.filled_quantity + event.filled_quantity
        )
        if new_filled_quantity >= pending.order.quantity:
            self._pending_orders.pop(event.internal_order_id)
        else:
            self._pending_orders[event.internal_order_id] = _PendingOrder(
                pending.order, new_filled_quantity
            )

    def _update_position_size_and_avg_entry_price(
        self, event: Events.BrokerResponse.Fill
    ) -> None:
        signed_quantity = (
            event.filled_quantity
            if event.side == Models.TradeSide.BUY
            else -event.filled_quantity
        )

        # Default 0 so the first fill is handled as a fresh entry below.
        old_position = self._position_sizes.get(event.symbol, 0)
        old_avg_entry_price = self._average_entry_prices.get(event.symbol, 0)
        new_position = old_position + signed_quantity

        # Flat: no position, no meaningful average entry price.
        if new_position == 0:
            self._position_sizes.pop(event.symbol, None)
            self._average_entry_prices.pop(event.symbol, None)
            return
        # Fresh entry or position flip: old average is irrelevant.
        elif old_position == 0 or old_position * new_position < 0:
            new_avg_entry_price = event.fill_price
        # Adding to existing position: weighted average.
        elif old_position * signed_quantity > 0:
            new_avg_entry_price = ScaledPrice(
                (
                    old_avg_entry_price * abs(old_position)
                    + event.fill_price * abs(signed_quantity)
                )
                // abs(new_position)
            )
        # Partial close: avg unchanged, remaining shares entered at old avg.
        else:
            new_avg_entry_price = ScaledPrice(old_avg_entry_price)

        self._position_sizes[event.symbol] = PositionSize(new_position)
        self._average_entry_prices[event.symbol] = ScaledPrice(new_avg_entry_price)

    def _on_order_expired(self, event: Events.BrokerResponse.OrderExpired) -> None:
        self._pending_orders.pop(event.internal_order_id, None)
        # The order is dead, so any in-flight modification or cancellation will never
        # get a response. Clean up to prevent leaks.
        self._submitted_modifications.pop(event.internal_order_id, None)
        self._submitted_cancellations.pop(event.internal_order_id, None)


class RunRecorderBase(_SubscriberBase):
    def __init__(self, event_bus: EventBus):
        super().__init__(event_bus)

        self._subscribe_to_events(
            Events.StrategyUpdate.IndicatorUpdate,
            Events.BrokerRequest.SubmitOrder,
            Events.BrokerRequest.ModifyOrder,
            Events.BrokerRequest.CancelOrder,
            Events.BrokerResponse.OrderAccepted,
            Events.BrokerResponse.OrderRejected,
            Events.BrokerResponse.ModificationAccepted,
            Events.BrokerResponse.ModificationRejected,
            Events.BrokerResponse.CancellationAccepted,
            Events.BrokerResponse.CancellationRejected,
            Events.BrokerResponse.Fill,
            Events.BrokerResponse.OrderExpired,
        )

    @abstractmethod
    def _on_event(self, event: _EventBase):
        pass

    @abstractmethod
    def _on_exception(self, exception: Exception):
        pass


class Orchestrator:
    def __init__(
        self,
        strategies: dict[
            type[StrategyBase],
            tuple[list[Symbol], Models.RecordType],
        ],
        broker: type[BrokerBase],
        datafeed: type[DatafeedBase],
    ) -> None:
        self._strategies: dict[
            type[StrategyBase],
            tuple[list[Symbol], Models.RecordType],
        ] = strategies
        self._broker: type[BrokerBase] = broker
        self._datafeed: type[DatafeedBase] = datafeed

        self._run_id: RunId | None = None

    def run(self):
        self._run_id: RunId = self._generate_run_id()

    def _generate_run_id(self) -> RunId:
        timestamp = time.strftime("%Y-%m-%d-%H-%M-%S")
        strategy_names = "_".join(s.__name__ for s in self._strategies)
        return RunId(f"{timestamp}_{strategy_names}")
