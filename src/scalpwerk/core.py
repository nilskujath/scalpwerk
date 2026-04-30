import abc
import collections
import dataclasses
import enum
import logging
import pathlib
import queue
import signal
import sqlite3
import threading
import time
import typing
import uuid

logger = logging.getLogger(__name__)  # no handlers; falls back to `logging.lastResort`


class Types:
    # fmt: off
    UnixNanoseconds = typing.NewType("UnixNanoseconds", int      )
    ScaledPrice     = typing.NewType("ScaledPrice",     int      )
    Volume          = typing.NewType("Volume",          int      )
    Quantity        = typing.NewType("Quantity",        int      )
    FilledQuantity  = typing.NewType("FilledQuantity",  int      )
    PositionSize    = typing.NewType("PositionSize",    int      )
    IndicatorValue  = typing.NewType("IndicatorValue",  float    )
    IndicatorName   = typing.NewType("IndicatorName",   str      )
    Symbol          = typing.NewType("Symbol",          str      )
    InternalOrderId = typing.NewType("InternalOrderId", uuid.UUID)
    InternalFillId  = typing.NewType("InternalFillId",  uuid.UUID)
    BrokerOrderId   = typing.NewType("BrokerOrderId",   str      )
    BrokerFillId    = typing.NewType("BrokerFillId",    str      )
    RunId           = typing.NewType("RunId",           str      )
    # fmt: on


class Enums:
    # fmt: off
    class OrderType(enum.Enum):
        MARKET     = enum.auto()
        LIMIT      = enum.auto()
        STOP       = enum.auto()
        STOP_LIMIT = enum.auto()

    class TradeSide(enum.Enum):
        BUY  = enum.auto()
        SELL = enum.auto()

    class RecordType(enum.Enum):
        OHLCV_1S = 32  # values match DataBento's schema values for convenience
        OHLCV_1M = 33
        OHLCV_1H = 34
        OHLCV_1D = 35
    # fmt: on


class Protocols:
    class EventLike(typing.Protocol):
        @property  # for `mypy`, plain attribute would imply settable
        def occurred_at_ns(self) -> Types.UnixNanoseconds: ...
        @property
        def created_at_ns(self) -> Types.UnixNanoseconds: ...

    class SubscriberLike(typing.Protocol):
        def receive(self, event: "Protocols.EventLike") -> None: ...
        def wait_until_idle(self) -> None: ...
        @property
        def is_idle(self) -> bool: ...

    class EventBusLike(typing.Protocol):
        def subscribe(
            self,
            subscriber: "Protocols.SubscriberLike",
            *event_types: type["Protocols.EventLike"],
        ) -> None: ...
        def publish(self, event: "Protocols.EventLike") -> None: ...


class Events:
    # fmt: off
    class Datafeed:
        @dataclasses.dataclass(kw_only=True, frozen=True, slots=True)
        class Bar:
            occurred_at_ns:    Types.UnixNanoseconds
            created_at_ns:     Types.UnixNanoseconds
            symbol:            Types.Symbol
            record_type:       typing.Literal[
                                   Enums.RecordType.OHLCV_1S,
                                   Enums.RecordType.OHLCV_1M,
                                   Enums.RecordType.OHLCV_1H,
                                   Enums.RecordType.OHLCV_1D,
                               ] # Hedge against extension of `RecordType`.
            open:              Types.ScaledPrice
            high:              Types.ScaledPrice
            low:               Types.ScaledPrice
            close:             Types.ScaledPrice
            volume:            Types.Volume | None = None

    class Strategy:
        @dataclasses.dataclass(kw_only=True, frozen=True, slots=True)
        class IndicatorUpdate:
            occurred_at_ns:    Types.UnixNanoseconds
            created_at_ns:     Types.UnixNanoseconds
            symbol:            Types.Symbol
            source_event:      "Events.Datafeed.Bar"
            indicator_values:  dict[Types.IndicatorName, Types.IndicatorValue]

        @dataclasses.dataclass(kw_only=True, frozen=True, slots=True)
        class _OrderBase:
            occurred_at_ns:    Types.UnixNanoseconds
            created_at_ns:     Types.UnixNanoseconds
            internal_order_id: Types.InternalOrderId
            symbol:            Types.Symbol

        @dataclasses.dataclass(kw_only=True, frozen=True, slots=True)
        class SubmitOrder(_OrderBase):
            order_type:        Enums.OrderType
            side:              Enums.TradeSide
            quantity:          Types.Quantity
            limit_price:       Types.ScaledPrice | None = None
            stop_price:        Types.ScaledPrice | None = None

        @dataclasses.dataclass(kw_only=True, frozen=True, slots=True)
        class ModifyOrder(_OrderBase):
            quantity:          Types.Quantity
            limit_price:       Types.ScaledPrice | None = None
            stop_price:        Types.ScaledPrice | None = None

        @dataclasses.dataclass(kw_only=True, frozen=True, slots=True)
        class CancelOrder(_OrderBase):
            pass

    class Broker:
        @dataclasses.dataclass(kw_only=True, frozen=True, slots=True)
        class _Base:
            occurred_at_ns:    Types.UnixNanoseconds
            created_at_ns:     Types.UnixNanoseconds
            internal_order_id: Types.InternalOrderId
            broker_order_id:   Types.BrokerOrderId

        @dataclasses.dataclass(kw_only=True, frozen=True, slots=True)
        class CancellationAccepted(_Base):
            pass

        @dataclasses.dataclass(kw_only=True, frozen=True, slots=True)
        class CancellationRejected(_Base):
            reason:            str

        @dataclasses.dataclass(kw_only=True, frozen=True, slots=True)
        class ModificationAccepted(_Base):
            pass

        @dataclasses.dataclass(kw_only=True, frozen=True, slots=True)
        class ModificationRejected(_Base):
            reason:            str

        @dataclasses.dataclass(kw_only=True, frozen=True, slots=True)
        class OrderAccepted(_Base):
            pass

        # Standalone: `broker_order_id` is optional (rejection may arrive before the
        # broker assigns an ID).
        @dataclasses.dataclass(kw_only=True, frozen=True, slots=True)
        class OrderRejected:
            occurred_at_ns:    Types.UnixNanoseconds
            created_at_ns:     Types.UnixNanoseconds
            internal_order_id: Types.InternalOrderId
            broker_order_id:   Types.BrokerOrderId | None = None
            reason:            str

        # A single execution against an order. Does not indicate partial vs. full fill;
        # the system must track fill quantities to determine remaining open quantity.
        @dataclasses.dataclass(kw_only=True, frozen=True, slots=True)
        class Fill(_Base):
            symbol:            Types.Symbol
            internal_fill_id:  Types.InternalFillId
            broker_fill_id:    Types.BrokerFillId
            side:              Enums.TradeSide
            filled_quantity:   Types.Quantity
            fill_price:        Types.ScaledPrice
            exchange:          str
            commission:        Types.ScaledPrice = Types.ScaledPrice(0)

        @dataclasses.dataclass(kw_only=True, frozen=True, slots=True)
        class OrderExpired(_Base):
            pass
    # fmt: on


class EventBus:
    def __init__(self) -> None:
        self._per_event_subscriptions: collections.defaultdict[
            type[Protocols.EventLike], set[Protocols.SubscriberLike]
        ] = collections.defaultdict(set)
        self._lock: threading.Lock = threading.Lock()

    def subscribe(
        self,
        subscriber: Protocols.SubscriberLike,
        *event_types: type[Protocols.EventLike],  # `type` for classes, not instances
    ) -> None:
        with self._lock:
            for event_type in event_types:  # no duplication; subscribers are in set
                self._per_event_subscriptions[event_type].add(subscriber)

    def publish(self, event: Protocols.EventLike) -> None:
        with self._lock:
            subscribers = self._per_event_subscriptions[type(event)].copy()
        for subscriber in subscribers:
            subscriber.receive(event)


class _ComponentBase(abc.ABC):
    def __init__(self, event_bus: Protocols.EventBusLike) -> None:
        self._event_bus: Protocols.EventBusLike = event_bus


class _SubscriberBase(_ComponentBase, Protocols.SubscriberLike):
    def __init__(self, event_bus: Protocols.EventBusLike) -> None:
        super().__init__(event_bus)
        self._queue: queue.Queue[Protocols.EventLike | None] = queue.Queue()
        self._running: threading.Event = threading.Event()
        self._running.set()
        self._thread = threading.Thread(
            target=self._event_loop, name=self.__class__.__name__
        )
        self._thread.start()

    @property
    def is_idle(self) -> bool:  # non-blocking check
        if not self._running.is_set():
            return True
        return self._queue.unfinished_tasks == 0

    def wait_until_idle(self) -> None:  # blocking check, waits for `join()`
        if not self._running.is_set():
            return
        self._queue.join()

    def receive(self, event: Protocols.EventLike) -> None:
        if self._running.is_set():
            self._queue.put(event)

    def shutdown(self) -> None:
        if not self._running.is_set():
            return
        self._running.clear()  # no new events are received
        self._queue.put(None)  # `None` will be last element in the closed queue
        self._thread.join()

    def _event_loop(self) -> None:
        while True:
            event = self._queue.get()
            if event is None:  # `None` is poison pill event
                self._queue.task_done()  # for `queue.join()` in `wait_until_idle`
                break
            self._on_event(event)
            self._queue.task_done()

    @abc.abstractmethod
    def _on_event(self, event: Protocols.EventLike) -> None:
        pass


# `T` is filled in when a subclass specifies its allowed event types in brackets,
# e.g. `_EmitterBase[Events.Datafeed.Bar]` — mypy then restricts `_emit_event`
# to only accept that type. `T: Protocols.EventLike` means only types satisfying
# `Protocols.EventLike` are allowed.
class _EmitterBase[T: Protocols.EventLike](_ComponentBase):
    def _emit_event(self, event: T) -> None:
        self._event_bus.publish(event)


class ComponentBases:
    # Mixin for components that need to manage an external connection lifecycle.
    class ExternalMixin(abc.ABC):
        @abc.abstractmethod
        def connect(self) -> None:
            pass

        @abc.abstractmethod
        def disconnect(self) -> None:
            pass

    class BrokerBase(
        ExternalMixin,
        _SubscriberBase,
        _EmitterBase[
            Events.Broker.OrderAccepted
            | Events.Broker.OrderRejected
            | Events.Broker.ModificationAccepted
            | Events.Broker.ModificationRejected
            | Events.Broker.CancellationAccepted
            | Events.Broker.CancellationRejected
            | Events.Broker.Fill
            | Events.Broker.OrderExpired
        ],
    ):
        def __init__(self, event_bus: Protocols.EventBusLike) -> None:
            super().__init__(event_bus)

            self._event_bus.subscribe(
                self,
                Events.Strategy.SubmitOrder,
                Events.Strategy.ModifyOrder,
                Events.Strategy.CancelOrder,
            )

        def _on_event(self, event: Protocols.EventLike) -> None:
            match event:
                case Events.Strategy.SubmitOrder() as e:
                    self._on_submit_order(e)
                case Events.Strategy.ModifyOrder() as e:
                    self._on_modify_order(e)
                case Events.Strategy.CancelOrder() as e:
                    self._on_cancel_order(e)
                case _:
                    return

        # Called by StrategyBase.setup() to initialize position state from the broker
        # account. The broker is the source of truth — no need to persist positions
        # in the system.
        @abc.abstractmethod
        def get_positions(
            self,
        ) -> dict[Types.Symbol, Types.PositionSize]:
            pass

        @abc.abstractmethod
        def _on_submit_order(self, event: Events.Strategy.SubmitOrder) -> None:
            pass

        @abc.abstractmethod
        def _on_modify_order(self, event: Events.Strategy.ModifyOrder) -> None:
            pass

        @abc.abstractmethod
        def _on_cancel_order(self, event: Events.Strategy.CancelOrder) -> None:
            pass

    class DatafeedBase(
        ExternalMixin,
        _EmitterBase[Events.Datafeed.Bar],
    ):
        def __init__(
            self,
            event_bus: Protocols.EventBusLike,
            on_fatal: typing.Callable[[], None],
            on_bar_emitted: typing.Callable[[], None] = lambda: None,
        ) -> None:
            super().__init__(event_bus)
            self._on_fatal = on_fatal
            self._on_bar_emitted = on_bar_emitted

        @abc.abstractmethod
        def subscribe(
            self,
            symbols: list[Types.Symbol],
            record_type: Enums.RecordType,
        ) -> None:
            pass

        @abc.abstractmethod
        def unsubscribe(
            self,
            symbols: list[Types.Symbol],
            record_type: Enums.RecordType,
        ) -> None:
            pass

    # Computes and stores a single scalar value per symbol on each `update` call.
    # Complex indicators must be split into separate instances, e.g. `BollingerUpper` et
    # cetera. Indicators are bar-driven: values are only computed when a new bar
    # arrives. On illiquid assets, be mindful that gaps may cause unexpected indicator
    # behavior.
    class IndicatorBase(abc.ABC):
        def __init__(self, max_history: int = 100) -> None:
            self._max_history = max(1, int(max_history))
            # Per-symbol bounded FIFO buffers. The bounded size ensures memory stays
            # predictable since indicators only need a finite lookback window.
            self._history: dict[
                Types.Symbol,
                collections.deque[Types.IndicatorValue],
            ] = {}

        # The name should be defined via an f-string so that instances of the same
        # indicator can be distinguished via their parameters, e.g. SMAs with different
        # window size.
        @property
        @abc.abstractmethod
        def name(self) -> Types.IndicatorName:
            pass

        def update(self, event: Events.Datafeed.Bar) -> None:
            value = self._compute(event)
            symbol = event.symbol
            if symbol not in self._history:
                self._history[symbol] = collections.deque(maxlen=self._max_history)
            self._history[symbol].append(value)

        @abc.abstractmethod
        def _compute(self, event: Events.Datafeed.Bar) -> Types.IndicatorValue:
            pass

        def latest(self, symbol: Types.Symbol) -> Types.IndicatorValue:
            return self[symbol, -1]

        # Supports standard negative indexing, e.g. `indicator["AAPL", -2]`.
        def __getitem__(self, key: tuple[Types.Symbol, int]) -> Types.IndicatorValue:
            symbol, index = key
            history = self._history.get(symbol)
            if history is None:
                return Types.IndicatorValue(float("nan"))
            try:
                return history[index]
            except IndexError:
                return Types.IndicatorValue(float("nan"))

    @dataclasses.dataclass(frozen=True, slots=True)
    class _PendingOrder:
        order: Events.Strategy.SubmitOrder
        filled_quantity: Types.FilledQuantity

    class StrategyBase(
        _SubscriberBase,
        _EmitterBase[
            Events.Strategy.SubmitOrder
            | Events.Strategy.ModifyOrder
            | Events.Strategy.CancelOrder
            | Events.Strategy.IndicatorUpdate
        ],
    ):
        def __init__(
            self,
            event_bus: Protocols.EventBusLike,
            symbols: list[Types.Symbol],
            record_type: Enums.RecordType,
        ) -> None:
            super().__init__(event_bus)

            self._symbols: set[Types.Symbol] = set(symbols)
            self._record_type: Enums.RecordType = record_type

            self._event_bus.subscribe(
                self,
                Events.Datafeed.Bar,
                Events.Broker.OrderAccepted,
                Events.Broker.OrderRejected,
                Events.Broker.ModificationAccepted,
                Events.Broker.ModificationRejected,
                Events.Broker.CancellationAccepted,
                Events.Broker.CancellationRejected,
                Events.Broker.Fill,
                Events.Broker.OrderExpired,
            )

            self._current_symbol: Types.Symbol | None = None
            self._current_event_ns: Types.UnixNanoseconds | None = None

            self._indicators: dict[
                Types.IndicatorName, "ComponentBases.IndicatorBase"
            ] = {}

            # `_submitted*` dicts store in-flight requests awaiting broker
            # acknowledgement. Orders move to `_pending_orders` on acceptance or are
            # removed on rejection.
            self._submitted_orders: dict[
                Types.InternalOrderId,
                Events.Strategy.SubmitOrder,
            ] = {}
            self._submitted_modifications: dict[
                Types.InternalOrderId,
                Events.Strategy.ModifyOrder,
            ] = {}
            self._submitted_cancellations: dict[
                Types.InternalOrderId,
                Events.Strategy.CancelOrder,
            ] = {}

            # Tracks accepted orders and their cumulative filled quantity. Orders leave
            # this dict on full fill, cancellation, or expiry.
            self._pending_orders: dict[
                Types.InternalOrderId, "ComponentBases._PendingOrder"
            ] = {}

            self._position_sizes: dict[
                Types.Symbol,
                Types.PositionSize,
            ] = {}
            self._average_entry_prices: dict[Types.Symbol, Types.ScaledPrice] = {}

            # Must be last so base class state exists before the subclass's `setup()`
            # runs.
            self.setup()

        # Subclasses implement `setup()` instead of `__init__` to avoid
        # `super().__init__`.
        @abc.abstractmethod
        def setup(self) -> None:
            pass

        def add_indicator(
            self, indicator: "ComponentBases.IndicatorBase"
        ) -> "ComponentBases.IndicatorBase":
            if indicator.name in self._indicators:
                raise ValueError(
                    f"Indicator with name '{indicator.name}' is already registered."
                )
            self._indicators[indicator.name] = indicator
            # Returns indicator for inline assignment:
            # `self.sma = self.add_indicator(...)`.
            return indicator

        def _on_event(self, event: Protocols.EventLike) -> None:
            match event:
                case Events.Datafeed.Bar() as e:
                    self._on_market_update(e)
                case Events.Broker.OrderAccepted() as e:
                    self._on_order_accepted(e)
                case Events.Broker.OrderRejected() as e:
                    self._on_order_rejected(e)
                case Events.Broker.ModificationAccepted() as e:
                    self._on_modification_accepted(e)
                case Events.Broker.ModificationRejected() as e:
                    self._on_modification_rejected(e)
                case Events.Broker.CancellationAccepted() as e:
                    self._on_cancellation_accepted(e)
                case Events.Broker.CancellationRejected() as e:
                    self._on_cancellation_rejected(e)
                case Events.Broker.Fill() as e:
                    self._on_fill(e)
                case Events.Broker.OrderExpired() as e:
                    self._on_order_expired(e)
                case _:
                    return

        # Wraps the abstract `on_market_update` with internal plumbing.
        def _on_market_update(self, event: Events.Datafeed.Bar) -> None:
            if (
                event.record_type != self._record_type
                or event.symbol not in self._symbols
            ):
                return
            self._current_symbol = event.symbol
            self._current_event_ns = event.occurred_at_ns
            for indicator in self._indicators.values():
                indicator.update(event)
            self.on_market_update(event)
            # Emitted after `on_market_update` so strategy logic isn't delayed by
            # emission.
            self._emit_indicator_update(event)

        @abc.abstractmethod
        def on_market_update(self, event: Events.Datafeed.Bar) -> None:
            pass

        def _emit_indicator_update(self, source_event: Events.Datafeed.Bar) -> None:
            if not self._indicators:
                return

            assert self._current_symbol is not None
            assert self._current_event_ns is not None

            indicator_values = {
                name: indicator.latest(self._current_symbol)
                for name, indicator in self._indicators.items()
            }

            self._emit_event(
                Events.Strategy.IndicatorUpdate(
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
        def position_size(self) -> Types.PositionSize:
            assert self._current_symbol is not None
            return self._position_sizes.get(self._current_symbol, Types.PositionSize(0))

        @property
        def flat(self) -> bool:
            return self.position_size == 0

        @property
        def average_entry_price(self) -> Types.ScaledPrice | None:
            assert self._current_symbol is not None
            return self._average_entry_prices.get(self._current_symbol)

        def submit_order(
            self,
            symbol: Types.Symbol,
            order_type: Enums.OrderType,
            side: Enums.TradeSide,
            quantity: Types.Quantity,
            limit_price: Types.ScaledPrice | None = None,
            stop_price: Types.ScaledPrice | None = None,
        ) -> Types.InternalOrderId:
            assert self._current_event_ns is not None

            internal_order_id = Types.InternalOrderId(uuid.uuid4())

            event = Events.Strategy.SubmitOrder(
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
            internal_order_id: Types.InternalOrderId,
            quantity: Types.Quantity,
            limit_price: Types.ScaledPrice | None = None,
            stop_price: Types.ScaledPrice | None = None,
        ) -> bool:
            assert self._current_event_ns is not None

            pending = self._pending_orders.get(internal_order_id)
            if pending is None:
                return False

            event = Events.Strategy.ModifyOrder(
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
        def submit_cancellation(self, internal_order_id: Types.InternalOrderId) -> bool:
            assert self._current_event_ns is not None

            pending = self._pending_orders.get(internal_order_id)
            if pending is None:
                return False

            event = Events.Strategy.CancelOrder(
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
        def _on_order_accepted(self, event: Events.Broker.OrderAccepted) -> None:
            order = self._submitted_orders.pop(event.internal_order_id, None)
            if order is not None:
                self._pending_orders[event.internal_order_id] = (
                    ComponentBases._PendingOrder(order, Types.FilledQuantity(0))
                )

        def _on_order_rejected(self, event: Events.Broker.OrderRejected) -> None:
            self._submitted_orders.pop(event.internal_order_id, None)

        def _on_modification_accepted(
            self, event: Events.Broker.ModificationAccepted
        ) -> None:
            modification = self._submitted_modifications.pop(
                event.internal_order_id, None
            )
            if modification is not None:
                pending = self._pending_orders.get(event.internal_order_id)
                if pending is not None:
                    updated_order = Events.Strategy.SubmitOrder(
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
                    # Check necessary because a quantity reduction may retroactively
                    # fully fill the order if fills arrived while the modification was
                    # in-flight.
                    if pending.filled_quantity >= modification.quantity:
                        self._pending_orders.pop(event.internal_order_id)
                    else:
                        self._pending_orders[event.internal_order_id] = (
                            ComponentBases._PendingOrder(
                                updated_order,
                                pending.filled_quantity,
                            )
                        )

        def _on_modification_rejected(
            self, event: Events.Broker.ModificationRejected
        ) -> None:
            self._submitted_modifications.pop(event.internal_order_id, None)

        def _on_cancellation_accepted(
            self, event: Events.Broker.CancellationAccepted
        ) -> None:
            self._submitted_cancellations.pop(event.internal_order_id, None)
            self._pending_orders.pop(event.internal_order_id, None)
            # In-flight modifications will not get a response after cancellation.
            self._submitted_modifications.pop(event.internal_order_id, None)

        def _on_cancellation_rejected(
            self, event: Events.Broker.CancellationRejected
        ) -> None:
            self._submitted_cancellations.pop(event.internal_order_id, None)

        def _on_fill(self, event: Events.Broker.Fill) -> None:
            self._update_position_size_and_avg_entry_price(event)
            self._update_pending_orders(event)

        def _update_pending_orders(self, event: Events.Broker.Fill) -> None:
            pending = self._pending_orders.get(event.internal_order_id)
            # The order may already be gone (prior fill, cancellation, or expiry).
            if pending is None:
                return
            new_filled_quantity = Types.FilledQuantity(
                pending.filled_quantity + event.filled_quantity
            )
            if new_filled_quantity >= pending.order.quantity:
                self._pending_orders.pop(event.internal_order_id)
            else:
                self._pending_orders[event.internal_order_id] = (
                    ComponentBases._PendingOrder(pending.order, new_filled_quantity)
                )

        def _update_position_size_and_avg_entry_price(
            self, event: Events.Broker.Fill
        ) -> None:
            signed_quantity = (
                event.filled_quantity
                if event.side == Enums.TradeSide.BUY
                else -event.filled_quantity
            )

            # Default 0 so the first fill is handled as a fresh entry below.
            old_position = self._position_sizes.get(event.symbol, Types.PositionSize(0))
            old_avg_entry_price = self._average_entry_prices.get(
                event.symbol, Types.ScaledPrice(0)
            )
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
                new_avg_entry_price = Types.ScaledPrice(
                    (
                        old_avg_entry_price * abs(old_position)
                        + event.fill_price * abs(signed_quantity)
                    )
                    // abs(new_position)
                )
            # Partial close: avg unchanged, remaining shares entered at old avg.
            else:
                new_avg_entry_price = Types.ScaledPrice(old_avg_entry_price)

            self._position_sizes[event.symbol] = Types.PositionSize(new_position)
            self._average_entry_prices[event.symbol] = Types.ScaledPrice(
                new_avg_entry_price
            )

        def _on_order_expired(self, event: Events.Broker.OrderExpired) -> None:
            self._pending_orders.pop(event.internal_order_id, None)
            # The order is dead, so any in-flight modification or cancellation will never
            # get a response. Clean up to prevent leaks.
            self._submitted_modifications.pop(event.internal_order_id, None)
            self._submitted_cancellations.pop(event.internal_order_id, None)

    class RunRecorder(_SubscriberBase):
        _SCHEMA_VERSION = 2

        _RUN_RECORDER_SCHEMA = """
            CREATE TABLE IF NOT EXISTS schema_version (
                version INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS runs (
                run_id TEXT NOT NULL PRIMARY KEY
            );

            CREATE TABLE IF NOT EXISTS run_strategies (
                run_id      TEXT NOT NULL REFERENCES runs(run_id),
                strategy    TEXT NOT NULL,
                symbol      TEXT NOT NULL,
                record_type TEXT NOT NULL,
                PRIMARY KEY (run_id, strategy, symbol, record_type)
            );

            CREATE TABLE IF NOT EXISTS ohlcv (
                run_id         TEXT    NOT NULL REFERENCES runs(run_id),
                occurred_at_ns INTEGER NOT NULL,
                created_at_ns  INTEGER NOT NULL,
                symbol         TEXT    NOT NULL,
                record_type    TEXT    NOT NULL,
                open           INTEGER NOT NULL,
                high           INTEGER NOT NULL,
                low            INTEGER NOT NULL,
                close          INTEGER NOT NULL,
                volume         INTEGER,
                PRIMARY KEY (run_id, symbol, record_type, occurred_at_ns)
            );

            CREATE TABLE IF NOT EXISTS indicator_values (
                run_id          TEXT    NOT NULL,
                symbol          TEXT    NOT NULL,
                record_type     TEXT    NOT NULL,
                occurred_at_ns  INTEGER NOT NULL,
                indicator_name  TEXT    NOT NULL,
                indicator_value REAL   NOT NULL,
                PRIMARY KEY (run_id, symbol, record_type, occurred_at_ns, indicator_name),
                FOREIGN KEY (run_id, symbol, record_type, occurred_at_ns)
                    REFERENCES ohlcv(run_id, symbol, record_type, occurred_at_ns)
            );

            CREATE TABLE IF NOT EXISTS broker_request_submit_order (
                run_id            TEXT    NOT NULL REFERENCES runs(run_id),
                occurred_at_ns    INTEGER NOT NULL,
                created_at_ns     INTEGER NOT NULL,
                internal_order_id TEXT    NOT NULL,
                symbol            TEXT    NOT NULL,
                order_type        TEXT    NOT NULL,
                side              TEXT    NOT NULL,
                quantity          INTEGER NOT NULL,
                limit_price       INTEGER,
                stop_price        INTEGER,
                PRIMARY KEY (run_id, internal_order_id)
            );

            CREATE TABLE IF NOT EXISTS broker_request_modify_order (
                run_id            TEXT    NOT NULL REFERENCES runs(run_id),
                occurred_at_ns    INTEGER NOT NULL,
                created_at_ns     INTEGER NOT NULL,
                internal_order_id TEXT    NOT NULL,
                symbol            TEXT    NOT NULL,
                quantity          INTEGER NOT NULL,
                limit_price       INTEGER,
                stop_price        INTEGER,
                PRIMARY KEY (run_id, internal_order_id, occurred_at_ns)
            );

            CREATE TABLE IF NOT EXISTS broker_request_cancel_order (
                run_id            TEXT    NOT NULL REFERENCES runs(run_id),
                occurred_at_ns    INTEGER NOT NULL,
                created_at_ns     INTEGER NOT NULL,
                internal_order_id TEXT    NOT NULL,
                symbol            TEXT    NOT NULL,
                PRIMARY KEY (run_id, internal_order_id, occurred_at_ns)
            );

            CREATE TABLE IF NOT EXISTS broker_response_order_accepted (
                run_id            TEXT    NOT NULL REFERENCES runs(run_id),
                occurred_at_ns    INTEGER NOT NULL,
                created_at_ns     INTEGER NOT NULL,
                internal_order_id TEXT    NOT NULL,
                broker_order_id   TEXT    NOT NULL,
                PRIMARY KEY (run_id, internal_order_id)
            );

            CREATE TABLE IF NOT EXISTS broker_response_order_rejected (
                run_id            TEXT    NOT NULL REFERENCES runs(run_id),
                occurred_at_ns    INTEGER NOT NULL,
                created_at_ns     INTEGER NOT NULL,
                internal_order_id TEXT    NOT NULL,
                broker_order_id   TEXT,
                reason            TEXT    NOT NULL,
                PRIMARY KEY (run_id, internal_order_id)
            );

            CREATE TABLE IF NOT EXISTS broker_response_modification_accepted (
                run_id            TEXT    NOT NULL REFERENCES runs(run_id),
                occurred_at_ns    INTEGER NOT NULL,
                created_at_ns     INTEGER NOT NULL,
                internal_order_id TEXT    NOT NULL,
                broker_order_id   TEXT    NOT NULL,
                PRIMARY KEY (run_id, internal_order_id, occurred_at_ns)
            );

            CREATE TABLE IF NOT EXISTS broker_response_modification_rejected (
                run_id            TEXT    NOT NULL REFERENCES runs(run_id),
                occurred_at_ns    INTEGER NOT NULL,
                created_at_ns     INTEGER NOT NULL,
                internal_order_id TEXT    NOT NULL,
                broker_order_id   TEXT    NOT NULL,
                reason            TEXT    NOT NULL,
                PRIMARY KEY (run_id, internal_order_id, occurred_at_ns)
            );

            CREATE TABLE IF NOT EXISTS broker_response_cancellation_accepted (
                run_id            TEXT    NOT NULL REFERENCES runs(run_id),
                occurred_at_ns    INTEGER NOT NULL,
                created_at_ns     INTEGER NOT NULL,
                internal_order_id TEXT    NOT NULL,
                broker_order_id   TEXT    NOT NULL,
                PRIMARY KEY (run_id, internal_order_id, occurred_at_ns)
            );

            CREATE TABLE IF NOT EXISTS broker_response_cancellation_rejected (
                run_id            TEXT    NOT NULL REFERENCES runs(run_id),
                occurred_at_ns    INTEGER NOT NULL,
                created_at_ns     INTEGER NOT NULL,
                internal_order_id TEXT    NOT NULL,
                broker_order_id   TEXT    NOT NULL,
                reason            TEXT    NOT NULL,
                PRIMARY KEY (run_id, internal_order_id, occurred_at_ns)
            );

            CREATE TABLE IF NOT EXISTS broker_response_fill (
                run_id            TEXT    NOT NULL REFERENCES runs(run_id),
                occurred_at_ns    INTEGER NOT NULL,
                created_at_ns     INTEGER NOT NULL,
                internal_order_id TEXT    NOT NULL,
                broker_order_id   TEXT    NOT NULL,
                symbol            TEXT    NOT NULL,
                internal_fill_id  TEXT    NOT NULL,
                broker_fill_id    TEXT    NOT NULL,
                side              TEXT    NOT NULL,
                filled_quantity   INTEGER NOT NULL,
                fill_price        INTEGER NOT NULL,
                exchange          TEXT    NOT NULL,
                commission        INTEGER NOT NULL,
                PRIMARY KEY (run_id, internal_fill_id)
            );

            CREATE TABLE IF NOT EXISTS broker_response_order_expired (
                run_id            TEXT    NOT NULL REFERENCES runs(run_id),
                occurred_at_ns    INTEGER NOT NULL,
                created_at_ns     INTEGER NOT NULL,
                internal_order_id TEXT    NOT NULL,
                broker_order_id   TEXT    NOT NULL,
                PRIMARY KEY (run_id, internal_order_id)
            );
        """

        _SQL_INSERT_OHLCV = """
            INSERT INTO ohlcv
                (run_id, occurred_at_ns, created_at_ns, symbol, record_type,
                 open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        _SQL_INSERT_INDICATOR_VALUE = """
            INSERT INTO indicator_values
                (run_id, symbol, record_type, occurred_at_ns,
                 indicator_name, indicator_value)
            VALUES (?, ?, ?, ?, ?, ?)
        """

        _SQL_INSERT_SUBMIT_ORDER = """
            INSERT INTO broker_request_submit_order
                (run_id, occurred_at_ns, created_at_ns, internal_order_id, symbol,
                 order_type, side, quantity, limit_price, stop_price)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        _SQL_INSERT_MODIFY_ORDER = """
            INSERT INTO broker_request_modify_order
                (run_id, occurred_at_ns, created_at_ns, internal_order_id, symbol,
                 quantity, limit_price, stop_price)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """

        _SQL_INSERT_CANCEL_ORDER = """
            INSERT INTO broker_request_cancel_order
                (run_id, occurred_at_ns, created_at_ns, internal_order_id, symbol)
            VALUES (?, ?, ?, ?, ?)
        """

        _SQL_INSERT_ORDER_ACCEPTED = """
            INSERT INTO broker_response_order_accepted
                (run_id, occurred_at_ns, created_at_ns, internal_order_id,
                 broker_order_id)
            VALUES (?, ?, ?, ?, ?)
        """

        _SQL_INSERT_ORDER_REJECTED = """
            INSERT INTO broker_response_order_rejected
                (run_id, occurred_at_ns, created_at_ns, internal_order_id,
                 broker_order_id, reason)
            VALUES (?, ?, ?, ?, ?, ?)
        """

        _SQL_INSERT_MODIFICATION_ACCEPTED = """
            INSERT INTO broker_response_modification_accepted
                (run_id, occurred_at_ns, created_at_ns, internal_order_id,
                 broker_order_id)
            VALUES (?, ?, ?, ?, ?)
        """

        _SQL_INSERT_MODIFICATION_REJECTED = """
            INSERT INTO broker_response_modification_rejected
                (run_id, occurred_at_ns, created_at_ns, internal_order_id,
                 broker_order_id, reason)
            VALUES (?, ?, ?, ?, ?, ?)
        """

        _SQL_INSERT_CANCELLATION_ACCEPTED = """
            INSERT INTO broker_response_cancellation_accepted
                (run_id, occurred_at_ns, created_at_ns, internal_order_id,
                 broker_order_id)
            VALUES (?, ?, ?, ?, ?)
        """

        _SQL_INSERT_CANCELLATION_REJECTED = """
            INSERT INTO broker_response_cancellation_rejected
                (run_id, occurred_at_ns, created_at_ns, internal_order_id,
                 broker_order_id, reason)
            VALUES (?, ?, ?, ?, ?, ?)
        """

        _SQL_INSERT_FILL = """
            INSERT INTO broker_response_fill
                (run_id, occurred_at_ns, created_at_ns, internal_order_id,
                 broker_order_id, symbol, internal_fill_id, broker_fill_id,
                 side, filled_quantity, fill_price, exchange, commission)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        _SQL_INSERT_ORDER_EXPIRED = """
            INSERT INTO broker_response_order_expired
                (run_id, occurred_at_ns, created_at_ns, internal_order_id,
                 broker_order_id)
            VALUES (?, ?, ?, ?, ?)
        """

        _SQL_CHECK_SCHEMA_VERSION_TABLE_EXISTS = """
            SELECT name FROM sqlite_master
            WHERE type='table' AND name='schema_version'
        """

        _SQL_SELECT_SCHEMA_VERSION = """
            SELECT version FROM schema_version
        """

        _SQL_INSERT_SCHEMA_VERSION = """
            INSERT INTO schema_version (version) VALUES (?)
        """

        _SQL_INSERT_RUN = """
            INSERT INTO runs (run_id) VALUES (?)
        """

        _SQL_INSERT_RUN_STRATEGY = """
            INSERT INTO run_strategies (run_id, strategy, symbol, record_type)
            VALUES (?, ?, ?, ?)
        """

        def __init__(
            self,
            event_bus: Protocols.EventBusLike,
            runs_db_path: pathlib.Path,
            run_id: Types.RunId,
            strategies: dict[
                type["ComponentBases.StrategyBase"],
                tuple[Enums.RecordType, list[Types.Symbol]],
            ],
        ) -> None:
            super().__init__(event_bus)
            self._runs_db_path: pathlib.Path = pathlib.Path(runs_db_path)
            self._run_id: Types.RunId = Types.RunId(run_id)
            self._strategies: dict[
                type["ComponentBases.StrategyBase"],
                tuple[Enums.RecordType, list[Types.Symbol]],
            ] = strategies

            self._conn: sqlite3.Connection | None = None

            self._event_bus.subscribe(
                self,
                Events.Strategy.IndicatorUpdate,
                Events.Strategy.SubmitOrder,
                Events.Strategy.ModifyOrder,
                Events.Strategy.CancelOrder,
                Events.Broker.OrderAccepted,
                Events.Broker.OrderRejected,
                Events.Broker.ModificationAccepted,
                Events.Broker.ModificationRejected,
                Events.Broker.CancellationAccepted,
                Events.Broker.CancellationRejected,
                Events.Broker.Fill,
                Events.Broker.OrderExpired,
            )

        def _setup_db(self) -> None:
            self._conn = sqlite3.connect(self._runs_db_path)

            self._conn.execute("PRAGMA journal_mode = WAL")
            self._conn.execute("PRAGMA synchronous = NORMAL")
            self._conn.execute("PRAGMA foreign_keys = ON")

            if self._conn.execute(
                self._SQL_CHECK_SCHEMA_VERSION_TABLE_EXISTS
            ).fetchone():
                row = self._conn.execute(self._SQL_SELECT_SCHEMA_VERSION).fetchone()
                version = row[0] if row else None
                if version != self._SCHEMA_VERSION:
                    raise RuntimeError(
                        f"Schema version mismatch: expected {self._SCHEMA_VERSION},"
                        f" found {version}"
                    )
            else:
                self._conn.executescript(self._RUN_RECORDER_SCHEMA)
                self._conn.execute(
                    self._SQL_INSERT_SCHEMA_VERSION, (self._SCHEMA_VERSION,)
                )

            self._conn.execute(self._SQL_INSERT_RUN, (str(self._run_id),))
            self._conn.executemany(
                self._SQL_INSERT_RUN_STRATEGY,
                [
                    (
                        str(self._run_id),
                        strategy.__name__,
                        str(symbol),
                        record_type.name,
                    )
                    for strategy, (record_type, symbols) in self._strategies.items()
                    for symbol in symbols
                ],
            )
            self._conn.commit()

        def _event_loop(self) -> None:
            # Called here instead of __init__ so the SQLite connection is created on
            # the same thread that will use it (SQLite connections are not thread-safe).
            self._setup_db()

            try:
                super()._event_loop()
            finally:
                if self._conn is not None:
                    self._conn.close()

        def _on_event(self, event: Protocols.EventLike) -> None:
            assert self._conn is not None  # for type checker; always set by _setup_db

            try:
                self._record(event)
            except Exception as exc:
                logger.warning("rolling back transaction: %s", exc)
                self._conn.rollback()

        def _record(self, event: Protocols.EventLike) -> None:
            assert self._conn is not None

            match event:
                case Events.Strategy.IndicatorUpdate() as e:
                    src = e.source_event
                    self._conn.execute(
                        self._SQL_INSERT_OHLCV,
                        (
                            str(self._run_id),
                            int(src.occurred_at_ns),
                            int(src.created_at_ns),
                            str(src.symbol),
                            src.record_type.name,
                            int(src.open),
                            int(src.high),
                            int(src.low),
                            int(src.close),
                            int(src.volume) if src.volume is not None else None,
                        ),
                    )
                    self._conn.executemany(
                        self._SQL_INSERT_INDICATOR_VALUE,
                        [
                            (
                                str(self._run_id),
                                str(src.symbol),
                                src.record_type.name,
                                int(src.occurred_at_ns),
                                str(name),
                                float(value),
                            )
                            for name, value in e.indicator_values.items()
                        ],
                    )

                case Events.Strategy.SubmitOrder() as e:
                    self._conn.execute(
                        self._SQL_INSERT_SUBMIT_ORDER,
                        (
                            str(self._run_id),
                            int(e.occurred_at_ns),
                            int(e.created_at_ns),
                            str(e.internal_order_id),
                            str(e.symbol),
                            e.order_type.name,
                            e.side.name,
                            int(e.quantity),
                            int(e.limit_price) if e.limit_price is not None else None,
                            int(e.stop_price) if e.stop_price is not None else None,
                        ),
                    )

                case Events.Strategy.ModifyOrder() as e:
                    self._conn.execute(
                        self._SQL_INSERT_MODIFY_ORDER,
                        (
                            str(self._run_id),
                            int(e.occurred_at_ns),
                            int(e.created_at_ns),
                            str(e.internal_order_id),
                            str(e.symbol),
                            int(e.quantity),
                            int(e.limit_price) if e.limit_price is not None else None,
                            int(e.stop_price) if e.stop_price is not None else None,
                        ),
                    )

                case Events.Strategy.CancelOrder() as e:
                    self._conn.execute(
                        self._SQL_INSERT_CANCEL_ORDER,
                        (
                            str(self._run_id),
                            int(e.occurred_at_ns),
                            int(e.created_at_ns),
                            str(e.internal_order_id),
                            str(e.symbol),
                        ),
                    )

                case Events.Broker.OrderAccepted() as e:
                    self._conn.execute(
                        self._SQL_INSERT_ORDER_ACCEPTED,
                        (
                            str(self._run_id),
                            int(e.occurred_at_ns),
                            int(e.created_at_ns),
                            str(e.internal_order_id),
                            str(e.broker_order_id),
                        ),
                    )

                case Events.Broker.OrderRejected() as e:
                    self._conn.execute(
                        self._SQL_INSERT_ORDER_REJECTED,
                        (
                            str(self._run_id),
                            int(e.occurred_at_ns),
                            int(e.created_at_ns),
                            str(e.internal_order_id),
                            (
                                str(e.broker_order_id)
                                if e.broker_order_id is not None
                                else None
                            ),
                            e.reason,
                        ),
                    )

                case Events.Broker.ModificationAccepted() as e:
                    self._conn.execute(
                        self._SQL_INSERT_MODIFICATION_ACCEPTED,
                        (
                            str(self._run_id),
                            int(e.occurred_at_ns),
                            int(e.created_at_ns),
                            str(e.internal_order_id),
                            str(e.broker_order_id),
                        ),
                    )

                case Events.Broker.ModificationRejected() as e:
                    self._conn.execute(
                        self._SQL_INSERT_MODIFICATION_REJECTED,
                        (
                            str(self._run_id),
                            int(e.occurred_at_ns),
                            int(e.created_at_ns),
                            str(e.internal_order_id),
                            str(e.broker_order_id),
                            e.reason,
                        ),
                    )

                case Events.Broker.CancellationAccepted() as e:
                    self._conn.execute(
                        self._SQL_INSERT_CANCELLATION_ACCEPTED,
                        (
                            str(self._run_id),
                            int(e.occurred_at_ns),
                            int(e.created_at_ns),
                            str(e.internal_order_id),
                            str(e.broker_order_id),
                        ),
                    )

                case Events.Broker.CancellationRejected() as e:
                    self._conn.execute(
                        self._SQL_INSERT_CANCELLATION_REJECTED,
                        (
                            str(self._run_id),
                            int(e.occurred_at_ns),
                            int(e.created_at_ns),
                            str(e.internal_order_id),
                            str(e.broker_order_id),
                            e.reason,
                        ),
                    )

                case Events.Broker.Fill() as e:
                    self._conn.execute(
                        self._SQL_INSERT_FILL,
                        (
                            str(self._run_id),
                            int(e.occurred_at_ns),
                            int(e.created_at_ns),
                            str(e.internal_order_id),
                            str(e.broker_order_id),
                            str(e.symbol),
                            str(e.internal_fill_id),
                            str(e.broker_fill_id),
                            e.side.name,
                            int(e.filled_quantity),
                            int(e.fill_price),
                            e.exchange,
                            int(e.commission),
                        ),
                    )

                case Events.Broker.OrderExpired() as e:
                    self._conn.execute(
                        self._SQL_INSERT_ORDER_EXPIRED,
                        (
                            str(self._run_id),
                            int(e.occurred_at_ns),
                            int(e.created_at_ns),
                            str(e.internal_order_id),
                            str(e.broker_order_id),
                        ),
                    )

                case _:
                    # Unrecognized event; `return` skips the `commit()` below.
                    return

            self._conn.commit()


@dataclasses.dataclass
class _RunInstances:
    event_bus: Protocols.EventBusLike | None = None
    run_recorder: ComponentBases.RunRecorder | None = None
    broker: ComponentBases.BrokerBase | None = None
    strategies: list[ComponentBases.StrategyBase] = dataclasses.field(
        default_factory=list
    )
    datafeed: ComponentBases.DatafeedBase | None = None

    @property
    def has_run_started(self) -> bool:
        return (
            self.event_bus is not None
            or self.run_recorder is not None
            or self.broker is not None
            or len(self.strategies) > 0
            or self.datafeed is not None
        )


class RunOrchestrator:
    def __init__(
        self,
        strategies: dict[
            type[ComponentBases.StrategyBase],
            tuple[Enums.RecordType, list[Types.Symbol]],
        ],
        broker: type[ComponentBases.BrokerBase],
        datafeed: type[ComponentBases.DatafeedBase],
        runs_db_path: pathlib.Path = pathlib.Path("runs.db"),
    ) -> None:
        self._strategies = strategies
        self._broker_class = broker
        self._datafeed_class = datafeed
        self._runs_db_path = runs_db_path

        self._instances = _RunInstances()
        self._shutdown_event = threading.Event()

    def run(self) -> None:
        if self._instances.has_run_started:
            return

        def _handle_sigterm(sig: int, frame: typing.Any) -> None:
            logger.info("SIGTERM received, shutting down")
            self._shutdown_event.set()

        signal.signal(signal.SIGTERM, _handle_sigterm)
        try:
            self._setup()
            self._shutdown_event.wait()
        finally:
            self._teardown()

    def stop(self) -> None:
        self._shutdown_event.set()

    def _setup(self) -> None:
        self._run_id = Types.RunId(
            f"{time.strftime('%Y-%m-%d-%H-%M-%S')}_{uuid.uuid4().hex[:4]}"
        )
        self._setup_event_bus()
        self._setup_run_recorder()
        self._setup_broker()
        self._setup_strategies()
        self._setup_datafeed()
        logger.info("run started: %s", self._run_id)

    def _setup_event_bus(self) -> None:
        self._instances.event_bus = EventBus()

    def _setup_run_recorder(self) -> None:
        assert self._instances.event_bus is not None
        self._instances.run_recorder = ComponentBases.RunRecorder(
            self._instances.event_bus,
            self._runs_db_path,
            self._run_id,
            self._strategies,
        )

    def _setup_broker(self) -> None:
        assert self._instances.event_bus is not None
        self._instances.broker = self._broker_class(self._instances.event_bus)
        self._instances.broker.connect()

    def _setup_strategies(self) -> None:
        assert self._instances.event_bus is not None
        self._instances.strategies = [
            strategy_class(
                self._instances.event_bus,
                symbols,
                record_type,
            )
            for strategy_class, (record_type, symbols) in self._strategies.items()
        ]

    # Blocks until all subscriber queues are fully processed, including cascading
    # events. Injected into the datafeed as `on_bar_emitted` so the datafeed paces
    # itself to the system's processing speed. Essential for deterministic backtesting;
    # also serves as backpressure control in live trading.
    def _wait_until_ready(self) -> None:
        subscribers = [
            s
            for s in [
                self._instances.run_recorder,
                self._instances.broker,
                *self._instances.strategies,
            ]
            if s is not None
        ]
        # A single pass is insufficient because processing can publish new events
        # into already-drained queues, so we loop until a re-check confirms idleness.
        while True:
            for subscriber in subscribers:
                subscriber.wait_until_idle()
            if all(subscriber.is_idle for subscriber in subscribers):
                break

    def _setup_datafeed(self) -> None:
        assert self._instances.event_bus is not None
        self._instances.datafeed = self._datafeed_class(
            self._instances.event_bus,
            on_fatal=self._shutdown_event.set,
            on_bar_emitted=self._wait_until_ready,
        )
        self._instances.datafeed.connect()
        for record_type, symbols in self._strategies.values():
            self._instances.datafeed.subscribe(symbols, record_type)

    def _teardown(self) -> None:
        if self._instances.datafeed is not None:
            try:
                for record_type, symbols in self._strategies.values():
                    self._instances.datafeed.unsubscribe(symbols, record_type)
                self._instances.datafeed.disconnect()
            except Exception:
                logger.error("datafeed teardown failed", exc_info=True)

        for strategy in self._instances.strategies:
            try:
                strategy.shutdown()
            except Exception:
                logger.error(
                    "strategy teardown failed for %s",
                    type(strategy).__name__,
                    exc_info=True,
                )

        if self._instances.broker is not None:
            try:
                self._instances.broker.shutdown()
                self._instances.broker.disconnect()
            except Exception:
                logger.error("broker teardown failed", exc_info=True)

        if self._instances.run_recorder is not None:
            try:
                self._instances.run_recorder.shutdown()
            except Exception:
                logger.error("run recorder teardown failed", exc_info=True)

        logger.info("teardown complete")
