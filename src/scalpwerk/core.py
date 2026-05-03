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


class _Protocols:
    class EventLike(typing.Protocol):
        @property  # for `mypy`, plain attribute would imply settable
        def occurred_at_ns(self) -> Types.UnixNanoseconds: ...
        @property
        def created_at_ns(self) -> Types.UnixNanoseconds: ...

    class SubscriberLike(typing.Protocol):
        def receive(self, event: "_Protocols.EventLike") -> None: ...
        def wait_until_idle(self) -> None: ...
        @property
        def is_idle(self) -> bool: ...

    class EventBusLike(typing.Protocol):
        def subscribe(
            self,
            subscriber: "_Protocols.SubscriberLike",
            *event_types: type["_Protocols.EventLike"],
        ) -> None: ...
        def publish(self, event: "_Protocols.EventLike") -> None: ...


class Events:
    # fmt: off
    class Datafeed:
        @dataclasses.dataclass(kw_only=True, frozen=True, slots=True)
        class _Base:
            occurred_at_ns:    Types.UnixNanoseconds
            created_at_ns:     Types.UnixNanoseconds = dataclasses.field(
                                   default_factory= lambda: Types.UnixNanoseconds(
                                       time.time_ns()))

        @dataclasses.dataclass(kw_only=True, frozen=True, slots=True)
        class Connected(_Base):
            pass

        @dataclasses.dataclass(kw_only=True, frozen=True, slots=True)
        class Disconnected(_Base):
            reason:            str = ""

        @dataclasses.dataclass(kw_only=True, frozen=True, slots=True)
        class Bar(_Base):
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
        class _Base:
            occurred_at_ns:    Types.UnixNanoseconds
            created_at_ns:     Types.UnixNanoseconds = dataclasses.field(
                                   default_factory= lambda: Types.UnixNanoseconds(
                                       time.time_ns()))

        @dataclasses.dataclass(kw_only=True, frozen=True, slots=True)
        class IndicatorUpdate(_Base):
            symbol:            Types.Symbol
            source_event:      "Events.Datafeed.Bar"
            indicator_values:  dict[Types.IndicatorName, Types.IndicatorValue]

        @dataclasses.dataclass(kw_only=True, frozen=True, slots=True)
        class _OrderBase(_Base):
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
            created_at_ns:     Types.UnixNanoseconds = dataclasses.field(
                                   default_factory= lambda: Types.UnixNanoseconds(
                                       time.time_ns()))

        @dataclasses.dataclass(kw_only=True, frozen=True, slots=True)
        class Connected(_Base):
            working_orders:    dict[Types.InternalOrderId, "_WorkingOrder"]
            positions:         dict[Types.Symbol, "_Position"]

        @dataclasses.dataclass(kw_only=True, frozen=True, slots=True)
        class Disconnected(_Base):
            reason:            str = ""

        @dataclasses.dataclass(kw_only=True, frozen=True, slots=True)
        class _OrderBase(_Base):
            internal_order_id: Types.InternalOrderId
            broker_order_id:   Types.BrokerOrderId

        @dataclasses.dataclass(kw_only=True, frozen=True, slots=True)
        class CancellationAccepted(_OrderBase):
            pass

        @dataclasses.dataclass(kw_only=True, frozen=True, slots=True)
        class CancellationRejected(_OrderBase):
            reason:            str

        @dataclasses.dataclass(kw_only=True, frozen=True, slots=True)
        class ModificationAccepted(_OrderBase):
            pass

        @dataclasses.dataclass(kw_only=True, frozen=True, slots=True)
        class ModificationRejected(_OrderBase):
            reason:            str

        @dataclasses.dataclass(kw_only=True, frozen=True, slots=True)
        class OrderAccepted(_OrderBase):
            pass

        # Standalone: `broker_order_id` is optional (rejection may arrive before the
        # broker assigns an ID).
        @dataclasses.dataclass(kw_only=True, frozen=True, slots=True)
        class OrderRejected(_Base):
            internal_order_id: Types.InternalOrderId
            broker_order_id:   Types.BrokerOrderId | None = None
            reason:            str = ""

        # A single execution against an order. Does not indicate partial vs. full fill;
        # the system must track fill quantities to determine remaining open quantity.
        @dataclasses.dataclass(kw_only=True, frozen=True, slots=True)
        class Fill(_OrderBase):
            symbol:            Types.Symbol
            internal_fill_id:  Types.InternalFillId
            broker_fill_id:    Types.BrokerFillId
            side:              Enums.TradeSide
            filled_quantity:   Types.Quantity
            fill_price:        Types.ScaledPrice
            exchange:          str
            commission:        Types.ScaledPrice = Types.ScaledPrice(0)

        @dataclasses.dataclass(kw_only=True, frozen=True, slots=True)
        class OrderExpired(_OrderBase):
            pass
    # fmt: on


class _EventBus:
    def __init__(self) -> None:
        self._per_event_subscriptions: collections.defaultdict[
            type[_Protocols.EventLike], set[_Protocols.SubscriberLike]
        ] = collections.defaultdict(set)
        self._lock: threading.Lock = threading.Lock()

    def subscribe(
        self,
        subscriber: _Protocols.SubscriberLike,
        *event_types: type[_Protocols.EventLike],  # `type` for classes, not instances
    ) -> None:
        with self._lock:
            for event_type in event_types:  # no duplication; subscribers are in set
                self._per_event_subscriptions[event_type].add(subscriber)

    def publish(self, event: _Protocols.EventLike) -> None:
        with self._lock:
            subscribers = self._per_event_subscriptions[type(event)].copy()
        for subscriber in subscribers:
            subscriber.receive(event)


class _ComponentBase(abc.ABC):
    def __init__(self, event_bus: _Protocols.EventBusLike) -> None:
        self._event_bus: _Protocols.EventBusLike = event_bus


class _SubscriberBase(_ComponentBase, _Protocols.SubscriberLike):
    SUBSCRIBE_TO: tuple[type[_Protocols.EventLike], ...] = ()

    def __init__(self, event_bus: _Protocols.EventBusLike) -> None:
        super().__init__(event_bus)
        self._event_bus.subscribe(self, *self.SUBSCRIBE_TO)
        self._queue: queue.Queue[_Protocols.EventLike | None] = queue.Queue()
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

    def receive(self, event: _Protocols.EventLike) -> None:
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
    def _on_event(self, event: _Protocols.EventLike) -> None:
        pass


class _EmitterBase(_ComponentBase):
    def _emit_event(self, event: _Protocols.EventLike) -> None:
        self._event_bus.publish(event)


class _Connectable(abc.ABC):
    @abc.abstractmethod
    def connect(self) -> None:
        pass

    @abc.abstractmethod
    def disconnect(self) -> None:
        pass


class BrokerBase(_Connectable, _SubscriberBase, _EmitterBase):
    SUBSCRIBE_TO = (
        Events.Strategy.SubmitOrder,
        Events.Strategy.ModifyOrder,
        Events.Strategy.CancelOrder,
    )

    def _on_event(self, event: _Protocols.EventLike) -> None:
        match event:
            case Events.Strategy.SubmitOrder() as incoming_event:
                self._on_submit_order(incoming_event)
            case Events.Strategy.ModifyOrder() as incoming_event:
                self._on_modify_order(incoming_event)
            case Events.Strategy.CancelOrder() as incoming_event:
                self._on_cancel_order(incoming_event)
            case _:
                return

    # TODO: design return type once strategy position management is finalized.
    # Should return open positions and working orders so the strategy can
    # initialize from the broker's account state (source of truth).
    @abc.abstractmethod
    def get_account_state(self) -> typing.Any:
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


class DatafeedBase(_Connectable, _EmitterBase):
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


class IndicatorBase(abc.ABC):
    def __init__(self, max_history: int = 100) -> None:
        self._max_history = max(1, int(max_history))
        self._history: dict[
            Types.Symbol,
            collections.deque[Types.IndicatorValue],
        ] = {}

    # The name should be defined via an f-string so that instances of the same indicator
    # can be distinguished via their parameters, e.g. `f"SMA_{period}_{source}"`
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


class _WorkingOrder(typing.NamedTuple):
    symbol: Types.Symbol
    order_type: Enums.OrderType
    side: Enums.TradeSide
    quantity: Types.Quantity
    limit_price: Types.ScaledPrice | None
    stop_price: Types.ScaledPrice | None
    filled_quantity: Types.FilledQuantity


class _Position(typing.NamedTuple):
    size: Types.PositionSize
    avg_price: Types.ScaledPrice


class StrategyBase(_SubscriberBase, _EmitterBase):
    # TODO: Implement readiness lifecycle. Strategy should only trade when both
    # Broker.Connected and Datafeed.Connected have been received. On disconnect,
    # stop submitting orders. Consider warming up indicators (all non-NaN) while
    # only the datafeed is connected, so the strategy is ready to trade the moment
    # the broker connects.

    SUBSCRIBE_TO = (
        Events.Datafeed.Connected,
        Events.Datafeed.Disconnected,
        Events.Datafeed.Bar,
        Events.Broker.Connected,
        Events.Broker.Disconnected,
        Events.Broker.OrderAccepted,
        Events.Broker.OrderRejected,
        Events.Broker.ModificationAccepted,
        Events.Broker.ModificationRejected,
        Events.Broker.CancellationAccepted,
        Events.Broker.CancellationRejected,
        Events.Broker.Fill,
        Events.Broker.OrderExpired,
    )

    SYMBOLS: set[Types.Symbol] = set()
    RECORD_TYPE: Enums.RecordType = Enums.RecordType.OHLCV_1S

    def __init__(self, event_bus: _Protocols.EventBusLike) -> None:
        super().__init__(event_bus)

        self._current_bar: Events.Datafeed.Bar | None = None
        self._indicators: dict[Types.IndicatorName, IndicatorBase] = {}

        # In-flight requests awaiting broker acknowledgement.
        self._submitted_orders: dict[
            Types.InternalOrderId, Events.Strategy.SubmitOrder
        ] = {}
        self._submitted_modifications: dict[
            Types.InternalOrderId, Events.Strategy.ModifyOrder
        ] = {}
        self._submitted_cancellations: dict[
            Types.InternalOrderId, Events.Strategy.CancelOrder
        ] = {}

        self._working_orders: dict[Types.InternalOrderId, _WorkingOrder] = {}
        self._positions: dict[Types.Symbol, _Position] = {}

        # Must be last so base class state exists before the subclass's `setup()` runs.
        self.setup()

    @abc.abstractmethod
    def setup(self) -> None:
        pass

    @abc.abstractmethod
    def on_bar(self, event: Events.Datafeed.Bar) -> None:
        pass

    # --- HELPER FUNCTIONS FOR

    def add_indicator(self, indicator: IndicatorBase) -> IndicatorBase:
        self._indicators[indicator.name] = indicator
        return indicator  # for inline assignment: `self.sma = self.add_indicator(...)`

    def submit_order(
        self,
        symbol: Types.Symbol,
        order_type: Enums.OrderType,
        side: Enums.TradeSide,
        quantity: Types.Quantity,
        limit_price: Types.ScaledPrice | None = None,
        stop_price: Types.ScaledPrice | None = None,
    ) -> Types.InternalOrderId:
        assert self._current_bar is not None  # silence mypy

        # Create order submission event message
        internal_order_id = Types.InternalOrderId(uuid.uuid4())
        order_submission_event = Events.Strategy.SubmitOrder(
            occurred_at_ns=self._current_bar.occurred_at_ns,
            internal_order_id=internal_order_id,
            symbol=symbol,
            order_type=order_type,
            side=side,
            quantity=quantity,
            limit_price=limit_price,
            stop_price=stop_price,
        )

        # Track submission and emit it
        self._submitted_orders[internal_order_id] = order_submission_event
        self._emit_event(order_submission_event)

        # Return the created order ID
        return internal_order_id

    def submit_modification(
        self,
        internal_order_id: Types.InternalOrderId,
        quantity: Types.Quantity,
        limit_price: Types.ScaledPrice | None = None,
        stop_price: Types.ScaledPrice | None = None,
    ) -> bool:
        assert self._current_bar is not None  # silence mypy

        # Check if working order that should be modified exists
        working_order_to_modify = self._working_orders.get(internal_order_id)
        if working_order_to_modify is None:
            return False

        order_modification_submission = Events.Strategy.ModifyOrder(
            occurred_at_ns=self._current_bar.occurred_at_ns,
            internal_order_id=internal_order_id,
            symbol=working_order_to_modify.symbol,
            quantity=quantity,
            limit_price=limit_price,
            stop_price=stop_price,
        )

        # Track submission and emit it
        self._submitted_modifications[internal_order_id] = order_modification_submission
        self._emit_event(order_modification_submission)

        return True

    def submit_cancellation(self, internal_order_id: Types.InternalOrderId) -> bool:
        assert self._current_bar is not None

        working = self._working_orders.get(internal_order_id)
        if working is None:
            return False

        event = Events.Strategy.CancelOrder(
            occurred_at_ns=self._current_bar.occurred_at_ns,
            internal_order_id=internal_order_id,
            symbol=working.symbol,
        )

        self._submitted_cancellations[internal_order_id] = event
        self._emit_event(event)
        return True

    # --- User-facing: properties ---

    @property
    def position_size(self) -> Types.PositionSize:
        assert self._current_bar is not None
        pos = self._positions.get(self._current_bar.symbol)
        return pos.size if pos else Types.PositionSize(0)

    @property
    def flat(self) -> bool:
        return self.position_size == 0

    @property
    def average_entry_price(self) -> Types.ScaledPrice | None:
        assert self._current_bar is not None
        pos = self._positions.get(self._current_bar.symbol)
        return pos.avg_price if pos else None

    # --- Internal plumbing ---

    def _on_event(self, event: _Protocols.EventLike) -> None:
        # fmt: off
        match event:
            case Events.Datafeed.Bar()                as event:
                self._on_bar(event)

            case Events.Broker.OrderAccepted()        as event:
                self._on_order_accepted(event)

            case Events.Broker.OrderRejected()        as event:
                self._on_order_rejected(event)

            case Events.Broker.ModificationAccepted() as event:
                self._on_modification_accepted(event)

            case Events.Broker.ModificationRejected() as event:
                self._on_modification_rejected(event)

            case Events.Broker.CancellationAccepted() as event:
                self._on_cancellation_accepted(event)

            case Events.Broker.CancellationRejected() as event:
                self._on_cancellation_rejected(event)

            case Events.Broker.Fill()                 as event:
                self._on_fill(event)

            case Events.Broker.OrderExpired()         as event:
                self._on_order_expired(event)
        # fmt: on

    def _on_bar(self, bar: Events.Datafeed.Bar) -> None:
        # Ignore bars that are not relevant to the strategy
        if bar.record_type != self.RECORD_TYPE or bar.symbol not in self.SYMBOLS:
            return

        # Set current bar
        self._current_bar = bar

        # Update indicators used by the strategy
        for indicator in self._indicators.values():
            indicator.update(bar)

        # Apply strategy logic
        self.on_bar(bar)

        # Emit indicator values incl. bar values after `on_bar` so strategy
        # logic isn't delayed.
        self._emit_event(
            Events.Strategy.IndicatorUpdate(
                occurred_at_ns=bar.occurred_at_ns,
                symbol=bar.symbol,
                source_event=bar,
                indicator_values={
                    name: indicator.latest(bar.symbol)
                    for name, indicator in self._indicators.items()
                },
            )
        )

    def _on_order_accepted(self, accepted_order: Events.Broker.OrderAccepted) -> None:
        # Remove order from submitted orders and register it as working order
        order = self._submitted_orders.pop(accepted_order.internal_order_id, None)
        if order is not None:
            self._working_orders[accepted_order.internal_order_id] = _WorkingOrder(
                order.symbol,
                order.order_type,
                order.side,
                order.quantity,
                order.limit_price,
                order.stop_price,
                Types.FilledQuantity(0),
            )

    def _on_order_rejected(self, rejected_order: Events.Broker.OrderRejected) -> None:
        # Remove order from submitted orders
        self._submitted_orders.pop(rejected_order.internal_order_id, None)

    def _on_modification_accepted(
        self, accepted_modification: Events.Broker.ModificationAccepted
    ) -> None:
        # Remove from submitted modifications and update the working order entry
        modification = self._submitted_modifications.pop(
            accepted_modification.internal_order_id, None
        )
        if modification is None:
            return

        working = self._working_orders.get(accepted_modification.internal_order_id)
        if working is None:
            return

        # Quantity reduction may retroactively fully fill if fills arrived
        # while the modification was in-flight.
        if working.filled_quantity >= modification.quantity:
            self._working_orders.pop(accepted_modification.internal_order_id)
        else:
            self._working_orders[accepted_modification.internal_order_id] = (
                working._replace(
                    quantity=modification.quantity,
                    limit_price=modification.limit_price,
                    stop_price=modification.stop_price,
                )
            )

    def _on_modification_rejected(
        self, rejected_modification: Events.Broker.ModificationRejected
    ) -> None:
        self._submitted_modifications.pop(rejected_modification.internal_order_id, None)

    def _on_cancellation_accepted(
        self, accepted_cancellation: Events.Broker.CancellationAccepted
    ) -> None:
        self._submitted_cancellations.pop(accepted_cancellation.internal_order_id, None)
        self._working_orders.pop(accepted_cancellation.internal_order_id, None)
        # In-flight modifications will not get a response after cancellation.
        self._submitted_modifications.pop(accepted_cancellation.internal_order_id, None)

    def _on_cancellation_rejected(
        self, rejected_cancellation: Events.Broker.CancellationRejected
    ) -> None:
        self._submitted_cancellations.pop(rejected_cancellation.internal_order_id, None)

    def _on_fill(self, fill: Events.Broker.Fill) -> None:
        # --- update position ---
        signed_qty = (
            fill.filled_quantity
            if fill.side == Enums.TradeSide.BUY
            else -fill.filled_quantity
        )
        pos = self._positions.get(fill.symbol)
        old_size = pos.size if pos else Types.PositionSize(0)
        old_avg = pos.avg_price if pos else Types.ScaledPrice(0)
        new_size = old_size + signed_qty

        if new_size == 0:
            self._positions.pop(fill.symbol, None)
        elif old_size == 0 or old_size * new_size < 0:
            self._positions[fill.symbol] = _Position(
                Types.PositionSize(new_size), fill.fill_price
            )
        elif old_size * signed_qty > 0:
            self._positions[fill.symbol] = _Position(
                Types.PositionSize(new_size),
                Types.ScaledPrice(
                    (old_avg * abs(old_size) + fill.fill_price * abs(signed_qty))
                    // abs(new_size)
                ),
            )
        else:
            self._positions[fill.symbol] = _Position(
                Types.PositionSize(new_size), Types.ScaledPrice(old_avg)
            )

        # --- update working orders ---
        working = self._working_orders.get(fill.internal_order_id)
        if working is not None:
            new_filled = Types.FilledQuantity(
                working.filled_quantity + fill.filled_quantity
            )
            if new_filled >= working.quantity:
                self._working_orders.pop(fill.internal_order_id)
            else:
                self._working_orders[fill.internal_order_id] = working._replace(
                    filled_quantity=new_filled
                )

    def _on_order_expired(self, expired_order: Events.Broker.OrderExpired) -> None:
        self._working_orders.pop(expired_order.internal_order_id)
        self._submitted_modifications.pop(expired_order.internal_order_id, None)
        self._submitted_cancellations.pop(expired_order.internal_order_id, None)


class RunRecorder(_SubscriberBase):
    SUBSCRIBE_TO = (
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
        event_bus: _Protocols.EventBusLike,
        runs_db_path: pathlib.Path,
        run_id: Types.RunId,
        strategies: list[type[StrategyBase]],
    ) -> None:
        super().__init__(event_bus)
        self._runs_db_path: pathlib.Path = pathlib.Path(runs_db_path)
        self._run_id: Types.RunId = Types.RunId(run_id)
        self._strategies = strategies

        self._conn: sqlite3.Connection | None = None

    def _setup_db(self) -> None:
        self._conn = sqlite3.connect(self._runs_db_path)

        self._conn.execute("PRAGMA journal_mode = WAL")
        self._conn.execute("PRAGMA synchronous = NORMAL")
        self._conn.execute("PRAGMA foreign_keys = ON")

        if self._conn.execute(self._SQL_CHECK_SCHEMA_VERSION_TABLE_EXISTS).fetchone():
            row = self._conn.execute(self._SQL_SELECT_SCHEMA_VERSION).fetchone()
            version = row[0] if row else None
            if version != self._SCHEMA_VERSION:
                raise RuntimeError(
                    f"Schema version mismatch: expected {self._SCHEMA_VERSION},"
                    f" found {version}"
                )
        else:
            self._conn.executescript(self._RUN_RECORDER_SCHEMA)
            self._conn.execute(self._SQL_INSERT_SCHEMA_VERSION, (self._SCHEMA_VERSION,))

        self._conn.execute(self._SQL_INSERT_RUN, (str(self._run_id),))
        self._conn.executemany(
            self._SQL_INSERT_RUN_STRATEGY,
            [
                (
                    str(self._run_id),
                    strategy_class.__name__,
                    str(symbol),
                    strategy_class.RECORD_TYPE.name,
                )
                for strategy_class in self._strategies
                for symbol in strategy_class.SYMBOLS
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

    def _on_event(self, event: _Protocols.EventLike) -> None:
        assert self._conn is not None  # for type checker; always set by _setup_db

        try:
            self._record(event)
        except Exception as exc:
            logger.warning("rolling back transaction: %s", exc)
            self._conn.rollback()

    def _record(self, event: _Protocols.EventLike) -> None:
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
    event_bus: _Protocols.EventBusLike | None = None
    run_recorder: RunRecorder | None = None
    broker: BrokerBase | None = None
    strategies: list[StrategyBase] = dataclasses.field(default_factory=list)
    datafeed: DatafeedBase | None = None

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
        strategies: list[type[StrategyBase]],
        broker: type[BrokerBase],
        datafeed: type[DatafeedBase],
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
        self._instances.event_bus = _EventBus()

    def _setup_run_recorder(self) -> None:
        assert self._instances.event_bus is not None
        self._instances.run_recorder = RunRecorder(
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
            strategy_class(self._instances.event_bus)
            for strategy_class in self._strategies
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
        self._instances.datafeed = self._datafeed_class(self._instances.event_bus)
        self._instances.datafeed.connect()
        for strategy_class in self._strategies:
            self._instances.datafeed.subscribe(
                list(strategy_class.SYMBOLS), strategy_class.RECORD_TYPE
            )

    def _teardown(self) -> None:
        if self._instances.datafeed is not None:
            try:
                for strategy_class in self._strategies:
                    self._instances.datafeed.unsubscribe(
                        list(strategy_class.SYMBOLS), strategy_class.RECORD_TYPE
                    )
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
