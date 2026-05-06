import abc
import collections
import dataclasses
import enum
import queue
import signal
import threading
import time
import typing
import uuid


class Types:  # aliases for types with non-obvious semantics
    UnixNanoseconds = typing.NewType("UnixNanoseconds", int)  # since UTC unix epoch
    ScaledPrice = typing.NewType("ScaledPrice", int)  # decimal prices scaled by 10^9


class Enums:  # enums to model domain concepts with a fixed set of possible values
    # fmt: off
    class OrderType(enum.Enum):
        MARKET     = enum.auto()
        LIMIT      = enum.auto()
        STOP       = enum.auto()
        STOP_LIMIT = enum.auto()

    class TradeSide(enum.Enum):
        BUY  = enum.auto()
        SELL = enum.auto()

    class BarPeriod(enum.Enum):
        OHLCV_1S = enum.auto()
        OHLCV_1M = enum.auto()
        OHLCV_1H = enum.auto()
        OHLCV_1D = enum.auto()

    class TimeInForce(enum.Enum):
        DAY = enum.auto()
        GTC = enum.auto()
        IOC = enum.auto()
    # fmt: on


class Exposure:
    class WorkingOrder(typing.NamedTuple):
        symbol: str
        order_type: Enums.OrderType
        side: Enums.TradeSide
        quantity: int
        limit_price: Types.ScaledPrice | None
        stop_price: Types.ScaledPrice | None
        filled_quantity: int

    class Position(typing.NamedTuple):
        size: int
        cost_basis: Types.ScaledPrice


class _Protocols:
    class EventLike(typing.Protocol):
        @property  # decorator for `mypy`; plain attribute would imply settable
        def occurred_at_ns(self) -> Types.UnixNanoseconds: ...
        @property
        def created_at_ns(self) -> Types.UnixNanoseconds: ...

    class SubscriberLike(typing.Protocol):
        def receive(self, event: "_Protocols.EventLike") -> None: ...
        def wait_until_idle(self) -> None: ...
        @property
        def is_idle(self) -> bool: ...


class Events:
    # fmt: off
    class Datafeed:
        @dataclasses.dataclass(kw_only=True, frozen=True, slots=True)
        class Bar:
            occurred_at_ns: Types.UnixNanoseconds
            created_at_ns:  Types.UnixNanoseconds = dataclasses.field(
                                default_factory= lambda: Types.UnixNanoseconds(
                                    time.time_ns()))
            symbol:         str
            record_type:    Enums.BarPeriod
            open:           Types.ScaledPrice
            high:           Types.ScaledPrice
            low:            Types.ScaledPrice
            close:          Types.ScaledPrice
            volume:         int | None = None

    class Strategy:
        @dataclasses.dataclass(kw_only=True, frozen=True, slots=True)
        class IndicatorUpdate:
            occurred_at_ns:     Types.UnixNanoseconds
            created_at_ns:      Types.UnixNanoseconds = dataclasses.field(
                                    default_factory= lambda: Types.UnixNanoseconds(
                                        time.time_ns()))
            symbol:             str
            source_event:       "Events.Datafeed.Bar"
            indicator_values:   dict[str, float]

        @dataclasses.dataclass(kw_only=True, frozen=True, slots=True)
        class _OrderBase:
            occurred_at_ns:     Types.UnixNanoseconds
            created_at_ns:      Types.UnixNanoseconds = dataclasses.field(
                                    default_factory= lambda: Types.UnixNanoseconds(
                                        time.time_ns()))
            symbol:             str
            internal_order_id:  uuid.UUID

        @dataclasses.dataclass(kw_only=True, frozen=True, slots=True)
        class SubmitOrder(_OrderBase):
            order_type:         Enums.OrderType
            side:               Enums.TradeSide
            quantity:           int
            time_in_force:      Enums.TimeInForce = Enums.TimeInForce.DAY
            limit_price:        Types.ScaledPrice | None = None
            stop_price:         Types.ScaledPrice | None = None

        @dataclasses.dataclass(kw_only=True, frozen=True, slots=True)
        class ModifyOrder(_OrderBase):
            quantity:           int
            limit_price:        Types.ScaledPrice | None
            stop_price:         Types.ScaledPrice | None

        @dataclasses.dataclass(kw_only=True, frozen=True, slots=True)
        class CancelOrder(_OrderBase):
            pass

    class Broker:
        @dataclasses.dataclass(kw_only=True, frozen=True, slots=True)
        class ExposureSnapshot:
            occurred_at_ns:         Types.UnixNanoseconds
            created_at_ns:          Types.UnixNanoseconds = dataclasses.field(
                                        default_factory= lambda: Types.UnixNanoseconds(
                                            time.time_ns()))
            working_orders:         dict[uuid.UUID, Exposure.WorkingOrder]
            positions:              dict[str, Exposure.Position]

        @dataclasses.dataclass(kw_only=True, frozen=True, slots=True)
        class _OrderBase:
            occurred_at_ns:         Types.UnixNanoseconds
            created_at_ns:          Types.UnixNanoseconds = dataclasses.field(
                                        default_factory= lambda: Types.UnixNanoseconds(
                                            time.time_ns()))
            internal_order_id:      uuid.UUID

        @dataclasses.dataclass(kw_only=True, frozen=True, slots=True)
        class OrderAccepted(_OrderBase):
            pass

        @dataclasses.dataclass(kw_only=True, frozen=True, slots=True)
        class OrderRejected(_OrderBase):
            reason:                 str = ""

        @dataclasses.dataclass(kw_only=True, frozen=True, slots=True)
        class CancellationAccepted(_OrderBase):
            pass

        @dataclasses.dataclass(kw_only=True, frozen=True, slots=True)
        class CancellationRejected(_OrderBase):
            reason:                 str

        @dataclasses.dataclass(kw_only=True, frozen=True, slots=True)
        class ModificationAccepted(_OrderBase):
            pass

        @dataclasses.dataclass(kw_only=True, frozen=True, slots=True)
        class ModificationRejected(_OrderBase):
            reason:                 str

        @dataclasses.dataclass(kw_only=True, frozen=True, slots=True)
        class Fill(_OrderBase):
            symbol:                 str  # needed for position bookkeeping
            internal_fill_id:       uuid.UUID
            side:                   Enums.TradeSide
            filled_quantity:        int
            fill_price:             Types.ScaledPrice
            position_size:          int
            position_cost_basis:    Types.ScaledPrice

        @dataclasses.dataclass(kw_only=True, frozen=True, slots=True)
        class OrderExpired(_OrderBase):
            pass
    # fmt: on


class _EventBus:
    def __init__(self) -> None:
        self._per_event_subscriptions: collections.defaultdict[
            type[_Protocols.EventLike], set[_Protocols.SubscriberLike]
        ] = collections.defaultdict(set)

    def subscribe(
        self,
        subscriber: _Protocols.SubscriberLike,
        *event_types: type[_Protocols.EventLike],  # `type` for classes, not instances
    ) -> None:
        for event_type in event_types:  # no duplication; subscribers are in set
            self._per_event_subscriptions[event_type].add(subscriber)

    def publish(self, event: _Protocols.EventLike) -> None:
        # No lock needed; subscriptions are setup-time only, before any events flow.
        for subscriber in self._per_event_subscriptions[type(event)]:
            subscriber.receive(event)


# Singleton for simplicity; trade-off: cannot run two isolated systems in same process.
_event_bus = _EventBus()


class _SubscriberBase(abc.ABC, _Protocols.SubscriberLike):
    SUBSCRIBE_TO: tuple[type[_Protocols.EventLike], ...] = ()

    def __init__(self) -> None:
        _event_bus.subscribe(self, *self.SUBSCRIBE_TO)
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


class _EmitterBase:
    @staticmethod
    def _emit_event(event: _Protocols.EventLike) -> None:
        _event_bus.publish(event)


class _Connectable(abc.ABC):
    # Set by the orchestrator. Subclasses call `self.trigger_shutdown()` to
    # shut down the entire system (e.g. end of CSV, lost broker connection).
    _shutdown_flag: threading.Event | None = None

    def trigger_shutdown(self) -> None:
        if self._shutdown_flag is not None:
            self._shutdown_flag.set()

    @abc.abstractmethod
    def connect(self) -> None:
        pass

    @abc.abstractmethod
    def disconnect(self) -> None:
        pass


class BrokerConnectorBase(_Connectable, _SubscriberBase, _EmitterBase):
    # Every type listed here must have a matching case in `_on_event`.
    SUBSCRIBE_TO = (
        Events.Strategy.SubmitOrder,
        Events.Strategy.ModifyOrder,
        Events.Strategy.CancelOrder,
    )

    def connect(self) -> None:
        self._connect()
        working_orders, positions = self._get_exposure_snapshot()
        self._emit_event(
            Events.Broker.ExposureSnapshot(
                occurred_at_ns=Types.UnixNanoseconds(time.time_ns()),
                working_orders=working_orders,
                positions=positions,
            )
        )

    def disconnect(self) -> None:
        self._disconnect()  # close connection
        self.shutdown()  # stop event thread

    def _on_event(self, event: _Protocols.EventLike) -> None:
        match event:
            case Events.Strategy.SubmitOrder() as incoming_event:
                self._on_submit_order(incoming_event)
            case Events.Strategy.ModifyOrder() as incoming_event:
                self._on_modify_order(incoming_event)
            case Events.Strategy.CancelOrder() as incoming_event:
                self._on_cancel_order(incoming_event)
            case _:
                raise RuntimeError(f"unhandled event type: {type(event).__name__}")

    @abc.abstractmethod
    def _connect(self) -> None:
        pass

    @abc.abstractmethod
    def _disconnect(self) -> None:
        pass

    @abc.abstractmethod
    def _get_exposure_snapshot(
        self,
    ) -> tuple[dict[uuid.UUID, Exposure.WorkingOrder], dict[str, Exposure.Position]]:
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


class DatafeedConnectorBase(_Connectable, _EmitterBase):
    def connect(self) -> None:
        self._connect()

    def disconnect(self) -> None:
        self._disconnect()

    @abc.abstractmethod
    def _connect(self) -> None:
        pass

    @abc.abstractmethod
    def _disconnect(self) -> None:
        pass

    @abc.abstractmethod
    def subscribe(
        self,
        symbols: list[str],
        record_type: Enums.BarPeriod,
    ) -> None:
        pass


class IndicatorBase(abc.ABC):
    def __init__(self, max_history: int = 100) -> None:
        self._max_history = max(1, int(max_history))
        self._history: dict[
            str,
            collections.deque[float],
        ] = {}
        self._input_indicators: dict[str, "IndicatorBase"] = {}

    # The name should be defined via an f-string so that instances of the same indicator
    # can be distinguished via their parameters, e.g. `f"SMA_{period}_{source}"`
    @property
    @abc.abstractmethod
    def name(self) -> str:
        pass

    def add_indicator(self, indicator: "IndicatorBase") -> "IndicatorBase":
        self._input_indicators[indicator.name] = indicator
        return indicator

    def update(self, event: Events.Datafeed.Bar) -> None:
        for input_indicator in self._input_indicators.values():
            input_indicator.update(event)
        value = self._compute(event)
        symbol = event.symbol
        if symbol not in self._history:
            self._history[symbol] = collections.deque(maxlen=self._max_history)
        self._history[symbol].append(value)

    @abc.abstractmethod
    def _compute(self, event: Events.Datafeed.Bar) -> float:
        pass

    def latest(self, symbol: str) -> float:
        return self[symbol, -1]

    # Supports standard negative indexing, e.g. `indicator["AAPL", -2]`.
    def __getitem__(self, key: tuple[str, int]) -> float:
        symbol, index = key
        history = self._history.get(symbol)
        if history is None:
            return float("nan")
        try:
            return history[index]
        except IndexError:
            return float("nan")


class StrategyBase(_SubscriberBase, _EmitterBase):
    # Every type listed here must have a matching case in `_on_event`.
    SUBSCRIBE_TO = (
        Events.Datafeed.Bar,
        Events.Broker.ExposureSnapshot,
        Events.Broker.OrderAccepted,
        Events.Broker.OrderRejected,
        Events.Broker.ModificationAccepted,
        Events.Broker.ModificationRejected,
        Events.Broker.CancellationAccepted,
        Events.Broker.CancellationRejected,
        Events.Broker.Fill,
        Events.Broker.OrderExpired,
    )

    SYMBOLS: set[str] = set()
    RECORD_TYPE: Enums.BarPeriod = Enums.BarPeriod.OHLCV_1S

    def __init__(self) -> None:
        super().__init__()

        self._current_bar: Events.Datafeed.Bar | None = None
        self._indicators: dict[str, IndicatorBase] = {}

        # In-flight requests awaiting broker acknowledgement.
        self._submitted_orders: dict[uuid.UUID, Events.Strategy.SubmitOrder] = {}
        self._submitted_modifications: dict[uuid.UUID, Events.Strategy.ModifyOrder] = {}
        self._submitted_cancellations: dict[uuid.UUID, Events.Strategy.CancelOrder] = {}

        self._working_orders: dict[uuid.UUID, Exposure.WorkingOrder] = {}
        self._positions: dict[str, Exposure.Position] = {}

        # Must be last so base class state exists before the subclass's `setup()` runs.
        self.setup()

    @abc.abstractmethod
    def setup(self) -> None:
        pass

    def add_indicator(self, indicator: IndicatorBase) -> IndicatorBase:
        self._indicators[indicator.name] = indicator
        return indicator  # for inline assignment: `self.sma = self.add_indicator(...)`

    @abc.abstractmethod
    def on_bar(self, event: Events.Datafeed.Bar) -> None:
        pass

    def submit_order(
        self,
        symbol: str,
        order_type: Enums.OrderType,
        side: Enums.TradeSide,
        quantity: int,
        time_in_force: Enums.TimeInForce = Enums.TimeInForce.DAY,
        limit_price: Types.ScaledPrice | None = None,
        stop_price: Types.ScaledPrice | None = None,
    ) -> uuid.UUID:
        if self._current_bar is None:
            raise RuntimeError("no bar received yet")
        internal_order_id = uuid.uuid4()
        order_submission_event = Events.Strategy.SubmitOrder(
            occurred_at_ns=self._current_bar.occurred_at_ns,
            internal_order_id=internal_order_id,
            symbol=symbol,
            order_type=order_type,
            side=side,
            quantity=quantity,
            time_in_force=time_in_force,
            limit_price=limit_price,
            stop_price=stop_price,
        )
        self._submitted_orders[internal_order_id] = order_submission_event
        self._emit_event(order_submission_event)
        return internal_order_id

    def submit_modification(
        self,
        internal_order_id: uuid.UUID,
        quantity: int | None = None,
        limit_price: Types.ScaledPrice | None = None,
        stop_price: Types.ScaledPrice | None = None,
    ) -> None:
        if self._current_bar is None:
            raise RuntimeError("no bar received yet")
        current_working_order = self._working_orders[internal_order_id]
        event = Events.Strategy.ModifyOrder(
            occurred_at_ns=self._current_bar.occurred_at_ns,
            internal_order_id=internal_order_id,
            symbol=current_working_order.symbol,
            quantity=(
                quantity if quantity is not None else current_working_order.quantity
            ),
            limit_price=(
                limit_price
                if limit_price is not None
                else current_working_order.limit_price
            ),
            stop_price=(
                stop_price
                if stop_price is not None
                else current_working_order.stop_price
            ),
        )
        self._submitted_modifications[internal_order_id] = event
        self._emit_event(event)

    def submit_cancellation(self, internal_order_id: uuid.UUID) -> None:
        if self._current_bar is None:
            raise RuntimeError("no bar received yet")
        current_working_order = self._working_orders[internal_order_id]
        event = Events.Strategy.CancelOrder(
            occurred_at_ns=self._current_bar.occurred_at_ns,
            internal_order_id=internal_order_id,
            symbol=current_working_order.symbol,
        )
        self._submitted_cancellations[internal_order_id] = event
        self._emit_event(event)

    @property
    def position_size(self) -> int:
        if self._current_bar is None:
            raise RuntimeError("no bar received yet")
        position = self._positions.get(self._current_bar.symbol)
        return position.size if position else 0

    @property
    def flat(self) -> bool:
        return self.position_size == 0

    @property
    def cost_basis(self) -> Types.ScaledPrice | None:
        if self._current_bar is None:
            raise RuntimeError("no bar received yet")
        position = self._positions.get(self._current_bar.symbol)
        return position.cost_basis if position else None

    def _on_event(self, event: _Protocols.EventLike) -> None:
        # fmt: off
        match event:
            case Events.Datafeed.Bar()                  as event:
                self._on_bar(event)

            case Events.Broker.ExposureSnapshot()       as event:
                self._on_exposure_snapshot(event)

            case Events.Broker.OrderAccepted()          as event:
                self._on_order_accepted(event)

            case Events.Broker.OrderRejected()          as event:
                self._on_order_rejected(event)

            case Events.Broker.ModificationAccepted()   as event:
                self._on_modification_accepted(event)

            case Events.Broker.ModificationRejected()   as event:
                self._on_modification_rejected(event)

            case Events.Broker.CancellationAccepted()   as event:
                self._on_cancellation_accepted(event)

            case Events.Broker.CancellationRejected()   as event:
                self._on_cancellation_rejected(event)

            case Events.Broker.Fill()                   as event:
                self._on_fill(event)

            case Events.Broker.OrderExpired()           as event:
                self._on_order_expired(event)

            case _:
                raise RuntimeError(f"unhandled event type: {type(event).__name__}")
        # fmt: on

    def _on_bar(self, bar: Events.Datafeed.Bar) -> None:
        # Ignore bars that are not relevant to the strategy
        if bar.record_type != self.RECORD_TYPE or bar.symbol not in self.SYMBOLS:
            return

        self._current_bar = bar

        for indicator in self._indicators.values():
            indicator.update(bar)

        self.on_bar(bar)  # apply strategy logic
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
        )  # emit after `on_bar` so it does not cause delay

    def _on_exposure_snapshot(self, event: Events.Broker.ExposureSnapshot) -> None:
        self._working_orders = {
            order_id: order
            for order_id, order in event.working_orders.items()
            if order.symbol in self.SYMBOLS
        }
        self._positions = {
            symbol: position
            for symbol, position in event.positions.items()
            if symbol in self.SYMBOLS
        }

    def _on_order_accepted(self, accepted_order: Events.Broker.OrderAccepted) -> None:
        order = self._submitted_orders.pop(accepted_order.internal_order_id)
        self._working_orders[accepted_order.internal_order_id] = Exposure.WorkingOrder(
            symbol=order.symbol,
            order_type=order.order_type,
            side=order.side,
            quantity=order.quantity,
            limit_price=order.limit_price,
            stop_price=order.stop_price,
            filled_quantity=0,
        )

    def _on_order_rejected(self, rejected_order: Events.Broker.OrderRejected) -> None:
        self._submitted_orders.pop(rejected_order.internal_order_id)

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
        if fill.position_size == 0:
            self._positions.pop(fill.symbol, None)
        else:
            self._positions[fill.symbol] = Exposure.Position(
                fill.position_size, fill.position_cost_basis
            )

        working_order = self._working_orders.get(fill.internal_order_id)
        if working_order is not None:
            total_filled = int(working_order.filled_quantity + fill.filled_quantity)
            if total_filled >= working_order.quantity:
                self._working_orders.pop(fill.internal_order_id)
            else:
                self._working_orders[fill.internal_order_id] = working_order._replace(
                    filled_quantity=total_filled
                )

    def _on_order_expired(self, expired_order: Events.Broker.OrderExpired) -> None:
        self._working_orders.pop(expired_order.internal_order_id)
        self._submitted_modifications.pop(expired_order.internal_order_id, None)
        self._submitted_cancellations.pop(expired_order.internal_order_id, None)


class RunRecorderBase(_SubscriberBase, abc.ABC):
    SUBSCRIBE_TO = (
        Events.Strategy.IndicatorUpdate,
        Events.Strategy.SubmitOrder,
        Events.Strategy.ModifyOrder,
        Events.Strategy.CancelOrder,
        Events.Broker.ExposureSnapshot,
        Events.Broker.OrderAccepted,
        Events.Broker.OrderRejected,
        Events.Broker.ModificationAccepted,
        Events.Broker.ModificationRejected,
        Events.Broker.CancellationAccepted,
        Events.Broker.CancellationRejected,
        Events.Broker.Fill,
        Events.Broker.OrderExpired,
    )


class Orchestrator:
    def __init__(
        self,
        strategy_classes: list[type[StrategyBase]],
        broker: type[BrokerConnectorBase],
        datafeed: type[DatafeedConnectorBase],
        recorder: RunRecorderBase | None = None,
    ) -> None:
        self._strategy_instances = [cls() for cls in strategy_classes]
        self._broker_instance = broker()
        self._datafeed_instance = datafeed()
        self._recorder_instance = recorder
        self._shutdown_flag = threading.Event()

    def run(self) -> None:
        # SIGTERM handler requires a (signal, frame) signature, but we only
        # need to set the shutdown flag. The lambda adapts the two-arg call.
        signal.signal(signal.SIGTERM, lambda sig, frame: self._shutdown_flag.set())

        try:
            # Broker is connected first so strategies receive the `ExposureSnapshot` and
            # eventual subseq. fills before starting to trade on received market data.
            # Direct access to _shutdown_flag is intentional: the orchestrator
            # owns the flag and the connectors, so a setter adds nothing.
            self._broker_instance._shutdown_flag = self._shutdown_flag
            self._broker_instance.connect()

            self._datafeed_instance._shutdown_flag = self._shutdown_flag
            self._datafeed_instance.connect()

            for strategy in self._strategy_instances:
                self._datafeed_instance.subscribe(
                    list(strategy.SYMBOLS), strategy.RECORD_TYPE
                )

            self._shutdown_flag.wait()

        finally:
            # Each step is guarded so one failure doesn't skip the rest.
            for strategy in self._strategy_instances:
                try:
                    strategy.shutdown()
                except Exception:
                    pass

            try:
                self._datafeed_instance.disconnect()
            except Exception:
                pass

            try:
                self._broker_instance.disconnect()
            except Exception:
                pass

            if self._recorder_instance is not None:
                try:
                    self._recorder_instance.shutdown()
                except Exception:
                    pass
