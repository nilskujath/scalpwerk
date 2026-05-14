from abc import ABC, abstractmethod
from collections import defaultdict, deque
from dataclasses import dataclass, field
from enum import Enum, auto
from queue import Queue
from threading import Thread
from time import time_ns
from typing import Protocol
from uuid import UUID

PRICE_SCALE_FACTOR = 1_000_000_000

type NanosecondsSinceUnixEpoch = int
type ScaledPrice = int


class PeriodType(Enum):
    # fmt: off
    SECOND  = auto()
    MINUTE  = auto()
    HOUR    = auto()
    DAY     = auto()
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

        @dataclass(frozen=True, kw_only=True)
        class ModifyOrder(_EventBase):
            # fmt: off
            symbol:         str
            order_id:       UUID
            quantity:       int
            limit_price:    ScaledPrice | None = None
            stop_price:     ScaledPrice | None = None
            # fmt: on

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
        class ModificationAccepted(_EventBase):
            # fmt: off
            symbol:         str
            order_id:       UUID
            # fmt: on

        @dataclass(frozen=True, kw_only=True)
        class ModificationRejected(_EventBase):
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


class _ComponentBase(_ComponentLike, ABC):
    SUBSCRIBE_TO: tuple[type[_EventBase], ...] = ()

    def __init__(self, event_bus: _EventBus = _system_event_bus) -> None:
        self._event_bus: _EventBus = event_bus
        self._event_bus.subscribe(self, *self.SUBSCRIBE_TO)
        self._queue: Queue[_EventBase] = Queue()
        self._thread: Thread = Thread(target=self._event_loop, name=type(self).__name__)
        self._thread.start()

    def receive(self, event: _EventBase) -> None:
        self._queue.put(event)

    def emit(self, event: _EventBase) -> None:
        self._event_bus.publish(event)

    def _event_loop(self):
        while True:
            event = self._queue.get()
            if event is None:
                self._queue.task_done()
                break
            self._on_event(event)
            self._queue.task_done()

    @abstractmethod
    def _on_event(self, event: _EventBase) -> None: ...


class _Connectable(ABC):
    @abstractmethod
    def _connect(self) -> None: ...

    @abstractmethod
    def _disconnect(self) -> None: ...


class RecorderBase(_ComponentBase, ABC): ...


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
                self._connect()


class _IndicatorBase(ABC):
    def __init__(self, max_history: int = 100) -> None:
        self._max_history = max(1, int(max_history))
        self._history: dict[str, deque[float]] = {}
        self._input_indicators: dict[str, "_IndicatorBase"] = {}

    @property
    @abstractmethod
    def name(self) -> str: ...

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

    def __getitem__(self, key: tuple[str, int]) -> float:  # `self.sma["ES", -1]`
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
        Events.Broker.ModificationAccepted,
        Events.Broker.ModificationRejected,
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

        self.setup()
        self.emit(
            Events.Strategy.StreamRequest(
                period_type=self.PERIOD_TYPE, symbols=frozenset(self.SYMBOLS)
            )
        )

    @abstractmethod
    def setup(self): ...

    def add_indicator(self, indicator: _IndicatorBase) -> _IndicatorBase:
        self._indicators[indicator.name] = indicator
        return indicator  # for inline assignment: `self.sma = self.add_indicator(...)`

    @abstractmethod
    def on_bar(self, event: Events.Datafeed.Bar) -> None: ...

    # TODO
    def submit_order(self):
        pass

    # TODO
    def submit_modification(self):
        pass

    # TODO
    def submit_cancel(self):
        pass

    def _on_event(self, event: _EventBase) -> None:
        # fmt: off
        match event:
            case Events.Datafeed.Bar()                  as event:
                self._on_bar(event)

            case Events.Broker.BrokerConnected()        as event:
                self._on_broker_connected(event)

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
        # fmt: on

    def _on_bar(self, bar: Events.Datafeed.Bar) -> None:
        self._current_bar = bar

        for indicator in self._indicators.values():
            indicator.update(bar)

        self.on_bar(bar)

        self.emit(
            Events.Strategy.IndicatorUpdate(
                symbol=bar.symbol,
                source_event=bar,
                ind_values={
                    name: indicator.latest(bar.symbol)
                    for name, indicator in self._indicators.items()
                },
            )
        )

    def _on_broker_connected(self, event: Events.Broker.BrokerConnected) -> None:
        self._working_orders = event.working_orders
        self._open_positions = event.open_positions

    # TODO
    def _on_order_accepted(self, event: Events.Broker.OrderAccepted) -> None:
        pass

    # TODO
    def _on_order_rejected(self, event: Events.Broker.OrderRejected) -> None:
        pass

    # TODO
    def _on_modification_accepted(
        self, event: Events.Broker.ModificationAccepted
    ) -> None:
        pass

    # TODO
    def _on_modification_rejected(
        self, event: Events.Broker.ModificationRejected
    ) -> None:
        pass

    # TODO
    def _on_cancellation_accepted(
        self, event: Events.Broker.CancellationAccepted
    ) -> None:
        pass

    # TODO
    def _on_cancellation_rejected(
        self, event: Events.Broker.CancellationRejected
    ) -> None:
        pass

    # TODO
    def _on_fill(self, event: Events.Broker.Fill) -> None:
        pass

    # TODO
    def _on_order_expired(self, event: Events.Broker.OrderExpired) -> None:
        pass


class BrokerConnectorBase(_ComponentBase, _Connectable):
    SUBSCRIBE_TO: tuple[type[_EventBase], ...] = (
        Events.Strategy.SubmitOrder,
        Events.Strategy.ModifyOrder,
        Events.Strategy.CancelOrder,
    )

    def __init__(self, event_bus: _EventBus = _system_event_bus):
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
            case Events.Strategy.ModifyOrder() as event:
                self._on_modify_order(event)
            case Events.Strategy.CancelOrder() as event:
                self._on_cancel_order(event)

    @abstractmethod
    def _on_submit_order(self, event: Events.Strategy.SubmitOrder) -> None: ...

    @abstractmethod
    def _on_modify_order(self, event: Events.Strategy.ModifyOrder) -> None: ...

    @abstractmethod
    def _on_cancel_order(self, event: Events.Strategy.CancelOrder) -> None: ...


if __name__ == "__main__":
    dummy_recorder = ...
    dummy_datafeed = ...
    dummy_strategy = ...
    dummy_broker = ...
