import uuid

from dataclasses import dataclass

from .models import OrderType, TradeSide


@dataclass(kw_only=True, frozen=True, slots=True)
class EventBase:
    occurred_at_ns: int
    created_at_ns: int


class MarketUpdate:

    @dataclass(kw_only=True, frozen=True, slots=True)
    class OHLCV(EventBase):
        symbol: str
        record_type: str
        open: int
        high: int
        low: int
        close: int
        volume: int | None = None


class BrokerRequest:

    @dataclass(kw_only=True, frozen=True, slots=True)
    class SubmitOrder(EventBase):
        internal_order_id: uuid.UUID
        symbol: str
        order_type: OrderType
        side: TradeSide
        quantity: float
        limit_price: int | None = None
        stop_price: int | None = None

    @dataclass(kw_only=True, frozen=True, slots=True)
    class ModifyOrder(EventBase):
        internal_order_id: uuid.UUID
        symbol: str
        quantity: float
        limit_price: int | None = None
        stop_price: int | None = None

    @dataclass(kw_only=True, frozen=True, slots=True)
    class CancelOrder(EventBase):
        internal_order_id: uuid.UUID
        symbol: str


class BrokerResponse:

    @dataclass(kw_only=True, frozen=True, slots=True)
    class CancellationAccepted(EventBase):
        internal_order_id: uuid.UUID
        broker_order_id: str | None = None

    @dataclass(kw_only=True, frozen=True, slots=True)
    class CancellationRejected(EventBase):
        internal_order_id: uuid.UUID
        broker_order_id: str | None = None
        reason: str

    @dataclass(kw_only=True, frozen=True, slots=True)
    class ModificationAccepted(EventBase):
        internal_order_id: uuid.UUID
        broker_order_id: str | None = None

    @dataclass(kw_only=True, frozen=True, slots=True)
    class ModificationRejected(EventBase):
        internal_order_id: uuid.UUID
        broker_order_id: str | None = None
        reason: str

    @dataclass(kw_only=True, frozen=True, slots=True)
    class OrderAccepted(EventBase):
        internal_order_id: uuid.UUID
        broker_order_id: str | None = None

    @dataclass(kw_only=True, frozen=True, slots=True)
    class OrderRejected(EventBase):
        internal_order_id: uuid.UUID
        broker_order_id: str | None = None
        reason: str

    @dataclass(kw_only=True, frozen=True, slots=True)
    class Fill(EventBase):
        internal_order_id: uuid.UUID
        broker_order_id: str | None = None
        internal_fill_id: uuid.UUID
        broker_fill_id: str | None = None
        side: TradeSide
        filled_quantity: float
        fill_price: int
        commission: int | None = None
        exchange: str = "SIMULATED"

    @dataclass(kw_only=True, frozen=True, slots=True)
    class OrderExpired(EventBase):
        internal_order_id: uuid.UUID
        broker_order_id: str | None = None
