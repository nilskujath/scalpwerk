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
    class BrokerRequestBase(EventBase):
        internal_order_id: uuid.UUID
        symbol: str

    @dataclass(kw_only=True, frozen=True, slots=True)
    class SubmitOrder(BrokerRequestBase):
        order_type: OrderType
        side: TradeSide
        quantity: float
        limit_price: int | None = None
        stop_price: int | None = None

    @dataclass(kw_only=True, frozen=True, slots=True)
    class ModifyOrder(BrokerRequestBase):
        quantity: float
        limit_price: int | None = None
        stop_price: int | None = None

    @dataclass(kw_only=True, frozen=True, slots=True)
    class CancelOrder(BrokerRequestBase):
        pass


class BrokerResponse:

    @dataclass(kw_only=True, frozen=True, slots=True)
    class BrokerResponseBase(EventBase):
        internal_order_id: uuid.UUID
        broker_order_id: str | int | None = None

    @dataclass(kw_only=True, frozen=True, slots=True)
    class CancellationAccepted(BrokerResponseBase):
        pass

    @dataclass(kw_only=True, frozen=True, slots=True)
    class CancellationRejected(BrokerResponseBase):
        reason: str

    @dataclass(kw_only=True, frozen=True, slots=True)
    class ModificationAccepted(BrokerResponseBase):
        pass

    @dataclass(kw_only=True, frozen=True, slots=True)
    class ModificationRejected(BrokerResponseBase):
        reason: str

    @dataclass(kw_only=True, frozen=True, slots=True)
    class OrderAccepted(BrokerResponseBase):
        pass

    @dataclass(kw_only=True, frozen=True, slots=True)
    class OrderRejected(BrokerResponseBase):
        reason: str

    @dataclass(kw_only=True, frozen=True, slots=True)
    class Fill(BrokerResponseBase):
        internal_fill_id: uuid.UUID
        broker_fill_id: str | int | None = None
        side: TradeSide
        filled_quantity: float
        fill_price: int
        commission: int | None = None
        exchange: str = "SIMULATED"

    @dataclass(kw_only=True, frozen=True, slots=True)
    class OrderExpired(BrokerResponseBase):
        pass
