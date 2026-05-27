from collections import deque
from dataclasses import replace
from uuid import UUID, uuid4


from .core import (
    NanosecondsSinceUnixEpoch,
    ScaledPrice,
    Symbol,
    Quantity,
    SignedQuantity,
    OrderType,
    TradeSide,
    WorkingOrder,
    OpenPosition,
    EventBase,
    Events,
    BrokerConnectorBase,
)


class SimulatedBrokerConnector(BrokerConnectorBase):
    SUBSCRIBE_TO: tuple[type[EventBase], ...] = (
        Events.Strategy.SubmitOrder,
        Events.Strategy.CancelOrder,
        Events.Datafeed.Bar,  # also subscribe to bars for order matching
    )

    # fmt: off
    COMMISSION_PER_UNIT:        float = 0.0
    MIN_COMMISSION_PER_ORDER:   float = 0.0
    # fmt: on

    def __init__(self) -> None:
        self._working_orders: dict[UUID, WorkingOrder] = {}  # incl. market orders
        self._open_entries: dict[Symbol, deque[tuple[SignedQuantity, ScaledPrice]]] = {}
        super().__init__()

    def _connect(self) -> None:  # called via `super().__init__()`
        pass

    def _disconnect(self) -> None:  # called at component shutdown from `_event_loop`
        pass

    def _exposure_snapshot(
        self,
    ) -> tuple[dict[UUID, WorkingOrder], dict[Symbol, OpenPosition]]:
        return self._working_orders.copy(), {
            symbol: OpenPosition(
                symbol=symbol,
                signed_qty=sum(qty for qty, _ in open_entries),
                cost_basis=(
                    sum(qty * price for qty, price in open_entries)
                    // sum(qty for qty, _ in open_entries)
                ),
            )
            for symbol, open_entries in self._open_entries.items()
        }

    def _on_event(self, event: EventBase) -> None:
        if isinstance(event, Events.Datafeed.Bar):
            self._on_bar(event)  # we need to handle bars for simulated order matching
        else:
            super()._on_event(event)

    def _on_submit_order(self, event: Events.Strategy.SubmitOrder) -> None:
        reason: str | None = None
        match event.order_type:
            case OrderType.STOP if event.stop_price is None:
                reason = "stop order requires stop_price"
            case OrderType.LIMIT if event.limit_price is None:
                reason = "limit order requires limit_price"
            case OrderType.STOP_LIMIT if (
                event.stop_price is None or event.limit_price is None
            ):
                reason = "stop-limit order requires both prices"
        if reason:
            self.emit(
                Events.Broker.OrderRejected(
                    timestamp=event.timestamp,
                    symbol=event.symbol,
                    order_id=event.order_id,
                    reason=reason,
                )
            )
            return

        self._working_orders[event.order_id] = WorkingOrder(
            symbol=event.symbol,
            order_id=event.order_id,
            order_type=event.order_type,
            trade_side=event.trade_side,
            qty=event.qty,
            filled_qty=0,
            time_in_force=event.time_in_force,
            limit_price=event.limit_price,
            stop_price=event.stop_price,
        )
        self.emit(
            Events.Broker.OrderAccepted(
                timestamp=event.timestamp,  # for charting purposes
                symbol=event.symbol,
                order_id=event.order_id,
            )
        )

    def _on_cancel_order(self, event: Events.Strategy.CancelOrder) -> None:
        if event.order_id in self._working_orders:
            del self._working_orders[event.order_id]
            self.emit(
                Events.Broker.CancellationAccepted(
                    timestamp=event.timestamp,
                    symbol=event.symbol,
                    order_id=event.order_id,
                )
            )
        else:
            self.emit(
                Events.Broker.CancellationRejected(
                    timestamp=event.timestamp,
                    symbol=event.symbol,
                    order_id=event.order_id,
                    reason="order not found",
                )
            )

    def _on_bar(self, bar: Events.Datafeed.Bar) -> None:
        for order in list(self._working_orders.values()):
            if order.symbol != bar.symbol:
                continue
            match order.order_type:
                case OrderType.MARKET:
                    self._match_mkt(order, bar)
                case OrderType.STOP:
                    self._match_stp(order, bar)
                case OrderType.STOP_LIMIT:
                    self._match_stp_lmt(order, bar)
                case OrderType.LIMIT:
                    self._match_lmt(order, bar)

    def _match_mkt(self, order: WorkingOrder, bar: Events.Datafeed.Bar) -> None:
        self._execute_order(order, bar.open, bar.period_start)

    def _match_stp(self, order: WorkingOrder, bar: Events.Datafeed.Bar) -> None:
        assert order.stop_price is not None
        stp = order.stop_price
        if order.trade_side is TradeSide.BUY and bar.high >= stp:
            self._execute_order(order, max(stp, bar.open), bar.period_start)
        elif order.trade_side is TradeSide.SELL and bar.low <= stp:
            self._execute_order(order, min(stp, bar.open), bar.period_start)

    def _match_stp_lmt(self, order: WorkingOrder, bar: Events.Datafeed.Bar) -> None:
        assert order.stop_price is not None and order.limit_price is not None
        stp, lmt = order.stop_price, order.limit_price
        if order.trade_side is TradeSide.BUY and bar.high >= stp:
            lmt_order = replace(order, order_type=OrderType.LIMIT, stop_price=None)
            self._working_orders[order.order_id] = lmt_order
            if bar.low <= lmt:
                self._execute_order(lmt_order, min(lmt, bar.open), bar.period_start)
        elif order.trade_side is TradeSide.SELL and bar.low <= stp:
            lmt_order = replace(order, order_type=OrderType.LIMIT, stop_price=None)
            self._working_orders[order.order_id] = lmt_order
            if bar.high >= lmt:
                self._execute_order(lmt_order, max(lmt, bar.open), bar.period_start)

    def _match_lmt(self, order: WorkingOrder, bar: Events.Datafeed.Bar) -> None:
        assert order.limit_price is not None
        lmt = order.limit_price
        if order.trade_side is TradeSide.BUY and bar.low <= lmt:
            self._execute_order(order, min(lmt, bar.open), bar.period_start)
        elif order.trade_side is TradeSide.SELL and bar.high >= lmt:
            self._execute_order(order, max(lmt, bar.open), bar.period_start)

    def _execute_order(
        self,
        order: WorkingOrder,
        fill_price: ScaledPrice,
        fill_timestamp: NanosecondsSinceUnixEpoch,
    ) -> None:
        del self._working_orders[order.order_id]

        signed_fill_qty: SignedQuantity = (
            order.qty if order.trade_side is TradeSide.BUY else -order.qty
        )
        open_entries = self._open_entries.get(order.symbol, deque())
        open_position_qty: SignedQuantity = sum(qty for qty, _ in open_entries)

        # CASE: no position -> create first entry
        if not open_entries:
            self._open_entries[order.symbol] = deque([(signed_fill_qty, fill_price)])

        # CASE: fill adds to position -> append new entry
        elif open_position_qty * signed_fill_qty > 0:
            open_entries.append((signed_fill_qty, fill_price))

        # CASE: fill exactly closes position -> flatten
        elif order.qty == abs(open_position_qty):
            self._open_entries.pop(order.symbol, None)

        # CASE: fill partially reduces position -> consume in FIFO order
        elif order.qty < abs(open_position_qty):
            qty_to_consume: Quantity = order.qty
            while qty_to_consume > 0:
                entry_qty, entry_price = open_entries[0]
                if qty_to_consume >= abs(entry_qty):
                    qty_to_consume -= abs(entry_qty)
                    self._open_entries[order.symbol].popleft()
                else:
                    self._open_entries[order.symbol][0] = (
                        (
                            entry_qty - qty_to_consume
                            if entry_qty > 0
                            else entry_qty + qty_to_consume
                        ),
                        entry_price,
                    )
                    qty_to_consume = 0

        # CASE: fill exceeds position -> flatten and open new position
        elif order.qty > abs(open_position_qty):
            self._open_entries.pop(order.symbol, None)
            remaining: Quantity = order.qty - abs(open_position_qty)
            remaining_signed: SignedQuantity = (
                remaining if signed_fill_qty > 0 else -remaining
            )
            self._open_entries[order.symbol] = deque([(remaining_signed, fill_price)])

        updated_open_entries = self._open_entries.get(order.symbol, deque())
        self.emit(
            Events.Broker.Fill(
                timestamp=fill_timestamp,
                symbol=order.symbol,
                fill_id=uuid4(),
                order_id=order.order_id,
                trade_side=order.trade_side,
                filled_qty=order.qty,
                fill_price=fill_price,
                signed_position_size=sum(q for q, _ in updated_open_entries),
                position_cost_basis=(
                    sum(q * p for q, p in updated_open_entries)
                    // sum(q for q, _ in updated_open_entries)
                    if updated_open_entries
                    else None
                ),
            )
        )
