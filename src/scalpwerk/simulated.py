import csv
import datetime
import threading
import typing
import uuid

from .core import (
    BrokerConnectorBase,
    Constants,
    DatafeedConnectorBase,
    Enums,
    Events,
    Exposure,
    Types,
)


class CSVDatafeedConnector(DatafeedConnectorBase):
    CSV_PATH: str
    START: str | None = None  # ISO 8601 UTC, e.g. "2020-01-06" or "2020-01-06T14:30:00"
    STOP: str | None = None  # bars outside [START, STOP) are skipped

    _DATABENTO_RTYPE_TO_BAR_PERIOD: dict[int, Enums.BarPeriod] = {
        32: Enums.BarPeriod.OHLCV_1S,
        33: Enums.BarPeriod.OHLCV_1M,
        34: Enums.BarPeriod.OHLCV_1H,
        35: Enums.BarPeriod.OHLCV_1D,
    }

    def __init__(self) -> None:
        self._subscriptions: set[tuple[Enums.BarPeriod, str]] = set()
        self._stop_event = threading.Event()
        self._streaming_thread: threading.Thread | None = None
        self._csv_file: typing.IO[str] | None = None
        self._csv_reader: typing.Any | None = None
        self._column_indices: dict[str, int] = {}

    # Called before connect(). Just collects; streaming starts in _connect().
    def subscribe(
        self,
        symbols: list[str],
        record_type: Enums.BarPeriod,
    ) -> None:
        for symbol in symbols:
            self._subscriptions.add((record_type, symbol))

    def _connect(self) -> None:
        self._start_ns = (
            self._iso_to_unix_ns(self.START) if self.START is not None else None
        )
        self._stop_ns = (
            self._iso_to_unix_ns(self.STOP) if self.STOP is not None else None
        )

        self._csv_file = open(self.CSV_PATH, newline="")
        self._csv_reader = csv.reader(self._csv_file)
        header = next(self._csv_reader)
        self._column_indices = {name: i for i, name in enumerate(header)}

        # Streaming starts here, not in subscribe(). The Orchestrator calls
        # subscribe() before connect(), so all subscriptions are set by now.
        self._stop_event.clear()
        self._streaming_thread = threading.Thread(target=self._stream)
        self._streaming_thread.start()

    @staticmethod
    def _iso_to_unix_ns(iso: str) -> int:
        dt = datetime.datetime.fromisoformat(iso)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=datetime.timezone.utc)
        return int(dt.timestamp() * 1_000_000_000)

    def _disconnect(self) -> None:
        self._stop_event.set()
        if self._streaming_thread is not None and self._streaming_thread.is_alive():
            self._streaming_thread.join()
            self._streaming_thread = None
        if self._csv_file is not None:
            self._csv_file.close()
            self._csv_file = None
        self._csv_reader = None

    def _stream(self) -> None:
        try:
            if self._csv_reader is None:
                raise RuntimeError("CSV reader not initialized")

            ts_i = self._column_indices["ts_event"]

            for row in self._csv_reader:
                if self._stop_event.is_set():
                    break

                ts_ns = int(row[ts_i])

                if self._start_ns is not None and ts_ns < self._start_ns:
                    continue
                # Data is chronological — no need to read past the stop.
                if self._stop_ns is not None and ts_ns >= self._stop_ns:
                    break

                symbol = row[self._column_indices["symbol"]]
                record_type = self._DATABENTO_RTYPE_TO_BAR_PERIOD[
                    int(row[self._column_indices["rtype"]])
                ]

                if (record_type, symbol) not in self._subscriptions:
                    continue

                self._emit_event(
                    Events.Datafeed.Bar(
                        occurred_at_ns=Types.UnixNanoseconds(ts_ns),
                        symbol=symbol,
                        record_type=record_type,
                        open=Types.ScaledPrice(int(row[self._column_indices["open"]])),
                        high=Types.ScaledPrice(int(row[self._column_indices["high"]])),
                        low=Types.ScaledPrice(int(row[self._column_indices["low"]])),
                        close=Types.ScaledPrice(
                            int(row[self._column_indices["close"]])
                        ),
                        volume=int(row[self._column_indices["volume"]]),
                    )
                )
                self._wait_until_system_idle()
        finally:
            self.trigger_shutdown()


class SimulatedBroker(BrokerConnectorBase):
    SUBSCRIBE_TO = (
        Events.Strategy.SubmitOrder,
        Events.Strategy.ModifyOrder,
        Events.Strategy.CancelOrder,
        Events.Datafeed.Bar,  # also subscribes to bars for order matching.
    )

    COMMISSION_PER_UNIT: float = 0.0  # e.g. 0.85 for $0.85/contract
    MINIMUM_COMMISSION_PER_ORDER: float = 0.0  # e.g. 1.00 for $1.00 minimum

    def __init__(self) -> None:
        self._market_orders: dict[uuid.UUID, Exposure.WorkingOrder] = {}
        self._stop_orders: dict[uuid.UUID, Exposure.WorkingOrder] = {}
        self._stop_limit_orders: dict[uuid.UUID, Exposure.WorkingOrder] = {}
        self._limit_orders: dict[uuid.UUID, Exposure.WorkingOrder] = {}

        self._positions: dict[str, Exposure.Position] = {}
        super().__init__()

    def _connect(self) -> None:
        pass

    def _disconnect(self) -> None:
        pass

    def _get_exposure_snapshot(
        self,
    ) -> tuple[dict[uuid.UUID, Exposure.WorkingOrder], dict[str, Exposure.Position]]:
        return {
            **self._stop_orders,
            **self._stop_limit_orders,
            **self._limit_orders,
        }, self._positions.copy()

    # Overrides base class to also handle bar events for order matching.
    def _on_event(self, event: typing.Any) -> None:
        if isinstance(event, Events.Datafeed.Bar):
            self._on_bar(event)
        else:
            super()._on_event(event)

    def _on_bar(self, bar: Events.Datafeed.Bar) -> None:
        self._process_market_orders(bar)
        self._process_stop_orders(bar)
        # Limit orders are processed after stop limit orders to ensure that limit orders
        # created by stop limit orders are evaluated against the same bar.
        self._process_stop_limit_orders(bar)
        self._process_limit_orders(bar)

    def _process_market_orders(self, bar: Events.Datafeed.Bar) -> None:
        # `list()` copies the keys so we can delete filled orders from the dict during
        # iteration without changing the dict's size which would raise a `RuntimeError`.
        for order_id in list(self._market_orders):
            order = self._market_orders[order_id]
            if order.symbol != bar.symbol:
                continue
            del self._market_orders[order_id]
            self._fill_order(
                order=order,
                fill_quantity=order.quantity,
                fill_price=bar.open,
                fill_occurred_at_ns=bar.occurred_at_ns,
            )

    def _process_stop_orders(self, bar: Events.Datafeed.Bar) -> None:
        for order_id in list(self._stop_orders):
            order = self._stop_orders[order_id]
            # stop_price is always set (enforced by _validate_submission);
            # the None check narrows the type for mypy.
            if order.symbol != bar.symbol or order.stop_price is None:
                continue

            if order.side == Enums.TradeSide.BUY and bar.high >= order.stop_price:
                del self._stop_orders[order_id]
                # Stop order filled at stop price, or at open price if gap up
                fill_price = Types.ScaledPrice(max(order.stop_price, bar.open))
                self._fill_order(
                    order=order,
                    fill_quantity=order.quantity,
                    fill_price=fill_price,
                    fill_occurred_at_ns=bar.occurred_at_ns,
                )

            elif order.side == Enums.TradeSide.SELL and bar.low <= order.stop_price:
                del self._stop_orders[order_id]
                # Stop order filled at stop price, or at open price if gap down
                fill_price = Types.ScaledPrice(min(order.stop_price, bar.open))
                self._fill_order(
                    order=order,
                    fill_quantity=order.quantity,
                    fill_price=fill_price,
                    fill_occurred_at_ns=bar.occurred_at_ns,
                )

    def _process_stop_limit_orders(self, bar: Events.Datafeed.Bar) -> None:
        for order_id in list(self._stop_limit_orders):
            order = self._stop_limit_orders[order_id]
            if order.symbol != bar.symbol or order.stop_price is None:
                continue

            stop_triggered = (
                order.side == Enums.TradeSide.BUY and bar.high >= order.stop_price
            ) or (order.side == Enums.TradeSide.SELL and bar.low <= order.stop_price)

            if stop_triggered:
                # Convert to limit order, evaluated in _process_limit_orders.
                del self._stop_limit_orders[order_id]
                self._limit_orders[order_id] = order._replace(
                    order_type=Enums.OrderType.LIMIT,
                    stop_price=None,
                )

    def _process_limit_orders(self, bar: Events.Datafeed.Bar) -> None:
        for order_id in list(self._limit_orders):
            order = self._limit_orders[order_id]
            if order.symbol != bar.symbol or order.limit_price is None:
                continue

            if order.side == Enums.TradeSide.BUY and bar.low <= order.limit_price:
                del self._limit_orders[order_id]
                # Limit order filled at limit price or open if gap down
                fill_price = Types.ScaledPrice(min(order.limit_price, bar.open))
                self._fill_order(
                    order=order,
                    fill_quantity=order.quantity,
                    fill_price=fill_price,
                    fill_occurred_at_ns=bar.occurred_at_ns,
                )

            elif order.side == Enums.TradeSide.SELL and bar.high >= order.limit_price:
                del self._limit_orders[order_id]
                # Limit order filled at limit price or open if gap up.
                fill_price = Types.ScaledPrice(max(order.limit_price, bar.open))
                self._fill_order(
                    order=order,
                    fill_quantity=order.quantity,
                    fill_price=fill_price,
                    fill_occurred_at_ns=bar.occurred_at_ns,
                )

    def _fill_order(
        self,
        order: Exposure.WorkingOrder,
        fill_quantity: int,
        fill_price: Types.ScaledPrice,
        fill_occurred_at_ns: Types.UnixNanoseconds,
    ) -> None:
        # Positive for buys, negative for sells — lets us compute new position
        # size with a single addition: current.size + signed_fill_quantity.
        signed_fill_quantity = (
            fill_quantity if order.side == Enums.TradeSide.BUY else -fill_quantity
        )

        # Commission always makes the effective price worse:
        # higher for buys, lower for sells.
        commission_scaled = int(
            max(
                fill_quantity * self.COMMISSION_PER_UNIT,
                self.MINIMUM_COMMISSION_PER_ORDER,
            )
            * Constants.PRICE_SCALE
        )
        if order.side == Enums.TradeSide.BUY:
            commission_adjusted_fill_price = Types.ScaledPrice(
                fill_price + commission_scaled // fill_quantity
            )
        else:
            commission_adjusted_fill_price = Types.ScaledPrice(
                fill_price - commission_scaled // fill_quantity
            )

        # Update position.
        current_position = self._positions.get(order.symbol)

        if current_position is None:
            # No existing position — new position at fill price.
            new_size = signed_fill_quantity
            new_cost_basis = commission_adjusted_fill_price
            self._positions[order.symbol] = Exposure.Position(
                size=new_size,
                cost_basis=new_cost_basis,
            )
        else:
            # Existing position — compute new size and cost basis.
            new_size = current_position.size + signed_fill_quantity
            adding_to_position = (current_position.size > 0) == (
                signed_fill_quantity > 0
            )

            if new_size == 0:
                new_cost_basis = Types.ScaledPrice(0)
            elif adding_to_position:
                # Weighted average cost basis.
                new_cost_basis = Types.ScaledPrice(
                    (
                        abs(current_position.size) * current_position.cost_basis
                        + abs(signed_fill_quantity) * commission_adjusted_fill_price
                    )
                    // abs(new_size)
                )
            elif abs(signed_fill_quantity) > abs(current_position.size):
                # Flipped position: cost basis is the fill price.
                new_cost_basis = commission_adjusted_fill_price
            else:
                # Reducing position: cost basis unchanged.
                new_cost_basis = current_position.cost_basis

            if new_size == 0:
                self._positions.pop(order.symbol, None)
            else:
                self._positions[order.symbol] = Exposure.Position(
                    size=new_size,
                    cost_basis=new_cost_basis,
                )

        # noinspection PyArgumentList
        self._emit_event(
            Events.Broker.Fill(
                occurred_at_ns=fill_occurred_at_ns,
                internal_order_id=order.internal_order_id,
                symbol=order.symbol,
                internal_fill_id=uuid.uuid4(),
                side=order.side,
                filled_quantity=fill_quantity,
                fill_price=fill_price,
                position_size=new_size,
                position_cost_basis=new_cost_basis,
            )
        )

    def _on_submit_order(self, event: Events.Strategy.SubmitOrder) -> None:
        rejection = self._validate_order_fields(
            order_type=event.order_type,
            quantity=event.quantity,
            limit_price=event.limit_price,
            stop_price=event.stop_price,
        )
        if rejection is not None:
            # noinspection PyArgumentList
            self._emit_event(
                Events.Broker.OrderRejected(
                    occurred_at_ns=event.occurred_at_ns,
                    internal_order_id=event.internal_order_id,
                    reason=rejection,
                )
            )
            return

        working_order = Exposure.WorkingOrder(
            internal_order_id=event.internal_order_id,
            symbol=event.symbol,
            order_type=event.order_type,
            side=event.side,
            quantity=event.quantity,
            time_in_force=event.time_in_force,
            limit_price=event.limit_price,
            stop_price=event.stop_price,
            filled_quantity=0,
        )

        match event.order_type:
            case Enums.OrderType.MARKET:
                self._market_orders[event.internal_order_id] = working_order
            case Enums.OrderType.LIMIT:
                self._limit_orders[event.internal_order_id] = working_order
            case Enums.OrderType.STOP:
                self._stop_orders[event.internal_order_id] = working_order
            case Enums.OrderType.STOP_LIMIT:
                self._stop_limit_orders[event.internal_order_id] = working_order
            case _:
                raise RuntimeError(f"unhandled order type: {event.order_type}")

        # noinspection PyArgumentList
        self._emit_event(
            Events.Broker.OrderAccepted(
                occurred_at_ns=event.occurred_at_ns,
                internal_order_id=event.internal_order_id,
            )
        )

    def _on_modify_order(self, event: Events.Strategy.ModifyOrder) -> None:
        for orders in (
            self._market_orders,
            self._stop_orders,
            self._stop_limit_orders,
            self._limit_orders,
        ):
            if event.internal_order_id not in orders:
                continue

            existing = orders[event.internal_order_id]
            rejection = self._validate_order_fields(
                order_type=existing.order_type,
                quantity=event.quantity,
                limit_price=event.limit_price,
                stop_price=event.stop_price,
            )
            if rejection is not None:
                # noinspection PyArgumentList
                self._emit_event(
                    Events.Broker.ModificationRejected(
                        occurred_at_ns=event.occurred_at_ns,
                        internal_order_id=event.internal_order_id,
                        reason=rejection,
                    )
                )
                return

            orders[event.internal_order_id] = existing._replace(
                quantity=event.quantity,
                limit_price=event.limit_price,
                stop_price=event.stop_price,
            )
            # noinspection PyArgumentList
            self._emit_event(
                Events.Broker.ModificationAccepted(
                    occurred_at_ns=event.occurred_at_ns,
                    internal_order_id=event.internal_order_id,
                )
            )
            return

        # noinspection PyArgumentList
        self._emit_event(
            Events.Broker.ModificationRejected(
                occurred_at_ns=event.occurred_at_ns,
                internal_order_id=event.internal_order_id,
                reason="order not found",
            )
        )

    def _on_cancel_order(self, event: Events.Strategy.CancelOrder) -> None:
        for orders in (
            self._market_orders,
            self._stop_orders,
            self._stop_limit_orders,
            self._limit_orders,
        ):
            if event.internal_order_id in orders:
                del orders[event.internal_order_id]
                # noinspection PyArgumentList
                self._emit_event(
                    Events.Broker.CancellationAccepted(
                        occurred_at_ns=event.occurred_at_ns,
                        internal_order_id=event.internal_order_id,
                    )
                )
                return

        # noinspection PyArgumentList
        self._emit_event(
            Events.Broker.CancellationRejected(
                occurred_at_ns=event.occurred_at_ns,
                internal_order_id=event.internal_order_id,
                reason="order not found",
            )
        )

    @staticmethod
    def _validate_order_fields(
        order_type: Enums.OrderType,
        quantity: int,
        limit_price: Types.ScaledPrice | None,
        stop_price: Types.ScaledPrice | None,
    ) -> str | None:
        if quantity <= 0:
            return "quantity must be positive"

        match order_type:
            case Enums.OrderType.MARKET:
                if limit_price is not None or stop_price is not None:
                    return "market order cannot have limit or stop price"
            case Enums.OrderType.LIMIT:
                if limit_price is None:
                    return "limit order requires limit price"
                if stop_price is not None:
                    return "limit order cannot have stop price"
            case Enums.OrderType.STOP:
                if stop_price is None:
                    return "stop order requires stop price"
                if limit_price is not None:
                    return "stop order cannot have limit price"
            case Enums.OrderType.STOP_LIMIT:
                if limit_price is None or stop_price is None:
                    return "stop-limit order requires both limit and stop price"
            case _:
                return f"unsupported order type: {order_type}"

        return None
