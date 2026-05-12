import math
import pathlib
import sqlite3
import uuid

from .core import (
    Events,
    RecorderBase,
)


class SQLiteRecorder(RecorderBase):
    DB_PATH: pathlib.Path = pathlib.Path("runs.db")

    _SCHEMA_VERSION = 6

    _SCHEMA = """
        CREATE TABLE IF NOT EXISTS runs (
            run_id TEXT NOT NULL PRIMARY KEY
        );

        CREATE TABLE IF NOT EXISTS datafeed_bar (
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

        CREATE TABLE IF NOT EXISTS strategy_indicator_value (
            run_id          TEXT    NOT NULL,
            symbol          TEXT    NOT NULL,
            record_type     TEXT    NOT NULL,
            occurred_at_ns  INTEGER NOT NULL,
            indicator_name  TEXT    NOT NULL,
            indicator_value REAL    NOT NULL,
            PRIMARY KEY (run_id, symbol, record_type, occurred_at_ns, indicator_name),
            FOREIGN KEY (run_id, symbol, record_type, occurred_at_ns)
                REFERENCES datafeed_bar(run_id, symbol, record_type, occurred_at_ns)
        );

        CREATE TABLE IF NOT EXISTS strategy_submit_order (
            run_id            TEXT    NOT NULL REFERENCES runs(run_id),
            occurred_at_ns    INTEGER NOT NULL,
            created_at_ns     INTEGER NOT NULL,
            internal_order_id TEXT    NOT NULL,
            symbol            TEXT    NOT NULL,
            order_type        TEXT    NOT NULL,
            side              TEXT    NOT NULL,
            quantity          INTEGER NOT NULL,
            time_in_force     TEXT    NOT NULL,
            limit_price       INTEGER,
            stop_price        INTEGER,
            PRIMARY KEY (run_id, internal_order_id)
        );

        CREATE TABLE IF NOT EXISTS strategy_modify_order (
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

        CREATE TABLE IF NOT EXISTS strategy_cancel_order (
            run_id            TEXT    NOT NULL REFERENCES runs(run_id),
            occurred_at_ns    INTEGER NOT NULL,
            created_at_ns     INTEGER NOT NULL,
            internal_order_id TEXT    NOT NULL,
            symbol            TEXT    NOT NULL,
            PRIMARY KEY (run_id, internal_order_id, occurred_at_ns)
        );

        CREATE TABLE IF NOT EXISTS broker_exposure_snapshot (
            run_id         TEXT    NOT NULL REFERENCES runs(run_id),
            occurred_at_ns INTEGER NOT NULL,
            created_at_ns  INTEGER NOT NULL,
            PRIMARY KEY (run_id, occurred_at_ns)
        );

        CREATE TABLE IF NOT EXISTS broker_exposure_snapshot_working_order (
            run_id            TEXT    NOT NULL,
            occurred_at_ns    INTEGER NOT NULL,
            internal_order_id TEXT    NOT NULL,
            symbol            TEXT    NOT NULL,
            order_type        TEXT    NOT NULL,
            side              TEXT    NOT NULL,
            quantity          INTEGER NOT NULL,
            time_in_force     TEXT    NOT NULL,
            limit_price       INTEGER,
            stop_price        INTEGER,
            filled_quantity   INTEGER NOT NULL,
            PRIMARY KEY (run_id, occurred_at_ns, internal_order_id),
            FOREIGN KEY (run_id, occurred_at_ns)
                REFERENCES broker_exposure_snapshot(run_id, occurred_at_ns)
        );

        CREATE TABLE IF NOT EXISTS broker_exposure_snapshot_position (
            run_id         TEXT    NOT NULL,
            occurred_at_ns INTEGER NOT NULL,
            symbol         TEXT    NOT NULL,
            size           INTEGER NOT NULL,
            cost_basis     INTEGER NOT NULL,
            PRIMARY KEY (run_id, occurred_at_ns, symbol),
            FOREIGN KEY (run_id, occurred_at_ns)
                REFERENCES broker_exposure_snapshot(run_id, occurred_at_ns)
        );

        CREATE TABLE IF NOT EXISTS broker_order_accepted (
            run_id            TEXT    NOT NULL REFERENCES runs(run_id),
            occurred_at_ns    INTEGER NOT NULL,
            created_at_ns     INTEGER NOT NULL,
            internal_order_id TEXT    NOT NULL,
            PRIMARY KEY (run_id, internal_order_id)
        );

        CREATE TABLE IF NOT EXISTS broker_order_rejected (
            run_id            TEXT    NOT NULL REFERENCES runs(run_id),
            occurred_at_ns    INTEGER NOT NULL,
            created_at_ns     INTEGER NOT NULL,
            internal_order_id TEXT    NOT NULL,
            reason            TEXT    NOT NULL,
            PRIMARY KEY (run_id, internal_order_id)
        );

        CREATE TABLE IF NOT EXISTS broker_modification_accepted (
            run_id            TEXT    NOT NULL REFERENCES runs(run_id),
            occurred_at_ns    INTEGER NOT NULL,
            created_at_ns     INTEGER NOT NULL,
            internal_order_id TEXT    NOT NULL,
            PRIMARY KEY (run_id, internal_order_id, occurred_at_ns)
        );

        CREATE TABLE IF NOT EXISTS broker_modification_rejected (
            run_id            TEXT    NOT NULL REFERENCES runs(run_id),
            occurred_at_ns    INTEGER NOT NULL,
            created_at_ns     INTEGER NOT NULL,
            internal_order_id TEXT    NOT NULL,
            reason            TEXT    NOT NULL,
            PRIMARY KEY (run_id, internal_order_id, occurred_at_ns)
        );

        CREATE TABLE IF NOT EXISTS broker_cancellation_accepted (
            run_id            TEXT    NOT NULL REFERENCES runs(run_id),
            occurred_at_ns    INTEGER NOT NULL,
            created_at_ns     INTEGER NOT NULL,
            internal_order_id TEXT    NOT NULL,
            PRIMARY KEY (run_id, internal_order_id, occurred_at_ns)
        );

        CREATE TABLE IF NOT EXISTS broker_cancellation_rejected (
            run_id            TEXT    NOT NULL REFERENCES runs(run_id),
            occurred_at_ns    INTEGER NOT NULL,
            created_at_ns     INTEGER NOT NULL,
            internal_order_id TEXT    NOT NULL,
            reason            TEXT    NOT NULL,
            PRIMARY KEY (run_id, internal_order_id, occurred_at_ns)
        );

        CREATE TABLE IF NOT EXISTS broker_fill (
            run_id              TEXT    NOT NULL REFERENCES runs(run_id),
            occurred_at_ns      INTEGER NOT NULL,
            created_at_ns       INTEGER NOT NULL,
            internal_order_id   TEXT    NOT NULL,
            symbol              TEXT    NOT NULL,
            internal_fill_id    TEXT    NOT NULL,
            side                TEXT    NOT NULL,
            filled_quantity     INTEGER NOT NULL,
            fill_price          INTEGER NOT NULL,
            position_size       INTEGER NOT NULL,
            position_cost_basis INTEGER NOT NULL,
            PRIMARY KEY (run_id, internal_fill_id)
        );

        CREATE TABLE IF NOT EXISTS broker_order_expired (
            run_id            TEXT    NOT NULL REFERENCES runs(run_id),
            occurred_at_ns    INTEGER NOT NULL,
            created_at_ns     INTEGER NOT NULL,
            internal_order_id TEXT    NOT NULL,
            PRIMARY KEY (run_id, internal_order_id)
        );

        CREATE TABLE IF NOT EXISTS indicator_config (
            run_id         TEXT    NOT NULL REFERENCES runs(run_id),
            indicator_name TEXT    NOT NULL,
            plot_at        INTEGER NOT NULL,
            PRIMARY KEY (run_id, indicator_name)
        );
    """

    # fmt: off
    _SQL_INSERT_BAR = """
        INSERT OR IGNORE INTO datafeed_bar
            (run_id, occurred_at_ns, created_at_ns, symbol, record_type,
             open, high, low, close, volume)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    _SQL_INSERT_INDICATOR_VALUE = """
        INSERT INTO strategy_indicator_value
            (run_id, symbol, record_type, occurred_at_ns,
             indicator_name, indicator_value)
        VALUES (?, ?, ?, ?, ?, ?)
    """

    _SQL_INSERT_SUBMIT_ORDER = """
        INSERT INTO strategy_submit_order
            (run_id, occurred_at_ns, created_at_ns, internal_order_id, symbol,
             order_type, side, quantity, time_in_force, limit_price, stop_price)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    _SQL_INSERT_MODIFY_ORDER = """
        INSERT INTO strategy_modify_order
            (run_id, occurred_at_ns, created_at_ns, internal_order_id, symbol,
             quantity, limit_price, stop_price)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """

    _SQL_INSERT_CANCEL_ORDER = """
        INSERT INTO strategy_cancel_order
            (run_id, occurred_at_ns, created_at_ns, internal_order_id, symbol)
        VALUES (?, ?, ?, ?, ?)
    """

    _SQL_INSERT_EXPOSURE_SNAPSHOT = """
        INSERT INTO broker_exposure_snapshot
            (run_id, occurred_at_ns, created_at_ns)
        VALUES (?, ?, ?)
    """

    _SQL_INSERT_EXPOSURE_SNAPSHOT_WORKING_ORDER = """
        INSERT INTO broker_exposure_snapshot_working_order
            (run_id, occurred_at_ns, internal_order_id, symbol, order_type,
             side, quantity, time_in_force, limit_price, stop_price, filled_quantity)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    _SQL_INSERT_EXPOSURE_SNAPSHOT_POSITION = """
        INSERT INTO broker_exposure_snapshot_position
            (run_id, occurred_at_ns, symbol, size, cost_basis)
        VALUES (?, ?, ?, ?, ?)
    """

    _SQL_INSERT_ORDER_ACCEPTED = """
        INSERT INTO broker_order_accepted
            (run_id, occurred_at_ns, created_at_ns, internal_order_id)
        VALUES (?, ?, ?, ?)
    """

    _SQL_INSERT_ORDER_REJECTED = """
        INSERT INTO broker_order_rejected
            (run_id, occurred_at_ns, created_at_ns, internal_order_id, reason)
        VALUES (?, ?, ?, ?, ?)
    """

    _SQL_INSERT_MODIFICATION_ACCEPTED = """
        INSERT INTO broker_modification_accepted
            (run_id, occurred_at_ns, created_at_ns, internal_order_id)
        VALUES (?, ?, ?, ?)
    """

    _SQL_INSERT_MODIFICATION_REJECTED = """
        INSERT INTO broker_modification_rejected
            (run_id, occurred_at_ns, created_at_ns, internal_order_id, reason)
        VALUES (?, ?, ?, ?, ?)
    """

    _SQL_INSERT_CANCELLATION_ACCEPTED = """
        INSERT INTO broker_cancellation_accepted
            (run_id, occurred_at_ns, created_at_ns, internal_order_id)
        VALUES (?, ?, ?, ?)
    """

    _SQL_INSERT_CANCELLATION_REJECTED = """
        INSERT INTO broker_cancellation_rejected
            (run_id, occurred_at_ns, created_at_ns, internal_order_id, reason)
        VALUES (?, ?, ?, ?, ?)
    """

    _SQL_INSERT_FILL = """
        INSERT INTO broker_fill
            (run_id, occurred_at_ns, created_at_ns, internal_order_id,
             symbol, internal_fill_id, side, filled_quantity, fill_price,
             position_size, position_cost_basis)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    _SQL_INSERT_ORDER_EXPIRED = """
        INSERT INTO broker_order_expired
            (run_id, occurred_at_ns, created_at_ns, internal_order_id)
        VALUES (?, ?, ?, ?)
    """

    _SQL_INSERT_INDICATOR_CONFIG = """
        INSERT OR IGNORE INTO indicator_config
            (run_id, indicator_name, plot_at)
        VALUES (?, ?, ?)
    """

    _SQL_INSERT_RUN = """
        INSERT INTO runs (run_id) VALUES (?)
    """

    # fmt: on

    def __init__(self) -> None:
        self._run_id: str = str(uuid.uuid4())
        self._conn: sqlite3.Connection | None = None
        self._indicator_config_written: bool = False
        super().__init__()

    def _setup_db(self) -> None:
        # Schema version is embedded in the filename so different versions never
        # collide. Old data is preserved in the old file.
        path = self.DB_PATH.with_stem(f"{self.DB_PATH.stem}_v{self._SCHEMA_VERSION}")
        self._conn = sqlite3.connect(path)

        self._conn.execute("PRAGMA journal_mode = WAL")
        self._conn.execute("PRAGMA synchronous = NORMAL")
        self._conn.execute("PRAGMA foreign_keys = ON")
        self._conn.execute("PRAGMA busy_timeout = 5000")

        self._conn.executescript(self._SCHEMA)
        self._conn.execute(self._SQL_INSERT_RUN, (self._run_id,))
        self._conn.commit()

    # Opens the DB connection before and closes it after the parent event loop
    # so it lives on the same thread that uses it since SQLite is not thread-safe.
    def _event_loop(self) -> None:
        self._setup_db()

        try:
            super()._event_loop()
        finally:
            if self._conn is not None:
                self._conn.close()

    def _on_event(self, event: object) -> None:
        if self._conn is None:
            raise RuntimeError("DB connection not initialized")
        self._record(event)

    def _record(self, event: object) -> None:
        if self._conn is None:
            raise RuntimeError("DB connection not initialized")

        match event:
            case Events.Strategy.IndicatorUpdate() as e:
                if not self._indicator_config_written:
                    self._indicator_config_written = True
                    for strategy in self._strategies:
                        self._conn.executemany(
                            self._SQL_INSERT_INDICATOR_CONFIG,
                            [
                                (str(self._run_id), name, plot_at)
                                for name, plot_at in strategy._indicator_plot_at.items()
                            ],
                        )

                src = e.source_event
                self._conn.execute(
                    self._SQL_INSERT_BAR,
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
                        if not math.isnan(value)
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
                        e.time_in_force.name,
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

            case Events.Broker.ExposureSnapshot() as e:
                self._conn.execute(
                    self._SQL_INSERT_EXPOSURE_SNAPSHOT,
                    (
                        str(self._run_id),
                        int(e.occurred_at_ns),
                        int(e.created_at_ns),
                    ),
                )
                self._conn.executemany(
                    self._SQL_INSERT_EXPOSURE_SNAPSHOT_WORKING_ORDER,
                    [
                        (
                            str(self._run_id),
                            int(e.occurred_at_ns),
                            str(wo.internal_order_id),
                            wo.symbol,
                            wo.order_type.name,
                            wo.side.name,
                            int(wo.quantity),
                            wo.time_in_force.name,
                            int(wo.limit_price) if wo.limit_price is not None else None,
                            int(wo.stop_price) if wo.stop_price is not None else None,
                            int(wo.filled_quantity),
                        )
                        for wo in e.working_orders.values()
                    ],
                )
                self._conn.executemany(
                    self._SQL_INSERT_EXPOSURE_SNAPSHOT_POSITION,
                    [
                        (
                            str(self._run_id),
                            int(e.occurred_at_ns),
                            symbol,
                            int(pos.size),
                            int(pos.cost_basis),
                        )
                        for symbol, pos in e.positions.items()
                    ],
                )

            case Events.Broker.OrderAccepted() as e:
                self._conn.execute(
                    self._SQL_INSERT_ORDER_ACCEPTED,
                    (
                        str(self._run_id),
                        int(e.occurred_at_ns),
                        int(e.created_at_ns),
                        str(e.internal_order_id),
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
                        str(e.symbol),
                        str(e.internal_fill_id),
                        e.side.name,
                        int(e.filled_quantity),
                        int(e.fill_price),
                        int(e.position_size),
                        int(e.position_cost_basis),
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
                    ),
                )

            case _:
                return  # skip, not raise — recorder is non-critical

        self._conn.commit()
