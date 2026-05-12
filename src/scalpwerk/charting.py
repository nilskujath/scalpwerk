"""Post-backtest trade charting.

Reads the SQLite database (single source of truth) and renders per-trade
PNG charts with candlesticks, indicators, trade markers, and cost basis lines.
No recorder or event bus integration — pure post-processing.

Requires: pip install scalpwerk[charts]
"""

from __future__ import annotations

import dataclasses
import pathlib
import sqlite3

from .core import Constants


@dataclasses.dataclass(frozen=True, slots=True)
class _Bar:
    occurred_at_ns: int
    open: float
    high: float
    low: float
    close: float
    volume: int | None


@dataclasses.dataclass(frozen=True, slots=True)
class _Fill:
    occurred_at_ns: int
    side: str  # "BUY" or "SELL"
    filled_quantity: int
    fill_price: float
    position_size: int
    position_cost_basis: float


@dataclasses.dataclass(frozen=True, slots=True)
class _RoundTrip:
    symbol: str
    fills: list[_Fill]
    pnl: float
    entry_ns: int
    exit_ns: int
    direction: str  # "LONG" or "SHORT"


def _load_bars(
    conn: sqlite3.Connection,
    run_id: str,
    symbol: str,
) -> list[_Bar]:
    rows = conn.execute(
        """
        SELECT occurred_at_ns, open, high, low, close, volume
        FROM datafeed_bar
        WHERE run_id = ? AND symbol = ?
        ORDER BY occurred_at_ns
        """,
        (run_id, symbol),
    ).fetchall()
    scale = Constants.PRICE_SCALE
    return [
        _Bar(
            occurred_at_ns=r[0],
            open=r[1] / scale,
            high=r[2] / scale,
            low=r[3] / scale,
            close=r[4] / scale,
            volume=r[5],
        )
        for r in rows
    ]


def _load_fills(
    conn: sqlite3.Connection,
    run_id: str,
    symbol: str,
) -> list[_Fill]:
    rows = conn.execute(
        """
        SELECT occurred_at_ns, side, filled_quantity, fill_price,
               position_size, position_cost_basis
        FROM broker_fill
        WHERE run_id = ? AND symbol = ?
        ORDER BY occurred_at_ns
        """,
        (run_id, symbol),
    ).fetchall()
    scale = Constants.PRICE_SCALE
    return [
        _Fill(
            occurred_at_ns=r[0],
            side=r[1],
            filled_quantity=r[2],
            fill_price=r[3] / scale,
            position_size=r[4],
            position_cost_basis=r[5] / scale,
        )
        for r in rows
    ]


def _load_indicator_config(
    conn: sqlite3.Connection,
    run_id: str,
) -> dict[str, int]:
    rows = conn.execute(
        "SELECT indicator_name, plot_at FROM indicator_config WHERE run_id = ?",
        (run_id,),
    ).fetchall()
    return {r[0]: r[1] for r in rows}


def _load_indicator_values(
    conn: sqlite3.Connection,
    run_id: str,
    symbol: str,
) -> dict[str, dict[int, float]]:
    """Returns {indicator_name: {occurred_at_ns: value}}."""
    rows = conn.execute(
        """
        SELECT indicator_name, occurred_at_ns, indicator_value
        FROM strategy_indicator_value
        WHERE run_id = ? AND symbol = ?
        ORDER BY occurred_at_ns
        """,
        (run_id, symbol),
    ).fetchall()
    result: dict[str, dict[int, float]] = {}
    for name, ts, value in rows:
        result.setdefault(name, {})[ts] = value
    return result


def _detect_round_trips(fills: list[_Fill], symbol: str) -> list[_RoundTrip]:
    """Walk fills chronologically, detect position open→flat transitions."""
    round_trips: list[_RoundTrip] = []
    current_fills: list[_Fill] = []
    prev_position_size = 0

    for fill in fills:
        # Trade starts: flat → non-flat
        if prev_position_size == 0 and fill.position_size != 0:
            current_fills = [fill]
        elif current_fills:
            current_fills.append(fill)

        # Trade completes: non-flat → flat
        if fill.position_size == 0 and current_fills:
            # P&L from cash flows: sells add cash, buys subtract
            pnl = sum(
                f.fill_price * f.filled_quantity * (1 if f.side == "SELL" else -1)
                for f in current_fills
            )
            direction = "LONG" if current_fills[0].side == "BUY" else "SHORT"
            round_trips.append(
                _RoundTrip(
                    symbol=symbol,
                    fills=list(current_fills),
                    pnl=pnl,
                    entry_ns=current_fills[0].occurred_at_ns,
                    exit_ns=current_fills[-1].occurred_at_ns,
                    direction=direction,
                )
            )
            current_fills = []

        prev_position_size = fill.position_size

    return round_trips


def _render_trade(
    trade: _RoundTrip,
    trade_index: int,
    bars: list[_Bar],
    indicator_config: dict[str, int],
    indicator_values: dict[str, dict[int, float]],
    output_dir: pathlib.Path,
    context_bars: int,
) -> pathlib.Path:
    """Render a single trade as a PNG. Returns the output path."""
    import matplotlib.dates as mdates
    import matplotlib.patches as mpatches
    import matplotlib.pyplot as plt
    from matplotlib.path import Path as MplPath
    from datetime import datetime, timezone

    # Find bar indices for the trade window
    bar_timestamps = [b.occurred_at_ns for b in bars]
    first_fill_ns = trade.entry_ns
    last_fill_ns = trade.exit_ns

    # Binary search for entry/exit bar indices
    entry_idx = 0
    exit_idx = len(bars) - 1
    for i, ts in enumerate(bar_timestamps):
        if ts <= first_fill_ns:
            entry_idx = i
        if ts <= last_fill_ns:
            exit_idx = i

    # Expand by context_bars
    start_idx = max(0, entry_idx - context_bars)
    end_idx = min(len(bars) - 1, exit_idx + context_bars)
    window_bars = bars[start_idx : end_idx + 1]

    if not window_bars:
        return output_dir / f"trade_{trade_index + 1:04d}.png"

    # Convert nanosecond timestamps to datetime for x-axis
    bar_dates = [
        datetime.fromtimestamp(b.occurred_at_ns / 1e9, tz=timezone.utc)
        for b in window_bars
    ]
    bar_ns_list = [b.occurred_at_ns for b in window_bars]

    # Compute bar width from actual bar spacing
    bar_date_nums = [mdates.date2num(d) for d in bar_dates]
    if len(bar_date_nums) >= 2:
        spacings = [
            bar_date_nums[i + 1] - bar_date_nums[i]
            for i in range(len(bar_date_nums) - 1)
        ]
        bar_width = sorted(spacings)[len(spacings) // 2]  # median
    else:
        bar_width = 1 / 86400  # fallback: 1 second
    rect_width = bar_width * 0.8

    # Determine panel layout from indicator_config
    above_panels: dict[int, list[str]] = {}  # plot_at < 0
    below_panels: dict[int, list[str]] = {}  # plot_at > 0
    overlay_indicators: list[str] = []  # plot_at == 0

    for name, plot_at in indicator_config.items():
        if name not in indicator_values:
            continue
        if plot_at == 0:
            overlay_indicators.append(name)
        elif plot_at < 0:
            above_panels.setdefault(plot_at, []).append(name)
        else:
            below_panels.setdefault(plot_at, []).append(name)

    # Sort: above panels sorted descending (e.g. -2 above -1)
    above_keys = sorted(above_panels.keys())  # [-2, -1]
    below_keys = sorted(below_panels.keys())  # [1, 2]
    n_above = len(above_keys)
    n_below = len(below_keys)
    n_panels = n_above + 1 + n_below  # above + price + below

    # Height ratios: above (1 each), price (3), below (1 each)
    height_ratios = [1] * n_above + [3] + [1] * n_below
    fig_height = 8 + 2 * (n_above + n_below)

    fig, axes = plt.subplots(
        n_panels,
        1,
        figsize=(14, fig_height),
        height_ratios=height_ratios,
        sharex=True,
        squeeze=False,
    )
    flat_axes = [ax[0] for ax in axes]
    price_ax_idx = n_above
    price_ax = flat_axes[price_ax_idx]

    # -- Draw candlesticks on price panel --
    for i, bar in enumerate(window_bars):
        date = mdates.date2num(bar_dates[i])
        color = "black"
        is_up = bar.close > bar.open

        # Body
        body_bottom = min(bar.open, bar.close)
        body_height = abs(bar.close - bar.open)
        if body_height == 0:
            body_height = (bar.high - bar.low) * 0.01 or 0.001
        body_top = body_bottom + body_height

        # Wicks (below and above body only)
        price_ax.plot(
            [date, date],
            [bar.low, body_bottom],
            color="black",
            linewidth=0.8,
            alpha=0.7,
        )
        price_ax.plot(
            [date, date],
            [body_top, bar.high],
            color="black",
            linewidth=0.8,
            alpha=0.7,
        )

        rect = mpatches.FancyBboxPatch(
            (date - rect_width / 2, body_bottom),
            rect_width,
            body_height,
            boxstyle="square,pad=0",
            facecolor="white" if is_up else "black",
            edgecolor="black",
            linewidth=0.5,
            alpha=0.7,
        )
        price_ax.add_patch(rect)

    # -- Trade highlighting: entry-to-exit lightblue rectangle --
    entry_date = mdates.date2num(
        datetime.fromtimestamp(trade.entry_ns / 1e9, tz=timezone.utc)
    )
    exit_date = mdates.date2num(
        datetime.fromtimestamp(trade.exit_ns / 1e9, tz=timezone.utc)
    )
    for ax in flat_axes:
        ax.axvspan(entry_date, exit_date, alpha=0.2, color="lightblue")

    # -- Trade markers on price panel --
    for fill in trade.fills:
        fill_date = mdates.date2num(
            datetime.fromtimestamp(fill.occurred_at_ns / 1e9, tz=timezone.utc)
        )
        if fill.side == "BUY":
            # Tip at top (0,0), body hangs below
            verts = [(0, 0), (-1, -2), (1, -2), (0, 0)]
            marker = MplPath(
                verts,
                [MplPath.MOVETO, MplPath.LINETO, MplPath.LINETO, MplPath.CLOSEPOLY],
            )
            color = "green"
        else:
            # Tip at bottom (0,0), body hangs above
            verts = [(0, 0), (-1, 2), (1, 2), (0, 0)]
            marker = MplPath(
                verts,
                [MplPath.MOVETO, MplPath.LINETO, MplPath.LINETO, MplPath.CLOSEPOLY],
            )
            color = "red"
        size = max(40, fill.filled_quantity * 20)

        price_ax.scatter(
            fill_date,
            fill.fill_price,
            marker=marker,
            color=color,
            s=size,
            zorder=5,
            edgecolors="black",
            linewidth=0.5,
        )
        price_ax.annotate(
            str(fill.filled_quantity),
            (fill_date, fill.fill_price),
            fontsize=8,
            fontweight="bold",
            ha="center",
            va="bottom" if fill.side == "BUY" else "top",
            bbox=dict(
                boxstyle="round,pad=0.2", facecolor="white", edgecolor="none", alpha=0.8
            ),
            xytext=(0, 8 if fill.side == "BUY" else -8),
            textcoords="offset points",
        )

    # -- Cost basis line --
    in_trade = False
    cost_basis_dates: list[float] = []
    cost_basis_values: list[float] = []

    for fill in trade.fills:
        fill_date = mdates.date2num(
            datetime.fromtimestamp(fill.occurred_at_ns / 1e9, tz=timezone.utc)
        )
        if not in_trade:
            in_trade = True

        if fill.position_size != 0:
            cost_basis_dates.append(fill_date)
            cost_basis_values.append(fill.position_cost_basis)
        else:
            # Extend previous cost basis to exit timestamp (horizontal line to exit)
            if cost_basis_values:
                cost_basis_dates.append(fill_date)
                cost_basis_values.append(cost_basis_values[-1])

        if fill.position_size == 0:
            in_trade = False
        else:
            # Extend cost basis to next fill (or exit)
            next_fills = [
                f for f in trade.fills if f.occurred_at_ns > fill.occurred_at_ns
            ]
            if next_fills:
                next_date = mdates.date2num(
                    datetime.fromtimestamp(
                        next_fills[0].occurred_at_ns / 1e9, tz=timezone.utc
                    )
                )
                cost_basis_dates.append(next_date)
                cost_basis_values.append(fill.position_cost_basis)

    if cost_basis_dates:
        price_ax.plot(
            cost_basis_dates,
            cost_basis_values,
            color="blue",
            linewidth=1.0,
            linestyle="--",
            label="Cost Basis",
            zorder=4,
        )

    # -- Overlay indicators on price panel --
    for name in overlay_indicators:
        vals = indicator_values.get(name, {})
        ind_dates = []
        ind_values = []
        for bar_ns, bar_date in zip(bar_ns_list, bar_dates):
            if bar_ns in vals:
                ind_dates.append(mdates.date2num(bar_date))
                ind_values.append(vals[bar_ns])
        if ind_dates:
            price_ax.plot(ind_dates, ind_values, linewidth=1.2, label=name)

    price_ax.set_ylabel("Price", fontsize=10)
    price_ax.legend(loc="upper left", fontsize=8)
    price_ax.grid(True, alpha=0.3)

    # Force y-limits from bar data so cost basis zeros don't drag axis down
    price_low = min(b.low for b in window_bars)
    price_high = max(b.high for b in window_bars)
    margin = (price_high - price_low) * 0.05 or 1.0
    price_ax.set_ylim(price_low - margin, price_high + margin)

    # -- Above-price indicator panels --
    for panel_idx, panel_key in enumerate(above_keys):
        ax = flat_axes[panel_idx]
        for name in above_panels[panel_key]:
            vals = indicator_values.get(name, {})
            ind_dates = []
            ind_values = []
            for bar_ns, bar_date in zip(bar_ns_list, bar_dates):
                if bar_ns in vals:
                    ind_dates.append(mdates.date2num(bar_date))
                    ind_values.append(vals[bar_ns])
            if ind_dates:
                ax.plot(ind_dates, ind_values, linewidth=1.2, label=name)
        ax.set_ylabel(", ".join(above_panels[panel_key]), fontsize=10)
        ax.legend(loc="upper left", fontsize=8)
        ax.grid(True, alpha=0.3)

    # -- Below-price indicator panels --
    for panel_offset, panel_key in enumerate(below_keys):
        ax = flat_axes[price_ax_idx + 1 + panel_offset]
        for name in below_panels[panel_key]:
            vals = indicator_values.get(name, {})
            ind_dates = []
            ind_values = []
            for bar_ns, bar_date in zip(bar_ns_list, bar_dates):
                if bar_ns in vals:
                    ind_dates.append(mdates.date2num(bar_date))
                    ind_values.append(vals[bar_ns])
            if ind_dates:
                ax.plot(ind_dates, ind_values, linewidth=1.2, label=name)
        ax.set_ylabel(", ".join(below_panels[panel_key]), fontsize=10)
        ax.legend(loc="upper left", fontsize=8)
        ax.grid(True, alpha=0.3)

    # -- X-axis formatting (bottom panel only) --
    bottom_ax = flat_axes[-1]
    bottom_ax.xaxis.set_major_formatter(mdates.DateFormatter("%m/%d %H:%M"))
    plt.setp(bottom_ax.xaxis.get_majorticklabels(), rotation=45, fontsize=9)

    # -- Title --
    duration_s = (trade.exit_ns - trade.entry_ns) / 1e9
    outcome = "WIN" if trade.pnl >= 0 else "LOSS"
    fig.suptitle(
        f"{trade.symbol} - {trade.direction} - {outcome} - "
        f"P&L: ${trade.pnl:.2f} - Duration: {duration_s:.0f}s",
        fontsize=14,
    )

    fig.tight_layout()
    output_path = output_dir / f"trade_{trade_index + 1:04d}.png"
    fig.savefig(output_path, dpi=500, bbox_inches="tight")
    plt.close(fig)
    return output_path


def render_trades(
    db_path: str | pathlib.Path,
    run_id: str | None = None,
    output_dir: str | pathlib.Path = "trade_charts",
    context_bars: int = 30,
) -> list[pathlib.Path]:
    """Render per-trade charts from a backtest SQLite database.

    Args:
        db_path: Path to the SQLite database file.
        run_id: Specific run to chart. If None, uses the most recent run.
        output_dir: Directory to save PNG files.
        context_bars: Number of bars to show before/after the trade.

    Returns:
        List of paths to generated PNG files.
    """
    db_path = pathlib.Path(db_path)
    output_dir = pathlib.Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(db_path)
    try:
        if run_id is None:
            row = conn.execute(
                "SELECT run_id FROM runs ORDER BY rowid DESC LIMIT 1"
            ).fetchone()
            if row is None:
                raise ValueError(f"No runs found in {db_path}")
            run_id = row[0]

        # Load indicator config
        indicator_config = _load_indicator_config(conn, run_id)

        # Get all symbols with fills in this run
        symbols = [
            r[0]
            for r in conn.execute(
                "SELECT DISTINCT symbol FROM broker_fill WHERE run_id = ?",
                (run_id,),
            ).fetchall()
        ]

        output_paths: list[pathlib.Path] = []
        trade_counter = 0

        for symbol in symbols:
            bars = _load_bars(conn, run_id, symbol)
            fills = _load_fills(conn, run_id, symbol)
            ind_values = _load_indicator_values(conn, run_id, symbol)

            if not bars or not fills:
                continue

            round_trips = _detect_round_trips(fills, symbol)

            for rt in round_trips:
                path = _render_trade(
                    trade=rt,
                    trade_index=trade_counter,
                    bars=bars,
                    indicator_config=indicator_config,
                    indicator_values=ind_values,
                    output_dir=output_dir,
                    context_bars=context_bars,
                )
                output_paths.append(path)
                trade_counter += 1

    finally:
        conn.close()

    return output_paths
