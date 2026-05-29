import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

from datetime import datetime, timezone
from enum import Enum, auto
from matplotlib.figure import Figure
from matplotlib.path import Path as MplPath

from .core import (
    EventBase,
    Events,
    NanosecondsSinceUnixEpoch,
    Symbol,
    PlotGroup,
    PeriodType,
    PRICE_SCALE_FACTOR,
    TradeSide,
    OpenPosition,
    IndicatorName,
    IndicatorReading,
)

type MplDate = float
type PanelLayout = list[PlotGroup]


class BarStyle(Enum):
    CANDLESTICK = auto()
    BAR = auto()
    CBAR = auto()
    OCBAR = auto()


def chart(
    events_to_chart: list[EventBase],
    for_symbol: Symbol,
    bar_style: BarStyle = BarStyle.CANDLESTICK,
) -> Figure:

    # ——— FILTER AND CLASSIFY EVENTS ——————————————————————————————————————————————————
    updates: list[Events.Strategy.IndicatorUpdate] = []
    fills: list[Events.Broker.Fill] = []
    starting_position: OpenPosition | None = None

    for event in events_to_chart:
        match event:
            case Events.Broker.BrokerConnected() as broker_event:
                starting_position = broker_event.open_positions.get(for_symbol)
            case _ if getattr(event, "symbol", None) != for_symbol:
                continue
            case Events.Strategy.IndicatorUpdate():
                updates.append(event)
            case Events.Broker.Fill():
                fills.append(event)

    if not updates:
        raise ValueError()

    # ——— CREATE FIGURE ————————————————————————————————————————————————————————————————
    has_fills: bool = bool(fills)
    panel_layout: PanelLayout = _infer_panel_layout(updates[0])
    fig: Figure = _create_unpopulated_figure(
        panel_layout,
        updates[0].source_event.period_type,
        has_fills,
    )

    if has_fills:
        position_axis: plt.Axes | None = fig.axes[0]
        plot_group_to_axis: dict[PlotGroup, plt.Axes] = dict(
            zip(panel_layout, fig.axes[1:])
        )
    else:
        position_axis = None
        plot_group_to_axis = dict(zip(panel_layout, fig.axes))

    # ——— DRAW CHART ELEMENTS ——————————————————————————————————————————————————————————
    match bar_style:
        case BarStyle.CANDLESTICK:
            _draw_candlesticks(plot_group_to_axis[0], updates)
        case BarStyle.BAR:
            _draw_bars(plot_group_to_axis[0], updates)
        case BarStyle.CBAR:
            _draw_cbars(plot_group_to_axis[0], updates)
        case BarStyle.OCBAR:
            _draw_ocbars(plot_group_to_axis[0], updates)
    _draw_indicators(plot_group_to_axis, updates)
    _draw_fills(plot_group_to_axis, fills)
    _draw_costbases_and_plboxes(plot_group_to_axis, fills, updates)
    _draw_position_background(plot_group_to_axis, fills, updates)
    if position_axis is not None:
        _draw_position_size(position_axis, fills, updates, starting_position)

    # ——— FORMAT FIGURE ————————————————————————————————————————————————————————————————
    _format_figure(fig, updates, for_symbol)

    return fig


def _format_figure(
    fig: Figure,
    updates: list[Events.Strategy.IndicatorUpdate],
    for_symbol: Symbol,
) -> None:
    first_bar_x: MplDate = _nanoseconds_to_mpldate(updates[0].source_event.period_start)
    last_bar_x: MplDate = _nanoseconds_to_mpldate(updates[-1].source_event.period_start)
    bar_padding: float = updates[0].source_event.period_type.duration_in_days
    fig.axes[0].set_xlim(first_bar_x - bar_padding, last_bar_x + bar_padding)

    fig.suptitle(
        f"{for_symbol}  {updates[0].source_event.period_type.name}  "
        f"{datetime.fromtimestamp(updates[0].source_event.period_start / 1_000_000_000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}"
        f" to "
        f"{datetime.fromtimestamp(updates[-1].source_event.period_start / 1_000_000_000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}",
        fontsize=10,
    )

    fig.tight_layout()


def _nanoseconds_to_mpldate(ns_since_epoch: NanosecondsSinceUnixEpoch) -> MplDate:
    seconds = ns_since_epoch / 1_000_000_000
    utc_datetime = datetime.fromtimestamp(seconds, tz=timezone.utc)
    return mdates.date2num(utc_datetime)


def _infer_panel_layout(
    representative_update_event: Events.Strategy.IndicatorUpdate,
) -> PanelLayout:
    return [
        p
        for p in sorted(
            {0}
            | {
                indicator_reading.plot_group
                for indicator_reading in representative_update_event.ind_values.values()
                if indicator_reading.plot_group is not None
            }
        )
    ]


def _create_unpopulated_figure(
    panel_layout: PanelLayout,
    period_type: PeriodType,
    include_position_panel: bool = False,
) -> Figure:
    # ——— CREATE SUBPLOTS (ACCORDING TO HEIGHT RATIOS) —————————————————————————————————
    height_ratios: list[float] = [9 if panel == 0 else 3 for panel in panel_layout]
    if include_position_panel:
        height_ratios = [1.5] + height_ratios
    fig, _ = plt.subplots(
        len(height_ratios),
        1,
        figsize=(16, sum(height_ratios)),
        height_ratios=height_ratios,
        sharex=True,
        squeeze=False,
    )

    # ——— FORMAT X-AXIS (ACCORDING TO BARS' PERIOD TYPE) ———————————————————————————————
    _format_x_axis(fig, period_type)
    return fig


def _format_x_axis(fig: Figure, period_type: PeriodType) -> None:
    tick_locator: dict[PeriodType, mdates.DateLocator] = {
        PeriodType.SECOND: mdates.SecondLocator(bysecond=range(0, 60, 10)),
        PeriodType.MINUTE: mdates.MinuteLocator(byminute=range(0, 60, 10)),
        PeriodType.HOUR: mdates.HourLocator(byhour=range(0, 24, 8)),
        PeriodType.DAY: mdates.DayLocator(interval=1),
    }
    bottom_axis = fig.axes[-1]
    bottom_axis.xaxis.set_major_locator(tick_locator[period_type])
    bottom_axis.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d %H:%M:%S"))
    plt.setp(bottom_axis.xaxis.get_majorticklabels(), rotation=90, fontsize=8)


def _draw_candlesticks(
    price_axis: plt.Axes,
    updates: list[Events.Strategy.IndicatorUpdate],
) -> None:
    # Matplotlib's x-axis is in days; candle should be 0.8 of bar duration to leave gaps
    representative_bar: Events.Datafeed.Bar = updates[0].source_event
    candle_width: float = representative_bar.period_type.duration_in_days * 0.8

    for update in updates:
        # fmt: off
        bar         : Events.Datafeed.Bar   = update.source_event
        x           : MplDate               = _nanoseconds_to_mpldate(bar.period_start)
        bar_open    : float                 = bar.open  / PRICE_SCALE_FACTOR
        bar_high    : float                 = bar.high  / PRICE_SCALE_FACTOR
        bar_low     : float                 = bar.low   / PRICE_SCALE_FACTOR
        bar_close   : float                 = bar.close / PRICE_SCALE_FACTOR
        # fmt: on

        # ——— DRAW CANDLE BODY —————————————————————————————————————————————————————————
        price_axis.add_patch(
            mpatches.FancyBboxPatch(
                (x - candle_width / 2, min(bar_open, bar_close)),
                candle_width,
                abs(bar_close - bar_open) or (bar_high - bar_low) * 0.01,
                boxstyle="square,pad=0",
                facecolor="white" if bar_close > bar_open else "black",
                edgecolor="black",
                lw=0.5,
                alpha=0.7,
            )
        )

        # ——— DRAW LOWER WICK (IF IT EXISTS) ———————————————————————————————————————————
        if min(bar_open, bar_close) > bar_low:
            price_axis.plot(
                [x, x],
                [bar_low, min(bar_open, bar_close)],
                color="black",
                lw=0.8,
                alpha=0.7,
            )

        # ——— DRAW UPPER WICK (IF IT EXISTS) ———————————————————————————————————————————
        if max(bar_open, bar_close) < bar_high:
            price_axis.plot(
                [x, x],
                [max(bar_open, bar_close), bar_high],
                color="black",
                lw=0.8,
                alpha=0.7,
            )


def _draw_bars(
    price_axis: plt.Axes,
    updates: list[Events.Strategy.IndicatorUpdate],
) -> None:
    for update in updates:
        bar: Events.Datafeed.Bar = update.source_event
        x: MplDate = _nanoseconds_to_mpldate(bar.period_start)
        bar_high: float = bar.high / PRICE_SCALE_FACTOR
        bar_low: float = bar.low / PRICE_SCALE_FACTOR

        # High-low vertical line
        price_axis.plot(
            [x, x],
            [bar_low, bar_high],
            color="black",
            lw=0.8,
            alpha=0.7,
        )


def _draw_cbars(
    price_axis: plt.Axes,
    updates: list[Events.Strategy.IndicatorUpdate],
) -> None:
    tick_width: float = updates[0].source_event.period_type.duration_in_days * 0.3

    for update in updates:
        bar: Events.Datafeed.Bar = update.source_event
        x: MplDate = _nanoseconds_to_mpldate(bar.period_start)
        bar_high: float = bar.high / PRICE_SCALE_FACTOR
        bar_low: float = bar.low / PRICE_SCALE_FACTOR
        bar_close: float = bar.close / PRICE_SCALE_FACTOR

        # High-low vertical line
        price_axis.plot(
            [x, x],
            [bar_low, bar_high],
            color="black",
            lw=0.8,
            alpha=0.7,
        )
        # Close tick (right)
        price_axis.plot(
            [x, x + tick_width],
            [bar_close, bar_close],
            color="black",
            lw=0.8,
            alpha=0.7,
        )


def _draw_ocbars(
    price_axis: plt.Axes,
    updates: list[Events.Strategy.IndicatorUpdate],
) -> None:
    tick_width: float = updates[0].source_event.period_type.duration_in_days * 0.3

    for update in updates:
        bar: Events.Datafeed.Bar = update.source_event
        x: MplDate = _nanoseconds_to_mpldate(bar.period_start)
        bar_open: float = bar.open / PRICE_SCALE_FACTOR
        bar_high: float = bar.high / PRICE_SCALE_FACTOR
        bar_low: float = bar.low / PRICE_SCALE_FACTOR
        bar_close: float = bar.close / PRICE_SCALE_FACTOR

        # High-low vertical line
        price_axis.plot(
            [x, x],
            [bar_low, bar_high],
            color="black",
            lw=0.8,
            alpha=0.7,
        )
        # Open tick (left)
        price_axis.plot(
            [x - tick_width, x],
            [bar_open, bar_open],
            color="black",
            lw=0.8,
            alpha=0.7,
        )
        # Close tick (right)
        price_axis.plot(
            [x, x + tick_width],
            [bar_close, bar_close],
            color="black",
            lw=0.8,
            alpha=0.7,
        )


def _draw_indicators(
    plot_group_to_axis: dict[PlotGroup, plt.Axes],
    updates: list[Events.Strategy.IndicatorUpdate],
) -> None:

    # All updates carry the same indicators; any can provide needed indicator metadata
    indicator_metadata: dict[IndicatorName, IndicatorReading] = updates[0].ind_values

    # ——— DRAW ALL INDICATORS ——————————————————————————————————————————————————————————
    for indicator_name, indicator_reading in indicator_metadata.items():
        if indicator_reading.plot_group is None:
            continue
        # ——— COLLECT INDICATOR DATAPOINTS FOR CURRENT INDICATOR ———————————————————————
        plot_x: list[MplDate] = []
        plot_y: list[float] = []
        for update in updates:
            value: float = update.ind_values[indicator_name].value
            if value != value:  # check if nan
                continue
            plot_x.append(_nanoseconds_to_mpldate(update.source_event.period_start))
            plot_y.append(
                value / PRICE_SCALE_FACTOR if indicator_reading.is_scaled else value
            )
        # ——— DRAW CURRENT INDICATOR ———————————————————————————————————————————————————
        plot_group_to_axis[indicator_reading.plot_group].plot(
            plot_x,
            plot_y,
            lw=1.2,
            label=indicator_name,
        )

    # ——— DRAW LEGEND FOR ALL INDICATORS ———————————————————————————————————————————————
    for axis in plot_group_to_axis.values():
        if axis.get_legend_handles_labels()[1]:
            axis.legend(loc="upper left", fontsize=8)


def _draw_fills(
    plot_group_to_axis: dict[PlotGroup, plt.Axes],
    fills: list[Events.Broker.Fill],
) -> None:
    # ——— CREATE FILL MARKERS ——————————————————————————————————————————————————————————
    buy_fill_marker = MplPath(
        [(0, 0), (-1, -2), (1, -2), (0, 0)],
        [MplPath.MOVETO, MplPath.LINETO, MplPath.LINETO, MplPath.CLOSEPOLY],
    )
    sell_fill_marker = MplPath(
        [(0, 0), (-1, 2), (1, 2), (0, 0)],
        [MplPath.MOVETO, MplPath.LINETO, MplPath.LINETO, MplPath.CLOSEPOLY],
    )

    # ——— DRAW FILL MARKERS ————————————————————————————————————————————————————————————
    for fill in fills:
        fill_is_buy = fill.trade_side is TradeSide.BUY
        plot_group_to_axis[0].scatter(
            _nanoseconds_to_mpldate(fill.timestamp),
            fill.fill_price / PRICE_SCALE_FACTOR,
            marker=(buy_fill_marker if fill_is_buy else sell_fill_marker),
            color="green" if fill_is_buy else "red",
            s=80,
            zorder=5,
            edgecolors="green" if fill_is_buy else "red",
            lw=0.5,
        )


def _draw_position_size(
    position_axis: plt.Axes,
    fills: list[Events.Broker.Fill],
    updates: list[Events.Strategy.IndicatorUpdate],
    starting_position: OpenPosition | None,
) -> None:
    # ——— SETUP TIME GRID AND FILL LOOKUP ——————————————————————————————————————————————
    bar_width: float = updates[0].source_event.period_type.duration_in_days * 0.8
    first_ts: NanosecondsSinceUnixEpoch = updates[0].source_event.period_start
    last_ts: NanosecondsSinceUnixEpoch = updates[-1].source_event.period_start
    step_ns: int = updates[0].source_event.period_type.duration_in_nanoseconds
    fill_at: dict[NanosecondsSinceUnixEpoch, int] = {
        f.timestamp: f.signed_position_size for f in fills
    }

    # ——— DRAW POSITION SIZE BAR AT EACH TIME SLOT ————————————————————————————————————
    current_size: int = starting_position.signed_qty if starting_position else 0
    first_bar: bool = True

    for ts in range(first_ts, last_ts + step_ns, step_ns):
        if ts in fill_at:
            current_size = fill_at[ts]
        if current_size == 0:
            continue
        position_axis.bar(
            _nanoseconds_to_mpldate(ts),
            current_size,
            width=bar_width,
            color="black",
            alpha=0.5,
            label="Signed Position Size" if first_bar else None,
        )
        first_bar = False

    # ——— DRAW ZERO LINE AND LEGEND ————————————————————————————————————————————————————
    position_axis.axhline(y=0, color="gray", lw=0.5, ls="-")
    position_axis.legend(loc="upper left", fontsize=8)


def _draw_position_background(
    plot_group_to_axis: dict[PlotGroup, plt.Axes],
    fills: list[Events.Broker.Fill],
    updates: list[Events.Strategy.IndicatorUpdate],
) -> None:
    half_bar: float = updates[0].source_event.period_type.duration_in_days / 2

    # ——— SHADE CLOSED POSITIONS ———————————————————————————————————————————————————————
    in_position_since_x: MplDate | None = None
    for fill in fills:
        x: MplDate = _nanoseconds_to_mpldate(fill.timestamp)
        if fill.position_cost_basis is not None and in_position_since_x is None:
            in_position_since_x = x
        elif fill.position_cost_basis is None and in_position_since_x is not None:
            for axis in plot_group_to_axis.values():
                axis.axvspan(
                    in_position_since_x - half_bar,
                    x + half_bar,
                    color="lightgray",
                    alpha=0.1,
                    zorder=0,
                )
            in_position_since_x = None

    # ——— SHADE UNCLOSED POSITION (EXTEND TO LAST BAR) ————————————————————————————————
    if in_position_since_x is not None:
        last_bar_x: MplDate = _nanoseconds_to_mpldate(
            updates[-1].source_event.period_start
        )
        for axis in plot_group_to_axis.values():
            axis.axvspan(
                in_position_since_x - half_bar,
                last_bar_x + half_bar,
                color="lightgray",
                alpha=0.1,
                zorder=0,
            )


def _draw_costbases_and_plboxes(
    plot_group_to_axis: dict[PlotGroup, plt.Axes],
    fills: list[Events.Broker.Fill],
    updates: list[Events.Strategy.IndicatorUpdate],
) -> None:
    type CostBasisLine = tuple[list[MplDate], list[float]]

    completed_lines: list[CostBasisLine] = []
    segment_directions: list[bool] = []  # True = long

    # ——— COLLECT COST BASIS SEGMENTS (ONE PER ROUND TRIP OR DIRECTION) ————————————————
    building_x: list[MplDate] = []
    building_y: list[float] = []
    building_is_long: bool = True

    for fill in fills:
        x: MplDate = _nanoseconds_to_mpldate(fill.timestamp)

        if fill.position_cost_basis is not None:
            new_is_long: bool = fill.signed_position_size > 0

            # Direction flipped: close current segment, start new one
            if building_y and new_is_long != building_is_long:
                building_x.append(x)
                building_y.append(building_y[-1])
                completed_lines.append((building_x, building_y))
                segment_directions.append(building_is_long)
                building_x = []
                building_y = []

            building_x.append(x)
            building_y.append(fill.position_cost_basis / PRICE_SCALE_FACTOR)
            building_is_long = new_is_long

        elif building_y:
            building_x.append(x)
            building_y.append(building_y[-1])
            completed_lines.append((building_x, building_y))
            segment_directions.append(building_is_long)
            building_x = []
            building_y = []

    # ——— EXTEND UNCLOSED POSITION TO LAST BAR ————————————————————————————————————————
    if building_x:
        building_x.append(
            _nanoseconds_to_mpldate(updates[-1].source_event.period_start)
        )
        building_y.append(building_y[-1])
        completed_lines.append((building_x, building_y))
        segment_directions.append(building_is_long)

    # ——— BUILD BAR EXTREME LOOKUP FOR P/L BOXES ——————————————————————————————————————
    scale: float = PRICE_SCALE_FACTOR
    bar_extremes: list[tuple[MplDate, float, float]] = [
        (
            _nanoseconds_to_mpldate(u.source_event.period_start),
            u.source_event.high / scale,
            u.source_event.low / scale,
        )
        for u in updates
    ]
    half_bar: float = updates[0].source_event.period_type.duration_in_days / 2

    # ——— DRAW COST BASIS LINES AND P/L BOXES PER SEGMENT —————————————————————————————
    for (line_x, line_y), is_long in zip(completed_lines, segment_directions):
        entry_x: MplDate = line_x[0]
        exit_x: MplDate = line_x[-1]

        # Find highest high and lowest low within this position
        highest_high: float = max(line_y)
        lowest_low: float = min(line_y)
        for bar_x, bar_high, bar_low in bar_extremes:
            if entry_x <= bar_x <= exit_x:
                highest_high = max(highest_high, bar_high)
                lowest_low = min(lowest_low, bar_low)

        # Cost basis line (extended half bar each side)
        extended_x: list[MplDate] = (
            [line_x[0] - half_bar] + line_x[1:] + [line_x[-1] + half_bar]
        )
        extended_y: list[float] = [line_y[0]] + line_y[1:] + [line_y[-1]]

        # P/L boxes split at cost basis (extend to axis edges)
        profit_color: str = "green" if is_long else "red"
        loss_color: str = "red" if is_long else "green"
        y_min, y_max = plot_group_to_axis[0].get_ylim()

        plot_group_to_axis[0].fill_between(
            extended_x,
            extended_y,
            y_max,
            step="post",
            color=profit_color,
            alpha=0.08,
            zorder=0,
        )
        plot_group_to_axis[0].fill_between(
            extended_x,
            y_min,
            extended_y,
            step="post",
            color=loss_color,
            alpha=0.08,
            zorder=0,
        )

        # Restore axis limits so fills don't expand them
        plot_group_to_axis[0].set_ylim(y_min, y_max)
