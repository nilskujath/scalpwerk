import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

from datetime import datetime, timezone
from enum import Enum, auto
from matplotlib.figure import Figure

from .core import (
    EventBase,
    Events,
    NanosecondsSinceUnixEpoch,
    Symbol,
    PlotGroup,
    PeriodType,
    PRICE_SCALE_FACTOR,
)

NANOSECONDS_PER_SECOND = 1_000_000_000  # for conversion of nanoseconds to seconds
SECONDS_PER_MPL_UNIT = 86400
CANDLE_WIDTH_RATIO = 0.8
FIGURE_WIDTH = 16
PRICE_PANEL_INCHES = 9
INDICATOR_PANEL_INCHES = 4
CHART_DPI = 300

type MplDate = float  # matplotlib internal date format (fractional days since epoch)
type PanelLayout = list[PlotGroup]


class BarStyle(Enum):
    CANDLESTICK = auto()


TICK_LOCATOR = {
    PeriodType.SECOND: mdates.SecondLocator(bysecond=range(0, 60, 10)),
    PeriodType.MINUTE: mdates.MinuteLocator(byminute=range(0, 60, 10)),
    PeriodType.HOUR: mdates.HourLocator(byhour=range(0, 24, 8)),
    PeriodType.DAY: mdates.DayLocator(interval=1),
}


def chart(events_to_chart: list[EventBase], for_symbol: Symbol) -> Figure:
    indicator_updates: list[Events.Strategy.IndicatorUpdate] = []

    for event in events_to_chart:
        if getattr(event, "symbol", None) != for_symbol:
            continue

        match event:
            case Events.Strategy.IndicatorUpdate():
                indicator_updates.append(event)

    if not indicator_updates:
        raise ValueError()

    panel_layout = _determine_indicator_panel_layout(indicator_updates)
    fig = _create_unpopulated_figure(
        panel_layout, indicator_updates[0].source_event.period_type
    )
    plot_group_to_axis: dict[PlotGroup, plt.Axes] = dict(zip(panel_layout, fig.axes))

    _draw_candlesticks(plot_group_to_axis, indicator_updates)
    _draw_indicators(plot_group_to_axis, indicator_updates)
    fig.tight_layout()
    return fig


def _determine_indicator_panel_layout(
    indicator_updates: list[Events.Strategy.IndicatorUpdate],
) -> PanelLayout:
    panels: set[int] = {0}  # always have main price panel
    representative_update = indicator_updates[0]
    for indicator_reading in representative_update.ind_values.values():
        if indicator_reading.plot_group is not None:
            panels.add(indicator_reading.plot_group)
    panel_layout: PanelLayout = [p for p in sorted(panels)]
    return panel_layout


def _create_unpopulated_figure(
    panel_layout: PanelLayout, period_type: PeriodType
) -> Figure:
    fig, _ = plt.subplots(
        len(panel_layout),
        1,
        figsize=(
            FIGURE_WIDTH,
            PRICE_PANEL_INCHES + (len(panel_layout) - 1) * INDICATOR_PANEL_INCHES,
        ),
        height_ratios=[
            PRICE_PANEL_INCHES if panel == 0 else INDICATOR_PANEL_INCHES
            for panel in panel_layout
        ],
        sharex=True,
        squeeze=False,
    )

    # x-axis formatting on bottom panel only
    bottom_axis = fig.axes[-1]
    bottom_axis.xaxis.set_major_locator(TICK_LOCATOR[period_type])
    bottom_axis.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d %H:%M:%S"))
    plt.setp(
        bottom_axis.xaxis.get_majorticklabels(),
        rotation=90,
        fontsize=8,
    )
    return fig


def _draw_candlesticks(
    plot_group_to_axis: dict[PlotGroup, plt.Axes],
    indicator_updates: list[Events.Strategy.IndicatorUpdate],
) -> None:
    price_axis = plot_group_to_axis[0]
    bar_width = (
        indicator_updates[0].source_event.period_type.duration_in_nanoseconds
        / NANOSECONDS_PER_SECOND
        / SECONDS_PER_MPL_UNIT
    ) * CANDLE_WIDTH_RATIO

    for update in indicator_updates:
        bar = update.source_event
        x = _nanoseconds_to_mpldate(bar.period_start)
        bar_open = bar.open / PRICE_SCALE_FACTOR
        bar_high = bar.high / PRICE_SCALE_FACTOR
        bar_low = bar.low / PRICE_SCALE_FACTOR
        bar_close = bar.close / PRICE_SCALE_FACTOR

        # Lower Wick
        if min(bar_open, bar_close) > bar_low:
            price_axis.plot(
                [x, x],
                [bar_low, min(bar_open, bar_close)],
                color="black",
                lw=0.8,
                alpha=0.7,
            )

        # Higher Wick
        if max(bar_open, bar_close) < bar_high:
            price_axis.plot(
                [x, x],
                [max(bar_open, bar_close), bar_high],
                color="black",
                lw=0.8,
                alpha=0.7,
            )

        # Candle Body
        price_axis.add_patch(
            mpatches.FancyBboxPatch(
                (x - bar_width / 2, min(bar_open, bar_close)),
                bar_width,
                abs(bar_close - bar_open) or (bar_high - bar_low) * 0.01,  # for Doji
                boxstyle="square,pad=0",
                facecolor="white" if bar_close > bar_open else "black",
                edgecolor="black",
                lw=0.5,
                alpha=0.7,
            )
        )


def _draw_indicators(
    plot_group_to_axis: dict[PlotGroup, plt.Axes],
    indicator_updates: list[Events.Strategy.IndicatorUpdate],
) -> None:
    representative_update = indicator_updates[0]

    for indicator_name, indicator_reading in representative_update.ind_values.items():
        if indicator_reading.plot_group is None:
            continue

        target_subplot: plt.Axes = plot_group_to_axis[indicator_reading.plot_group]
        indicator_output_is_scaled: bool = indicator_reading.is_scaled

        plot_x: list[MplDate] = []
        plot_y: list[float] = []
        for update in indicator_updates:
            value = update.ind_values[indicator_name].value
            if value != value:  # nan filtering for indicator warmup phase
                continue
            plot_x.append(_nanoseconds_to_mpldate(update.source_event.period_start))
            plot_y.append(
                value / PRICE_SCALE_FACTOR if indicator_output_is_scaled else value
            )

        if plot_x:
            target_subplot.plot(plot_x, plot_y, lw=1.2, label=indicator_name)

    for axis in plot_group_to_axis.values():
        handles, labels = axis.get_legend_handles_labels()
        if labels:
            axis.legend(loc="upper left", fontsize=8)


def _nanoseconds_to_mpldate(ns_since_epoch: NanosecondsSinceUnixEpoch) -> MplDate:
    seconds = ns_since_epoch / NANOSECONDS_PER_SECOND  # needed for `fromtimestamp`
    utc_datetime = datetime.fromtimestamp(seconds, tz=timezone.utc)
    return mdates.date2num(utc_datetime)
