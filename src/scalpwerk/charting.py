# fmt: off
from collections        import defaultdict
from dataclasses        import dataclass, field
from datetime           import datetime, timezone
from enum import Enum
from itertools          import accumulate
from matplotlib         import dates as mdates, pyplot as plt, patches as mpatches
from matplotlib.axes    import Axes
from matplotlib.figure  import Figure
from matplotlib.path    import Path as MplPath
from pathlib            import Path

from .core import (
    ChartPlotGroup,
    DomainEvents,
    EventMessageBase,
    IndicatorName,
    IndicatorPlotConfig,
    NanosecondsSinceUnixEpoch,
    PeriodType,
    PlotStyle,
    ScaledPrice,
    SignedPositionSize,
    Symbol,
    SystemEvents,
    TradeSide,
)
# fmt: on

BarStyle = Enum("BarStyle", ["CANDLESTICK", "BAR", "CBAR", "OCBAR"])
SortBy = Enum(
    "SortBy", ["BEST_PNL", "WORST_PNL", "HIGHEST_HWM", "LOWEST_LWM", "RANDOM"]
)


class Chartist:
    PRICE_SCALE_FACTOR: int = 1_000_000_000
    PRICE_PANEL_HEIGHT: int = 9
    INDICATOR_PANEL_HEIGHT: int = 3
    POSITION_PANEL_HEIGHT: int = 2
    LINESTYLE_MAP: dict[PlotStyle, str] = {
        PlotStyle.SOLID: "-",
        PlotStyle.DASHED: "--",
        PlotStyle.DOTTED: ":",
        PlotStyle.DASHDOT: "-.",
    }

    @dataclass
    class _GroupedEvents:
        fills: defaultdict[Symbol, list[DomainEvents.Fill]] = field(
            default_factory=lambda: defaultdict(list)
        )
        readings: defaultdict[Symbol, list[SystemEvents.IndicatorUpdate]] = field(
            default_factory=lambda: defaultdict(list)
        )
        submissions: defaultdict[Symbol, list[DomainEvents.OrderSubmitted]] = field(
            default_factory=lambda: defaultdict(list)
        )
        cancellations: defaultdict[Symbol, list[DomainEvents.OrderCancelled]] = field(
            default_factory=lambda: defaultdict(list)
        )
        expiries: defaultdict[Symbol, list[DomainEvents.OrderExpired]] = field(
            default_factory=lambda: defaultdict(list)
        )

    @dataclass
    class _ChartAxes:
        position: Axes
        price: Axes
        panels: dict[ChartPlotGroup, Axes]

    @staticmethod
    def chart(
        events: list[EventMessageBase],
        bar_style: BarStyle = BarStyle.CANDLESTICK,
        indicator_plot_configs: dict[IndicatorName, IndicatorPlotConfig] | None = None,
    ) -> dict[Symbol, Figure]:
        figures: dict[Symbol, Figure] = {}

        grouped_events: Chartist._GroupedEvents = Chartist._group_events(events)
        if not grouped_events.readings:
            return figures

        for symbol, readings in grouped_events.readings.items():
            plot_configs: dict[IndicatorName, IndicatorPlotConfig] = (
                Chartist._resolve_plot_config(readings[0], indicator_plot_configs or {})
            )

            fig, chart_axes = Chartist._create_figure(plot_configs=plot_configs)
            Chartist._draw_price(
                chart_axes=chart_axes, readings=readings, bar_style=bar_style
            )
            Chartist._draw_indicators(
                chart_axes=chart_axes, readings=readings, plot_configs=plot_configs
            )
            Chartist._draw_fills(
                chart_axes=chart_axes,
                fills=grouped_events.fills[symbol],
                readings=readings,
            )
            Chartist._draw_cost_basis(
                chart_axes=chart_axes,
                fills=grouped_events.fills[symbol],
                readings=readings,
            )
            Chartist._draw_position(
                chart_axes=chart_axes,
                fills=grouped_events.fills[symbol],
                readings=readings,
            )
            Chartist._style_figure(fig=fig, readings=readings, symbol=symbol)
            figures[symbol] = fig

        return figures

    @staticmethod
    def _group_events(events: list[EventMessageBase]) -> _GroupedEvents:
        grouped_events = Chartist._GroupedEvents()
        for event in events:
            match event:
                case DomainEvents.Fill() as event:
                    grouped_events.fills[event.symbol].append(event)
                case SystemEvents.IndicatorUpdate() as event:
                    grouped_events.readings[event.source_bar.symbol].append(event)
                case DomainEvents.OrderSubmitted() as event:
                    grouped_events.submissions[event.symbol].append(event)
                case DomainEvents.OrderCancelled() as event:
                    grouped_events.cancellations[event.symbol].append(event)
                case DomainEvents.OrderExpired() as event:
                    grouped_events.expiries[event.symbol].append(event)
        return grouped_events

    @staticmethod
    def _resolve_plot_config(
        sample_indicator_update: SystemEvents.IndicatorUpdate,
        indicator_plot_configs: dict[IndicatorName, IndicatorPlotConfig],
    ) -> dict[IndicatorName, IndicatorPlotConfig]:

        # Each indicator's plot config is resolved through three layers, where each
        # layer overrides the previous: first we set automatic defaults in case the user
        # does not provide anything (scaled indicators go on the price panel (0),
        # unscaled indicators each get their own panel; colors cycle through a palette);
        # the defaults then get overwritten with configs set via `register_indicator` in
        # the strategy; then at chart-time, the config passed to `Chartist.chart()` by
        # the caller has the final authority to overwrite anything set in the last two
        # stages.

        color_palette = [
            "#1f77b4",  # blue
            "#ff7f0e",  # orange
            "#2ca02c",  # green
            "#d62728",  # red
            "#9467bd",  # purple
            "#8c564b",  # brown
            "#e377c2",  # pink
            "#7f7f7f",  # gray
            "#bcbd22",  # olive
            "#17becf",  # cyan
        ]

        resolved_plot_configs: dict[IndicatorName, IndicatorPlotConfig] = {}
        for name, (
            _,
            is_scaled,
            registered_config,
        ) in sample_indicator_update.readings.items():
            # Layer 1:  Auto defaults from data (style is always overridden in layer 2)
            # fmt: off
            panel = 0 if is_scaled else 1 + sum(
                1 for config in resolved_plot_configs.values() if config.plot_group != 0
            )
            # fmt: on
            color = color_palette[len(resolved_plot_configs) % len(color_palette)]

            # Layer 2:  Config override from indicator registration data
            if registered_config.plot_group is not None:
                panel = registered_config.plot_group
            if registered_config.color is not None:
                color = registered_config.color
            style = registered_config.style

            # Layer 3:  Chart-time override
            if name in indicator_plot_configs:
                caller_config = indicator_plot_configs[name]
                if caller_config.plot_group is not None:
                    panel = caller_config.plot_group
                if caller_config.color is not None:
                    color = caller_config.color
                style = caller_config.style

            resolved_plot_configs[name] = IndicatorPlotConfig(
                plot_group=panel,
                color=color,
                style=style,
            )

        return resolved_plot_configs

    @staticmethod
    def _period_to_nanoseconds(period_type: PeriodType) -> int:
        match period_type:
            case PeriodType.SECOND:
                return 1_000_000_000
            case PeriodType.MINUTE:
                return 60_000_000_000
            case PeriodType.HOUR:
                return 3_600_000_000_000
            case PeriodType.DAY:
                return 86_400_000_000_000

    @staticmethod
    def _period_to_days(period_type: PeriodType) -> float:
        return Chartist._period_to_nanoseconds(period_type) / 86_400_000_000_000

    @staticmethod
    def _nanoseconds_to_mpldate(ns: NanosecondsSinceUnixEpoch) -> float:
        utc_datetime = datetime.fromtimestamp(ns / 1_000_000_000, tz=timezone.utc)
        return mdates.date2num(utc_datetime)

    @staticmethod
    def _create_figure(
        plot_configs: dict[IndicatorName, IndicatorPlotConfig],
    ) -> tuple[Figure, "Chartist._ChartAxes"]:
        negative_panels = sorted(
            {
                c.plot_group
                for c in plot_configs.values()
                if c.plot_group is not None and c.plot_group < 0
            },
        )
        positive_panels = sorted(
            {
                c.plot_group
                for c in plot_configs.values()
                if c.plot_group is not None and c.plot_group > 0 and c.plot_group != 99
            },
        )

        panel_order = [-999] + negative_panels + [0] + positive_panels
        height_ratios = []
        for panel in panel_order:
            if panel == -999:
                height_ratios.append(Chartist.POSITION_PANEL_HEIGHT)
            elif panel == 0:
                height_ratios.append(Chartist.PRICE_PANEL_HEIGHT)
            else:
                height_ratios.append(Chartist.INDICATOR_PANEL_HEIGHT)

        fig, axes_array = plt.subplots(
            nrows=len(panel_order),
            ncols=1,
            figsize=(16, sum(height_ratios)),
            sharex=True,
            gridspec_kw={"height_ratios": height_ratios},
            squeeze=False,
        )

        axes_list = [axes_array[i, 0] for i in range(len(panel_order))]

        panels: dict[ChartPlotGroup, Axes] = {}
        for panel_id, axis in zip(panel_order[1:], axes_list[1:]):
            panels[panel_id] = axis

        return fig, Chartist._ChartAxes(
            position=axes_list[0],
            price=panels[0],
            panels=panels,
        )

    @staticmethod
    def _draw_price(
        chart_axes: "Chartist._ChartAxes",
        readings: list[SystemEvents.IndicatorUpdate],
        bar_style: BarStyle,
    ) -> None:
        match bar_style:
            case BarStyle.CANDLESTICK:
                Chartist._draw_candlesticks(chart_axes=chart_axes, readings=readings)
            case BarStyle.BAR:
                Chartist._draw_bars(chart_axes=chart_axes, readings=readings)
            case BarStyle.CBAR:
                Chartist._draw_cbars(chart_axes=chart_axes, readings=readings)
            case BarStyle.OCBAR:
                Chartist._draw_ocbars(chart_axes=chart_axes, readings=readings)

    @staticmethod
    def _draw_candlesticks(
        chart_axes: "Chartist._ChartAxes",
        readings: list[SystemEvents.IndicatorUpdate],
    ) -> None:
        price_axis = chart_axes.price
        scale = Chartist.PRICE_SCALE_FACTOR
        candle_width = (
            Chartist._period_to_days(readings[0].source_bar.period_type) * 0.8
        )

        for reading in readings:
            # fmt: off
            bar       = reading.source_bar
            x         = Chartist._nanoseconds_to_mpldate(bar.period_start)
            bar_open  = bar.open  / scale
            bar_high  = bar.high  / scale
            bar_low   = bar.low   / scale
            bar_close = bar.close / scale
            # fmt: on

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

            if min(bar_open, bar_close) > bar_low:
                price_axis.plot(
                    [x, x],
                    [bar_low, min(bar_open, bar_close)],
                    color="black",
                    lw=0.8,
                    alpha=0.7,
                )

            if max(bar_open, bar_close) < bar_high:
                price_axis.plot(
                    [x, x],
                    [max(bar_open, bar_close), bar_high],
                    color="black",
                    lw=0.8,
                    alpha=0.7,
                )

    @staticmethod
    def _draw_bars(
        chart_axes: "Chartist._ChartAxes",
        readings: list[SystemEvents.IndicatorUpdate],
    ) -> None:
        price_axis = chart_axes.price
        scale = Chartist.PRICE_SCALE_FACTOR

        for reading in readings:
            bar = reading.source_bar
            x = Chartist._nanoseconds_to_mpldate(bar.period_start)
            price_axis.plot(
                [x, x],
                [bar.low / scale, bar.high / scale],
                color="black",
                lw=0.8,
                alpha=0.7,
            )

    @staticmethod
    def _draw_cbars(
        chart_axes: "Chartist._ChartAxes",
        readings: list[SystemEvents.IndicatorUpdate],
    ) -> None:
        price_axis = chart_axes.price
        scale = Chartist.PRICE_SCALE_FACTOR
        tick_width = Chartist._period_to_days(readings[0].source_bar.period_type) * 0.3

        for reading in readings:
            # fmt: off
            bar       = reading.source_bar
            x         = Chartist._nanoseconds_to_mpldate(bar.period_start)
            bar_high  = bar.high  / scale
            bar_low   = bar.low   / scale
            bar_close = bar.close / scale
            # fmt: on

            price_axis.plot(
                [x, x],
                [bar_low, bar_high],
                color="black",
                lw=0.8,
                alpha=0.7,
            )
            price_axis.plot(
                [x, x + tick_width],
                [bar_close, bar_close],
                color="black",
                lw=0.8,
                alpha=0.7,
            )

    @staticmethod
    def _draw_ocbars(
        chart_axes: "Chartist._ChartAxes",
        readings: list[SystemEvents.IndicatorUpdate],
    ) -> None:
        price_axis = chart_axes.price
        scale = Chartist.PRICE_SCALE_FACTOR
        tick_width = Chartist._period_to_days(readings[0].source_bar.period_type) * 0.3

        for reading in readings:
            # fmt: off
            bar       = reading.source_bar
            x         = Chartist._nanoseconds_to_mpldate(bar.period_start)
            bar_open  = bar.open  / scale
            bar_high  = bar.high  / scale
            bar_low   = bar.low   / scale
            bar_close = bar.close / scale
            # fmt: on

            price_axis.plot(
                [x, x],
                [bar_low, bar_high],
                color="black",
                lw=0.8,
                alpha=0.7,
            )
            price_axis.plot(
                [x - tick_width, x],
                [bar_open, bar_open],
                color="black",
                lw=0.8,
                alpha=0.7,
            )
            price_axis.plot(
                [x, x + tick_width],
                [bar_close, bar_close],
                color="black",
                lw=0.8,
                alpha=0.7,
            )

    @staticmethod
    def _draw_indicators(
        chart_axes: "Chartist._ChartAxes",
        readings: list[SystemEvents.IndicatorUpdate],
        plot_configs: dict[IndicatorName, IndicatorPlotConfig],
    ) -> None:
        scale = Chartist.PRICE_SCALE_FACTOR
        bar_width = Chartist._period_to_days(readings[0].source_bar.period_type) * 0.8

        for name, config in plot_configs.items():
            if config.plot_group is None or config.plot_group == 99:
                continue

            plot_x: list[float] = []
            plot_y: list[float] = []
            is_scaled = readings[0].readings[name][1]

            for reading in readings:
                value = reading.readings[name][0]
                if value != value:
                    continue
                plot_x.append(
                    Chartist._nanoseconds_to_mpldate(reading.source_bar.period_start)
                )
                plot_y.append(value / scale if is_scaled else value)

            axis = chart_axes.panels[config.plot_group]

            match config.style:
                case PlotStyle.HISTOGRAM:
                    axis.bar(
                        plot_x,
                        plot_y,
                        width=bar_width,
                        color=config.color,
                        alpha=0.7,
                        label=name,
                    )
                case _:
                    axis.plot(
                        plot_x,
                        plot_y,
                        lw=1.2,
                        label=name,
                        color=config.color,
                        linestyle=Chartist.LINESTYLE_MAP[config.style],
                    )

        for axis in chart_axes.panels.values():
            if axis.get_legend_handles_labels()[1]:
                axis.legend(loc="upper left", fontsize=8)

    @staticmethod
    def _draw_fills(
        chart_axes: "Chartist._ChartAxes",
        fills: list[DomainEvents.Fill],
        readings: list[SystemEvents.IndicatorUpdate],
    ) -> None:
        if not fills:
            return

        scale = Chartist.PRICE_SCALE_FACTOR
        price_axis = chart_axes.price

        buy_marker = MplPath(
            [(0, 0), (-1, -2), (1, -2), (0, 0)],
            [MplPath.MOVETO, MplPath.LINETO, MplPath.LINETO, MplPath.CLOSEPOLY],
        )
        sell_marker = MplPath(
            [(0, 0), (-1, 2), (1, 2), (0, 0)],
            [MplPath.MOVETO, MplPath.LINETO, MplPath.LINETO, MplPath.CLOSEPOLY],
        )

        fill_idx = 0
        for reading in readings:
            x = Chartist._nanoseconds_to_mpldate(reading.source_bar.period_start)
            while (
                fill_idx < len(fills) and fills[fill_idx].timestamp <= reading.timestamp
            ):
                fill = fills[fill_idx]
                is_buy = fill.trade_side is TradeSide.BUY
                price_axis.scatter(
                    x,
                    fill.fill_price / scale,
                    marker=buy_marker if is_buy else sell_marker,
                    color="green" if is_buy else "red",
                    edgecolors="green" if is_buy else "red",
                    s=80,
                    lw=0.5,
                    zorder=5,
                )
                fill_idx += 1

    @staticmethod
    def _draw_cost_basis(
        chart_axes: "Chartist._ChartAxes",
        fills: list[DomainEvents.Fill],
        readings: list[SystemEvents.IndicatorUpdate],
    ) -> None:
        if not fills:
            return

        scale = Chartist.PRICE_SCALE_FACTOR
        price_axis = chart_axes.price

        completed_lines: list[tuple[list[float], list[float]]] = []
        segment_directions: list[bool] = []

        building_x: list[float] = []
        building_y: list[float] = []
        building_is_long: bool = True

        fill_idx = 0
        for reading in readings:
            x = Chartist._nanoseconds_to_mpldate(reading.source_bar.period_start)
            while (
                fill_idx < len(fills) and fills[fill_idx].timestamp <= reading.timestamp
            ):
                fill = fills[fill_idx]
                fill_idx += 1

                if fill.position_cost_basis is not None:
                    new_is_long = fill.signed_position_size > 0

                    if building_y and new_is_long != building_is_long:
                        building_x.append(x)
                        building_y.append(building_y[-1])
                        completed_lines.append((building_x, building_y))
                        segment_directions.append(building_is_long)
                        building_x = []
                        building_y = []

                    building_x.append(x)
                    building_y.append(fill.position_cost_basis / scale)
                    building_is_long = new_is_long

                elif building_y:
                    building_x.append(x)
                    building_y.append(building_y[-1])
                    completed_lines.append((building_x, building_y))
                    segment_directions.append(building_is_long)
                    building_x = []
                    building_y = []

        if building_x:
            building_x.append(
                Chartist._nanoseconds_to_mpldate(readings[-1].source_bar.period_start)
            )
            building_y.append(building_y[-1])
            completed_lines.append((building_x, building_y))
            segment_directions.append(building_is_long)

        half_bar = Chartist._period_to_days(readings[0].source_bar.period_type) / 2

        for (line_x, line_y), is_long in zip(completed_lines, segment_directions):
            extended_x = [line_x[0] - half_bar] + line_x[1:] + [line_x[-1] + half_bar]
            extended_y = [line_y[0]] + line_y[1:] + [line_y[-1]]

            profit_color = "green" if is_long else "red"
            loss_color = "red" if is_long else "green"
            y_min, y_max = price_axis.get_ylim()

            price_axis.fill_between(
                extended_x,
                extended_y,
                y_max,
                step="post",
                color=profit_color,
                alpha=0.08,
                zorder=0,
            )
            price_axis.fill_between(
                extended_x,
                y_min,
                extended_y,
                step="post",
                color=loss_color,
                alpha=0.08,
                zorder=0,
            )

            price_axis.set_ylim(y_min, y_max)

    @staticmethod
    def _draw_position(
        chart_axes: "Chartist._ChartAxes",
        fills: list[DomainEvents.Fill],
        readings: list[SystemEvents.IndicatorUpdate],
    ) -> None:
        if not fills:
            return

        Chartist._draw_position_background(chart_axes, fills, readings)

        position_axis = chart_axes.position
        period_type = readings[0].source_bar.period_type
        bar_width = Chartist._period_to_days(period_type) * 0.8
        step_ns = Chartist._period_to_nanoseconds(period_type)
        first_ts = readings[0].source_bar.period_start
        last_ts = readings[-1].source_bar.period_start

        fill_idx = 0
        position: SignedPositionSize = 0
        display_at: dict[NanosecondsSinceUnixEpoch, SignedPositionSize] = {}
        actual_at: dict[NanosecondsSinceUnixEpoch, SignedPositionSize] = {}

        for reading in readings:
            position_before = position
            while (
                fill_idx < len(fills) and fills[fill_idx].timestamp <= reading.timestamp
            ):
                position = fills[fill_idx].signed_position_size
                fill_idx += 1

            # Display: show position if held at any point during this bar.
            # Entry bar: before=0, after=1 → show 1.
            # Exit bar: before=1, after=0 → show 1.
            display = (
                position if abs(position) >= abs(position_before) else position_before
            )
            display_at[reading.source_bar.period_start] = display
            # Actual: for gap carry-forward after this bar.
            actual_at[reading.source_bar.period_start] = position

        current_position: SignedPositionSize = 0
        first_bar = True

        for ts in range(first_ts, last_ts + step_ns, step_ns):
            if ts in display_at:
                draw = display_at[ts]
                current_position = actual_at[ts]
            else:
                draw = current_position
            if draw == 0:
                continue
            x = Chartist._nanoseconds_to_mpldate(ts)
            position_axis.bar(
                x,
                draw,
                width=bar_width,
                color="black",
                alpha=0.5,
                label="Position Size" if first_bar else None,
            )
            first_bar = False

        position_axis.axhline(y=0, color="gray", lw=0.5, ls="-")
        if not first_bar:
            position_axis.legend(loc="upper left", fontsize=8)

    @staticmethod
    def _draw_position_background(
        chart_axes: "Chartist._ChartAxes",
        fills: list[DomainEvents.Fill],
        readings: list[SystemEvents.IndicatorUpdate],
    ) -> None:
        if not fills:
            return

        half_bar = Chartist._period_to_days(readings[0].source_bar.period_type) / 2
        all_axes = [chart_axes.position, chart_axes.price] + list(
            chart_axes.panels.values()
        )

        in_position_since_x: float | None = None
        pending_close_x: float | None = None
        fill_idx = 0

        for reading in readings:
            x = Chartist._nanoseconds_to_mpldate(reading.source_bar.period_start)

            # If we deferred a close on the previous bar's fill, close now on this bar.
            if pending_close_x is not None and in_position_since_x is not None:
                for axis in all_axes:
                    axis.axvspan(
                        in_position_since_x - half_bar,
                        pending_close_x + half_bar,
                        color="lightgray",
                        alpha=0.1,
                        zorder=0,
                    )
                in_position_since_x = None
                pending_close_x = None

            while (
                fill_idx < len(fills) and fills[fill_idx].timestamp <= reading.timestamp
            ):
                fill = fills[fill_idx]
                fill_idx += 1
                if fill.position_cost_basis is not None and in_position_since_x is None:
                    in_position_since_x = x
                elif (
                    fill.position_cost_basis is None and in_position_since_x is not None
                ):
                    # Defer: the exit fill's bar should still be shaded.
                    pending_close_x = x

        # Handle open position or deferred close at end of data.
        close_x = pending_close_x or Chartist._nanoseconds_to_mpldate(
            readings[-1].source_bar.period_start
        )
        if in_position_since_x is not None:
            for axis in all_axes:
                axis.axvspan(
                    in_position_since_x - half_bar,
                    close_x + half_bar,
                    color="lightgray",
                    alpha=0.1,
                    zorder=0,
                )

    @staticmethod
    def _style_figure(
        fig: Figure,
        readings: list[SystemEvents.IndicatorUpdate],
        symbol: Symbol,
    ) -> None:
        period_type = readings[0].source_bar.period_type
        first_ts = readings[0].source_bar.period_start
        last_ts = readings[-1].source_bar.period_start

        first_bar_x = Chartist._nanoseconds_to_mpldate(first_ts)
        last_bar_x = Chartist._nanoseconds_to_mpldate(last_ts)
        bar_padding = Chartist._period_to_days(period_type)
        fig.axes[0].set_xlim(first_bar_x - bar_padding, last_bar_x + bar_padding)

        tick_locator: dict = {
            PeriodType.SECOND: mdates.SecondLocator(bysecond=range(0, 60, 10)),
            PeriodType.MINUTE: mdates.MinuteLocator(byminute=range(0, 60, 10)),
            PeriodType.HOUR: mdates.HourLocator(byhour=range(0, 24, 8)),
            PeriodType.DAY: mdates.DayLocator(interval=1),
        }
        bottom_axis = fig.axes[-1]
        bottom_axis.xaxis.set_major_locator(tick_locator[period_type])
        bottom_axis.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d %H:%M:%S"))
        plt.setp(bottom_axis.xaxis.get_majorticklabels(), rotation=90, fontsize=8)

        def fmt_ts(ns: NanosecondsSinceUnixEpoch) -> str:
            return datetime.fromtimestamp(ns / 1_000_000_000, tz=timezone.utc).strftime(
                "%Y-%m-%d %H:%M:%S"
            )

        fig.suptitle(
            f"{symbol}  {period_type.name}  {fmt_ts(first_ts)} to {fmt_ts(last_ts)}",
            fontsize=10,
        )

        title_space_inches = 0.2
        fig.tight_layout(rect=(0, 0, 1, 1 - title_space_inches / fig.get_figheight()))

    # ——— Roundtrip Extraction ————————————————————————————————————————————

    @staticmethod
    def _extract_roundtrips(
        fills: list[DomainEvents.Fill],
    ) -> list[list[DomainEvents.Fill]]:
        roundtrips: list[list[DomainEvents.Fill]] = []
        current: list[DomainEvents.Fill] = []
        position: SignedPositionSize = 0

        for fill in fills:
            current.append(fill)
            old = position
            position = fill.signed_position_size
            if position == 0:
                roundtrips.append(current)
                current = []
            elif old != 0 and old * position < 0:
                roundtrips.append(current)
                current = [fill]

        return roundtrips

    @staticmethod
    def _roundtrip_pnl(fills: list[DomainEvents.Fill]) -> ScaledPrice:
        return sum(
            f.fill_price * f.filled_qty * (-1 if f.trade_side is TradeSide.BUY else 1)
            for f in fills
        )

    @staticmethod
    def _roundtrip_hwm(
        fills: list[DomainEvents.Fill],
        readings: list[SystemEvents.IndicatorUpdate],
    ) -> ScaledPrice:
        fill_idx = 0
        position: SignedPositionSize = 0
        cost_basis: ScaledPrice = 0
        hwm: ScaledPrice = 0

        for reading in readings:
            position_before = position
            cost_basis_before = cost_basis
            while (
                fill_idx < len(fills) and fills[fill_idx].timestamp <= reading.timestamp
            ):
                position = fills[fill_idx].signed_position_size
                cost_basis = fills[fill_idx].position_cost_basis or 0
                fill_idx += 1

            active_pos = position if position != 0 else position_before
            active_cb = cost_basis if position != 0 else cost_basis_before
            if active_pos == 0:
                continue

            bar = reading.source_bar
            if active_pos > 0:
                best = (bar.high - active_cb) * active_pos
            else:
                best = (active_cb - bar.low) * abs(active_pos)

            hwm = max(hwm, best)

        return hwm

    @staticmethod
    def _roundtrip_lwm(
        fills: list[DomainEvents.Fill],
        readings: list[SystemEvents.IndicatorUpdate],
    ) -> ScaledPrice:
        fill_idx = 0
        position: SignedPositionSize = 0
        cost_basis: ScaledPrice = 0
        lwm: ScaledPrice = 0

        for reading in readings:
            position_before = position
            cost_basis_before = cost_basis
            while (
                fill_idx < len(fills) and fills[fill_idx].timestamp <= reading.timestamp
            ):
                position = fills[fill_idx].signed_position_size
                cost_basis = fills[fill_idx].position_cost_basis or 0
                fill_idx += 1

            active_pos = position if position != 0 else position_before
            active_cb = cost_basis if position != 0 else cost_basis_before
            if active_pos == 0:
                continue

            bar = reading.source_bar
            if active_pos > 0:
                worst = (bar.low - active_cb) * active_pos
            else:
                worst = (active_cb - bar.high) * abs(active_pos)

            lwm = min(lwm, worst)

        return lwm

    # ——— Roundtrip Slicing ———————————————————————————————————————————————

    @staticmethod
    def _slice_for_roundtrip(
        roundtrip_fills: list[DomainEvents.Fill],
        all_readings: list[SystemEvents.IndicatorUpdate],
        context_bars: int = 20,
    ) -> list[EventMessageBase]:
        first_fill_ts = roundtrip_fills[0].timestamp
        last_fill_ts = roundtrip_fills[-1].timestamp

        # Find reading indices that bracket the trade.
        first_idx = len(all_readings)
        last_idx = 0
        for i, reading in enumerate(all_readings):
            if reading.timestamp >= first_fill_ts and first_idx == len(all_readings):
                first_idx = i
            if reading.timestamp <= last_fill_ts:
                last_idx = i

        # Add context.
        start = max(0, first_idx - context_bars)
        end = min(len(all_readings), last_idx + context_bars + 1)

        sliced_readings = all_readings[start:end]
        return list(roundtrip_fills) + list(sliced_readings)

    # ——— Trade Selection and Charting ————————————————————————————————————

    @staticmethod
    def select(
        events: list[EventMessageBase],
        sort_by: SortBy = SortBy.BEST_PNL,
        n: int = 10,
        context_bars: int = 20,
        bar_style: BarStyle = BarStyle.CANDLESTICK,
        indicator_plot_configs: dict[IndicatorName, IndicatorPlotConfig] | None = None,
    ) -> dict[Symbol, list[Figure]]:
        grouped = Chartist._group_events(events)
        if not grouped.readings:
            return {}

        figures: dict[Symbol, list[Figure]] = {}

        for symbol, fills in grouped.fills.items():
            readings = grouped.readings[symbol]
            roundtrips = Chartist._extract_roundtrips(fills)

            if not roundtrips:
                continue

            ranked: list[list[DomainEvents.Fill]] = []

            match sort_by:
                case SortBy.BEST_PNL:
                    ranked = sorted(
                        roundtrips,
                        key=Chartist._roundtrip_pnl,
                        reverse=True,
                    )[:n]
                case SortBy.WORST_PNL:
                    ranked = sorted(
                        roundtrips,
                        key=Chartist._roundtrip_pnl,
                    )[:n]
                case SortBy.HIGHEST_HWM:
                    ranked = sorted(
                        roundtrips,
                        key=lambda rt: Chartist._roundtrip_hwm(rt, readings),
                        reverse=True,
                    )[:n]
                case SortBy.LOWEST_LWM:
                    ranked = sorted(
                        roundtrips,
                        key=lambda rt: Chartist._roundtrip_lwm(rt, readings),
                    )[:n]
                case SortBy.RANDOM:
                    import random

                    ranked = random.sample(roundtrips, min(n, len(roundtrips)))

            symbol_figures: list[Figure] = []
            for roundtrip in ranked:
                trade_events = Chartist._slice_for_roundtrip(
                    roundtrip, readings, context_bars
                )
                chart_result = Chartist.chart(
                    trade_events,
                    bar_style=bar_style,
                    indicator_plot_configs=indicator_plot_configs,
                )
                if symbol in chart_result:
                    symbol_figures.append(chart_result[symbol])

            figures[symbol] = symbol_figures

        return figures

    @staticmethod
    def save_sampled_roundtrips(
        events: list[EventMessageBase],
        output_dir: Path,
        n: int = 10,
        context_bars: int = 20,
        bar_style: BarStyle = BarStyle.CANDLESTICK,
        indicator_plot_configs: dict[IndicatorName, IndicatorPlotConfig] | None = None,
    ) -> None:
        for sort_by in SortBy:
            figures = Chartist.select(
                events,
                sort_by=sort_by,
                n=n,
                context_bars=context_bars,
                bar_style=bar_style,
                indicator_plot_configs=indicator_plot_configs,
            )
            for symbol, figs in figures.items():
                sort_dir = output_dir / sort_by.name.lower()
                sort_dir.mkdir(parents=True, exist_ok=True)
                for i, fig in enumerate(figs):
                    fig.savefig(
                        sort_dir / f"{symbol}_{i + 1:04d}.png",
                        dpi=300,
                        bbox_inches="tight",
                    )
                    plt.close(fig)

    @staticmethod
    def save_all_roundtrips(
        events: list[EventMessageBase],
        output_dir: Path,
        context_bars: int = 20,
        bar_style: BarStyle = BarStyle.CANDLESTICK,
        indicator_plot_configs: dict[IndicatorName, IndicatorPlotConfig] | None = None,
    ) -> None:
        output_dir.mkdir(parents=True, exist_ok=True)
        grouped = Chartist._group_events(events)

        for symbol, fills in grouped.fills.items():
            readings = grouped.readings[symbol]
            roundtrips = Chartist._extract_roundtrips(fills)
            for i, roundtrip in enumerate(roundtrips):
                trade_events = Chartist._slice_for_roundtrip(
                    roundtrip, readings, context_bars
                )
                result = Chartist.chart(
                    trade_events,
                    bar_style=bar_style,
                    indicator_plot_configs=indicator_plot_configs,
                )
                if symbol in result:
                    result[symbol].savefig(
                        output_dir / f"{symbol}_trade_{i + 1:04d}.png",
                        dpi=300,
                        bbox_inches="tight",
                    )
                    plt.close(result[symbol])

    @staticmethod
    def journey(events: list[EventMessageBase]) -> dict[Symbol, Figure]:
        grouped = Chartist._group_events(events)
        if not grouped.readings:
            return {}

        figures: dict[Symbol, Figure] = {}
        scale = Chartist.PRICE_SCALE_FACTOR

        for symbol, fills in grouped.fills.items():
            roundtrips = Chartist._extract_roundtrips(fills)
            readings = grouped.readings[symbol]
            if not roundtrips:
                continue

            journey_data: list[dict] = []
            for rt in roundtrips:
                max_pos: float = 0.0
                max_neg: float = 0.0
                bar_count = 0

                fill_idx = 0
                position: SignedPositionSize = 0
                cost_basis: ScaledPrice = 0

                for reading in readings:
                    position_before = position
                    cost_basis_before = cost_basis
                    while (
                        fill_idx < len(rt)
                        and rt[fill_idx].timestamp <= reading.timestamp
                    ):
                        position = rt[fill_idx].signed_position_size
                        cost_basis = rt[fill_idx].position_cost_basis or 0
                        fill_idx += 1

                    active_pos = position if position != 0 else position_before
                    active_cb = cost_basis if position != 0 else cost_basis_before
                    if active_pos == 0:
                        continue

                    bar = reading.source_bar
                    bar_count += 1
                    if active_pos > 0:
                        pos_move = (bar.high - active_cb) * active_pos / scale
                        neg_move = (bar.low - active_cb) * active_pos / scale
                    else:
                        pos_move = (active_cb - bar.low) * abs(active_pos) / scale
                        neg_move = (active_cb - bar.high) * abs(active_pos) / scale

                    max_pos = max(max_pos, pos_move)
                    max_neg = min(max_neg, neg_move)

                exit_pnl = Chartist._roundtrip_pnl(rt) / scale

                journey_data.append(
                    {
                        "max_positive": max_pos,
                        "max_negative": max_neg,
                        "exit_pnl": exit_pnl,
                        "is_winner": exit_pnl > 0,
                        "bar_count": bar_count,
                    }
                )

            fig = Figure(figsize=(14, 7))
            ax = fig.subplots()

            win_color = "#3fb950"
            loss_color = "#f85149"
            win_exit_color = "#1a7f37"
            loss_exit_color = "#a40e26"

            all_durations = [d["bar_count"] for d in journey_data]
            max_duration = max(all_durations) if all_durations else 1
            min_width = 0.2
            max_width = 0.9

            winning_exits: list[float] = []
            losing_exits: list[float] = []
            winning_bars: list[int] = []
            losing_bars: list[int] = []

            for i, data in enumerate(journey_data):
                trade_num = i + 1
                bar_width = (
                    min_width
                    + (data["bar_count"] / max_duration) * (max_width - min_width)
                    if max_duration > 1
                    else max_width
                )
                bar_color = win_color if data["is_winner"] else loss_color

                if data["max_positive"] > 0:
                    ax.bar(
                        trade_num,
                        data["max_positive"],
                        bottom=0,
                        color=bar_color,
                        width=bar_width,
                        alpha=0.7,
                    )
                if data["max_negative"] < 0:
                    ax.bar(
                        trade_num,
                        abs(data["max_negative"]),
                        bottom=data["max_negative"],
                        color=bar_color,
                        width=bar_width,
                        alpha=0.7,
                    )

                half = bar_width / 2
                ax.hlines(
                    data["exit_pnl"],
                    trade_num - half,
                    trade_num + half,
                    colors="black",
                    linewidth=1.5,
                    zorder=5,
                )

                if data["is_winner"]:
                    winning_exits.append(data["exit_pnl"])
                    winning_bars.append(data["bar_count"])
                else:
                    losing_exits.append(data["exit_pnl"])
                    losing_bars.append(data["bar_count"])

            ax.axhline(y=0, color="black", linestyle="-", linewidth=0.8, alpha=0.5)

            if winning_exits:
                avg_win = sum(winning_exits) / len(winning_exits)
                ax.axhline(
                    y=avg_win,
                    color=win_exit_color,
                    linestyle="--",
                    linewidth=1.5,
                    alpha=0.8,
                    label=f"Avg Win Exit: {avg_win:.1f}",
                )

            if losing_exits:
                avg_loss = sum(losing_exits) / len(losing_exits)
                ax.axhline(
                    y=avg_loss,
                    color=loss_exit_color,
                    linestyle="--",
                    linewidth=1.5,
                    alpha=0.8,
                    label=f"Avg Loss Exit: {avg_loss:.1f}",
                )

            # Summary statistics.
            total_trades = len(journey_data)
            max_pos_values = [d["max_positive"] for d in journey_data]
            max_neg_values = [d["max_negative"] for d in journey_data]
            exit_values = [d["exit_pnl"] for d in journey_data]

            avg_max_pos = sum(max_pos_values) / total_trades if total_trades else 0
            highest_pos = max(max_pos_values) if max_pos_values else 0
            avg_max_neg = abs(sum(max_neg_values) / total_trades) if total_trades else 0
            worst_neg = abs(min(max_neg_values)) if max_neg_values else 0
            avg_exit = sum(exit_values) / total_trades if total_trades else 0
            best_exit = max(exit_values) if exit_values else 0
            worst_exit = min(exit_values) if exit_values else 0

            max_win_bars = max(winning_bars) if winning_bars else 0
            avg_win_bars = sum(winning_bars) / len(winning_bars) if winning_bars else 0
            max_loss_bars = max(losing_bars) if losing_bars else 0
            avg_loss_bars = sum(losing_bars) / len(losing_bars) if losing_bars else 0

            summary_text = (
                f"Trade Journey Summary\n"
                f"Total Trades: {total_trades}\n\n"
                f"Max Positive Movement:\n"
                f"  Average: {avg_max_pos:.1f} pts\n"
                f"  Highest: {highest_pos:.1f} pts\n\n"
                f"Max Negative Movement:\n"
                f"  Average: {avg_max_neg:.1f} pts\n"
                f"  Worst: {worst_neg:.1f} pts\n\n"
                f"Exit Points:\n"
                f"  Average: {avg_exit:.1f} pts\n"
                f"  Best: {best_exit:.1f} pts\n"
                f"  Worst: {worst_exit:.1f} pts\n\n"
                f"Trade Duration (Bars):\n"
                f"  Wins: Max {max_win_bars}, Avg {avg_win_bars:.1f}\n"
                f"  Losses: Max {max_loss_bars}, Avg {avg_loss_bars:.1f}"
            )

            ax.text(
                0.02,
                0.98,
                summary_text,
                transform=ax.transAxes,
                fontsize=9,
                verticalalignment="top",
                fontfamily="monospace",
                zorder=10,
                bbox=dict(
                    boxstyle="round,pad=0.5",
                    facecolor="#add8e6",
                    edgecolor="#4682b4",
                    alpha=0.9,
                ),
            )

            from matplotlib.lines import Line2D

            legend_elements = [
                Line2D(
                    [0],
                    [0],
                    color=win_color,
                    linewidth=8,
                    alpha=0.7,
                    label="Winning Trades",
                ),
                Line2D(
                    [0],
                    [0],
                    color=loss_color,
                    linewidth=8,
                    alpha=0.7,
                    label="Losing Trades",
                ),
                Line2D([0], [0], color="black", linewidth=2, label="Exit Point"),
            ]
            ax.legend(handles=legend_elements, loc="upper right", fontsize=9)

            ax.set_title(f"Trade Journey — {symbol}", fontsize=14)
            ax.set_xlabel("Trade Number", fontsize=11)
            ax.set_ylabel("Points from Entry", fontsize=11)
            ax.grid(True, alpha=0.3, axis="y")

            num_trades = len(journey_data)
            if num_trades > 20:
                tick_interval = max(1, num_trades // 15)
                ax.set_xticks(list(range(1, num_trades + 1, tick_interval)))

            fig.tight_layout()
            figures[symbol] = fig

        return figures

    @staticmethod
    def pnl(events: list[EventMessageBase]) -> dict[Symbol, Figure]:
        grouped = Chartist._group_events(events)
        if not grouped.readings:
            return {}

        figures: dict[Symbol, Figure] = {}
        scale = Chartist.PRICE_SCALE_FACTOR

        for symbol, fills in grouped.fills.items():
            roundtrips = Chartist._extract_roundtrips(fills)
            readings = grouped.readings[symbol]
            if not roundtrips:
                continue

            trade_nums: list[int] = []
            cumulative_pnl: list[float] = []
            max_drawdowns: list[float] = []

            running_pnl: float = 0.0
            for i, rt in enumerate(roundtrips):
                trade_nums.append(i + 1)
                rt_pnl = Chartist._roundtrip_pnl(rt) / scale
                running_pnl += rt_pnl
                cumulative_pnl.append(running_pnl)
                rt_lwm = Chartist._roundtrip_lwm(rt, readings) / scale
                max_drawdowns.append(rt_lwm)

            fig = Figure(figsize=(14, 7))
            ax = fig.subplots()

            ax.plot(
                trade_nums,
                cumulative_pnl,
                color="#3fb950",
                linewidth=2,
                label="Cumulative PnL",
                marker="o",
                markersize=4,
            )
            ax.bar(
                trade_nums,
                max_drawdowns,
                color="#f85149",
                alpha=0.5,
                width=0.4,
                label="Max Drawdown",
            )

            total_trades = len(roundtrips)
            pnl_values = [Chartist._roundtrip_pnl(rt) / scale for rt in roundtrips]

            winning_trades = [p for p in pnl_values if p > 0]
            losing_trades = [p for p in pnl_values if p <= 0]
            num_winners = len(winning_trades)
            num_losers = len(losing_trades)

            avg_winner = sum(winning_trades) / num_winners if num_winners else 0
            avg_loser = sum(losing_trades) / num_losers if num_losers else 0
            win_rate = num_winners / total_trades * 100 if total_trades else 0

            max_dd_from_peak = (
                min(
                    c - p
                    for c, p in zip(
                        cumulative_pnl,
                        accumulate(cumulative_pnl, max),
                    )
                )
                if cumulative_pnl
                else 0
            )

            summary_text = (
                f"PnL Summary\n"
                f"Total Trades: {total_trades}\n\n"
                f"Overall PnL: {cumulative_pnl[-1]:+.2f} pts\n\n"
                f"Win Rate: {win_rate:.1f}%\n"
                f"Winning Trades: {num_winners}\n"
                f"Losing Trades: {num_losers}\n\n"
                f"Avg Winner: {avg_winner:+.2f} pts\n"
                f"Avg Loser: {avg_loser:+.2f} pts\n\n"
                f"Max Drawdown from Peak: {max_dd_from_peak:.2f} pts"
            )

            ax.text(
                0.02,
                0.98,
                summary_text,
                transform=ax.transAxes,
                fontsize=9,
                verticalalignment="top",
                fontfamily="monospace",
                zorder=10,
                bbox=dict(
                    boxstyle="round,pad=0.5",
                    facecolor="#add8e6",
                    edgecolor="#4682b4",
                    alpha=0.9,
                ),
            )

            ax.axhline(y=0, color="black", linestyle="-", alpha=0.5, linewidth=0.8)
            ax.legend(loc="upper right", fontsize=9)
            ax.set_title(f"PnL Summary — {symbol}", fontsize=14)
            ax.set_xlabel("Trade Number", fontsize=11)
            ax.set_ylabel("Cumulative PnL (pts)", fontsize=11)
            ax.grid(True, alpha=0.3, axis="y")

            if total_trades > 20:
                tick_interval = max(1, total_trades // 15)
                ax.set_xticks(list(range(1, total_trades + 1, tick_interval)))

            fig.tight_layout()
            figures[symbol] = fig

        return figures

    @staticmethod
    def save_overview(
        events: list[EventMessageBase],
        output_dir: Path,
    ) -> None:
        output_dir.mkdir(parents=True, exist_ok=True)
        for symbol, fig in Chartist.journey(events).items():
            fig.savefig(
                output_dir / f"{symbol}_journey.png", dpi=300, bbox_inches="tight"
            )
            plt.close(fig)
        for symbol, fig in Chartist.pnl(events).items():
            fig.savefig(output_dir / f"{symbol}_pnl.png", dpi=300, bbox_inches="tight")
            plt.close(fig)
