from collections import deque

from src.scalpwerk.core import _IndicatorBase, Events


class Open(_IndicatorBase):
    @property
    def name(self) -> str:
        return "Open"

    def _compute(self, bar: Events.Datafeed.Bar) -> float:
        return bar.open


class High(_IndicatorBase):
    @property
    def name(self) -> str:
        return "High"

    def _compute(self, bar: Events.Datafeed.Bar) -> float:
        return bar.high


class Low(_IndicatorBase):
    @property
    def name(self) -> str:
        return "Low"

    def _compute(self, bar: Events.Datafeed.Bar) -> float:
        return bar.low


class Close(_IndicatorBase):
    @property
    def name(self) -> str:
        return "Close"

    def _compute(self, bar: Events.Datafeed.Bar) -> float:
        return bar.close


class Volume(_IndicatorBase):
    IS_OUTPUT_SCALED = False

    @property
    def name(self) -> str:
        return "Volume"

    def _compute(self, bar: Events.Datafeed.Bar) -> float:
        return float(bar.volume) if bar.volume is not None else float("nan")


class SimpleMovingAverage(_IndicatorBase):
    def __init__(self, window_length: int, source: _IndicatorBase) -> None:
        super().__init__(max_history=window_length)
        self.IS_OUTPUT_SCALED = source.IS_OUTPUT_SCALED
        self._window_len = window_length
        source._max_history = max(source._max_history, window_length)
        self._source_ind = self.add_indicator(source)

    @property
    def name(self) -> str:
        return f"SMA ({self._window_len}, {self._source_ind.name})"

    def _compute(self, bar: Events.Datafeed.Bar) -> float:
        src_hist: deque[float] | None = self._source_ind.get_history(bar.symbol)
        if src_hist is None or len(src_hist) < self._window_len:
            return float("nan")
        return sum(src_hist[i] for i in range(-self._window_len, 0)) / self._window_len
