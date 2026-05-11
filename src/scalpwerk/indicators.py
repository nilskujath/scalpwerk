import collections
import math

from .core import Enums, Events, IndicatorBase


class SMA(IndicatorBase):

    def __init__(
        self,
        period: int = 200,
        bar_field: Enums.BarField = Enums.BarField.CLOSE,
        max_history: int = 100,
    ) -> None:
        super().__init__(max_history=max_history)
        self._period = period
        self._bar_field = bar_field
        self._windows: dict[str, collections.deque[float]] = {}

    @property
    def name(self) -> str:
        return f"SMA_{self._period}_{self._bar_field.name}"

    def _compute(self, event: Events.Datafeed.Bar) -> float:
        value = float(getattr(event, self._bar_field.value))
        symbol = event.symbol

        if symbol not in self._windows:
            self._windows[symbol] = collections.deque(maxlen=self._period)

        self._windows[symbol].append(value)

        if len(self._windows[symbol]) < self._period:
            return float("nan")

        return sum(self._windows[symbol]) / self._period


class ATR(IndicatorBase):

    def __init__(
        self,
        period: int = 14,
        max_history: int = 100,
    ) -> None:
        super().__init__(max_history=max_history)
        self._period = period
        self._prev_close: dict[str, float] = {}
        self._tr_buffer: dict[str, list[float]] = {}
        self._atr: dict[str, float] = {}

    @property
    def name(self) -> str:
        return f"ATR_{self._period}"

    def _compute(self, event: Events.Datafeed.Bar) -> float:
        symbol = event.symbol
        high = float(event.high)
        low = float(event.low)
        close = float(event.close)

        # First bar: no previous close, so no True Range yet.
        if symbol not in self._prev_close:
            self._prev_close[symbol] = close
            self._tr_buffer[symbol] = []
            return math.nan

        # True Range: largest of intra-bar range, gap-up reach, gap-down reach.
        prev_close = self._prev_close[symbol]
        true_range = max(high - low, abs(high - prev_close), abs(low - prev_close))
        self._prev_close[symbol] = close

        # Accumulation phase: collect True Ranges for the initial simple average.
        if symbol not in self._atr:
            self._tr_buffer[symbol].append(true_range)
            if len(self._tr_buffer[symbol]) < self._period:
                return math.nan
            self._atr[symbol] = sum(self._tr_buffer[symbol]) / self._period
            del self._tr_buffer[symbol]
            return self._atr[symbol]

        # Wilder's smoothing.
        self._atr[symbol] = (
            self._atr[symbol] * (self._period - 1) + true_range
        ) / self._period
        return self._atr[symbol]
