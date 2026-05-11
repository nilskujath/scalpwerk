import abc
import collections
import math

from .core import Constants, Enums, Events, IndicatorBase

_PRICE_FIELDS = frozenset(
    {
        Enums.BarField.OPEN,
        Enums.BarField.HIGH,
        Enums.BarField.LOW,
        Enums.BarField.CLOSE,
    }
)


def _read_bar_field(event: Events.Datafeed.Bar, bar_field: Enums.BarField) -> float:
    raw = float(getattr(event, bar_field.value))
    return raw / Constants.PRICE_SCALE if bar_field in _PRICE_FIELDS else raw


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
        value = _read_bar_field(event, self._bar_field)
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
        scale = Constants.PRICE_SCALE
        high = float(event.high) / scale
        low = float(event.low) / scale
        close = float(event.close) / scale

        if symbol not in self._prev_close:
            self._prev_close[symbol] = close
            self._tr_buffer[symbol] = []
            return math.nan

        prev_close = self._prev_close[symbol]
        true_range = max(high - low, abs(high - prev_close), abs(low - prev_close))
        self._prev_close[symbol] = close

        if symbol not in self._atr:
            self._tr_buffer[symbol].append(true_range)
            if len(self._tr_buffer[symbol]) < self._period:
                return math.nan
            self._atr[symbol] = sum(self._tr_buffer[symbol]) / self._period
            del self._tr_buffer[symbol]
            return self._atr[symbol]

        self._atr[symbol] = (
            self._atr[symbol] * (self._period - 1) + true_range
        ) / self._period
        return self._atr[symbol]


class RSI(IndicatorBase):
    def __init__(
        self,
        period: int = 14,
        bar_field: Enums.BarField = Enums.BarField.CLOSE,
        max_history: int = 100,
    ) -> None:
        super().__init__(max_history=max_history)
        self._period = period
        self._bar_field = bar_field
        self._prev_value: dict[str, float] = {}
        self._gain_buffer: dict[str, list[tuple[float, float]]] = {}
        self._avg_gain: dict[str, float] = {}
        self._avg_loss: dict[str, float] = {}

    @property
    def name(self) -> str:
        return f"RSI_{self._period}_{self._bar_field.name}"

    def _compute(self, event: Events.Datafeed.Bar) -> float:
        symbol = event.symbol
        value = _read_bar_field(event, self._bar_field)

        if symbol not in self._prev_value:
            self._prev_value[symbol] = value
            self._gain_buffer[symbol] = []
            return math.nan

        change = value - self._prev_value[symbol]
        self._prev_value[symbol] = value
        gain = max(change, 0.0)
        loss = max(-change, 0.0)

        if symbol not in self._avg_gain:
            self._gain_buffer[symbol].append((gain, loss))
            if len(self._gain_buffer[symbol]) < self._period:
                return math.nan
            gains, losses = zip(*self._gain_buffer[symbol])
            self._avg_gain[symbol] = sum(gains) / self._period
            self._avg_loss[symbol] = sum(losses) / self._period
            del self._gain_buffer[symbol]
        else:
            self._avg_gain[symbol] = (
                self._avg_gain[symbol] * (self._period - 1) + gain
            ) / self._period
            self._avg_loss[symbol] = (
                self._avg_loss[symbol] * (self._period - 1) + loss
            ) / self._period

        avg_gain = self._avg_gain[symbol]
        avg_loss = self._avg_loss[symbol]

        if avg_loss == 0.0:
            return 100.0

        rs = avg_gain / avg_loss
        return 100.0 - 100.0 / (1.0 + rs)


class _BollingerBase(IndicatorBase, abc.ABC):
    def __init__(
        self,
        period: int = 20,
        num_std: float = 2.0,
        bar_field: Enums.BarField = Enums.BarField.CLOSE,
        max_history: int = 100,
    ) -> None:
        super().__init__(max_history=max_history)
        self._period = period
        self._num_std = num_std
        self._bar_field = bar_field
        self._windows: dict[str, collections.deque[float]] = {}

    def _compute_bands(self, event: Events.Datafeed.Bar) -> tuple[float, float, float]:
        value = _read_bar_field(event, self._bar_field)
        symbol = event.symbol

        if symbol not in self._windows:
            self._windows[symbol] = collections.deque(maxlen=self._period)
        self._windows[symbol].append(value)

        if len(self._windows[symbol]) < self._period:
            return math.nan, math.nan, math.nan

        window = self._windows[symbol]
        mean = sum(window) / self._period

        variance = sum((x - mean) ** 2 for x in window) / self._period
        std = math.sqrt(variance)

        upper = mean + self._num_std * std
        lower = mean - self._num_std * std
        bandwidth = (upper - lower) / mean * 100.0 if mean != 0.0 else math.nan

        return upper, lower, bandwidth


class BollingerUpper(_BollingerBase):
    @property
    def name(self) -> str:
        return f"BB_UPPER_{self._period}_{self._num_std}_{self._bar_field.name}"

    def _compute(self, event: Events.Datafeed.Bar) -> float:
        upper, _, _ = self._compute_bands(event)
        return upper


class BollingerLower(_BollingerBase):
    @property
    def name(self) -> str:
        return f"BB_LOWER_{self._period}_{self._num_std}_{self._bar_field.name}"

    def _compute(self, event: Events.Datafeed.Bar) -> float:
        _, lower, _ = self._compute_bands(event)
        return lower


class BollingerBandwidth(_BollingerBase):
    @property
    def name(self) -> str:
        return f"BB_BW_{self._period}_{self._num_std}_{self._bar_field.name}"

    def _compute(self, event: Events.Datafeed.Bar) -> float:
        _, _, bandwidth = self._compute_bands(event)
        return bandwidth


class BoostedRSI(IndicatorBase):
    def __init__(
        self,
        rsi_period: int = 14,
        momentum_period: int = 9,
        short_rsi_period: int = 3,
        short_rsi_smoothing: int = 3,
        bar_field: Enums.BarField = Enums.BarField.CLOSE,
        max_history: int = 100,
    ) -> None:
        super().__init__(max_history=max_history)
        self._rsi_period = rsi_period
        self._momentum_period = momentum_period
        self._short_rsi_period = short_rsi_period
        self._short_rsi_smoothing = short_rsi_smoothing
        self._bar_field = bar_field

        self._rsi_main = RSI(
            period=rsi_period,
            bar_field=bar_field,
            max_history=max(max_history, momentum_period + 1),
        )
        self._rsi_short = RSI(
            period=short_rsi_period,
            bar_field=bar_field,
            max_history=max(max_history, short_rsi_smoothing),
        )
        self.add_indicator(self._rsi_main)
        self.add_indicator(self._rsi_short)

    @property
    def name(self) -> str:
        return (
            f"CBCI_{self._rsi_period}_{self._momentum_period}"
            f"_{self._short_rsi_period}_{self._short_rsi_smoothing}"
            f"_{self._bar_field.name}"
        )

    def _compute(self, event: Events.Datafeed.Bar) -> float:
        symbol = event.symbol

        rsi_now = self._rsi_main.latest(symbol)
        if math.isnan(rsi_now):
            return math.nan

        rsi_past = self._rsi_main[symbol, -(self._momentum_period + 1)]
        if math.isnan(rsi_past):
            return math.nan
        rsi_momentum = rsi_now - rsi_past

        short_values = [
            self._rsi_short[symbol, -i] for i in range(1, self._short_rsi_smoothing + 1)
        ]
        if any(math.isnan(v) for v in short_values):
            return math.nan
        rsi_smoothed = sum(short_values) / self._short_rsi_smoothing

        return rsi_momentum + rsi_smoothed


class ReverseRSI(IndicatorBase):
    def __init__(
        self,
        period: int = 14,
        target_rsi: float = 80.0,
        bar_field: Enums.BarField = Enums.BarField.CLOSE,
        max_history: int = 100,
    ) -> None:
        super().__init__(max_history=max_history)
        self._period = period
        self._target_rsi = target_rsi
        self._bar_field = bar_field

        self._rsi = RSI(period=period, bar_field=bar_field, max_history=max_history)
        self.add_indicator(self._rsi)

    @property
    def name(self) -> str:
        return f"REV_RSI_{self._period}_{self._target_rsi}_{self._bar_field.name}"

    def _compute(self, event: Events.Datafeed.Bar) -> float:
        symbol = event.symbol

        if symbol not in self._rsi._avg_gain:
            return math.nan

        auc = self._rsi._avg_gain[symbol]
        adc = self._rsi._avg_loss[symbol]
        c0 = self._rsi._prev_value[symbol]
        k = self._period
        target = self._target_rsi

        if target <= 0.0 or target >= 100.0:
            return math.nan

        x = (k - 1) * (adc * (target / (100.0 - target)) - auc)

        if x >= 0.0:
            return c0 + x
        else:
            return c0 + x * ((100.0 - target) / target)


_SEEKING_HIGH = 0
_SEEKING_LOW = 1


class _SwingBase(IndicatorBase, abc.ABC):
    def __init__(
        self,
        atr_period: int = 14,
        atr_multiplier: float = 2.0,
        max_history: int = 100,
    ) -> None:
        super().__init__(max_history=max_history)
        self._atr_period = atr_period
        self._atr_multiplier = atr_multiplier

        self._atr_ind = ATR(period=atr_period, max_history=2)
        self.add_indicator(self._atr_ind)

        self._state: dict[str, int] = {}
        self._tracked_high: dict[str, float] = {}
        self._tracked_high_ns: dict[str, int] = {}
        self._tracked_low: dict[str, float] = {}
        self._tracked_low_ns: dict[str, int] = {}
        self._last_high: dict[str, float] = {}
        self._last_high_ns: dict[str, int] = {}
        self._last_low: dict[str, float] = {}
        self._last_low_ns: dict[str, int] = {}

    def _compute_swings(self, event: Events.Datafeed.Bar) -> tuple[float, float]:
        symbol = event.symbol
        scale = Constants.PRICE_SCALE
        high = float(event.high) / scale
        low = float(event.low) / scale
        ts = event.occurred_at_ns

        atr = self._atr_ind.latest(symbol)
        if math.isnan(atr):
            return math.nan, math.nan

        threshold = self._atr_multiplier * atr

        if symbol not in self._state:
            self._state[symbol] = _SEEKING_HIGH
            self._tracked_high[symbol] = high
            self._tracked_high_ns[symbol] = ts
            self._tracked_low[symbol] = low
            self._tracked_low_ns[symbol] = ts
            return math.nan, math.nan

        if self._state[symbol] == _SEEKING_HIGH:
            if high > self._tracked_high[symbol]:
                self._tracked_high[symbol] = high
                self._tracked_high_ns[symbol] = ts

            if self._tracked_high[symbol] - low >= threshold:
                self._last_high[symbol] = self._tracked_high[symbol]
                self._last_high_ns[symbol] = self._tracked_high_ns[symbol]
                self._state[symbol] = _SEEKING_LOW
                self._tracked_low[symbol] = low
                self._tracked_low_ns[symbol] = ts

        else:
            if low < self._tracked_low[symbol]:
                self._tracked_low[symbol] = low
                self._tracked_low_ns[symbol] = ts

            if high - self._tracked_low[symbol] >= threshold:
                self._last_low[symbol] = self._tracked_low[symbol]
                self._last_low_ns[symbol] = self._tracked_low_ns[symbol]
                self._state[symbol] = _SEEKING_HIGH
                self._tracked_high[symbol] = high
                self._tracked_high_ns[symbol] = ts

        return (
            self._last_high.get(symbol, math.nan),
            self._last_low.get(symbol, math.nan),
        )


class SwingHigh(_SwingBase):
    @property
    def name(self) -> str:
        return f"SWING_HIGH_{self._atr_period}_{self._atr_multiplier}"

    def _compute(self, event: Events.Datafeed.Bar) -> float:
        high, _ = self._compute_swings(event)
        return high

    def swing_bar_ns(self, symbol: str) -> int:
        return self._last_high_ns.get(symbol, 0)


class SwingLow(_SwingBase):
    @property
    def name(self) -> str:
        return f"SWING_LOW_{self._atr_period}_{self._atr_multiplier}"

    def _compute(self, event: Events.Datafeed.Bar) -> float:
        _, low = self._compute_swings(event)
        return low

    def swing_bar_ns(self, symbol: str) -> int:
        return self._last_low_ns.get(symbol, 0)
