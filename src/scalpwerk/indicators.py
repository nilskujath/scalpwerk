from collections import deque
from enum import Enum, auto

from src.scalpwerk.core import IndicatorBase, Events, IndicatorName, Symbol


class ClampSide(Enum):
    KEEP_POSITIVES = auto()
    KEEP_NEGATIVES = auto()


class BollingerSide(Enum):
    UPPER = auto()
    LOWER = auto()


class Open(IndicatorBase):
    @property
    def name(self) -> IndicatorName:
        return "Open"

    def _compute(self, bar: Events.Datafeed.Bar) -> float:
        return bar.open


class High(IndicatorBase):
    @property
    def name(self) -> IndicatorName:
        return "High"

    def _compute(self, bar: Events.Datafeed.Bar) -> float:
        return bar.high


class Low(IndicatorBase):
    @property
    def name(self) -> IndicatorName:
        return "Low"

    def _compute(self, bar: Events.Datafeed.Bar) -> float:
        return bar.low


class Close(IndicatorBase):
    @property
    def name(self) -> IndicatorName:
        return "Close"

    def _compute(self, bar: Events.Datafeed.Bar) -> float:
        return bar.close


class Volume(IndicatorBase):
    IS_OUTPUT_SCALED = False

    @property
    def name(self) -> IndicatorName:
        return "Volume"

    def _compute(self, bar: Events.Datafeed.Bar) -> float:
        return float(bar.volume) if bar.volume is not None else float("nan")


class SMA(IndicatorBase):
    def __init__(self, period: int, source: IndicatorBase) -> None:
        super().__init__()
        self.IS_OUTPUT_SCALED = source.IS_OUTPUT_SCALED
        if period < 1:
            raise ValueError("period must be positive")
        self._period = period
        self._source = self.add_indicator(source)
        self._source_value_buffer: dict[Symbol, deque[float]] = {}

    @property
    def name(self) -> IndicatorName:
        return f"SMA ({self._period}, {self._source.name})"

    def _compute(self, bar: Events.Datafeed.Bar) -> float:
        if bar.symbol not in self._source_value_buffer:
            self._source_value_buffer[bar.symbol] = deque(maxlen=self._period)
        self._source_value_buffer[bar.symbol].append(self._source.latest(bar.symbol))
        if len(self._source_value_buffer[bar.symbol]) < self._period:
            return float("nan")
        return sum(self._source_value_buffer[bar.symbol]) / self._period


class EMA(IndicatorBase):
    def __init__(
        self, period: int, source: IndicatorBase, *, alpha: float | None = None
    ) -> None:
        super().__init__()
        self.IS_OUTPUT_SCALED = source.IS_OUTPUT_SCALED
        if period < 1:
            raise ValueError("period must be positive")
        self._period = period
        self._alpha = alpha if alpha is not None else 2 / (period + 1)
        self._source = self.add_indicator(source)
        self._source_buffer_for_seed: dict[Symbol, deque[float]] = {}

    @property
    def name(self) -> IndicatorName:
        return f"EMA ({self._period}, {self._source.name})"

    def _compute(self, bar: Events.Datafeed.Bar) -> float:
        val = self._source.latest(bar.symbol)
        prev = self[bar.symbol, -1]

        if prev == prev:  # not NaN, so already past seed phase
            return prev * (1 - self._alpha) + val * self._alpha

        if bar.symbol not in self._source_buffer_for_seed:
            self._source_buffer_for_seed[bar.symbol] = deque(maxlen=self._period)
        self._source_buffer_for_seed[bar.symbol].append(val)
        if len(self._source_buffer_for_seed[bar.symbol]) < self._period:
            return float("nan")
        seed = sum(self._source_buffer_for_seed[bar.symbol]) / self._period
        del self._source_buffer_for_seed[bar.symbol]
        return seed


class SMADetrendOscillator(IndicatorBase):
    def __init__(
        self,
        fast_period: int,
        slow_period: int,
        sma_source: IndicatorBase | None = None,  # `None` to avoid mutable default
    ) -> None:
        super().__init__()
        if not 0 < fast_period < slow_period:
            raise ValueError("requires 0 < `fast_period` < `slow_period`")
        self._fast_period = fast_period
        self._slow_period = slow_period
        if sma_source is None:
            sma_source = Close()
        self.IS_OUTPUT_SCALED = sma_source.IS_OUTPUT_SCALED
        self._fast_sma = self.add_indicator(SMA(fast_period, sma_source))
        self._slow_sma = self.add_indicator(SMA(slow_period, sma_source))

    @property
    def name(self) -> IndicatorName:
        return f"Detrend ({self._fast_period}, {self._slow_period})"

    def _compute(self, bar: Events.Datafeed.Bar) -> float:
        fast = self._fast_sma.latest(bar.symbol)
        slow = self._slow_sma.latest(bar.symbol)
        if fast != fast or slow != slow:  # NaN check
            return float("nan")
        return fast - slow


class Momentum(IndicatorBase):
    def __init__(self, period: int, source: IndicatorBase) -> None:
        super().__init__()
        self.IS_OUTPUT_SCALED = source.IS_OUTPUT_SCALED
        if period < 1:
            raise ValueError("period must be positive")
        self._period = period
        self._source = self.add_indicator(source)
        self._source_value_buffer: dict[Symbol, deque[float]] = {}

    @property
    def name(self) -> IndicatorName:
        return f"Momentum ({self._period}, {self._source.name})"

    def _compute(self, bar: Events.Datafeed.Bar) -> float:
        if bar.symbol not in self._source_value_buffer:
            self._source_value_buffer[bar.symbol] = deque(maxlen=self._period + 1)
        self._source_value_buffer[bar.symbol].append(self._source.latest(bar.symbol))
        if len(self._source_value_buffer[bar.symbol]) < self._period + 1:
            return float("nan")
        return (
            self._source_value_buffer[bar.symbol][-1]
            - self._source_value_buffer[bar.symbol][0]
        )


class Clamp(IndicatorBase):
    def __init__(self, source: IndicatorBase, *, clamp_side: ClampSide) -> None:
        super().__init__()
        self.IS_OUTPUT_SCALED = source.IS_OUTPUT_SCALED
        self._clamp_side = clamp_side
        self._source = self.add_indicator(source)

    @property
    def name(self) -> IndicatorName:
        return f"Clamp {self._clamp_side.name} ({self._source.name})"

    def _compute(self, bar: Events.Datafeed.Bar) -> float:
        latest_value = self._source.latest(bar.symbol)
        match self._clamp_side:
            case ClampSide.KEEP_POSITIVES:
                return max(0.0, latest_value)
            case ClampSide.KEEP_NEGATIVES:
                return min(0.0, latest_value)


class RSI(IndicatorBase):
    IS_OUTPUT_SCALED = False

    def __init__(
        self,
        period: int,
        source: IndicatorBase | None = None,
    ) -> None:
        super().__init__()
        if period < 1:
            raise ValueError("period must be positive")
        self._period = period
        if source is None:
            source = Close()

        self._avg_gain = self.add_indicator(
            EMA(
                period=period,
                source=Clamp(
                    source=Momentum(1, source),
                    clamp_side=ClampSide.KEEP_POSITIVES,
                ),
                alpha=1 / period,
            )
        )
        self._avg_loss = self.add_indicator(
            EMA(
                period=period,
                source=Clamp(
                    source=Momentum(1, source),
                    clamp_side=ClampSide.KEEP_NEGATIVES,
                ),
                alpha=1 / period,
            )
        )

    @property
    def name(self) -> IndicatorName:
        return f"RSI ({self._period})"

    def _compute(self, bar: Events.Datafeed.Bar) -> float:
        gain = self._avg_gain.latest(bar.symbol)
        loss = abs(self._avg_loss.latest(bar.symbol))
        if gain != gain or loss != loss:
            return float("nan")
        if loss == 0:
            return 100.0
        return 100 - 100 / (1 + gain / loss)


class BollingerBand(IndicatorBase):
    def __init__(
        self,
        period: int,
        side: BollingerSide,
        source: IndicatorBase | None = None,
        *,
        num_std: float = 2.0,
    ) -> None:
        super().__init__()
        if period < 1:
            raise ValueError("period must be positive")
        if source is None:
            source = Close()
        self.IS_OUTPUT_SCALED = source.IS_OUTPUT_SCALED
        self._period = period
        self._side = side
        self._num_std = float(num_std)
        self._source = self.add_indicator(source)
        self._source_value_buffer: dict[Symbol, deque[float]] = {}

    @property
    def name(self) -> IndicatorName:
        return f"BB{self._side.name.capitalize()} ({self._period}, {self._num_std:g})"

    def _compute(self, bar: Events.Datafeed.Bar) -> float:
        if bar.symbol not in self._source_value_buffer:
            self._source_value_buffer[bar.symbol] = deque(maxlen=self._period)
        self._source_value_buffer[bar.symbol].append(self._source.latest(bar.symbol))
        if len(self._source_value_buffer[bar.symbol]) < self._period:
            return float("nan")
        buf = list(self._source_value_buffer[bar.symbol])
        mean = sum(buf) / self._period
        std = (sum((x - mean) ** 2 for x in buf) / self._period) ** 0.5
        sign = 1 if self._side == BollingerSide.UPPER else -1
        return mean + sign * self._num_std * std


class BollingerBandwidth(IndicatorBase):
    IS_OUTPUT_SCALED = False

    def __init__(
        self,
        period: int,
        source: IndicatorBase | None = None,
        *,
        num_std: float = 2.0,
    ) -> None:
        super().__init__()
        if period < 1:
            raise ValueError("period must be positive")
        if source is None:
            source = Close()
        self._period = period
        self._num_std = float(num_std)
        self._source = self.add_indicator(source)
        self._source_value_buffer: dict[Symbol, deque[float]] = {}

    @property
    def name(self) -> IndicatorName:
        return f"BBWidth ({self._period}, {self._num_std:g})"

    def _compute(self, bar: Events.Datafeed.Bar) -> float:
        if bar.symbol not in self._source_value_buffer:
            self._source_value_buffer[bar.symbol] = deque(maxlen=self._period)
        self._source_value_buffer[bar.symbol].append(self._source.latest(bar.symbol))
        if len(self._source_value_buffer[bar.symbol]) < self._period:
            return float("nan")
        buf = list(self._source_value_buffer[bar.symbol])
        mean = sum(buf) / self._period
        if mean == 0:
            return float("nan")
        std = (sum((x - mean) ** 2 for x in buf) / self._period) ** 0.5
        return (2.0 * self._num_std * std) / mean
