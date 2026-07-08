from collections import defaultdict, deque

from .core import DomainEvents, IndicatorBase, IndicatorName, IndicatorValue, Symbol


def indicator_from_bar_field(field: str, is_scaled: bool = True) -> type[IndicatorBase]:
    class Indicator(IndicatorBase):
        IS_SCALED = is_scaled

        @property
        def name(self) -> IndicatorName:
            return field.capitalize()

        def _compute(self, bar: DomainEvents.NewBar) -> IndicatorValue:
            return getattr(bar, field)

    Indicator.__name__ = field.capitalize()
    Indicator.__qualname__ = field.capitalize()
    return Indicator


# fmt: off
Open    = indicator_from_bar_field("open")
High    = indicator_from_bar_field("high")
Low     = indicator_from_bar_field("low")
Close   = indicator_from_bar_field("close")
Volume  = indicator_from_bar_field("volume", is_scaled=False)
# fmt: on


class SMA(IndicatorBase):
    def __init__(self, period: int, source: IndicatorBase) -> None:
        if period < 1:
            raise ValueError("period must be positive")
        super().__init__()
        self.IS_SCALED = source.IS_SCALED
        self._period = period
        self._source = self.add_input(source)
        self._sliding_window: defaultdict[Symbol, deque[IndicatorValue]] = defaultdict(
            lambda: deque(maxlen=self._period)
        )

    @property
    def name(self) -> IndicatorName:
        return f"SMA ({self._period}, {self._source.name})"

    def _compute(self, bar: DomainEvents.NewBar) -> IndicatorValue:
        self._sliding_window[bar.symbol].append(self._source[bar.symbol, -1])
        if len(self._sliding_window[bar.symbol]) < self._period:
            return float("nan")
        return sum(self._sliding_window[bar.symbol]) / self._period
