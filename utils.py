import csv
import json
import pickle
from dataclasses import asdict
from pathlib import Path

from src.scalpwerk.core import EventMessageBase, DomainEvents, PeriodType


def convert_databento_csv(csv_path: Path, output_path: Path) -> None:
    rtype_to_period: dict[int, PeriodType] = {
        32: PeriodType.SECOND,
        33: PeriodType.MINUTE,
        34: PeriodType.HOUR,
        35: PeriodType.DAY,
    }
    with open(csv_path) as f_in, open(output_path, "wb") as f_out:
        header = next(csv.reader(f_in))
        col = {name: pos for pos, name in enumerate(header)}
        for row in csv.reader(f_in):
            period = rtype_to_period.get(int(row[col["rtype"]]))
            if period is None:
                continue
            pickle.dump(
                DomainEvents.NewBar(
                    symbol=row[col["symbol"]],
                    period_start=int(row[col["ts_event"]]),
                    period_type=period,
                    open=int(row[col["open"]]),
                    high=int(row[col["high"]]),
                    low=int(row[col["low"]]),
                    close=int(row[col["close"]]),
                    volume=int(row[col["volume"]]),
                ),
                f_out,
            )


def load_events(pkl_path: Path) -> list[EventMessageBase]:
    events: list[EventMessageBase] = []
    with open(pkl_path, "rb") as f:
        while True:
            try:
                events.append(pickle.load(f))
            except EOFError:
                break
    return events


def export_as_jsonl(pkl_path: Path, output_path: Path) -> None:
    with open(output_path, "w") as f:
        for event in load_events(pkl_path):
            f.write(
                json.dumps(
                    {"event_type": type(event).__qualname__, "data": asdict(event)},
                    default=str,
                )
                + "\n"
            )
