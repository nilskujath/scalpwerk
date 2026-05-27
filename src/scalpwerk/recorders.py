import json
import pickle

from collections.abc import Iterator
from dataclasses import asdict
from io import BufferedWriter
from pathlib import Path

from .core import EventBase, RecorderBase


class PickleRecorder(RecorderBase):
    def __init__(self, run_id: str, runs_dir_path: Path = Path("./runs")) -> None:
        self._run_id = run_id
        self._pkl_path = runs_dir_path / self._run_id / "events.pkl"
        self._pkl_file: BufferedWriter | None = None
        super().__init__()

    @property
    def path(self) -> Path:
        return self._pkl_path

    def _event_loop(self) -> None:
        self._pkl_path.parent.mkdir(parents=True, exist_ok=True)
        self._pkl_file = open(self._pkl_path, "wb")
        try:
            super()._event_loop()
        finally:
            if self._pkl_file is not None:
                self._pkl_file.close()

    def _on_event(self, event: EventBase) -> None:
        assert self._pkl_file is not None
        pickle.dump(event, self._pkl_file)
        self._pkl_file.flush()

    @staticmethod
    def load_events_from_pkl(pkl_path: Path) -> Iterator[EventBase]:
        with open(pkl_path, "rb") as f:
            while True:
                try:
                    yield pickle.load(f)
                except EOFError:
                    break

    @staticmethod
    def export_pkl_as_jsonl(
        pkl_path: Path, output_jsonl_path: Path | None = None
    ) -> None:
        output_jsonl_path = output_jsonl_path or pkl_path.with_suffix(".jsonl")
        lines = (
            json.dumps(
                {"event_type": type(event).__qualname__, "data": asdict(event)},
                default=str,
            )
            for event in PickleRecorder.load_events_from_pkl(pkl_path)
        )

        with open(output_jsonl_path, "w") as f:
            for line in lines:
                f.write(line + "\n")
