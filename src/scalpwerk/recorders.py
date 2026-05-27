import json

from dataclasses import asdict
from io import TextIOWrapper
from pathlib import Path

from .core import EventBase, RecorderBase


class JSONLRecorder(RecorderBase):
    def __init__(self, run_id: str, runs_dir_path: Path = Path("./runs")) -> None:
        self._run_id: str = run_id
        self._jsonl_path: Path = runs_dir_path / self._run_id / "events.jsonl"
        self._jsonl_file: TextIOWrapper | None = None
        super().__init__()  # attributes must exist before starting thread

    @property
    def path(self) -> Path:
        return self._jsonl_path

    def _event_loop(self) -> None:
        self._jsonl_path.parent.mkdir(parents=True, exist_ok=True)
        self._jsonl_file = open(self._jsonl_path, "w")
        try:
            super()._event_loop()
        finally:
            if self._jsonl_file is not None:
                self._jsonl_file.close()

    def _on_event(self, event: EventBase) -> None:
        assert self._jsonl_file is not None
        record = {
            "run_id": self._run_id,
            "event_type": type(event).__qualname__,  # includes parent class chain
            "data": asdict(event),
        }
        self._jsonl_file.write(json.dumps(record, default=str) + "\n")
        self._jsonl_file.flush()
