import os
from dataclasses import dataclass
from pathlib import Path

TLC_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
ZONES_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"


@dataclass(frozen=True)
class Paths:
    root: Path

    @property
    def raw(self) -> Path:
        return self.root / "raw"

    @property
    def bronze(self) -> Path:
        return self.root / "bronze" / "yellow_trips"

    @property
    def silver(self) -> Path:
        return self.root / "silver" / "yellow_trips"

    @property
    def quarantine(self) -> Path:
        return self.root / "silver" / "yellow_trips_quarantine"

    @property
    def runs(self) -> Path:
        return self.root / "runs"


def load_paths() -> Paths:
    return Paths(Path(os.environ.get("LAKEHOUSE_DATA", "data")).resolve())
