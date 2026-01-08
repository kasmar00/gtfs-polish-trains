# SPDX-FileCopyrightText: 2026 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

import csv
import re
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date
from io import TextIOWrapper
from typing import Self
from zipfile import ZipFile

from impuls.tools.types import StrPath

PlkDate = date
GtfsDate = date
Trains = dict["TrainKey", dict[PlkDate, "Trips"]]


@dataclass(frozen=True, eq=True)
class TrainKey:
    schedule_id: int
    order_id: int

    @classmethod
    def from_trip_id(cls, trip_id: str) -> Self | None:
        if m := re.search(r"^([0-9]+)_([0-9]+)", trip_id):
            return cls(int(m[1]), int(m[2]))
        return None


@dataclass
class Trips:
    gtfs_start_date: GtfsDate
    all_ids: list[str] = field(default_factory=list[str])
    by_order: dict[int, "StopTime"] = field(default_factory=dict[int, "StopTime"])


@dataclass
class StopTime:
    trip_id: str
    stop_sequence: int
    stop_id: str


def load_from_gtfs(gtfs_path: StrPath) -> Trains:
    trains = Trains()
    services = dict[str, list[date]]()
    trip_id_to_plk_dates = defaultdict[str, list[date]](list)

    with ZipFile(gtfs_path, "r") as arch:
        # Load calendar_dates.txt
        with arch.open("calendar_dates.txt", "r") as f:
            for row in csv.DictReader(TextIOWrapper(f, "utf-8-sig", newline="")):
                services.setdefault(row["service_id"], []).append(extract_date(row["date"]))

        # Load trips.txt
        with arch.open("trips.txt", "r") as f:
            for row in csv.DictReader(TextIOWrapper(f, "utf-8-sig", newline="")):
                trip_id = row["trip_id"]
                key = TrainKey.from_trip_id(trip_id)
                if key is None:
                    raise ValueError(f"failed to extract key from trip_id {trip_id!r}")

                service_id = row["service_id"]
                start_date_offset = extract_start_date_offset(service_id)
                active_gtfs_dates = services[service_id]

                keyed_trains = trains.setdefault(key, {})

                for gtfs_date in active_gtfs_dates:
                    # start_date_offset goes from gtfs_date to plk_date,
                    # here we want to do the opposite
                    plk_date = add_days(gtfs_date, -start_date_offset)
                    trip_id_to_plk_dates[trip_id].append(plk_date)

                    if t := keyed_trains.get(plk_date):
                        t.all_ids.append(trip_id)
                    else:
                        keyed_trains[plk_date] = Trips(
                            gtfs_start_date=gtfs_date,
                            all_ids=[trip_id],
                        )

        # Load stop_times.txt
        with arch.open("stop_times.txt", "r") as f:
            for row in csv.DictReader(TextIOWrapper(f, "utf-8-sig", newline="")):
                trip_id = row["trip_id"]
                key = TrainKey.from_trip_id(trip_id)
                if key and (by_date := trains.get(key)):
                    for plk_date in trip_id_to_plk_dates[trip_id]:
                        by_date[plk_date].by_order[int(row["plk_order"])] = StopTime(
                            trip_id=trip_id,
                            stop_sequence=int(row["stop_sequence"]),
                            stop_id=row["stop_id"],
                        )

    return trains


def extract_start_date_offset(service_id: str) -> int:
    if m := re.search(r"([+-][0-9]+)D$", service_id):
        return int(m[1])
    return 0


def extract_date(day: str) -> date:
    y = int(day[0:4])
    m = int(day[4:6])
    d = int(day[6:8])
    return date(y, m, d)


def add_days(d: date, offset: int) -> date:
    return date.fromordinal(d.toordinal() + offset)
