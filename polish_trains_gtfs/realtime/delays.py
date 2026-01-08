# SPDX-FileCopyrightText: 2025 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

import logging
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, cast

import requests

from .. import json
from . import gtfs_realtime_pb2, lookup
from .fact import Fact, FactContainer
from .tools import TripDate

logger = logging.getLogger("Delays")

PAGE_SIZE = 5000  # Limited by the API
MAX_PAGES = 4


@dataclass
class Stats:
    total: int = 0
    matched: int = 0
    invalid_order_id: int = 0
    invalid_start_date: int = 0

    def __str__(self) -> str:
        matched_percentage = 100 * self.matched / self.total
        return (
            f"matched {self.matched} / {self.total} ({matched_percentage:.2f} %); "
            f"invalid order id: {self.invalid_order_id}; "
            f"invalid start date: {self.invalid_start_date}"
        )


@dataclass
class StopDelay:
    stop_id: str
    stop_sequence: int
    cancelled: bool
    confirmed: bool
    live_arrival: datetime | None = None
    live_departure: datetime | None = None

    def as_json(self) -> json.Object:
        return {
            "stop_id": str(self.stop_id),
            "stop_sequence": self.stop_sequence,
            "confirmed": self.confirmed,
            "cancelled": self.cancelled,
            "arrival": self.live_arrival.isoformat() if self.live_arrival else None,
            "departure": self.live_departure.isoformat() if self.live_departure else None,
        }

    def as_gtfs_rt(self) -> gtfs_realtime_pb2.TripUpdate.StopTimeUpdate:
        u = gtfs_realtime_pb2.TripUpdate.StopTimeUpdate(
            stop_sequence=self.stop_sequence,
            stop_id=str(self.stop_id),
        )
        if self.cancelled:
            u.schedule_relationship = gtfs_realtime_pb2.TripUpdate.StopTimeUpdate.SKIPPED
        else:
            uncertainty = 0 if self.confirmed else 1
            u.schedule_relationship = gtfs_realtime_pb2.TripUpdate.StopTimeUpdate.SCHEDULED
            if self.live_arrival:
                u.arrival.uncertainty = uncertainty
                u.arrival.time = round(self.live_arrival.timestamp())
            if self.live_departure:
                u.departure.uncertainty = uncertainty
                u.departure.time = round(self.live_departure.timestamp())
        return u


@dataclass
class TripDelay(Fact):
    trip: TripDate
    stops: list[StopDelay]

    def as_json(self) -> Mapping[str, Any]:
        return {
            "type": "delay",
            "id": f"D_{self.trip.start_date.isoformat()}_{self.trip.trip_id}",
            "trip": self.trip.as_json(),
            "stops": [i.as_json() for i in self.stops],
        }

    def as_gtfs_rt(self) -> gtfs_realtime_pb2.FeedEntity:
        return gtfs_realtime_pb2.FeedEntity(
            id=f"D_{self.trip.start_date.isoformat()}_{self.trip.trip_id}",
            trip_update=gtfs_realtime_pb2.TripUpdate(
                trip=self.trip.as_gtfs_rt(),
                stop_time_update=(i.as_gtfs_rt() for i in self.stops),
            ),
        )


def fetch_delays(apikey: str, trains: lookup.Trains) -> FactContainer[TripDelay]:
    container = FactContainer[TripDelay](timestamp=datetime.min, facts=[])
    page_size = PAGE_SIZE
    stats = Stats()

    for page in range(1, MAX_PAGES + 1):
        with requests.get(
            "https://pdp-api.plk-sa.pl/api/v1/operations/shortened",
            params={"page": str(page), "pageSize": str(page_size), "fullRoutes": "true"},
            headers={"X-Api-Key": apikey},
        ) as r:
            r.raise_for_status()
            data = r.json()

            # Parse the timestamp at the first page
            if page == 1:
                container.timestamp = datetime.fromisoformat(data["ts"])

            # Parse the facts
            container.facts.extend(
                f for i in data["tr"] for f in parse_train_delay(i, trains, stats).values()
            )

            # Stop requesting data if there is no next page
            if not data["pg"].get("hn", False):
                logger.critical("%s", stats)
                return container

    else:
        raise ValueError(f"couldn't retrieve all delays in {MAX_PAGES} requests")


def parse_train_delay(d: json.Object, l: lookup.Trains, stats: Stats) -> dict[str, TripDelay]:
    stats.total += 1

    key = lookup.TrainKey(d["sid"], d["oid"])
    trips_by_date = l.get(key)
    if not trips_by_date:
        stats.invalid_order_id += 1
        # logger.warning("Unknown %r", key)
        return {}

    by_trip = dict[str, TripDelay]()
    plk_start_date = date.fromordinal(date.fromisoformat(d["od"][:10]).toordinal())
    trips = trips_by_date.get(plk_start_date)
    if not trips:
        stats.invalid_start_date += 1
        # logger.warning("%r doesn't run on %s", key, plk_start_date.isoformat())
        return {}

    stats.matched += 1

    for stop_obj in d["st"]:
        order = cast(int, stop_obj["psn"])
        stop_time = trips.by_order.get(order)
        if stop_time is None:
            logger.warning("%r refers to unknown stop order %d", key, order)
            continue

        stop_delay = parse_stop_delay(stop_obj, stop_time)
        if stop_delay.stop_id != stop_time.stop_id:
            logger.warning(
                "%r at order=%d changes stop %s → %s",
                key,
                order,
                stop_delay.stop_id,
                stop_time.stop_id,
            )

        if trip_delay := by_trip.get(stop_time.trip_id):
            trip_delay.stops.append(stop_delay)
        else:
            by_trip[stop_time.trip_id] = TripDelay(
                trip=TripDate(stop_time.trip_id, trips.gtfs_start_date),
                stops=[stop_delay],
            )

    return by_trip


def parse_stop_delay(s: json.Object, stop_time: lookup.StopTime) -> StopDelay:
    return StopDelay(
        stop_id=str(s["id"]),
        stop_sequence=stop_time.stop_sequence,
        cancelled=s.get("cn") or False,
        confirmed=s.get("cf") or False,
        live_arrival=(datetime.fromisoformat(timestamp) if (timestamp := s.get("aa")) else None),
        live_departure=(datetime.fromisoformat(timestamp) if (timestamp := s.get("ad")) else None),
    )
