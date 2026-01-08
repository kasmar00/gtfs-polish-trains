# SPDX-FileCopyrightText: 2025-2026 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

import logging
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import requests
from impuls.model import Date

from .. import json
from . import gtfs_realtime_pb2
from .fact import Fact, FactContainer
from .schedules import GtfsTripKey, LiveTripKey, Schedules

logger = logging.getLogger("Alerts")


@dataclass
class Alert(Fact):
    id: int
    title: str
    description: str
    trips: list[GtfsTripKey]

    def as_json(self) -> Mapping[str, Any]:
        return {
            "type": "alert",
            "id": f"A_{self.id}",
            "title": self.title,
            "description": self.description,
            "trips": [i.as_json() for i in self.trips],
        }

    def as_gtfs_rt(self) -> gtfs_realtime_pb2.FeedEntity:
        return gtfs_realtime_pb2.FeedEntity(
            id=f"A_{self.id}",
            alert=gtfs_realtime_pb2.Alert(
                header_text=as_translation(self.title),
                description_text=as_translation(self.description),
                informed_entity=(
                    gtfs_realtime_pb2.EntitySelector(trip=i.as_gtfs_rt()) for i in self.trips
                ),
            ),
        )


def fetch_alerts(s: requests.Session, schedules: Schedules) -> FactContainer[Alert]:
    with s.get("https://pdp-api.plk-sa.pl/api/v1/disruptions/shortened") as r:
        r.raise_for_status()
        data = r.json()

    c = FactContainer(
        timestamp=datetime.fromisoformat(data["ts"]),
        facts=[a for i in data["ds"] if (a := parse_alert(i, schedules))],
    )

    logger.info("Alerts fetched successfully; total %d", len(c.facts))
    return c


def parse_alert(d: json.Object, schedules: Schedules) -> Alert | None:
    trips = list[GtfsTripKey]()
    for i in d["ar"]:
        live_key = LiveTripKey(i["sid"], i["oid"], Date.from_ymd_str(i["od"][:10]))
        if t := schedules.by_live_key.get(live_key):
            trips.extend(t.all)

    if not trips:
        return None

    return Alert(
        id=d["id"],
        title=d["tt"] or "",
        description=d["msg"] or "",
        trips=trips,
    )


def as_translation(x: str, lang: str = "pl") -> gtfs_realtime_pb2.TranslatedString:
    return gtfs_realtime_pb2.TranslatedString(
        translation=[gtfs_realtime_pb2.TranslatedString.Translation(x, lang)],
    )
