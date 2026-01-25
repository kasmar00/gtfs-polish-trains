# SPDX-FileCopyrightText: 2026 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

import re
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping
from typing import NamedTuple, TypedDict, cast

from impuls import DBConnection, Task, TaskRuntime
from impuls.errors import DataError, MultipleDataErrors
from impuls.model import Stop

from ..geo import Platform, Station, load_stations
from .util import json


class TripStop(NamedTuple):
    trip_id: str
    stop_sequence: int


class PlatformTrack(NamedTuple):
    platform: str
    track: str


class StationToUpdate(NamedTuple):
    station_id: str
    name: str
    trips: defaultdict[PlatformTrack, list[TripStop]]


class ExternalPlatform(TypedDict):
    platform: str
    track: str
    location: list[float] | None  # [lon, lat]
    exact_location: bool


FAKE_PLATFORMS = {"", "BUS", "WAYPOINT"}

PLATFORM_MATCH_OVERRIDE: Mapping[tuple[str, str, str], tuple[str, str]] = {
    ("33571", "1", ""): ("1/2", ""),  # W-wa Śródmieście - match platform "1" with "1/2"
    ("33571", "3", ""): ("2/3", ""),  # W-wa Śródmieście - match platform "3" with "2/3"
}


TRANSLATION_TABLE = str.maketrans(
    {
        "ą": "a",
        "ć": "c",
        "ę": "e",
        "ł": "l",
        "ó": "o",
        "ń": "n",
        "ś": "s",
        "ź": "z",
        "ż": "z",
    }
)


def slug(s: str) -> str:
    text = s.translate(TRANSLATION_TABLE).replace("(", "").replace(")", "")
    return re.sub(r"[ \-\(\)]+", "-", re.sub(r"[^\x00-\x7F]+", "", text)).lower()


class LoadStations(Task):
    to_update: dict[str, StationToUpdate]
    external_platforms: dict[str, list[ExternalPlatform]]
    location_sources: Counter[str]

    def __init__(self) -> None:
        super().__init__()
        self.to_update = {}
        self.external_platforms = {}
        self.location_sources = Counter()

    def execute(self, r: TaskRuntime) -> None:
        self.to_update = self.get_stations_to_update(r.db)
        self.external_platforms = r.resources["platforms.json"].json(encoding="utf-8")
        stations = load_stations(r.resources["geo.osm"].stored_at)
        with r.db.transaction():
            for station in stations.values():
                self.apply(r.db, station)
        self.logger.info("Platform locations by source: %s", dict(self.location_sources))
        self.ensure_everything_updated()

    def apply(self, db: DBConnection, station: Station) -> None:
        # Don't apply the station if it's not used
        if not any(id in self.to_update for id in station.all_ids()):
            return

        if station.waypoint:
            self.upsert_waypoint(db, station)
        else:
            self.upsert_station(db, station)
            self.create_fallback_platform(db, station)
            self.create_platforms(db, station)
            self.create_exits(db, station)
            self.remove_secondary_stops(db, station)

        self.mark_as_updated(station)

    def upsert_station(self, db: DBConnection, station: Station) -> None:
        stop = Stop(
            id=station.id,
            name=station.name,
            lat=station.lat,
            lon=station.lon,
            wheelchair_boarding=station.accessible,
            location_type=Stop.LocationType.STATION,
            extra_fields_json=json.dumps(
                {
                    "country": station.country,
                    "plk_secondary_id": "/".join(station.other_ids),
                }
            ),
        )

        if station.request_stop:
            db.raw_execute(
                "UPDATE stop_times SET drop_off_type = 3 WHERE stop_id = ? AND drop_off_type = 0",
                (station.id,),
            )
            db.raw_execute(
                "UPDATE stop_times SET pickup_type = 3 WHERE stop_id = ? AND pickup_type = 0",
                (station.id,),
            )

        if station.id in self.to_update:
            db.update(stop)  # departures are moved to a stop later on
        else:
            db.create(stop)

    def upsert_waypoint(self, db: DBConnection, waypoint: Station) -> None:
        stop = Stop(
            id=waypoint.id,
            name=waypoint.name,
            lat=waypoint.lat,
            lon=waypoint.lon,
            wheelchair_boarding=waypoint.accessible,
            location_type=Stop.LocationType.STOP,
            extra_fields_json=json.dumps(
                {
                    "country": waypoint.country,
                    "waypoint": "1",
                }
            ),
        )

        if waypoint.id in self.to_update:
            db.update(stop)
        else:
            raise NotImplementedError("TODO: waypoints changing ids is not supported")

    def create_fallback_platform(self, db: DBConnection, station: Station) -> None:
        db.create(
            Stop(
                id=f"{station.id}_FALLBACK",
                name=station.name,
                lat=station.lat,
                lon=station.lon,
                wheelchair_boarding=station.accessible,
                location_type=Stop.LocationType.STOP,
                parent_station=station.id,
                extra_fields_json=json.dumps({"country": station.country, "stop_access": "0"}),
            )
        )

    def create_platforms(self, db: DBConnection, station: Station) -> None:
        to_move = self.get_all_trips_to_move(station.all_ids())
        station_slug = slug(station.name)
        if station_slug not in self.external_platforms and any(
            platform not in FAKE_PLATFORMS for platform, _ in to_move
        ):
            self.logger.warning(
                "%s %s: station slug %r not found in platforms.json",
                station.id,
                station.name,
                station_slug,
            )
        external_candidates = self.external_platforms.get(station_slug, [])
        for (platform, track), trips in to_move.items():
            if platform in FAKE_PLATFORMS:
                self.move_departures(db, f"{station.id}_FALLBACK", trips)
            else:
                lat, lon, accessible = self.locate_platform(
                    station, external_candidates, platform, track
                )
                db_platform = Stop(
                    id=f"{station.id}_RAIL_{platform}_{track}",
                    name=station.name,
                    lat=lat,
                    lon=lon,
                    wheelchair_boarding=accessible,
                    location_type=Stop.LocationType.STOP,
                    parent_station=station.id,
                    platform_code=f"{platform}/{track}" if track else platform,
                    extra_fields_json=json.dumps({"country": station.country, "stop_access": "0"}),
                )
                db.create(db_platform)
                self.move_departures(db, db_platform.id, trips)

    def locate_platform(
        self,
        station: Station,
        external_candidates: list[ExternalPlatform],
        platform: str,
        track: str,
    ) -> tuple[float, float, bool | None]:
        geo_platform = self.find_matching_platform(station, platform, track)

        if geo_platform and geo_platform.track:
            self.location_sources["geo+track"] += 1
            return geo_platform.lat, geo_platform.lon, geo_platform.accessible

        external_platform = (
            self.find_external_platform(external_candidates, platform, track) if track else None
        )
        if external_platform:
            if external_platform["exact_location"] and external_platform["location"]:
                self.location_sources["platforms.json"] += 1
                location = external_platform["location"]
                return location[1], location[0], station.accessible
            self.logger.debug(
                "%s %s: skipping platforms.json location for platform %r track %r "
                "(exact_location=%s)",
                station.id,
                station.name,
                platform,
                track,
                external_platform["exact_location"],
            )

        if geo_platform:
            self.location_sources["geo platform-only"] += 1
            return geo_platform.lat, geo_platform.lon, geo_platform.accessible

        self.logger.debug(
            "%s %s: missing data for platform %r track %r",
            station.id,
            station.name,
            platform,
            track,
        )
        self.location_sources["station"] += 1
        return station.lat, station.lon, station.accessible

    def find_external_platform(
        self,
        candidates: list[ExternalPlatform],
        platform: str,
        track: str,
    ) -> ExternalPlatform | None:
        hits = [p for p in candidates if p["platform"] == platform]
        if len(hits) > 1:
            hits = [p for p in hits if p["track"] == track]
        return hits[0] if len(hits) == 1 else None

    def find_matching_platform(
        self,
        station: Station,
        platform: str,
        track: str,
    ) -> Platform | None:
        # Override matching criteria
        if track_override := PLATFORM_MATCH_OVERRIDE.get((station.id, platform, track)):
            platform, track = track_override
        elif platform_override := PLATFORM_MATCH_OVERRIDE.get((station.id, platform, "")):
            platform, _ = platform_override

        best: Platform | None = None

        for candidate in station.platforms:
            matches_platform = platform == candidate.name
            matches_track = track == candidate.track or candidate.track == ""

            if matches_platform and matches_track and (best is None or best.track == ""):
                best = candidate

        return best

    def move_departures(self, db: DBConnection, new_id: str, trips: Iterable[TripStop]) -> None:
        db.raw_execute_many(
            "UPDATE stop_times SET stop_id = ? WHERE trip_id = ? AND stop_sequence = ?",
            ((new_id, trip_id, stop_sequence) for trip_id, stop_sequence in trips),
        )

    def create_exits(self, db: DBConnection, station: Station) -> None:
        db.create_many(
            Stop,
            (
                Stop(
                    id=f"{station.id}_EXIT_{exit.node_id}",
                    name=exit.name,
                    lat=exit.lat,
                    lon=exit.lon,
                    wheelchair_boarding=exit.accessible,
                    location_type=Stop.LocationType.EXIT,
                    parent_station=station.id,
                    extra_fields_json=json.dumps(
                        {
                            "country": station.country,
                            "only_platforms": "/".join(exit.platforms),
                        }
                    ),
                )
                for exit in station.exits
            ),
        )

    def remove_secondary_stops(self, db: DBConnection, station: Station) -> None:
        db.raw_execute_many(
            "DELETE FROM stops WHERE stop_id = ?",
            ((id,) for id in station.other_ids),
        )

    def mark_as_updated(self, station: Station) -> None:
        for id in station.all_ids():
            self.to_update.pop(id, None)

    def get_stations_to_update(self, db: DBConnection) -> dict[str, StationToUpdate]:
        to_update = dict[str, StationToUpdate]()
        # FIXME: use arrival_platform and arrival_track when (departure) platform is fake
        with db.raw_execute(
            "SELECT stop_id, name, trip_id, stop_sequence, platform, "
            " json_extract(t.extra_fields_json, '$.track'), "
            " json_extract(t.extra_fields_json, '$.arrival_platform'), "
            " json_extract(t.extra_fields_json, '$.arrival_track') "
            "FROM stop_times t JOIN stops s USING (stop_id)"
        ) as q:
            for row in q:
                stop_id = cast(str, row[0])
                stop_name = cast(str, row[1])
                trip_id = cast(str, row[2])
                stop_sequence = cast(int, row[3])

                # Resolve the platform
                dep_platform = cast(str, row[4])
                arr_platform = cast(str, row[6]) or ""
                platform = arr_platform if dep_platform in FAKE_PLATFORMS else dep_platform

                # Resolve the track, but only for non-fake platforms
                if platform not in FAKE_PLATFORMS:
                    dep_track = (cast(str | None, row[5]) or "").upper()
                    arr_track = (cast(str | None, row[7]) or "").upper()
                    if dep_platform in FAKE_PLATFORMS:
                        track = arr_track or dep_track
                    else:
                        track = dep_track or arr_track
                else:
                    track = ""

                key = PlatformTrack(platform, track)
                if (station := to_update.get(stop_id)) is None:
                    station = StationToUpdate(stop_id, stop_name, defaultdict(list))
                    to_update[stop_id] = station

                station.trips[key].append(TripStop(trip_id, stop_sequence))

        return to_update

    def get_all_trips_to_move(
        self,
        ids: Iterable[str],
    ) -> defaultdict[PlatformTrack, list[TripStop]]:
        trips = defaultdict[PlatformTrack, list[TripStop]](list)
        for id in ids:
            if s := self.to_update.get(id):
                for key, stops in s.trips.items():
                    trips[key].extend(stops)
        return trips

    def ensure_everything_updated(self) -> None:
        if self.to_update:
            raise MultipleDataErrors(
                "LoadStations",
                [
                    DataError(f"Missing data for {i.station_id} {i.name!r}")
                    for i in self.to_update.values()
                ],
            )
