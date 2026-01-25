from typing import Any, Dict, List
import impuls
import json


class LoadPlatformData(impuls.Task):
    def __init__(self) -> None:
        super().__init__()
        self.parents_created: Dict[str, impuls.model.Stop] = {}

    @staticmethod
    def load_platforms(
        path: impuls.tools.types.StrPath,
    ) -> Dict[str, List[Dict[str, Any]]]:
        with open(path, "r", encoding="utf-8") as f:
            platforms = json.load(f)
        return platforms

    def execute(self, r: impuls.TaskRuntime) -> None:
        platforms_in_db = r.db.raw_execute(
            """
        SELECT DISTINCT name, stop_id, platform, json_extract(stop_times.extra_fields_json, '$.track') AS track
        FROM stop_times JOIN stops USING (stop_id)
        """
        ).all()
        self.logger.info(f"Found {len(platforms_in_db)} platforms in DB")
        platforms = self.load_platforms(r.resources["platforms.json"].stored_at)
        for name, stop_id, platform_number, track in platforms_in_db:
            if platform_number == "BUS":
                stop_with_platform_id = f"{stop_id}_BUS"
                parent_stop = self._ensure_parent_station(r, str(stop_id))
                try:
                    r.db.create(
                        impuls.model.Stop(
                            id=stop_with_platform_id,
                            name=str(name),
                            parent_station=parent_stop.id,
                            location_type=impuls.model.Stop.LocationType.STOP,
                            platform_code="BUS",
                            lat=parent_stop.lat,
                            lon=parent_stop.lon,
                        )
                    )
                    r.db.raw_execute(
                        """
                        UPDATE stop_times
                        SET stop_id = ?
                        WHERE stop_id = ? AND platform = ?
                        """,
                        (stop_with_platform_id, stop_id, platform_number),
                    )
                except Exception as e:
                    self.logger.warning(
                        f"Error creating Bus stop {stop_with_platform_id}: {e}, skipping"
                    )
                continue

            if not platform_number or not track:
                continue

            stop_with_platform_id = f"{stop_id}_{platform_number}_{track}"
            platforms_for_station = platforms.get(slug(str(name)), [])
            if len(platforms_for_station) == 0:
                self.logger.warning(f"Station not found {name}")
                continue
            platforms_for_platform_number = [
                p for p in platforms_for_station if p.get("platform") == platform_number
            ]
            if len(platforms_for_platform_number) == 1:
                platform = platforms_for_platform_number[0]
            elif len(platforms_for_platform_number) > 1:
                platforms_for_track = [
                    p for p in platforms_for_platform_number if p.get("track") == track
                ]
                if len(platforms_for_track) == 1:
                    platform = platforms_for_track[0]
                elif len(platforms_for_track) > 1:
                    self.logger.warning(
                        f"Multiple matching platforms found for {name} platform {platform_number} track {track}"
                    )
                    continue
                else:
                    self.logger.warning(
                        f"Platform not found for {name} platform {platform_number} track {track}"
                    )
                    continue
            else:
                self.logger.warning(
                    f"Platform not found for {name} platform {platform_number}"
                )
                continue

            parent_stop = self._ensure_parent_station(r, str(stop_id))

            if platform.get("exact_location") == False:
                self.logger.debug(
                    f"Platform has no exact location {name} {platform_number}/{track}"
                )
            location = platform.get("location")
            if not location:
                self.logger.error(
                    f"Platform {name} track {track} has no location, falling back to parent stop"
                )
                location = [parent_stop.lon, parent_stop.lat]

            r.db.create(
                impuls.model.Stop(
                    id=stop_with_platform_id,
                    name=str(name),
                    parent_station=parent_stop.id,
                    location_type=impuls.model.Stop.LocationType.STOP,
                    platform_code=f"{platform_number}/{track}",
                    lon=location[0],
                    lat=location[1],
                )
            )
            r.db.raw_execute(
                """
                UPDATE stop_times
                SET stop_id = ?
                WHERE stop_id = ? AND platform = ? AND json_extract(extra_fields_json, '$.track') = ?
                """,
                (stop_with_platform_id, stop_id, platform_number, track),
            )

    def _ensure_parent_station(
        self, r: impuls.TaskRuntime, stop_id: str
    ) -> impuls.model.Stop:
        parent_id = f"{stop_id}_parent"
        if parent_id not in self.parents_created.keys():
            no_platform_stop = r.db.retrieve_must(impuls.model.Stop, stop_id)
            parent_stop = impuls.model.Stop(
                id=parent_id,
                name=no_platform_stop.name,
                location_type=impuls.model.Stop.LocationType.STATION,
                lon=no_platform_stop.lon,
                lat=no_platform_stop.lat,
            )
            r.db.create(parent_stop)

            self.parents_created[parent_id] = parent_stop
            no_platform_stop.parent_station = parent_id
            r.db.update(no_platform_stop)

        return self.parents_created[parent_id]


class FixTransferPlatforms(impuls.Task):
    def execute(self, r: impuls.TaskRuntime):
        transfers = r.db.retrieve_all(impuls.model.Transfer)
        with r.db.transaction():
            for transfer in transfers:
                from_trip_stop = r.db.raw_execute(
                    """
                    SELECT stop_id
                    FROM stop_times
                    WHERE trip_id = (?)
                    ORDER BY stop_sequence DESC
                    LIMIT 1
                    """,
                    (transfer.from_trip_id, ),
                ).all()[0][0]
                to_trip_stop = r.db.raw_execute(
                    """
                    SELECT stop_id
                    FROM stop_times
                    WHERE trip_id = (?)
                    ORDER BY stop_sequence ASC
                    LIMIT 1
                    """,
                    (transfer.to_trip_id,),
                ).all()[0][0]
                transfer.to_stop_id = str(to_trip_stop)
                transfer.from_stop_id = str(from_trip_stop)
                r.db.update(transfer)


import re

TRANSLATION_TABLE = str.maketrans(
    {
        "ą": "a",
        "ć": "c",
        "ę": "e",
        "ł": "l",
        "ó": "o",
        "ń": "n",
        "ó": "o",
        "ś": "s",
        "ź": "z",
        "ż": "z",
    }
)


def slug(s: str) -> str:
    text = s.translate(TRANSLATION_TABLE)
    return re.sub(r"[ -]+", "-", re.sub(r"[^\x00-\x7F]+", "", text)).lower()
