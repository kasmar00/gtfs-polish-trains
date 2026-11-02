# SPDX-FileCopyrightText: 2026 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from enum import Enum, auto
from math import cos, nan, radians
from typing import ClassVar

from impuls import DBConnection
from routx import earth_distance


class GraphTag(Enum):
    RAILWAYS_PBF = auto()
    GEO_OSM = auto()


@dataclass(frozen=True)
class MatchedNode:
    node_id: int
    graph: GraphTag
    stop_sequence: int | None = None


@dataclass(frozen=True)
class MatchedTrip:
    trip_id: str
    nodes: tuple[MatchedNode, ...]
    stop_ids: tuple[str, ...]  # ids of all stops of the trip, in stop_sequence order


@dataclass(frozen=True)
class Point:
    lat: float
    lon: float
    distance: float = nan

    def distance_to_km(self, lat: float, lon: float) -> float:
        return earth_distance(self.lat, self.lon, lat, lon)


@dataclass
class GeneratedShape:
    shape_id: str
    points: list[Point] = field(default_factory=list[Point])
    stop_vertices: list[tuple[int, int]] = field(default_factory=list[tuple[int, int]])

    # Keeps distances of consecutive stops strictly increasing after rounding to 6 decimals
    MIN_DISTANCE_STEP_KM: ClassVar[float] = 2e-6

    def insert_into(self, db: DBConnection) -> None:
        db.raw_execute("INSERT INTO shapes (shape_id) VALUES (?)", (self.shape_id,))
        db.raw_execute_many(
            "INSERT INTO shape_points (shape_id, sequence, lat, lon, shape_dist_traveled) "
            "VALUES (?, ?, ?, ?, ?)",
            (
                (self.shape_id, i, round(pt.lat, 6), round(pt.lon, 6), round(pt.distance, 6))
                for i, pt in enumerate(self.points)
            ),
        )

    def project_stops(self, stop_coords: Sequence[tuple[float, float]]) -> dict[int, float]:
        """Calculates shape_dist_traveled of every stop by projecting its location onto
        the closest segment between the matched vertices of the previous and next stop.
        Distances never decrease along the trip, and differently located stops never share
        a distance. `stop_coords` must be aligned with `stop_vertices`.
        """
        distances = dict[int, float]()
        last_segment = len(self.points) - 2
        previous = 0.0
        previous_coords: tuple[float, float] | None = None
        for i, ((stop_sequence, _), coords) in enumerate(
            zip(self.stop_vertices, stop_coords, strict=True)
        ):
            first = self.stop_vertices[i - 1][1] if i > 0 else 0
            if i + 1 < len(self.stop_vertices):
                last = self.stop_vertices[i + 1][1] - 1
            else:
                last = last_segment
            first = min(first, last_segment)
            last = min(max(first, last), last_segment)
            _, distance = min(
                (self._project(segment, *coords) for segment in range(first, last + 1)),
                default=(0.0, self.points[self.stop_vertices[i][1]].distance),
            )
            if distance > previous or previous_coords in (None, coords):
                previous = max(previous, distance)
            else:
                previous += self.MIN_DISTANCE_STEP_KM
            distances[stop_sequence] = previous
            previous_coords = coords
        return distances

    def _project(self, segment: int, lat: float, lon: float) -> tuple[float, float]:
        """Projects a location onto the segment between points `segment` and `segment + 1`.
        Returns the squared (planar approximation) offset and distance along the shape."""
        a, b = self.points[segment], self.points[segment + 1]
        scale = cos(radians(a.lat))
        dx, dy = (b.lon - a.lon) * scale, b.lat - a.lat
        px, py = (lon - a.lon) * scale, lat - a.lat
        length_sq = dx * dx + dy * dy
        t = 0.0 if length_sq == 0 else max(0.0, min(1.0, (px * dx + py * dy) / length_sq))
        offset_sq = (px - t * dx) ** 2 + (py - t * dy) ** 2
        return offset_sq, a.distance + t * (b.distance - a.distance)

    def apply_for_many(
        self,
        db: DBConnection,
        trip_ids: Sequence[str],
        stop_distances: Mapping[int, float],
    ) -> None:
        db.raw_execute_many(
            "UPDATE trips SET shape_id = ? WHERE trip_id = ?",
            ((self.shape_id, trip_id) for trip_id in trip_ids),
        )
        db.raw_execute_many(
            "UPDATE stop_times SET shape_dist_traveled = ? WHERE trip_id = ? AND stop_sequence = ?",
            (
                (round(dist, 6), trip_id, stop_seq)
                for trip_id in trip_ids
                for stop_seq, dist in stop_distances.items()
            ),
        )
