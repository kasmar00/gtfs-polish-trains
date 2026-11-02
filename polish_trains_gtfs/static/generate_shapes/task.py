# SPDX-FileCopyrightText: 2026 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

from collections import defaultdict
from collections.abc import Sequence
from typing import cast

import routx
from impuls import DBConnection, Task, TaskRuntime
from impuls.model import Route, Stop, StopTime, Trip
from impuls.tools.types import StrPath

from .generator import ShapeGenerator
from .matcher import BusMatcher, Matcher, TrainMatcher
from .model import GraphTag, MatchedNode, MatchedTrip


class GenerateShapes(Task):
    def __init__(
        self,
        graph_resource: str,
        extra_config_resource: str | None = None,
        pbf_resource: str | None = None,
    ) -> None:
        super().__init__()
        self.graph_resource = graph_resource
        self.extra_config_resource = extra_config_resource
        self.pbf_resource = pbf_resource

    def execute(self, r: TaskRuntime) -> None:
        # 1. Load resources
        geo_path = r.resources[self.graph_resource].stored_at
        extra_config = (  # type: ignore
            r.resources[self.extra_config_resource].yaml() if self.extra_config_resource else {}
        )

        # 2. Load the graphs
        graphs = dict[GraphTag, routx.Graph]()
        self.logger.info("Loading routing graph from %s", self.graph_resource)
        graphs[GraphTag.GEO_OSM] = self.load_graph(geo_path)
        if self.pbf_resource:
            self.logger.info("Loading routing graph from %s", self.pbf_resource)
            graphs[GraphTag.RAILWAYS_PBF] = self.load_graph(
                r.resources[self.pbf_resource].stored_at
            )

        # 3. Load the trip_matcher
        self.logger.info("Loading trip matching lookup table")
        with r.db.retrieve_all(Stop) as q:
            stops = list(q)
        stop_coords = {s.id: (s.lat, s.lon) for s in stops}
        matcher = self.create_matcher()
        matcher.load(graphs, stops, geo_path, extra_config)

        # 4. Select trips to generate shapes for
        self.logger.info("Selecting trips")
        unmatched_trips = self.select_trips(r.db)

        # 5. Match each trips' stops with nodes
        self.logger.debug("Matching %d trips with nodes", len(unmatched_trips))
        matched_trips = self.match_trips(r.db, matcher, unmatched_trips)
        self.logger.debug(
            "Matched %d / %d (%.2f %%) trips",
            len(matched_trips),
            len(unmatched_trips),
            100 * len(matched_trips) / max(len(unmatched_trips), 1),
        )
        matched_ids = {t.trip_id for t in matched_trips}
        skipped_unmatched = [t.id for t in unmatched_trips if t.id not in matched_ids]

        # 6. Group trips by the same sequence of nodes
        self.logger.debug("Grouping trips with the same shape")
        grouped_trips = defaultdict[tuple[MatchedNode, ...], list[MatchedTrip]](list)
        for matched_trip in matched_trips:
            grouped_trips[matched_trip.nodes].append(matched_trip)

        # 7. Generate shapes for every unique sequence of nodes
        self.logger.info("Generating %d shapes", len(grouped_trips))
        generator = ShapeGenerator(graphs, self.get_shape_id_prefix())
        with r.db.transaction():
            for shape_id, (nodes, trips) in enumerate(grouped_trips.items()):
                if (shape_id + 1) % 50 == 0:
                    self.logger.debug(
                        "Generated %d / %d (%.2f %%) shapes",
                        shape_id,
                        len(grouped_trips),
                        100 * shape_id / len(grouped_trips),
                    )
                shape = generator.generate(nodes)
                shape.insert_into(r.db)

                # One shape can serve trips calling at different stops that snapped to the same
                # nodes (e.g. different tracks without a platforms.json point), so stop distances
                # are projected separately for every distinct sequence of stop ids on this shape.
                trips_by_stops = defaultdict[tuple[str, ...], list[str]](list)
                for trip in trips:
                    trips_by_stops[trip.stop_ids].append(trip.trip_id)
                for stop_ids, trip_ids in trips_by_stops.items():
                    stop_distances = shape.project_stops([stop_coords[i] for i in stop_ids])
                    shape.apply_for_many(r.db, trip_ids, stop_distances)

        if generator.connector_count:
            self.logger.info(
                "Inserted %d graph switch connectors, longest %.3f km",
                generator.connector_count,
                generator.max_connector_km,
            )
        self.log_skipped(len(unmatched_trips), skipped_unmatched)

    def log_skipped(self, total: int, skipped_unmatched: list[str]) -> None:
        if not skipped_unmatched:
            return
        self.logger.warning(
            "Skipped shapes for %d / %d (%.2f %%) trips "
            "with a stop too far from or missing in the required graph",
            len(skipped_unmatched),
            total,
            100 * len(skipped_unmatched) / total,
        )
        self.logger.warning("Trips with unmatched stops: %s", skipped_unmatched)

    def load_graph(self, osm_path: StrPath) -> routx.Graph:
        g = routx.Graph()
        g.add_from_osm_file(osm_path, self.get_routx_profile())
        return g

    def get_routx_profile(self) -> routx.OsmProfile | routx.OsmCustomProfile:
        # Like routx.OsmProfile.RAILWAY, but also allows (discouraged) routing over
        # lines under construction, as OSM lags behind reopened lines. Service spurs are
        # heavily discouraged; first match wins, so they must precede railway=rail.
        return routx.OsmCustomProfile(
            name="train",
            penalties=[
                routx.OsmPenalty("service", "spur", 10.0),
                routx.OsmPenalty("service", "siding", 2.0),
                routx.OsmPenalty("railway", "rail", 1.0),
                routx.OsmPenalty("railway", "light_rail", 1.0),
                routx.OsmPenalty("railway", "subway", 1.0),
                routx.OsmPenalty("railway", "narrow_gauge", 1.0),
                routx.OsmPenalty("railway", "construction", 2.0),
            ],
            access=["access", "train"],
        )

    def create_matcher(self) -> Matcher:
        return TrainMatcher()

    def select_trips(self, db: DBConnection) -> list[Trip]:
        return _select_trips_with_route_type(db, Route.Type.RAIL)

    def match_trips(
        self,
        db: DBConnection,
        matcher: Matcher,
        trips: Sequence[Trip],
    ) -> list[MatchedTrip]:
        return [m for trip in trips if (m := matcher.match(trip, _get_stop_times(db, trip.id)))]

    def get_shape_id_prefix(self) -> str:
        return "RAIL_"


class GenerateBusShapes(GenerateShapes):
    def get_routx_profile(self) -> routx.OsmProfile:
        return routx.OsmProfile.BUS

    def select_trips(self, db: DBConnection) -> list[Trip]:
        return _select_trips_with_route_type(db, Route.Type.BUS)

    def create_matcher(self) -> Matcher:
        return BusMatcher()

    def get_shape_id_prefix(self) -> str:
        return "BUS_"


def _select_trips_with_route_type(db: DBConnection, typ: Route.Type) -> list[Trip]:
    with db.raw_execute("SELECT route_id FROM routes WHERE type = ?", (typ.value,)) as q:
        route_ids = [cast(str, i[0]) for i in q]

    trips = list[Trip]()
    for id in route_ids:
        with db.typed_out_execute("SELECT * FROM trips WHERE route_id = ?", Trip, (id,)) as q:
            trips.extend(q)
    return trips


def _get_stop_times(db: DBConnection, trip_id: str) -> list[StopTime]:
    with db.typed_out_execute(
        "SELECT * FROM stop_times WHERE trip_id = ? ORDER BY stop_sequence ASC",
        StopTime,
        (trip_id,),
    ) as query:
        return list(query)
