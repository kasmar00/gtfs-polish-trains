# SPDX-FileCopyrightText: 2026 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

from collections.abc import Iterable, Mapping, Sequence
from itertools import chain, product
from typing import Any, Protocol, cast

import routx
from impuls.model import Stop, StopTime, Trip
from impuls.tools.types import StrPath

from ...geo import Station, load_stations
from .model import GraphTag, MatchedNode, MatchedTrip


class Matcher(Protocol):
    def load(
        self,
        graphs: Mapping[GraphTag, routx.Graph],
        stops: Iterable[Stop],
        osm_path: StrPath,
        extra_config: Any,
    ) -> None: ...
    def match(self, trip: Trip, stop_times: Sequence[StopTime]) -> MatchedTrip | None: ...


class TrainMatcher(Matcher):
    MAX_DISTANCE_KM = 0.5
    geo_graph: routx.Graph

    def __init__(self) -> None:
        self.force_via = dict[GraphTag, dict[tuple[str, str], int]]()
        self.stop_to_node = dict[GraphTag, dict[str, int]]()
        self.stop_country = dict[str, str]()
        self.foreign_stations = dict[str, Station]()

    def load(
        self,
        graphs: Mapping[GraphTag, routx.Graph],
        stops: Iterable[Stop],
        osm_path: StrPath,
        extra_config: Any,
    ) -> None:
        self.force_via.clear()
        self.stop_to_node.clear()
        self.stop_country.clear()
        self.foreign_stations.clear()
        self.geo_graph = graphs[GraphTag.GEO_OSM]

        if GraphTag.RAILWAYS_PBF not in graphs:
            raise ValueError("TrainMatcher requires the railways.osm.pbf graph")

        kd_trees = {tag: routx.KDTree.build(graph) for tag, graph in graphs.items()}
        for tag, kd_tree in kd_trees.items():
            self.force_via[tag] = self.load_force_via(kd_tree, extra_config)
            self.stop_to_node[tag] = {}

        stations = load_stations(osm_path)
        for stop in stops:
            if "BUS" in stop.id:
                continue

            country = stop.get_extra_field("country") or "PL"
            self.stop_country[stop.id] = country

            if country == "PL":
                rail_lat = stop.get_extra_field("rail_lat")
                rail_lon = stop.get_extra_field("rail_lon")
                if rail_lat and rail_lon:
                    pbf_position = float(rail_lat), float(rail_lon)
                else:
                    pbf_position = stop.lat, stop.lon
                self._snap(GraphTag.RAILWAYS_PBF, kd_trees, stop.id, *pbf_position)
                self._snap(GraphTag.GEO_OSM, kd_trees, stop.id, stop.lat, stop.lon)
            elif station := stations.get(_extract_station_id(stop.id)):
                self.foreign_stations[stop.id] = station

    def _snap(
        self,
        tag: GraphTag,
        kd_trees: Mapping[GraphTag, routx.KDTree],
        stop_id: str,
        lat: float,
        lon: float,
    ) -> None:
        candidate = kd_trees[tag].find_nearest_node(lat, lon)
        distance = routx.earth_distance(lat, lon, candidate.lat, candidate.lon)
        if distance <= self.MAX_DISTANCE_KM:
            self.stop_to_node[tag][stop_id] = candidate.id

    @staticmethod
    def load_force_via(kd_tree: routx.KDTree, extra_config: Any) -> dict[tuple[str, str], int]:
        force_via = dict[tuple[str, str], int]()
        for cfg in extra_config.get("force_via", []):
            node_id = kd_tree.find_nearest_node(*cfg["via"]).id
            from_ids = cast(list[str], _ensure_list(cfg["from"]))
            to_ids = cast(list[str], _ensure_list(cfg["to"]))
            pairs = (
                product(from_ids, to_ids)
                if cfg.get("oneway", False)
                else chain(product(from_ids, to_ids), product(to_ids, from_ids))
            )

            for from_, to in pairs:
                force_via[from_, to] = node_id
        return force_via

    def match(self, trip: Trip, stop_times: Sequence[StopTime]) -> MatchedTrip | None:
        if len(stop_times) < 2:
            return None

        nodes = list[MatchedNode]()
        previous_graph: GraphTag | None = None
        for i in range(1, len(stop_times)):
            prev, cur = stop_times[i - 1], stop_times[i]
            graph = self.leg_graph(prev.stop_id, cur.stop_id)
            prev_node = self.resolve_node(graph, stop_times, i - 1)
            cur_node = self.resolve_node(graph, stop_times, i)
            if prev_node is None or cur_node is None:
                return None  # Don't generate a shape without a matched node in the graph

            if previous_graph is None:
                nodes.append(MatchedNode(prev_node, graph, prev.stop_sequence))
            elif graph != previous_graph:
                nodes.append(MatchedNode(prev_node, graph))

            via_pair = (_extract_station_id(prev.stop_id), _extract_station_id(cur.stop_id))
            if (via_node := self.force_via[graph].get(via_pair)) is not None:
                nodes.append(MatchedNode(via_node, graph))

            nodes.append(MatchedNode(cur_node, graph, cur.stop_sequence))
            previous_graph = graph

        stop_ids = tuple(st.stop_id for st in stop_times)
        reference_nodes = tuple(
            self.resolve_node(GraphTag.GEO_OSM, stop_times, i) for i in range(len(stop_times))
        )
        return MatchedTrip(trip.id, tuple(nodes), stop_ids, reference_nodes)

    def resolve_node(
        self,
        graph: GraphTag,
        stop_times: Sequence[StopTime],
        index: int,
    ) -> int | None:
        stop_time = stop_times[index]
        station = self.foreign_stations.get(stop_time.stop_id)
        if station is None:
            return self.stop_to_node[graph].get(stop_time.stop_id)
        return self.resolve_foreign_node(station, stop_times, index)

    def resolve_foreign_node(
        self,
        station: Station,
        stop_times: Sequence[StopTime],
        index: int,
    ) -> int | None:
        candidates = [p for p in station.stop_positions if p.node_id in self.geo_graph]
        if len(candidates) == 1:
            return candidates[0].node_id

        platform = stop_times[index].platform
        if platform:
            for candidate in candidates:
                if platform in candidate.platforms:
                    return candidate.node_id

        neighbours = {
            _extract_station_id(stop_times[i].stop_id)
            for i in (index - 1, index + 1)
            if 0 <= i < len(stop_times)
        }
        for candidate in candidates:
            if neighbours.intersection(candidate.towards):
                return candidate.node_id

        for candidate in candidates:
            if not candidate.towards or "fallback" in candidate.towards:
                return candidate.node_id

        return station.node_id if station.node_id in self.geo_graph else None

    def leg_graph(self, from_stop_id: str, to_stop_id: str) -> GraphTag:
        if self.stop_country.get(from_stop_id, "PL") != "PL":
            return GraphTag.GEO_OSM
        if self.stop_country.get(to_stop_id, "PL") != "PL":
            return GraphTag.GEO_OSM
        return GraphTag.RAILWAYS_PBF


class BusMatcher(Matcher):
    MAX_DISTANCE_KM = 0.05

    def __init__(self) -> None:
        self.stop_to_node = dict[str, int]()

    def load(
        self,
        graphs: Mapping[GraphTag, routx.Graph],
        stops: Iterable[Stop],
        osm_path: StrPath,
        extra_config: Any,
    ) -> None:
        kd_tree = routx.KDTree.build(graphs[GraphTag.GEO_OSM])
        self.stop_to_node.clear()
        for stop in stops:
            if "BUS" in stop.id:
                candidate = kd_tree.find_nearest_node(stop.lat, stop.lon)
                distance = routx.earth_distance(stop.lat, stop.lon, candidate.lat, candidate.lon)
                if distance <= self.MAX_DISTANCE_KM:
                    self.stop_to_node[stop.id] = candidate.id

    def match(self, trip: Trip, stop_times: Sequence[StopTime]) -> MatchedTrip | None:
        matched_nodes = list[MatchedNode]()
        for stop_time in stop_times:
            if node_id := self.stop_to_node.get(stop_time.stop_id):
                matched_nodes.append(
                    MatchedNode(node_id, GraphTag.GEO_OSM, stop_time.stop_sequence)
                )
            else:
                return None  # Don't generate a shape without a matched node in the graph
        return MatchedTrip(
            trip.id,
            tuple(matched_nodes),
            tuple(st.stop_id for st in stop_times),
            tuple(n.node_id for n in matched_nodes),
        )


def _ensure_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else [value]  # type: ignore


def _extract_station_id(x: str) -> str:
    return x.partition("_")[0]
