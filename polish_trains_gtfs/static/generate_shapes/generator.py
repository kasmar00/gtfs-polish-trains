# SPDX-FileCopyrightText: 2026 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

import logging
from collections.abc import Iterable, Mapping, Sequence
from itertools import pairwise

import routx

from .model import GeneratedShape, GraphTag, MatchedNode, Point

logger = logging.getLogger("ShapeGenerator")


class ShapeGenerator:
    def __init__(
        self,
        graphs: Mapping[GraphTag, routx.Graph],
        shape_id_prefix: str = "",
    ) -> None:
        self.graphs = graphs
        self.shape_id_counter = 0
        self.shape_id_prefix = shape_id_prefix
        self.connector_count = 0
        self.max_connector_km = 0.0

    def _get_next_shape_id(self) -> str:
        id = f"{self.shape_id_prefix}{self.shape_id_counter}"
        self.shape_id_counter += 1
        return id

    def generate(self, nodes: Iterable[MatchedNode]) -> GeneratedShape:
        shape = GeneratedShape(self._get_next_shape_id())
        total_distance = 0.0

        for i, (from_, to) in enumerate(pairwise(nodes)):
            # Record the vertex of the first stop
            if i == 0:
                assert from_.stop_sequence is not None
                shape.stop_vertices.append((from_.stop_sequence, 0))

            if from_.graph is to.graph:
                leg_nodes = self.generate_leg(from_.graph, from_.node_id, to.node_id)

                # Skip first node, as it's the same as previous leg's last node,
                # except for the very first leg
                offset = 0 if i == 0 else 1
            else:
                # Graph switch: connect the nodes with a straight line, keeping only the
                # second node. The matcher never switches graphs on the first pair, so the
                # first node was already added by the previous leg.
                leg_nodes = [to.node_id]
                offset = 0
                node = self.graphs[to.graph][to.node_id]
                connector_km = shape.points[-1].distance_to_km(node.lat, node.lon)
                self.connector_count += 1
                self.max_connector_km = max(self.max_connector_km, connector_km)
                logger.debug(
                    "Graph switch connector of %.3f km between nodes %d and %d",
                    connector_km,
                    from_.node_id,
                    to.node_id,
                )

            # Save the points of the shape
            graph = self.graphs[to.graph]
            for node_id in leg_nodes[offset:]:
                node = graph[node_id]
                if shape.points:
                    last = shape.points[-1]
                    if (round(last.lat, 6), round(last.lon, 6)) == (
                        round(node.lat, 6),
                        round(node.lon, 6),
                    ):
                        continue
                    total_distance += last.distance_to_km(node.lat, node.lon)
                shape.points.append(Point(node.lat, node.lon, total_distance))

            # Record the vertex of the stop
            if to.stop_sequence is not None:
                shape.stop_vertices.append((to.stop_sequence, len(shape.points) - 1))

        return shape

    def generate_leg(self, graph: GraphTag, from_: int, to: int) -> Sequence[int]:
        nodes: Sequence[int] = []
        try:
            nodes = self.graphs[graph].find_route(
                from_,
                to,
                without_turn_around=(graph is GraphTag.RAILWAYS_PBF),
            )
        except routx.StepLimitExceeded:
            pass

        if not nodes:
            logger.error("No shape between nodes %d and %d in %s", from_, to, graph.name)
            return [from_, to]

        return nodes
