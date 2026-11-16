# SPDX-FileCopyrightText: 2026 Marcin Kasznia
# SPDX-License-Identifier: MIT

import logging
import os
import subprocess
from collections.abc import Iterable, Sequence
from itertools import batched
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import cast

import requests
from osmium.replication.server import OsmosisState, ReplicationServer
from osmium.replication.utils import get_replication_header

OSM_API_NODES_URL = "https://api.openstreetmap.org/api/0.6/nodes"
OSM_API_BATCH_SIZE = 500
USER_AGENT = "PolishTrainsGTFS"

logger = logging.getLogger("update_railways")


class UpdateError(Exception):
    pass


def update(path: Path, tags_filter: Sequence[str]) -> None:
    base_url, sequence, _ = get_replication_header(str(path))
    if base_url is None or sequence is None:
        raise UpdateError(f"{path} has no replication header - rebuild it with make")

    with ReplicationServer(base_url) as server:
        latest = server.get_state_info()
        if latest is None:
            raise UpdateError(f"failed to fetch replication state from {base_url}")
        if latest.sequence <= sequence:
            logger.info("%s is up to date (sequence %d)", path, sequence)
            return

        with TemporaryDirectory(dir=path.parent) as temp_dir_str:
            temp_dir = Path(temp_dir_str)
            logger.info("Downloading %s diffs %d-%d", path, sequence + 1, latest.sequence)
            diffs = download_diffs(server, range(sequence + 1, latest.sequence + 1), temp_dir)
            header = replication_header_options(base_url, latest)

            applied = temp_dir / "applied.pbf"
            filtered = temp_dir / "filtered.pbf"
            osmium("apply-changes", "-o", applied, path, *diffs)
            osmium("tags-filter", "-o", filtered, *header, applied, *tags_filter)

            result = add_missing_nodes(filtered, temp_dir, header)
            os.replace(result, path)

    logger.info("Updated %s to sequence %d (%s)", path, latest.sequence, latest.timestamp)


def download_diffs(server: ReplicationServer, sequences: range, temp_dir: Path) -> list[Path]:
    diffs = list[Path]()
    for sequence in sequences:
        diff = temp_dir / f"{sequence}.osc.gz"
        diff.write_bytes(cast(bytes, server.get_diff_block(sequence)))
        diffs.append(diff)
    return diffs


def replication_header_options(base_url: str, state: OsmosisState) -> list[str]:
    return [
        f"--output-header=osmosis_replication_base_url={base_url}",
        f"--output-header=osmosis_replication_sequence_number={state.sequence}",
        "--output-header=osmosis_replication_timestamp="
        + state.timestamp.strftime("%Y-%m-%dT%H:%M:%SZ"),
    ]


def add_missing_nodes(path: Path, temp_dir: Path, header: Sequence[str]) -> Path:
    """Diffs only contain changed objects, so a way which became a railway without changing
    its nodes references nodes absent from the filtered file. Such nodes are fetched from
    the OSM API."""
    missing = find_missing_nodes(path)
    if not missing:
        return path

    logger.info("Fetching %d missing nodes from the OSM API", len(missing))
    fetched = fetch_nodes(missing, temp_dir)
    completed = temp_dir / "completed.pbf"
    osmium("apply-changes", "-o", completed, *header, path, *fetched)

    if still_missing := find_missing_nodes(completed):
        logger.warning("%d nodes are still missing: %s", len(still_missing), still_missing)
    return completed


def find_missing_nodes(path: Path) -> list[int]:
    result = subprocess.run(
        ["osmium", "check-refs", "--show-ids", "--no-progress", path],
        check=False,
        stdout=subprocess.PIPE,
        text=True,
    )
    # check-refs exits with 1 when references are missing
    if result.returncode not in (0, 1):
        raise UpdateError(f"osmium check-refs failed with exit code {result.returncode}")
    return sorted({int(line.split()[0][1:]) for line in result.stdout.splitlines()})


def fetch_nodes(ids: Iterable[int], temp_dir: Path) -> list[Path]:
    files = list[Path]()
    with requests.Session() as session:
        session.headers["User-Agent"] = USER_AGENT
        for i, batch in enumerate(batched(ids, OSM_API_BATCH_SIZE)):
            response = session.get(
                OSM_API_NODES_URL,
                params={"nodes": ",".join(map(str, batch))},
                timeout=60,
            )
            response.raise_for_status()
            file = temp_dir / f"nodes_{i}.osm"
            file.write_bytes(response.content)
            files.append(file)
    return files


def osmium(*args: str | Path) -> None:
    subprocess.run(["osmium", *map(str, args), "--no-progress"], check=True)


if __name__ == "__main__":
    from argparse import ArgumentParser

    arg_parser = ArgumentParser(
        description="Apply replication diffs to filtered OSM extracts, in place",
    )
    arg_parser.add_argument(
        "-f",
        "--filter",
        required=True,
        nargs="+",
        help="osmium tags-filter expressions used to create the extracts",
    )
    arg_parser.add_argument("files", type=Path, nargs="+")
    args = arg_parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        for file in cast(list[Path], args.files):
            update(file, cast(list[str], args.filter))
    except (UpdateError, requests.RequestException, subprocess.CalledProcessError) as e:
        logger.error("%s", e)
        raise SystemExit(1)
