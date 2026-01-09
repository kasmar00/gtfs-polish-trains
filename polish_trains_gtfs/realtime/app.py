# SPDX-FileCopyrightText: 2025-2026 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

import json
from argparse import ArgumentParser
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Self

import requests
from impuls.tools import logs

from ..apikey import get_apikey
from .alerts import fetch_alerts
from .backoff import Backoff
from .delays import fetch_delays
from .schedules import Schedules


@dataclass
class Args:
    type: Literal["alerts", "updates"]
    gtfs: Path
    output: Path
    human_readable: bool
    json: bool
    user_agent_suffix: str
    loop: int
    verbose: bool

    def get_json_path(self) -> Path:
        return self.output.with_suffix(".json")

    @classmethod
    def parse(cls, argv: Sequence[str] | None = None) -> Self:
        arg_parser = ArgumentParser()
        arg_parser.add_argument(
            "-g",
            "--gtfs",
            type=Path,
            default=Path("polish_trains.zip"),
            help="path to the GTFS Schedule file (defaults to polish_trains.zip)",
        )
        arg_parser.add_argument(
            "-o",
            "--output",
            type=Path,
            default=Path("polish_trains.pb"),
            help="path to output GTFS-Realtime feed",
        )
        arg_parser.add_argument(
            "-r",
            "--human-readable",
            action="store_true",
            help="use human-readable protobuf format (instead of default binary)",
        )
        arg_parser.add_argument(
            "-j",
            "--json",
            action="store_true",
            help="also write realtime data in json format",
        )
        arg_parser.add_argument(
            "-u",
            "--user-agent",
            default="",
            help="additional string to include in User-Agent sent to the API",
        )
        arg_parser.add_argument(
            "-l",
            "--loop",
            type=int,
            default=0,
            help=(
                "run the converter indefinitely every N seconds "
                "(<= 0 for single run, defaults to zero)"
            ),
        )
        arg_parser.add_argument(
            "-v",
            "--verbose",
            action="store_true",
            help="show debug logging",
        )
        arg_parser.add_argument("type", choices=["alerts", "updates"])
        args = arg_parser.parse_args(argv)
        return cls(
            type=args.type,
            gtfs=args.gtfs,
            output=args.output,
            human_readable=args.human_readable,
            json=args.json,
            user_agent_suffix=args.user_agent,
            loop=args.loop,
            verbose=args.verbose,
        )


def main(argv: Sequence[str] | None = None) -> None:
    args = Args.parse(argv)
    logs.initialize(verbose=args.verbose)

    session = get_session(get_apikey(), args.user_agent_suffix)
    schedules = Schedules.load_from_gtfs(args.gtfs)

    run = lambda: one_shot(session, schedules, args)  # noqa: E731

    if args.loop <= 0:
        run()
    else:
        Backoff(args.loop).loop(run)


def get_session(key: str, user_agent_suffix: str = "") -> requests.Session:
    s = requests.Session()
    s.headers["X-Api-Key"] = key
    if user_agent_suffix:
        s.headers["User-Agent"] = f"{requests.utils.default_user_agent()} ({user_agent_suffix})"
    return s


def one_shot(session: requests.Session, schedules: Schedules, args: Args) -> None:
    if args.type == "alerts":
        facts = fetch_alerts(session, schedules)
    elif args.type == "updates":
        facts = fetch_delays(session, schedules)
    else:
        raise RuntimeError(f"invalid feed type: {args.type!r}")

    update_file(
        str(facts.as_gtfs_rt()) if args.human_readable else facts.as_gtfs_rt().SerializeToString(),
        args.output,
    )
    if args.json:
        update_file(
            json.dumps(
                facts.as_json(),
                indent=2 if args.human_readable else None,
                separators=(",", ": ") if args.human_readable else (",", ":"),
            ),
            args.get_json_path(),
        )


def update_file(s: bytes | str, dst: Path):
    tmp = dst.with_name(f".{dst.name}.tmp")
    if isinstance(s, bytes):
        tmp.write_bytes(s)
    else:
        tmp.write_text(s, encoding="utf-8")
    tmp.rename(dst)
