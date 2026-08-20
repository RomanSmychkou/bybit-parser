#!/usr/bin/env python3
"""Download all public trade files from public.bybit.com."""

from __future__ import annotations

import argparse
import asyncio
import os
import re
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import unquote, urljoin, urlparse
from urllib.request import Request, urlopen

from download_progress import DownloadProgress


BASE_URL = "https://public.bybit.com/spot/{symbol}/"
DATE_RE = re.compile(r"(?<!\d)(20\d{2}-\d{2}-\d{2})(?!\d)")
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:128.0) "
    "Gecko/20100101 Firefox/128.0"
)


class LinkParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.links: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() != "a":
            return
        href = dict(attrs).get("href")
        if href:
            self.links.append(href)


def read_url(url: str, timeout: float) -> bytes:
    request = Request(url, headers={"User-Agent": USER_AGENT})
    with urlopen(request, timeout=timeout) as response:
        return response.read()


def save_url(url: str, destination: Path, timeout: float) -> int:
    request = Request(url, headers={"User-Agent": USER_AGENT})
    temporary = destination.with_name(destination.name + ".part")
    try:
        with urlopen(request, timeout=timeout) as response, temporary.open("wb") as output:
            while chunk := response.read(1024 * 1024):
                output.write(chunk)
        os.replace(temporary, destination)
        return destination.stat().st_size
    finally:
        if temporary.exists():
            temporary.unlink()


def extract_files(symbol: str, timeout: float) -> list[tuple[str, str]]:
    page_url = BASE_URL.format(symbol=symbol)
    page = read_url(page_url, timeout).decode("utf-8", errors="replace")
    parser = LinkParser()
    parser.feed(page)

    result: list[tuple[str, str]] = []
    seen: set[str] = set()
    for href in parser.links:
        file_url = urljoin(page_url, href)
        parsed = urlparse(file_url)
        filename = unquote(Path(parsed.path).name)
        if parsed.scheme not in {"http", "https"} or not filename:
            continue
        if file_url in seen:
            continue
        seen.add(file_url)
        result.append((filename, file_url))
    return result


def filter_files(
    files: list[tuple[str, str]],
    start: date | None,
    end: date | None,
) -> list[tuple[str, str]]:
    if start is None and end is None:
        return files

    filtered: list[tuple[str, str]] = []
    for filename, url in files:
        match = DATE_RE.search(filename)
        if not match:
            continue
        file_date = date.fromisoformat(match.group(1))
        if start is not None and file_date < start:
            continue
        if end is not None and file_date > end:
            continue
        filtered.append((filename, url))
    return filtered


async def download_files(
    executor: ThreadPoolExecutor,
    jobs: list[tuple[str, str, Path]],
    timeout: float,
) -> None:
    progress = DownloadProgress(len(jobs))
    progress.start()

    async def download(filename: str, url: str, destination: Path) -> None:
        if destination.exists():
            progress.task_skipped()
            return
        destination.parent.mkdir(parents=True, exist_ok=True)
        progress.task_started()
        try:
            size = await asyncio.get_running_loop().run_in_executor(
                executor, save_url, url, destination, timeout
            )
            progress.task_finished(size)
        except Exception as error:
            progress.task_failed()
            print(f"error {url}: {error}", file=sys.stderr)

    try:
        await asyncio.gather(*(download(*job) for job in jobs))
    finally:
        progress.close()


async def main(args: argparse.Namespace) -> int:
    executor = ThreadPoolExecutor(max_workers=args.concurrency)
    try:
        for symbol in (value.upper() for value in args.symbols):
            try:
                files = await asyncio.get_running_loop().run_in_executor(
                    executor, extract_files, symbol, args.timeout
                )
                files = filter_files(files, args.start, args.end)
                print(f"{symbol}: selected {len(files)} files")
            except Exception as error:
                print(f"{symbol}: cannot read directory: {error}", file=sys.stderr)
                continue

            jobs = [
                (
                    filename,
                    url,
                    (
                        Path(args.output) / filename
                        if args.flat_output
                        else Path(args.output) / symbol / "public_trades" / filename
                    ),
                )
                for filename, url in files
            ]
            if jobs:
                print(f"{symbol}: starting {len(jobs)} files")
                await download_files(executor, jobs, args.timeout)
    finally:
        executor.shutdown(wait=True)
    return 0


def parse_date(value: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as error:
        raise argparse.ArgumentTypeError(
            f"invalid date {value!r}; expected YYYY-MM-DD"
        ) from error


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download public trade files symbol by symbol."
    )
    parser.add_argument("symbols", nargs="+", help="Symbols, for example BTCUSDT SOLUSDT")
    parser.add_argument(
        "-c",
        "--concurrency",
        type=int,
        default=12,
        help="Maximum number of simultaneous downloads (default: 12)",
    )
    parser.add_argument(
        "-o", "--output", default=".", help="Output directory (default: current directory)"
    )
    parser.add_argument(
        "--flat-output",
        action="store_true",
        help="Write files directly into --output instead of symbol subdirectories",
    )
    parser.add_argument(
        "--timeout", type=float, default=60.0, help="Per-request timeout in seconds"
    )
    parser.add_argument(
        "--start",
        type=parse_date,
        help="First date to download, inclusive (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end",
        type=parse_date,
        help="Last date to download, inclusive (YYYY-MM-DD)",
    )
    args = parser.parse_args()
    if args.concurrency < 1:
        parser.error("--concurrency must be at least 1")
    if args.timeout <= 0:
        parser.error("--timeout must be greater than 0")
    if args.start and args.end and args.start > args.end:
        parser.error("--start must not be later than --end")
    return args


if __name__ == "__main__":
    try:
        raise SystemExit(asyncio.run(main(parse_args())))
    except KeyboardInterrupt:
        print("\ninterrupted", file=sys.stderr)
        raise SystemExit(130)
