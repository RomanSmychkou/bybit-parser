#!/usr/bin/env python3
"""Download ob500 and ob200 files from quote-saver.bycsi.com."""

from __future__ import annotations

import argparse
import asyncio
import os
import re
import sys
from concurrent.futures import ThreadPoolExecutor
from html.parser import HTMLParser
from pathlib import Path
from typing import Iterable
from urllib.parse import unquote, urljoin, urlparse
from urllib.request import Request, urlopen

from download_progress import DownloadProgress


BASE_URL = "https://quote-saver.bycsi.com/orderbook/linear/{symbol}/"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:128.0) "
    "Gecko/20100101 Firefox/128.0"
)
FILE_RE = re.compile(r"ob(?:500|200)", re.IGNORECASE)


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
            while True:
                chunk = response.read(1024 * 1024)
                if not chunk:
                    break
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
        if not FILE_RE.search(filename) or file_url in seen:
            continue
        seen.add(file_url)
        result.append((filename, file_url))
    return result


async def run_in_pool(
    executor: ThreadPoolExecutor,
    jobs: Iterable[tuple[str, str, Path]],
    timeout: float,
) -> None:
    jobs = list(jobs)
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
    symbols = [symbol.upper() for symbol in args.symbols]
    executor = ThreadPoolExecutor(max_workers=args.concurrency)
    try:
        discovered: dict[str, list[tuple[str, str]]] = {}
        for symbol in symbols:
            try:
                files = await asyncio.get_running_loop().run_in_executor(
                    executor, extract_files, symbol, args.timeout
                )
                discovered[symbol] = files
                print(f"{symbol}: found {len(files)} files")
            except Exception as error:
                print(f"{symbol}: cannot read directory: {error}", file=sys.stderr)
                discovered[symbol] = []

        # Complete ob500 for every symbol before starting any ob200 download.
        markers = []
        if not args.exclude_ob500:
            markers.append("ob500")
        if not args.exclude_ob200:
            markers.append("ob200")
        for marker in markers:
            marker_lower = marker.lower()
            for symbol in symbols:
                jobs = [
                    (
                        filename,
                        url,
                        Path(args.output) / symbol / marker / filename,
                    )
                    for filename, url in discovered[symbol]
                    if marker_lower in filename.lower()
                ]
                if jobs:
                    print(f"{symbol}: starting {marker}: {len(jobs)} files")
                    await run_in_pool(executor, jobs, args.timeout)
    finally:
        executor.shutdown(wait=True)
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download ob500 files first, then ob200 files."
    )
    parser.add_argument("symbols", nargs="+", help="Symbols, for example BTCUSDT SOLUSDT")
    parser.add_argument(
        "-c",
        "--concurrency",
        type=int,
        default=4,
        help="Maximum number of simultaneous downloads (default: 4)",
    )
    parser.add_argument(
        "-o", "--output", default=".", help="Output directory (default: current directory)"
    )
    parser.add_argument(
        "--timeout", type=float, default=60.0, help="Per-request timeout in seconds"
    )
    parser.add_argument(
        "--exclude-ob200",
        action="store_true",
        help="Do not download ob200 files",
    )
    parser.add_argument(
        "--exclude-ob500",
        action="store_true",
        help="Do not download ob500 files",
    )
    args = parser.parse_args()
    if args.concurrency < 1:
        parser.error("--concurrency must be at least 1")
    if args.timeout <= 0:
        parser.error("--timeout must be greater than 0")
    return args


if __name__ == "__main__":
    try:
        raise SystemExit(asyncio.run(main(parse_args())))
    except KeyboardInterrupt:
        print("\ninterrupted", file=sys.stderr)
        raise SystemExit(130)
