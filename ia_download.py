#!/usr/bin/env python3
# Copyright (C) 2026 Whyte
# SPDX-License-Identifier: GPL-3.0-or-later
"""Resilient downloads from a single archive.org item.

Requires: pip install internetarchive
Public downloads need no credentials. Configured IA cookies/keys are used
when available. Re-run the identical command to resume partial downloads.

  ./ia_download.py my-item ./files/ --dry-run
  ./ia_download.py my-item ./files/ --original --log download.log

Exit codes: 0 complete, 1 failed files, 2 bad usage/local paths, 130 interrupted.
"""

from __future__ import annotations

import argparse
from contextlib import ExitStack
from dataclasses import dataclass, field
from email.utils import parsedate_to_datetime
import fnmatch
import hashlib
import json
import logging
import math
import os
from pathlib import Path
import random
import re
import signal
import stat
import sys
import tempfile
import time
from urllib.parse import quote

try:
    import requests
    from internetarchive import get_session
    from internetarchive.auth import S3Auth
    from internetarchive.utils import validate_s3_identifier
    from ia_common import NETWORK_ERRORS, RETRYABLE_STATUS, clock, human, md5_of, safe
except ImportError as exc:
    sys.exit(f"Missing dependency ({exc.name}). Install with:\n    pip install internetarchive")


log = logging.getLogger("ia_download")
WORK_DIR = ".ia_download"
CHUNK_SIZE = 1024 * 1024
PROGRESS_SECONDS = 15
STATE_VERSION = 1


class UsageError(ValueError):
    pass


class TransferError(Exception):
    def __init__(self, message, retryable=True, fatal=False, retry_after=0):
        super().__init__(message)
        self.retryable = retryable
        self.fatal = fatal
        self.retry_after = retry_after


class IntegrityError(TransferError):
    """These bytes must be discarded before retrying."""


class Stopped(Exception):
    pass


class Interrupt:
    """Stop between chunks; a second signal aborts a blocked read immediately."""

    def __init__(self):
        self.requested = False
        self.previous = {}

    def __enter__(self):
        for sig in (signal.SIGINT, signal.SIGTERM):
            self.previous[sig] = signal.signal(sig, self.handle)
        return self

    def __exit__(self, *exc):
        for sig, handler in self.previous.items():
            signal.signal(sig, handler)

    def handle(self, signum, frame):
        if self.requested:
            raise KeyboardInterrupt
        self.requested = True
        log.warning("Stopping after the current read; keeping partial downloads. "
                    "Press Ctrl-C again to abort immediately.")

    def check(self):
        if self.requested:
            raise Stopped

    def sleep(self, seconds):
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            self.check()
            time.sleep(max(0, min(0.25, deadline - time.monotonic())))
        self.check()


def absolute(path: Path) -> Path:
    # Keep symlinks visible so check_path can refuse them.
    return Path(os.path.abspath(path.expanduser()))


def check_path(path: Path, directory=False):
    """Refuse symlinks, special files, and non-directory parent components."""
    for component in reversed((path, *path.parents)):
        try:
            info = component.lstat()
        except FileNotFoundError:
            continue
        if stat.S_ISLNK(info.st_mode):
            raise UsageError(f"Refusing symlink: {component}")
        is_dir = component != path or directory
        if not (stat.S_ISDIR(info.st_mode) if is_dir else stat.S_ISREG(info.st_mode)):
            raise UsageError(f"Expected {'directory' if is_dir else 'regular file'}: {component}")


class RunLock:
    """Exclusive PID lock, reclaimed only when its owner is definitely gone."""

    def __init__(self, path):
        self.path = path

    def __enter__(self):
        check_path(self.path)
        for attempt in range(2):
            try:
                fd = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
                break
            except FileExistsError:
                try:
                    pid = int(self.path.read_text().strip())
                    if pid <= 0:
                        raise ValueError
                    os.kill(pid, 0)
                except ProcessLookupError:
                    if attempt == 0:
                        self.path.unlink()
                        continue
                except (OSError, ValueError):
                    pass
                raise UsageError(f"Download lock is held or cannot be verified: {self.path}")
        with os.fdopen(fd, "w") as stream:
            stream.write(str(os.getpid()))
            self.inode = os.fstat(stream.fileno()).st_ino
        return self

    def __exit__(self, *exc):
        try:
            if self.path.lstat().st_ino == self.inode:
                self.path.unlink()
        except FileNotFoundError:
            pass


@dataclass(frozen=True)
class RemoteFile:
    name: str
    size: int | None
    md5: str

    def identity(self):
        return {"size": self.size, "md5": self.md5}


def select_files(metadata, args):
    entries = metadata.get("files")
    if not isinstance(entries, list):
        raise UsageError("Item does not exist or has no accessible file list")
    files = []
    names = set()
    for entry in entries:
        if not isinstance(entry, dict) or not isinstance(entry.get("name"), str):
            raise UsageError("Malformed file entry in item metadata")
        name = entry["name"]
        if args.original and entry.get("source") != "original":
            continue
        if args.glob and not any(fnmatch.fnmatchcase(name, p) for p in args.glob):
            continue
        if any(fnmatch.fnmatchcase(name, p) for p in args.exclude):
            continue
        segments = name.split("/")
        if (any(s in ("", ".", "..") or s != s.strip() or s.endswith(".") for s in segments)
                or any(ord(c) < 32 or ord(c) == 127 for c in name)
                or "\\" in name or ":" in name
                or segments[0].casefold() == WORK_DIR.casefold()):
            raise UsageError(f"Unsafe or reserved remote filename: {name!r}")
        # Avoid Windows device names even when the download is made on Unix.
        devices = {"con", "prn", "aux", "nul"} | {
            f"{prefix}{n}" for prefix in ("com", "lpt") for n in range(1, 10)
        }
        if any(s.split(".")[0].casefold() in devices for s in segments):
            raise UsageError(f"Reserved device filename: {name!r}")
        key = name.casefold()
        if key in names:
            raise UsageError(f"Duplicate or case-insensitive filename collision: {name!r}")
        names.add(key)
        raw_size = entry.get("size")
        if raw_size is not None and not re.fullmatch(r"\d+", str(raw_size)):
            raise UsageError(f"Invalid size for {name!r}")
        digest = entry.get("md5") or ""
        if not isinstance(digest, str) or (digest and not re.fullmatch(r"[0-9a-fA-F]{32}", digest)):
            raise UsageError(f"Invalid MD5 for {name!r}")
        if digest == "0" * 32:
            digest = ""  # Treat an unavailable-checksum placeholder as absent.
        files.append(RemoteFile(name, int(raw_size) if raw_size is not None else None, digest.lower()))
    for name in names:
        parts = name.split("/")
        if any("/".join(parts[:i]) in names for i in range(1, len(parts))):
            raise UsageError(f"File/directory filename collision: {name!r}")
    return sorted(files, key=lambda f: f.name)


def snapshot(path):
    check_path(path)
    try:
        info = path.stat()
    except FileNotFoundError:
        return None
    return [info.st_size, info.st_mtime_ns, info.st_ctime_ns, info.st_ino, info.st_dev]


@dataclass
class State:
    path: Path
    identifier: str
    destination: Path
    partial: dict = field(default_factory=dict)
    completed: dict = field(default_factory=dict)

    @classmethod
    def load(cls, path, identifier, destination):
        check_path(path)
        state = cls(path, identifier, destination)
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            if (not isinstance(data, dict) or data.get("version") != STATE_VERSION
                    or data.get("identifier") != identifier
                    or data.get("destination") != str(destination)
                    or not isinstance(data.get("partial"), dict)
                    or not isinstance(data.get("completed"), dict)):
                raise ValueError("state belongs to another item, destination, or version")
            state.partial = data["partial"]
            state.completed = data["completed"]
        except FileNotFoundError:
            pass
        except (OSError, ValueError, UnicodeError) as exc:
            log.warning("Cannot use state (%s); checking local files again.", safe(exc))
        return state

    def save(self):
        check_path(self.path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        data = {"version": STATE_VERSION, "identifier": self.identifier,
                "destination": str(self.destination), "partial": self.partial,
                "completed": self.completed}
        tmp = None
        try:
            with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8",
                                             dir=self.path.parent, delete=False) as stream:
                tmp = Path(stream.name)
                json.dump(data, stream)
            os.replace(tmp, self.path)
        finally:
            if tmp is not None:
                tmp.unlink(missing_ok=True)

    def matches(self, file, target):
        info = snapshot(target)
        if info is None or (file.size is not None and info[0] != file.size):
            return False
        cached = self.completed.get(file.name)
        if cached == {**file.identity(), "stat": info}:
            return True
        if file.size is None and not file.md5:
            return False
        if file.md5 and md5_of(target) != file.md5:
            return False
        self.completed[file.name] = {**file.identity(), "stat": info}
        return True


def response_error(response):
    status = response.status_code
    if status < 400:
        return
    wait = 0
    header = response.headers.get("Retry-After", "")
    try:
        wait = float(header)
    except ValueError:
        try:
            wait = parsedate_to_datetime(header).timestamp() - time.time()
        except (TypeError, ValueError, OverflowError):
            pass
    if not math.isfinite(wait):
        wait = 0
    raise TransferError(f"HTTP {status}", retryable=status in RETRYABLE_STATUS or status >= 500,
                        fatal=status in (401, 403), retry_after=max(0, wait))


def retry_sleep(args, interrupt, attempt, exc):
    if not exc.retryable or attempt == args.retries:
        raise exc
    delay = min(args.max_retry_wait, args.retry_wait * 2 ** min(attempt - 1, 30))
    delay = max(delay * (0.5 + random.random()), exc.retry_after)
    log.warning("    %s (attempt %d/%d); retrying in %s", safe(exc), attempt,
                args.retries, clock(delay))
    interrupt.sleep(delay)


def read_manifest(session, args, interrupt):
    url = f"https://archive.org/metadata/{quote(args.identifier, safe='')}"
    for attempt in range(1, args.retries + 1):
        interrupt.check()
        try:
            with session.get(url, timeout=(args.connect_timeout, args.read_timeout)) as response:
                response_error(response)
                try:
                    metadata = response.json()
                except ValueError as exc:
                    raise TransferError("Invalid JSON in item metadata") from exc
                if not isinstance(metadata, dict):
                    raise TransferError("Invalid item metadata")
                return metadata
        except NETWORK_ERRORS as exc:
            retry_sleep(args, interrupt, attempt, TransferError(safe(exc)))
        except TransferError as exc:
            retry_sleep(args, interrupt, attempt, exc)


def archive_session():
    try:
        return get_session(http_adapter_kwargs={"max_retries": 0})
    except ValueError as exc:
        raise UsageError(f"Cannot load IA configuration: {safe(exc)}") from exc


def partial_path(work, identifier, name):
    key = hashlib.sha256(name.encode("utf-8")).hexdigest()
    return work / "parts" / identifier / f"{key}.part"


def clear_partial(part):
    check_path(part)
    part.unlink(missing_ok=True)


def verified(part, file):
    return (file.size is None or part.stat().st_size == file.size) and (
        not file.md5 or md5_of(part) == file.md5)


def size_label(size):
    return human(size) if size is not None else "unknown size"


def download_one(session, file, target, part, state, args, interrupt, expected_local):
    """Resume only a partial belonging to the same advertised content."""
    check_path(part)
    part.parent.mkdir(parents=True, exist_ok=True)
    if state.partial.get(file.name) != file.identity() or not file.md5:
        clear_partial(part)
    state.partial[file.name] = file.identity()
    state.save()  # Persist identity before writing any bytes.
    started = last_report = time.monotonic()
    received = 0
    url = f"https://archive.org/download/{quote(args.identifier, safe='')}/{quote(file.name, safe='/')}"
    auth = S3Auth(session.access_key, session.secret_key) if (
        session.access_key and session.secret_key) else None

    for attempt in range(1, args.retries + 1):
        interrupt.check()
        try:
            check_path(part)
            if attempt > 1 and not file.md5:
                clear_partial(part)
            offset = part.stat().st_size if part.exists() else 0
            if part.exists() and file.size is not None and (
                    offset > file.size or (offset == file.size and not verified(part, file))):
                log.warning("    Discarding invalid partial for %s", file.name)
                clear_partial(part)
                offset = 0
            complete = part.exists() and (
                offset == file.size if file.size is not None else bool(file.md5) and verified(part, file))
            if not complete:
                headers = {"Accept-Encoding": "identity"}
                if offset:
                    headers["Range"] = f"bytes={offset}-"
                    log.info("    Resuming at %s of %s", human(offset), size_label(file.size))
                with session.get(url, stream=True, headers=headers, auth=auth, params={"cnt": "0"},
                                 timeout=(args.connect_timeout, args.read_timeout)) as response:
                    if response.status_code == 416 and offset:
                        clear_partial(part)
                        raise TransferError("Range rejected; restarting from byte zero")
                    response_error(response)
                    if response.status_code not in (200, 206):
                        raise TransferError(f"Unexpected HTTP {response.status_code}", retryable=False)
                    if response.headers.get("Content-Encoding", "identity").lower() != "identity":
                        raise TransferError("Server ignored Accept-Encoding: identity")
                    if response.status_code == 206:
                        match = re.fullmatch(r"bytes (\d+)-(\d+)/(\d+)",
                                             response.headers.get("Content-Range", ""))
                        if not match:
                            raise TransferError("Missing or malformed Content-Range")
                        start, end, total = map(int, match.groups())
                        if (start != offset or (file.size is not None and total != file.size)
                                or not start <= end < total):
                            raise TransferError("Content-Range does not match requested bytes or metadata")
                        expected_body = end - start + 1
                        expected_total = total
                    else:
                        if offset:
                            log.info("    Server ignored Range; restarting from byte zero")
                        offset = 0
                        expected_body = file.size
                        expected_total = file.size
                    length = response.headers.get("Content-Length")
                    if length is not None:
                        if not re.fullmatch(r"\d+", length):
                            raise TransferError("Invalid Content-Length")
                        if expected_body is not None and int(length) != expected_body:
                            raise TransferError("Content-Length does not match metadata/range")
                        expected_body = int(length)
                        if expected_total is None:
                            expected_total = offset + expected_body
                    check_path(part)
                    written = 0
                    with part.open("ab" if offset else "wb") as stream:
                        for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                            interrupt.check()
                            if not chunk:
                                continue
                            if expected_body is not None and written + len(chunk) > expected_body:
                                raise IntegrityError("Response is larger than its advertised size")
                            stream.write(chunk)
                            written += len(chunk)
                            received += len(chunk)
                            now = time.monotonic()
                            if now - last_report >= PROGRESS_SECONDS:
                                rate = received / max(now - started, 0.001)
                                if expected_total is not None:
                                    eta = (expected_total - offset - written) / max(rate, 0.001)
                                    log.info("    %s / %s, %s/s, ETA %s", human(offset + written),
                                             human(expected_total), human(rate), clock(eta))
                                else:
                                    log.info("    %s received, %s/s", human(written), human(rate))
                                last_report = now
                    if expected_total is not None and offset + written != expected_total:
                        raise TransferError("Incomplete response; keeping partial download")
            interrupt.check()
            if not verified(part, file):
                raise IntegrityError("MD5 mismatch; discarding corrupt download")
            check_path(target)
            if snapshot(target) != expected_local:
                raise UsageError(f"Destination changed during download; re-run to review it: {target}")
            target.parent.mkdir(parents=True, exist_ok=True)
            check_path(target)
            os.replace(part, target)
            state.completed[file.name] = {**file.identity(), "stat": snapshot(target)}
            state.partial.pop(file.name, None)
            state.save()
            elapsed = max(time.monotonic() - started, 0.001)
            verification = "MD5 verified" if file.md5 else (
                "Size verified only" if file.size is not None else "No metadata size/checksum to verify")
            log.info("    %s (%s), %s/s", verification,
                     human(target.stat().st_size), human(received / elapsed))
            return
        except NETWORK_ERRORS as exc:
            retry_sleep(args, interrupt, attempt, TransferError(safe(exc)))
        except TransferError as exc:
            if isinstance(exc, IntegrityError):
                clear_partial(part)
            retry_sleep(args, interrupt, attempt, exc)


def build_parser():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("identifier", help="Source archive.org item identifier")
    parser.add_argument("destination", type=Path, help="Directory to download into")
    parser.add_argument("--dry-run", action="store_true", help="Read metadata and plan; write no files")
    parser.add_argument("--original", action="store_true", help="Download only original files")
    parser.add_argument("--glob", action="append", default=[], metavar="GLOB",
                        help="Include matching remote paths, repeatable (e.g. '*.zip')")
    parser.add_argument("--exclude", action="append", default=[], metavar="GLOB",
                        help="Exclude matching remote paths, repeatable")
    parser.add_argument("--retries", type=int, default=12, metavar="N", help="Attempts per request (default: 12)")
    parser.add_argument("--retry-wait", type=float, default=15, metavar="SEC", help="Initial backoff (default: 15)")
    parser.add_argument("--max-retry-wait", type=float, default=600, metavar="SEC", help="Backoff ceiling before jitter (default: 600)")
    parser.add_argument("--wait-between", type=float, default=0, metavar="SEC", help="Pause between files (default: 0)")
    parser.add_argument("--connect-timeout", type=float, default=30, metavar="SEC", help="Connect timeout (default: 30)")
    parser.add_argument("--read-timeout", type=float, default=60, metavar="SEC", help="Read inactivity timeout (default: 60)")
    parser.add_argument("--state", type=Path, metavar="PATH", help="State file (default: DEST/.ia_download/state/ID.json)")
    parser.add_argument("--log", type=Path, metavar="PATH", help="Append logs to this file (ignored in dry runs)")
    parser.add_argument("--yes", "-y", action="store_true", help="Replace differing local files without prompting")
    return parser


class RedactingFormatter(logging.Formatter):
    def format(self, record):
        return safe(super().format(record))


def setup_logging(logfile=None):
    formatter = RedactingFormatter("%(asctime)s  %(levelname)-7s %(message)s", "%Y-%m-%d %H:%M:%S")
    if logfile:
        check_path(logfile)
        logfile.parent.mkdir(parents=True, exist_ok=True)
        handler = logging.FileHandler(logfile, encoding="utf-8")
    else:
        handler = logging.StreamHandler(sys.stdout)
        for previous in log.handlers:
            previous.close()
        log.handlers.clear()
        log.propagate = False
        log.setLevel(logging.INFO)
        for name in ("internetarchive", "requests", "urllib3"):
            logging.getLogger(name).setLevel(logging.CRITICAL)
    handler.setFormatter(formatter)
    log.addHandler(handler)


def overlapping(a, b):
    aa, bb = tuple(s.casefold() for s in a.parts), tuple(s.casefold() for s in b.parts)
    return aa[:len(bb)] == bb or bb[:len(aa)] == aa


def run(args, interrupt):
    try:
        validate_s3_identifier(args.identifier)
    except Exception as exc:
        raise UsageError(f"Invalid archive.org identifier: {safe(exc)}") from exc
    if args.retries < 1:
        raise UsageError("--retries must be at least 1")
    for name in ("retry_wait", "max_retry_wait", "wait_between", "connect_timeout", "read_timeout"):
        value = getattr(args, name)
        if not math.isfinite(value) or value < 0 or (name.endswith("timeout") and value == 0):
            raise UsageError(f"--{name.replace('_', '-')} must be finite and "
                             f"{'positive' if name.endswith('timeout') else 'nonnegative'}")
    root = absolute(args.destination)
    work = root / WORK_DIR
    state_path = absolute(args.state) if args.state else work / "state" / f"{args.identifier}.json"
    logfile = absolute(args.log) if args.log else None
    check_path(root, directory=True)
    check_path(work, directory=True)
    check_path(state_path)
    if args.state and overlapping(state_path, work):
        raise UsageError("Custom --state must be outside the reserved .ia_download directory")
    if logfile:
        check_path(logfile)
        if overlapping(logfile, work) or overlapping(logfile, state_path) or overlapping(
                logfile, state_path.with_name(state_path.name + ".lock")):
            raise UsageError("--log overlaps download bookkeeping")
    log.info("Reading item '%s'", args.identifier)
    with archive_session() as session:
        files = select_files(read_manifest(session, args, interrupt), args)
        if not files:
            log.info("No matching files.")
            return 0
        for file in files:
            target = root / file.name
            check_path(target)
            part = partial_path(work, args.identifier, file.name)
            check_path(part)
            reserved = [state_path, state_path.with_name(state_path.name + ".lock")]
            if logfile:
                reserved.append(logfile)
            if any(overlapping(target, path) for path in reserved):
                raise UsageError(f"Remote file overlaps state, lock, or log: {file.name!r}")
        log.info("Selected %d files (%s known size).", len(files),
                 human(sum(f.size for f in files if f.size is not None)))
        missing = sum(not f.md5 for f in files)
        if missing:
            log.warning("%d file(s) have no MD5 in metadata: checking sizes where available; "
                        "their partial downloads restart from zero.", missing)

        with ExitStack() as stack:
            if not args.dry_run:
                work.mkdir(parents=True, exist_ok=True)
                stack.enter_context(RunLock(work / "run.lock"))
                state_path.parent.mkdir(parents=True, exist_ok=True)
                stack.enter_context(RunLock(state_path.with_name(state_path.name + ".lock")))
                if logfile:
                    setup_logging(logfile)
            state = State.load(state_path, args.identifier, root)
            pending, replacing = [], []
            for file in files:
                interrupt.check()
                target = root / file.name
                if state.matches(file, target):
                    log.info("  [present] %s", file.name)
                    continue
                existing = snapshot(target)
                pending.append((file, existing))
                if existing is not None:
                    replacing.append(file)
                if args.dry_run:
                    log.info("  [%-7s] %s (%s)", "replace" if existing else "download",
                             file.name, size_label(file.size))
            if args.dry_run:
                log.info("Dry run: %d to download; %d already present. No files written.",
                         len(pending), len(files) - len(pending))
                return 0
            if replacing and not args.yes:
                log.warning("%d existing local file(s) would be replaced:", len(replacing))
                for file in replacing:
                    log.warning("    %s", file.name)
                if not sys.stdin.isatty():
                    raise UsageError("Use --yes to replace differing files in an unattended run")
                if input("Replace these local files? [y/N] ").strip().lower() not in ("y", "yes"):
                    raise UsageError("Replacement declined")
            state.save()
            succeeded = failed = 0
            for index, (file, existing) in enumerate(pending, 1):
                interrupt.check()
                log.info("[%d/%d] %s (%s)", index, len(pending), file.name, size_label(file.size))
                try:
                    download_one(session, file, root / file.name,
                                 partial_path(work, args.identifier, file.name),
                                 state, args, interrupt, existing)
                    succeeded += 1
                except TransferError as exc:
                    log.error("    %s: %s", file.name, safe(exc))
                    failed += 1
                    if exc.fatal:
                        log.error("Access denied; check credentials and item permissions.")
                        break
                except OSError as exc:
                    log.error("Local file error for %s: %s", file.name, safe(exc))
                    return 1
                if index < len(pending):
                    interrupt.sleep(args.wait_between)
            log.info("Downloaded %d; already present %d; failed %d.",
                     succeeded, len(files) - len(pending), failed)
            return 1 if failed else 0


def main(argv=None):
    args = build_parser().parse_args(argv)
    setup_logging()
    try:
        with Interrupt() as interrupt:
            return run(args, interrupt)
    except (Stopped, KeyboardInterrupt):
        log.warning("Interrupted. Re-run the same command to resume.")
        return 130
    except UsageError as exc:
        log.error("%s", safe(exc))
        return 2
    except (OSError, TransferError, requests.exceptions.RequestException) as exc:
        log.error("%s", safe(exc))
        return 1


if __name__ == "__main__":
    sys.exit(main())
