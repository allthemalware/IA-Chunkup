#!/usr/bin/env python3
# Copyright (C) 2026 Whyte
# SPDX-License-Identifier: GPL-3.0-or-later
"""Replace named IA folders with verified, uncompressed tar files, one at a time.

  ./ia_repack.py ITEM 'folder one' 'parent/folder two' --dry-run
  ./ia_repack.py ITEM 'folder one' --delete-originals --yes --clean-local

Archives go into the same item: parent/folder becomes parent/folder.tar.
Deletion happens only AFTER the uploaded tar's size and MD5 are confirmed.
Re-run with the same work directory after a failure or interruption.
"""

from __future__ import annotations

import argparse
from dataclasses import asdict
import hashlib
import json
import math
import os
from pathlib import Path
import re
import sys
import tarfile
import tempfile
from urllib.parse import quote

import ia_download as download
import ia_upload as upload
from ia_common import _md5, human, md5_of, safe
from internetarchive.item import Item
from internetarchive.iarequest import S3Request


log = download.log


def write_job(path, data):
    download.check_path(path)
    tmp = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent,
                                         delete=False) as stream:
            tmp = Path(stream.name)
            json.dump(data, stream)
        os.replace(tmp, path)
    finally:
        if tmp is not None:
            tmp.unlink(missing_ok=True)


def selected(metadata, folder):
    entries = metadata.get("files")
    if not isinstance(entries, list):
        raise download.UsageError("Item does not exist or has no accessible file list")
    return download.select_files({"files": [entry for entry in entries
        if isinstance(entry, dict) and isinstance(entry.get("name"), str)
        and entry["name"].startswith(folder + "/")]},
        argparse.Namespace(original=False, glob=[], exclude=[]))


def archive_entry(metadata, name):
    return next((entry for entry in metadata.get("files", []) if entry.get("name") == name), None)


def archive_matches(metadata, name, digest, size):
    entry = archive_entry(metadata, name)
    return entry is not None and entry.get("md5", "").lower() == digest and str(entry.get("size")) == str(size)


def validate_sources(metadata, folder, files):
    expected = {file.name: file for file in files}
    for current in selected(metadata, folder):
        if expected.get(current.name) != current:
            raise download.UsageError(f"Source folder changed on IA: {current.name!r}; stopping deletion")


def load_job(path, identifier, folder, current):
    download.check_path(path)
    if path.exists():
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            if (data["version"] != 1 or data["identifier"] != identifier or data["folder"] != folder
                    or not isinstance(data["deleted"], list)):
                raise ValueError("wrong job identity or version")
            files = [download.RemoteFile(**record) for record in data["files"]]
            checked = download.select_files({"files": [asdict(file) for file in files]},
                argparse.Namespace(original=False, glob=[], exclude=[]))
            if not files or files != checked or any(not f.name.startswith(folder + "/") for f in files):
                raise ValueError("invalid job file list")
            if any(f.size is None or not f.md5 for f in files):
                raise ValueError("job lacks source sizes/checksums")
            if any(name not in {f.name for f in files} for name in data["deleted"]):
                raise ValueError("invalid deletion record")
            record = data["archive"]
            if record is not None and (
                    not isinstance(record, dict) or not isinstance(record.get("size"), int)
                    or record["size"] < 0 or not isinstance(record.get("md5"), str)
                    or not re.fullmatch(r"[0-9a-f]{32}", record["md5"])):
                raise ValueError("invalid archive checksum/size")
            if data["deleted"] and record is None:
                raise ValueError("deletion record without a verified archive")
            return data, files
        except (KeyError, TypeError, ValueError) as exc:
            raise download.UsageError(f"Unusable repack journal {path}: {safe(exc)}") from exc
    if not current:
        raise download.UsageError(f"No files found under folder {folder!r}")
    if any(f.size is None or not f.md5 for f in current):
        raise download.UsageError(f"Repacking requires a size and MD5 for every file in {folder!r}")
    return {"version": 1, "identifier": identifier, "folder": folder,
            "files": [asdict(file) for file in current], "archive": None,
            "deleted": [], "complete": False}, current


def verify_tar(path, files, interrupt):
    """Read back every tar member and compare its contents to IA's checksum."""
    expected = {file.name: file for file in files}
    seen = set()
    with tarfile.open(path, mode="r:") as archive:
        for member in archive:
            interrupt.check()
            file = expected.get(member.name)
            if file is None or member.name in seen or not member.isfile() or member.size != file.size:
                raise download.IntegrityError(f"Unexpected tar member: {member.name!r}", retryable=False)
            digest = _md5()
            with archive.extractfile(member) as stream:
                for chunk in iter(lambda: stream.read(download.CHUNK_SIZE), b""):
                    interrupt.check()
                    digest.update(chunk)
            if digest.hexdigest() != file.md5:
                raise download.IntegrityError(f"Tar member checksum mismatch: {member.name!r}", retryable=False)
            seen.add(member.name)
    if seen != set(expected):
        raise download.IntegrityError("Tar is missing source files", retryable=False)


def build_tar(path, root, files, interrupt):
    download.check_path(path)
    temporary = path.with_name(path.name + ".tmp")
    download.check_path(temporary)
    try:
        with tarfile.open(temporary, mode="w", format=tarfile.PAX_FORMAT) as archive:
            for file in files:
                interrupt.check()
                source = root / file.name
                download.check_path(source)
                archive.add(source, arcname=file.name, recursive=False)
        verify_tar(temporary, files, interrupt)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def poll_archive(session, args, interrupt, name, record, folder=None, files=None, removed=False):
    for attempt in range(1, args.retries + 1):
        metadata = download.read_manifest(session, args, interrupt)
        if folder is not None:
            validate_sources(metadata, folder, files)
        if archive_matches(metadata, name, record["md5"], record["size"]):
            if not removed or not selected(metadata, folder):
                return metadata
        download.retry_sleep(args, interrupt, attempt, download.TransferError(
            "Waiting for IA metadata to confirm " + ("source deletion" if removed else "the uploaded tar")))


def upload_tar(session, metadata, path, name, record, args, interrupt):
    existing = archive_entry(metadata, name)
    if existing:
        if archive_matches(metadata, name, record["md5"], record["size"]):
            log.info("Tar already present and verified: %s", name)
            return
        raise download.UsageError(f"Refusing to overwrite an existing, different archive: {name!r}")
    item = Item(session, args.identifier, item_metadata=metadata)
    file = upload.LocalFile(path, name, record["size"], path.stat().st_mtime, record["md5"])
    settings = upload.build_parser().parse_args([args.identifier, str(path)])
    settings.retries = args.retries
    settings.retry_wait = args.retry_wait
    settings.max_retry_wait = args.max_retry_wait
    settings.connect_timeout = args.connect_timeout
    previous_logger = upload.log
    try:
        upload.log = log
        outcome, reason = upload.upload_one(item, file, settings, record["size"], None, interrupt)
    finally:
        upload.log = previous_logger
    if interrupt.requested:
        raise download.Stopped
    if outcome != upload.Outcome.OK:
        raise download.TransferError(f"Tar upload failed: {safe(reason)}", retryable=False)


def delete_one(session, args, interrupt, name):
    url = f"https://s3.us.archive.org/{quote(args.identifier, safe='')}/{quote(name, safe='/')}"
    for attempt in range(1, args.retries + 1):
        interrupt.check()
        try:
            request = S3Request(method="DELETE", url=url, queue_derive=False,
                headers={"x-archive-cascade-delete": "0", "x-archive-queue-derive": "0"},
                access_key=session.access_key, secret_key=session.secret_key)
            with session.send(session.prepare_request(request),
                              timeout=(args.connect_timeout, args.read_timeout)) as response:
                if response.status_code != 404:
                    download.response_error(response)
            return
        except download.NETWORK_ERRORS as exc:
            download.retry_sleep(args, interrupt, attempt, download.TransferError(safe(exc)))
        except download.TransferError as exc:
            download.retry_sleep(args, interrupt, attempt, exc)


def clean_local(path, root, files, data, journal):
    """Delete only generated files enumerated by this job, retaining its journal."""
    for file in files:
        target = root / file.name
        download.check_path(target)
        if target.exists():
            if not download.verified(target, file):
                raise download.UsageError(f"Local source changed; refusing cleanup: {target}")
            target.unlink()
    download.check_path(path)
    if path.exists():
        if path.stat().st_size != data["archive"]["size"] or md5_of(path) != data["archive"]["md5"]:
            raise download.UsageError(f"Local tar changed; refusing cleanup: {path}")
        path.unlink()
    # Empty directories cost no significant space. Keep download state and
    # the journal so an interrupted cleanup can safely be repeated.
    data["cleaned"] = True
    write_job(journal, data)


def process_folder(session, args, interrupt, folder, work):
    key = hashlib.sha256(folder.encode("utf-8")).hexdigest()
    job_dir = work / key
    root, path, journal = job_dir / "files", job_dir / "archive.tar", job_dir / "job.json"
    for local, directory in ((job_dir, True), (root, True), (path, False), (journal, False)):
        download.check_path(local, directory=directory)
    metadata = download.read_manifest(session, args, interrupt)
    current = selected(metadata, folder)
    data, files = load_job(journal, args.identifier, folder, current)
    validate_sources(metadata, folder, files)
    name = folder + ".tar"
    for entry in metadata.get("files", []):
        if entry["name"].startswith(name + "/"):
            raise download.UsageError(f"Archive name collides with an existing remote directory: {name}")
    log.info("Folder %r: %d files, %s -> %s", folder, len(files),
             human(sum(file.size for file in files)), name)
    if args.dry_run:
        log.info("    Download, build uncompressed tar, upload and verify%s.",
                 ", then delete exactly these source files" if args.delete_originals else "")
        return
    job_dir.mkdir(parents=True, exist_ok=True)
    write_job(journal, data)
    if data.get("complete") and current:
        raise download.UsageError(f"Source files reappeared in a completed folder: {folder!r}")

    record = data.get("archive")
    remote_ready = record and archive_matches(metadata, name, record["md5"], record["size"])
    if data["deleted"] and not remote_ready:
        raise download.TransferError("Previously verified remote tar cannot be confirmed; keeping local files", retryable=False)
    if not remote_ready:
        if len(current) != len(files):
            raise download.UsageError("Source files disappeared before the tar was verified on IA; keeping local files")
        root.mkdir(parents=True, exist_ok=True)
        state = download.State.load(job_dir / "downloads.json", args.identifier, root)
        for index, file in enumerate(files, 1):
            interrupt.check()
            target = root / file.name
            if not state.matches(file, target):
                log.info("    Downloading %d/%d: %s", index, len(files), file.name)
                download.download_one(session, file, target,
                    download.partial_path(root / download.WORK_DIR, args.identifier, file.name),
                    state, args, interrupt, download.snapshot(target))
        state.save()
        if not path.exists():
            log.info("    Building and reading back uncompressed tar")
            build_tar(path, root, files, interrupt)
        else:
            verify_tar(path, files, interrupt)
        record = {"size": path.stat().st_size, "md5": md5_of(path)}
        data["archive"] = record
        write_job(journal, data)
        metadata = download.read_manifest(session, args, interrupt)
        validate_sources(metadata, folder, files)
        upload_tar(session, metadata, path, name, record, args, interrupt)
    metadata = poll_archive(session, args, interrupt, name, record, folder, files)
    if args.delete_originals:
        # Only the frozen, checksum-verified source list is ever deleted.
        remaining = {file.name for file in selected(metadata, folder)}
        for file in files:
            interrupt.check()
            if file.name in remaining and file.name not in data["deleted"]:
                # Recheck both the archive and source snapshot before each
                # destructive request, including when resuming a deletion.
                poll_archive(session, args, interrupt, name, record, folder, files)
                log.info("    Deleting verified source on IA: %s", file.name)
                delete_one(session, args, interrupt, file.name)
                data["deleted"].append(file.name)
                write_job(journal, data)
        poll_archive(session, args, interrupt, name, record, folder, files, removed=True)
        data["complete"] = True
        write_job(journal, data)
    if args.clean_local:
        clean_local(path, root, files, data, journal)
    log.info("Finished folder %r%s", folder, "; remote sources deleted" if args.delete_originals else "")


def build_parser():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("identifier")
    parser.add_argument("folders", nargs="+", help="Exact remote folder paths, processed in this order")
    parser.add_argument("--workdir", type=Path, help="Persistent work directory (default: .ia_repack_ID)")
    parser.add_argument("--dry-run", action="store_true", help="Plan only; write and change nothing")
    parser.add_argument("--delete-originals", action="store_true", help="Delete source files after verifying the tar on IA")
    parser.add_argument("--clean-local", action="store_true", help="Remove generated downloads and tar after success")
    parser.add_argument("--yes", "-y", action="store_true", help="Confirm deletion for unattended operation")
    parser.add_argument("--log", type=Path)
    parser.add_argument("--retries", type=int, default=12)
    parser.add_argument("--retry-wait", type=float, default=15)
    parser.add_argument("--max-retry-wait", type=float, default=600)
    parser.add_argument("--wait-between", type=float, default=0)
    parser.add_argument("--connect-timeout", type=float, default=30)
    parser.add_argument("--read-timeout", type=float, default=60)
    return parser


def run(args, interrupt):
    try:
        download.validate_s3_identifier(args.identifier)
    except Exception as exc:
        raise download.UsageError(f"Invalid identifier: {safe(exc)}") from exc
    for name in ("retry_wait", "max_retry_wait", "wait_between", "connect_timeout", "read_timeout"):
        value = getattr(args, name)
        if not math.isfinite(value) or value < 0 or (name.endswith("timeout") and value == 0):
            raise download.UsageError(f"Invalid --{name.replace('_', '-')}")
    if args.retries < 1:
        raise download.UsageError("--retries must be at least 1")
    args.folders = [folder.rstrip("/") for folder in args.folders]
    for folder in args.folders:
        download.select_files({"files": [{"name": folder + "/check", "size": "0"}]},
                             argparse.Namespace(original=False, glob=[], exclude=[]))
    for index, folder in enumerate(args.folders):
        for other in args.folders[index + 1:]:
            aa, bb = folder.casefold(), other.casefold()
            if aa == bb or aa.startswith(bb + "/") or bb.startswith(aa + "/") or (
                    aa.startswith(bb + ".tar/") or bb.startswith(aa + ".tar/")):
                raise download.UsageError("Named folders overlap or collide with an output tar")
    work = download.absolute(args.workdir or Path(f".ia_repack_{args.identifier}"))
    download.check_path(work, directory=True)
    if args.log and download.overlapping(download.absolute(args.log), work):
        raise download.UsageError("--log must be outside the work directory")
    with download.archive_session() as session:
        if not args.dry_run:
            if not session.access_key or not session.secret_key:
                raise download.UsageError("IA write credentials required; run ia configure first")
            if args.delete_originals and not args.yes:
                if not sys.stdin.isatty():
                    raise download.UsageError("Use --yes with --delete-originals for unattended operation")
                print("Folders to replace on IA: " + ", ".join(repr(f) for f in args.folders))
                if input("Delete their source files after verifying each uploaded tar? [y/N] ").strip().lower() not in ("y", "yes"):
                    raise download.UsageError("Deletion declined")
            work.mkdir(parents=True, exist_ok=True)
            if args.log:
                download.setup_logging(download.absolute(args.log))
        if args.dry_run:
            for folder in args.folders:
                process_folder(session, args, interrupt, folder, work)
        else:
            with download.RunLock(work / "run.lock"):
                for folder in args.folders:
                    interrupt.check()
                    process_folder(session, args, interrupt, folder, work)
                    interrupt.sleep(args.wait_between)
    return 0


def main(argv=None):
    args = build_parser().parse_args(argv)
    download.setup_logging()
    try:
        with download.Interrupt() as interrupt:
            return run(args, interrupt)
    except (download.Stopped, KeyboardInterrupt):
        log.warning("Interrupted. Keep the work directory and re-run the same command.")
        return 130
    except download.UsageError as exc:
        log.error("%s", safe(exc))
        return 2
    except (OSError, tarfile.TarError, download.TransferError, download.requests.exceptions.RequestException) as exc:
        log.error("%s. Keep the work directory and re-run to continue.", safe(exc))
        return 1


if __name__ == "__main__":
    sys.exit(main())
