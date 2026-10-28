"""HTTP integration tests; no archive.org access or credentials required."""

import contextlib
import io
import json
import os
from pathlib import Path
import signal
import tempfile
import threading
import unittest
from unittest.mock import patch
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import unquote, urlsplit

import requests

import ia_common
import ia_download as download
import ia_upload as upload


class LocalSession(requests.Session):
    access_key = secret_key = None

    def get(self, url, **kwargs):
        return super().get(url.replace("https://archive.org", self.base_url, 1), **kwargs)


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_GET(self):
        fixture = self.server.fixture
        path = unquote(urlsplit(self.path).path)
        if path.startswith("/metadata/"):
            fixture.metadata_requests += 1
            status = fixture.metadata_status.pop(0) if fixture.metadata_status else 200
            payload = json.dumps(fixture.manifest()).encode()
            self.send_response(status)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return
        name = path.split("/", 3)[3]
        count = sum(r["name"] == name for r in fixture.requests)
        fixture.requests.append({"name": name, "range": self.headers.get("Range"),
                                 "encoding": self.headers.get("Accept-Encoding")})
        modes = fixture.modes.get(name, [])
        mode = modes[count] if count < len(modes) else {}
        if "status" in mode:
            self.send_response(mode["status"])
            if "retry_after" in mode:
                self.send_header("Retry-After", mode["retry_after"])
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        payload = fixture.files[name]
        if mode.get("corrupt"):
            payload = payload[:-1] + bytes([payload[-1] ^ 0xFF])
        requested = self.headers.get("Range")
        offset = int(requested[6:-1]) if requested and not mode.get("ignore_range") else 0
        ranged = requested is not None and not mode.get("ignore_range")
        self.send_response(206 if ranged else 200)
        if ranged:
            content_range = f"bytes {offset + mode.get('range_delta', 0)}-{len(payload) - 1}/{len(payload)}"
            self.send_header("Content-Range", mode.get("content_range", content_range))
        body = payload[offset:]
        if not mode.get("no_length"):
            self.send_header("Content-Length", str(mode.get("header_length", len(body))))
        if mode.get("content_encoding"):
            self.send_header("Content-Encoding", mode["content_encoding"])
        self.end_headers()
        try:
            self.wfile.write(body[:mode["cut"]] if "cut" in mode else body)
            self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            pass
        self.close_connection = True


class DownloadTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        cls.server.daemon_threads = True
        cls.thread = threading.Thread(target=cls.server.serve_forever,
                                      kwargs={"poll_interval": 0.01}, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join()

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name) / "destination"
        self.files = {"maps/a #%.zip": b"map contents", "empty": b"",
                      "preview.jpg": b"preview"}
        self.sources = {"preview.jpg": "derivative"}
        self.no_md5 = set()
        self.entries = None
        self.modes = {}
        self.requests = []
        self.metadata_requests = 0
        self.metadata_status = []
        self.server.fixture = self
        session_factory = self.session
        self.mock_session = patch.object(download, "get_session", side_effect=session_factory).start()
        self.addCleanup(patch.stopall)
        self.console = io.StringIO()

    def session(self, **kwargs):
        session = LocalSession()
        session.base_url = f"http://127.0.0.1:{self.server.server_port}"
        session.trust_env = False
        return session

    def manifest(self):
        if self.entries is not None:
            return {"files": self.entries}
        return {"files": [
            {"name": name, "size": str(len(body)), "source": self.sources.get(name, "original"),
             **({} if name in self.no_md5 else {"md5": digest_for_test(body)})}
            for name, body in self.files.items()
        ]}

    def run_download(self, *options):
        with contextlib.redirect_stdout(self.console):
            return download.main(["test-item", str(self.root), "--retry-wait", "0",
                                  "--max-retry-wait", "0", "--retries", "4", *options])

    def large_file(self):
        self.files = {"large.zip": bytes(range(256)) * (download.CHUNK_SIZE // 128) + b"tail"}
        return self.files["large.zip"]

    def target(self):
        return self.root / "large.zip"

    def test_download_nested_quoted_empty_and_derivative_then_skip(self):
        self.assertEqual(self.run_download(), 0)
        for name, content in self.files.items():
            self.assertEqual((self.root / name).read_bytes(), content)
        self.assertEqual(len(self.requests), 3)
        self.assertTrue(all(r["encoding"] == "identity" for r in self.requests))
        with patch.object(download, "md5_of", side_effect=AssertionError("cache not used")):
            self.assertEqual(self.run_download(), 0)
        self.assertEqual(len(self.requests), 3)
        self.assertFalse((self.root / download.WORK_DIR / "run.lock").exists())

    def test_dry_run_writes_no_destination_state_or_log(self):
        logfile = Path(self.tmp.name) / "new-logs" / "download.log"
        state = Path(self.tmp.name) / "new-state" / "state.json"
        self.assertEqual(self.run_download("--dry-run", "--state", str(state), "--log", str(logfile)), 0)
        self.assertFalse(self.root.exists())
        self.assertFalse(logfile.parent.exists())
        self.assertFalse(state.parent.exists())
        self.assertEqual(self.requests, [])

    def test_filters_match_remote_paths_and_exclude_independently(self):
        self.assertEqual(self.run_download("--original", "--glob", "maps/*", "--glob", "empty",
                                           "--exclude", "empty"), 0)
        self.assertEqual([r["name"] for r in self.requests], ["maps/a #%.zip"])

    def test_disconnect_resumes_with_validated_range(self):
        body = self.large_file()
        self.modes["large.zip"] = [{"cut": download.CHUNK_SIZE + 10}]
        self.assertEqual(self.run_download(), 0)
        self.assertEqual(self.target().read_bytes(), body)
        self.assertEqual([r["range"] for r in self.requests], [None, f"bytes={download.CHUNK_SIZE}-"])

    def test_partial_survives_failed_run_and_resumes_next_invocation(self):
        body = self.large_file()
        self.modes["large.zip"] = [{"cut": download.CHUNK_SIZE + 10}]
        self.assertEqual(self.run_download("--retries", "1"), 1)
        self.assertFalse(self.target().exists())
        part = download.partial_path(self.root / download.WORK_DIR, "test-item", "large.zip")
        self.assertEqual(part.stat().st_size, download.CHUNK_SIZE)
        self.assertEqual(self.run_download(), 0)
        self.assertEqual(self.target().read_bytes(), body)
        self.assertEqual(self.requests[-1]["range"], f"bytes={download.CHUNK_SIZE}-")

    def test_range_ignored_restarts_without_appending(self):
        body = self.large_file()
        self.modes["large.zip"] = [{"cut": download.CHUNK_SIZE}, {"ignore_range": True}]
        self.assertEqual(self.run_download(), 0)
        self.assertEqual(self.target().read_bytes(), body)
        self.assertIn("ignored Range", self.console.getvalue())

    def test_invalid_ranges_are_rejected_before_appending(self):
        for mode in ({"range_delta": 1}, {"content_range": "bytes 1-3/*"},
                     {"content_range": "bytes 0-1/2"}):
            with self.subTest(mode=mode):
                self.requests.clear()
                self.large_file()
                self.target().unlink(missing_ok=True)
                self.modes["large.zip"] = [{"cut": download.CHUNK_SIZE}, mode, {}]
                self.assertEqual(self.run_download(), 0)
                self.assertEqual(self.target().read_bytes(), self.files["large.zip"])
                self.assertEqual([r["range"] for r in self.requests][1:],
                                 [f"bytes={download.CHUNK_SIZE}-"] * 2)

    def test_416_restarts_from_zero(self):
        body = self.large_file()
        self.modes["large.zip"] = [{"cut": download.CHUNK_SIZE}, {"status": 416}]
        self.assertEqual(self.run_download(), 0)
        self.assertEqual(self.target().read_bytes(), body)
        self.assertEqual(self.requests[-1]["range"], None)

    def test_checksum_mismatch_retries_and_keeps_existing_target_until_success(self):
        body = self.large_file()
        self.root.mkdir()
        self.target().write_bytes(b"original local contents")
        self.modes["large.zip"] = [{"corrupt": True}]
        with patch.object(download, "retry_sleep", wraps=download.retry_sleep) as retry:
            self.assertEqual(self.run_download("--yes"), 0)
        self.assertEqual(retry.call_count, 1)
        self.assertEqual(self.target().read_bytes(), body)
        self.assertEqual([r["range"] for r in self.requests], [None, None])

    def test_failed_checksum_never_replaces_existing_file(self):
        self.large_file()
        self.root.mkdir()
        original = b"keep this"
        self.target().write_bytes(original)
        self.modes["large.zip"] = [{"corrupt": True}]
        self.assertEqual(self.run_download("--yes", "--retries", "1"), 1)
        self.assertEqual(self.target().read_bytes(), original)

    def test_different_existing_file_requires_yes(self):
        self.large_file()
        self.root.mkdir()
        self.target().write_bytes(b"keep")
        with patch("sys.stdin.isatty", return_value=False):
            self.assertEqual(self.run_download(), 2)
        self.assertEqual(self.requests, [])
        self.assertEqual(self.target().read_bytes(), b"keep")

    def test_metadata_changed_invalidates_partial(self):
        self.large_file()
        self.modes["large.zip"] = [{"cut": download.CHUNK_SIZE}]
        self.assertEqual(self.run_download("--retries", "1"), 1)
        changed = b"Z" * len(self.files["large.zip"])
        self.files["large.zip"] = changed
        self.assertEqual(self.run_download(), 0)
        self.assertEqual(self.requests[-1]["range"], None)
        self.assertEqual(self.target().read_bytes(), changed)

    def test_missing_md5_restarts_partial_and_reports_size_only(self):
        body = self.large_file()
        self.no_md5.add("large.zip")
        self.modes["large.zip"] = [{"cut": download.CHUNK_SIZE}]
        self.assertEqual(self.run_download(), 0)
        self.assertEqual([r["range"] for r in self.requests], [None, None])
        self.assertEqual(self.target().read_bytes(), body)
        self.assertIn("Size verified only", self.console.getvalue())

    def test_missing_size_and_checksum_is_streamed_and_remembered(self):
        self.files = {"test-item_files.xml": b"<files/>"}
        self.entries = [{"name": "test-item_files.xml", "source": "original"}]
        self.assertEqual(self.run_download(), 0)
        self.assertEqual((self.root / "test-item_files.xml").read_bytes(), b"<files/>")
        self.assertIn("No metadata size/checksum", self.console.getvalue())
        self.assertEqual(self.run_download(), 0)
        self.assertEqual(len(self.requests), 1)

    def test_all_zero_checksum_is_treated_as_unavailable(self):
        self.files = {"test-item_files.xml": b"<files/>"}
        self.entries = [{"name": "test-item_files.xml", "md5": "0" * 32}]
        self.assertEqual(self.run_download(), 0)
        self.assertIn("No metadata size/checksum", self.console.getvalue())

    def test_bad_content_length_and_encoding_fail_without_publishing(self):
        for mode in ({"header_length": 10}, {"content_encoding": "gzip"}):
            with self.subTest(mode=mode):
                self.requests.clear()
                self.large_file()
                self.modes["large.zip"] = [mode]
                self.assertEqual(self.run_download("--retries", "1"), 1)
                self.assertFalse(self.target().exists())

    def test_incomplete_body_without_length_is_resumed(self):
        body = self.large_file()
        self.modes["large.zip"] = [{"no_length": True, "cut": download.CHUNK_SIZE}]
        self.assertEqual(self.run_download(), 0)
        self.assertEqual(self.requests[-1]["range"], f"bytes={download.CHUNK_SIZE}-")
        self.assertEqual(self.target().read_bytes(), body)

    def test_retryable_status_and_retry_after(self):
        self.files = {"data": b"contents"}
        self.modes["data"] = [{"status": 429, "retry_after": "3"}, {"status": 503}]
        with patch.object(download.Interrupt, "sleep") as sleep:
            self.assertEqual(self.run_download(), 0)
        self.assertEqual(sleep.call_args_list[0].args, (3.0,))
        self.assertEqual(len(self.requests), 3)

    def test_permission_denied_stops_batch_without_retries(self):
        self.files = {"a": b"a", "b": b"b"}
        self.modes["a"] = [{"status": 403}]
        self.assertEqual(self.run_download(), 1)
        self.assertEqual([r["name"] for r in self.requests], ["a"])

    def test_missing_file_does_not_prevent_next_download(self):
        self.files = {"a": b"a", "b": b"b"}
        self.modes["a"] = [{"status": 404}]
        self.assertEqual(self.run_download(), 1)
        self.assertEqual([r["name"] for r in self.requests], ["a", "b"])
        self.assertEqual((self.root / "b").read_bytes(), b"b")

    def test_metadata_request_retries_and_invalid_item_fails(self):
        self.metadata_status = [503]
        self.assertEqual(self.run_download("--dry-run"), 0)
        self.assertEqual(self.metadata_requests, 2)
        self.entries = "invalid"
        self.assertEqual(self.run_download(), 2)

    def test_unsafe_names_and_collisions_fail_before_writes(self):
        cases = [[name] for name in ("../escape", "/absolute", "a/../b", "a//b", "./b",
                                     "a\\b", "C:/file", "a\nfile", ".ia_download/stuff", "NUL", "a.")]
        cases += [["a", "A"], ["a", "a/child"]]
        for names in cases:
            with self.subTest(names=names):
                self.entries = [{"name": n, "size": "0"} for n in names]
                self.assertEqual(self.run_download(), 2)
                self.assertFalse(self.root.exists())

    def test_symlink_parent_target_and_partial_are_refused(self):
        self.files = {"folder/data": b"contents"}
        outside = Path(self.tmp.name) / "outside"
        outside.mkdir()
        self.root.mkdir()
        (self.root / "folder").symlink_to(outside, target_is_directory=True)
        self.assertEqual(self.run_download(), 2)
        self.assertEqual(list(outside.iterdir()), [])
        (self.root / "folder").unlink()
        (self.root / "folder").mkdir()
        (self.root / "folder/data").symlink_to(outside / "target")
        self.assertEqual(self.run_download(), 2)
        (self.root / "folder/data").unlink()
        part = download.partial_path(self.root / download.WORK_DIR, "test-item", "folder/data")
        part.parent.mkdir(parents=True)
        part.symlink_to(outside / "target")
        self.assertEqual(self.run_download(), 2)
        self.assertFalse((outside / "target").exists())

    def test_log_and_state_cannot_overlap_download_targets(self):
        self.files = {"data": b"contents"}
        for option in ("--state", "--log"):
            with self.subTest(option=option):
                self.assertEqual(self.run_download(option, str(self.root / "data")), 2)
                self.assertFalse(self.root.exists())

    def test_destination_lock_blocks_even_when_using_custom_state(self):
        work = self.root / download.WORK_DIR
        work.mkdir(parents=True)
        (work / "run.lock").write_text(str(os.getpid()))
        self.assertEqual(self.run_download("--state", str(Path(self.tmp.name) / "custom.json")), 2)
        self.assertEqual(self.requests, [])

    def test_interrupt_keeps_partial_and_restores_signal_handlers(self):
        body = self.large_file()
        previous = signal.getsignal(signal.SIGINT)
        part = download.partial_path(self.root / download.WORK_DIR, "test-item", "large.zip")
        original_check = download.Interrupt.check

        def stop_after_chunk(interrupt):
            if part.exists() and part.stat().st_size:
                raise download.Stopped
            original_check(interrupt)

        with patch.object(download.Interrupt, "check", stop_after_chunk):
            self.assertEqual(self.run_download(), 130)
        self.assertEqual(signal.getsignal(signal.SIGINT), previous)
        self.assertFalse(self.target().exists())
        self.assertEqual(self.run_download(), 0)
        self.assertEqual(self.target().read_bytes(), body)
        self.assertIsNotNone(self.requests[-1]["range"])

    def test_changed_local_file_is_rehashed(self):
        self.assertEqual(self.run_download(), 0)
        target = self.root / "maps/a #%.zip"
        target.write_bytes(b"X" * len(self.files["maps/a #%.zip"]))
        self.assertEqual(self.run_download("--yes"), 0)
        self.assertEqual(target.read_bytes(), self.files["maps/a #%.zip"])
        self.assertEqual(len(self.requests), 4)

    def test_invalid_retry_and_timeout_arguments(self):
        for option, value in (("--retries", "0"), ("--retry-wait", "nan"),
                              ("--read-timeout", "0"), ("--connect-timeout", "inf")):
            with self.subTest(option=option):
                self.assertEqual(self.run_download(option, value), 2)
        self.assertEqual(self.metadata_requests, 0)

    def test_log_redacts_credentials_and_uploader_shares_helpers(self):
        self.assertIs(upload.md5_of, ia_common.md5_of)
        self.assertIs(upload.safe, ia_common.safe)
        logfile = Path(self.tmp.name) / "download.log"
        with contextlib.redirect_stdout(self.console):
            download.setup_logging()
            download.setup_logging(logfile)
            download.log.error("Authorization: LOW ABC123:SECRET456")
        self.assertIn("LOW <redacted>", logfile.read_text())
        self.assertNotIn("SECRET456", logfile.read_text())


def digest_for_test(body):
    digest = ia_common._md5()
    digest.update(body)
    return digest.hexdigest()


if __name__ == "__main__":
    unittest.main()
