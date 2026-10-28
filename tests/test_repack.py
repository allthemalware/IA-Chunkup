"""Exercise the repack workflow with real local tars and simulated IA writes."""

import contextlib
import io
from pathlib import Path
import tarfile
import tempfile
import unittest
from unittest.mock import MagicMock, patch

import ia_common
import ia_download as download
import ia_repack as repack

REAL_DELETE_ONE = repack.delete_one
REAL_UPLOAD_TAR = repack.upload_tar


def digest(body):
    checksum = ia_common._md5()
    checksum.update(body)
    return checksum.hexdigest()


class RepackTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.work = Path(self.tmp.name) / "work"
        self.remote = {"folder one/a.txt": b"first", "folder one/sub/empty": b"",
                       "folder two/b.bin": b"second", "unselected/keep": b"keep"}
        self.original = dict(self.remote)
        self.events = []
        self.output = io.StringIO()
        self.session = MagicMock()
        self.session.access_key, self.session.secret_key = "access", "secret"
        self.session.__enter__.return_value = self.session
        self.addCleanup(patch.stopall)
        patch.object(download, "get_session", return_value=self.session).start()
        self.manifest_mock = patch.object(download, "read_manifest", side_effect=self.manifest).start()
        self.download_mock = patch.object(download, "download_one", side_effect=self.download).start()
        self.upload_mock = patch.object(repack, "upload_tar", side_effect=self.upload).start()
        self.delete_mock = patch.object(repack, "delete_one", side_effect=self.delete).start()

    def manifest(self, *args):
        return {"files": [{"name": name, "size": str(len(body)), "md5": digest(body), "source": "original"}
                          for name, body in self.remote.items()]}

    def download(self, session, file, target, part, state, args, interrupt, existing):
        self.events.append(("download", file.name))
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(self.remote[file.name])

    def upload(self, session, metadata, path, name, record, args, interrupt):
        self.events.append(("upload", name))
        self.assertEqual(path.read_bytes()[:2] == b"\x1f\x8b", False)
        with tarfile.open(path, "r:") as archive:
            for member in archive:
                self.assertTrue(member.isfile())
                self.assertEqual(archive.extractfile(member).read(), self.original[member.name])
        self.remote[name] = path.read_bytes()

    def delete(self, session, args, interrupt, name):
        self.events.append(("delete", name))
        folder = "folder one" if name.startswith("folder one/") else "folder two"
        self.assertIn(folder + ".tar", self.remote, "Deletion happened before tar upload")
        self.remote.pop(name, None)

    def run_repack(self, *options, folders=None):
        with contextlib.redirect_stdout(self.output):
            return repack.main(["test-item", *(folders or ["folder one", "folder two"]),
                "--workdir", str(self.work), "--retry-wait", "0", "--max-retry-wait", "0",
                "--retries", "2", *options])

    def test_sequential_uncompressed_replacement_and_cleanup(self):
        self.assertEqual(self.run_repack("--delete-originals", "--yes", "--clean-local"), 0)
        self.assertEqual(self.events, [
            ("download", "folder one/a.txt"), ("download", "folder one/sub/empty"),
            ("upload", "folder one.tar"), ("delete", "folder one/a.txt"),
            ("delete", "folder one/sub/empty"), ("download", "folder two/b.bin"),
            ("upload", "folder two.tar"), ("delete", "folder two/b.bin")])
        self.assertEqual(set(self.remote), {"folder one.tar", "folder two.tar", "unselected/keep"})
        self.assertEqual(list(self.work.rglob("archive.tar")), [])
        self.assertEqual(list(self.work.rglob("a.txt")), [])
        self.assertEqual(len(list(self.work.rglob("job.json"))), 2)
        self.assertEqual(self.run_repack("--delete-originals", "--yes", "--clean-local"), 0)
        self.assertEqual(self.upload_mock.call_count, 2)
        self.assertEqual(self.delete_mock.call_count, 3)

    def test_dry_run_has_no_local_or_remote_writes(self):
        self.assertEqual(self.run_repack("--dry-run", "--delete-originals"), 0)
        self.assertFalse(self.work.exists())
        self.assertEqual(self.remote, self.original)
        self.assertEqual(self.events, [])

    def test_default_keeps_remote_originals_and_local_files(self):
        self.assertEqual(self.run_repack(), 0)
        self.assertTrue(set(self.original) <= set(self.remote))
        self.delete_mock.assert_not_called()
        self.assertEqual(len(list(self.work.rglob("archive.tar"))), 2)

    def test_upload_failure_never_deletes_or_starts_next_folder(self):
        self.upload_mock.side_effect = download.TransferError("upload failed", retryable=False)
        self.assertEqual(self.run_repack("--delete-originals", "--yes"), 1)
        self.delete_mock.assert_not_called()
        self.assertEqual(self.remote, self.original)
        self.assertFalse(any(name.startswith("folder two") for action, name in self.events))
        self.assertEqual(len(list(self.work.rglob("archive.tar"))), 1)

    def test_unconfirmed_remote_checksum_prevents_deletion_and_cleanup(self):
        def bad_upload(session, metadata, path, name, record, args, interrupt):
            self.remote[name] = b"wrong archive"
        self.upload_mock.side_effect = bad_upload
        self.assertEqual(self.run_repack("--delete-originals", "--yes", "--clean-local"), 1)
        self.delete_mock.assert_not_called()
        self.assertTrue(set(self.original) <= set(self.remote))
        self.assertEqual(len(list(self.work.rglob("archive.tar"))), 1)

    def test_deletion_failure_resumes_without_redownloading_or_reuploading(self):
        calls = 0
        def fail_second_delete(session, args, interrupt, name):
            nonlocal calls
            calls += 1
            if calls == 2:
                raise download.TransferError("delete failed", retryable=False)
            self.delete(session, args, interrupt, name)
        self.delete_mock.side_effect = fail_second_delete
        self.assertEqual(self.run_repack("--delete-originals", "--yes"), 1)
        downloaded, uploaded = self.download_mock.call_count, self.upload_mock.call_count
        self.delete_mock.side_effect = self.delete
        self.assertEqual(self.run_repack("--delete-originals", "--yes"), 0)
        self.assertEqual(self.download_mock.call_count, downloaded + 1)  # second folder only
        self.assertEqual(self.upload_mock.call_count, uploaded + 1)

    def test_changed_source_prevents_all_deletion(self):
        original_upload = self.upload
        def change_source(*args):
            original_upload(*args)
            self.remote["folder one/a.txt"] = b"changed since download"
        self.upload_mock.side_effect = change_source
        self.assertEqual(self.run_repack("--delete-originals", "--yes"), 2)
        self.delete_mock.assert_not_called()

    def test_new_source_prevents_all_deletion(self):
        original_upload = self.upload
        def new_source(*args):
            original_upload(*args)
            self.remote["folder one/new"] = b"new source"
        self.upload_mock.side_effect = new_source
        self.assertEqual(self.run_repack("--delete-originals", "--yes"), 2)
        self.delete_mock.assert_not_called()

    def test_lost_remote_tar_after_partial_deletion_stops(self):
        def fail_delete(*args):
            self.delete(*args)
            self.remote.pop("folder one.tar", None)
        self.delete_mock.side_effect = fail_delete
        self.assertEqual(self.run_repack("--delete-originals", "--yes"), 1)
        self.assertEqual(self.run_repack("--delete-originals", "--yes"), 1)
        self.assertEqual(len(list(self.work.rglob("archive.tar"))), 1)

    def test_corrupt_tar_is_rejected_before_upload(self):
        real_build = repack.build_tar
        def corrupt_tar(path, root, files, interrupt):
            (root / files[0].name).write_bytes(b"corrupt source")
            real_build(path, root, files, interrupt)
        with patch.object(repack, "build_tar", side_effect=corrupt_tar):
            self.assertEqual(self.run_repack("--delete-originals", "--yes"), 1)
        self.upload_mock.assert_not_called()
        self.delete_mock.assert_not_called()

    def test_named_folder_prefixes_are_exact(self):
        self.remote["folder one extra/keep"] = b"keep this too"
        self.assertEqual(self.run_repack("--delete-originals", "--yes", folders=["folder one/"]), 0)
        self.assertIn("folder one extra/keep", self.remote)
        self.assertIn("folder two/b.bin", self.remote)

    def test_unsafe_and_overlapping_folder_names_are_rejected(self):
        for folders in (["../escape"], ["/absolute"], ["a", "a/b"], ["a", "A"], ["a", "a.tar/sub"]):
            with self.subTest(folders=folders):
                self.assertEqual(self.run_repack(folders=folders), 2)
        self.assertFalse(self.work.exists())

    def test_missing_credentials_and_unattended_deletion_confirmation(self):
        self.session.access_key = None
        self.assertEqual(self.run_repack(), 2)
        self.session.access_key = "access"
        with patch("sys.stdin.isatty", return_value=False):
            self.assertEqual(self.run_repack("--delete-originals"), 2)
        self.assertEqual(self.events, [])

    def test_s3_delete_is_signed_exact_and_non_cascading(self):
        # Exercise the real deletion helper without issuing an HTTP request.
        from internetarchive.session import ArchiveSession
        session = ArchiveSession(config={"s3": {"access": "testaccess", "secret": "testsecret"}})
        response = MagicMock(status_code=204, headers={})
        response.__enter__.return_value = response
        args = repack.build_parser().parse_args(["test-item", "folder"])
        with patch.object(session, "send", return_value=response) as send:
            REAL_DELETE_ONE(session, args, download.Interrupt(), "folder/a #%.zip")
        request = send.call_args.args[0]
        self.assertEqual(request.method, "DELETE")
        self.assertTrue(request.url.endswith("/folder/a%20%23%25.zip"))
        self.assertEqual(request.headers["x-archive-cascade-delete"], "0")
        self.assertEqual(request.headers["x-archive-queue-derive"], "0")
        self.assertTrue(request.headers["Authorization"].startswith("LOW "))

    def test_actual_upload_helper_refuses_different_remote_tar(self):
        path = Path(self.tmp.name) / "archive.tar"
        path.write_bytes(b"local archive")
        record = {"size": path.stat().st_size, "md5": ia_common.md5_of(path)}
        args = repack.build_parser().parse_args(["test-item", "folder one"])
        metadata = {"files": [{"name": "folder one.tar", "size": "13", "md5": "f" * 32}]}
        with patch.object(repack.upload, "upload_one") as send:
            with self.assertRaises(download.UsageError):
                REAL_UPLOAD_TAR(self.session, metadata, path, "folder one.tar", record,
                                args, download.Interrupt())
            send.assert_not_called()

    def test_s3_delete_retries_transient_status_and_accepts_404(self):
        from internetarchive.session import ArchiveSession
        session = ArchiveSession(config={"s3": {"access": "testaccess", "secret": "testsecret"}})
        responses = []
        for status in (503, 404):
            response = MagicMock(status_code=status, headers={})
            response.__enter__.return_value = response
            responses.append(response)
        args = repack.build_parser().parse_args(["test-item", "folder", "--retry-wait", "0"])
        with patch.object(session, "send", side_effect=responses) as send:
            REAL_DELETE_ONE(session, args, download.Interrupt(), "folder/file")
        self.assertEqual(send.call_count, 2)


if __name__ == "__main__":
    unittest.main()
