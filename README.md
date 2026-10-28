# ia_upload

A resilient replacement for `ia upload`, with resumable downloads and a
folder-to-tar workflow for archive.org items.

## Why

Uploading anything sizeable through the `internetarchive` library tends to die
like this:

```
requests.exceptions.ReadTimeout: (ReadTimeoutError("HTTPSConnectionPool(
host='s3.us.archive.org', port=443): Read timed out. (read timeout=120)"),
'https://s3.us.archive.org/Game-maps-collection/.../rectf012.zip')
```

The 120 is hardcoded in the library, in `upload_file`:

```python
if 'timeout' not in request_kwargs:
    request_kwargs['timeout'] = 120
```

It is a *read* timeout, so it limits how long the client waits for IA's reply,
which only begins once the whole file is already on the wire. IA writes the
object to a storage node and checksums it before answering, so a large zip runs
past 120s while the transfer is working perfectly.

Two consequences worth knowing:

* This is not a bandwidth problem, and it is not fixed by chunking. `requests`
  already streams the body off disk in small blocks and never holds the file in
  memory. There is nothing left to chunk on the client side.
* Raising `--retries` does not help either. The library's retry loop only fires
  on HTTP 503 and on its "S3 overloaded" pre-check. A `ReadTimeout` is raised
  straight out and never retried, at any retry count.

The `ia` command line exposes no timeout option, so none of this can be fixed
with a flag. Hence this script.

## Install

```sh
pip install internetarchive
ia configure          # once, for your archive.org login
chmod +x ia_upload.py
chmod +x ia_download.py ia_repack.py
```

Python 3.9 or newer. No dependencies beyond `internetarchive`.
Keep `ia_common.py` alongside the scripts. The repacking workflow also uses
`ia_download.py` and `ia_upload.py` from the same directory.

## Downloads

Download an item into a directory, preserving its remote paths:

```sh
./ia_download.py Game-maps-collection ./maps/ --dry-run
./ia_download.py Game-maps-collection ./maps/ --log download.log
```

Re-run the identical command after an interruption. Completed files are
compared with IA's metadata; partial files resume with HTTP byte ranges.
Every range response is checked before appending. If the server ignores the
range, the file restarts from zero. Downloads stay in temporary `.part` files
until their size and MD5 match the metadata, then move into place atomically.
An existing file with different contents requires confirmation or `--yes`.

Public items need no credentials. Configured `ia` cookies and S3 keys are used
when available, subject to the item's access restrictions.

Select original files or match remote paths with repeatable globs:

```sh
./ia_download.py Game-maps-collection ./maps/ --original
./ia_download.py Game-maps-collection ./maps/ --glob '*.zip' --exclude 'old/*'
```

Globs match the complete remote path, case-sensitively; `*` also matches `/`.
Hidden files, empty files, IA metadata and derivatives are included unless
filtered out. Unsafe paths, symlinks and filename collisions are rejected
before starting a transfer.

The retry controls are `--retries` (12 attempts), `--retry-wait` (15 seconds),
`--max-retry-wait` (600 seconds before jitter), and `--wait-between` (0 seconds).
HTTP `Retry-After` can extend a retry delay. `--connect-timeout` defaults to
30 seconds and `--read-timeout` to 60 seconds of read inactivity; this is not
a limit on the total download duration. Progress and ETA are logged during
transfers.

State and partials live under `DESTINATION/.ia_download/`. An exclusive
destination lock prevents overlapping download runs, including runs using
different `--state` paths. `--state PATH` overrides the state file location;
the state also binds partials to the item, destination and current checksums.
Unchanged local files reuse cached verification results. Deleting the state
is safe but causes re-hashing and restarts partial downloads.

Some IA metadata files lack a size or checksum. The downloader checks whatever
metadata is available and reports the reduced verification. Without an MD5,
partial downloads restart from zero; without either size or MD5, an existing
file is reused only when unchanged from a completed download in the state.
Dry runs read item metadata and local files but write no destination, state,
lock or log files, even if `--log` is supplied.

Download exit codes are 0 for completion, 1 for transfer/local I/O failures,
2 for invalid options/paths or a refused overwrite, and 130 for interruption.

## Replace named folders with uncompressed tars

`ia_repack.py` processes only the folder paths you name, in their given order.
IA folders are filename prefixes: `maps/quake` selects `maps/quake/...`, and
does not select `maps/quake2/...`.

Preview the complete workflow:

```sh
./ia_repack.py Game-maps-collection 'folder one' 'parent/folder two' \
    --delete-originals --dry-run
```

Then run it with your configured IA write credentials:

```sh
./ia_repack.py Game-maps-collection 'folder one' 'parent/folder two' \
    --delete-originals --yes --log repack.log
```

For each folder, it:

1. Downloads and verifies every file beneath that exact prefix.
2. Builds an **uncompressed `.tar`**, then reads it back and checks every
   member against the source size and MD5.
3. Uploads the tar into the **same IA item**. `parent/folder` becomes
   `parent/folder.tar`; tar members retain their full item-relative paths.
4. Waits for IA's metadata to confirm the tar's size and MD5.
5. With `--delete-originals`, deletes only the frozen list of verified source
   files. It checks the remote tar and source snapshot before each deletion,
   disables cascade deletion, and waits for metadata to confirm removal.
6. Moves to the next named folder.

Without `--delete-originals`, it uploads the tars and retains the remote
sources. `--yes` confirms source deletion for unattended runs. An existing
remote tar with a different checksum is never overwritten. Repacking requires
a published size and MD5 for every source file. Avoid editing the selected IA
folders during a run: newly added or changed source files stop the workflow.

The default work directory is `.ia_repack_IDENTIFIER/`; use `--workdir PATH`
to put it on a disk with sufficient space. During packing, allow roughly twice
the current folder's size for the downloads and uncompressed tar. Local files
are retained by default. Add **`--clean-local`** to remove each folder's local
downloads and tar after its remote operations succeed, before starting the
next folder. Small state files and the journal remain for resuming.

After a failure or interruption, **keep the work directory** and re-run the
same command. A journal records the original source checksums, tar checksum
and accepted deletions, so partially completed deletions can resume without
downloading the folder again. A failure stops processing before the next
folder. Metadata verification uses the retry settings; if IA's metadata is
still catching up when retries run out, re-run later.

Repacking shares the downloader's retry, pacing and connection timeout
options. Tar uploads use the uploader's file-size-based response timeout.
Dry runs write nothing and need no credentials. Exit codes match the
downloader's.

## Usage

Dry run first. It reads the item but sends nothing, and needs no credentials:

```sh
./ia_upload.py Game-maps-collection ./maps/ --dry-run
```

Then the real thing:

```sh
./ia_upload.py Game-maps-collection ./maps/ --derive --log upload.log
```

If it dies, gets killed, or you Ctrl-C it, run the identical command again. It
re-reads the item, compares MD5s, and sends only what is missing. Nothing is
ever uploaded twice.

Creating a new item needs metadata on the first run, or IA will usually reject
the upload as spam:

```sh
./ia_upload.py Game-maps-collection ./maps/ \
    -m 'title:Game Maps Collection' \
    -m 'description:Community map packs, 1999-2007.' \
    -m 'mediatype:software' \
    --derive --log upload.log
```

## Coming from `ia upload`

```sh
ia upload ID DIR --checksum --verify --retries 150 --sleep 15
```

becomes

```sh
./ia_upload.py ID DIR
```

| `ia upload` | here |
|---|---|
| `--checksum` | Default. Reads the item's file list once per run rather than once per file, and caches local MD5s between runs. |
| `--verify` | Always on. Every request carries a `Content-MD5` computed from the cached hash, so IA verifies server-side without re-reading the file on each attempt. |
| `--retries 150` | `--retries 12` by default, but these retries cover timeouts and dropped connections, and back off exponentially rather than sleeping a flat interval. |
| `--sleep 15` | `--retry-wait 15` by default, doubling up to `--max-retry-wait 600`. |

The read timeout scales with file size: 600s plus 300s per GB, capped at two
hours. A 4 GB map pack gets half an hour to be acknowledged instead of 120
seconds.

## What it adds

* Read timeout scaled to file size instead of a fixed 120s.
* On a timeout, re-reads the item's file list to check whether the upload
  landed anyway before spending another full transfer on it. Timeouts on IA are
  more often a lost acknowledgement than a lost transfer.
* Retries timeouts, connection resets, 429 and 5xx, with exponential backoff
  and jitter. Stops immediately on 401/403 instead of retrying 150 times
  against a credentials problem.
* Checks every filename for what IA rejects (control characters, over-long
  names, space-padded path segments, case-insensitive collisions) before
  starting rather than four hours in.
* Confirms every uploaded file's MD5 against the item at the end.
* Progress, throughput and ETA, plus a heartbeat during long single-file
  transfers so an unattended run is distinguishable from a hung one.
* Never follows symlinks. Prompts before overwriting a file whose contents
  differ, since IA keeps the old copy and it still counts against the item.
* Redacts anything shaped like an S3 credential from logs.
* Refuses to start a second run against the same state file (e.g. an
  overlapping cron job) instead of racing it.

## Documentation

```sh
man -l ia_upload.1
```

Or install it:

```sh
mkdir -p ~/.local/share/man/man1
cp ia_upload.1 ~/.local/share/man/man1/
man ia_upload
```

Download and repacking documentation:

```sh
man -l ia_download.1
man -l ia_repack.1
```

## Tests

With `internetarchive` installed:

```sh
python3 -m unittest discover -s tests -v
```

Tests use a local HTTP server and simulated IA mutations. They do not upload
or delete anything on archive.org.

## Exit codes

| code | meaning |
|---|---|
| 0 | everything uploaded and verified, or nothing to do |
| 1 | some files failed; re-run the same command to retry just those |
| 2 | bad usage, bad identifier, missing credentials, or a filename IA will reject |
| 130 | interrupted; re-run to continue |

## Credentials

`ia configure` is the easy path. If you prefer environment variables the names
are `IA_ACCESS_KEY_ID` and `IA_SECRET_ACCESS_KEY`. Set both or neither, since
the library raises if only one is present.

## License

GPL-3.0-or-later. See [LICENSE](LICENSE).
