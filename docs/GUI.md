# Desktop browser (`osc-gui`)

`osc-gui` is a two-panel browser for every store the crate supports. Each panel
shows one folder. You copy, move, rename and delete objects and whole folders
within a panel or between the two. It is built on egui, and it does all its
storage work through the same client as `osc`: listing, copy, move, delete,
encryption and credentials behave exactly as they do there.

## Install

The browser is behind the `gui` Cargo feature, which is off by default:

```bash
export CARGO_REGISTRIES_BIXORITY_INDEX="sparse+https://codeberg.org/api/packages/bixority/cargo/"
cargo install object-storage-client --registry bixority --features gui
# or, from a checkout: cargo install --path . --features gui
```

This installs `osc` and `osc-gui`. On Linux the window needs a Wayland session;
X11 (including `ssh -X` forwarding) is not supported. Use waypipe to run it
remotely. No extra system packages are needed.

## Launch

```bash
osc-gui                                         # both panels on Local, the current directory
osc-gui --keyring keyring                       # copies encrypt and decrypt
```

Both panels start on Local. The panels' connections are chosen in the app, not
on the command line.

The options are the ones `osc` takes:

| Option                 | Environment              | Meaning                                                   |
|------------------------|--------------------------|-----------------------------------------------------------|
| `--keyring FILE`       | `OSC_KEYRING_FILE`       | Keyring file; enables encryption                          |
|                        | `OSC_KEYRING_COMMAND`    | A command that prints the keyring (instead of a file)     |
| `--require-encryption` | `OSC_REQUIRE_ENCRYPTION` | Refuse to start without a key source                      |

The key source is opened once, at launch; a keyring file then reloads itself
when it changes, while a key command is not run again. The top right of the
toolbar shows a key (🔑) when client-side encryption is on and ⊘ when it is
off; hover it to see whether the keys came from a keyring file or a key
command. Next to each connection selector, a shield (🛡️) means the endpoint
uses HTTPS and its certificate is verified, a warning sign (⚠️) means HTTPS with
certificate verification turned off for that connection, and ◌ means no TLS
(a plain `http` endpoint, or Local). An AWS profile's `endpoint_url` is read when
the connections are found, so "Rescan" picks up a change. Hover an icon for its text. With encryption on,
every copy and move encrypts what it writes and decrypts what it reads, as `osc
cp` and `osc mv` do.

If the keys cannot be loaded, `osc-gui` prints the error and
shows it in a small window, then exits with status 1.

## Using it

The selector at the left of each panel's address bar picks the panel's
connection. It lists Local and what `osc-gui` finds on this machine. It reads
local files and variables and never asks you to log in; for Azure it also asks
`az` for a token and lists the storage accounts of each subscription:

- AWS profiles from `~/.aws/config` and `~/.aws/credentials`, and the
  `AWS_*` and `S3_*` variables. A profile without credentials (only settings
  such as `region`) is left out and named in a note;
- gcloud configurations, with their own credentials file or the application
  default credentials, and the `GOOGLE_*` variables;
- Azure CLI subscriptions (the storage accounts of each are looked up through
  Azure while the selector shows "Searching…") and the
  `AZURE_STORAGE_ACCOUNT_NAME` variable.

Anything it could not use is listed under the connections as a small grey
note. A note also names an AWS profile that repeats `sso_start_url` or
`sso_region` next to `sso_session`: the AWS SDK refuses that profile, so
remove the repeated keys (they belong in the `[sso-session]` section). After
an SSO login, every panel whose profile shares that login is reloaded. An AWS
profile is refused while `AWS_ENDPOINT_URL` is set, and a panel on
it shows that error (see CONFIGURATION.md). "Rescan" searches again. A
connection uses only its own settings, so one panel can use another account than
the other, and copies between panels on different connections stream through
the app.

Type an address into a panel's address bar and press Enter or click "Go": any
URL `osc` accepts, or a local path. The scheme must match the panel's
connection: Local takes `file://`, `http(s)://` and paths, S3 `s3://`, GCS
`gs://` and `gcs://`, Azure `az://` and its aliases; otherwise pick a matching
connection. The address is always opened as a folder. Selecting a cloud
connection opens its root, which lists the buckets (or containers) as folders;
open one to browse it. Going up from a bucket returns to that list. Copy, move,
rename and delete are refused at the list itself, and so are copies and moves
into it.

Archived objects (S3 Glacier Flexible Retrieval and Deep Archive, Azure Archive
tier) show ❄, and objects being restored or rehydrated show ⌛. Both are greyed
with a hover text. Copying, moving or renaming them is skipped with that
reason; deleting them works. Inside a selected folder their state is not known,
so copying such an object fails with an "Archived" error, as it does for the S3
Intelligent-Tiering archive tiers, which are not visible in a listing. See
"Archived objects" in BEHAVIOR.md.

If a listing fails, the panel shows the error, wrapped to the panel's width,
with "Retry" on a row below it. If a connection cannot authenticate, the error
says so. For an AWS SSO profile (or a role profile that takes its credentials
from one) that row has a "Log in" button: it shows a code, "Open browser" opens
the approval page, and the token is written to `~/.aws/sso/cache` like the AWS
CLI does, so both share it; the panel then lists again. For gcloud and Azure
the row shows the command to run in a terminal
(`gcloud auth application-default login`, `az login`); run it, then press
"Rescan" in the selector.

| Key                               | Action                                         |
|-----------------------------------|------------------------------------------------|
| Up / Down                         | Move to the previous or next row and select it |
| Shift+Up / Down                   | Extend the selection from where it started     |
| Page Up / Page Down               | Move one page (the rows visible) up or down    |
| Home / End                        | Go to the first or last row                    |
| Shift+Page Up / Down / Home / End | Extend the selection the same way              |
| Enter                             | Open the folder under the cursor               |
| F5                                | Copy the selection to the other panel's folder |
| F6                                | Move the selection to the other panel's folder |
| F2                                | Rename the selected entry                      |
| Del                               | Delete the selection                           |
| Backspace                         | Up one level                                   |
| Tab                               | Switch panel                                   |
| Ctrl+A                            | Select everything listed                       |
| Esc                               | Clear the selection (or cancel a dialog)       |
| Ctrl+R                            | Refresh                                        |
| Ctrl+L                            | Edit the address                               |

The cursor is the row last moved to with the keys or clicked; it has no mark of
its own. The arrow keys move it, and Enter opens it if it is a folder. Click,
Ctrl-click and Shift-click select entries; double-click opens a folder. A ".."
row at the top of the list goes up one level on double-click, as Backspace
does; there is none on the list of buckets or at the top of the file system (or
of an `http(s)://` server). In a dialog, Enter takes the answer that cannot
lose data, and Esc cancels.

## Operations

- **Folders are handled recursively.** Copying, moving, renaming or deleting a
  folder works on every object under it, except console folder markers (see
  below). A zero-size entry that is listed but cannot be read, such as a
  console folder marker below the folder, is skipped without a failure.
- **Symlinks are shown as links and acted on as links.** A local symlink is
  listed with a link mark (🔗), never as a folder or an object, and Enter or a
  double-click does not open it. Delete removes only the link. Rename renames
  the link. Move to a local folder renames the link; if that folder is on
  another file system, it fails with "symbolic link skipped (other file
  system)". Copy, and Move to any other store, fail with "symbolic link
  skipped". Links inside a folder are skipped when the folder is copied, moved
  or renamed, and never followed. See "Listing" in BEHAVIOR.md for the limit of
  this check.
- **One operation runs at a time.** You can keep browsing while it runs. The
  status bar shows how far it got, and Cancel stops it. Objects already done
  stay done.
- **Jobs run 4 items at a time.** Objects selected for deletion on a bucket
  store are deleted in batches instead; the count then advances in bursts, and
  Cancel can leave a batch partly applied.
- **A failure does not stop the job.** At the end, a list of the failed items
  and their errors is shown, and it can be copied.
- **Existing destinations are checked first**, for up to 1,000 items: one
  question per job offers to overwrite all, skip existing or cancel. A larger
  job is not checked; you are warned that existing objects will be overwritten.
- **Moves and deletes ask first.**
- **Copying or moving a folder into itself is refused.** So is copying or
  moving onto the folder the selection already is in.
- **Empty directories can stay behind.** Moving or deleting a local folder can
  leave empty directories behind, as deleting a prefix with the client does.
- **Console-created cloud folders can stay listed.** The S3 and GCS consoles
  and MinIO make a folder as a zero-byte `name/` key, which object_store cannot
  address. After a delete, move or rename the folder (and any such subfolders)
  stays listed, though the job reports success. Remove those keys with the
  provider's tools. See "Deleting by prefix" in BEHAVIOR.md.

## Limits

- Listings load 1,000 entries per page; **Load more** fetches the next page. A
  panel holds at most 50,000 entries. Past that, a banner says the listing is
  incomplete; open a deeper folder to see the rest.
- Copying, moving or renaming folders plans at most 1,000,000 objects in all.
  A larger job fails before anything runs; select a deeper folder.
- Local folders, `http(s)://` listings and the list of buckets arrive in one
  page.
- After a job, only the panels it can have changed are listed again: a copy
  changes its destination folder, a move its source and destination, a rename
  or delete its source. A panel inside or above such a folder is refreshed and
  starts again at the first page; the other keeps its loaded pages, selection
  and cursor.
- Sizes of encrypted objects are their stored sizes, as in `osc ls`.
- Nothing is saved between runs: no settings or history. The only file written
  is the SSO token cache of a login, in `~/.aws/sso/cache`.

## Closing

Closing the window while a job runs asks first. Quitting cancels the job,
then waits up to 10 seconds for interrupted uploads to be cleaned up (aborted
multipart uploads, removed staging files) before the window closes. Ctrl-C or
SIGTERM in the terminal that started `osc-gui` does the same without asking.

## Licences

`osc-gui` bundles egui's default fonts (the `epaint_default_fonts` crate):
Ubuntu Light under the Ubuntu Font Licence 1.0, Noto Emoji under the SIL Open
Font Licence 1.1, Hack under the MIT licence and the Bitstream Vera licence,
and emoji-icon-font under the MIT licence.
