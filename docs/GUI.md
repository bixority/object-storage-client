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

Keys are loaded once, at launch. The toolbar shows whether encryption is on and
where the keys came from. With encryption on, every copy and move encrypts
what it writes and decrypts what it reads, as `osc cp` and `osc mv` do.

If the keys cannot be loaded, `osc-gui` prints the error and
shows it in a small window, then exits with status 1.

## Using it

The selector at the left of each panel's address bar picks the panel's
connection. It lists Local and what `osc-gui` finds on this machine. It reads
local files and variables and never asks you to log in; for Azure it also asks
`az` for a token and lists the storage accounts of each subscription:

- AWS profiles from `~/.aws/config` and `~/.aws/credentials`, and the
  `AWS_*` variables;
- gcloud configurations and Google application default credentials;
- Azure CLI subscriptions (the storage accounts of each are looked up through
  Azure while the selector shows "Searching…") and the `AZURE_STORAGE_*`
  variables.

Anything it could not use is listed under the connections as a small grey
note. "Rescan" searches again. A connection uses only its
own settings, so one panel can use another account than the other, and copies
between panels stream through the app.

Type an address into a panel's address bar and press Enter: any URL `osc`
accepts, or a local path. The scheme must match the panel's connection: Local
takes `file://`, `http(s)://` and paths, S3 `s3://`, GCS `gs://`, Azure `az://`
and its aliases; otherwise pick a matching connection. The address is always
opened as a folder. Selecting a cloud connection opens its root, which lists
the buckets (or containers) as folders; open one to browse it. Going up from a
bucket returns to that list. Copy, move, rename and delete are refused at the
list itself.

If a connection cannot authenticate, the error banner says so. For an AWS SSO
profile it has a "Log in" button: it shows a code, "Open browser" opens the
approval page, and the token is written to `~/.aws/sso/cache` like the AWS CLI
does, so both share it. For gcloud and Azure the banner shows the command to run
in a terminal (`gcloud auth application-default login`, `az login`); run it, then press
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

The cursor is the outlined row: the arrow keys move it, and Enter opens it if
it is a folder. A click moves the cursor to the clicked row. Click, Ctrl-click
and Shift-click select entries; double-click opens a folder.

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
- **Moves and deletes ask first.** Deleting at a bucket root also asks you to
  type the bucket name.
- **Copying a folder into itself is refused.** So is copying onto the folder it
  already is in.
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
- Local folders and `http(s)://` listings arrive in one page.
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
Font Licence 1.1, and Hack and emoji-icon-font under the MIT licence.
