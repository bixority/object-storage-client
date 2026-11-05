Object Storage Client for macOS (Apple silicon)
===============================================

Install
-------
Drag "Object Storage Client.app" onto the Applications folder in this window.

The app contains both programs:
  osc       command-line client
  osc-gui   graphical client (the app itself)


Step 1: put "osc" on your PATH
------------------------------
Open Terminal and run:

  sudo mkdir -p /usr/local/bin
  sudo ln -s "/Applications/Object Storage Client.app/Contents/MacOS/osc" /usr/local/bin/osc

Then check it works:

  osc --version

If /usr/local/bin is not on your PATH, add it in your shell profile, or link
into another directory that is.


Step 2: approve the app on first launch
---------------------------------------
This app is not signed with an Apple Developer ID and is not notarized, so
macOS blocks it the first time you open it. This is expected. To approve it:

  1. Open "Object Storage Client" from Applications. macOS shows a message
     that the app cannot be opened. Click "Done" (or "OK").
  2. Open System Settings -> Privacy & Security.
  3. Scroll down to the Security section. You will see a message that
     "Object Storage Client" was blocked. Click "Open Anyway".
  4. Enter your password if asked, then click "Open" in the dialog.

You only need to do this once. The "osc" command-line tool may be blocked
the same way on first run; approve it in the same place, or run:

  xattr -dr com.apple.quarantine "/Applications/Object Storage Client.app"
