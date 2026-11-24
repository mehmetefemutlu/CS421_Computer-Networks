# CS421 PA1 - TextEditor

TextEditor lets several peers edit their own copies of a shared text file over
TCP. Each peer runs the same program and keeps its document in PeerID.txt.
It needs Python 3.7 or later and uses only the standard library.

## Running the program

Open a terminal in the project folder and start the first peer:

python3 TextEditor.py peerA 60000

In a second terminal, join that peer:

python3 TextEditor.py peerB 60001 127.0.0.1 60000

More peers can join through anyone already in the group. For example, open a
third terminal and join peerC through peerB:

python3 TextEditor.py peerC 60002 127.0.0.1 60001

On Windows, use python if python3 is unavailable. For different machines,
replace 127.0.0.1 with the contact peer's reachable IPv4 address and allow its
listening port through the firewall. The program listens on all IPv4 interfaces
and gets the joining peer's IP address from its TCP connection.

Use a different ID and port for each peer. IDs can contain ASCII letters and
digits, and uppercase and lowercase letters count as different characters.
Starting a peer clears any existing PeerID.txt file in that folder.

Once every peer has printed READY, enter START in the first peer's terminal.
At least two peers must be present. After START, no more peers can join.

## Commands

Enter commands one at a time across the whole group. Wait for each result before
entering the next command, even if you switch terminals.

START: Starts the session. Only the peer that created the group can use it.

APND Hello World: Adds Hello World as a new line.

WRTE 0 New text: Replaces line 0 with New text. Line numbers start at 0.

UPDT: Downloads the latest document if the local copy is out of date.

READ: Shows the local document, both versions, and the latest version's holder.

EXIT: Closes the peer.


An edit changes only the editing peer's file. Everyone else receives a BCAST
notification with the new version and its holder. Their files stay as they were
until they run UPDT.

For example, after A enters APND Hello, B's READ still shows an empty file at
local version 0, but the latest known version is 1. If B tries to edit, it gets
VERSION_CONFLICT local=0 latest=1. B must enter UPDT and then enter the edit
again. If B is already up to date, UPDT returns UNCHANGED without downloading
anything.

APND and WRTE need nonempty ASCII text. Spaces in the text, including leading
and trailing spaces, are kept. CR and LF cannot appear inside the text. An
invalid line number returns INVALID_LINE. A rejected command leaves the file
and version unchanged. Every stored line ends with LF, including the last line.

## How it works

Each peer has a listener thread and a worker thread for each incoming connection.
The main thread reads console input, so messages can arrive while the user is
typing. A lock protects shared state and is released before waiting for network
responses. Peers open direct connections to one another as needed.

The peer contacted during JOIN checks the new ID and sends the proposed member
list to everyone. The list is sorted by PeerID using ASCII order. Each peer
checks and acknowledges it before the contact commits the change. This also
works when the contact is not the creator. Duplicate IDs are rejected.

START uses a prepare step followed by a commit step. Every peer must agree on
the member list, and the creator waits for all acknowledgements before reporting
success. The member list stays fixed for the rest of the session.

Each peer tracks its local version, the latest known version, and who holds that
latest version. Both versions start at 0, with the creator recorded as holder.
Before editing, the program compares its stored versions without polling other
peers. A valid edit increments both versions, records the editor as holder, and
sends BCAST to everyone else. The command finishes after all acknowledgements.
UPDT fetches the document directly from the recorded holder without creating a
new version. READ only uses local information.

## Network messages

All messages use US-ASCII. Headers end with CRLF (\r\n). Most messages have
the form TYPE JSON object\r\n, with JSON kept on one line and control
characters escaped. BCAST and its reply use the assignment's exact format.

JOIN: id, port, contact_ip: the joining peer's ID, listening port, and the address used to reach the contact. Its own IP comes from the TCP connection.

MEMBERS_PREPARE: previous, members, creator: the old list, proposed list, and creator ID. Each member is [ID, IPv4, port]. Existing peers check the old list; a new peer accepts its first list.

MEMBERS_COMMIT: The same object as MEMBERS_PREPARE. Installs the agreed list and admits the new peer.

ABORT: The same object as the preparation being cancelled. Clears it if it matches.

START_PREPARE: creator, members: checks the creator and list, requires at least two peers, and prepares the session to start.

START_COMMIT: The same object as START_PREPARE. Starts the prepared session.

OK with JSON: members, creator: acknowledges JOIN, membership, ABORT, and START messages.

ERROR: message: explains why a request failed.

BCAST N ID\r\n: Announces version N and holder ID. Contains no document text.

OK N\r\n: Acknowledges BCAST for version N.

FETCH: version: asks the holder for the full document at that version.

DATA N L\r\npayload: Returns version N and byte length L, followed by exactly L document bytes.


There is no extra separator after a DATA payload. The receiver uses the byte
count to find its end, then checks the version, ASCII encoding, and LF endings
before replacing the file. Buffered header reads and an exact-length read loop
handle messages split across TCP reads or combined in one read. A connection can
carry several requests. Headers are limited to 1 MiB, and socket I/O has a
15-second timeout. Outgoing connections close after the reply; incoming workers
stop when the connection closes or times out. Responses are checked before a
command is reported as successful.

These are the main exchanges:

- C joins through B: C sends JOIN to B. B sends MEMBERS_PREPARE to A and C,
  waits for their OK replies, then sends MEMBERS_COMMIT to both. After their
  acknowledgements, B commits locally and replies to C. Everyone has the same
  list. A competing JOIN or START is rejected while preparation is pending.
- A starts the session: A sends START_PREPARE to every other peer and checks
  their replies. It sends START_COMMIT, waits for all replies, commits locally,
  and reports success. Edits are rejected until START is committed. No edit
  should be entered while this exchange is running.
- A edits: A updates its file and sends BCAST 1 A\r\n directly to each
  other peer. Each replies OK 1\r\n after recording the version and holder.
  Older notifications do not reduce the latest known version.
- B updates: B sends FETCH {"version":1}\r\n to A. For a document containing
  Hello and a newline, A replies DATA 1 6\r\nHello\n. B reads the six bytes
  and installs version 1.

As required by the assignment, peers are assumed to stay reachable and commands
must be issued sequentially. Concurrent edits are not supported. Preparation
failures are cancelled with ABORT; recovery from failures during commit or
editing is not implemented. There is no coordinator after START.
