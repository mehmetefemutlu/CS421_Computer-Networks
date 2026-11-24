import json
import re
import socket
import sys
import threading
from pathlib import Path


class ProtocolError(Exception):
    pass


def send_header(stream, kind, fields=None):
    line = kind if fields is None else kind + " " + json.dumps(fields, ensure_ascii=True, separators=(",", ":"))
    stream.write((line + "\r\n").encode("ascii"))
    stream.flush()


def read_header(stream):
    line = stream.readline(1048577)
    if not line:
        raise EOFError("connection closed")
    if len(line) > 1048576 or not line.endswith(b"\r\n"):
        raise ProtocolError("invalid or oversized header")
    return line[:-2].decode("ascii")


def read_exact(stream, size):
    if size < 0:
        raise ProtocolError("negative payload length")
    chunks = []
    while size:
        chunk = stream.read(min(size, 65536))
        if not chunk:
            raise ProtocolError("truncated document")
        chunks.append(chunk)
        size -= len(chunk)
    return b"".join(chunks)


class Peer:
    def __init__(self, peer_id, port):
        self.peer_id = peer_id
        self.port = port
        self.path = Path(peer_id + ".txt")
        self.lock = threading.RLock()
        self.members = {peer_id: ["127.0.0.1", port]}
        self.creator = peer_id
        self.joined = False
        self.started = False
        self.pending = None
        self.local_version = self.latest_version = 0
        self.holder = peer_id
        self.stopping = threading.Event()
        self.listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            self.listener.bind(("0.0.0.0", port))
            self.listener.listen()
            self.listener.settimeout(0.5)
            self.path.write_bytes(b"")
        except Exception:
            self.listener.close()
            raise
        threading.Thread(target=self.accept_loop, daemon=True).start()

    def accept_loop(self):
        while not self.stopping.is_set():
            try:
                conn, addr = self.listener.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            threading.Thread(target=self.serve, args=(conn, addr), daemon=True).start()

    def serve(self, conn, addr):
        with conn:
            conn.settimeout(15)
            with conn.makefile("rwb") as stream:
                while not self.stopping.is_set():
                    try:
                        header = read_header(stream)
                    except EOFError:
                        break
                    except (OSError, ValueError, ProtocolError):
                        break
                    try:
                        kind, _, arguments = header.partition(" ")
                        if kind == "BCAST":
                            version, holder = arguments.split(" ")
                            version = int(version)
                            with self.lock:
                                if not self.started or holder not in self.members or version < 0:
                                    raise ProtocolError("invalid BCAST")
                                if version > self.latest_version:
                                    self.latest_version, self.holder = version, holder
                            stream.write(("OK %d\r\n" % version).encode("ascii"))
                            stream.flush()
                        elif kind == "FETCH":
                            fields = json.loads(arguments)
                            with self.lock:
                                if not self.started or fields["version"] != self.local_version:
                                    raise ProtocolError("requested version unavailable")
                                data = self.path.read_bytes()
                                version = self.local_version
                            stream.write(("DATA %d %d\r\n" % (version, len(data))).encode("ascii"))
                            stream.write(data)
                            stream.flush()
                        else:
                            result = self.control(kind, json.loads(arguments), addr[0])
                            send_header(stream, "OK", result)
                    except (KeyError, TypeError, ValueError, ProtocolError, OSError, EOFError) as exc:
                        try:
                            send_header(stream, "ERROR", {"message": str(exc)})
                        except OSError:
                            break

    def request(self, address, kind, fields):
        with socket.create_connection(tuple(address), timeout=15) as conn:
            with conn.makefile("rwb") as stream:
                send_header(stream, kind, fields)
                header = read_header(stream)
                response, _, body = header.partition(" ")
                if response == "ERROR":
                    raise ProtocolError(json.loads(body)["message"])
                if response != "OK":
                    raise ProtocolError("unexpected response: " + header)
                return json.loads(body)

    def snapshot(self):
        return [[pid, address[0], address[1]] for pid, address in sorted(self.members.items())]

    @staticmethod
    def parse_members(rows):
        result = {}
        for pid, host, port in rows:
            if not isinstance(pid, str) or not re.fullmatch(r"[A-Za-z0-9]+", pid):
                raise ProtocolError("invalid PeerID")
            if pid in result or not isinstance(host, str) or not isinstance(port, int) or not 1 <= port <= 65535:
                raise ProtocolError("invalid membership")
            result[pid] = [host, port]
        if rows != [[pid, *addr] for pid, addr in sorted(result.items())]:
            raise ProtocolError("membership must be PeerID-sorted")
        return result

    def control(self, kind, fields, source_ip):
        if kind == "JOIN":
            return self.admit(fields, source_ip)
        with self.lock:
            if kind == "MEMBERS_PREPARE":
                if self.started or self.pending:
                    raise ProtocolError("session started or membership busy")
                members = self.parse_members(fields["members"])
                if self.peer_id not in members or fields["creator"] not in members:
                    raise ProtocolError("incomplete membership")
                if self.joined and (fields["previous"] != self.snapshot() or fields["creator"] != self.creator):
                    raise ProtocolError("membership disagreement")
                self.pending = ("MEMBERS", fields)
            elif kind == "MEMBERS_COMMIT":
                if self.pending != ("MEMBERS", fields):
                    raise ProtocolError("membership not prepared")
                self.members = self.parse_members(fields["members"])
                self.creator = fields["creator"]
                self.holder = self.creator
                self.joined = True
                self.pending = None
            elif kind == "ABORT":
                if self.pending and self.pending[1] == fields:
                    self.pending = None
            elif kind == "START_PREPARE":
                if not self.joined or self.started or self.pending:
                    raise ProtocolError("session started or busy")
                if fields["creator"] != self.creator or fields["members"] != self.snapshot() or len(self.members) < 2:
                    raise ProtocolError("membership disagreement")
                self.pending = ("START", fields)
            elif kind == "START_COMMIT":
                if self.pending != ("START", fields):
                    raise ProtocolError("START not prepared")
                self.started = True
                self.pending = None
            else:
                raise ProtocolError("unknown message type")
        return {"members": self.snapshot(), "creator": self.creator}

    def admit(self, fields, source_ip):
        pid, port = fields["id"], fields["port"]
        if not isinstance(pid, str) or not re.fullmatch(r"[A-Za-z0-9]+", pid):
            raise ProtocolError("invalid PeerID")
        if not isinstance(port, int) or not 1 <= port <= 65535:
            raise ProtocolError("invalid port")
        with self.lock:
            if not self.joined or self.started or self.pending:
                raise ProtocolError("session started or busy")
            if pid in self.members:
                raise ProtocolError("duplicate PeerID")
            members = dict(self.members)
            members[pid] = [source_ip, port]
            members[self.peer_id] = [fields["contact_ip"], self.port]
            proposed = {"previous": self.snapshot(), "members": [[p, *a] for p, a in sorted(members.items())], "creator": self.creator}
            self.pending = ("MEMBERS", proposed)
        prepared = []
        try:
            for other, address in sorted(members.items()):
                if other != self.peer_id:
                    reply = self.request(address, "MEMBERS_PREPARE", proposed)
                    if reply["creator"] != self.creator and other != pid:
                        raise ProtocolError("creator disagreement")
                    prepared.append(address)
        except Exception:
            for address in prepared:
                try:
                    self.request(address, "ABORT", proposed)
                except (OSError, ProtocolError):
                    pass
            with self.lock:
                self.pending = None
            raise
        for address in prepared:
            reply = self.request(address, "MEMBERS_COMMIT", proposed)
            if reply["members"] != proposed["members"]:
                raise ProtocolError("membership commit disagreement")
        return self.control("MEMBERS_COMMIT", proposed, source_ip)

    def join(self, host, port):
        with socket.create_connection((host, port), timeout=15) as conn:
            local_ip = conn.getsockname()[0]
            contact_ip = conn.getpeername()[0]
        with self.lock:
            self.members[self.peer_id] = [local_ip, self.port]
        reply = self.request([host, port], "JOIN", {"id": self.peer_id, "port": self.port, "contact_ip": contact_ip})
        with self.lock:
            if not self.joined or self.snapshot() != reply["members"] or self.creator != reply["creator"]:
                raise ProtocolError("join not committed")

    def start(self):
        with self.lock:
            if self.peer_id != self.creator:
                raise ProtocolError("only the creator may START")
            if self.started or self.pending:
                raise ProtocolError("session already started or busy")
            if len(self.members) < 2:
                raise ProtocolError("START requires at least two peers")
            fields = {"creator": self.creator, "members": self.snapshot()}
            addresses = [a for p, a in sorted(self.members.items()) if p != self.peer_id]
            self.pending = ("START", fields)
        prepared = []
        try:
            for address in addresses:
                reply = self.request(address, "START_PREPARE", fields)
                if reply["members"] != fields["members"] or reply["creator"] != self.creator:
                    raise ProtocolError("membership disagreement")
                prepared.append(address)
        except Exception:
            for address in prepared:
                try:
                    self.request(address, "ABORT", fields)
                except (OSError, ProtocolError):
                    pass
            with self.lock:
                self.pending = None
            raise
        for address in addresses:
            reply = self.request(address, "START_COMMIT", fields)
            if reply["members"] != fields["members"]:
                raise ProtocolError("START commit disagreement")
        self.control("START_COMMIT", fields, "")
        return "OK START members=" + ",".join(row[0] for row in fields["members"])

    def edit(self, kind, arguments):
        with self.lock:
            if not self.started:
                raise ProtocolError("NOT_STARTED")
            if self.local_version != self.latest_version:
                return "VERSION_CONFLICT local=%d latest=%d; run UPDT" % (self.local_version, self.latest_version)
            data = self.path.read_bytes()
            lines = [line + "\n" for line in data.decode("ascii").split("\n")[:-1]]
            if kind == "WRTE":
                number, separator, text = arguments.partition(" ")
                if not re.fullmatch(r"-?[0-9]+", number):
                    raise ProtocolError("INVALID_LINE")
                index = int(number)
                if index < 0 or index >= len(lines):
                    raise ProtocolError("INVALID_LINE")
                if not separator:
                    raise ProtocolError("INVALID_TEXT")
            else:
                text = arguments
            if not text or "\r" in text or "\n" in text:
                raise ProtocolError("INVALID_TEXT")
            try:
                text.encode("ascii")
            except UnicodeEncodeError:
                raise ProtocolError("INVALID_TEXT: US-ASCII required")
            if kind == "APND":
                new_data = data + text.encode("ascii") + b"\n"
            else:
                lines[index] = text + "\n"
                new_data = "".join(lines).encode("ascii")
            self.path.write_bytes(new_data)
            self.local_version += 1
            self.latest_version = self.local_version
            self.holder = self.peer_id
            version = self.local_version
            addresses = [a for p, a in sorted(self.members.items()) if p != self.peer_id]
        for address in addresses:
            with socket.create_connection(tuple(address), timeout=15) as conn:
                with conn.makefile("rwb") as stream:
                    stream.write(("BCAST %d %s\r\n" % (version, self.peer_id)).encode("ascii"))
                    stream.flush()
                    response = read_header(stream)
                    if response != "OK %d" % version:
                        raise ProtocolError("BCAST acknowledgement failed: " + response)
        return "OK %s version=%d acknowledgements=%d" % (kind, version, len(addresses))

    def update(self):
        with self.lock:
            if not self.started:
                raise ProtocolError("NOT_STARTED")
            if self.local_version == self.latest_version:
                return "UNCHANGED version=%d" % self.local_version
            version, holder = self.latest_version, self.holder
            address = self.members[holder]
        with socket.create_connection(tuple(address), timeout=15) as conn:
            with conn.makefile("rwb") as stream:
                send_header(stream, "FETCH", {"version": version})
                header = read_header(stream)
                parts = header.split(" ")
                if len(parts) != 3 or parts[0] != "DATA" or int(parts[1]) != version:
                    raise ProtocolError("invalid document response: " + header)
                data = read_exact(stream, int(parts[2]))
        data.decode("ascii")
        if b"\r" in data or (data and not data.endswith(b"\n")):
            raise ProtocolError("invalid document line endings")
        with self.lock:
            self.path.write_bytes(data)
            self.local_version = version
        return "OK UPDT version=%d bytes=%d holder=%s" % (version, len(data), holder)

    def command(self, line):
        kind, separator, arguments = line.partition(" ")
        if kind in ("APND", "WRTE"):
            return self.edit(kind, arguments if separator else "")
        if separator:
            raise ProtocolError("unexpected arguments")
        if kind == "START":
            return self.start()
        if kind == "UPDT":
            return self.update()
        if kind == "READ":
            with self.lock:
                return "READ local=%d latest=%d holder=%s\nBEGIN DOCUMENT\n%sEND DOCUMENT" % (
                    self.local_version, self.latest_version, self.holder, self.path.read_bytes().decode("ascii"))
        if kind == "EXIT":
            self.close()
            return "OK EXIT"
        raise ProtocolError("unknown command")

    def close(self):
        self.stopping.set()
        self.listener.close()


def main(argv):
    if len(argv) not in (3, 5):
        print("Usage: python3 TextEditor.py <PeerID> <ListenPort> [<PeerAddr> <PeerPort>]", flush=True)
        return 1
    peer = None
    try:
        if not re.fullmatch(r"[A-Za-z0-9]+", argv[1]):
            raise ValueError("PeerID must contain ASCII letters and digits only")
        port = int(argv[2])
        if not 1 <= port <= 65535:
            raise ValueError("port must be in 1..65535")
        if len(argv) == 5 and not 1 <= int(argv[4]) <= 65535:
            raise ValueError("peer port must be in 1..65535")
        peer = Peer(argv[1], port)
        if len(argv) == 5:
            peer.join(argv[3], int(argv[4]))
        else:
            peer.joined = True
        print("READY %s port=%d creator=%s members=%s" % (
            peer.peer_id, port, peer.creator, ",".join(row[0] for row in peer.snapshot())), flush=True)
        for raw in sys.stdin:
            line = raw[:-1] if raw.endswith("\n") else raw
            try:
                result = peer.command(line)
                print(result, flush=True)
            except (ProtocolError, OSError, ValueError, EOFError) as exc:
                print("ERROR " + str(exc), flush=True)
            if peer.stopping.is_set():
                break
        return 0
    except (ProtocolError, OSError, ValueError, EOFError) as exc:
        print("ERROR " + str(exc), flush=True)
        return 1
    except KeyboardInterrupt:
        return 0
    finally:
        if peer:
            peer.close()


if __name__ == "__main__":
    sys.exit(main(sys.argv))
