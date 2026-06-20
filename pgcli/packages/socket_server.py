import logging
import os
import socket
import threading
import uuid

log = logging.getLogger(__name__)

SOCKET_DIR = "/tmp/rlocal/db_socket"


class SocketServer:
    """A generic Unix-domain socket server running on a daemon thread.

    Each connection carries a single newline-terminated request line; the
    handler is called with that line and returns the raw bytes to send back,
    after which the connection is closed.
    """

    def __init__(self, handler):
        self._handler = handler
        self.sock = None
        self.path = None
        self.thread = None

    def start(self):
        os.makedirs(SOCKET_DIR, exist_ok=True)
        self.path = os.path.join(SOCKET_DIR, uuid.uuid4().hex)
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.bind(self.path)
        self.sock.listen(4)
        self.thread = threading.Thread(target=self._serve, daemon=True)
        self.thread.start()

    def stop(self):
        if self.sock is not None:
            self.sock.close()
            self.sock = None
        if self.path and os.path.exists(self.path):
            os.remove(self.path)
        self.path = None

    def _serve(self):
        sock = self.sock
        while True:
            try:
                conn, _ = sock.accept()
            except OSError:
                return  # listening socket closed -> shutdown
            with conn:
                try:
                    self._handle(conn)
                except Exception:
                    log.exception("socket server request failed")

    def _handle(self, conn):
        request = self._recv_line(conn)
        if not request:
            return
        response = self._handler(request)
        if response:
            conn.sendall(response)

    @staticmethod
    def _recv_line(conn):
        buf = b""
        while b"\n" not in buf:
            chunk = conn.recv(4096)
            if not chunk:
                break
            buf += chunk
        line, _, _ = buf.partition(b"\n")
        return line.decode().strip()
