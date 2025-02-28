import logging
import socket


class HostFilter(logging.Filter):
    """Filter to inject the hostname into log records."""

    def __init__(self):
        super().__init__()
        self.hostname = socket.gethostname()  # Retrieve hostname

    def filter(self, record):
        record.host = self.hostname  # Inject hostname into the record
        return True
