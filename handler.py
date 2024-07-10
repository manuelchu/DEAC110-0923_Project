import sys
import logging


class DualOutputHandler(logging.Handler):
    def __init__(self, filename):
        super().__init__()
        self.console_handler = logging.StreamHandler(sys.stdout)
        self.file_handler = logging.FileHandler(filename, mode='w')

    def emit(self, record):
        self.console_handler.emit(record)
        self.file_handler.emit(record)