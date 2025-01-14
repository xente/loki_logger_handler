# Compatibility for Python 2 and 3 queue module
try:
    import queue  # Python 3.x
except ImportError:
    import Queue as queue  # Python 2.7

import atexit
import json
import logging
import threading

import requests

from loki_logger_handler.formatters.logger_formatter import LoggerFormatter
from loki_logger_handler.loki_request import LokiRequest
from loki_logger_handler.stream import Stream
from loki_logger_handler.streams import Streams


class LokiLoggerHandler(logging.Handler):
    """
    A custom logging handler that sends logs to a Loki server.
    """

    def __init__(
        self,
        url,
        labels,
        label_keys=None,
        additional_headers=None,
        message_in_json_format=True,
        timeout=10,
        compressed=True,
        default_formatter=LoggerFormatter(),
        max_stream_size=0,
        enable_self_errors=False,
        enable_structured_loki_metadata=False,
        loki_metadata=None,
        loki_metadata_keys=None,
    ):
        """
        Initialize the LokiLoggerHandler object.

        Args:
            url (str): The URL of the Loki server.
            labels (dict): Default labels for the logs.
            label_keys (dict, optional): Specific log record keys to extract as labels. Defaults to None.
            additional_headers (dict, optional): Additional headers for the Loki request. Defaults to None.
            message_in_json_format (bool): Whether to format log values as JSON.
            timeout (int, optional): Timeout interval for flushing logs. Defaults to 10 seconds.
            compressed (bool, optional): Whether to compress the logs using gzip. Defaults to True.
            default_formatter (logging.Formatter, optional): Formatter for the log records. If not provided,
                LoggerFormatter or LoguruFormatter will be used.
            max_stream_size (int, optional): Max stream size in bytes to forcefully send to server instead of
                waiting for timeout. If max stream size is 0 forcefully sending by byte size is disabled.
                Defaults to 0.
            enable_self_errors (bool, optional): Set to True to show Hanlder errors on console. Default False
            enable_structured_loki_metadata (bool, optional):  Whether to include structured loki_metadata in the logs. Defaults to False. Only supported for Loki 3.0 and above
            loki_metadata (dict, optional): Default loki_metadata values. Defaults to None. Only supported for Loki 3.0 and above
            loki_metadata_keys (arrray, optional): Specific log record keys to extract as loki_metadata. Only supported for Loki 3.0 and above
        """
        super(LokiLoggerHandler, self).__init__()

        self.labels = labels
        self.label_keys = label_keys if label_keys is not None else {}
        self.timeout = timeout
        self.formatter = default_formatter

        self.enable_self_errors = enable_self_errors

        # Create a logger for self-errors if enabled
        if self.enable_self_errors:
            self.debug_logger = logging.getLogger("LokiHandlerDebug")
            self.debug_logger.setLevel(logging.ERROR)
            console_handler = logging.StreamHandler()
            self.debug_logger.addHandler(console_handler)

        self.request = LokiRequest(
            url=url, compressed=compressed, additional_headers=additional_headers or {}
        )

        self.buffer = queue.Queue()
        self.flush_thread = threading.Thread(target=self._flush)

        self.flush_event = threading.Event()

        # Set daemon for Python 2 and 3 compatibility
        self.flush_thread.daemon = True
        self.flush_thread.start()

        self.message_in_json_format = message_in_json_format
        self.max_stream_size = max_stream_size
        self._current_stream_size = 0

        # Halndler working with errors
        self.error = False
        self.enable_structured_loki_metadata = enable_structured_loki_metadata
        self.loki_metadata = loki_metadata
        self.loki_metadata_keys = (
            loki_metadata_keys if loki_metadata_keys is not None else []
        )

    def emit(self, record):
        """
        Emit a log record.

        Args:
            record (logging.LogRecord): The log record to be emitted.
        """
        try:
            formatted_record, log_loki_metadata = self.formatter.format(record)
            self._put(formatted_record, log_loki_metadata)
        except Exception as e:
            self.handle_unexpected_error(e)

    def _flush(self):
        """
        Flush the buffer by sending the logs to the Loki server.
        This function runs in a separate thread and periodically sends logs.
        """
        atexit.register(self._send)

        while True:
            # Wait until flush_event is set or timeout elapses
            self.flush_event.wait(timeout=self.timeout)
            # Reset the event for the next cycle
            self.flush_event.clear()

            # Flush the logs if buffer is not empty
            if not self.buffer.empty():
                try:
                    self._send()
                except Exception as e:
                    self.handle_unexpected_error(e)

    def _send(self):
        """
        Send the buffered logs to the Loki server.
        """
        temp_streams = {}
        self._current_stream_size = 0

        while not self.buffer.empty():
            log = self.buffer.get()
            if log.key not in temp_streams:
                stream = Stream(
                    log.labels, self.loki_metadata, self.message_in_json_format
                )
                temp_streams[log.key] = stream

            temp_streams[log.key].append_value(log.line, log.loki_metadata)

        if temp_streams:
            streams = Streams(list(temp_streams.values()))
            try:
                self.request.send(streams.serialize())
            except requests.RequestException as e:
                self.handle_unexpected_error(e)

    def write(self, message):
        """
        Write a message to the log.

        Args:
            message (str): The message to be logged.
        """
        self.emit(message.record)

    def _put(self, log_record, log_loki_metadata):
        """
        Put a log record into the buffer.

        Args:
            log_record (dict): The formatted log record.
        """
        labels = self.labels.copy()

        self.assign_labels_from_log(log_record, labels)

        if self.enable_structured_loki_metadata:
            self.extract_and_clean_metadata(log_record, log_loki_metadata)

            log_line = LogLine(labels, log_record, log_loki_metadata)
        else:
            log_line = LogLine(labels, log_record)

        self.buffer.put(log_line)

    def assign_labels_from_log(self, log_record, labels):
        """
        This method iterates over the keys specified in `self.label_keys` and checks if each key is present in the `log_record`.
        If a key is found in the `log_record`, it assigns the corresponding value from the `log_record` to the `labels` dictionary.

        Args:
            log_record (dict): The log record containing potential label keys and values.
            labels (dict): The dictionary to which the labels will be assigned.

        Returns:
            None
        """
        for key in self.label_keys:
            if key in log_record:
                labels[key] = log_record[key]

        log_line = LogLine(labels, log_record)
        log_size = log_line.size()
        self.buffer.put(log_line)
        self._current_stream_size += log_size

        if (
            self.max_stream_size > 0
            and self._current_stream_size >= self.max_stream_size
        ):
            # send event to call flush_event and then unlock it without waiting timeout
            self.flush_event.set()

    def extract_and_clean_metadata(self, log_record, log_loki_metadata):
        """
        This method iterates over the keys defined in `self.loki_metadata_keys` and checks if they are present
        in the `log_record`. If a key is found, it is added to the `log_loki_metadata` dictionary and marked
        for deletion from the `log_record`. After collecting all keys to be deleted, it removes them from the
        `log_record`.

        Args:
            log_record (dict): The original log record containing various log data.
            log_loki_metadata (dict): The dictionary where extracted metadata will be stored.

        Returns:
            None
        """
        keys_to_delete = []
        for key in self.loki_metadata_keys:
            if key in log_record:
                log_loki_metadata[key] = log_record[key]
                keys_to_delete.append(key)

        for key in keys_to_delete:
            del log_record[key]

    def handle_unexpected_error(self, e):
        """
        Handles unexpected errors by logging them and setting the error flag.

        Args:
            e (Exception): The exception that was raised.

        Returns:
            None
        """
        if self.enable_self_errors:
            self.debug_logger.error("Unexpected error: %s", e, exc_info=True)
        self.error = True


class LogLine:
    """
    Represents a single log line with associated labels.

    Attributes:
        labels (dict): Labels associated with the log line.
        key (str): A unique key generated from the labels.
        line (dict|str): The actual log line content.
    """

    def __init__(self, labels, line, loki_metadata=None):
        """
        Initialize a LogLine object.

        Args:
            labels (dict): Labels associated with the log line.
            line (dict|str): The actual log line content.
        """
        self.labels = labels
        self.key = self._key_from_labels(labels)
        self.line = line
        self.loki_metadata = loki_metadata

    def size(self):
        """
        Calculate the approximate size of the log line in bytes.

        Returns:
            int: The size of the log line in bytes.
        """
        # Convert labels to a JSON string and calculate its size
        labels_size = len(json.dumps(self.labels).encode("utf-8"))
        # Calculate the size of the line
        if isinstance(self.line, str):
            line_size = len(self.line.encode("utf-8"))
        elif isinstance(self.line, dict):
            line_size = len(json.dumps(self.line).encode("utf-8"))
        else:
            line_size = 0
        # Total size is the sum of labels size and line size
        return labels_size + line_size

    @staticmethod
    def _key_from_labels(labels):
        """
        Generate a unique key from the labels values.

        Args:
            labels (dict): Labels to generate the key from.

        Returns:
            str: A unique key generated from the labels values.
        """
        key_list = sorted(labels.values())
        return "_".join(key_list)
