#
# This piece of code is written by
#    Jianing Yang <jianingy.yang@gmail.com>
# with love and passion!
#
#        H A P P Y    H A C K I N G !
#              _____               ______
#     ____====  ]OO|_n_n__][.      |    |
#    [________]_|__|________)<     |YANG|
#     oo    oo  'oo OOOO-| oo\\_   ~o~~o~
# +--+--+--+--+--+--+--+--+--+--+--+--+--+
#                             25 Sep, 2017
#
from collections import deque
from tornado import ioloop
from tornado import stack_context
from tornado.concurrent import TracebackFuture, Future
from tornado.log import gen_log

import errno
import serial
import sys


class AsyncSerial(object):

    def __init__(self, device, config, io_loop=None):
        self.io_loop = io_loop or ioloop.IOLoop.current()
        self.device = self.open_device(device, config)
        self._state = None
        self.max_write_chunk_size = 65536
        self.max_read_chunk_size = 65536
        self.read_chunk_size = self.max_read_chunk_size
        self._read_buffer = []
        self._read_callback = None
        self._write_buffer = deque()
        self._write_callback = None
        self.bytes_to_read = None
        self.delimiter_to_read = None

    def open_device(self, device, config):
        config = config.upper()
        baudrate = int(config[0:-3])
        stopbits = {
            '1': serial.STOPBITS_ONE,
            '2': serial.STOPBITS_TWO,
            '5': serial.STOPBITS_ONE_POINT_FIVE,
        }[config[-1]]
        bytesize = int(config[-2])
        parity = {
            'N': serial.PARITY_NONE,
            'E': serial.PARITY_EVEN,
            'O': serial.PARITY_ODD,
        }[config[-3]]

        return serial.Serial(device,
                             baudrate,
                             bytesize=bytesize,
                             stopbits=stopbits,
                             parity=parity,
                             exclusive=True,
                             timeout=0,
                             write_timeout=0,
                             inter_byte_timeout=0)

    def _add_io_state(self, state):
        if self._state is None:
            self._state = ioloop.IOLoop.ERROR | state
            self.io_loop.add_handler(
                self.fileno(), self._handle_events, self._state)
        elif not self._state & state:
            self._state = self._state | state
            self.io_loop.update_handler(self.fileno(), self._state)

    def _handle_events(self, fd, events):
        try:
            if events & self.io_loop.READ:
                self._handle_read()
            if events & self.io_loop.WRITE:
                self._handle_write()
            if events & self.io_loop.ERROR:
                self.error = self.get_fd_error()
                return
            state = self.io_loop.ERROR
            if self.reading():
                state |= self.io_loop.READ
            if self.writing():
                state |= self.io_loop.WRITE
            if state != self._state:
                assert self._state is not None, \
                    "shouldn't happen: _handle_events without self._state"
                self._state = state
                self.io_loop.update_handler(self.fileno(), self._state)
        except Exception:
            raise

    def fileno(self):
        return self.device.fileno()

    def _finish_read(self, buffer_return):
        if isinstance(self._read_callback, Future):
            future = self._read_callback
            self._read_callback = None
            future.set_result(buffer_return)
        elif callable(self._read_callback):
            callback = self._read_callback
            self._read_callback = None
            callback(buffer_return)

    def _drain_read_buffer(self, bytes_to_read=None):
        if bytes_to_read is None:
            bytes_to_read = len(self._read_buffer)
        if (sys.version_info > (3, 0)):
            buffer_return = bytes(self._read_buffer[:bytes_to_read])
        else:
            buffer_return = "".join(self._read_buffer[:bytes_to_read])
        self._read_buffer = self._read_buffer[bytes_to_read:]

        return buffer_return

    def _handle_read(self):
        chunk = self.read_from_fd()
        if chunk is None:
            return
        if self.bytes_to_read is None and self.delimiter_to_read is None:
            return self._finish_read(chunk)

        self._read_buffer.extend(chunk)
        if self.bytes_to_read is not None:
            if len(self._read_buffer) < self.bytes_to_read:
                return  # keep reading
            else:
                buffer_return = self._drain_read_buffer(self.bytes_to_read)
                return self._finish_read(buffer_return)

        if self.delimiter_to_read is not None:
            pos = chunk.find(self.delimiter_to_read)
            if pos < 0:
                return  # keep reading
            else:
                bytes_to_read = len(self._read_buffer) - (len(chunk) - pos)
                buffer_return = self._drain_read_buffer(bytes_to_read)
                return self._finish_read(buffer_return)

    def _handle_write(self):
        while self._write_buffer:
            try:
                data = self._write_buffer[0]
                num_bytes = self.write_to_fd(data)
                self._write_buffer.popleft()
                if num_bytes < len(data):
                    self._write_buffer.appendleft(data[num_bytes:])
            except(IOError, OSError) as e:
                if e.args[0] in (errno.EAGAIN, errno.EWOULDBLOCK):
                    break
                else:
                    gen_log.warning("Write error on %s: %s", self.fileno(), e)
                    return
        if not self._write_buffer:
            if isinstance(self._write_callback, Future):
                future = self._write_callback
                self._write_callback = None
                future.set_result(None)
            elif callable(self._write_callback):
                callback = self._write_callback
                self._write_callback = None
                callback()

    def read_from_fd(self):
        try:
            chunk = self.device.read(self.read_chunk_size)
        except (IOError, OSError) as e:
            if e.args[0] in (errno.EAGAIN, errno.EWOULDBLOCK):
                return None
            else:
                raise
        if not chunk:
            return None
        return chunk

    def read(self, callback=None):
        assert not self.reading(), 'Already reading'
        self.bytes_to_read = None
        if callback is not None:
            self._read_callback = stack_context.wrap(callback)
            future = None
        else:
            future = self._read_callback = TracebackFuture()
            future.add_done_callback(lambda f: f.exception())

        return future

    def read_until(self, delimiter, callback=None):
        assert not self.reading(), 'Already reading'
        self.bytes_to_read = None
        self.delimiter_to_read = delimiter

        if callback is not None:
            self._read_callback = stack_context.wrap(callback)
            future = None
        else:
            future = self._read_callback = TracebackFuture()
            future.add_done_callback(lambda f: f.exception())
        if delimiter not in self._read_buffer:
            self._add_io_state(self.io_loop.READ)
        else:
            bytes_to_read = self._read_buffer.index(delimiter) + 1
            self._finish_read(self._drain_read_buffer(bytes_to_read))

        return future

    def read_exact(self, num=0, callback=None):
        assert not self.reading(), 'Already reading'
        self.bytes_to_read = num

        if callback is not None:
            self._read_callback = stack_context.wrap(callback)
            future = None
        else:
            future = self._read_callback = TracebackFuture()
            future.add_done_callback(lambda f: f.exception())

        if len(self._read_buffer) < self.bytes_to_read:
            self._add_io_state(self.io_loop.READ)
        else:
            self._finish_read(self._drain_read_buffer(self.bytes_to_read))

        return future

    def reading(self):
        return bool(self._read_callback)

    def write(self, data, callback=None):
        for i in range(0, len(data), self.max_write_chunk_size):
            payload = data[i:i + self.max_write_chunk_size]
            self._write_buffer.append(payload)
        if callback is not None:
            self._write_callback = stack_context.wrap(callback)
            future = None
        else:
            future = self._write_callback = TracebackFuture()
            future.add_done_callback(lambda f: f.exception())
        self._handle_write()
        if self._write_buffer:
            self._add_io_state(self.io_loop.WRITE)
        return future

    def issue(self, data, callback=None):
        future = self.read(callback=callback)
        self.write(data)
        return future

    def write_to_fd(self, data):
        return self.device.write(data)

    def writing(self):
        return bool(self._write_buffer)

    def cancel(self):
        if isinstance(self._read_callback, Future):
            self._read_callback.cancel()
            self._read_callback = None
        if isinstance(self._write_callback, Future):
            self._write_callback.cancel()
            self._write_callback = None
