import threading

class MoveBuffer:
    def __init__(self, max_size):
        self.buffer = []
        self.lock = threading.Lock()
        self.max_size = max_size

    def add(self, event):
        with self.lock:
            self.buffer.append(event)
            return len(self.buffer) >= self.max_size

    def flush(self):
        with self.lock:
            data = self.buffer
            self.buffer = []
            return data

    def size(self):
        with self.lock:
            return len(self.buffer)
