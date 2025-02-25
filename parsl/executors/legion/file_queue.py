from filelock import FileLock 
import json
from pathlib import Path
import time

class RobustFsQueue:
    def __init__(self, queue_dir):
        self.queue_dir = Path(queue_dir)
        self.queue_dir.mkdir(parents=True, exist_ok=True)
        self.lock_file = self.queue_dir / "queue.lock"

    def push(self, message):
        with FileLock(self.lock_file):
            msg_file = self.queue_dir / f"{time.time():.6f}.msg"
            with open(msg_file, 'w') as f:
                json.dump(message, f)
                
    
    def pop(self,timeout=None):
        try:
            with FileLock(self.lock_file, timeout=timeout):
                messages = sorted(self.queue_dir.glob("*.msg"))
                if not messages:
                    return None

                msg_file = messages[0]
                try:
                    with open(msg_file, 'r') as f:
                        message = json.load(f)
                    msg_file.unlink()
                    return message
                except(FileNotFoundError, json.JSONDecodeError):
                    return None
        except:
            return None