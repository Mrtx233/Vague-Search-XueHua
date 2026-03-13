import time
import random
import os
import hashlib
import logging

class SnowflakeIdGenerator:
    """雪花ID生成器 - 生成11位纯数字ID"""
    def __init__(self, worker_id: int = 1):
        self.sequence = 0
        self.last_timestamp = -1
        self.worker_id = worker_id & 0x3F
        self.epoch = 1577836800000

    def _current_timestamp(self) -> int:
        return int(time.time() * 1000)

    def _wait_next_millis(self, last_timestamp: int) -> int:
        timestamp = self._current_timestamp()
        while timestamp <= last_timestamp:
            timestamp = self._current_timestamp()
        return timestamp

    def generate(self) -> str:
        timestamp = self._current_timestamp()

        if timestamp == self.last_timestamp:
            self.sequence = (self.sequence + 1) & 0xFFF
            if self.sequence == 0:
                timestamp = self._wait_next_millis(self.last_timestamp)
        elif timestamp < self.last_timestamp:
            raise Exception("系统时间回退")
        else:
            self.sequence = 0

        self.last_timestamp = timestamp

        snowflake_id = ((timestamp - self.epoch) << 18) | (self.worker_id << 12) | self.sequence

        snowflake_str = str(snowflake_id)
        if len(snowflake_str) > 11:
            snowflake_str = snowflake_str[-11:]
        elif len(snowflake_str) < 11:
            snowflake_str = snowflake_str.zfill(11)

        return snowflake_str

def calculate_file_md5(file_path: str, chunk_size: int = 16384) -> str:
    md5_hash = hashlib.md5()
    try:
        if not os.path.exists(file_path):
            return ""
        with open(file_path, 'rb') as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                md5_hash.update(chunk)
        return md5_hash.hexdigest()
    except Exception as e:
        return ""
