"""Log shipper：類似 tail -F 讀取 Nginx access log，逐行送進 Kafka。

真實環境通常用 Filebeat 或 Fluent Bit 做這件事，這裡自己寫是為了理解原理。
"""
import logging
import os
import time

from confluent_kafka import Producer
from prometheus_client import Counter, start_http_server

LOG_PATH = os.getenv("LOG_PATH", "/logs/access.json")
TOPIC = os.getenv("KAFKA_TOPIC", "nginx-access")
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
METRICS_PORT = int(os.getenv("METRICS_PORT", "8001"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s shipper %(levelname)s %(message)s")
log = logging.getLogger()

LINES_READ = Counter("shipper_lines_read", "Lines read from the access log")
LINES_SHIPPED = Counter("shipper_lines_shipped", "Lines acknowledged by Kafka")
DELIVERY_ERRORS = Counter("shipper_delivery_errors", "Kafka delivery failures")


def on_delivery(err, msg):
    if err is not None:
        DELIVERY_ERRORS.inc()
        log.warning("delivery failed: %s", err)
    else:
        LINES_SHIPPED.inc()


def follow(path):
    """持續讀取檔案新增的行。沒有新資料時 yield None，讓呼叫端有機會做其他事。

    - 檔案還不存在：等待
    - 啟動時從檔尾開始讀（不重送舊資料）
    - 檔案被輪替或清空：重新開啟並從頭讀
    """
    while not os.path.exists(path):
        log.info("waiting for %s ...", path)
        time.sleep(1)

    f = open(path, "r", encoding="utf-8")
    f.seek(0, os.SEEK_END)
    inode = os.fstat(f.fileno()).st_ino
    log.info("following %s", path)
    partial = ""

    while True:
        chunk = f.readline()
        if chunk:
            partial += chunk
            if partial.endswith("\n"):  # 只送完整的一行
                yield partial.rstrip("\n")
                partial = ""
            continue

        try:
            st = os.stat(path)
            if st.st_ino != inode or st.st_size < f.tell():
                log.info("log rotated or truncated, reopening")
                f.close()
                f = open(path, "r", encoding="utf-8")
                inode = os.fstat(f.fileno()).st_ino
                partial = ""
                continue
        except FileNotFoundError:
            pass
        yield None


def main():
    start_http_server(METRICS_PORT)
    producer = Producer({
        "bootstrap.servers": BOOTSTRAP,
        "acks": "all",        # 等 broker 確認寫入才算成功
        "linger.ms": 50,      # 稍微累積再送，提高吞吐量
        "enable.idempotence": True,
    })

    for line in follow(LOG_PATH):
        if line is None:
            producer.poll(0.2)  # 閒置時順便處理 delivery callback
            continue
        if not line.strip():
            continue
        LINES_READ.inc()
        while True:
            try:
                producer.produce(TOPIC, value=line.encode("utf-8"), on_delivery=on_delivery)
                break
            except BufferError:
                producer.poll(0.5)  # 本地佇列滿了（例如 Kafka 掛掉），等一下再送
        producer.poll(0)


if __name__ == "__main__":
    main()
