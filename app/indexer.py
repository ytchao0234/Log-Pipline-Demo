"""Indexer：從 Kafka 消費日誌，清洗與加工後批次寫入 Elasticsearch。

採用「寫入 ES 成功後才手動 commit offset」，確保至少處理一次（at-least-once）：
indexer 或 ES 掛掉時，未寫入的訊息會留在 Kafka，恢復後繼續處理，不會遺失。
"""
import json
import logging
import os
import time
from datetime import datetime

from confluent_kafka import Consumer, KafkaError
from elasticsearch import Elasticsearch, helpers
from elasticsearch.exceptions import ConnectionError as ESConnectionError
from prometheus_client import Counter, Gauge, Histogram, start_http_server

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "nginx-access")
GROUP = os.getenv("KAFKA_GROUP", "log-indexer")
ES_URL = os.getenv("ES_URL", "http://elasticsearch:9200")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "200"))
METRICS_PORT = int(os.getenv("METRICS_PORT", "8000"))
INDEX_PREFIX = "nginx-access"

logging.basicConfig(level=logging.INFO, format="%(asctime)s indexer %(levelname)s %(message)s")
log = logging.getLogger()

MESSAGES_CONSUMED = Counter("pipeline_messages_consumed", "Messages consumed from Kafka")
DOCS_INDEXED = Counter("pipeline_docs_indexed", "Documents written to Elasticsearch")
PARSE_ERRORS = Counter("pipeline_parse_errors", "Messages that could not be parsed")
INDEX_ERRORS = Counter("pipeline_index_errors", "Documents rejected by Elasticsearch")
ES_RETRIES = Counter("pipeline_es_retries", "Bulk requests retried because ES was unreachable")
HTTP_REQUESTS = Counter("pipeline_http_requests", "HTTP requests seen in logs", ["status_class", "method"])
BULK_SECONDS = Histogram("pipeline_bulk_index_seconds", "Time spent on one bulk request",
                         buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5))
LAST_BATCH = Gauge("pipeline_last_batch_size", "Number of documents in the last batch")

INDEX_TEMPLATE = {
    "index_patterns": [f"{INDEX_PREFIX}-*"],
    "template": {
        "settings": {"number_of_shards": 1, "number_of_replicas": 0},  # 單節點，不需要副本
        "mappings": {
            "properties": {
                "@timestamp": {"type": "date"},
                "client_ip": {"type": "ip"},
                "method": {"type": "keyword"},
                "path": {"type": "keyword"},
                "uri": {"type": "keyword"},
                "status": {"type": "integer"},
                "status_class": {"type": "keyword"},
                "is_error": {"type": "boolean"},
                "bytes": {"type": "long"},
                "request_time_ms": {"type": "float"},
                "referer": {"type": "keyword"},
                "user_agent": {"type": "text", "fields": {"raw": {"type": "keyword", "ignore_above": 256}}},
                "device": {"type": "keyword"},
            }
        },
    },
}


def detect_device(ua: str) -> str:
    ua = ua.lower()
    if "curl" in ua or "bot" in ua:
        return "bot"
    if "iphone" in ua or "android" in ua or "mobile" in ua:
        return "mobile"
    return "desktop"


def transform(raw: bytes):
    """把 Nginx 原始 JSON 轉成適合搜尋與分析的文件；格式不對就回傳 None。"""
    try:
        r = json.loads(raw)
        status = int(r["status"])
        uri = r.get("request_uri", "")
        ua = r.get("http_user_agent", "")
        return {
            "@timestamp": r["time_iso8601"],
            "client_ip": r.get("remote_addr"),
            "method": r.get("request_method"),
            "path": uri.split("?", 1)[0],
            "uri": uri,
            "status": status,
            "status_class": f"{status // 100}xx",
            "is_error": status >= 500,
            "bytes": int(r.get("body_bytes_sent") or 0),
            "request_time_ms": float(r.get("request_time") or 0) * 1000,
            "referer": r.get("http_referer") or None,
            "user_agent": ua,
            "device": detect_device(ua),
        }
    except (ValueError, KeyError, TypeError) as e:
        log.warning("parse error: %s | %r", e, raw[:200])
        return None


def index_name(ts: str) -> str:
    # 每天一個 index，方便日後依日期刪除舊資料
    return f"{INDEX_PREFIX}-{datetime.fromisoformat(ts).strftime('%Y.%m.%d')}"


def bulk_with_retry(es, actions):
    delay = 1
    while True:
        try:
            with BULK_SECONDS.time():
                ok, errors = helpers.bulk(es, actions, raise_on_error=False)
            DOCS_INDEXED.inc(ok)
            if errors:
                INDEX_ERRORS.inc(len(errors))
                log.warning("%d docs rejected, first: %s", len(errors), errors[0])
            return
        except ESConnectionError:
            ES_RETRIES.inc()
            log.warning("Elasticsearch unreachable, retry in %ss", delay)
            time.sleep(delay)
            delay = min(delay * 2, 30)  # 指數退避，避免一直猛打


def main():
    start_http_server(METRICS_PORT)

    es = Elasticsearch(ES_URL)
    while not es.ping():
        log.info("waiting for Elasticsearch ...")
        time.sleep(2)
    es.indices.put_index_template(name=INDEX_PREFIX, **INDEX_TEMPLATE)
    log.info("index template ready")

    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,  # 手動 commit，見檔案開頭說明
    })
    consumer.subscribe([TOPIC])
    log.info("consuming topic %s as group %s", TOPIC, GROUP)

    try:
        while True:
            msgs = consumer.consume(num_messages=BATCH_SIZE, timeout=1.0)
            if not msgs:
                continue

            actions = []
            for m in msgs:
                if m.error():
                    if m.error().code() != KafkaError.UNKNOWN_TOPIC_OR_PART:
                        log.warning("kafka error: %s", m.error())
                    continue
                MESSAGES_CONSUMED.inc()
                doc = transform(m.value())
                if doc is None:
                    PARSE_ERRORS.inc()
                    continue
                HTTP_REQUESTS.labels(doc["status_class"], doc["method"]).inc()
                actions.append({"_index": index_name(doc["@timestamp"]), "_source": doc})

            LAST_BATCH.set(len(actions))
            if actions:
                bulk_with_retry(es, actions)
            consumer.commit(asynchronous=False)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
