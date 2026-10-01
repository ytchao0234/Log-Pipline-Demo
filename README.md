# Log Pipeline Demo：Nginx → Kafka → Python → Elasticsearch ＋ Prometheus／Grafana 監控

用一個 `docker compose up` 跑起一套小型的「日誌收集與可觀測性」系統，涵蓋職缺提到的 Nginx、Kafka、Python、Elasticsearch、Prometheus、Grafana。

## 架構

```
資料流
loadgen ──HTTP──▶ Nginx ──JSON access log──▶ shipper (Python)
                                                 │ produce
                                                 ▼
                                    Kafka topic: nginx-access (3 partitions)
                                                 │ consume (group: log-indexer)
                                                 ▼
                                         indexer (Python)：解析、加工
                                                 │ bulk
                                                 ▼
                                   Elasticsearch：nginx-access-YYYY.MM.DD ──▶ Kibana

監控流
shipper /metrics、indexer /metrics、nginx-exporter、kafka-exporter、es-exporter
                                  ──▶ Prometheus（含告警規則）──▶ Grafana 儀表板
```

| 元件 | 角色 | 網址 |
|---|---|---|
| Nginx | 模擬電商 API，輸出 JSON 格式 access log | http://localhost:8080 |
| loadgen | 每秒約 5 個請求，混合 2xx／4xx／5xx | — |
| shipper | 自寫的 tail -F，把每行 log 送進 Kafka | — |
| Kafka | 訊息佇列，緩衝上下游速度差異（KRaft 模式，不需 ZooKeeper） | — |
| indexer | 消費、解析、補欄位（狀態碼分類、裝置類型），批次寫入 ES | — |
| Elasticsearch | 儲存與搜尋日誌 | http://localhost:9200 |
| Kibana | 查詢日誌 | http://localhost:5601 |
| Prometheus | 抓取所有指標、評估告警 | http://localhost:9090 |
| Grafana | 監控儀表板（帳號 admin／admin） | http://localhost:3000 |

## 需求

- Docker Desktop（或 Docker Engine ＋ Compose v2）
- 分配給 Docker 至少 **6 GB 記憶體**（Elasticsearch 與 Kibana 比較吃記憶體）
- 本機 8080、9200、5601、9090、3000 埠沒有被佔用

## 啟動

```bash
docker compose up -d --build
docker compose ps          # 等所有服務都是 running / healthy，約 1～2 分鐘
```

## 逐段驗證（建議每一步都自己跑一次，面試時才講得出細節）

**1. Nginx 有在寫 log**
```bash
curl -i http://localhost:8080/api/products
docker compose exec nginx tail -n 3 /var/log/nginx/app/access.json
```

**2. 訊息有進 Kafka**
```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic nginx-access
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic nginx-access --max-messages 3
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group log-indexer
```
最後一個指令的 `LAG` 欄位就是還沒被處理的訊息數。

**3. 資料有進 Elasticsearch**
```bash
curl "localhost:9200/_cat/indices/nginx-access-*?v"
curl -s "localhost:9200/nginx-access-*/_search?q=status:500&size=1&pretty"

# 聚合：各狀態碼分類的數量
curl -s -H 'Content-Type: application/json' "localhost:9200/nginx-access-*/_search?size=0&pretty" -d '
{ "aggs": { "by_status": { "terms": { "field": "status_class" } } } }'
```

**4. Kibana 查日誌**：開 http://localhost:5601 → Discover → 建立 Data view，index pattern 填 `nginx-access-*`、時間欄位選 `@timestamp`。試試搜尋 `status >= 500` 或 `device : "mobile"`。

**5. Prometheus**：開 http://localhost:9090 → Status → Targets，六個 job 都應該是 UP。Alerts 頁面可以看到三條告警規則。

**6. Grafana**：開 http://localhost:3000 → Dashboards → **Log Pipeline 監控**。

## 三個值得做的實驗（也是面試時最好的故事）

**實驗 A：下游掛掉，資料不會掉**
```bash
docker compose stop indexer
# 看 Grafana 的「Kafka Consumer Lag」持續上升，約 1 分多鐘後 Prometheus 的 KafkaConsumerLagHigh 告警觸發
docker compose start indexer
# lag 很快降回 0，ES 文件數追上
```
重點：Kafka 把資料留住，indexer 恢復後從上次 commit 的 offset 繼續處理。這就是在系統間放 message queue 的意義。

**實驗 B：Elasticsearch 掛掉**
```bash
docker compose stop elasticsearch
docker compose logs -f indexer     # 看到指數退避重試
docker compose start elasticsearch
```
因為 indexer 是「寫入成功才 commit offset」，ES 恢復後資料會補齊。

**實驗 C：流量暴增**
```bash
docker compose up -d --scale loadgen=5
```
觀察請求量、ES bulk 延遲、lag 的變化。再試試 `docker compose up -d --scale indexer=3`：因為 topic 有 3 個 partition，最多可以有 3 個 indexer 平行消費（這就是 consumer group 的水平擴展）。

## 要放到簡報第 8 頁的東西

1. 跑完實驗 A 後，截一張 Grafana 儀表板（最好看得到 lag 上升再下降的曲線）
2. 程式推到 GitHub，連結放在簡報上
3. 記下一兩個你實際踩到的坑和解法

## 面試可能被問的問題

- **為什麼 Nginx 和 Elasticsearch 中間要放 Kafka？** 解耦上下游、緩衝流量尖峰、下游故障時不遺失資料、可以有多個消費者（例如另一個服務也想即時讀 log）。
- **你怎麼確保資料不遺失？** Producer 用 `acks=all`；consumer 關閉自動 commit，寫入 ES 成功才 commit（at-least-once）。代價是故障時可能重複寫入，可以用文件 ID 去重來改善。
- **shipper 有什麼限制？** 重啟時從檔尾開始讀，停機期間的 log 會漏掉。Filebeat 用 registry 記錄讀取位置來解決，正式環境會直接用 Filebeat 或 Fluent Bit。
- **為什麼 index 依日期切？** 方便用 ILM 或排程直接刪除舊 index，比 delete-by-query 有效率很多。
- **Prometheus 是 pull 還是 push？** Pull，Prometheus 定期到各服務的 `/metrics` 抓取。短暫的批次工作才會用 Pushgateway。
- **Counter 和 Gauge 差在哪？** Counter 只增不減，要搭配 `rate()` 看速率；Gauge 可增可減，例如目前連線數。

## 常見問題排除

- **Elasticsearch 一直重啟（Linux）**：`sudo sysctl -w vm.max_map_count=262144`
- **服務被 OOM kill／電腦很卡**：調高 Docker Desktop 的記憶體，或先 `docker compose stop kibana`
- **Apple Silicon 拉映像失敗**：在該服務加上 `platform: linux/amd64`
- **看服務 log**：`docker compose logs -f <服務名稱>`

## 關閉

```bash
docker compose down        # 停止
docker compose down -v     # 停止並刪除資料（ES 資料、log）
```
