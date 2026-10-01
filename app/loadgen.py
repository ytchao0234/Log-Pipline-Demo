"""流量產生器：持續對 Nginx 發送混合請求，模擬真實網站流量。"""
import os
import random
import time
import urllib.error
import urllib.request

TARGET = os.getenv("TARGET", "http://nginx")
RPS = float(os.getenv("RPS", "5"))

# (路徑, 方法, 權重)：大部分正常，少部分 404 與 500
ROUTES = [
    ("/", "GET", 25),
    ("/api/products", "GET", 30),
    ("/api/products/{id}", "GET", 20),
    ("/api/orders", "POST", 10),
    ("/api/not-exist", "GET", 10), # 404
    ("/api/error", "GET", 5),      # 500
]
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/128.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_6) Safari/17.6",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_6) Mobile/15E148",
    "curl/8.9.1",
]


def pick_route():
    path, method, _ = random.choices(ROUTES, weights=[r[2] for r in ROUTES])[0]
    return path.replace("{id}", str(random.randint(1, 500))), method


def main():
    print(f"loadgen: sending ~{RPS} req/s to {TARGET}")
    while True:
        path, method = pick_route()
        req = urllib.request.Request(
            TARGET + path,
            method=method,
            data=b"{}" if method == "POST" else None,
            headers={"User-Agent": random.choice(USER_AGENTS)},
        )
        try:
            urllib.request.urlopen(req, timeout=3).read()
        except urllib.error.HTTPError:
            pass  # 4xx / 5xx 是刻意製造的，正常
        except urllib.error.URLError as e:
            print(f"loadgen: nginx not reachable yet ({e.reason}), retrying")
            time.sleep(2)
        time.sleep(random.expovariate(RPS))  # 讓請求間隔不規則，比較像真實流量


if __name__ == "__main__":
    main()
