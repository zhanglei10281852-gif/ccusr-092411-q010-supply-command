"""HTTP 服务启动入口。

用法：
    python3 -m supply_command.serve --host 0.0.0.0 --port 8080 --db data/supply.db

不传 --db 时使用内存存储（进程退出即清空，适合演示）。
"""
from __future__ import annotations

import argparse
import signal
import sys

from .api import ApiContext, create_app
from http.server import ThreadingHTTPServer


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="多业态保供指挥后端")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--db", default=":memory:", help="SQLite 文件路径，默认内存")
    args = parser.parse_args(argv)

    ctx = ApiContext(args.db)
    server = ThreadingHTTPServer((args.host, args.port), create_app(ctx))

    def shutdown(_signum, _frame) -> None:
        server.shutdown()
        ctx.store.close()
        sys.exit(0)

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    print(f"保供指挥后端已启动：http://{args.host}:{args.port}（存储：{args.db}）")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        shutdown(None, None)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
