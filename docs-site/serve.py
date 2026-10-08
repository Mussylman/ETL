"""
Статический сервер портала: отдаёт docs-site/dist/. Только стандартная библиотека, без backend-логики.

    python3 docs-site/serve.py --bind 10.10.1.142 --port 8700

Маршруты /operations и /analytics отдаются как есть (200, без редиректа на слэш).
Слушать — только внутренний адрес; 0.0.0.0 не использовать (сайт содержит внутреннюю архитектуру).
"""
import argparse
import functools
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

DIST = Path(__file__).resolve().parent / "dist"
ROUTES = {"/": "/index.html", "/operations": "/operations/index.html", "/operations/": "/operations/index.html",
          "/analytics": "/analytics/index.html", "/analytics/": "/analytics/index.html"}


class Handler(SimpleHTTPRequestHandler):
    def do_GET(self):  # noqa: N802
        path = self.path.split("?", 1)[0].split("#", 1)[0]
        if path in ROUTES:
            self.path = ROUTES[path]
        return super().do_GET()

    def list_directory(self, path):  # листинг каталогов не отдаём
        self.send_error(404)

    def end_headers(self):
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Cache-Control", "no-cache")
        super().end_headers()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bind", required=True, help="внутренний адрес, например 10.10.1.142")
    ap.add_argument("--port", type=int, default=8700)
    a = ap.parse_args()
    if a.bind in ("0.0.0.0", "::"):
        raise SystemExit("отказ: портал не публикуется на все интерфейсы — укажите внутренний адрес")
    server = ThreadingHTTPServer((a.bind, a.port), functools.partial(Handler, directory=str(DIST)))
    print(f"docs-site: http://{a.bind}:{a.port}/ ← {DIST}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
