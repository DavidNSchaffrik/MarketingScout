"""
monitor.py — dashboard API server for BHW scraper
Run with: py src/monitor.py
Then open dashboard.html in your browser.
"""

from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
import json
import psycopg
from dotenv import load_dotenv
import os
import logging
import traceback
from datetime import datetime

load_dotenv(override=False)

PORT = 5055

# ---------------------------------------------------------------------------
# Logging setup — writes to both console and monitor.log
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("monitor.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("monitor")


# ---------------------------------------------------------------------------
# DB connection
# ---------------------------------------------------------------------------

def get_conn(params: dict):
    host     = params.get("host", ["localhost"])[0]
    port     = int(params.get("port", ["5432"])[0])
    dbname   = params.get("db",   [os.getenv("PG_DATABASE", "")])[0]
    user     = params.get("user", [os.getenv("PG_USER", "")])[0]
    password = params.get("pass", [os.getenv("PG_PASSWORD", "")])[0]

    log.debug(f"Connecting to DB: host={host} port={port} db={dbname} user={user}")
    try:
        conn = psycopg.connect(
            host=host, port=port, dbname=dbname, user=user, password=password,
        )
        log.debug("DB connection established.")
        return conn
    except Exception as e:
        log.error(f"DB connection FAILED: {e}")
        raise


# ---------------------------------------------------------------------------
# Dashboard queries
# ---------------------------------------------------------------------------

def _query(cur, label: str, sql: str, params=None):
    """Run a single query with logging. Returns the cursor after execute."""
    log.debug(f"Query [{label}]: {sql.strip()[:120]}")
    try:
        cur.execute(sql, params or ())
        return cur
    except Exception as e:
        log.error(f"Query [{label}] FAILED: {e}\n{traceback.format_exc()}")
        raise


def get_dashboard_data(conn) -> dict:
    log.info("Building dashboard data...")
    data = {
        "stats": {},
        "workers": [],
        "queue_breakdown": [],
        "recent": [],
    }

    with conn.cursor() as cur:

        _query(cur, "total_posts", "SELECT COUNT(*) FROM item")
        data["stats"]["total_posts"] = cur.fetchone()[0]
        log.debug(f"total_posts={data['stats']['total_posts']}")

        _query(cur, "total_threads", "SELECT COUNT(*) FROM container")
        data["stats"]["total_threads"] = cur.fetchone()[0]
        log.debug(f"total_threads={data['stats']['total_threads']}")

        _query(cur, "queue_pending", "SELECT COUNT(*) FROM crawl_queue WHERE status = 'pending'")
        data["stats"]["queue_pending"] = cur.fetchone()[0]
        log.debug(f"queue_pending={data['stats']['queue_pending']}")

        _query(cur, "queue_done", "SELECT COUNT(*) FROM crawl_queue WHERE status = 'done'")
        data["stats"]["queue_done"] = cur.fetchone()[0]
        log.debug(f"queue_done={data['stats']['queue_done']}")

        _query(cur, "total_quotes", "SELECT COUNT(*) FROM post_quote")
        data["stats"]["total_quotes"] = cur.fetchone()[0]
        log.debug(f"total_quotes={data['stats']['total_quotes']}")

        try:
            _query(cur, "total_errors", "SELECT COALESCE(SUM(errors), 0) FROM worker_stats")
            data["stats"]["total_errors"] = cur.fetchone()[0]
            log.debug(f"total_errors={data['stats']['total_errors']}")
        except Exception as e:
            log.warning(f"total_errors query failed (worker_stats may not exist yet): {e}")
            data["stats"]["total_errors"] = 0

        try:
            _query(cur, "worker_stats", """
                SELECT worker_id, worker_type, pages_fetched, posts_saved,
                       errors, last_url, status, updated_at
                FROM worker_stats
                ORDER BY worker_type, worker_id
            """)
            rows = cur.fetchall()
            log.debug(f"worker_stats rows returned: {len(rows)}")
            data["workers"] = [
                {
                    "worker_id": r[0],
                    "worker_type": r[1],
                    "pages_fetched": r[2],
                    "posts_saved": r[3],
                    "errors": r[4],
                    "last_url": r[5],
                    "status": r[6],
                    "updated_at": r[7].isoformat() if r[7] else None,
                }
                for r in rows
            ]
        except Exception as e:
            log.warning(f"worker_stats query failed: {e}\n{traceback.format_exc()}")
            data["workers"] = []

        try:
            _query(cur, "queue_breakdown", """
                SELECT status, COUNT(*) as count,
                       MIN(queued_at)::text as oldest
                FROM crawl_queue
                GROUP BY status
                ORDER BY count DESC
            """)
            rows = cur.fetchall()
            log.debug(f"queue_breakdown rows: {len(rows)}")
            data["queue_breakdown"] = [
                {"status": r[0], "count": r[1], "oldest": r[2]}
                for r in rows
            ]
        except Exception as e:
            log.error(f"queue_breakdown query failed: {e}\n{traceback.format_exc()}")

        try:
            _query(cur, "recent_crawled", """
                SELECT pf.url,
                       COUNT(i.item_id) as post_count,
                       MAX(pf.fetched_at)::text as fetched_at
                FROM page_fetch pf
                LEFT JOIN item i ON i.canonical_url LIKE '%' || split_part(pf.url, '/', 5) || '%'
                WHERE pf.status = 'ok'
                GROUP BY pf.url
                ORDER BY MAX(pf.fetched_at) DESC
                LIMIT 10
            """)
            rows = cur.fetchall()
            log.debug(f"recent_crawled rows: {len(rows)}")
            data["recent"] = [
                {"url": r[0], "post_count": r[1], "fetched_at": r[2]}
                for r in rows
            ]
        except Exception as e:
            log.error(f"recent_crawled query failed: {e}\n{traceback.format_exc()}")

    log.info("Dashboard data built successfully.")
    return data


# ---------------------------------------------------------------------------
# Stop signal
# ---------------------------------------------------------------------------

def send_stop_signal(conn, worker_id: int, worker_type: str):
    log.info(f"Sending stop signal: worker_id={worker_id} worker_type={worker_type}")
    try:
        with conn.cursor() as cur:
            # Short lock_timeout so we never hang waiting for a busy worker transaction
            cur.execute("SET lock_timeout = '2s'")
            cur.execute("""
                INSERT INTO stop_signals (worker_id, worker_type)
                VALUES (%s, %s)
                ON CONFLICT DO NOTHING
            """, (worker_id, worker_type))
        conn.commit()
        log.info(f"Stop signal sent successfully for {worker_type} #{worker_id}")
    except Exception as e:
        log.error(f"Failed to send stop signal: {e}\n{traceback.format_exc()}")
        raise


# ---------------------------------------------------------------------------
# HTTP handler
# ---------------------------------------------------------------------------

class Handler(BaseHTTPRequestHandler):

    def log_message(self, format, *args):
        # Route HTTP access log through our logger instead of stderr
        log.debug(f"HTTP {self.address_string()} - {format % args}")

    def do_OPTIONS(self):
        log.debug(f"OPTIONS {self.path}")
        self.send_response(200)
        self._cors()
        self.end_headers()

    def do_GET(self):
        parsed = urlparse(self.path)
        params = parse_qs(parsed.query)
        log.info(f"GET {parsed.path} params={list(params.keys())}")

        if parsed.path != "/api":
            log.warning(f"404 for path: {parsed.path}")
            self.send_response(404)
            self.end_headers()
            return

        try:
            conn = get_conn(params)
            data = get_dashboard_data(conn)
            conn.close()
            log.info("GET /api responded 200")
            self._json(200, data)
        except Exception as e:
            log.error(f"GET /api error: {e}\n{traceback.format_exc()}")
            self._json(500, {"error": str(e)})

    def do_POST(self):
        parsed = urlparse(self.path)
        params = parse_qs(parsed.query)
        log.info(f"POST {parsed.path} params={list(params.keys())}")

        if parsed.path != "/api":
            log.warning(f"404 for path: {parsed.path}")
            self.send_response(404)
            self.end_headers()
            return

        action = params.get("action", [None])[0]
        log.debug(f"POST action={action}")

        if action == "stop":
            worker_id   = int(params.get("worker_id",   [0])[0])
            worker_type = params.get("worker_type", ["worker"])[0]
            try:
                conn = get_conn(params)
                send_stop_signal(conn, worker_id, worker_type)
                conn.close()
                log.info(f"POST /api stop responded 200 for {worker_type} #{worker_id}")
                self._json(200, {"ok": True})
            except Exception as e:
                log.error(f"POST /api stop error: {e}\n{traceback.format_exc()}")
                self._json(500, {"error": str(e)})
        else:
            log.warning(f"Unknown action: {action}")
            self._json(400, {"error": "unknown action"})

    def _cors(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")

    def _json(self, code, data):
        body = json.dumps(data).encode()
        self.send_response(code)
        self._cors()
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", len(body))
        self.end_headers()
        self.wfile.write(body)


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    log.info(f"Starting monitor server on http://localhost:{PORT}")
    log.info(f"Logging to monitor.log")
    log.info(f"ENV PG_HOST={os.getenv('PG_HOST')} PG_PORT={os.getenv('PG_PORT')} PG_DATABASE={os.getenv('PG_DATABASE')} PG_USER={os.getenv('PG_USER')}")
    try:
        server = HTTPServer(("localhost", PORT), Handler)
        log.info("Server ready. Open dashboard.html in your browser.")
        server.serve_forever()
    except OSError as e:
        log.critical(f"Failed to bind to port {PORT}: {e} — is another instance already running?")
        raise