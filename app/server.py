from __future__ import annotations

import json
import mimetypes
import secrets
from http import HTTPStatus
from http.cookies import SimpleCookie
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse

from .services import TraceabilityService


BASE_DIR = Path(__file__).resolve().parent.parent
STATIC_DIR = BASE_DIR / "static"
DATA_DIR = BASE_DIR / "data"
DB_PATH = DATA_DIR / "traceability.db"
HOST = "127.0.0.1"
PORT = 8000
SESSION_COOKIE = "trace_session"


class AuthError(Exception):
    def __init__(self, status: HTTPStatus, message: str) -> None:
        super().__init__(message)
        self.status = status
        self.message = message


class SessionStore:
    def __init__(self) -> None:
        self._sessions: dict[str, int] = {}

    def create(self, user_id: int) -> str:
        token = secrets.token_urlsafe(32)
        self._sessions[token] = user_id
        return token

    def get(self, token: str | None) -> int | None:
        if not token:
            return None
        return self._sessions.get(token)

    def delete(self, token: str | None) -> None:
        if token:
            self._sessions.pop(token, None)


class TraceabilityRequestHandler(BaseHTTPRequestHandler):
    service = TraceabilityService(DB_PATH)
    sessions = SessionStore()

    def do_GET(self) -> None:  # noqa: N802
        try:
            parsed = urlparse(self.path)
            if parsed.path.startswith("/api/"):
                self._handle_api_get(parsed)
                return
            self._serve_static(parsed.path)
        except AuthError as exc:
            self._send_json(exc.status, {"error": exc.message})
        except ValueError as exc:
            self._send_json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
        except Exception as exc:  # pragma: no cover
            self._send_json(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": f"服务器异常：{exc}"})

    def do_POST(self) -> None:  # noqa: N802
        try:
            parsed = urlparse(self.path)
            payload = self._read_json()
            client_ip = self.client_address[0] if self.client_address else None

            if parsed.path == "/api/login":
                user = self.service.authenticate_user(payload.get("username", ""), payload.get("password", ""))
                token = self.sessions.create(user["id"])
                self._send_json(
                    HTTPStatus.OK,
                    {"user": user},
                    cookie=f"{SESSION_COOKIE}={token}; Path=/; HttpOnly; SameSite=Lax",
                )
                return

            if parsed.path == "/api/logout":
                self.sessions.delete(self._get_session_token())
                self._send_json(
                    HTTPStatus.OK,
                    {"ok": True},
                    cookie=f"{SESSION_COOKIE}=; Path=/; Max-Age=0; HttpOnly; SameSite=Lax",
                )
                return

            if parsed.path == "/api/devices":
                user = self._require_user("devices:write")
                result = self.service.create_device(payload, actor=user, ip_address=client_ip)
            elif parsed.path == "/api/warehouse/inbound":
                user = self._require_user("warehouse:write")
                result = self.service.record_inbound(payload, actor=user, ip_address=client_ip)
            elif parsed.path == "/api/warehouse/outbound":
                user = self._require_user("warehouse:write")
                result = self.service.record_outbound(payload, actor=user, ip_address=client_ip)
            elif parsed.path == "/api/warehouse/stocktake":
                user = self._require_user("warehouse:write")
                result = self.service.record_stocktake(payload, actor=user, ip_address=client_ip)
            elif parsed.path == "/api/clinical-uses":
                user = self._require_user("clinical:write")
                result = self.service.record_clinical_use(payload, actor=user, ip_address=client_ip)
            elif parsed.path == "/api/maintenance":
                user = self._require_user("maintenance:write")
                result = self.service.record_maintenance(payload, actor=user, ip_address=client_ip)
            elif parsed.path == "/api/procurements":
                user = self._require_user("procurement:create")
                result = self.service.create_procurement(payload, actor=user, ip_address=client_ip)
            elif parsed.path == "/api/procurements/approve":
                user = self._require_user("procurement:approve")
                result = self.service.approve_procurement(payload, actor=user, ip_address=client_ip)
            elif parsed.path == "/api/procurements/receive":
                user = self._require_user("procurement:receive")
                result = self.service.receive_procurement(payload, actor=user, ip_address=client_ip)
            elif parsed.path == "/api/recalls":
                user = self._require_user("recall:manage")
                result = self.service.create_recall_case(payload, actor=user, ip_address=client_ip)
            elif parsed.path == "/api/recalls/close":
                user = self._require_user("recall:manage")
                result = self.service.close_recall_case(payload, actor=user, ip_address=client_ip)
            elif parsed.path == "/api/scraps":
                user = self._require_user("scrap:create")
                result = self.service.create_scrap_request(payload, actor=user, ip_address=client_ip)
            elif parsed.path == "/api/scraps/approve":
                user = self._require_user("scrap:approve")
                result = self.service.approve_scrap_request(payload, actor=user, ip_address=client_ip)
            elif parsed.path == "/api/scraps/dispose":
                user = self._require_user("scrap:dispose")
                result = self.service.dispose_scrap_request(payload, actor=user, ip_address=client_ip)
            else:
                self._send_json(HTTPStatus.NOT_FOUND, {"error": "接口不存在。"})
                return
            self._send_json(HTTPStatus.CREATED, result)
        except AuthError as exc:
            self._send_json(exc.status, {"error": exc.message})
        except ValueError as exc:
            self._send_json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
        except Exception as exc:  # pragma: no cover
            self._send_json(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": f"服务器异常：{exc}"})

    def log_message(self, format: str, *args) -> None:  # noqa: A003
        return

    def _handle_api_get(self, parsed) -> None:
        query = parse_qs(parsed.query)
        client_ip = self.client_address[0] if self.client_address else None
        if parsed.path == "/api/me":
            user = self._require_user()
            self._send_json(HTTPStatus.OK, {"user": user})
            return
        if parsed.path == "/api/lookups":
            self._require_user()
            payload = self.service.get_lookups()
        elif parsed.path == "/api/dashboard":
            self._require_user("dashboard:view")
            payload = self.service.get_dashboard()
        elif parsed.path == "/api/devices":
            self._require_user("devices:view")
            payload = self.service.list_devices()
        elif parsed.path == "/api/inventory":
            self._require_user("devices:view")
            payload = self.service.list_inventory()
        elif parsed.path == "/api/maintenance":
            self._require_user("devices:view")
            payload = self.service.list_maintenance_records()
        elif parsed.path == "/api/procurements":
            self._require_user("procurement:view")
            payload = self.service.list_procurements()
        elif parsed.path == "/api/recalls":
            self._require_user("recall:view")
            payload = self.service.list_recalls()
        elif parsed.path == "/api/scraps":
            self._require_user("scrap:view")
            payload = self.service.list_scrap_requests()
        elif parsed.path == "/api/alerts":
            self._require_user("alerts:view")
            payload = self.service.get_alerts()
        elif parsed.path == "/api/traceability":
            self._require_user("trace:view")
            payload = self.service.search_traceability(query.get("keyword", [""])[0])
        elif parsed.path == "/api/audit-logs":
            self._require_user("audit:view")
            limit = int(query.get("limit", ["120"])[0])
            payload = self.service.list_audit_logs(limit=limit)
        elif parsed.path == "/api/reports/summary":
            self._require_user("reports:view")
            payload = self.service.get_report_summary()
        elif parsed.path == "/api/reports/export":
            user = self._require_user("reports:view")
            workbook = self.service.export_reports_workbook(actor=user, ip_address=client_ip)
            self._send_bytes(
                HTTPStatus.OK,
                workbook,
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                "traceability-report.xlsx",
            )
            return
        else:
            self._send_json(HTTPStatus.NOT_FOUND, {"error": "接口不存在。"})
            return
        self._send_json(HTTPStatus.OK, payload)

    def _read_json(self):
        content_length = int(self.headers.get("Content-Length", 0))
        if content_length <= 0:
            return {}
        raw = self.rfile.read(content_length)
        if not raw:
            return {}
        return json.loads(raw.decode("utf-8"))

    def _send_json(self, status: HTTPStatus, payload, cookie: str | None = None) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status.value)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        if cookie:
            self.send_header("Set-Cookie", cookie)
        self.end_headers()
        self.wfile.write(body)

    def _send_bytes(
        self,
        status: HTTPStatus,
        body: bytes,
        content_type: str,
        filename: str,
    ) -> None:
        self.send_response(status.value)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _serve_static(self, request_path: str) -> None:
        relative_path = "index.html" if request_path in ("", "/") else unquote(request_path.lstrip("/"))
        file_path = (STATIC_DIR / relative_path).resolve()
        if STATIC_DIR not in file_path.parents and file_path != STATIC_DIR:
            self.send_error(HTTPStatus.FORBIDDEN.value)
            return
        if not file_path.exists() or not file_path.is_file():
            self.send_error(HTTPStatus.NOT_FOUND.value)
            return
        content_type = mimetypes.guess_type(str(file_path))[0] or "application/octet-stream"
        body = file_path.read_bytes()
        self.send_response(HTTPStatus.OK.value)
        if content_type.startswith("text/") or content_type in {"application/javascript", "application/json"}:
            self.send_header("Content-Type", f"{content_type}; charset=utf-8")
        else:
            self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _require_user(self, permission: str | None = None) -> dict[str, object]:
        user = self._current_user()
        if user is None:
            raise AuthError(HTTPStatus.UNAUTHORIZED, "请先登录后再操作。")
        permissions = set(user.get("permissions", []))
        if permission and permission not in permissions:
            raise AuthError(HTTPStatus.FORBIDDEN, "当前账号没有该操作权限。")
        return user

    def _current_user(self) -> dict[str, object] | None:
        token = self._get_session_token()
        user_id = self.sessions.get(token)
        if user_id is None:
            return None
        return self.service.get_user_by_id(user_id)

    def _get_session_token(self) -> str | None:
        cookie_header = self.headers.get("Cookie")
        if not cookie_header:
            return None
        cookie = SimpleCookie()
        cookie.load(cookie_header)
        morsel = cookie.get(SESSION_COOKIE)
        return morsel.value if morsel else None


def run() -> None:
    server = ThreadingHTTPServer((HOST, PORT), TraceabilityRequestHandler)
    print(f"Traceability demo running at http://{HOST}:{PORT}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    run()
