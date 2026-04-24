from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from .db import get_connection, initialize_database, now_text, seed_demo_data
from .exporters import build_xlsx


DEVICE_SELECT = """
SELECT
    d.id,
    d.device_name,
    d.category,
    d.trace_mode,
    d.risk_level,
    d.model,
    d.specification,
    d.manufacturer,
    d.supplier_id,
    s.name AS supplier_name,
    d.unit,
    d.batch_no,
    d.serial_no,
    d.production_date,
    d.expiry_date,
    d.reorder_threshold,
    d.stock_qty,
    d.status,
    d.current_location,
    d.last_seen_at,
    d.is_high_value,
    d.created_at,
    MAX(CASE WHEN cm.code_type = 'UDI' THEN cm.code_value END) AS udi_code,
    MAX(CASE WHEN cm.code_type = 'INTERNAL' THEN cm.code_value END) AS internal_code,
    MAX(CASE WHEN cm.code_type = 'SUPPLIER' THEN cm.code_value END) AS supplier_code,
    MAX(CASE WHEN cm.code_type = 'RFID' THEN cm.code_value END) AS rfid_code,
    MAX(CASE WHEN cm.code_type = 'QR' THEN cm.code_value END) AS qr_code
FROM devices d
LEFT JOIN suppliers s ON s.id = d.supplier_id
LEFT JOIN code_mappings cm ON cm.device_id = d.id
"""


GROUP_ORDER = """
GROUP BY d.id
ORDER BY d.created_at DESC, d.id DESC
"""


ROLE_PERMISSIONS = {
    "admin": {
        "alerts:view",
        "audit:view",
        "clinical:write",
        "dashboard:view",
        "devices:view",
        "devices:write",
        "inbound_order:approve",
        "inbound_order:create",
        "inbound_order:view",
        "maintenance:write",
        "purchase_plan:approve",
        "purchase_plan:create",
        "purchase_plan:view",
        "purchase_staff:manage",
        "quality:manage",
        "quality:write",
        "quality:view",
        "procurement:approve",
        "procurement:create",
        "procurement:receive",
        "procurement:view",
        "recall:manage",
        "request:approve",
        "request:create",
        "request:issue",
        "request:view",
        "recall:view",
        "reports:view",
        "scrap:approve",
        "scrap:create",
        "scrap:dispose",
        "scrap:view",
        "suppliers:write",
        "suppliers:view",
        "trace:view",
        "transfer:create",
        "transfer:view",
        "users:manage",
        "users:view",
        "warehouse:write",
    },
    "warehouse": {
        "alerts:view",
        "dashboard:view",
        "devices:view",
        "inbound_order:approve",
        "inbound_order:view",
        "procurement:create",
        "procurement:receive",
        "procurement:view",
        "request:issue",
        "request:view",
        "trace:view",
        "transfer:create",
        "transfer:view",
        "warehouse:write",
    },
    "clinician": {
        "alerts:view",
        "dashboard:view",
        "devices:view",
        "clinical:write",
        "quality:write",
        "quality:view",
        "request:create",
        "request:view",
        "trace:view",
    },
    "engineer": {
        "alerts:view",
        "dashboard:view",
        "devices:view",
        "maintenance:write",
        "quality:manage",
        "quality:view",
        "scrap:create",
        "scrap:view",
        "trace:view",
    },
    "manager": {
        "alerts:view",
        "audit:view",
        "dashboard:view",
        "devices:view",
        "inbound_order:approve",
        "inbound_order:view",
        "procurement:approve",
        "procurement:view",
        "purchase_plan:approve",
        "purchase_plan:view",
        "purchase_staff:manage",
        "quality:manage",
        "quality:view",
        "recall:manage",
        "recall:view",
        "request:approve",
        "request:view",
        "reports:view",
        "scrap:approve",
        "scrap:dispose",
        "scrap:view",
        "suppliers:view",
        "trace:view",
        "transfer:view",
        "users:view",
    },
    "purchaser": {
        "alerts:view",
        "dashboard:view",
        "devices:view",
        "inbound_order:create",
        "inbound_order:view",
        "procurement:create",
        "procurement:view",
        "purchase_plan:create",
        "purchase_plan:view",
        "suppliers:write",
        "suppliers:view",
        "trace:view",
    },
}


class TraceabilityService:
    CODE_FIELDS = {
        "udi_code": "UDI",
        "internal_code": "INTERNAL",
        "supplier_code": "SUPPLIER",
        "rfid_code": "RFID",
        "qr_code": "QR",
    }

    def __init__(self, db_path: str | Path, seed_demo: bool = True) -> None:
        self.db_path = Path(db_path)
        initialize_database(self.db_path)
        if seed_demo:
            seed_demo_data(self.db_path)

    def authenticate_user(self, username: str, password: str) -> dict[str, Any]:
        with get_connection(self.db_path) as connection:
            row = connection.execute(
                """
                SELECT id, username, display_name, role, is_active, job_no, phone, department
                FROM users
                WHERE username = ? AND password = ?
                """,
                (username.strip(), password.strip()),
            ).fetchone()
            if row is None or not row["is_active"]:
                raise ValueError("用户名或密码错误。")
            return self._serialize_user(row)

    def get_user_by_id(self, user_id: int) -> dict[str, Any] | None:
        with get_connection(self.db_path) as connection:
            row = connection.execute(
                """
                SELECT id, username, display_name, role, is_active, job_no, phone, department
                FROM users
                WHERE id = ?
                """,
                (user_id,),
            ).fetchone()
            if row is None:
                return None
            return self._serialize_user(row)

    def get_permissions_for_role(self, role: str) -> list[str]:
        return sorted(ROLE_PERMISSIONS.get(role, set()))

    def get_lookups(self) -> dict[str, Any]:
        with get_connection(self.db_path) as connection:
            suppliers = [
                dict(row)
                for row in connection.execute(
                    """
                    SELECT id, name, license_no, contact_person, phone, business_scope, qualification, status
                    FROM suppliers
                    ORDER BY name
                    """
                ).fetchall()
            ]
            departments = [
                dict(row)
                for row in connection.execute("SELECT id, name, type FROM departments ORDER BY type, name").fetchall()
            ]
            patients = [
                dict(row)
                for row in connection.execute(
                    "SELECT id, patient_no, name, gender, age FROM patients ORDER BY patient_no"
                ).fetchall()
            ]
            devices = [
                {
                    "id": row["id"],
                    "device_name": row["device_name"],
                    "batch_no": row["batch_no"],
                    "udi_code": row["udi_code"],
                    "stock_qty": row["stock_qty"],
                    "supplier_id": row["supplier_id"],
                    "supplier_name": row["supplier_name"],
                }
                for row in self._fetch_devices(connection)
            ]
            procurements = [
                dict(row)
                for row in connection.execute(
                    "SELECT id, request_no, status FROM procurements ORDER BY requested_at DESC, id DESC"
                ).fetchall()
            ]
            recalls = [
                dict(row)
                for row in connection.execute(
                    "SELECT id, recall_no, status FROM recall_cases ORDER BY initiated_at DESC, id DESC"
                ).fetchall()
            ]
            scraps = [
                dict(row)
                for row in connection.execute(
                    "SELECT id, scrap_no, status FROM scrap_requests ORDER BY requested_at DESC, id DESC"
                ).fetchall()
            ]
            purchase_plans = [
                dict(row)
                for row in connection.execute(
                    "SELECT id, plan_no, status FROM purchase_plans ORDER BY submitted_at DESC, id DESC"
                ).fetchall()
            ]
            inbound_orders = [
                dict(row)
                for row in connection.execute(
                    "SELECT id, order_no, status FROM inbound_orders ORDER BY submitted_at DESC, id DESC"
                ).fetchall()
            ]
            users = [
                dict(row)
                for row in connection.execute(
                    "SELECT id, username, display_name, role, department FROM users WHERE is_active = 1 ORDER BY role, display_name"
                ).fetchall()
            ]
        return {
            "suppliers": suppliers,
            "departments": departments,
            "patients": patients,
            "devices": devices,
            "procurements": procurements,
            "recalls": recalls,
            "scraps": scraps,
            "purchase_plans": purchase_plans,
            "inbound_orders": inbound_orders,
            "users": users,
            "maintenance_types": ["保养", "维修", "校准"],
            "risk_levels": ["I类", "II类", "III类"],
            "trace_modes": ["RFID", "QR"],
            "categories": ["高值耗材", "普耗", "设备"],
            "recall_severities": ["高", "中", "低"],
            "procurement_statuses": ["requested", "approved", "completed"],
            "scrap_statuses": ["requested", "approved", "disposed"],
            "purchase_plan_statuses": ["submitted", "approved", "rejected", "purchased"],
            "inbound_order_statuses": ["submitted", "approved", "rejected", "received"],
            "device_request_statuses": ["submitted", "approved", "rejected", "issued"],
            "quality_report_statuses": ["submitted", "processing", "resolved", "rejected"],
            "roles": ["admin", "manager", "purchaser", "warehouse", "clinician", "engineer"],
        }

    def list_devices(self) -> list[dict[str, Any]]:
        with get_connection(self.db_path) as connection:
            return [self._serialize_device(row) for row in self._fetch_devices(connection)]

    def list_inventory(self) -> list[dict[str, Any]]:
        devices = self.list_devices()
        return sorted(devices, key=lambda item: (item["stock_qty"], item["expiry_date"] or "9999-12-31"))

    def list_suppliers(self) -> list[dict[str, Any]]:
        with get_connection(self.db_path) as connection:
            rows = connection.execute(
                """
                SELECT id, name, license_no, contact_person, phone,
                       business_scope, qualification, status, created_at
                FROM suppliers
                ORDER BY name
                """
            ).fetchall()
            return [dict(row) for row in rows]

    def list_users(self) -> list[dict[str, Any]]:
        with get_connection(self.db_path) as connection:
            rows = connection.execute(
                """
                SELECT id, username, display_name, role, job_no, phone,
                       department, is_active, created_at
                FROM users
                ORDER BY role, display_name
                """
            ).fetchall()
            return [self._serialize_user(row) for row in rows]

    def list_purchase_staff(self) -> list[dict[str, Any]]:
        with get_connection(self.db_path) as connection:
            rows = connection.execute(
                """
                SELECT
                    ps.id,
                    ps.staff_no,
                    ps.name,
                    ps.phone,
                    ps.department,
                    ps.position,
                    ps.qualification,
                    ps.status,
                    ps.created_at,
                    u.username,
                    u.display_name AS account_name
                FROM purchase_staff ps
                LEFT JOIN users u ON u.id = ps.user_id
                ORDER BY ps.staff_no
                """
            ).fetchall()
            return [dict(row) for row in rows]

    def list_purchase_plans(self) -> list[dict[str, Any]]:
        with get_connection(self.db_path) as connection:
            rows = connection.execute(
                """
                SELECT
                    pp.id,
                    pp.plan_no,
                    pp.quantity,
                    pp.estimated_unit_price,
                    pp.reason,
                    pp.source,
                    pp.status,
                    pp.submitted_by,
                    pp.submitted_at,
                    pp.reviewed_by,
                    pp.reviewed_at,
                    pp.review_note,
                    pp.purchased_at,
                    pp.remark,
                    d.device_name,
                    d.batch_no,
                    d.stock_qty,
                    d.reorder_threshold,
                    d.unit,
                    s.name AS supplier_name
                FROM purchase_plans pp
                JOIN devices d ON d.id = pp.device_id
                LEFT JOIN suppliers s ON s.id = pp.supplier_id
                ORDER BY pp.submitted_at DESC, pp.id DESC
                """
            ).fetchall()
            return [dict(row) for row in rows]

    def list_inbound_orders(self) -> list[dict[str, Any]]:
        with get_connection(self.db_path) as connection:
            rows = connection.execute(
                """
                SELECT
                    io.id,
                    io.order_no,
                    io.quantity,
                    io.warehouse,
                    io.status,
                    io.submitted_by,
                    io.submitted_at,
                    io.reviewed_by,
                    io.reviewed_at,
                    io.review_note,
                    io.received_at,
                    io.remark,
                    pp.plan_no,
                    p.request_no AS procurement_no,
                    d.device_name,
                    d.batch_no,
                    d.unit,
                    s.name AS supplier_name
                FROM inbound_orders io
                LEFT JOIN purchase_plans pp ON pp.id = io.plan_id
                LEFT JOIN procurements p ON p.id = io.procurement_id
                JOIN devices d ON d.id = io.device_id
                LEFT JOIN suppliers s ON s.id = io.supplier_id
                ORDER BY io.submitted_at DESC, io.id DESC
                """
            ).fetchall()
            return [dict(row) for row in rows]

    def list_device_requests(self) -> list[dict[str, Any]]:
        with get_connection(self.db_path) as connection:
            rows = connection.execute(
                """
                SELECT
                    dr.id,
                    dr.request_no,
                    dr.requester_name,
                    dep.name AS department_name,
                    d.device_name,
                    d.batch_no,
                    d.unit,
                    dr.quantity,
                    dr.purpose,
                    dr.status,
                    dr.submitted_at,
                    dr.reviewed_by,
                    dr.reviewed_at,
                    dr.review_note,
                    dr.issued_by,
                    dr.issued_at,
                    dr.remark
                FROM device_requests dr
                JOIN departments dep ON dep.id = dr.department_id
                JOIN devices d ON d.id = dr.device_id
                ORDER BY dr.submitted_at DESC, dr.id DESC
                """
            ).fetchall()
            return [dict(row) for row in rows]

    def list_quality_reports(self) -> list[dict[str, Any]]:
        with get_connection(self.db_path) as connection:
            rows = connection.execute(
                """
                SELECT
                    qr.id,
                    qr.report_no,
                    qr.reporter_name,
                    dep.name AS department_name,
                    d.device_name,
                    d.batch_no,
                    p.patient_no,
                    p.name AS patient_name,
                    qr.problem_type,
                    qr.severity,
                    qr.description,
                    qr.status,
                    qr.submitted_at,
                    qr.handled_by,
                    qr.handled_at,
                    qr.handling_result,
                    qr.remark
                FROM quality_reports qr
                JOIN departments dep ON dep.id = qr.department_id
                JOIN devices d ON d.id = qr.device_id
                LEFT JOIN patients p ON p.id = qr.patient_id
                ORDER BY qr.submitted_at DESC, qr.id DESC
                """
            ).fetchall()
            return [dict(row) for row in rows]

    def list_transfers(self) -> list[dict[str, Any]]:
        with get_connection(self.db_path) as connection:
            rows = connection.execute(
                """
                SELECT
                    dt.id,
                    dt.transfer_no,
                    d.device_name,
                    d.batch_no,
                    d.unit,
                    dt.quantity,
                    fd.name AS from_department,
                    td.name AS to_department,
                    dt.operator_name,
                    dt.reason,
                    dt.transferred_at,
                    dt.remark
                FROM department_transfers dt
                JOIN devices d ON d.id = dt.device_id
                JOIN departments fd ON fd.id = dt.from_department_id
                JOIN departments td ON td.id = dt.to_department_id
                ORDER BY dt.transferred_at DESC, dt.id DESC
                """
            ).fetchall()
            return [dict(row) for row in rows]

    def list_maintenance_records(self) -> list[dict[str, Any]]:
        with get_connection(self.db_path) as connection:
            rows = connection.execute(
                """
                SELECT
                    mr.id,
                    mr.record_type,
                    mr.plan_date,
                    mr.completed_at,
                    mr.result,
                    mr.vendor_name,
                    mr.operator_name,
                    mr.next_due_date,
                    mr.remark,
                    d.device_name,
                    d.batch_no,
                    d.serial_no,
                    d.current_location
                FROM maintenance_records mr
                JOIN devices d ON d.id = mr.device_id
                ORDER BY COALESCE(mr.completed_at, mr.plan_date) DESC, mr.id DESC
                """
            ).fetchall()
            return [dict(row) for row in rows]

    def list_procurements(self) -> list[dict[str, Any]]:
        with get_connection(self.db_path) as connection:
            rows = connection.execute(
                """
                SELECT
                    p.id,
                    p.request_no,
                    p.quantity,
                    p.unit_price,
                    p.purpose,
                    p.expected_arrival_date,
                    p.status,
                    p.requested_by,
                    p.requested_at,
                    p.approved_by,
                    p.approved_at,
                    p.received_by,
                    p.received_at,
                    p.inbound_completed_at,
                    p.remark,
                    d.device_name,
                    d.batch_no,
                    s.name AS supplier_name
                FROM procurements p
                JOIN devices d ON d.id = p.device_id
                JOIN suppliers s ON s.id = p.supplier_id
                ORDER BY p.requested_at DESC, p.id DESC
                """
            ).fetchall()
            return [dict(row) for row in rows]

    def list_recalls(self) -> list[dict[str, Any]]:
        with get_connection(self.db_path) as connection:
            rows = connection.execute(
                """
                SELECT
                    rc.id,
                    rc.recall_no,
                    rc.scope_type,
                    rc.batch_no,
                    rc.reason,
                    rc.severity,
                    rc.status,
                    rc.initiated_by,
                    rc.initiated_at,
                    rc.disposal_note,
                    rc.closed_at,
                    d.device_name,
                    COUNT(DISTINCT CASE WHEN ri.impact_type = 'device' THEN ri.device_id END) AS affected_device_count,
                    COUNT(DISTINCT CASE WHEN ri.impact_type = 'patient' THEN ri.patient_id END) AS affected_patient_count
                FROM recall_cases rc
                LEFT JOIN devices d ON d.id = rc.device_id
                LEFT JOIN recall_impacts ri ON ri.recall_case_id = rc.id
                GROUP BY rc.id
                ORDER BY rc.initiated_at DESC, rc.id DESC
                """
            ).fetchall()
            return [dict(row) for row in rows]

    def list_scrap_requests(self) -> list[dict[str, Any]]:
        with get_connection(self.db_path) as connection:
            rows = connection.execute(
                """
                SELECT
                    sr.id,
                    sr.scrap_no,
                    sr.quantity,
                    sr.reason,
                    sr.status,
                    sr.requested_by,
                    sr.requested_at,
                    sr.approved_by,
                    sr.approved_at,
                    sr.disposed_by,
                    sr.disposed_at,
                    sr.remark,
                    d.device_name,
                    d.batch_no,
                    d.stock_qty,
                    d.current_location
                FROM scrap_requests sr
                JOIN devices d ON d.id = sr.device_id
                ORDER BY sr.requested_at DESC, sr.id DESC
                """
            ).fetchall()
            return [dict(row) for row in rows]

    def list_audit_logs(self, limit: int = 120) -> list[dict[str, Any]]:
        safe_limit = max(10, min(limit, 500))
        with get_connection(self.db_path) as connection:
            rows = connection.execute(
                """
                SELECT
                    id,
                    username,
                    role,
                    action,
                    target_type,
                    target_id,
                    target_name,
                    detail,
                    ip_address,
                    created_at
                FROM audit_logs
                ORDER BY created_at DESC, id DESC
                LIMIT ?
                """,
                (safe_limit,),
            ).fetchall()
            return [dict(row) for row in rows]

    def get_dashboard(self) -> dict[str, Any]:
        alerts = self.get_alerts()
        with get_connection(self.db_path) as connection:
            total_devices = connection.execute("SELECT COUNT(*) AS count FROM devices").fetchone()["count"]
            served_patients = connection.execute(
                "SELECT COUNT(DISTINCT patient_id) AS count FROM clinical_usages"
            ).fetchone()["count"]
            open_recalls = connection.execute(
                "SELECT COUNT(*) AS count FROM recall_cases WHERE status = 'open'"
            ).fetchone()["count"]
            pending_scraps = connection.execute(
                "SELECT COUNT(*) AS count FROM scrap_requests WHERE status != 'disposed'"
            ).fetchone()["count"]
            pending_procurements = connection.execute(
                "SELECT COUNT(*) AS count FROM procurements WHERE status != 'completed'"
            ).fetchone()["count"]
            pending_purchase_plans = connection.execute(
                "SELECT COUNT(*) AS count FROM purchase_plans WHERE status = 'submitted'"
            ).fetchone()["count"]
            pending_device_requests = connection.execute(
                "SELECT COUNT(*) AS count FROM device_requests WHERE status = 'submitted'"
            ).fetchone()["count"]
            pending_inbound_orders = connection.execute(
                "SELECT COUNT(*) AS count FROM inbound_orders WHERE status = 'submitted'"
            ).fetchone()["count"]
            open_quality_reports = connection.execute(
                "SELECT COUNT(*) AS count FROM quality_reports WHERE status IN ('submitted', 'processing')"
            ).fetchone()["count"]

        return {
            "total_devices": total_devices,
            "expiring_soon": len(alerts["expiry_alerts"]),
            "maintenance_due": len(alerts["maintenance_alerts"]),
            "served_patients": served_patients,
            "low_stock": len(alerts["stock_alerts"]),
            "open_recalls": open_recalls,
            "overdue_calibration": len(alerts["overdue_calibration_alerts"]),
            "stale_stocktake": len(alerts["stale_stocktake_alerts"]),
            "pending_scraps": pending_scraps,
            "pending_procurements": pending_procurements,
            "pending_purchase_plans": pending_purchase_plans,
            "pending_device_requests": pending_device_requests,
            "pending_inbound_orders": pending_inbound_orders,
            "open_quality_reports": open_quality_reports,
        }

    def get_alerts(self) -> dict[str, Any]:
        today = date.today()
        expiry_cutoff = today + timedelta(days=30)
        maintenance_cutoff = today + timedelta(days=15)
        stale_cutoff = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d %H:%M:%S")

        with get_connection(self.db_path) as connection:
            expiry_alerts = [
                dict(row)
                for row in connection.execute(
                    """
                    SELECT device_name, batch_no, expiry_date, stock_qty
                    FROM devices
                    WHERE expiry_date IS NOT NULL
                      AND expiry_date BETWEEN ? AND ?
                    ORDER BY expiry_date ASC
                    """,
                    (today.isoformat(), expiry_cutoff.isoformat()),
                ).fetchall()
            ]

            maintenance_alerts = [
                dict(row)
                for row in connection.execute(
                    """
                    SELECT d.device_name, mr.record_type, mr.next_due_date, d.current_location
                    FROM maintenance_records mr
                    JOIN devices d ON d.id = mr.device_id
                    WHERE mr.next_due_date IS NOT NULL
                      AND mr.next_due_date BETWEEN ? AND ?
                    ORDER BY mr.next_due_date ASC
                    """,
                    (today.isoformat(), maintenance_cutoff.isoformat()),
                ).fetchall()
            ]

            stock_alerts = [
                dict(row)
                for row in connection.execute(
                    """
                    SELECT device_name, stock_qty, reorder_threshold, current_location
                    FROM devices
                    WHERE stock_qty <= reorder_threshold
                    ORDER BY stock_qty ASC, device_name ASC
                    """
                ).fetchall()
            ]

            recall_alerts = [
                dict(row)
                for row in connection.execute(
                    """
                    SELECT
                        rc.recall_no,
                        COALESCE(d.device_name, '按批次召回') AS device_name,
                        rc.batch_no,
                        rc.severity,
                        rc.initiated_at
                    FROM recall_cases rc
                    LEFT JOIN devices d ON d.id = rc.device_id
                    WHERE rc.status = 'open'
                    ORDER BY rc.initiated_at DESC
                    """
                ).fetchall()
            ]

            overdue_calibration_alerts = [
                dict(row)
                for row in connection.execute(
                    """
                    SELECT
                        d.device_name,
                        mr.next_due_date,
                        d.current_location
                    FROM maintenance_records mr
                    JOIN devices d ON d.id = mr.device_id
                    WHERE mr.record_type = '校准'
                      AND mr.next_due_date IS NOT NULL
                      AND mr.next_due_date < ?
                      AND mr.id IN (
                        SELECT MAX(id)
                        FROM maintenance_records
                        WHERE device_id = d.id AND record_type = '校准'
                      )
                    ORDER BY mr.next_due_date ASC
                    """,
                    (today.isoformat(),),
                ).fetchall()
            ]

            stale_stocktake_alerts = [
                dict(row)
                for row in connection.execute(
                    """
                    SELECT
                        d.device_name,
                        d.current_location,
                        COALESCE(
                            MAX(CASE WHEN te.event_type = 'STOCKTAKE' THEN te.occurred_at END),
                            d.created_at
                        ) AS last_stocktake_at
                    FROM devices d
                    LEFT JOIN trace_events te ON te.device_id = d.id
                    GROUP BY d.id
                    HAVING last_stocktake_at < ?
                    ORDER BY last_stocktake_at ASC
                    """,
                    (stale_cutoff,),
                ).fetchall()
            ]

        return {
            "expiry_alerts": expiry_alerts,
            "maintenance_alerts": maintenance_alerts,
            "stock_alerts": stock_alerts,
            "recall_alerts": recall_alerts,
            "overdue_calibration_alerts": overdue_calibration_alerts,
            "stale_stocktake_alerts": stale_stocktake_alerts,
        }

    def get_report_summary(self) -> dict[str, Any]:
        dashboard = self.get_dashboard()
        with get_connection(self.db_path) as connection:
            report = {
                "dashboard": dashboard,
                "recent_procurements": connection.execute(
                    "SELECT COUNT(*) AS count FROM procurements WHERE requested_at >= ?",
                    ((date.today() - timedelta(days=30)).isoformat(),),
                ).fetchone()["count"],
                "recent_usages": connection.execute(
                    "SELECT COUNT(*) AS count FROM clinical_usages WHERE used_at >= ?",
                    ((date.today() - timedelta(days=30)).isoformat(),),
                ).fetchone()["count"],
                "recent_maintenances": connection.execute(
                    "SELECT COUNT(*) AS count FROM maintenance_records WHERE COALESCE(completed_at, plan_date) >= ?",
                    ((date.today() - timedelta(days=30)).isoformat(),),
                ).fetchone()["count"],
                "recent_audits": connection.execute(
                    "SELECT COUNT(*) AS count FROM audit_logs WHERE created_at >= ?",
                    ((date.today() - timedelta(days=30)).isoformat(),),
                ).fetchone()["count"],
            }
        return report

    def export_reports_workbook(
        self,
        actor: dict[str, Any] | None = None,
        ip_address: str | None = None,
    ) -> bytes:
        with get_connection(self.db_path) as connection:
            inventory_rows = connection.execute(
                """
                SELECT device_name, category, udi_code, batch_no, stock_qty, reorder_threshold, expiry_date, current_location, status
                FROM (
                    SELECT
                        d.device_name,
                        d.category,
                        d.batch_no,
                        d.stock_qty,
                        d.reorder_threshold,
                        d.expiry_date,
                        d.current_location,
                        d.status,
                        MAX(CASE WHEN cm.code_type = 'UDI' THEN cm.code_value END) AS udi_code
                    FROM devices d
                    LEFT JOIN code_mappings cm ON cm.device_id = d.id
                    GROUP BY d.id
                )
                ORDER BY category, device_name
                """
            ).fetchall()

            usage_rows = connection.execute(
                """
                SELECT
                    d.device_name,
                    d.batch_no,
                    p.patient_no,
                    p.name,
                    dep.name AS department_name,
                    cu.operation_name,
                    cu.quantity,
                    cu.used_at,
                    cu.operator_name
                FROM clinical_usages cu
                JOIN devices d ON d.id = cu.device_id
                JOIN patients p ON p.id = cu.patient_id
                JOIN departments dep ON dep.id = cu.department_id
                ORDER BY cu.used_at DESC
                """
            ).fetchall()

            maintenance_rows = connection.execute(
                """
                SELECT
                    d.device_name,
                    mr.record_type,
                    mr.plan_date,
                    mr.completed_at,
                    mr.result,
                    mr.next_due_date,
                    mr.operator_name,
                    mr.vendor_name
                FROM maintenance_records mr
                JOIN devices d ON d.id = mr.device_id
                ORDER BY COALESCE(mr.completed_at, mr.plan_date) DESC
                """
            ).fetchall()

            procurement_rows = connection.execute(
                """
                SELECT
                    p.request_no,
                    d.device_name,
                    s.name AS supplier_name,
                    p.quantity,
                    p.unit_price,
                    p.status,
                    p.requested_by,
                    p.requested_at,
                    p.approved_by,
                    p.approved_at,
                    p.received_by,
                    p.received_at
                FROM procurements p
                JOIN devices d ON d.id = p.device_id
                JOIN suppliers s ON s.id = p.supplier_id
                ORDER BY p.requested_at DESC
                """
            ).fetchall()

            recall_rows = connection.execute(
                """
                SELECT
                    rc.recall_no,
                    COALESCE(d.device_name, '-') AS device_name,
                    COALESCE(rc.batch_no, '-') AS batch_no,
                    rc.severity,
                    rc.status,
                    rc.initiated_by,
                    rc.initiated_at,
                    COUNT(DISTINCT CASE WHEN ri.impact_type = 'patient' THEN ri.patient_id END) AS affected_patients
                FROM recall_cases rc
                LEFT JOIN devices d ON d.id = rc.device_id
                LEFT JOIN recall_impacts ri ON ri.recall_case_id = rc.id
                GROUP BY rc.id
                ORDER BY rc.initiated_at DESC
                """
            ).fetchall()

            scrap_rows = connection.execute(
                """
                SELECT
                    sr.scrap_no,
                    d.device_name,
                    sr.quantity,
                    sr.reason,
                    sr.status,
                    sr.requested_by,
                    sr.requested_at,
                    sr.approved_by,
                    sr.approved_at,
                    sr.disposed_by,
                    sr.disposed_at
                FROM scrap_requests sr
                JOIN devices d ON d.id = sr.device_id
                ORDER BY sr.requested_at DESC
                """
            ).fetchall()

            purchase_plan_rows = connection.execute(
                """
                SELECT
                    pp.plan_no,
                    d.device_name,
                    s.name AS supplier_name,
                    pp.quantity,
                    pp.estimated_unit_price,
                    pp.source,
                    pp.status,
                    pp.submitted_by,
                    pp.submitted_at,
                    pp.reviewed_by,
                    pp.reviewed_at,
                    pp.review_note
                FROM purchase_plans pp
                JOIN devices d ON d.id = pp.device_id
                LEFT JOIN suppliers s ON s.id = pp.supplier_id
                ORDER BY pp.submitted_at DESC
                """
            ).fetchall()

            inbound_order_rows = connection.execute(
                """
                SELECT
                    io.order_no,
                    pp.plan_no,
                    d.device_name,
                    s.name AS supplier_name,
                    io.quantity,
                    io.warehouse,
                    io.status,
                    io.submitted_by,
                    io.submitted_at,
                    io.reviewed_by,
                    io.reviewed_at,
                    io.review_note
                FROM inbound_orders io
                LEFT JOIN purchase_plans pp ON pp.id = io.plan_id
                JOIN devices d ON d.id = io.device_id
                LEFT JOIN suppliers s ON s.id = io.supplier_id
                ORDER BY io.submitted_at DESC
                """
            ).fetchall()

            request_rows = connection.execute(
                """
                SELECT
                    dr.request_no,
                    dr.requester_name,
                    dep.name AS department_name,
                    d.device_name,
                    dr.quantity,
                    dr.purpose,
                    dr.status,
                    dr.submitted_at,
                    dr.reviewed_by,
                    dr.reviewed_at,
                    dr.issued_by,
                    dr.issued_at
                FROM device_requests dr
                JOIN departments dep ON dep.id = dr.department_id
                JOIN devices d ON d.id = dr.device_id
                ORDER BY dr.submitted_at DESC
                """
            ).fetchall()

            quality_rows = connection.execute(
                """
                SELECT
                    qr.report_no,
                    qr.reporter_name,
                    dep.name AS department_name,
                    d.device_name,
                    p.patient_no,
                    p.name AS patient_name,
                    qr.problem_type,
                    qr.severity,
                    qr.status,
                    qr.submitted_at,
                    qr.handled_by,
                    qr.handled_at,
                    qr.handling_result
                FROM quality_reports qr
                JOIN departments dep ON dep.id = qr.department_id
                JOIN devices d ON d.id = qr.device_id
                LEFT JOIN patients p ON p.id = qr.patient_id
                ORDER BY qr.submitted_at DESC
                """
            ).fetchall()

            transfer_rows = connection.execute(
                """
                SELECT
                    dt.transfer_no,
                    d.device_name,
                    dt.quantity,
                    fd.name AS from_department,
                    td.name AS to_department,
                    dt.operator_name,
                    dt.reason,
                    dt.transferred_at
                FROM department_transfers dt
                JOIN devices d ON d.id = dt.device_id
                JOIN departments fd ON fd.id = dt.from_department_id
                JOIN departments td ON td.id = dt.to_department_id
                ORDER BY dt.transferred_at DESC
                """
            ).fetchall()

            audit_rows = connection.execute(
                """
                SELECT username, role, action, target_type, target_name, detail, created_at
                FROM audit_logs
                ORDER BY created_at DESC, id DESC
                LIMIT 300
                """
            ).fetchall()

            workbook = build_xlsx(
                [
                    (
                        "库存总览",
                        [["器械名称", "分类", "UDI", "批次号", "库存", "阈值", "效期", "位置", "状态"]]
                        + [list(row) for row in inventory_rows],
                    ),
                    (
                        "临床使用",
                        [["器械名称", "批次号", "患者编号", "患者姓名", "科室", "手术名称", "数量", "使用时间", "操作人"]]
                        + [list(row) for row in usage_rows],
                    ),
                    (
                        "维护校准",
                        [["器械名称", "类型", "计划日期", "完成时间", "结果", "下次到期", "执行人", "服务单位"]]
                        + [list(row) for row in maintenance_rows],
                    ),
                    (
                        "采购闭环",
                        [["申请单号", "器械名称", "供应商", "数量", "单价", "状态", "申请人", "申请时间", "审批人", "审批时间", "收货人", "收货时间"]]
                        + [list(row) for row in procurement_rows],
                    ),
                    (
                        "采购计划",
                        [["计划号", "器械名称", "供应商", "数量", "预估单价", "来源", "状态", "提交人", "提交时间", "审核人", "审核时间", "审核意见"]]
                        + [list(row) for row in purchase_plan_rows],
                    ),
                    (
                        "采购入库单",
                        [["入库单号", "计划号", "器械名称", "供应商", "数量", "入库位置", "状态", "提交人", "提交时间", "审核人", "审核时间", "审核意见"]]
                        + [list(row) for row in inbound_order_rows],
                    ),
                    (
                        "申领审批",
                        [["申领单号", "申请人", "科室", "器械名称", "数量", "用途", "状态", "提交时间", "审核人", "审核时间", "发放人", "发放时间"]]
                        + [list(row) for row in request_rows],
                    ),
                    (
                        "质量问题",
                        [["报告号", "上报人", "科室", "器械名称", "患者编号", "患者姓名", "问题类型", "严重程度", "状态", "上报时间", "处理人", "处理时间", "处理结果"]]
                        + [list(row) for row in quality_rows],
                    ),
                    (
                        "科室调拨",
                        [["调拨单号", "器械名称", "数量", "调出科室", "调入科室", "操作人", "原因", "调拨时间"]]
                        + [list(row) for row in transfer_rows],
                    ),
                    (
                        "召回管理",
                        [["召回单号", "器械名称", "批次号", "等级", "状态", "发起人", "发起时间", "受影响患者数"]]
                        + [list(row) for row in recall_rows],
                    ),
                    (
                        "报废管理",
                        [["报废单号", "器械名称", "数量", "原因", "状态", "申请人", "申请时间", "审批人", "审批时间", "处置人", "处置时间"]]
                        + [list(row) for row in scrap_rows],
                    ),
                    (
                        "审计日志",
                        [["用户", "角色", "动作", "目标类型", "目标名称", "详情", "时间"]]
                        + [list(row) for row in audit_rows],
                    ),
                ]
            )
            self._log_audit(
                connection,
                actor,
                "EXPORT_REPORT",
                "reports",
                None,
                "追溯综合报表.xlsx",
                "导出库存、临床、维护、采购、召回、报废与审计报表。",
                ip_address,
            )
            return workbook

    def create_user(
        self,
        payload: dict[str, Any],
        actor: dict[str, Any] | None = None,
        ip_address: str | None = None,
    ) -> dict[str, Any]:
        username = self._required_text(payload, "username", "用户名不能为空。")
        display_name = self._required_text(payload, "display_name", "姓名不能为空。")
        role = self._required_text(payload, "role", "请选择用户角色。")
        if role not in ROLE_PERMISSIONS:
            raise ValueError("用户角色不正确。")
        password = self._optional_text(payload, "password") or "123456"
        with get_connection(self.db_path) as connection:
            cursor = connection.execute(
                """
                INSERT INTO users (
                    username, password, display_name, role, is_active,
                    job_no, phone, department, created_at
                )
                VALUES (?, ?, ?, ?, 1, ?, ?, ?, ?)
                """,
                (
                    username,
                    password,
                    display_name,
                    role,
                    self._optional_text(payload, "job_no"),
                    self._optional_text(payload, "phone"),
                    self._optional_text(payload, "department"),
                    now_text(),
                ),
            )
            self._log_audit(connection, actor, "CREATE_USER", "user", cursor.lastrowid, display_name, f"创建 {role} 用户。", ip_address)
            row = connection.execute(
                """
                SELECT id, username, display_name, role, is_active, job_no, phone, department
                FROM users
                WHERE id = ?
                """,
                (cursor.lastrowid,),
            ).fetchone()
            return self._serialize_user(row)

    def create_supplier(
        self,
        payload: dict[str, Any],
        actor: dict[str, Any] | None = None,
        ip_address: str | None = None,
    ) -> dict[str, Any]:
        name = self._required_text(payload, "name", "供应商名称不能为空。")
        with get_connection(self.db_path) as connection:
            cursor = connection.execute(
                """
                INSERT INTO suppliers (
                    name, license_no, contact_person, phone, business_scope,
                    qualification, status, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, 'active', ?)
                """,
                (
                    name,
                    self._optional_text(payload, "license_no"),
                    self._optional_text(payload, "contact_person"),
                    self._optional_text(payload, "phone"),
                    self._optional_text(payload, "business_scope"),
                    self._optional_text(payload, "qualification"),
                    now_text(),
                ),
            )
            self._log_audit(connection, actor, "CREATE_SUPPLIER", "supplier", cursor.lastrowid, name, "新增供应商档案。", ip_address)
            return dict(connection.execute("SELECT * FROM suppliers WHERE id = ?", (cursor.lastrowid,)).fetchone())

    def create_purchase_staff(
        self,
        payload: dict[str, Any],
        actor: dict[str, Any] | None = None,
        ip_address: str | None = None,
    ) -> dict[str, Any]:
        staff_no = self._required_text(payload, "staff_no", "采购员工号不能为空。")
        name = self._required_text(payload, "name", "采购员姓名不能为空。")
        user_id = self._optional_int(payload, "user_id")
        with get_connection(self.db_path) as connection:
            if user_id:
                self._ensure_exists(connection, "users", user_id, "关联用户不存在。")
            cursor = connection.execute(
                """
                INSERT INTO purchase_staff (
                    user_id, staff_no, name, phone, department, position,
                    qualification, status, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, 'active', ?)
                """,
                (
                    user_id,
                    staff_no,
                    name,
                    self._optional_text(payload, "phone"),
                    self._optional_text(payload, "department") or "采购办",
                    self._optional_text(payload, "position") or "采购专员",
                    self._optional_text(payload, "qualification"),
                    now_text(),
                ),
            )
            self._log_audit(connection, actor, "CREATE_PURCHASE_STAFF", "purchase_staff", cursor.lastrowid, name, "新增采购人员档案。", ip_address)
            return dict(connection.execute("SELECT * FROM purchase_staff WHERE id = ?", (cursor.lastrowid,)).fetchone())

    def create_purchase_plan(
        self,
        payload: dict[str, Any],
        actor: dict[str, Any] | None = None,
        ip_address: str | None = None,
    ) -> dict[str, Any]:
        device_id = self._required_int(payload, "device_id", "请选择采购器械。")
        quantity = self._required_positive_int(payload, "quantity", "采购计划数量必须大于 0。")
        reason = self._required_text(payload, "reason", "请填写采购原因。")
        supplier_id = self._optional_int(payload, "supplier_id")
        submitted_at = now_text()
        with get_connection(self.db_path) as connection:
            device = self._fetch_devices(connection, "WHERE d.id = ?", (device_id,))
            if not device:
                raise ValueError("器械不存在。")
            supplier_id = supplier_id or device[0]["supplier_id"]
            if supplier_id:
                self._ensure_exists(connection, "suppliers", supplier_id, "供应商不存在。")
            plan_no = self._generate_number(connection, "PP", "purchase_plans", "plan_no")
            cursor = connection.execute(
                """
                INSERT INTO purchase_plans (
                    plan_no, device_id, supplier_id, quantity, estimated_unit_price,
                    reason, source, status, submitted_by, submitted_at, remark
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, 'submitted', ?, ?, ?)
                """,
                (
                    plan_no,
                    device_id,
                    supplier_id,
                    quantity,
                    self._optional_float(payload, "estimated_unit_price") or 0,
                    reason,
                    self._optional_text(payload, "source") or "手工提交",
                    self._actor_name(actor),
                    submitted_at,
                    self._optional_text(payload, "remark"),
                ),
            )
            self._insert_trace_event(connection, device_id, "PURCHASE_PLAN", "提交采购计划", f"采购计划 {plan_no}：{reason}，数量 {quantity}{device[0]['unit']}。", "purchase_plans", cursor.lastrowid, self._actor_name(actor), "采购办", submitted_at)
            self._log_audit(connection, actor, "PURCHASE_PLAN_CREATE", "purchase_plan", cursor.lastrowid, plan_no, f"提交 {device[0]['device_name']} 采购计划。", ip_address)
            return self._fetch_purchase_plan(connection, cursor.lastrowid)

    def approve_purchase_plan(
        self,
        payload: dict[str, Any],
        actor: dict[str, Any] | None = None,
        ip_address: str | None = None,
    ) -> dict[str, Any]:
        plan_id = self._required_int(payload, "plan_id", "请选择采购计划。")
        decision = self._optional_text(payload, "decision") or "approved"
        if decision not in {"approved", "rejected"}:
            raise ValueError("采购计划审核结果不正确。")
        review_note = self._optional_text(payload, "review_note")
        reviewed_at = now_text()
        with get_connection(self.db_path) as connection:
            plan = connection.execute("SELECT * FROM purchase_plans WHERE id = ?", (plan_id,)).fetchone()
            if plan is None:
                raise ValueError("采购计划不存在。")
            if plan["status"] not in {"submitted", "approved"}:
                raise ValueError("当前采购计划状态不可审核。")
            connection.execute(
                """
                UPDATE purchase_plans
                SET status = ?, reviewed_by = ?, reviewed_at = ?, review_note = ?
                WHERE id = ?
                """,
                (decision, self._actor_name(actor), reviewed_at, review_note, plan_id),
            )
            self._insert_trace_event(connection, plan["device_id"], "PURCHASE_PLAN_REVIEW", "采购计划审核", review_note or f"采购计划审核结果：{decision}。", "purchase_plans", plan_id, self._actor_name(actor), "管理员工作台", reviewed_at)
            self._log_audit(connection, actor, "PURCHASE_PLAN_REVIEW", "purchase_plan", plan_id, plan["plan_no"], f"采购计划审核结果：{decision}。", ip_address)
            return self._fetch_purchase_plan(connection, plan_id)

    def create_inbound_order(
        self,
        payload: dict[str, Any],
        actor: dict[str, Any] | None = None,
        ip_address: str | None = None,
    ) -> dict[str, Any]:
        device_id = self._required_int(payload, "device_id", "请选择到货器械。")
        quantity = self._required_positive_int(payload, "quantity", "入库数量必须大于 0。")
        warehouse = self._required_text(payload, "warehouse", "入库位置不能为空。")
        plan_id = self._optional_int(payload, "plan_id")
        procurement_id = self._optional_int(payload, "procurement_id")
        submitted_at = now_text()
        with get_connection(self.db_path) as connection:
            device = self._fetch_devices(connection, "WHERE d.id = ?", (device_id,))
            if not device:
                raise ValueError("器械不存在。")
            if plan_id:
                self._ensure_exists(connection, "purchase_plans", plan_id, "采购计划不存在。")
            if procurement_id:
                self._ensure_exists(connection, "procurements", procurement_id, "采购单不存在。")
            supplier_id = self._optional_int(payload, "supplier_id") or device[0]["supplier_id"]
            order_no = self._generate_number(connection, "IN", "inbound_orders", "order_no")
            cursor = connection.execute(
                """
                INSERT INTO inbound_orders (
                    order_no, plan_id, procurement_id, device_id, supplier_id,
                    quantity, warehouse, status, submitted_by, submitted_at, remark
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, 'submitted', ?, ?, ?)
                """,
                (
                    order_no,
                    plan_id,
                    procurement_id,
                    device_id,
                    supplier_id,
                    quantity,
                    warehouse,
                    self._actor_name(actor),
                    submitted_at,
                    self._optional_text(payload, "remark"),
                ),
            )
            self._insert_trace_event(connection, device_id, "INBOUND_ORDER", "提交采购入库单", f"入库单 {order_no} 到货 {quantity}{device[0]['unit']}，待管理员审核。", "inbound_orders", cursor.lastrowid, self._actor_name(actor), warehouse, submitted_at)
            self._log_audit(connection, actor, "INBOUND_ORDER_CREATE", "inbound_order", cursor.lastrowid, order_no, f"提交 {device[0]['device_name']} 采购入库单。", ip_address)
            return self._fetch_inbound_order(connection, cursor.lastrowid)

    def approve_inbound_order(
        self,
        payload: dict[str, Any],
        actor: dict[str, Any] | None = None,
        ip_address: str | None = None,
    ) -> dict[str, Any]:
        order_id = self._required_int(payload, "order_id", "请选择入库单。")
        decision = self._optional_text(payload, "decision") or "received"
        if decision not in {"approved", "received", "rejected"}:
            raise ValueError("入库单审核结果不正确。")
        review_note = self._optional_text(payload, "review_note")
        reviewed_at = now_text()
        with get_connection(self.db_path) as connection:
            order = connection.execute("SELECT * FROM inbound_orders WHERE id = ?", (order_id,)).fetchone()
            if order is None:
                raise ValueError("入库单不存在。")
            if order["status"] not in {"submitted", "approved"}:
                raise ValueError("当前入库单状态不可审核。")
            device = self._fetch_devices(connection, "WHERE d.id = ?", (order["device_id"],))[0]
            received_at = reviewed_at if decision == "received" else None
            connection.execute(
                """
                UPDATE inbound_orders
                SET status = ?, reviewed_by = ?, reviewed_at = ?, review_note = ?, received_at = COALESCE(?, received_at)
                WHERE id = ?
                """,
                (decision, self._actor_name(actor), reviewed_at, review_note, received_at, order_id),
            )
            if decision == "received":
                movement_cursor = connection.execute(
                    """
                    INSERT INTO stock_movements (
                        device_id, movement_type, quantity, warehouse, department_id,
                        operator_name, remark, occurred_at
                    )
                    VALUES (?, 'INBOUND', ?, ?, NULL, ?, ?, ?)
                    """,
                    (order["device_id"], order["quantity"], order["warehouse"], self._actor_name(actor), review_note or "采购入库单审核通过并入库", reviewed_at),
                )
                connection.execute(
                    """
                    UPDATE devices
                    SET stock_qty = stock_qty + ?, status = 'in_stock',
                        current_location = ?, last_seen_at = ?
                    WHERE id = ?
                    """,
                    (order["quantity"], order["warehouse"], reviewed_at, order["device_id"]),
                )
                self._insert_trace_event(connection, order["device_id"], "INBOUND", "采购入库审核通过", f"入库 {order['quantity']}{device['unit']}，位置：{order['warehouse']}。", "stock_movements", movement_cursor.lastrowid, self._actor_name(actor), order["warehouse"], reviewed_at)
            else:
                self._insert_trace_event(connection, order["device_id"], "INBOUND_REVIEW", "入库单审核", review_note or f"入库单审核结果：{decision}。", "inbound_orders", order_id, self._actor_name(actor), order["warehouse"], reviewed_at)
            self._log_audit(connection, actor, "INBOUND_ORDER_REVIEW", "inbound_order", order_id, order["order_no"], f"入库单审核结果：{decision}。", ip_address)
            return self._fetch_inbound_order(connection, order_id)

    def create_device_request(
        self,
        payload: dict[str, Any],
        actor: dict[str, Any] | None = None,
        ip_address: str | None = None,
    ) -> dict[str, Any]:
        device_id = self._required_int(payload, "device_id", "请选择申领器械。")
        department_id = self._required_int(payload, "department_id", "请选择申领科室。")
        quantity = self._required_positive_int(payload, "quantity", "申领数量必须大于 0。")
        purpose = self._required_text(payload, "purpose", "申领用途不能为空。")
        submitted_at = now_text()
        with get_connection(self.db_path) as connection:
            device = self._fetch_devices(connection, "WHERE d.id = ?", (device_id,))
            if not device:
                raise ValueError("器械不存在。")
            self._ensure_exists(connection, "departments", department_id, "科室不存在。")
            department = connection.execute("SELECT name FROM departments WHERE id = ?", (department_id,)).fetchone()["name"]
            request_no = self._generate_number(connection, "REQ", "device_requests", "request_no")
            cursor = connection.execute(
                """
                INSERT INTO device_requests (
                    request_no, requester_id, requester_name, department_id,
                    device_id, quantity, purpose, status, submitted_at, remark
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, 'submitted', ?, ?)
                """,
                (
                    request_no,
                    actor.get("id") if actor else None,
                    self._actor_name(actor),
                    department_id,
                    device_id,
                    quantity,
                    purpose,
                    submitted_at,
                    self._optional_text(payload, "remark"),
                ),
            )
            self._insert_trace_event(connection, device_id, "DEVICE_REQUEST", "提交器械申领单", f"{department} 申领 {quantity}{device[0]['unit']}，用途：{purpose}。", "device_requests", cursor.lastrowid, self._actor_name(actor), department, submitted_at)
            self._log_audit(connection, actor, "DEVICE_REQUEST_CREATE", "device_request", cursor.lastrowid, request_no, f"提交 {device[0]['device_name']} 申领单。", ip_address)
            return self._fetch_device_request(connection, cursor.lastrowid)

    def approve_device_request(
        self,
        payload: dict[str, Any],
        actor: dict[str, Any] | None = None,
        ip_address: str | None = None,
    ) -> dict[str, Any]:
        request_id = self._required_int(payload, "request_id", "请选择申领单。")
        decision = self._optional_text(payload, "decision") or "approved"
        if decision not in {"approved", "rejected"}:
            raise ValueError("申领单审核结果不正确。")
        review_note = self._optional_text(payload, "review_note")
        reviewed_at = now_text()
        with get_connection(self.db_path) as connection:
            request = connection.execute("SELECT * FROM device_requests WHERE id = ?", (request_id,)).fetchone()
            if request is None:
                raise ValueError("申领单不存在。")
            if request["status"] != "submitted":
                raise ValueError("只有待审批申领单可以审核。")
            connection.execute(
                """
                UPDATE device_requests
                SET status = ?, reviewed_by = ?, reviewed_at = ?, review_note = ?
                WHERE id = ?
                """,
                (decision, self._actor_name(actor), reviewed_at, review_note, request_id),
            )
            self._insert_trace_event(connection, request["device_id"], "REQUEST_REVIEW", "申领单审批", review_note or f"申领单审批结果：{decision}。", "device_requests", request_id, self._actor_name(actor), "管理员工作台", reviewed_at)
            self._log_audit(connection, actor, "DEVICE_REQUEST_REVIEW", "device_request", request_id, request["request_no"], f"申领单审批结果：{decision}。", ip_address)
            return self._fetch_device_request(connection, request_id)

    def issue_device_request(
        self,
        payload: dict[str, Any],
        actor: dict[str, Any] | None = None,
        ip_address: str | None = None,
    ) -> dict[str, Any]:
        request_id = self._required_int(payload, "request_id", "请选择申领单。")
        issued_at = now_text()
        with get_connection(self.db_path) as connection:
            request = connection.execute("SELECT * FROM device_requests WHERE id = ?", (request_id,)).fetchone()
            if request is None:
                raise ValueError("申领单不存在。")
            if request["status"] != "approved":
                raise ValueError("只有已审批通过的申领单可以发放。")
            device = self._fetch_devices(connection, "WHERE d.id = ?", (request["device_id"],))[0]
            if device["stock_qty"] < request["quantity"]:
                raise ValueError("库存不足，无法发放申领器械。")
            department = connection.execute("SELECT name FROM departments WHERE id = ?", (request["department_id"],)).fetchone()["name"]
            movement_cursor = connection.execute(
                """
                INSERT INTO stock_movements (
                    device_id, movement_type, quantity, warehouse, department_id,
                    operator_name, remark, occurred_at
                )
                VALUES (?, 'REQUEST_ISSUE', ?, '中央库房', ?, ?, ?, ?)
                """,
                (request["device_id"], request["quantity"], request["department_id"], self._actor_name(actor), request["purpose"], issued_at),
            )
            connection.execute(
                """
                UPDATE devices
                SET stock_qty = stock_qty - ?,
                    status = CASE WHEN stock_qty - ? > 0 THEN 'in_stock' ELSE 'distributed' END,
                    current_location = ?,
                    last_seen_at = ?
                WHERE id = ?
                """,
                (request["quantity"], request["quantity"], department, issued_at, request["device_id"]),
            )
            connection.execute(
                """
                UPDATE device_requests
                SET status = 'issued', issued_by = ?, issued_at = ?, remark = COALESCE(?, remark)
                WHERE id = ?
                """,
                (self._actor_name(actor), issued_at, self._optional_text(payload, "remark"), request_id),
            )
            self._insert_trace_event(connection, request["device_id"], "REQUEST_ISSUE", "申领单发放出库", f"{department} 领用 {request['quantity']}{device['unit']}。", "stock_movements", movement_cursor.lastrowid, self._actor_name(actor), department, issued_at)
            self._log_audit(connection, actor, "DEVICE_REQUEST_ISSUE", "device_request", request_id, request["request_no"], "申领单已发放并扣减库存。", ip_address)
            return self._fetch_device_request(connection, request_id)

    def create_quality_report(
        self,
        payload: dict[str, Any],
        actor: dict[str, Any] | None = None,
        ip_address: str | None = None,
    ) -> dict[str, Any]:
        device_id = self._required_int(payload, "device_id", "请选择问题器械。")
        department_id = self._required_int(payload, "department_id", "请选择上报科室。")
        problem_type = self._required_text(payload, "problem_type", "问题类型不能为空。")
        severity = self._required_text(payload, "severity", "严重程度不能为空。")
        description = self._required_text(payload, "description", "问题描述不能为空。")
        patient_id = self._optional_int(payload, "patient_id")
        submitted_at = now_text()
        with get_connection(self.db_path) as connection:
            device = self._fetch_devices(connection, "WHERE d.id = ?", (device_id,))
            if not device:
                raise ValueError("器械不存在。")
            self._ensure_exists(connection, "departments", department_id, "科室不存在。")
            if patient_id:
                self._ensure_exists(connection, "patients", patient_id, "患者不存在。")
            department = connection.execute("SELECT name FROM departments WHERE id = ?", (department_id,)).fetchone()["name"]
            report_no = self._generate_number(connection, "QR", "quality_reports", "report_no")
            cursor = connection.execute(
                """
                INSERT INTO quality_reports (
                    report_no, reporter_id, reporter_name, department_id,
                    device_id, patient_id, problem_type, severity, description,
                    status, submitted_at, remark
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'submitted', ?, ?)
                """,
                (
                    report_no,
                    actor.get("id") if actor else None,
                    self._actor_name(actor),
                    department_id,
                    device_id,
                    patient_id,
                    problem_type,
                    severity,
                    description,
                    submitted_at,
                    self._optional_text(payload, "remark"),
                ),
            )
            self._insert_trace_event(connection, device_id, "QUALITY_REPORT", "质量问题上报", f"{problem_type}：{description}", "quality_reports", cursor.lastrowid, self._actor_name(actor), department, submitted_at)
            self._log_audit(connection, actor, "QUALITY_REPORT_CREATE", "quality_report", cursor.lastrowid, report_no, f"上报 {device[0]['device_name']} 质量问题。", ip_address)
            return self._fetch_quality_report(connection, cursor.lastrowid)

    def handle_quality_report(
        self,
        payload: dict[str, Any],
        actor: dict[str, Any] | None = None,
        ip_address: str | None = None,
    ) -> dict[str, Any]:
        report_id = self._required_int(payload, "report_id", "请选择质量问题报告。")
        status = self._optional_text(payload, "status") or "processing"
        if status not in {"processing", "resolved", "rejected"}:
            raise ValueError("质量问题处理状态不正确。")
        handling_result = self._required_text(payload, "handling_result", "请填写处理意见。")
        handled_at = now_text()
        with get_connection(self.db_path) as connection:
            report = connection.execute("SELECT * FROM quality_reports WHERE id = ?", (report_id,)).fetchone()
            if report is None:
                raise ValueError("质量问题报告不存在。")
            connection.execute(
                """
                UPDATE quality_reports
                SET status = ?, handled_by = ?, handled_at = ?,
                    handling_result = ?, remark = COALESCE(?, remark)
                WHERE id = ?
                """,
                (status, self._actor_name(actor), handled_at, handling_result, self._optional_text(payload, "remark"), report_id),
            )
            self._insert_trace_event(connection, report["device_id"], "QUALITY_HANDLE", "质量问题处理", handling_result, "quality_reports", report_id, self._actor_name(actor), "质控办", handled_at)
            self._log_audit(connection, actor, "QUALITY_REPORT_HANDLE", "quality_report", report_id, report["report_no"], f"质量问题处理状态：{status}。", ip_address)
            return self._fetch_quality_report(connection, report_id)

    def create_transfer(
        self,
        payload: dict[str, Any],
        actor: dict[str, Any] | None = None,
        ip_address: str | None = None,
    ) -> dict[str, Any]:
        device_id = self._required_int(payload, "device_id", "请选择调拨器械。")
        quantity = self._required_positive_int(payload, "quantity", "调拨数量必须大于 0。")
        from_department_id = self._required_int(payload, "from_department_id", "请选择调出科室。")
        to_department_id = self._required_int(payload, "to_department_id", "请选择调入科室。")
        reason = self._required_text(payload, "reason", "调拨原因不能为空。")
        transferred_at = now_text()
        with get_connection(self.db_path) as connection:
            device = self._fetch_devices(connection, "WHERE d.id = ?", (device_id,))
            if not device:
                raise ValueError("器械不存在。")
            self._ensure_exists(connection, "departments", from_department_id, "调出科室不存在。")
            self._ensure_exists(connection, "departments", to_department_id, "调入科室不存在。")
            if device[0]["stock_qty"] < quantity:
                raise ValueError("库存不足，无法调拨。")
            from_department = connection.execute("SELECT name FROM departments WHERE id = ?", (from_department_id,)).fetchone()["name"]
            to_department = connection.execute("SELECT name FROM departments WHERE id = ?", (to_department_id,)).fetchone()["name"]
            transfer_no = self._generate_number(connection, "TF", "department_transfers", "transfer_no")
            cursor = connection.execute(
                """
                INSERT INTO department_transfers (
                    transfer_no, device_id, quantity, from_department_id,
                    to_department_id, operator_name, reason, transferred_at, remark
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    transfer_no,
                    device_id,
                    quantity,
                    from_department_id,
                    to_department_id,
                    self._actor_name(actor),
                    reason,
                    transferred_at,
                    self._optional_text(payload, "remark"),
                ),
            )
            connection.execute(
                """
                INSERT INTO stock_movements (
                    device_id, movement_type, quantity, warehouse, department_id,
                    operator_name, remark, occurred_at
                )
                VALUES (?, 'TRANSFER', ?, ?, ?, ?, ?, ?)
                """,
                (device_id, quantity, from_department, to_department_id, self._actor_name(actor), reason, transferred_at),
            )
            connection.execute(
                """
                UPDATE devices
                SET current_location = ?, last_seen_at = ?
                WHERE id = ?
                """,
                (to_department, transferred_at, device_id),
            )
            self._insert_trace_event(connection, device_id, "TRANSFER", "完成科室调拨", f"{from_department} 调拨至 {to_department}，数量 {quantity}{device[0]['unit']}。", "department_transfers", cursor.lastrowid, self._actor_name(actor), to_department, transferred_at)
            self._log_audit(connection, actor, "TRANSFER_CREATE", "transfer", cursor.lastrowid, transfer_no, f"{from_department} 调拨至 {to_department}。", ip_address)
            return self._fetch_transfer(connection, cursor.lastrowid)

    def create_device(
        self,
        payload: dict[str, Any],
        actor: dict[str, Any] | None = None,
        ip_address: str | None = None,
    ) -> dict[str, Any]:
        device_name = self._required_text(payload, "device_name", "器械名称不能为空。")
        category = self._required_text(payload, "category", "器械分类不能为空。")
        trace_mode = self._required_text(payload, "trace_mode", "识别方式不能为空。")
        risk_level = self._required_text(payload, "risk_level", "风险等级不能为空。")
        supplier_id = self._required_int(payload, "supplier_id", "供应商不能为空。")
        udi_code = self._required_text(payload, "udi_code", "UDI 编码不能为空。")

        with get_connection(self.db_path) as connection:
            self._ensure_exists(connection, "suppliers", supplier_id, "供应商不存在。")
            cursor = connection.execute(
                """
                INSERT INTO devices (
                    device_name, category, trace_mode, risk_level, model, specification,
                    manufacturer, supplier_id, unit, batch_no, serial_no, production_date,
                    expiry_date, reorder_threshold, stock_qty, status, current_location,
                    last_seen_at, is_high_value, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 'registered', '待入库', ?, ?, ?)
                """,
                (
                    device_name,
                    category,
                    trace_mode,
                    risk_level,
                    self._optional_text(payload, "model"),
                    self._optional_text(payload, "specification"),
                    self._optional_text(payload, "manufacturer"),
                    supplier_id,
                    self._optional_text(payload, "unit") or "件",
                    self._optional_text(payload, "batch_no"),
                    self._optional_text(payload, "serial_no"),
                    self._optional_text(payload, "production_date"),
                    self._optional_text(payload, "expiry_date"),
                    self._optional_int(payload, "reorder_threshold", 0),
                    now_text(),
                    1 if trace_mode == "RFID" else 0,
                    now_text(),
                ),
            )
            device_id = cursor.lastrowid
            mappings = self._collect_code_pairs(payload)
            if "UDI" not in {code_type for code_type, _ in mappings}:
                mappings.append(("UDI", udi_code))
            self._insert_code_mappings(connection, device_id, mappings)
            self._insert_trace_event(
                connection,
                device_id,
                "REGISTER",
                "完成主数据登记",
                "新增器械主数据并完成多码映射。",
                "devices",
                device_id,
                self._actor_name(actor),
                "基础信息中心",
                now_text(),
            )
            self._log_audit(
                connection,
                actor,
                "CREATE_DEVICE",
                "device",
                device_id,
                device_name,
                "创建器械主数据并完成多码映射。",
                ip_address,
            )
            row = self._fetch_devices(connection, "WHERE d.id = ?", (device_id,))[0]
            return self._serialize_device(row)

    def record_inbound(
        self,
        payload: dict[str, Any],
        actor: dict[str, Any] | None = None,
        ip_address: str | None = None,
    ) -> dict[str, Any]:
        identifier = self._required_text(payload, "identifier", "请输入器械编码。")
        quantity = self._required_positive_int(payload, "quantity", "入库数量必须大于 0。")
        warehouse = self._required_text(payload, "warehouse", "入库位置不能为空。")
        operator_name = self._operator_name(payload, actor)
        remark = self._optional_text(payload, "remark")
        occurred_at = now_text()

        with get_connection(self.db_path) as connection:
            device = self._resolve_device(connection, identifier)
            if device is None:
                raise ValueError("未找到对应器械，请检查编码。")

            connection.execute(
                """
                UPDATE devices
                SET stock_qty = stock_qty + ?, status = 'in_stock', current_location = ?, last_seen_at = ?
                WHERE id = ?
                """,
                (quantity, warehouse, occurred_at, device["id"]),
            )
            cursor = connection.execute(
                """
                INSERT INTO stock_movements (
                    device_id, movement_type, quantity, warehouse, department_id,
                    operator_name, remark, occurred_at
                )
                VALUES (?, 'INBOUND', ?, ?, NULL, ?, ?, ?)
                """,
                (device["id"], quantity, warehouse, operator_name, remark, occurred_at),
            )
            self._insert_trace_event(
                connection,
                device["id"],
                "INBOUND",
                "完成入库",
                f"{device['device_name']} 入库 {quantity}{device['unit']}。",
                "stock_movements",
                cursor.lastrowid,
                operator_name,
                warehouse,
                occurred_at,
            )
            self._log_audit(
                connection,
                actor,
                "INBOUND",
                "device",
                device["id"],
                device["device_name"],
                f"入库 {quantity}{device['unit']}，位置：{warehouse}。",
                ip_address,
            )
            row = self._fetch_devices(connection, "WHERE d.id = ?", (device["id"],))[0]
            return self._serialize_device(row)

    def record_outbound(
        self,
        payload: dict[str, Any],
        actor: dict[str, Any] | None = None,
        ip_address: str | None = None,
    ) -> dict[str, Any]:
        identifier = self._required_text(payload, "identifier", "请输入器械编码。")
        quantity = self._required_positive_int(payload, "quantity", "出库数量必须大于 0。")
        department_id = self._required_int(payload, "department_id", "请选择出库科室。")
        operator_name = self._operator_name(payload, actor)
        remark = self._optional_text(payload, "remark")
        occurred_at = now_text()

        with get_connection(self.db_path) as connection:
            device = self._resolve_device(connection, identifier)
            if device is None:
                raise ValueError("未找到对应器械，请检查编码。")
            self._ensure_exists(connection, "departments", department_id, "科室不存在。")
            if device["stock_qty"] < quantity:
                raise ValueError("库存不足，无法完成出库。")
            department = connection.execute(
                "SELECT name FROM departments WHERE id = ?",
                (department_id,),
            ).fetchone()["name"]
            new_status = "in_stock" if device["stock_qty"] - quantity > 0 else "distributed"
            cursor = connection.execute(
                """
                INSERT INTO stock_movements (
                    device_id, movement_type, quantity, warehouse, department_id,
                    operator_name, remark, occurred_at
                )
                VALUES (?, 'OUTBOUND', ?, '中央库房', ?, ?, ?, ?)
                """,
                (device["id"], quantity, department_id, operator_name, remark, occurred_at),
            )
            connection.execute(
                """
                UPDATE devices
                SET stock_qty = stock_qty - ?, status = ?, current_location = ?, last_seen_at = ?
                WHERE id = ?
                """,
                (quantity, new_status, department, occurred_at, device["id"]),
            )
            self._insert_trace_event(
                connection,
                device["id"],
                "OUTBOUND",
                "完成出库",
                f"{device['device_name']} 出库至 {department}，数量 {quantity}{device['unit']}。",
                "stock_movements",
                cursor.lastrowid,
                operator_name,
                department,
                occurred_at,
            )
            self._log_audit(
                connection,
                actor,
                "OUTBOUND",
                "device",
                device["id"],
                device["device_name"],
                f"出库 {quantity}{device['unit']} 至 {department}。",
                ip_address,
            )
            row = self._fetch_devices(connection, "WHERE d.id = ?", (device["id"],))[0]
            return self._serialize_device(row)

    def record_stocktake(
        self,
        payload: dict[str, Any],
        actor: dict[str, Any] | None = None,
        ip_address: str | None = None,
    ) -> dict[str, Any]:
        codes = payload.get("codes")
        operator_name = self._operator_name(payload, actor)
        if not isinstance(codes, list) or not codes:
            raise ValueError("请至少提供一条盘点编码。")

        occurred_at = now_text()
        recognized: list[dict[str, Any]] = []
        seen_ids: set[int] = set()
        with get_connection(self.db_path) as connection:
            for raw_code in codes:
                if not str(raw_code).strip():
                    continue
                device = self._resolve_device(connection, str(raw_code))
                if device is None or device["id"] in seen_ids:
                    continue
                seen_ids.add(device["id"])
                cursor = connection.execute(
                    """
                    INSERT INTO stock_movements (
                        device_id, movement_type, quantity, warehouse, department_id,
                        operator_name, remark, occurred_at
                    )
                    VALUES (?, 'STOCKTAKE', 0, ?, NULL, ?, ?, ?)
                    """,
                    (device["id"], device["current_location"], operator_name, "批量盘点模拟", occurred_at),
                )
                connection.execute(
                    "UPDATE devices SET last_seen_at = ? WHERE id = ?",
                    (occurred_at, device["id"]),
                )
                self._insert_trace_event(
                    connection,
                    device["id"],
                    "STOCKTAKE",
                    "完成盘点",
                    "批量盘点识别成功，已刷新最近盘点时间。",
                    "stock_movements",
                    cursor.lastrowid,
                    operator_name,
                    device["current_location"],
                    occurred_at,
                )
                self._log_audit(
                    connection,
                    actor,
                    "STOCKTAKE",
                    "device",
                    device["id"],
                    device["device_name"],
                    "完成盘点识别。",
                    ip_address,
                )
                row = self._fetch_devices(connection, "WHERE d.id = ?", (device["id"],))[0]
                recognized.append(self._serialize_device(row))
        return {"count": len(recognized), "recognized": recognized, "occurred_at": occurred_at}

    def record_clinical_use(
        self,
        payload: dict[str, Any],
        actor: dict[str, Any] | None = None,
        ip_address: str | None = None,
    ) -> dict[str, Any]:
        identifier = self._required_text(payload, "identifier", "请输入器械编码。")
        patient_id = self._required_int(payload, "patient_id", "请选择患者。")
        department_id = self._required_int(payload, "department_id", "请选择科室。")
        operation_name = self._required_text(payload, "operation_name", "手术名称不能为空。")
        operator_name = self._operator_name(payload, actor)
        quantity = self._required_positive_int(payload, "quantity", "使用数量必须大于 0。")
        remark = self._optional_text(payload, "remark")
        used_at = now_text()

        with get_connection(self.db_path) as connection:
            device = self._resolve_device(connection, identifier)
            if device is None:
                raise ValueError("未找到对应器械，请检查编码。")
            self._ensure_exists(connection, "patients", patient_id, "患者不存在。")
            self._ensure_exists(connection, "departments", department_id, "科室不存在。")
            if device["stock_qty"] < quantity:
                raise ValueError("库存不足，无法登记临床使用。")

            patient = connection.execute(
                "SELECT patient_no, name FROM patients WHERE id = ?",
                (patient_id,),
            ).fetchone()
            department = connection.execute(
                "SELECT name FROM departments WHERE id = ?",
                (department_id,),
            ).fetchone()["name"]
            new_stock = device["stock_qty"] - quantity
            new_status = "used" if new_stock == 0 and device["category"] != "设备" else "in_use"
            cursor = connection.execute(
                """
                INSERT INTO clinical_usages (
                    device_id, patient_id, department_id, operation_name,
                    operator_name, quantity, used_at, remark
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (device["id"], patient_id, department_id, operation_name, operator_name, quantity, used_at, remark),
            )
            connection.execute(
                """
                UPDATE devices
                SET stock_qty = ?, status = ?, current_location = ?, last_seen_at = ?
                WHERE id = ?
                """,
                (new_stock, new_status, department, used_at, device["id"]),
            )
            self._insert_trace_event(
                connection,
                device["id"],
                "USE",
                "完成临床使用",
                f"患者 {patient['name']}({patient['patient_no']}) 使用 {quantity}{device['unit']}。",
                "clinical_usages",
                cursor.lastrowid,
                operator_name,
                department,
                used_at,
            )
            self._log_audit(
                connection,
                actor,
                "CLINICAL_USE",
                "device",
                device["id"],
                device["device_name"],
                f"患者 {patient['name']} 使用 {quantity}{device['unit']}。",
                ip_address,
            )
            return {
                "usage_id": cursor.lastrowid,
                "device_name": device["device_name"],
                "patient_name": patient["name"],
                "department_name": department,
                "remaining_stock": new_stock,
            }

    def record_maintenance(
        self,
        payload: dict[str, Any],
        actor: dict[str, Any] | None = None,
        ip_address: str | None = None,
    ) -> dict[str, Any]:
        identifier = self._required_text(payload, "identifier", "请输入器械编码。")
        record_type = self._required_text(payload, "record_type", "维护类型不能为空。")
        operator_name = self._operator_name(payload, actor)
        plan_date = self._optional_text(payload, "plan_date") or date.today().isoformat()
        completed_at = self._normalize_datetime(self._optional_text(payload, "completed_at")) or now_text()
        next_due_date = self._optional_text(payload, "next_due_date")
        result = self._optional_text(payload, "result")
        vendor_name = self._optional_text(payload, "vendor_name")
        remark = self._optional_text(payload, "remark")

        with get_connection(self.db_path) as connection:
            device = self._resolve_device(connection, identifier)
            if device is None:
                raise ValueError("未找到对应器械，请检查编码。")
            cursor = connection.execute(
                """
                INSERT INTO maintenance_records (
                    device_id, record_type, plan_date, completed_at, result,
                    vendor_name, operator_name, next_due_date, remark
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    device["id"],
                    record_type,
                    plan_date,
                    completed_at,
                    result,
                    vendor_name,
                    operator_name,
                    next_due_date,
                    remark,
                ),
            )
            connection.execute(
                """
                UPDATE devices
                SET status = 'in_stock', current_location = COALESCE(current_location, '设备科'), last_seen_at = ?
                WHERE id = ?
                """,
                (completed_at, device["id"]),
            )
            self._insert_trace_event(
                connection,
                device["id"],
                "MAINTENANCE",
                "完成维护/校准",
                f"{record_type}已登记，结果：{result or '未填写'}。",
                "maintenance_records",
                cursor.lastrowid,
                operator_name,
                device["current_location"] or "设备科",
                completed_at,
            )
            self._log_audit(
                connection,
                actor,
                "MAINTENANCE",
                "device",
                device["id"],
                device["device_name"],
                f"登记{record_type}，结果：{result or '未填写'}。",
                ip_address,
            )
            return {
                "maintenance_id": cursor.lastrowid,
                "device_name": device["device_name"],
                "record_type": record_type,
                "next_due_date": next_due_date,
            }

    def create_procurement(
        self,
        payload: dict[str, Any],
        actor: dict[str, Any] | None = None,
        ip_address: str | None = None,
    ) -> dict[str, Any]:
        device_id = self._required_int(payload, "device_id", "请选择器械。")
        quantity = self._required_positive_int(payload, "quantity", "采购数量必须大于 0。")
        unit_price = self._required_float(payload, "unit_price", "采购单价不能为空。")
        purpose = self._required_text(payload, "purpose", "采购用途不能为空。")
        supplier_id = self._optional_int(payload, "supplier_id")
        expected_arrival_date = self._optional_text(payload, "expected_arrival_date")
        remark = self._optional_text(payload, "remark")

        with get_connection(self.db_path) as connection:
            self._ensure_exists(connection, "devices", device_id, "器械不存在。")
            device = self._fetch_devices(connection, "WHERE d.id = ?", (device_id,))[0]
            final_supplier_id = supplier_id or device["supplier_id"]
            self._ensure_exists(connection, "suppliers", final_supplier_id, "供应商不存在。")
            request_no = self._generate_number(connection, "PR", "procurements", "request_no")
            cursor = connection.execute(
                """
                INSERT INTO procurements (
                    request_no, device_id, supplier_id, quantity, unit_price, purpose,
                    expected_arrival_date, status, requested_by, requested_at, remark
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, 'requested', ?, ?, ?)
                """,
                (
                    request_no,
                    device_id,
                    final_supplier_id,
                    quantity,
                    unit_price,
                    purpose,
                    expected_arrival_date,
                    self._actor_name(actor),
                    now_text(),
                    remark,
                ),
            )
            self._insert_trace_event(
                connection,
                device_id,
                "PROCUREMENT_REQUEST",
                "发起采购申请",
                f"申请采购 {quantity}{device['unit']}，用途：{purpose}。",
                "procurements",
                cursor.lastrowid,
                self._actor_name(actor),
                "采购管理",
                now_text(),
            )
            self._log_audit(
                connection,
                actor,
                "PROCUREMENT_REQUEST",
                "procurement",
                cursor.lastrowid,
                request_no,
                f"为 {device['device_name']} 发起采购申请。",
                ip_address,
            )
            return self._fetch_procurement(connection, cursor.lastrowid)

    def approve_procurement(
        self,
        payload: dict[str, Any],
        actor: dict[str, Any] | None = None,
        ip_address: str | None = None,
    ) -> dict[str, Any]:
        procurement_id = self._required_int(payload, "procurement_id", "请选择采购单。")
        unit_price = self._optional_float(payload, "unit_price")
        expected_arrival_date = self._optional_text(payload, "expected_arrival_date")
        remark = self._optional_text(payload, "remark")

        with get_connection(self.db_path) as connection:
            procurement = connection.execute(
                "SELECT * FROM procurements WHERE id = ?",
                (procurement_id,),
            ).fetchone()
            if procurement is None:
                raise ValueError("采购单不存在。")
            if procurement["status"] not in {"requested", "approved"}:
                raise ValueError("当前采购单状态不可审批。")
            updates = {
                "unit_price": unit_price if unit_price is not None else procurement["unit_price"],
                "expected_arrival_date": expected_arrival_date or procurement["expected_arrival_date"],
                "remark": remark or procurement["remark"],
            }
            connection.execute(
                """
                UPDATE procurements
                SET status = 'approved',
                    approved_by = ?,
                    approved_at = ?,
                    unit_price = ?,
                    expected_arrival_date = ?,
                    remark = ?
                WHERE id = ?
                """,
                (
                    self._actor_name(actor),
                    now_text(),
                    updates["unit_price"],
                    updates["expected_arrival_date"],
                    updates["remark"],
                    procurement_id,
                ),
            )
            device = self._fetch_devices(connection, "WHERE d.id = ?", (procurement["device_id"],))[0]
            self._insert_trace_event(
                connection,
                procurement["device_id"],
                "PROCUREMENT_APPROVE",
                "采购申请审批通过",
                f"采购单 {procurement['request_no']} 已审批，待到货入库。",
                "procurements",
                procurement_id,
                self._actor_name(actor),
                "采购管理",
                now_text(),
            )
            self._log_audit(
                connection,
                actor,
                "PROCUREMENT_APPROVE",
                "procurement",
                procurement_id,
                procurement["request_no"],
                f"审批通过 {device['device_name']} 采购单。",
                ip_address,
            )
            return self._fetch_procurement(connection, procurement_id)

    def receive_procurement(
        self,
        payload: dict[str, Any],
        actor: dict[str, Any] | None = None,
        ip_address: str | None = None,
    ) -> dict[str, Any]:
        procurement_id = self._required_int(payload, "procurement_id", "请选择采购单。")
        warehouse = self._required_text(payload, "warehouse", "入库位置不能为空。")
        remark = self._optional_text(payload, "remark")
        operator_name = self._operator_name(payload, actor)
        received_at = now_text()

        with get_connection(self.db_path) as connection:
            procurement = connection.execute(
                "SELECT * FROM procurements WHERE id = ?",
                (procurement_id,),
            ).fetchone()
            if procurement is None:
                raise ValueError("采购单不存在。")
            if procurement["status"] != "approved":
                raise ValueError("只有已审批采购单才能执行收货入库。")
            device = self._fetch_devices(connection, "WHERE d.id = ?", (procurement["device_id"],))[0]
            movement_cursor = connection.execute(
                """
                INSERT INTO stock_movements (
                    device_id, movement_type, quantity, warehouse, department_id,
                    operator_name, remark, occurred_at
                )
                VALUES (?, 'INBOUND', ?, ?, NULL, ?, ?, ?)
                """,
                (
                    procurement["device_id"],
                    procurement["quantity"],
                    warehouse,
                    operator_name,
                    remark or "采购到货转入库存",
                    received_at,
                ),
            )
            connection.execute(
                """
                UPDATE devices
                SET stock_qty = stock_qty + ?,
                    status = 'in_stock',
                    current_location = ?,
                    last_seen_at = ?
                WHERE id = ?
                """,
                (procurement["quantity"], warehouse, received_at, procurement["device_id"]),
            )
            connection.execute(
                """
                UPDATE procurements
                SET status = 'completed',
                    received_by = ?,
                    received_at = ?,
                    inbound_completed_at = ?,
                    remark = COALESCE(?, remark)
                WHERE id = ?
                """,
                (operator_name, received_at, received_at, remark, procurement_id),
            )
            self._insert_trace_event(
                connection,
                procurement["device_id"],
                "PROCUREMENT_RECEIVE",
                "采购到货并完成入库",
                f"采购单 {procurement['request_no']} 到货入库 {procurement['quantity']}{device['unit']}。",
                "procurements",
                procurement_id,
                operator_name,
                warehouse,
                received_at,
            )
            self._insert_trace_event(
                connection,
                procurement["device_id"],
                "INBOUND",
                "采购闭环完成",
                f"已从采购闭环转为库存，关联采购单 {procurement['request_no']}。",
                "stock_movements",
                movement_cursor.lastrowid,
                operator_name,
                warehouse,
                received_at,
            )
            self._log_audit(
                connection,
                actor,
                "PROCUREMENT_RECEIVE",
                "procurement",
                procurement_id,
                procurement["request_no"],
                f"完成到货入库，数量 {procurement['quantity']}{device['unit']}。",
                ip_address,
            )
            return self._fetch_procurement(connection, procurement_id)

    def create_recall_case(
        self,
        payload: dict[str, Any],
        actor: dict[str, Any] | None = None,
        ip_address: str | None = None,
    ) -> dict[str, Any]:
        reason = self._required_text(payload, "reason", "召回原因不能为空。")
        severity = self._required_text(payload, "severity", "请选择召回等级。")
        batch_no = self._optional_text(payload, "batch_no")
        identifier = self._optional_text(payload, "identifier")

        with get_connection(self.db_path) as connection:
            devices = []
            scope_type = "batch"
            target_device_id = None
            if batch_no:
                devices = self._fetch_devices(connection, "WHERE d.batch_no = ?", (batch_no,))
                if not devices:
                    raise ValueError("未找到该批次器械。")
                target_device_id = devices[0]["id"]
            elif identifier:
                device = self._resolve_device(connection, identifier)
                if device is None:
                    raise ValueError("未找到对应器械，请检查编码。")
                devices = [device]
                batch_no = device["batch_no"]
                scope_type = "device"
                target_device_id = device["id"]
            else:
                raise ValueError("请提供批次号或器械编码。")

            recall_no = self._generate_number(connection, "RC", "recall_cases", "recall_no")
            cursor = connection.execute(
                """
                INSERT INTO recall_cases (
                    recall_no, scope_type, device_id, batch_no, reason,
                    severity, status, initiated_by, initiated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, 'open', ?, ?)
                """,
                (
                    recall_no,
                    scope_type,
                    target_device_id,
                    batch_no,
                    reason,
                    severity,
                    self._actor_name(actor),
                    now_text(),
                ),
            )
            recall_id = cursor.lastrowid
            affected_patients: set[int] = set()
            affected_departments: set[int] = set()
            for device in devices:
                connection.execute(
                    """
                    INSERT INTO recall_impacts (
                        recall_case_id, device_id, patient_id, department_id, impact_type, status, note
                    )
                    VALUES (?, ?, NULL, NULL, 'device', 'pending', '待核查库存与使用流向')
                    """,
                    (recall_id, device["id"]),
                )
                usages = connection.execute(
                    """
                    SELECT DISTINCT patient_id, department_id
                    FROM clinical_usages
                    WHERE device_id = ?
                    """,
                    (device["id"],),
                ).fetchall()
                for usage in usages:
                    if usage["patient_id"] not in affected_patients:
                        affected_patients.add(usage["patient_id"])
                        affected_departments.add(usage["department_id"])
                        connection.execute(
                            """
                            INSERT INTO recall_impacts (
                                recall_case_id, device_id, patient_id, department_id, impact_type, status, note
                            )
                            VALUES (?, NULL, ?, ?, 'patient', 'pending', '需通知临床复核使用记录')
                            """,
                            (recall_id, usage["patient_id"], usage["department_id"]),
                        )
                connection.execute(
                    "UPDATE devices SET status = 'recalled' WHERE id = ?",
                    (device["id"],),
                )
                self._insert_trace_event(
                    connection,
                    device["id"],
                    "RECALL",
                    "触发召回预警",
                    f"召回单 {recall_no} 已创建，原因：{reason}",
                    "recall_cases",
                    recall_id,
                    self._actor_name(actor),
                    "质量管理办公室",
                    now_text(),
                )

            self._log_audit(
                connection,
                actor,
                "RECALL_CREATE",
                "recall",
                recall_id,
                recall_no,
                f"发起 {scope_type} 召回，影响器械 {len(devices)} 台/批。",
                ip_address,
            )
            return self._fetch_recall(connection, recall_id)

    def close_recall_case(
        self,
        payload: dict[str, Any],
        actor: dict[str, Any] | None = None,
        ip_address: str | None = None,
    ) -> dict[str, Any]:
        recall_case_id = self._required_int(payload, "recall_case_id", "请选择召回单。")
        disposal_note = self._required_text(payload, "disposal_note", "请填写处置说明。")
        closed_at = now_text()

        with get_connection(self.db_path) as connection:
            recall = connection.execute(
                "SELECT * FROM recall_cases WHERE id = ?",
                (recall_case_id,),
            ).fetchone()
            if recall is None:
                raise ValueError("召回单不存在。")
            if recall["status"] == "closed":
                raise ValueError("该召回单已关闭。")
            connection.execute(
                """
                UPDATE recall_cases
                SET status = 'closed',
                    disposal_note = ?,
                    closed_at = ?
                WHERE id = ?
                """,
                (disposal_note, closed_at, recall_case_id),
            )
            device_ids = [
                row["device_id"]
                for row in connection.execute(
                    """
                    SELECT DISTINCT device_id
                    FROM recall_impacts
                    WHERE recall_case_id = ? AND device_id IS NOT NULL
                    """,
                    (recall_case_id,),
                ).fetchall()
            ]
            for device_id in device_ids:
                device = connection.execute(
                    "SELECT stock_qty, status FROM devices WHERE id = ?",
                    (device_id,),
                ).fetchone()
                if device["status"] == "scrapped":
                    continue
                restored_status = "in_stock" if device["stock_qty"] > 0 else "used"
                connection.execute(
                    "UPDATE devices SET status = ? WHERE id = ?",
                    (restored_status, device_id),
                )
                self._insert_trace_event(
                    connection,
                    device_id,
                    "RECALL_CLOSE",
                    "召回流程关闭",
                    disposal_note,
                    "recall_cases",
                    recall_case_id,
                    self._actor_name(actor),
                    "质量管理办公室",
                    closed_at,
                )
            self._log_audit(
                connection,
                actor,
                "RECALL_CLOSE",
                "recall",
                recall_case_id,
                recall["recall_no"],
                "关闭召回并填写处置说明。",
                ip_address,
            )
            return self._fetch_recall(connection, recall_case_id)

    def create_scrap_request(
        self,
        payload: dict[str, Any],
        actor: dict[str, Any] | None = None,
        ip_address: str | None = None,
    ) -> dict[str, Any]:
        identifier = self._required_text(payload, "identifier", "请输入器械编码。")
        quantity = self._required_positive_int(payload, "quantity", "报废数量必须大于 0。")
        reason = self._required_text(payload, "reason", "报废原因不能为空。")
        remark = self._optional_text(payload, "remark")
        requested_at = now_text()

        with get_connection(self.db_path) as connection:
            device = self._resolve_device(connection, identifier)
            if device is None:
                raise ValueError("未找到对应器械，请检查编码。")
            if device["stock_qty"] < quantity:
                raise ValueError("库存不足，无法申请报废。")
            scrap_no = self._generate_number(connection, "SC", "scrap_requests", "scrap_no")
            cursor = connection.execute(
                """
                INSERT INTO scrap_requests (
                    scrap_no, device_id, quantity, reason, status,
                    requested_by, requested_at, remark
                )
                VALUES (?, ?, ?, ?, 'requested', ?, ?, ?)
                """,
                (scrap_no, device["id"], quantity, reason, self._actor_name(actor), requested_at, remark),
            )
            self._insert_trace_event(
                connection,
                device["id"],
                "SCRAP_REQUEST",
                "发起报废申请",
                f"申请报废 {quantity}{device['unit']}，原因：{reason}",
                "scrap_requests",
                cursor.lastrowid,
                self._actor_name(actor),
                device["current_location"],
                requested_at,
            )
            self._log_audit(
                connection,
                actor,
                "SCRAP_REQUEST",
                "scrap",
                cursor.lastrowid,
                scrap_no,
                f"为 {device['device_name']} 发起报废申请。",
                ip_address,
            )
            return self._fetch_scrap(connection, cursor.lastrowid)

    def approve_scrap_request(
        self,
        payload: dict[str, Any],
        actor: dict[str, Any] | None = None,
        ip_address: str | None = None,
    ) -> dict[str, Any]:
        scrap_request_id = self._required_int(payload, "scrap_request_id", "请选择报废单。")
        remark = self._optional_text(payload, "remark")

        with get_connection(self.db_path) as connection:
            scrap = connection.execute(
                "SELECT * FROM scrap_requests WHERE id = ?",
                (scrap_request_id,),
            ).fetchone()
            if scrap is None:
                raise ValueError("报废单不存在。")
            if scrap["status"] != "requested":
                raise ValueError("只有待审批报废单可以审批。")
            connection.execute(
                """
                UPDATE scrap_requests
                SET status = 'approved',
                    approved_by = ?,
                    approved_at = ?,
                    remark = COALESCE(?, remark)
                WHERE id = ?
                """,
                (self._actor_name(actor), now_text(), remark, scrap_request_id),
            )
            self._insert_trace_event(
                connection,
                scrap["device_id"],
                "SCRAP_APPROVE",
                "报废申请审批通过",
                f"报废单 {scrap['scrap_no']} 已审批通过。",
                "scrap_requests",
                scrap_request_id,
                self._actor_name(actor),
                "管理办公室",
                now_text(),
            )
            self._log_audit(
                connection,
                actor,
                "SCRAP_APPROVE",
                "scrap",
                scrap_request_id,
                scrap["scrap_no"],
                "报废申请审批通过。",
                ip_address,
            )
            return self._fetch_scrap(connection, scrap_request_id)

    def dispose_scrap_request(
        self,
        payload: dict[str, Any],
        actor: dict[str, Any] | None = None,
        ip_address: str | None = None,
    ) -> dict[str, Any]:
        scrap_request_id = self._required_int(payload, "scrap_request_id", "请选择报废单。")
        remark = self._optional_text(payload, "remark")
        disposed_at = now_text()

        with get_connection(self.db_path) as connection:
            scrap = connection.execute(
                "SELECT * FROM scrap_requests WHERE id = ?",
                (scrap_request_id,),
            ).fetchone()
            if scrap is None:
                raise ValueError("报废单不存在。")
            if scrap["status"] != "approved":
                raise ValueError("只有已审批报废单才能执行处置。")
            device = self._fetch_devices(connection, "WHERE d.id = ?", (scrap["device_id"],))[0]
            if device["stock_qty"] < scrap["quantity"]:
                raise ValueError("当前库存已不足，无法完成报废处置。")
            new_stock = device["stock_qty"] - scrap["quantity"]
            new_status = "scrapped" if new_stock == 0 else "in_stock"
            connection.execute(
                """
                UPDATE devices
                SET stock_qty = ?, status = ?, last_seen_at = ?
                WHERE id = ?
                """,
                (new_stock, new_status, disposed_at, device["id"]),
            )
            connection.execute(
                """
                UPDATE scrap_requests
                SET status = 'disposed',
                    disposed_by = ?,
                    disposed_at = ?,
                    remark = COALESCE(?, remark)
                WHERE id = ?
                """,
                (self._actor_name(actor), disposed_at, remark, scrap_request_id),
            )
            self._insert_trace_event(
                connection,
                device["id"],
                "SCRAP",
                "完成报废处置",
                f"报废 {scrap['quantity']}{device['unit']}，原因：{scrap['reason']}",
                "scrap_requests",
                scrap_request_id,
                self._actor_name(actor),
                device["current_location"],
                disposed_at,
            )
            self._log_audit(
                connection,
                actor,
                "SCRAP_DISPOSE",
                "scrap",
                scrap_request_id,
                scrap["scrap_no"],
                f"完成报废处置，数量 {scrap['quantity']}{device['unit']}。",
                ip_address,
            )
            return self._fetch_scrap(connection, scrap_request_id)

    def search_traceability(self, keyword: str) -> dict[str, Any]:
        query = keyword.strip()
        if not query:
            raise ValueError("请输入查询关键字。")

        with get_connection(self.db_path) as connection:
            direct_device = self._resolve_device(connection, query)
            if direct_device is not None:
                return self._build_device_trace(connection, direct_device["id"], query, "device")

            batch_devices = self._fetch_devices(connection, "WHERE d.batch_no = ?", (query,))
            if batch_devices:
                return self._build_batch_trace(connection, query, batch_devices)

            patient = connection.execute(
                """
                SELECT id, patient_no, name, gender, age
                FROM patients
                WHERE patient_no = ? OR name = ?
                LIMIT 1
                """,
                (query, query),
            ).fetchone()
            if patient:
                return self._build_patient_trace(connection, patient["id"], query)

        return {
            "query": query,
            "mode": "none",
            "summary": None,
            "related_devices": [],
            "impacted_patients": [],
            "timeline": [],
            "forward_path": [],
            "reverse_relations": {},
            "mappings": [],
            "graph": {"nodes": [], "edges": []},
        }

    def resolve_device_snapshot(self, identifier: str) -> dict[str, Any] | None:
        with get_connection(self.db_path) as connection:
            device = self._resolve_device(connection, identifier)
            if device is None:
                return None
            return self._build_device_trace(connection, device["id"], identifier, "device")["summary"]

    def _build_device_trace(
        self,
        connection,
        device_id: int,
        query: str,
        mode: str,
    ) -> dict[str, Any]:
        row = self._fetch_devices(connection, "WHERE d.id = ?", (device_id,))[0]
        summary = self._serialize_device(row)
        mappings = [
            dict(item)
            for item in connection.execute(
                """
                SELECT code_type, code_value
                FROM code_mappings
                WHERE device_id = ?
                ORDER BY code_type
                """,
                (device_id,),
            ).fetchall()
        ]
        impacted_patients = [
            dict(item)
            for item in connection.execute(
                """
                SELECT DISTINCT
                    p.patient_no,
                    p.name,
                    p.gender,
                    p.age,
                    cu.operation_name,
                    cu.used_at,
                    cu.quantity
                FROM clinical_usages cu
                JOIN patients p ON p.id = cu.patient_id
                WHERE cu.device_id = ?
                ORDER BY cu.used_at DESC
                """,
                (device_id,),
            ).fetchall()
        ]
        departments = [
            dict(item)
            for item in connection.execute(
                """
                SELECT DISTINCT dep.name, dep.type
                FROM clinical_usages cu
                JOIN departments dep ON dep.id = cu.department_id
                WHERE cu.device_id = ?
                ORDER BY dep.name
                """,
                (device_id,),
            ).fetchall()
        ]
        recalls = [
            dict(item)
            for item in connection.execute(
                """
                SELECT DISTINCT rc.recall_no, rc.status, rc.severity, rc.reason, rc.initiated_at
                FROM recall_cases rc
                LEFT JOIN recall_impacts ri ON ri.recall_case_id = rc.id
                WHERE rc.device_id = ?
                   OR ri.device_id = ?
                   OR rc.batch_no = ?
                ORDER BY rc.initiated_at DESC
                """,
                (device_id, device_id, summary["batch_no"]),
            ).fetchall()
        ]
        scraps = [
            dict(item)
            for item in connection.execute(
                """
                SELECT scrap_no, status, quantity, reason, requested_at, disposed_at
                FROM scrap_requests
                WHERE device_id = ?
                ORDER BY requested_at DESC
                """,
                (device_id,),
            ).fetchall()
        ]
        procurements = [
            dict(item)
            for item in connection.execute(
                """
                SELECT request_no, status, quantity, requested_at, approved_at, inbound_completed_at
                FROM procurements
                WHERE device_id = ?
                ORDER BY requested_at DESC
                """,
                (device_id,),
            ).fetchall()
        ]
        device_requests = [
            dict(item)
            for item in connection.execute(
                """
                SELECT request_no, requester_name, quantity, purpose, status, submitted_at, issued_at
                FROM device_requests
                WHERE device_id = ?
                ORDER BY submitted_at DESC
                """,
                (device_id,),
            ).fetchall()
        ]
        quality_reports = [
            dict(item)
            for item in connection.execute(
                """
                SELECT report_no, problem_type, severity, status, submitted_at, handled_at
                FROM quality_reports
                WHERE device_id = ?
                ORDER BY submitted_at DESC
                """,
                (device_id,),
            ).fetchall()
        ]
        timeline = self._fetch_timeline(connection, [device_id])
        forward_path = list(reversed(timeline))
        graph = self._device_graph(summary, impacted_patients, departments, recalls, scraps, procurements)
        return {
            "query": query,
            "mode": mode,
            "summary": summary,
            "related_devices": [summary],
            "impacted_patients": impacted_patients,
            "timeline": timeline,
            "forward_path": forward_path,
            "reverse_relations": {
                "patients": impacted_patients,
                "departments": departments,
                "recalls": recalls,
                "scraps": scraps,
                "procurements": procurements,
                "device_requests": device_requests,
                "quality_reports": quality_reports,
            },
            "mappings": mappings,
            "graph": graph,
        }

    def _build_batch_trace(self, connection, batch_no: str, rows) -> dict[str, Any]:
        device_ids = [row["id"] for row in rows]
        related_devices = [self._serialize_device(row) for row in rows]
        placeholders = ",".join("?" for _ in device_ids)
        impacted_patients = [
            dict(item)
            for item in connection.execute(
                f"""
                SELECT DISTINCT
                    p.patient_no,
                    p.name,
                    p.gender,
                    p.age,
                    cu.operation_name,
                    cu.used_at,
                    d.device_name,
                    cu.quantity
                FROM clinical_usages cu
                JOIN patients p ON p.id = cu.patient_id
                JOIN devices d ON d.id = cu.device_id
                WHERE cu.device_id IN ({placeholders})
                ORDER BY cu.used_at DESC
                """,
                device_ids,
            ).fetchall()
        ]
        departments = [
            dict(item)
            for item in connection.execute(
                f"""
                SELECT DISTINCT dep.name, dep.type
                FROM clinical_usages cu
                JOIN departments dep ON dep.id = cu.department_id
                WHERE cu.device_id IN ({placeholders})
                ORDER BY dep.name
                """,
                device_ids,
            ).fetchall()
        ]
        recalls = [
            dict(item)
            for item in connection.execute(
                """
                SELECT recall_no, status, severity, reason, initiated_at
                FROM recall_cases
                WHERE batch_no = ?
                ORDER BY initiated_at DESC
                """,
                (batch_no,),
            ).fetchall()
        ]
        timeline = self._fetch_timeline(connection, device_ids)
        forward_path = list(reversed(timeline))
        graph = self._batch_graph(batch_no, related_devices, impacted_patients, departments, recalls)
        return {
            "query": batch_no,
            "mode": "batch",
            "summary": {
                "batch_no": batch_no,
                "device_count": len(related_devices),
                "impacted_patient_count": len(impacted_patients),
            },
            "related_devices": related_devices,
            "impacted_patients": impacted_patients,
            "timeline": timeline,
            "forward_path": forward_path,
            "reverse_relations": {
                "patients": impacted_patients,
                "departments": departments,
                "recalls": recalls,
            },
            "mappings": [],
            "graph": graph,
        }

    def _build_patient_trace(self, connection, patient_id: int, query: str) -> dict[str, Any]:
        patient = connection.execute(
            "SELECT patient_no, name, gender, age FROM patients WHERE id = ?",
            (patient_id,),
        ).fetchone()
        usage_rows = connection.execute(
            """
            SELECT
                d.id,
                d.device_name,
                d.category,
                d.batch_no,
                d.serial_no,
                d.current_location,
                cu.operation_name,
                cu.used_at,
                cu.quantity
            FROM clinical_usages cu
            JOIN devices d ON d.id = cu.device_id
            WHERE cu.patient_id = ?
            ORDER BY cu.used_at DESC
            """,
            (patient_id,),
        ).fetchall()
        device_ids = [row["id"] for row in usage_rows]
        device_rows = self._fetch_devices(
            connection,
            """
            WHERE d.id IN (
                SELECT DISTINCT cu.device_id
                FROM clinical_usages cu
                WHERE cu.patient_id = ?
            )
            """,
            (patient_id,),
        )
        related_devices = [self._serialize_device(row) for row in device_rows]
        recalls = []
        if device_ids:
            placeholders = ",".join("?" for _ in device_ids)
            recalls = [
                dict(item)
                for item in connection.execute(
                    f"""
                    SELECT DISTINCT rc.recall_no, rc.status, rc.severity, rc.reason, rc.initiated_at
                    FROM recall_cases rc
                    LEFT JOIN recall_impacts ri ON ri.recall_case_id = rc.id
                    WHERE ri.patient_id = ?
                       OR rc.device_id IN ({placeholders})
                    ORDER BY rc.initiated_at DESC
                    """,
                    [patient_id] + device_ids,
                ).fetchall()
            ]
        timeline = [
            {
                "event_type": "USE",
                "event_title": f"{row['device_name']} 完成临床使用",
                "event_desc": f"手术名称：{row['operation_name']}，使用数量：{row['quantity']}",
                "actor": "临床记录",
                "location": row["current_location"],
                "occurred_at": row["used_at"],
            }
            for row in usage_rows
        ]
        graph = self._patient_graph(dict(patient), related_devices, recalls)
        return {
            "query": query,
            "mode": "patient",
            "summary": {
                "patient_no": patient["patient_no"],
                "name": patient["name"],
                "gender": patient["gender"],
                "age": patient["age"],
                "usage_count": len(usage_rows),
            },
            "related_devices": related_devices,
            "impacted_patients": [dict(patient)],
            "timeline": timeline,
            "forward_path": list(reversed(timeline)),
            "reverse_relations": {
                "devices": related_devices,
                "recalls": recalls,
            },
            "mappings": [],
            "graph": graph,
        }

    def _fetch_timeline(self, connection, device_ids: list[int]) -> list[dict[str, Any]]:
        if not device_ids:
            return []
        placeholders = ",".join("?" for _ in device_ids)
        rows = connection.execute(
            f"""
            SELECT
                d.device_name,
                te.event_type,
                te.event_title,
                te.event_desc,
                te.actor,
                te.location,
                te.occurred_at
            FROM trace_events te
            JOIN devices d ON d.id = te.device_id
            WHERE te.device_id IN ({placeholders})
            ORDER BY te.occurred_at DESC, te.id DESC
            """,
            device_ids,
        ).fetchall()
        return [dict(row) for row in rows]

    def _device_graph(
        self,
        summary: dict[str, Any],
        patients: list[dict[str, Any]],
        departments: list[dict[str, Any]],
        recalls: list[dict[str, Any]],
        scraps: list[dict[str, Any]],
        procurements: list[dict[str, Any]],
    ) -> dict[str, Any]:
        nodes = [
            {"id": f"device-{summary['id']}", "label": summary["device_name"], "type": "device"},
            {"id": f"batch-{summary['batch_no']}", "label": summary["batch_no"] or "无批次", "type": "batch"},
        ]
        edges = [{"from": f"batch-{summary['batch_no']}", "to": f"device-{summary['id']}", "label": "包含"}]
        for patient in patients:
            node_id = f"patient-{patient['patient_no']}"
            nodes.append({"id": node_id, "label": patient["name"], "type": "patient"})
            edges.append({"from": f"device-{summary['id']}", "to": node_id, "label": "使用于"})
        for department in departments:
            node_id = f"department-{department['name']}"
            nodes.append({"id": node_id, "label": department["name"], "type": "department"})
            edges.append({"from": f"device-{summary['id']}", "to": node_id, "label": "流向"})
        for recall in recalls:
            node_id = f"recall-{recall['recall_no']}"
            nodes.append({"id": node_id, "label": recall["recall_no"], "type": "recall"})
            edges.append({"from": f"device-{summary['id']}", "to": node_id, "label": "召回"})
        for scrap in scraps:
            node_id = f"scrap-{scrap['scrap_no']}"
            nodes.append({"id": node_id, "label": scrap["scrap_no"], "type": "scrap"})
            edges.append({"from": f"device-{summary['id']}", "to": node_id, "label": "报废"})
        for procurement in procurements:
            node_id = f"procurement-{procurement['request_no']}"
            nodes.append({"id": node_id, "label": procurement["request_no"], "type": "procurement"})
            edges.append({"from": node_id, "to": f"device-{summary['id']}", "label": "采购"})
        return {"nodes": self._unique_nodes(nodes), "edges": edges}

    def _batch_graph(
        self,
        batch_no: str,
        devices: list[dict[str, Any]],
        patients: list[dict[str, Any]],
        departments: list[dict[str, Any]],
        recalls: list[dict[str, Any]],
    ) -> dict[str, Any]:
        nodes = [{"id": f"batch-{batch_no}", "label": batch_no, "type": "batch"}]
        edges = []
        for device in devices:
            nodes.append({"id": f"device-{device['id']}", "label": device["device_name"], "type": "device"})
            edges.append({"from": f"batch-{batch_no}", "to": f"device-{device['id']}", "label": "包含"})
        for patient in patients:
            node_id = f"patient-{patient['patient_no']}"
            nodes.append({"id": node_id, "label": patient["name"], "type": "patient"})
            edges.append({"from": f"batch-{batch_no}", "to": node_id, "label": "影响"})
        for department in departments:
            node_id = f"department-{department['name']}"
            nodes.append({"id": node_id, "label": department["name"], "type": "department"})
            edges.append({"from": f"batch-{batch_no}", "to": node_id, "label": "流向"})
        for recall in recalls:
            node_id = f"recall-{recall['recall_no']}"
            nodes.append({"id": node_id, "label": recall["recall_no"], "type": "recall"})
            edges.append({"from": f"batch-{batch_no}", "to": node_id, "label": "召回"})
        return {"nodes": self._unique_nodes(nodes), "edges": edges}

    def _patient_graph(
        self,
        patient: dict[str, Any],
        devices: list[dict[str, Any]],
        recalls: list[dict[str, Any]],
    ) -> dict[str, Any]:
        nodes = [{"id": f"patient-{patient['patient_no']}", "label": patient["name"], "type": "patient"}]
        edges = []
        for device in devices:
            device_id = f"device-{device['id']}"
            nodes.append({"id": device_id, "label": device["device_name"], "type": "device"})
            edges.append({"from": f"patient-{patient['patient_no']}", "to": device_id, "label": "使用"})
            if device["batch_no"]:
                batch_id = f"batch-{device['batch_no']}"
                nodes.append({"id": batch_id, "label": device["batch_no"], "type": "batch"})
                edges.append({"from": device_id, "to": batch_id, "label": "批次"})
        for recall in recalls:
            node_id = f"recall-{recall['recall_no']}"
            nodes.append({"id": node_id, "label": recall["recall_no"], "type": "recall"})
            edges.append({"from": f"patient-{patient['patient_no']}", "to": node_id, "label": "关联召回"})
        return {"nodes": self._unique_nodes(nodes), "edges": edges}

    def _fetch_devices(self, connection, where_clause: str = "", params: tuple[Any, ...] = ()) -> list[Any]:
        sql = f"{DEVICE_SELECT} {where_clause} {GROUP_ORDER}"
        return connection.execute(sql, params).fetchall()

    def _resolve_device(self, connection, identifier: str):
        value = identifier.strip()
        if not value:
            return None
        where_clause = """
        WHERE d.id = ?
           OR d.batch_no = ?
           OR d.serial_no = ?
           OR cm.code_value = ?
           OR d.device_name = ?
        """
        numeric_id = int(value) if value.isdigit() else -1
        rows = self._fetch_devices(connection, where_clause, (numeric_id, value, value, value, value))
        return rows[0] if rows else None

    def _fetch_procurement(self, connection, procurement_id: int) -> dict[str, Any]:
        row = connection.execute(
            """
            SELECT
                p.id,
                p.request_no,
                p.quantity,
                p.unit_price,
                p.purpose,
                p.expected_arrival_date,
                p.status,
                p.requested_by,
                p.requested_at,
                p.approved_by,
                p.approved_at,
                p.received_by,
                p.received_at,
                p.inbound_completed_at,
                p.remark,
                d.device_name,
                d.batch_no,
                s.name AS supplier_name
            FROM procurements p
            JOIN devices d ON d.id = p.device_id
            JOIN suppliers s ON s.id = p.supplier_id
            WHERE p.id = ?
            """,
            (procurement_id,),
        ).fetchone()
        return dict(row)

    def _fetch_recall(self, connection, recall_id: int) -> dict[str, Any]:
        row = connection.execute(
            """
            SELECT
                rc.id,
                rc.recall_no,
                rc.scope_type,
                rc.batch_no,
                rc.reason,
                rc.severity,
                rc.status,
                rc.initiated_by,
                rc.initiated_at,
                rc.disposal_note,
                rc.closed_at,
                d.device_name,
                COUNT(DISTINCT CASE WHEN ri.impact_type = 'device' THEN ri.device_id END) AS affected_device_count,
                COUNT(DISTINCT CASE WHEN ri.impact_type = 'patient' THEN ri.patient_id END) AS affected_patient_count
            FROM recall_cases rc
            LEFT JOIN devices d ON d.id = rc.device_id
            LEFT JOIN recall_impacts ri ON ri.recall_case_id = rc.id
            WHERE rc.id = ?
            GROUP BY rc.id
            """,
            (recall_id,),
        ).fetchone()
        return dict(row)

    def _fetch_scrap(self, connection, scrap_id: int) -> dict[str, Any]:
        row = connection.execute(
            """
            SELECT
                sr.id,
                sr.scrap_no,
                sr.quantity,
                sr.reason,
                sr.status,
                sr.requested_by,
                sr.requested_at,
                sr.approved_by,
                sr.approved_at,
                sr.disposed_by,
                sr.disposed_at,
                sr.remark,
                d.device_name,
                d.batch_no,
                d.stock_qty,
                d.current_location
            FROM scrap_requests sr
            JOIN devices d ON d.id = sr.device_id
            WHERE sr.id = ?
            """,
            (scrap_id,),
        ).fetchone()
        return dict(row)

    def _fetch_purchase_plan(self, connection, plan_id: int) -> dict[str, Any]:
        row = connection.execute(
            """
            SELECT
                pp.id,
                pp.plan_no,
                pp.quantity,
                pp.estimated_unit_price,
                pp.reason,
                pp.source,
                pp.status,
                pp.submitted_by,
                pp.submitted_at,
                pp.reviewed_by,
                pp.reviewed_at,
                pp.review_note,
                pp.purchased_at,
                pp.remark,
                d.device_name,
                d.batch_no,
                d.stock_qty,
                d.reorder_threshold,
                d.unit,
                s.name AS supplier_name
            FROM purchase_plans pp
            JOIN devices d ON d.id = pp.device_id
            LEFT JOIN suppliers s ON s.id = pp.supplier_id
            WHERE pp.id = ?
            """,
            (plan_id,),
        ).fetchone()
        return dict(row)

    def _fetch_inbound_order(self, connection, order_id: int) -> dict[str, Any]:
        row = connection.execute(
            """
            SELECT
                io.id,
                io.order_no,
                io.quantity,
                io.warehouse,
                io.status,
                io.submitted_by,
                io.submitted_at,
                io.reviewed_by,
                io.reviewed_at,
                io.review_note,
                io.received_at,
                io.remark,
                pp.plan_no,
                p.request_no AS procurement_no,
                d.device_name,
                d.batch_no,
                d.unit,
                s.name AS supplier_name
            FROM inbound_orders io
            LEFT JOIN purchase_plans pp ON pp.id = io.plan_id
            LEFT JOIN procurements p ON p.id = io.procurement_id
            JOIN devices d ON d.id = io.device_id
            LEFT JOIN suppliers s ON s.id = io.supplier_id
            WHERE io.id = ?
            """,
            (order_id,),
        ).fetchone()
        return dict(row)

    def _fetch_device_request(self, connection, request_id: int) -> dict[str, Any]:
        row = connection.execute(
            """
            SELECT
                dr.id,
                dr.request_no,
                dr.requester_name,
                dep.name AS department_name,
                d.device_name,
                d.batch_no,
                d.unit,
                dr.quantity,
                dr.purpose,
                dr.status,
                dr.submitted_at,
                dr.reviewed_by,
                dr.reviewed_at,
                dr.review_note,
                dr.issued_by,
                dr.issued_at,
                dr.remark
            FROM device_requests dr
            JOIN departments dep ON dep.id = dr.department_id
            JOIN devices d ON d.id = dr.device_id
            WHERE dr.id = ?
            """,
            (request_id,),
        ).fetchone()
        return dict(row)

    def _fetch_quality_report(self, connection, report_id: int) -> dict[str, Any]:
        row = connection.execute(
            """
            SELECT
                qr.id,
                qr.report_no,
                qr.reporter_name,
                dep.name AS department_name,
                d.device_name,
                d.batch_no,
                p.patient_no,
                p.name AS patient_name,
                qr.problem_type,
                qr.severity,
                qr.description,
                qr.status,
                qr.submitted_at,
                qr.handled_by,
                qr.handled_at,
                qr.handling_result,
                qr.remark
            FROM quality_reports qr
            JOIN departments dep ON dep.id = qr.department_id
            JOIN devices d ON d.id = qr.device_id
            LEFT JOIN patients p ON p.id = qr.patient_id
            WHERE qr.id = ?
            """,
            (report_id,),
        ).fetchone()
        return dict(row)

    def _fetch_transfer(self, connection, transfer_id: int) -> dict[str, Any]:
        row = connection.execute(
            """
            SELECT
                dt.id,
                dt.transfer_no,
                d.device_name,
                d.batch_no,
                d.unit,
                dt.quantity,
                fd.name AS from_department,
                td.name AS to_department,
                dt.operator_name,
                dt.reason,
                dt.transferred_at,
                dt.remark
            FROM department_transfers dt
            JOIN devices d ON d.id = dt.device_id
            JOIN departments fd ON fd.id = dt.from_department_id
            JOIN departments td ON td.id = dt.to_department_id
            WHERE dt.id = ?
            """,
            (transfer_id,),
        ).fetchone()
        return dict(row)

    def _insert_code_mappings(self, connection, device_id: int, mappings: list[tuple[str, str]]) -> None:
        seen: set[str] = set()
        values_to_insert = []
        for code_type, code_value in mappings:
            clean_value = str(code_value).strip()
            if not clean_value or clean_value in seen:
                continue
            seen.add(clean_value)
            values_to_insert.append((device_id, code_type, clean_value, now_text()))
        if not values_to_insert:
            return
        try:
            connection.executemany(
                """
                INSERT INTO code_mappings (device_id, code_type, code_value, created_at)
                VALUES (?, ?, ?, ?)
                """,
                values_to_insert,
            )
        except Exception as exc:
            raise ValueError("编码值重复，无法完成多码映射。") from exc

    def _insert_trace_event(
        self,
        connection,
        device_id: int,
        event_type: str,
        event_title: str,
        event_desc: str,
        related_table: str,
        related_id: int,
        actor: str,
        location: str | None,
        occurred_at: str,
    ) -> None:
        connection.execute(
            """
            INSERT INTO trace_events (
                device_id, event_type, event_title, event_desc,
                related_table, related_id, actor, location, occurred_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (device_id, event_type, event_title, event_desc, related_table, related_id, actor, location, occurred_at),
        )

    def _log_audit(
        self,
        connection,
        actor: dict[str, Any] | None,
        action: str,
        target_type: str,
        target_id: int | None,
        target_name: str | None,
        detail: str,
        ip_address: str | None = None,
    ) -> None:
        info = self._actor_info(actor)
        connection.execute(
            """
            INSERT INTO audit_logs (
                user_id, username, role, action, target_type, target_id,
                target_name, detail, ip_address, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                info["id"],
                info["username"],
                info["role"],
                action,
                target_type,
                target_id,
                target_name,
                detail,
                ip_address,
                now_text(),
            ),
        )

    def _serialize_device(self, row) -> dict[str, Any]:
        device = dict(row)
        device["stock_qty"] = int(device["stock_qty"])
        device["is_high_value"] = bool(device["is_high_value"])
        return device

    def _serialize_user(self, row) -> dict[str, Any]:
        payload = dict(row)
        payload["permissions"] = self.get_permissions_for_role(payload["role"])
        payload["is_active"] = bool(payload["is_active"])
        return payload

    def _collect_code_pairs(self, payload: dict[str, Any]) -> list[tuple[str, str]]:
        pairs = []
        for field_name, code_type in self.CODE_FIELDS.items():
            code_value = self._optional_text(payload, field_name)
            if code_value:
                pairs.append((code_type, code_value))
        return pairs

    def _ensure_exists(self, connection, table_name: str, record_id: int, message: str) -> None:
        row = connection.execute(f"SELECT id FROM {table_name} WHERE id = ?", (record_id,)).fetchone()
        if row is None:
            raise ValueError(message)

    def _generate_number(self, connection, prefix: str, table: str, field: str) -> str:
        date_part = date.today().strftime("%Y%m%d")
        start = f"{prefix}-{date_part}"
        count = connection.execute(
            f"SELECT COUNT(*) AS count FROM {table} WHERE {field} LIKE ?",
            (f"{start}-%",),
        ).fetchone()["count"]
        return f"{start}-{count + 1:03d}"

    def _unique_nodes(self, nodes: list[dict[str, Any]]) -> list[dict[str, Any]]:
        seen: set[str] = set()
        result = []
        for node in nodes:
            if node["id"] in seen:
                continue
            seen.add(node["id"])
            result.append(node)
        return result

    def _actor_info(self, actor: dict[str, Any] | None) -> dict[str, Any]:
        if actor:
            return {
                "id": actor.get("id"),
                "username": actor.get("username") or actor.get("display_name") or "system",
                "role": actor.get("role") or "system",
                "display_name": actor.get("display_name") or actor.get("username") or "系统",
            }
        return {"id": None, "username": "system", "role": "system", "display_name": "系统"}

    def _actor_name(self, actor: dict[str, Any] | None) -> str:
        return self._actor_info(actor)["display_name"]

    def _operator_name(self, payload: dict[str, Any], actor: dict[str, Any] | None) -> str:
        return self._optional_text(payload, "operator_name") or self._actor_name(actor)

    def _required_text(self, payload: dict[str, Any], field_name: str, message: str) -> str:
        value = self._optional_text(payload, field_name)
        if not value:
            raise ValueError(message)
        return value

    def _optional_text(self, payload: dict[str, Any], field_name: str) -> str | None:
        value = payload.get(field_name)
        if value is None:
            return None
        text = str(value).strip()
        return text or None

    def _required_int(self, payload: dict[str, Any], field_name: str, message: str) -> int:
        try:
            return int(payload.get(field_name))
        except Exception as exc:
            raise ValueError(message) from exc

    def _optional_int(self, payload: dict[str, Any], field_name: str, default: int | None = None) -> int | None:
        value = payload.get(field_name)
        if value in (None, ""):
            return default
        return int(value)

    def _required_positive_int(self, payload: dict[str, Any], field_name: str, message: str) -> int:
        value = self._required_int(payload, field_name, message)
        if value <= 0:
            raise ValueError(message)
        return value

    def _required_float(self, payload: dict[str, Any], field_name: str, message: str) -> float:
        value = payload.get(field_name)
        try:
            number = float(value)
        except Exception as exc:
            raise ValueError(message) from exc
        if number < 0:
            raise ValueError(message)
        return number

    def _optional_float(self, payload: dict[str, Any], field_name: str) -> float | None:
        value = payload.get(field_name)
        if value in (None, ""):
            return None
        return float(value)

    def _normalize_datetime(self, value: str | None) -> str | None:
        if not value:
            return None
        clean = value.replace("T", " ")
        if len(clean) == 16:
            clean = f"{clean}:00"
        return clean
