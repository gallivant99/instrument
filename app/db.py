from __future__ import annotations

import sqlite3
from datetime import date, datetime, time, timedelta
from pathlib import Path


SCHEMA = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS suppliers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    license_no TEXT,
    contact_person TEXT,
    phone TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS departments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    type TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS patients (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    patient_no TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    gender TEXT,
    age INTEGER,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT NOT NULL UNIQUE,
    password TEXT NOT NULL,
    display_name TEXT NOT NULL,
    role TEXT NOT NULL,
    is_active INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS devices (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    device_name TEXT NOT NULL,
    category TEXT NOT NULL,
    trace_mode TEXT NOT NULL,
    risk_level TEXT NOT NULL,
    model TEXT,
    specification TEXT,
    manufacturer TEXT,
    supplier_id INTEGER,
    unit TEXT NOT NULL DEFAULT '件',
    batch_no TEXT,
    serial_no TEXT,
    production_date TEXT,
    expiry_date TEXT,
    reorder_threshold INTEGER NOT NULL DEFAULT 0,
    stock_qty INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'registered',
    current_location TEXT,
    last_seen_at TEXT,
    is_high_value INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    FOREIGN KEY (supplier_id) REFERENCES suppliers(id)
);

CREATE TABLE IF NOT EXISTS code_mappings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id INTEGER NOT NULL,
    code_type TEXT NOT NULL,
    code_value TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL,
    FOREIGN KEY (device_id) REFERENCES devices(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS stock_movements (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id INTEGER NOT NULL,
    movement_type TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    warehouse TEXT,
    department_id INTEGER,
    operator_name TEXT NOT NULL,
    remark TEXT,
    occurred_at TEXT NOT NULL,
    FOREIGN KEY (device_id) REFERENCES devices(id) ON DELETE CASCADE,
    FOREIGN KEY (department_id) REFERENCES departments(id)
);

CREATE TABLE IF NOT EXISTS clinical_usages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id INTEGER NOT NULL,
    patient_id INTEGER NOT NULL,
    department_id INTEGER NOT NULL,
    operation_name TEXT NOT NULL,
    operator_name TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    used_at TEXT NOT NULL,
    remark TEXT,
    FOREIGN KEY (device_id) REFERENCES devices(id) ON DELETE CASCADE,
    FOREIGN KEY (patient_id) REFERENCES patients(id),
    FOREIGN KEY (department_id) REFERENCES departments(id)
);

CREATE TABLE IF NOT EXISTS maintenance_records (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id INTEGER NOT NULL,
    record_type TEXT NOT NULL,
    plan_date TEXT,
    completed_at TEXT,
    result TEXT,
    vendor_name TEXT,
    operator_name TEXT NOT NULL,
    next_due_date TEXT,
    remark TEXT,
    FOREIGN KEY (device_id) REFERENCES devices(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS procurements (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    request_no TEXT NOT NULL UNIQUE,
    device_id INTEGER NOT NULL,
    supplier_id INTEGER NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price REAL NOT NULL DEFAULT 0,
    purpose TEXT,
    expected_arrival_date TEXT,
    status TEXT NOT NULL,
    requested_by TEXT NOT NULL,
    requested_at TEXT NOT NULL,
    approved_by TEXT,
    approved_at TEXT,
    received_by TEXT,
    received_at TEXT,
    inbound_completed_at TEXT,
    remark TEXT,
    FOREIGN KEY (device_id) REFERENCES devices(id) ON DELETE CASCADE,
    FOREIGN KEY (supplier_id) REFERENCES suppliers(id)
);

CREATE TABLE IF NOT EXISTS recall_cases (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    recall_no TEXT NOT NULL UNIQUE,
    scope_type TEXT NOT NULL,
    device_id INTEGER,
    batch_no TEXT,
    reason TEXT NOT NULL,
    severity TEXT NOT NULL,
    status TEXT NOT NULL,
    initiated_by TEXT NOT NULL,
    initiated_at TEXT NOT NULL,
    disposal_note TEXT,
    closed_at TEXT,
    FOREIGN KEY (device_id) REFERENCES devices(id)
);

CREATE TABLE IF NOT EXISTS recall_impacts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    recall_case_id INTEGER NOT NULL,
    device_id INTEGER,
    patient_id INTEGER,
    department_id INTEGER,
    impact_type TEXT NOT NULL,
    status TEXT NOT NULL,
    note TEXT,
    FOREIGN KEY (recall_case_id) REFERENCES recall_cases(id) ON DELETE CASCADE,
    FOREIGN KEY (device_id) REFERENCES devices(id),
    FOREIGN KEY (patient_id) REFERENCES patients(id),
    FOREIGN KEY (department_id) REFERENCES departments(id)
);

CREATE TABLE IF NOT EXISTS scrap_requests (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    scrap_no TEXT NOT NULL UNIQUE,
    device_id INTEGER NOT NULL,
    quantity INTEGER NOT NULL,
    reason TEXT NOT NULL,
    status TEXT NOT NULL,
    requested_by TEXT NOT NULL,
    requested_at TEXT NOT NULL,
    approved_by TEXT,
    approved_at TEXT,
    disposed_by TEXT,
    disposed_at TEXT,
    remark TEXT,
    FOREIGN KEY (device_id) REFERENCES devices(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS purchase_staff (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    staff_no TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    phone TEXT,
    department TEXT,
    position TEXT,
    qualification TEXT,
    status TEXT NOT NULL DEFAULT 'active',
    created_at TEXT NOT NULL,
    FOREIGN KEY (user_id) REFERENCES users(id)
);

CREATE TABLE IF NOT EXISTS purchase_plans (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    plan_no TEXT NOT NULL UNIQUE,
    device_id INTEGER NOT NULL,
    supplier_id INTEGER,
    quantity INTEGER NOT NULL,
    estimated_unit_price REAL NOT NULL DEFAULT 0,
    reason TEXT NOT NULL,
    source TEXT,
    status TEXT NOT NULL,
    submitted_by TEXT NOT NULL,
    submitted_at TEXT NOT NULL,
    reviewed_by TEXT,
    reviewed_at TEXT,
    review_note TEXT,
    purchased_at TEXT,
    remark TEXT,
    FOREIGN KEY (device_id) REFERENCES devices(id) ON DELETE CASCADE,
    FOREIGN KEY (supplier_id) REFERENCES suppliers(id)
);

CREATE TABLE IF NOT EXISTS inbound_orders (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_no TEXT NOT NULL UNIQUE,
    plan_id INTEGER,
    procurement_id INTEGER,
    device_id INTEGER NOT NULL,
    supplier_id INTEGER,
    quantity INTEGER NOT NULL,
    warehouse TEXT NOT NULL,
    status TEXT NOT NULL,
    submitted_by TEXT NOT NULL,
    submitted_at TEXT NOT NULL,
    reviewed_by TEXT,
    reviewed_at TEXT,
    review_note TEXT,
    received_at TEXT,
    remark TEXT,
    FOREIGN KEY (plan_id) REFERENCES purchase_plans(id),
    FOREIGN KEY (procurement_id) REFERENCES procurements(id),
    FOREIGN KEY (device_id) REFERENCES devices(id) ON DELETE CASCADE,
    FOREIGN KEY (supplier_id) REFERENCES suppliers(id)
);

CREATE TABLE IF NOT EXISTS device_requests (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    request_no TEXT NOT NULL UNIQUE,
    requester_id INTEGER,
    requester_name TEXT NOT NULL,
    department_id INTEGER NOT NULL,
    device_id INTEGER NOT NULL,
    quantity INTEGER NOT NULL,
    purpose TEXT NOT NULL,
    status TEXT NOT NULL,
    submitted_at TEXT NOT NULL,
    reviewed_by TEXT,
    reviewed_at TEXT,
    review_note TEXT,
    issued_by TEXT,
    issued_at TEXT,
    remark TEXT,
    FOREIGN KEY (requester_id) REFERENCES users(id),
    FOREIGN KEY (department_id) REFERENCES departments(id),
    FOREIGN KEY (device_id) REFERENCES devices(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS quality_reports (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    report_no TEXT NOT NULL UNIQUE,
    reporter_id INTEGER,
    reporter_name TEXT NOT NULL,
    department_id INTEGER NOT NULL,
    device_id INTEGER NOT NULL,
    patient_id INTEGER,
    problem_type TEXT NOT NULL,
    severity TEXT NOT NULL,
    description TEXT NOT NULL,
    status TEXT NOT NULL,
    submitted_at TEXT NOT NULL,
    handled_by TEXT,
    handled_at TEXT,
    handling_result TEXT,
    remark TEXT,
    FOREIGN KEY (reporter_id) REFERENCES users(id),
    FOREIGN KEY (department_id) REFERENCES departments(id),
    FOREIGN KEY (device_id) REFERENCES devices(id) ON DELETE CASCADE,
    FOREIGN KEY (patient_id) REFERENCES patients(id)
);

CREATE TABLE IF NOT EXISTS department_transfers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    transfer_no TEXT NOT NULL UNIQUE,
    device_id INTEGER NOT NULL,
    quantity INTEGER NOT NULL,
    from_department_id INTEGER NOT NULL,
    to_department_id INTEGER NOT NULL,
    operator_name TEXT NOT NULL,
    reason TEXT NOT NULL,
    transferred_at TEXT NOT NULL,
    remark TEXT,
    FOREIGN KEY (device_id) REFERENCES devices(id) ON DELETE CASCADE,
    FOREIGN KEY (from_department_id) REFERENCES departments(id),
    FOREIGN KEY (to_department_id) REFERENCES departments(id)
);

CREATE TABLE IF NOT EXISTS trace_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    event_title TEXT NOT NULL,
    event_desc TEXT,
    related_table TEXT,
    related_id INTEGER,
    actor TEXT,
    location TEXT,
    occurred_at TEXT NOT NULL,
    FOREIGN KEY (device_id) REFERENCES devices(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS audit_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    username TEXT NOT NULL,
    role TEXT NOT NULL,
    action TEXT NOT NULL,
    target_type TEXT NOT NULL,
    target_id INTEGER,
    target_name TEXT,
    detail TEXT,
    ip_address TEXT,
    created_at TEXT NOT NULL,
    FOREIGN KEY (user_id) REFERENCES users(id)
);

CREATE INDEX IF NOT EXISTS idx_code_mappings_value ON code_mappings(code_value);
CREATE INDEX IF NOT EXISTS idx_devices_batch_no ON devices(batch_no);
CREATE INDEX IF NOT EXISTS idx_patients_patient_no ON patients(patient_no);
CREATE INDEX IF NOT EXISTS idx_trace_events_device_time ON trace_events(device_id, occurred_at);
CREATE INDEX IF NOT EXISTS idx_procurements_status ON procurements(status);
CREATE INDEX IF NOT EXISTS idx_recall_cases_status ON recall_cases(status);
CREATE INDEX IF NOT EXISTS idx_scrap_requests_status ON scrap_requests(status);
CREATE INDEX IF NOT EXISTS idx_purchase_plans_status ON purchase_plans(status);
CREATE INDEX IF NOT EXISTS idx_inbound_orders_status ON inbound_orders(status);
CREATE INDEX IF NOT EXISTS idx_device_requests_status ON device_requests(status);
CREATE INDEX IF NOT EXISTS idx_quality_reports_status ON quality_reports(status);
CREATE INDEX IF NOT EXISTS idx_department_transfers_time ON department_transfers(transferred_at);
CREATE INDEX IF NOT EXISTS idx_audit_logs_created_at ON audit_logs(created_at);
"""


def now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def date_text(value: date) -> str:
    return value.strftime("%Y-%m-%d")


def timestamp_text(value: date, hour: int, minute: int = 0) -> str:
    return datetime.combine(value, time(hour, minute)).strftime("%Y-%m-%d %H:%M:%S")


def get_connection(db_path: Path | str) -> sqlite3.Connection:
    connection = sqlite3.connect(str(db_path))
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    return connection


def initialize_database(db_path: Path | str) -> None:
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with get_connection(path) as connection:
        connection.executescript(SCHEMA)
        _ensure_schema_upgrades(connection)


def _ensure_schema_upgrades(connection: sqlite3.Connection) -> None:
    _ensure_columns(
        connection,
        "suppliers",
        {
            "business_scope": "TEXT",
            "qualification": "TEXT",
            "status": "TEXT NOT NULL DEFAULT 'active'",
        },
    )
    _ensure_columns(
        connection,
        "users",
        {
            "job_no": "TEXT",
            "phone": "TEXT",
            "department": "TEXT",
        },
    )


def _ensure_columns(connection: sqlite3.Connection, table_name: str, columns: dict[str, str]) -> None:
    existing_columns = {
        row["name"]
        for row in connection.execute(f"PRAGMA table_info({table_name})").fetchall()
    }
    for column_name, column_definition in columns.items():
        if column_name not in existing_columns:
            connection.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_definition}")


def seed_demo_data(db_path: Path | str) -> None:
    with get_connection(db_path) as connection:
        if _has_rows(connection, "devices"):
            _seed_workflow_demo_data(connection, now_text(), date.today())
            return

        created_at = now_text()
        today = date.today()
        base_day = today - timedelta(days=160)

        suppliers = [
            ("西安智联医疗科技有限公司", "SX-UDI-2025-001", "刘敏", "029-83620001"),
            ("秦康高值耗材供应链有限公司", "SX-UDI-2025-019", "赵鹏", "029-83620019"),
            ("华睿医疗设备服务有限公司", "SX-OPS-2025-055", "王蕾", "029-83620055"),
            ("国药控股陕西医疗器械有限公司", "SX-GY-2025-088", "周明", "029-86580088"),
            ("美敦力医疗用品技术服务有限公司", "UDI-MDT-2025-204", "陈璐", "400-820-9988"),
            ("强生医疗器材供应链有限公司", "UDI-JNJ-2025-116", "何佳", "400-610-1188"),
            ("迈瑞医疗设备股份有限公司", "UDI-MR-2025-066", "孙涛", "400-700-5652"),
            ("威高医用材料西北配送中心", "SX-WEGO-2025-013", "高洁", "029-85260013"),
        ]
        connection.executemany(
            """
            INSERT INTO suppliers (name, license_no, contact_person, phone, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            [(name, license_no, contact, phone, created_at) for name, license_no, contact, phone in suppliers],
        )
        supplier_profiles = [
            ("西安智联医疗科技有限公司", "一次性无菌耗材、导管类耗材、基础护理材料", "医疗器械经营许可证；ISO13485；UDI 数据接口已备案"),
            ("秦康高值耗材供应链有限公司", "骨科、心血管介入、植入类高值耗材配送", "陕西省高值耗材配送资质；冷链运输能力备案"),
            ("华睿医疗设备服务有限公司", "输注、手术室、监护类设备供货与维保", "医疗设备维修服务能力等级证明；计量校准合作资质"),
            ("国药控股陕西医疗器械有限公司", "综合医疗器械、检验耗材、设备备件", "国药控股集团授权；医疗器械三类经营许可证"),
            ("美敦力医疗用品技术服务有限公司", "心血管介入、神经介入及植入类耗材", "厂家授权书；进口医疗器械注册证备案"),
            ("强生医疗器材供应链有限公司", "外科缝线、骨科内固定、介入辅助耗材", "厂家一级授权；质量追溯平台对接证明"),
            ("迈瑞医疗设备股份有限公司", "监护、呼吸、急救和输注设备", "生产企业许可证；售后服务承诺书；软件版本合规证明"),
            ("威高医用材料西北配送中心", "穿刺、留置、透析和敷料类普耗配送", "医疗器械经营许可证；批号追溯能力说明"),
        ]
        connection.executemany(
            """
            UPDATE suppliers
            SET business_scope = ?, qualification = ?, status = 'active'
            WHERE name = ?
            """,
            [(scope, qualification, name) for name, scope, qualification in supplier_profiles],
        )

        departments = [
            ("设备科", "管理"),
            ("采购办", "管理"),
            ("质控办", "管理"),
            ("中央库房", "仓储"),
            ("高值耗材库", "仓储"),
            ("心内科", "临床"),
            ("骨科", "临床"),
            ("普外科", "临床"),
            ("神经外科", "临床"),
            ("眼科", "临床"),
            ("肾内科", "临床"),
            ("ICU", "临床"),
            ("手术室", "临床"),
            ("消毒供应中心", "保障"),
        ]
        connection.executemany(
            """
            INSERT INTO departments (name, type, created_at)
            VALUES (?, ?, ?)
            """,
            [(name, dep_type, created_at) for name, dep_type in departments],
        )

        patients = [
            ("P2026001", "张敏", "女", 48),
            ("P2026002", "李强", "男", 55),
            ("P2026003", "王悦", "女", 36),
            ("P2026004", "赵建国", "男", 63),
            ("P2026005", "刘芳", "女", 51),
            ("P2026006", "孙浩", "男", 42),
            ("P2026007", "陈晓兰", "女", 67),
            ("P2026008", "周伟", "男", 58),
            ("P2026009", "马丽", "女", 45),
            ("P2026010", "郭强", "男", 61),
            ("P2026011", "何静", "女", 39),
            ("P2026012", "唐磊", "男", 47),
            ("P2026013", "白雪", "女", 28),
            ("P2026014", "吴军", "男", 72),
            ("P2026015", "郑蕾", "女", 54),
            ("P2026016", "冯斌", "男", 33),
            ("P2026017", "罗敏", "女", 60),
            ("P2026018", "梁涛", "男", 49),
        ]
        connection.executemany(
            """
            INSERT INTO patients (patient_no, name, gender, age, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            [(patient_no, name, gender, age, created_at) for patient_no, name, gender, age in patients],
        )

        users = [
            ("admin", "admin123", "系统管理员", "admin"),
            ("warehouse", "warehouse123", "库房管理员", "warehouse"),
            ("nurse", "nurse123", "临床护士", "clinician"),
            ("doctor", "doctor123", "临床医生", "clinician"),
            ("buyer", "buyer123", "采购员王倩", "purchaser"),
            ("engineer", "engineer123", "设备工程师", "engineer"),
            ("manager", "manager123", "管理人员", "manager"),
        ]
        connection.executemany(
            """
            INSERT INTO users (username, password, display_name, role, is_active, created_at)
            VALUES (?, ?, ?, ?, 1, ?)
            """,
            [(username, password, display_name, role, created_at) for username, password, display_name, role in users],
        )
        user_profiles = [
            ("admin", "ADM-001", "18802990001", "信息与设备管理中心"),
            ("warehouse", "WH-002", "18802990002", "中央库房"),
            ("nurse", "CLN-018", "18802990018", "心内科"),
            ("doctor", "CLN-006", "18802990006", "普外科"),
            ("buyer", "PUR-003", "18802990003", "采购办"),
            ("engineer", "ENG-011", "18802990011", "设备科"),
            ("manager", "MGR-001", "18802990010", "质控办"),
        ]
        connection.executemany(
            """
            UPDATE users
            SET job_no = ?, phone = ?, department = ?
            WHERE username = ?
            """,
            [(job_no, phone, department, username) for username, job_no, phone, department in user_profiles],
        )

        supplier_ids = _id_map(connection, "suppliers")
        department_ids = _id_map(connection, "departments")
        patient_ids = {row["patient_no"]: row["id"] for row in connection.execute("SELECT id, patient_no FROM patients")}

        historical_consumed = {
            "冠脉支架系统": 1,
            "一次性导管包": 8,
            "血管鞘组": 3,
            "中心静脉导管包": 1,
            "可吸收缝线": 8,
            "锁定加压钢板": 1,
            "人工晶状体": 1,
            "内镜活检钳": 2,
            "透析管路": 1,
            "PTCA 导丝": 1,
            "医用敷料包": 3,
            "血糖试纸": 4,
        }

        devices = [
            ("冠脉支架系统", "高值耗材", "RFID", "III类", "ST-Plus", "3.0mm x 24mm", "迈川医疗", "秦康高值耗材供应链有限公司", "套", "BATCH-CS-202601", "SN-CS-0001", 260, 720, 1, 0, "used", "心内科导管室", 1, {"UDI": "UDI-MD-CS-20260001", "INTERNAL": "HOSP-HV-0001", "SUPPLIER": "SUP-CS-8891", "RFID": "RFID-0001", "QR": "QR-0001"}),
            ("人工关节假体", "高值耗材", "RFID", "III类", "JT-A9", "右侧 44mm", "辰康医疗", "秦康高值耗材供应链有限公司", "套", "BATCH-GJ-202602", "SN-GJ-0002", 230, 540, 1, 2, "in_stock", "高值耗材库", 1, {"UDI": "UDI-MD-GJ-20260002", "INTERNAL": "HOSP-HV-0002", "SUPPLIER": "SUP-GJ-1120", "RFID": "RFID-0002", "QR": "QR-0002"}),
            ("注射泵设备", "设备", "RFID", "II类", "PUMP-8", "双通道", "华睿设备", "华睿医疗设备服务有限公司", "台", "BATCH-PUMP-202511", "SN-PUMP-1008", 420, 1080, 1, 1, "in_stock", "设备科", 0, {"UDI": "UDI-MD-PUMP-20260003", "INTERNAL": "HOSP-EQ-1008", "SUPPLIER": "SUP-PUMP-3001", "RFID": "RFID-0003", "QR": "QR-0003"}),
            ("一次性导管包", "普耗", "QR", "II类", "DG-Set", "成人型", "西安智联医疗", "西安智联医疗科技有限公司", "包", "BATCH-DG-202604", "LOT-DG-202604", 96, 15, 10, 4, "recalled", "手术室二级库", 0, {"UDI": "UDI-MD-DG-20260004", "INTERNAL": "HOSP-LV-3004", "SUPPLIER": "SUP-DG-4502", "QR": "QR-0004"}),
            ("药物洗脱球囊导管", "高值耗材", "RFID", "III类", "DEB-30", "3.0mm x 20mm", "美敦力", "美敦力医疗用品技术服务有限公司", "根", "BATCH-DEB-202603", "SN-DEB-0301", 180, 690, 2, 5, "in_stock", "高值耗材库", 1, {"UDI": "UDI-MD-DEB-20260005", "INTERNAL": "HOSP-HV-0005", "SUPPLIER": "MDT-DEB-7391", "RFID": "RFID-0005", "QR": "QR-0005"}),
            ("PTCA 导丝", "高值耗材", "RFID", "III类", "GW-014", "0.014in x 180cm", "强生医疗", "强生医疗器材供应链有限公司", "根", "BATCH-GW-202603", "SN-GW-0141", 150, 630, 5, 24, "in_stock", "高值耗材库", 1, {"UDI": "UDI-MD-GW-20260006", "INTERNAL": "HOSP-HV-0006", "SUPPLIER": "JNJ-GW-014", "RFID": "RFID-0006", "QR": "QR-0006"}),
            ("血管鞘组", "普耗", "QR", "II类", "VS-6F", "6F", "威高医用材料", "威高医用材料西北配送中心", "套", "BATCH-VS-202604", "LOT-VS-202604", 82, 365, 15, 18, "recalled", "手术室二级库", 0, {"UDI": "UDI-MD-VS-20260007", "INTERNAL": "HOSP-LV-3007", "SUPPLIER": "WEGO-VS-604", "QR": "QR-0007"}),
            ("中心静脉导管包", "普耗", "QR", "II类", "CVC-3L", "三腔 7Fr", "西安智联医疗", "西安智联医疗科技有限公司", "包", "BATCH-CVC-202605", "LOT-CVC-202605", 74, 400, 12, 13, "in_stock", "ICU耗材柜", 0, {"UDI": "UDI-MD-CVC-20260008", "INTERNAL": "HOSP-LV-3008", "SUPPLIER": "SUP-CVC-6501", "QR": "QR-0008"}),
            ("锁定加压钢板", "高值耗材", "RFID", "III类", "LCP-128", "胫骨近端 8孔", "强生医疗", "强生医疗器材供应链有限公司", "块", "BATCH-LCP-202603", "SN-LCP-1288", 135, 900, 2, 7, "in_stock", "高值耗材库", 1, {"UDI": "UDI-MD-LCP-20260009", "INTERNAL": "HOSP-HV-0009", "SUPPLIER": "JNJ-LCP-128", "RFID": "RFID-0009", "QR": "QR-0009"}),
            ("可吸收缝线", "普耗", "QR", "II类", "VICRYL-2-0", "2-0 70cm", "强生医疗", "强生医疗器材供应链有限公司", "包", "BATCH-SUT-202604", "LOT-SUT-202604", 70, 365, 30, 96, "in_stock", "手术室二级库", 0, {"UDI": "UDI-MD-SUT-20260010", "INTERNAL": "HOSP-LV-3010", "SUPPLIER": "JNJ-SUT-204", "QR": "QR-0010"}),
            ("电动手术床", "设备", "RFID", "II类", "OT-500", "四段式", "迈瑞医疗", "迈瑞医疗设备股份有限公司", "台", "BATCH-OT-202412", "SN-OT-0500", 560, 1800, 1, 1, "in_use", "手术室1间", 0, {"UDI": "UDI-EQ-OT-20260011", "INTERNAL": "HOSP-EQ-0500", "SUPPLIER": "MR-OT-500", "RFID": "RFID-0011", "QR": "QR-0011"}),
            ("高频电刀", "设备", "RFID", "II类", "HF-300", "300W", "华睿设备", "华睿医疗设备服务有限公司", "台", "BATCH-HF-202502", "SN-HF-0300", 430, 1440, 1, 1, "in_stock", "手术室", 0, {"UDI": "UDI-EQ-HF-20260012", "INTERNAL": "HOSP-EQ-0300", "SUPPLIER": "HR-HF-300", "RFID": "RFID-0012", "QR": "QR-0012"}),
            ("多参数监护仪", "设备", "RFID", "II类", "BeneVision-N12", "N12", "迈瑞医疗", "迈瑞医疗设备股份有限公司", "台", "BATCH-MON-202501", "SN-MON-1208", 470, 1440, 2, 1, "in_use", "ICU床旁区", 0, {"UDI": "UDI-EQ-MON-20260013", "INTERNAL": "HOSP-EQ-1208", "SUPPLIER": "MR-MON-N12", "RFID": "RFID-0013", "QR": "QR-0013"}),
            ("除颤监护仪", "设备", "RFID", "II类", "DFM-100", "双相波", "迈瑞医疗", "迈瑞医疗设备股份有限公司", "台", "BATCH-DFM-202501", "SN-DFM-0106", 460, 1440, 1, 1, "in_stock", "急救车1号", 0, {"UDI": "UDI-EQ-DFM-20260014", "INTERNAL": "HOSP-EQ-0106", "SUPPLIER": "MR-DFM-100", "RFID": "RFID-0014", "QR": "QR-0014"}),
            ("呼吸机", "设备", "RFID", "III类", "SV-800", "成人/儿童", "迈瑞医疗", "迈瑞医疗设备股份有限公司", "台", "BATCH-VENT-202410", "SN-VENT-0801", 620, 1800, 1, 1, "in_stock", "ICU设备间", 0, {"UDI": "UDI-EQ-VENT-20260015", "INTERNAL": "HOSP-EQ-0801", "SUPPLIER": "MR-SV800", "RFID": "RFID-0015", "QR": "QR-0015"}),
            ("输液泵", "设备", "RFID", "II类", "VP-50", "单通道", "华睿设备", "华睿医疗设备服务有限公司", "台", "BATCH-INF-202511", "SN-INF-0050", 390, 1200, 3, 5, "in_stock", "设备科", 0, {"UDI": "UDI-EQ-INF-20260016", "INTERNAL": "HOSP-EQ-0050", "SUPPLIER": "HR-INF-50", "RFID": "RFID-0016", "QR": "QR-0016"}),
            ("人工晶状体", "高值耗材", "RFID", "III类", "IOL-Acry", "+21.0D", "国药控股", "国药控股陕西医疗器械有限公司", "枚", "BATCH-IOL-202604", "SN-IOL-2101", 80, 730, 3, 8, "in_stock", "眼科耗材柜", 1, {"UDI": "UDI-MD-IOL-20260017", "INTERNAL": "HOSP-HV-0017", "SUPPLIER": "GY-IOL-210", "RFID": "RFID-0017", "QR": "QR-0017"}),
            ("内镜活检钳", "普耗", "QR", "II类", "BIO-18", "2.3mm x 180cm", "国药控股", "国药控股陕西医疗器械有限公司", "把", "BATCH-BIO-202604", "LOT-BIO-202604", 88, 365, 10, 31, "in_stock", "内镜中心", 0, {"UDI": "UDI-MD-BIO-20260018", "INTERNAL": "HOSP-LV-3018", "SUPPLIER": "GY-BIO-180", "QR": "QR-0018"}),
            ("透析管路", "普耗", "QR", "II类", "HD-Line", "成人型", "威高医用材料", "威高医用材料西北配送中心", "套", "BATCH-HD-202605", "LOT-HD-202605", 40, 360, 20, 54, "in_stock", "肾内科库房", 0, {"UDI": "UDI-MD-HD-20260019", "INTERNAL": "HOSP-LV-3019", "SUPPLIER": "WEGO-HD-605", "QR": "QR-0019"}),
            ("血糖试纸", "普耗", "QR", "II类", "GLU-100", "50片/盒", "国药控股", "国药控股陕西医疗器械有限公司", "盒", "BATCH-GLU-202604", "LOT-GLU-202604", 65, 45, 20, 26, "in_stock", "内分泌科库房", 0, {"UDI": "UDI-MD-GLU-20260020", "INTERNAL": "HOSP-LV-3020", "SUPPLIER": "GY-GLU-100", "QR": "QR-0020"}),
            ("静脉留置针", "普耗", "QR", "II类", "IV-24G", "24G", "威高医用材料", "威高医用材料西北配送中心", "支", "BATCH-IV-202604", "LOT-IV-202604", 55, 365, 80, 220, "in_stock", "中央库房", 0, {"UDI": "UDI-MD-IV-20260021", "INTERNAL": "HOSP-LV-3021", "SUPPLIER": "WEGO-IV-24G", "QR": "QR-0021"}),
            ("医用敷料包", "普耗", "QR", "I类", "DRS-10", "10cm x 10cm", "威高医用材料", "威高医用材料西北配送中心", "包", "BATCH-DRS-202604", "LOT-DRS-202604", 35, 300, 60, 140, "in_stock", "中央库房", 0, {"UDI": "UDI-MD-DRS-20260022", "INTERNAL": "HOSP-LV-3022", "SUPPLIER": "WEGO-DRS-10", "QR": "QR-0022"}),
            ("手术器械包", "设备", "RFID", "II类", "SURG-KIT-A", "普外基础包", "消毒供应中心", "国药控股陕西医疗器械有限公司", "包", "BATCH-KIT-202604", "SN-KIT-A001", 120, 720, 2, 12, "in_stock", "消毒供应中心", 0, {"UDI": "UDI-EQ-KIT-20260023", "INTERNAL": "HOSP-EQ-KIT-A", "SUPPLIER": "GY-KIT-A", "RFID": "RFID-0023", "QR": "QR-0023"}),
            ("鼻氧管", "普耗", "QR", "I类", "OXY-NC", "成人型", "西安智联医疗", "西安智联医疗科技有限公司", "根", "BATCH-OXY-202604", "LOT-OXY-202604", 30, 365, 100, 260, "in_stock", "中央库房", 0, {"UDI": "UDI-MD-OXY-20260024", "INTERNAL": "HOSP-LV-3024", "SUPPLIER": "SUP-OXY-604", "QR": "QR-0024"}),
        ]

        device_ids: dict[str, int] = {}
        device_units: dict[int, str] = {}
        for index, spec in enumerate(devices, start=1):
            (
                device_name,
                category,
                trace_mode,
                risk_level,
                model,
                specification,
                manufacturer,
                supplier_name,
                unit,
                batch_no,
                serial_no,
                prod_days_ago,
                expiry_days_from_today,
                reorder_threshold,
                stock_qty,
                status,
                current_location,
                is_high_value,
                codes,
            ) = spec
            registered_at = timestamp_text(base_day + timedelta(days=index * 3), 9 + index % 7, 10)
            cursor = connection.execute(
                """
                INSERT INTO devices (
                    device_name, category, trace_mode, risk_level, model, specification,
                    manufacturer, supplier_id, unit, batch_no, serial_no, production_date,
                    expiry_date, reorder_threshold, stock_qty, status, current_location,
                    last_seen_at, is_high_value, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    device_name,
                    category,
                    trace_mode,
                    risk_level,
                    model,
                    specification,
                    manufacturer,
                    supplier_ids[supplier_name],
                    unit,
                    batch_no,
                    serial_no,
                    date_text(today - timedelta(days=prod_days_ago)),
                    date_text(today + timedelta(days=expiry_days_from_today)),
                    reorder_threshold,
                    stock_qty,
                    status,
                    current_location,
                    timestamp_text(today - timedelta(days=(index * 2) % 35), 15, 0),
                    is_high_value,
                    registered_at,
                ),
            )
            device_id = cursor.lastrowid
            device_ids[device_name] = device_id
            device_units[device_id] = unit
            connection.executemany(
                """
                INSERT INTO code_mappings (device_id, code_type, code_value, created_at)
                VALUES (?, ?, ?, ?)
                """,
                [(device_id, code_type, code_value, registered_at) for code_type, code_value in codes.items()],
            )
            _trace(
                connection,
                device_id,
                "REGISTER",
                "完成主数据登记",
                f"录入 {device_name} 主数据、UDI 与院内编码。",
                "devices",
                device_id,
                "系统管理员",
                "基础信息中心",
                registered_at,
            )
            inbound_qty = stock_qty + historical_consumed.get(device_name, 0)
            if inbound_qty > 0:
                inbound_at = timestamp_text(base_day + timedelta(days=index * 3 + 2), 10, index % 50)
                cursor = connection.execute(
                    """
                    INSERT INTO stock_movements (
                        device_id, movement_type, quantity, warehouse, department_id,
                        operator_name, remark, occurred_at
                    )
                    VALUES (?, 'INBOUND', ?, ?, NULL, ?, ?, ?)
                    """,
                    (
                        device_id,
                        inbound_qty,
                        "中央库房" if category != "高值耗材" else "高值耗材库",
                        "库房管理员韩雪",
                        "供应商到货验收合格，完成入库。",
                        inbound_at,
                    ),
                )
                _trace(
                    connection,
                    device_id,
                    "INBOUND",
                    "完成入库",
                    f"{device_name} 到货入库 {inbound_qty}{unit}。",
                    "stock_movements",
                    cursor.lastrowid,
                    "库房管理员韩雪",
                    "中央库房" if category != "高值耗材" else "高值耗材库",
                    inbound_at,
                )

        stocktake_plan = [
            ("人工关节假体", 2),
            ("一次性导管包", 2),
            ("PTCA 导丝", 3),
            ("血管鞘组", 4),
            ("中心静脉导管包", 5),
            ("可吸收缝线", 6),
            ("多参数监护仪", 7),
            ("除颤监护仪", 8),
            ("输液泵", 9),
            ("人工晶状体", 10),
            ("内镜活检钳", 11),
            ("透析管路", 12),
            ("静脉留置针", 13),
            ("医用敷料包", 14),
            ("手术器械包", 15),
            ("鼻氧管", 16),
        ]
        for device_name, days_ago in stocktake_plan:
            device_id = device_ids[device_name]
            stocktake_at = timestamp_text(today - timedelta(days=days_ago), 11, days_ago % 50)
            location = connection.execute("SELECT current_location FROM devices WHERE id = ?", (device_id,)).fetchone()["current_location"]
            cursor = connection.execute(
                """
                INSERT INTO stock_movements (
                    device_id, movement_type, quantity, warehouse, department_id,
                    operator_name, remark, occurred_at
                )
                VALUES (?, 'STOCKTAKE', 0, ?, NULL, '盘点员许宁', 'RFID/二维码例行盘点，账实一致。', ?)
                """,
                (device_id, location, stocktake_at),
            )
            _trace(
                connection,
                device_id,
                "STOCKTAKE",
                "完成盘点",
                "例行盘点完成，账实一致。",
                "stock_movements",
                cursor.lastrowid,
                "盘点员许宁",
                location,
                stocktake_at,
            )

        clinical_usages = [
            ("冠脉支架系统", "P2026001", "心内科", "冠脉介入手术", "护士李珊", 1, 118, "植入完成，UDI 已写入病历。"),
            ("一次性导管包", "P2026002", "手术室", "介入导管置入", "护士白洁", 2, 24, "术中使用，批次已绑定。"),
            ("一次性导管包", "P2026003", "手术室", "导管更换", "护士白洁", 6, 8, "术中追加使用。"),
            ("血管鞘组", "P2026004", "心内科", "冠脉造影", "护士李珊", 2, 12, "术中使用。"),
            ("血管鞘组", "P2026005", "心内科", "支架植入术", "护士李珊", 1, 6, "召回批次关联患者。"),
            ("中心静脉导管包", "P2026006", "ICU", "中心静脉置管", "护士赵萌", 1, 11, "ICU床旁置管。"),
            ("可吸收缝线", "P2026007", "普外科", "腹腔镜胆囊切除", "护士周颖", 8, 18, "术中使用。"),
            ("锁定加压钢板", "P2026008", "骨科", "胫骨平台骨折内固定", "护士王可", 1, 30, "高值耗材术中使用。"),
            ("人工晶状体", "P2026009", "眼科", "白内障超声乳化联合人工晶体植入", "护士黄琳", 1, 14, "植入材料已绑定患者。"),
            ("内镜活检钳", "P2026010", "普外科", "胃镜活检", "护士周颖", 2, 10, "内镜中心使用。"),
            ("透析管路", "P2026011", "肾内科", "血液透析", "护士何清", 1, 3, "透析治疗使用。"),
            ("PTCA 导丝", "P2026012", "心内科", "冠脉介入手术", "护士李珊", 1, 4, "导丝已随病例归档。"),
            ("医用敷料包", "P2026013", "普外科", "换药处置", "护士周颖", 3, 2, "门诊换药使用。"),
        ]
        for device_name, patient_no, department_name, operation_name, operator, quantity, days_ago, remark in clinical_usages:
            device_id = device_ids[device_name]
            used_at = timestamp_text(today - timedelta(days=days_ago), 14, days_ago % 55)
            cursor = connection.execute(
                """
                INSERT INTO clinical_usages (
                    device_id, patient_id, department_id, operation_name,
                    operator_name, quantity, used_at, remark
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    device_id,
                    patient_ids[patient_no],
                    department_ids[department_name],
                    operation_name,
                    operator,
                    quantity,
                    used_at,
                    remark,
                ),
            )
            patient_name = connection.execute("SELECT name FROM patients WHERE patient_no = ?", (patient_no,)).fetchone()["name"]
            _trace(
                connection,
                device_id,
                "USE",
                "完成临床使用",
                f"患者 {patient_name}({patient_no}) 使用 {quantity}{device_units[device_id]}，手术/处置：{operation_name}。",
                "clinical_usages",
                cursor.lastrowid,
                operator,
                department_name,
                used_at,
            )

        maintenance_records = [
            ("注射泵设备", "校准", 12, 11, "通过", "华睿医疗设备服务有限公司", "工程师吴涛", 5, "计量校准正常，下次校准日期已更新。"),
            ("多参数监护仪", "保养", 20, 19, "通过", "迈瑞医疗设备股份有限公司", "工程师吴涛", 35, "更换血氧探头线缆。"),
            ("呼吸机", "校准", 32, 31, "通过", "迈瑞医疗设备股份有限公司", "工程师吴涛", -3, "氧浓度模块校准通过，已进入超期复核提醒。"),
            ("高频电刀", "维修", 18, 17, "通过", "华睿医疗设备服务有限公司", "工程师吴涛", 80, "脚踏开关更换完成。"),
            ("除颤监护仪", "保养", 40, 39, "通过", "迈瑞医疗设备股份有限公司", "工程师吴涛", 25, "电池容量检测合格。"),
            ("电动手术床", "保养", 58, 57, "通过", "华睿医疗设备服务有限公司", "工程师吴涛", 120, "液压系统维护完成。"),
        ]
        for device_name, record_type, plan_days_ago, done_days_ago, result, vendor, operator, next_due_days, remark in maintenance_records:
            device_id = device_ids[device_name]
            completed_at = timestamp_text(today - timedelta(days=done_days_ago), 16, done_days_ago % 40)
            cursor = connection.execute(
                """
                INSERT INTO maintenance_records (
                    device_id, record_type, plan_date, completed_at, result,
                    vendor_name, operator_name, next_due_date, remark
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    device_id,
                    record_type,
                    date_text(today - timedelta(days=plan_days_ago)),
                    completed_at,
                    result,
                    vendor,
                    operator,
                    date_text(today + timedelta(days=next_due_days)),
                    remark,
                ),
            )
            _trace(
                connection,
                device_id,
                "MAINTENANCE",
                "完成维护/校准",
                f"{record_type}完成，结果：{result}。{remark}",
                "maintenance_records",
                cursor.lastrowid,
                operator,
                "设备科",
                completed_at,
            )

        procurements = [
            ("PR-20260301-001", "人工关节假体", 2, 18800, "骨科择期手术备货", 72, "completed", "王倩", "陈晨", "韩雪", "到货验收通过并完成入库。"),
            ("PR-20260308-002", "PTCA 导丝", 30, 980, "心内介入耗材补货", 61, "completed", "王倩", "陈晨", "韩雪", "供应商扫码到货，UDI 批量校验通过。"),
            ("PR-20260320-003", "呼吸机", 1, 198000, "ICU急救设备储备", 42, "completed", "吴涛", "陈晨", "韩雪", "设备验收、安装调试完成。"),
            ("PR-20260410-004", "中心静脉导管包", 30, 268, "ICU耗材补货", 7, "approved", "王倩", "陈晨", None, "供应商已确认发货，待到货。"),
            ("PR-20260414-005", "血糖试纸", 80, 95, "内分泌科耗材补货", 3, "requested", "王倩", None, None, "待管理人员审批。"),
            ("PR-20260416-006", "输液泵", 3, 13800, "病区输注设备补充", 2, "approved", "吴涛", "陈晨", None, "待供应商送货。"),
            ("PR-20260418-007", "静脉留置针", 500, 4.8, "全院普耗月度补货", 1, "requested", "王倩", None, None, "按月消耗量申请。"),
        ]
        for request_no, device_name, quantity, unit_price, purpose, days_ago, status, requested_by, approved_by, received_by, remark in procurements:
            device_id = device_ids[device_name]
            supplier_id = connection.execute("SELECT supplier_id FROM devices WHERE id = ?", (device_id,)).fetchone()["supplier_id"]
            requested_at = timestamp_text(today - timedelta(days=days_ago), 10, days_ago % 45)
            approved_at = timestamp_text(today - timedelta(days=max(days_ago - 1, 0)), 15, days_ago % 40) if approved_by else None
            received_at = timestamp_text(today - timedelta(days=max(days_ago - 3, 0)), 9, days_ago % 40) if status == "completed" else None
            cursor = connection.execute(
                """
                INSERT INTO procurements (
                    request_no, device_id, supplier_id, quantity, unit_price, purpose,
                    expected_arrival_date, status, requested_by, requested_at,
                    approved_by, approved_at, received_by, received_at, inbound_completed_at, remark
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    request_no,
                    device_id,
                    supplier_id,
                    quantity,
                    unit_price,
                    purpose,
                    date_text(today + timedelta(days=5 if status != "completed" else -max(days_ago - 3, 0))),
                    status,
                    requested_by,
                    requested_at,
                    approved_by,
                    approved_at,
                    received_by,
                    received_at,
                    received_at,
                    remark,
                ),
            )
            _trace(
                connection,
                device_id,
                "PROCUREMENT_REQUEST",
                "发起采购申请",
                f"采购单 {request_no}，申请数量 {quantity}{device_units[device_id]}，用途：{purpose}。",
                "procurements",
                cursor.lastrowid,
                requested_by,
                "采购办",
                requested_at,
            )
            if approved_at:
                _trace(connection, device_id, "PROCUREMENT_APPROVE", "采购申请审批通过", f"采购单 {request_no} 已审批。", "procurements", cursor.lastrowid, approved_by, "采购办", approved_at)
            if received_at:
                _trace(connection, device_id, "PROCUREMENT_RECEIVE", "采购到货并完成入库", f"采购单 {request_no} 到货并完成入库。", "procurements", cursor.lastrowid, received_by or "库房管理员", "中央库房", received_at)

        recall_id = _create_recall(connection, "RC-20260418-001", "batch", device_ids["一次性导管包"], "BATCH-DG-202604", "供应商通报同批次导管包封签稳定性异常，需暂停发放并核查流向。", "高", "open", "陈晨", today - timedelta(days=1), None, None, device_ids, patient_ids, department_ids)
        _create_recall(connection, "RC-20260412-002", "batch", device_ids["血管鞘组"], "BATCH-VS-202604", "省级抽检提示同批次标签黏附强度不足，要求完成患者追溯与库存封存。", "中", "open", "陈晨", today - timedelta(days=4), None, None, device_ids, patient_ids, department_ids)
        _create_recall(connection, "RC-20260322-003", "batch", device_ids["血糖试纸"], "BATCH-GLU-202604", "厂家主动召回部分批次外包装批号印刷偏差，经核查未影响临床使用。", "低", "closed", "陈晨", today - timedelta(days=30), "已完成库存核查，无患者不良事件，召回关闭。", today - timedelta(days=24), device_ids, patient_ids, department_ids)

        scrap_requests = [
            ("SC-20260416-001", "人工关节假体", 1, "包装外观受损，需按不合格耗材报废处理。", "approved", "吴涛", 5, "陈晨", 4, None, None, "待执行销毁并回填报废证明。"),
            ("SC-20260417-002", "血糖试纸", 4, "外包装受潮，已隔离待销毁。", "disposed", "韩雪", 4, "陈晨", 3, "吴涛", 2, "已完成销毁并归档照片。"),
            ("SC-20260419-003", "医用敷料包", 6, "库内破损污染，申请报废。", "requested", "韩雪", 2, None, None, None, None, "待质控办审批。"),
        ]
        for scrap_no, device_name, quantity, reason, status, requested_by, requested_days_ago, approved_by, approved_days_ago, disposed_by, disposed_days_ago, remark in scrap_requests:
            device_id = device_ids[device_name]
            requested_at = timestamp_text(today - timedelta(days=requested_days_ago), 11, 0)
            approved_at = timestamp_text(today - timedelta(days=approved_days_ago), 15, 20) if approved_by else None
            disposed_at = timestamp_text(today - timedelta(days=disposed_days_ago), 16, 10) if disposed_by else None
            cursor = connection.execute(
                """
                INSERT INTO scrap_requests (
                    scrap_no, device_id, quantity, reason, status,
                    requested_by, requested_at, approved_by, approved_at,
                    disposed_by, disposed_at, remark
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (scrap_no, device_id, quantity, reason, status, requested_by, requested_at, approved_by, approved_at, disposed_by, disposed_at, remark),
            )
            _trace(connection, device_id, "SCRAP_REQUEST", "发起报废申请", f"申请报废 {quantity}{device_units[device_id]}，原因：{reason}", "scrap_requests", cursor.lastrowid, requested_by, "设备科", requested_at)
            if approved_at:
                _trace(connection, device_id, "SCRAP_APPROVE", "报废申请审批通过", f"报废单 {scrap_no} 已审批。", "scrap_requests", cursor.lastrowid, approved_by, "质控办", approved_at)
            if disposed_at:
                _trace(connection, device_id, "SCRAP", "完成报废处置", f"报废单 {scrap_no} 已完成销毁处置。", "scrap_requests", cursor.lastrowid, disposed_by, "医疗废物暂存间", disposed_at)

        audit_logs = [
            ("admin", "admin", "CREATE_DEVICE", "device", "冠脉支架系统", "初始化演示主数据。", 160),
            ("warehouse", "warehouse", "INBOUND", "device", "PTCA 导丝", "批量扫码验收入库。", 61),
            ("manager", "manager", "PROCUREMENT_APPROVE", "procurement", "PR-20260320-003", "审批 ICU 呼吸机采购单。", 41),
            ("nurse", "clinician", "CLINICAL_USE", "device", "血管鞘组", "登记冠脉造影耗材使用。", 12),
            ("engineer", "engineer", "MAINTENANCE", "device", "呼吸机", "登记呼吸机计量校准记录。", 31),
            ("manager", "manager", "RECALL_CREATE", "recall", "RC-20260418-001", "创建导管包批次召回。", 1),
            ("engineer", "engineer", "SCRAP_REQUEST", "scrap", "SC-20260416-001", "发起人工关节假体报废申请。", 5),
            ("manager", "manager", "EXPORT_REPORT", "reports", "追溯综合报表.xlsx", "导出演示报表。", 0),
        ]
        user_ids = {row["username"]: row["id"] for row in connection.execute("SELECT id, username FROM users")}
        for username, role, action, target_type, target_name, detail, days_ago in audit_logs:
            connection.execute(
                """
                INSERT INTO audit_logs (
                    user_id, username, role, action, target_type, target_id,
                    target_name, detail, ip_address, created_at
                )
                VALUES (?, ?, ?, ?, ?, NULL, ?, ?, '127.0.0.1', ?)
                """,
                (user_ids.get(username), username, role, action, target_type, target_name, detail, timestamp_text(today - timedelta(days=days_ago), 17, 10)),
            )

        _seed_workflow_demo_data(connection, created_at, today)

        connection.execute("UPDATE devices SET stock_qty = MAX(stock_qty - 4, 0) WHERE device_name = '血糖试纸'")


def _seed_workflow_demo_data(connection: sqlite3.Connection, created_at: str, today: date) -> None:
    supplier_profiles = [
        ("西安智联医疗科技有限公司", "一次性无菌耗材、导管类耗材、基础护理材料", "医疗器械经营许可证；ISO13485；UDI 数据接口已备案"),
        ("秦康高值耗材供应链有限公司", "骨科、心血管介入、植入类高值耗材配送", "陕西省高值耗材配送资质；冷链运输能力备案"),
        ("华睿医疗设备服务有限公司", "输注、手术室、监护类设备供货与维保", "医疗设备维修服务能力等级证明；计量校准合作资质"),
        ("国药控股陕西医疗器械有限公司", "综合医疗器械、检验耗材、设备备件", "国药控股集团授权；医疗器械三类经营许可证"),
        ("美敦力医疗用品技术服务有限公司", "心血管介入、神经介入及植入类耗材", "厂家授权书；进口医疗器械注册证备案"),
        ("强生医疗器材供应链有限公司", "外科缝线、骨科内固定、介入辅助耗材", "厂家一级授权；质量追溯平台对接证明"),
        ("迈瑞医疗设备股份有限公司", "监护、呼吸、急救和输注设备", "生产企业许可证；售后服务承诺书；软件版本合规证明"),
        ("威高医用材料西北配送中心", "穿刺、留置、透析和敷料类普耗配送", "医疗器械经营许可证；批号追溯能力说明"),
    ]
    connection.executemany(
        """
        UPDATE suppliers
        SET business_scope = ?, qualification = ?, status = COALESCE(status, 'active')
        WHERE name = ?
        """,
        [(scope, qualification, name) for name, scope, qualification in supplier_profiles],
    )

    extra_users = [
        ("doctor", "doctor123", "临床医生", "clinician", "CLN-006", "18802990006", "普外科"),
        ("buyer", "buyer123", "采购员王倩", "purchaser", "PUR-003", "18802990003", "采购办"),
        ("buyer_li", "buyer123", "采购员李晨", "purchaser", "PUR-004", "18802990004", "采购办"),
    ]
    for username, password, display_name, role, job_no, phone, department in extra_users:
        connection.execute(
            """
            INSERT OR IGNORE INTO users (
                username, password, display_name, role, is_active, created_at,
                job_no, phone, department
            )
            VALUES (?, ?, ?, ?, 1, ?, ?, ?, ?)
            """,
            (username, password, display_name, role, created_at, job_no, phone, department),
        )
        connection.execute(
            """
            UPDATE users
            SET display_name = ?, role = ?, job_no = ?, phone = ?, department = ?
            WHERE username = ?
            """,
            (display_name, role, job_no, phone, department, username),
        )

    user_ids = {row["username"]: row["id"] for row in connection.execute("SELECT id, username FROM users")}
    if not _has_rows(connection, "purchase_staff"):
        purchase_staff = [
            ("buyer", "PUR-003", "王倩", "18802990003", "采购办", "采购专员", "医疗器械采购内控培训合格；供应商准入审核授权"),
            ("buyer_li", "PUR-004", "李晨", "18802990004", "采购办", "采购专员", "合同归档与验收入库流程培训合格"),
            (None, "PUR-008", "刘思敏", "18802990008", "采购办", "采购助理", "供应商证照复核与价格比对培训合格"),
        ]
        connection.executemany(
            """
            INSERT INTO purchase_staff (
                user_id, staff_no, name, phone, department, position,
                qualification, status, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, 'active', ?)
            """,
            [
                (user_ids.get(username), staff_no, name, phone, department, position, qualification, created_at)
                for username, staff_no, name, phone, department, position, qualification in purchase_staff
            ],
        )

    device_rows = connection.execute("SELECT id, device_name, supplier_id, unit FROM devices").fetchall()
    if not device_rows:
        return
    device_ids = {row["device_name"]: row["id"] for row in device_rows}
    device_units = {row["id"]: row["unit"] for row in device_rows}
    device_suppliers = {row["id"]: row["supplier_id"] for row in device_rows}
    department_ids = _id_map(connection, "departments")
    patient_ids = {row["patient_no"]: row["id"] for row in connection.execute("SELECT id, patient_no FROM patients")}

    if not _has_rows(connection, "purchase_plans"):
        purchase_plans = [
            ("PP-20260402-001", "中心静脉导管包", 40, 268, "ICU 一周消耗高于安全库存，需补充三腔导管包", "库存预警", 22, "approved", "王倩", "陈晨", "同意按预警数量采购", None, "已转采购执行"),
            ("PP-20260406-002", "输液泵", 4, 13800, "住院病区新增床位，输注设备不足", "科室需求", 18, "approved", "李晨", "陈晨", "同意采购，要求到货后设备科验收", None, "待供应商发货"),
            ("PP-20260408-003", "人工晶状体", 12, 3200, "眼科择期手术排期增加，补充常用屈光度库存", "临床需求", 16, "submitted", "王倩", None, None, None, "待管理员审核"),
            ("PP-20260410-004", "静脉留置针", 800, 4.8, "全院月度普耗补货", "月度计划", 14, "submitted", "王倩", None, None, None, "按月度消耗量测算"),
            ("PP-20260413-005", "PTCA 导丝", 30, 980, "心内介入耗材低库存补货", "库存预警", 11, "approved", "李晨", "陈晨", "同意采购，保留批号追溯", None, "审批通过"),
            ("PP-20260415-006", "血糖试纸", 100, 95, "内分泌科门诊与病区消耗增加", "库存预警", 9, "rejected", "王倩", "陈晨", "本周已有在途订单，暂缓重复采购", None, "已退回"),
        ]
        for plan_no, device_name, quantity, price, reason, source, days_ago, status, submitted_by, reviewed_by, review_note, purchased_days, remark in purchase_plans:
            if device_name not in device_ids:
                continue
            device_id = device_ids[device_name]
            submitted_at = timestamp_text(today - timedelta(days=days_ago), 10, days_ago % 40)
            reviewed_at = timestamp_text(today - timedelta(days=max(days_ago - 1, 0)), 15, days_ago % 35) if reviewed_by else None
            purchased_at = timestamp_text(today - timedelta(days=purchased_days), 11, 20) if purchased_days is not None else None
            cursor = connection.execute(
                """
                INSERT INTO purchase_plans (
                    plan_no, device_id, supplier_id, quantity, estimated_unit_price,
                    reason, source, status, submitted_by, submitted_at,
                    reviewed_by, reviewed_at, review_note, purchased_at, remark
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    plan_no,
                    device_id,
                    device_suppliers[device_id],
                    quantity,
                    price,
                    reason,
                    source,
                    status,
                    submitted_by,
                    submitted_at,
                    reviewed_by,
                    reviewed_at,
                    review_note,
                    purchased_at,
                    remark,
                ),
            )
            _trace(connection, device_id, "PURCHASE_PLAN", "提交采购计划", f"{plan_no}：{reason}，数量 {quantity}{device_units[device_id]}。", "purchase_plans", cursor.lastrowid, submitted_by, "采购办", submitted_at)
            if reviewed_at:
                _trace(connection, device_id, "PURCHASE_PLAN_REVIEW", "采购计划审核", review_note or "采购计划已审核。", "purchase_plans", cursor.lastrowid, reviewed_by, "管理员工作台", reviewed_at)

    if not _has_rows(connection, "inbound_orders"):
        plan_ids = {row["plan_no"]: row["id"] for row in connection.execute("SELECT id, plan_no FROM purchase_plans")}
        inbound_orders = [
            ("IN-20260405-001", "PP-20260402-001", "中心静脉导管包", 40, "中央库房", 19, "approved", "王倩", "韩雪", "票据、批号、UDI 批量校验通过", None),
            ("IN-20260411-002", "PP-20260413-005", "PTCA 导丝", 30, "高值耗材库", 7, "submitted", "李晨", None, None, "供应商已送达，待管理员验收入库"),
            ("IN-20260412-003", None, "输液泵", 3, "设备科", 6, "received", "王倩", "韩雪", "设备科完成开箱验收，序列号已登记", "已入库并等待科室领用"),
            ("IN-20260416-004", "PP-20260406-002", "输液泵", 1, "设备科", 4, "submitted", "李晨", None, None, "分批到货，待管理员审核"),
            ("IN-20260417-005", None, "医用敷料包", 50, "中央库房", 3, "rejected", "刘思敏", "韩雪", "外包装破损比例超限，退回供应商", "供应商重新发货中"),
        ]
        for order_no, plan_no, device_name, quantity, warehouse, days_ago, status, submitted_by, reviewed_by, review_note, remark in inbound_orders:
            if device_name not in device_ids:
                continue
            device_id = device_ids[device_name]
            submitted_at = timestamp_text(today - timedelta(days=days_ago), 9, days_ago % 45)
            reviewed_at = timestamp_text(today - timedelta(days=max(days_ago - 1, 0)), 14, days_ago % 35) if reviewed_by else None
            received_at = reviewed_at if status == "received" else None
            cursor = connection.execute(
                """
                INSERT INTO inbound_orders (
                    order_no, plan_id, device_id, supplier_id, quantity, warehouse,
                    status, submitted_by, submitted_at, reviewed_by, reviewed_at,
                    review_note, received_at, remark
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    order_no,
                    plan_ids.get(plan_no) if plan_no else None,
                    device_id,
                    device_suppliers[device_id],
                    quantity,
                    warehouse,
                    status,
                    submitted_by,
                    submitted_at,
                    reviewed_by,
                    reviewed_at,
                    review_note,
                    received_at,
                    remark,
                ),
            )
            _trace(connection, device_id, "INBOUND_ORDER", "提交采购入库单", f"{order_no} 到货 {quantity}{device_units[device_id]}，位置：{warehouse}。", "inbound_orders", cursor.lastrowid, submitted_by, "采购办", submitted_at)
            if reviewed_at:
                _trace(connection, device_id, "INBOUND_REVIEW", "入库单审核", review_note or "入库单已审核。", "inbound_orders", cursor.lastrowid, reviewed_by, warehouse, reviewed_at)

    if not _has_rows(connection, "device_requests"):
        device_requests = [
            ("REQ-20260409-001", "nurse", "心内科", "PTCA 导丝", 2, "冠脉介入手术备用", 15, "issued", "陈晨", "同意发放，按手术排期领用", "韩雪", "已由高值耗材库扫码出库"),
            ("REQ-20260412-002", "doctor", "普外科", "可吸收缝线", 12, "腹腔镜胆囊切除术周计划", 12, "approved", "陈晨", "同意，库房按批号先进先出发放", None, "待库房发放"),
            ("REQ-20260414-003", "nurse", "ICU", "中心静脉导管包", 6, "ICU 留置导管治疗备用", 10, "submitted", None, None, None, "待管理员审批"),
            ("REQ-20260416-004", "doctor", "骨科", "人工关节假体", 1, "右髋置换择期手术", 8, "rejected", "陈晨", "患者手术方案调整，暂不发放", None, "已退回科室"),
            ("REQ-20260418-005", "nurse", "肾内科", "透析管路", 20, "血液透析中心日常消耗", 6, "approved", "陈晨", "同意补充二级库", None, "待库房配送"),
            ("REQ-20260420-006", "doctor", "眼科", "人工晶状体", 1, "白内障人工晶体植入", 4, "issued", "陈晨", "与患者病历绑定后发放", "韩雪", "已发放到眼科耗材柜"),
        ]
        for request_no, username, department_name, device_name, quantity, purpose, days_ago, status, reviewed_by, review_note, issued_by, remark in device_requests:
            if device_name not in device_ids or department_name not in department_ids:
                continue
            submitted_at = timestamp_text(today - timedelta(days=days_ago), 8, days_ago % 50)
            reviewed_at = timestamp_text(today - timedelta(days=max(days_ago - 1, 0)), 15, days_ago % 30) if reviewed_by else None
            issued_at = timestamp_text(today - timedelta(days=max(days_ago - 2, 0)), 10, days_ago % 30) if issued_by else None
            requester = connection.execute("SELECT id, display_name FROM users WHERE username = ?", (username,)).fetchone()
            device_id = device_ids[device_name]
            cursor = connection.execute(
                """
                INSERT INTO device_requests (
                    request_no, requester_id, requester_name, department_id, device_id,
                    quantity, purpose, status, submitted_at, reviewed_by, reviewed_at,
                    review_note, issued_by, issued_at, remark
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    request_no,
                    requester["id"] if requester else None,
                    requester["display_name"] if requester else username,
                    department_ids[department_name],
                    device_id,
                    quantity,
                    purpose,
                    status,
                    submitted_at,
                    reviewed_by,
                    reviewed_at,
                    review_note,
                    issued_by,
                    issued_at,
                    remark,
                ),
            )
            _trace(connection, device_id, "DEVICE_REQUEST", "提交器械申领单", f"{department_name} 申领 {quantity}{device_units[device_id]}，用途：{purpose}。", "device_requests", cursor.lastrowid, requester["display_name"] if requester else username, department_name, submitted_at)
            if issued_at:
                _trace(connection, device_id, "REQUEST_ISSUE", "申领单发放", f"{request_no} 已发放到 {department_name}。", "device_requests", cursor.lastrowid, issued_by, department_name, issued_at)

    if not _has_rows(connection, "quality_reports"):
        quality_reports = [
            ("QR-20260411-001", "nurse", "心内科", "血管鞘组", "P2026005", "包装/标签异常", "中", "同批次外包装标签粘附力不足，已暂停使用并封存剩余库存。", 13, "processing", "陈晨", "已关联召回单 RC-20260412-002，等待供应商复核。", "涉及批号已锁定"),
            ("QR-20260415-002", "doctor", "普外科", "医用敷料包", "P2026013", "污染/破损", "高", "换药时发现外包装边缘破损，未用于患者，已拍照留证。", 9, "resolved", "韩雪", "确认运输破损，已报废 6 包并要求供应商补发。", "处理完成"),
            ("QR-20260418-003", "nurse", "ICU", "中心静脉导管包", "P2026006", "疑似性能异常", "中", "导丝推进阻力偏大，现场更换备用包后治疗完成。", 6, "submitted", None, None, "待管理员处理"),
            ("QR-20260419-004", "doctor", "眼科", "人工晶状体", "P2026009", "资料不完整", "低", "随货合格证批号与外盒批号不一致，未发放。", 5, "processing", "陈晨", "已联系供应商补充批号说明。", "待供应商回函"),
            ("QR-20260421-005", "nurse", "肾内科", "透析管路", "P2026011", "使用反馈", "低", "个别包装开启手感偏紧，未影响透析治疗。", 3, "resolved", "韩雪", "抽检同批次未复现，记录观察。", "持续关注"),
        ]
        for report_no, username, department_name, device_name, patient_no, problem_type, severity, description, days_ago, status, handled_by, handling_result, remark in quality_reports:
            if device_name not in device_ids or department_name not in department_ids:
                continue
            submitted_at = timestamp_text(today - timedelta(days=days_ago), 13, days_ago % 45)
            handled_at = timestamp_text(today - timedelta(days=max(days_ago - 1, 0)), 16, days_ago % 35) if handled_by else None
            reporter = connection.execute("SELECT id, display_name FROM users WHERE username = ?", (username,)).fetchone()
            device_id = device_ids[device_name]
            cursor = connection.execute(
                """
                INSERT INTO quality_reports (
                    report_no, reporter_id, reporter_name, department_id, device_id,
                    patient_id, problem_type, severity, description, status,
                    submitted_at, handled_by, handled_at, handling_result, remark
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    report_no,
                    reporter["id"] if reporter else None,
                    reporter["display_name"] if reporter else username,
                    department_ids[department_name],
                    device_id,
                    patient_ids.get(patient_no),
                    problem_type,
                    severity,
                    description,
                    status,
                    submitted_at,
                    handled_by,
                    handled_at,
                    handling_result,
                    remark,
                ),
            )
            _trace(connection, device_id, "QUALITY_REPORT", "质量问题上报", f"{problem_type}：{description}", "quality_reports", cursor.lastrowid, reporter["display_name"] if reporter else username, department_name, submitted_at)
            if handled_at:
                _trace(connection, device_id, "QUALITY_HANDLE", "质量问题处理", handling_result or "质量问题已处理。", "quality_reports", cursor.lastrowid, handled_by, "质控办", handled_at)

    if not _has_rows(connection, "department_transfers"):
        transfers = [
            ("TF-20260407-001", "输液泵", 1, "设备科", "ICU", "韩雪", "ICU 临时扩容，设备科调拨备用输液泵", 17, "调拨后由 ICU 保管"),
            ("TF-20260410-002", "高频电刀", 1, "手术室", "普外科", "韩雪", "普外科专项手术周借用", 14, "手术结束后归还手术室"),
            ("TF-20260413-003", "医用敷料包", 30, "中央库房", "普外科", "韩雪", "普外科换药室补充二级库库存", 11, "按先进先出批号配送"),
            ("TF-20260417-004", "鼻氧管", 80, "中央库房", "ICU", "韩雪", "ICU 呼吸治疗消耗补充", 7, "二级库补货"),
            ("TF-20260420-005", "透析管路", 25, "中央库房", "肾内科", "韩雪", "血液透析中心周库存补充", 4, "完成扫码交接"),
        ]
        for transfer_no, device_name, quantity, from_department, to_department, operator, reason, days_ago, remark in transfers:
            if device_name not in device_ids or from_department not in department_ids or to_department not in department_ids:
                continue
            device_id = device_ids[device_name]
            transferred_at = timestamp_text(today - timedelta(days=days_ago), 10, days_ago % 45)
            cursor = connection.execute(
                """
                INSERT INTO department_transfers (
                    transfer_no, device_id, quantity, from_department_id, to_department_id,
                    operator_name, reason, transferred_at, remark
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    transfer_no,
                    device_id,
                    quantity,
                    department_ids[from_department],
                    department_ids[to_department],
                    operator,
                    reason,
                    transferred_at,
                    remark,
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
                (device_id, quantity, from_department, department_ids[to_department], operator, reason, transferred_at),
            )
            _trace(connection, device_id, "TRANSFER", "完成科室调拨", f"{from_department} 调拨至 {to_department}，数量 {quantity}{device_units[device_id]}。", "department_transfers", cursor.lastrowid, operator, to_department, transferred_at)


def _has_rows(connection: sqlite3.Connection, table_name: str) -> bool:
    row = connection.execute(f"SELECT 1 FROM {table_name} LIMIT 1").fetchone()
    return row is not None


def _id_map(connection: sqlite3.Connection, table_name: str) -> dict[str, int]:
    return {row["name"]: row["id"] for row in connection.execute(f"SELECT id, name FROM {table_name}")}


def _trace(
    connection: sqlite3.Connection,
    device_id: int,
    event_type: str,
    event_title: str,
    event_desc: str,
    related_table: str,
    related_id: int,
    actor: str,
    location: str,
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


def _create_recall(
    connection: sqlite3.Connection,
    recall_no: str,
    scope_type: str,
    device_id: int,
    batch_no: str,
    reason: str,
    severity: str,
    status: str,
    initiated_by: str,
    initiated_date: date,
    disposal_note: str | None,
    closed_date: date | None,
    device_ids: dict[str, int],
    patient_ids: dict[str, int],
    department_ids: dict[str, int],
) -> int:
    initiated_at = timestamp_text(initiated_date, 9, 30)
    closed_at = timestamp_text(closed_date, 16, 20) if closed_date else None
    cursor = connection.execute(
        """
        INSERT INTO recall_cases (
            recall_no, scope_type, device_id, batch_no, reason,
            severity, status, initiated_by, initiated_at, disposal_note, closed_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (recall_no, scope_type, device_id, batch_no, reason, severity, status, initiated_by, initiated_at, disposal_note, closed_at),
    )
    recall_id = cursor.lastrowid
    impacted_devices = [row["id"] for row in connection.execute("SELECT id FROM devices WHERE batch_no = ?", (batch_no,))]
    for impacted_device_id in impacted_devices:
        connection.execute(
            """
            INSERT INTO recall_impacts (
                recall_case_id, device_id, patient_id, department_id, impact_type, status, note
            )
            VALUES (?, ?, NULL, NULL, 'device', ?, '待核查库存与流向')
            """,
            (recall_id, impacted_device_id, "done" if status == "closed" else "pending"),
        )
        if status != "closed":
            connection.execute("UPDATE devices SET status = 'recalled' WHERE id = ?", (impacted_device_id,))
        _trace(
            connection,
            impacted_device_id,
            "RECALL",
            "触发召回预警",
            f"召回单 {recall_no} 已创建，原因：{reason}",
            "recall_cases",
            recall_id,
            initiated_by,
            "质控办",
            initiated_at,
        )
        usage_rows = connection.execute(
            """
            SELECT DISTINCT patient_id, department_id
            FROM clinical_usages
            WHERE device_id = ?
            """,
            (impacted_device_id,),
        )
        for usage in usage_rows:
            connection.execute(
                """
                INSERT INTO recall_impacts (
                    recall_case_id, device_id, patient_id, department_id, impact_type, status, note
                )
                VALUES (?, NULL, ?, ?, 'patient', ?, '需通知临床复核使用记录')
                """,
                (recall_id, usage["patient_id"], usage["department_id"], "done" if status == "closed" else "pending"),
            )
        if closed_at:
            _trace(
                connection,
                impacted_device_id,
                "RECALL_CLOSE",
                "召回流程关闭",
                disposal_note or "召回核查完成。",
                "recall_cases",
                recall_id,
                initiated_by,
                "质控办",
                closed_at,
            )
    return recall_id
