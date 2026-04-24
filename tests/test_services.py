from __future__ import annotations

import unittest
import uuid
import time
from pathlib import Path

from app.services import TraceabilityService


class TraceabilityServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.workspace_tmp = Path(__file__).resolve().parents[1] / ".tmp_tests"
        self.workspace_tmp.mkdir(exist_ok=True)
        self.db_path = self.workspace_tmp / f"traceability_{uuid.uuid4().hex}.db"
        self.service = TraceabilityService(self.db_path, seed_demo=True)

    def tearDown(self) -> None:
        for _ in range(5):
            if not self.db_path.exists():
                break
            try:
                self.db_path.unlink()
                break
            except PermissionError:
                time.sleep(0.05)

    def test_can_resolve_device_by_rfid(self) -> None:
        summary = self.service.resolve_device_snapshot("RFID-0001")
        self.assertIsNotNone(summary)
        self.assertEqual(summary["device_name"], "冠脉支架系统")
        self.assertEqual(summary["trace_mode"], "RFID")

    def test_traceability_by_patient_returns_related_devices(self) -> None:
        result = self.service.search_traceability("P2026001")
        self.assertEqual(result["mode"], "patient")
        device_names = {item["device_name"] for item in result["related_devices"]}
        self.assertIn("冠脉支架系统", device_names)

    def test_end_to_end_device_registration_and_usage_flow(self) -> None:
        lookups = self.service.get_lookups()
        supplier_id = lookups["suppliers"][0]["id"]
        patient_id = lookups["patients"][0]["id"]
        department_id = next(item["id"] for item in lookups["departments"] if item["type"] == "临床")

        created = self.service.create_device(
            {
                "device_name": "测试射频止血夹",
                "category": "高值耗材",
                "trace_mode": "RFID",
                "risk_level": "III类",
                "supplier_id": supplier_id,
                "udi_code": "UDI-DEMO-001",
                "internal_code": "HOSP-DEMO-001",
                "rfid_code": "RFID-DEMO-001",
                "batch_no": "BATCH-DEMO-001",
                "unit": "套",
                "reorder_threshold": 1,
            }
        )
        self.assertEqual(created["status"], "registered")

        inbound = self.service.record_inbound(
            {
                "identifier": "UDI-DEMO-001",
                "quantity": 3,
                "warehouse": "中央库房",
                "operator_name": "测试库管员",
                "remark": "单元测试入库",
            }
        )
        self.assertEqual(inbound["stock_qty"], 3)

        usage = self.service.record_clinical_use(
            {
                "identifier": "RFID-DEMO-001",
                "patient_id": patient_id,
                "department_id": department_id,
                "operation_name": "测试手术",
                "operator_name": "测试护士",
                "quantity": 1,
                "remark": "单元测试临床使用",
            }
        )
        self.assertEqual(usage["remaining_stock"], 2)

        maintenance = self.service.record_maintenance(
            {
                "identifier": "UDI-DEMO-001",
                "record_type": "校准",
                "operator_name": "测试工程师",
                "result": "通过",
                "next_due_date": "2099-01-01",
            }
        )
        self.assertEqual(maintenance["record_type"], "校准")

        trace_result = self.service.search_traceability("UDI-DEMO-001")
        event_titles = {item["event_title"] for item in trace_result["timeline"]}
        self.assertEqual(trace_result["mode"], "device")
        self.assertIn("完成主数据登记", event_titles)
        self.assertIn("完成入库", event_titles)
        self.assertIn("完成临床使用", event_titles)
        self.assertIn("完成维护/校准", event_titles)

    def test_procurement_closure_increases_stock(self) -> None:
        lookups = self.service.get_lookups()
        target = next(item for item in lookups["devices"] if item["device_name"] == "人工关节假体")
        before = self.service.resolve_device_snapshot(target["udi_code"])["stock_qty"]

        created = self.service.create_procurement(
            {
                "device_id": target["id"],
                "supplier_id": target["supplier_id"],
                "quantity": 3,
                "unit_price": 19999.5,
                "purpose": "单元测试采购闭环",
                "expected_arrival_date": "2099-01-01",
            }
        )
        self.assertEqual(created["status"], "requested")

        approved = self.service.approve_procurement({"procurement_id": created["id"]})
        self.assertEqual(approved["status"], "approved")

        received = self.service.receive_procurement(
            {
                "procurement_id": created["id"],
                "warehouse": "中央库房",
            }
        )
        self.assertEqual(received["status"], "completed")

        after = self.service.resolve_device_snapshot(target["udi_code"])["stock_qty"]
        self.assertEqual(after, before + 3)

    def test_purchase_plan_and_inbound_order_flow(self) -> None:
        lookups = self.service.get_lookups()
        target = next(item for item in lookups["devices"] if item["device_name"] == "中心静脉导管包")
        before = self.service.resolve_device_snapshot(target["udi_code"])["stock_qty"]

        plan = self.service.create_purchase_plan(
            {
                "device_id": target["id"],
                "supplier_id": target["supplier_id"],
                "quantity": 5,
                "estimated_unit_price": 280,
                "reason": "单元测试库存预警补货",
                "source": "库存预警",
            }
        )
        self.assertEqual(plan["status"], "submitted")

        approved_plan = self.service.approve_purchase_plan({"plan_id": plan["id"], "review_note": "同意测试采购计划"})
        self.assertEqual(approved_plan["status"], "approved")

        inbound_order = self.service.create_inbound_order(
            {
                "plan_id": plan["id"],
                "device_id": target["id"],
                "quantity": 5,
                "warehouse": "中央库房",
            }
        )
        self.assertEqual(inbound_order["status"], "submitted")

        received_order = self.service.approve_inbound_order(
            {"order_id": inbound_order["id"], "decision": "received", "review_note": "测试验收入库通过"}
        )
        self.assertEqual(received_order["status"], "received")

        after = self.service.resolve_device_snapshot(target["udi_code"])["stock_qty"]
        self.assertEqual(after, before + 5)

    def test_request_quality_and_transfer_flow(self) -> None:
        lookups = self.service.get_lookups()
        target = next(item for item in lookups["devices"] if item["device_name"] == "医用敷料包")
        department_id = next(item["id"] for item in lookups["departments"] if item["name"] == "普外科")
        to_department_id = next(item["id"] for item in lookups["departments"] if item["name"] == "ICU")
        patient_id = lookups["patients"][0]["id"]

        request = self.service.create_device_request(
            {
                "device_id": target["id"],
                "department_id": department_id,
                "quantity": 2,
                "purpose": "单元测试申领换药耗材",
            }
        )
        self.assertEqual(request["status"], "submitted")
        approved_request = self.service.approve_device_request({"request_id": request["id"]})
        self.assertEqual(approved_request["status"], "approved")
        issued_request = self.service.issue_device_request({"request_id": request["id"]})
        self.assertEqual(issued_request["status"], "issued")

        report = self.service.create_quality_report(
            {
                "device_id": target["id"],
                "department_id": department_id,
                "patient_id": patient_id,
                "problem_type": "包装破损",
                "severity": "中",
                "description": "单元测试质量问题上报",
            }
        )
        self.assertEqual(report["status"], "submitted")
        handled = self.service.handle_quality_report(
            {
                "report_id": report["id"],
                "status": "resolved",
                "handling_result": "单元测试处理完成",
            }
        )
        self.assertEqual(handled["status"], "resolved")

        transfer = self.service.create_transfer(
            {
                "device_id": target["id"],
                "quantity": 1,
                "from_department_id": department_id,
                "to_department_id": to_department_id,
                "reason": "单元测试科室调拨",
            }
        )
        self.assertEqual(transfer["to_department"], "ICU")

    def test_recall_create_and_close_updates_device_status(self) -> None:
        created = self.service.create_recall_case(
            {
                "batch_no": "BATCH-GJ-202602",
                "severity": "中",
                "reason": "单元测试召回",
            }
        )
        self.assertEqual(created["status"], "open")
        self.assertGreaterEqual(created["affected_device_count"], 1)

        snapshot = self.service.resolve_device_snapshot("UDI-MD-GJ-20260002")
        self.assertEqual(snapshot["status"], "recalled")

        closed = self.service.close_recall_case(
            {
                "recall_case_id": created["id"],
                "disposal_note": "单元测试关闭召回",
            }
        )
        self.assertEqual(closed["status"], "closed")

    def test_scrap_flow_disposes_and_decreases_stock(self) -> None:
        before = self.service.resolve_device_snapshot("UDI-MD-GJ-20260002")["stock_qty"]
        created = self.service.create_scrap_request(
            {
                "identifier": "UDI-MD-GJ-20260002",
                "quantity": 1,
                "reason": "单元测试报废",
            }
        )
        self.assertEqual(created["status"], "requested")

        approved = self.service.approve_scrap_request({"scrap_request_id": created["id"]})
        self.assertEqual(approved["status"], "approved")

        disposed = self.service.dispose_scrap_request({"scrap_request_id": created["id"]})
        self.assertEqual(disposed["status"], "disposed")

        after = self.service.resolve_device_snapshot("UDI-MD-GJ-20260002")["stock_qty"]
        self.assertEqual(after, before - 1)

    def test_report_export_returns_xlsx_bytes(self) -> None:
        workbook = self.service.export_reports_workbook()
        self.assertTrue(workbook.startswith(b"PK"))
        self.assertGreater(len(workbook), 1000)


if __name__ == "__main__":
    unittest.main()
