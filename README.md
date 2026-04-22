# 医疗器械全生命周期追溯管理系统

这是基于开题报告《医疗器械全生命周期追溯管理系统设计与实现》完成的毕业设计演示项目。系统覆盖 UDI 主数据、多码映射、采购闭环、仓储流转、临床使用、维护校准、召回管理、报废处置、审计日志、报表导出和多维追溯。

## 功能概览

- UDI 主数据管理：维护器械基础资料、供应商资质和多码映射。
- 采购闭环管理：支持采购申请、审批、到货入库。
- 智能仓储管理：支持入库、出库、批量盘点模拟。
- 临床使用追溯：支持器械与患者、科室、手术事件绑定。
- 维护校准管理：记录保养、维修、计量校准和到期预警。
- 召回管理：支持按批次或单器械发起召回，并统计受影响患者。
- 报废管理：支持报废申请、审批和最终处置。
- 报表与审计：支持角色登录、操作审计和 Excel 报表导出。
- 全维度追溯门户：支持按 UDI、RFID、二维码、批次号、患者编号等维度查询时间轴、正向路径、逆向影响和流程图关系。

## 技术方案

- 后端：Python 标准库 `http.server` + `sqlite3`
- 前端：原生 HTML / CSS / JavaScript
- 数据库：SQLite
- 依赖：无第三方包，可离线运行

## 项目结构

```text
app/
  db.py            数据库初始化与演示数据
  exporters.py     Excel 报表导出
  services.py      业务服务、权限与追溯逻辑
  server.py        HTTP 服务、登录会话与 API 路由
data/
  traceability.db  SQLite 演示数据库
docs/
  需求文档.md
scripts/
  reset_demo_database.py
  create_portable_archive.ps1
static/
  index.html
  styles.css
  app.js
tests/
  test_services.py
```

## 启动方式

在项目根目录执行：

```powershell
$env:UV_CACHE_DIR = ".uv-cache"
uv run python -m app.server
```

启动后访问：

```text
http://127.0.0.1:8000
```

如果另一台电脑没有安装 `uv`，且已经安装 Python 3.11+，也可以直接运行：

```powershell
python -m app.server
```

## 演示账号

- `admin / admin123`：系统管理员
- `warehouse / warehouse123`：库房管理员
- `nurse / nurse123`：临床护士
- `engineer / engineer123`：设备工程师
- `manager / manager123`：管理人员

## 演示数据库

当前数据库文件：

```text
data/traceability.db
```

内置数据规模：

- 8 家供应商
- 14 个科室
- 18 位患者
- 24 条器械主数据
- 40 条库存流水
- 13 条临床使用记录
- 6 条维护校准记录
- 7 条采购单
- 3 条召回单
- 3 条报废单
- 108 条追溯事件

如需重建演示数据库：

```powershell
$env:UV_CACHE_DIR = ".uv-cache"
uv run python scripts/reset_demo_database.py
```

脚本会先把旧数据库备份到 `data/traceability.backup-时间.db`，再生成新的 `data/traceability.db`。

## 运行测试

```powershell
$env:UV_CACHE_DIR = ".uv-cache"
uv run python -m unittest discover -s tests -v
```

## 迁移打包

生成不带数据库的源码包：

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\create_portable_archive.ps1
```

生成带当前演示数据库的迁移包：

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\create_portable_archive.ps1 -IncludeData
```

生成文件：

```text
medical-device-traceability-portable.zip
```

迁移到另一台电脑后，解压并在项目根目录运行启动命令即可。

## 演示建议

- 使用 `admin / admin123` 登录查看完整功能。
- 在“采购闭环”中演示采购申请、审批、到货入库。
- 在“智能仓储”中演示入库、出库、批量盘点。
- 在“临床与维护”中演示患者绑定和维护校准。
- 在“召回与报废”中演示批次召回和报废流程。
- 在“追溯门户”中输入 `RFID-0001`、`UDI-MD-CS-20260001`、`BATCH-DG-202604`、`BATCH-VS-202604` 或 `P2026001` 查看追溯链路。
