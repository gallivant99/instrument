# 医疗器械全生命周期追溯管理系统

这是一个面向毕业设计演示的医疗器械全生命周期追溯管理系统。项目使用 Python 标准库 HTTP 服务、SQLite 数据库和原生 HTML/CSS/JavaScript，不依赖全局 Python 包，不需要 Node/Vite。

## 已覆盖功能

- 管理员：医疗器械基础信息、用户管理、采购员管理、采购计划审核、采购单审批、入库单审核、申领单审批、出入库、科室调拨、召回、报废、质量问题处理、审计与报表。
- 医院工作人员：器械申领、使用登记、患者绑定、质量问题上报、追溯查询。
- 采购员：供应商管理、低库存查看、采购计划提交、审核状态查询、采购到货入库单提交。
- 追溯链路：支持按 UDI、RFID、院内码、批号、患者编号查询采购、入库、申领、使用、质量问题、召回和报废记录。

## 启动方式

推荐双击或运行：

```powershell
powershell -ExecutionPolicy Bypass -File .\start.ps1
```

也可以在项目根目录手动运行：

```powershell
$env:UV_CACHE_DIR = ".uv-cache"
uv run python -m app.server
```

启动后访问：

```text
http://127.0.0.1:8000
```

注意：必须先进入项目根目录再运行 `uv run python -m app.server`，否则会出现 `No module named 'app'`。

## 演示账号

- `admin / admin123`：系统管理员
- `buyer / buyer123`：采购员
- `warehouse / warehouse123`：库房管理员
- `nurse / nurse123`：医院工作人员
- `doctor / doctor123`：临床医生
- `engineer / engineer123`：设备工程师
- `manager / manager123`：管理人员

## 数据库

项目使用 SQLite，数据库文件在：

```text
data/traceability.db
```

重建演示数据库：

```powershell
$env:UV_CACHE_DIR = ".uv-cache"
uv run python scripts/reset_demo_database.py
```

## 测试

```powershell
$env:UV_CACHE_DIR = ".uv-cache"
uv run python -m unittest discover -s tests -v
```

## 迁移打包

生成带当前演示数据的迁移包：

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\create_portable_archive.ps1 -IncludeData
```

生成文件：

```text
medical-device-traceability-portable.zip
```

迁移到另一台电脑后，解压到普通目录，进入项目根目录，运行 `start.ps1` 或上面的 `uv run` 命令即可。
