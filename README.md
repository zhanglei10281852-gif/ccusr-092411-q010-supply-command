# 多业态保供指挥台

果蔬、粮油、水产、休闲食品四业态的带版本数据快照、窗口指标、告警与处置单后端。
基于事件溯源（event sourcing）与双时态时间线构建，解决三类问题：

1. **汇总数字无法还原判断依据**——日报每个数字都带窗口指标与处置单引用，可逐项追溯到来源点位；
2. **数据迟到 / 数据缺失 / 真实供应异常被混为一谈**——窗口指标分别标记 `late` / `missing` / `anomaly`，迟到数据只追加修正版，首算结论不抹除；
3. **班次交接与重复上报污染责任链**——转交只追加唯一责任链，重复上报/派单走幂等键，恢复补算不重复派单。

## 核心设计

| 需求 | 实现 |
| --- | --- |
| 统一时间线 | 所有事实为只追加事件，同时带 `occurred_at`（业务时间）与 `recorded_at`（到达时间） |
| 双时态视图 | `view(late=False)` 还原"夜班当时看到的样子"；`view(late=True)` 用全部已知数据校正历史 |
| 版本快照 | 同一业务时刻的修正数据以新版本追加，不覆盖；规则按版本发布，历史窗口按当时规则计算 |
| 窗口指标 | 30 分钟窗口计算覆盖天数、吞吐压力、告警等级，质量四分类 `ok/late/missing/anomaly` |
| 迟到校正 | 迟到快照触发 `window.recalculated`（revision+1）；已解除告警转 `corrected`，处置单保留原判断并追加 `incident.amended` |
| 告警工作流 | 打开、合并去重、升级、转交、抑制、解除；升级/抑制/解除/修正需角色合规的双人复核 |
| 解除依据 | 解除必须引用窗口指标编号，仅凭汇总日报解除会被拒绝（`BasisError`） |
| 唯一责任链 | `correlation_id` 贯穿告警与处置单；转交追加 `Handoff`，当前责任人始终唯一 |
| 恢复补算 | `recover(until)` 补算全部遗漏窗口，缺失如实标记；窗口、告警、派单均幂等 |
| 血缘追溯 | 日报 → 处置单 → 告警 → 窗口（规则版本）→ 快照 → 来源点位 |
| 沙盘推演 | `WhatIfSandbox` 隔离模拟仓容/到货变化，结果不回写主时间线、不触发派单 |
| 日报核对 | `DailyReport.reconcile` 逐项重算汇总值、检查悬空引用与当日处置单覆盖率 |

## 模块

- `supply_command/events.py`：事件类型与双时态事件记录
- `supply_command/store.py`：SQLite 只追加事件存储（幂等键、双序重放）
- `supply_command/catalog.py`：业态/角色目录与带版本阈值规则
- `supply_command/snapshots.py`：带版本与来源的快照
- `supply_command/windows.py`：窗口指标引擎（覆盖天数/吞吐压力/等级/质量四分类）
- `supply_command/alerts.py`：告警状态机、双人复核、合并与责任链
- `supply_command/incidents.py`：处置单（不可变原判断 + 追加修正、派单幂等）
- `supply_command/projections.py`：统一时间线、双时态视图、补算与重算
- `supply_command/lineage.py`：血缘图与解除依据还原
- `supply_command/whatif.py`：隔离沙盘推演
- `supply_command/reports.py`：日报发布与逐项核对
- `supply_command/app.py`：命令侧服务门面
- `supply_command/api.py` / `serve.py`：零依赖 HTTP API 与启动入口
- `tools/demo.py`：夜班误判 → 早班校正的完整叙事演示
- `tools/validate_contract.py`：领域合同与样例事件一致性校验

## 构建与测试

```bash
python3 -m compileall -q .
python3 -m unittest discover -s tests -v
python3 tools/validate_contract.py
```

## 端到端演示

```bash
python3 tools/demo.py
```

演示内容：夜班 21 点水产严重越线并派单、22 点来源系统故障导致窗口缺失、
23:30 迟到快照到达后历史窗口重算、早班将告警置为 corrected 而处置单保留原判断、
双时态视图对比、血缘追溯与日报逐项核对。

## 启动 HTTP 服务

```bash
python3 -m supply_command.serve --host 0.0.0.0 --port 8080 --db data/supply.db
```

主要接口（均为 JSON；需双人复核的动作携带 `actor` 与 `reviewer`）：

| 方法与路径 | 说明 |
| --- | --- |
| `POST /v1/rules` | 发布规则版本 |
| `POST /v1/snapshots` | 上报快照（按 业务时刻+来源+版本 幂等） |
| `POST /v1/windows/close` | 结算窗口（无数据 → `window.missing`） |
| `POST /v1/recover` | 恢复后补算遗漏窗口，不重复派单 |
| `GET /v1/timeline` | 统一时间线（品类/区域/时间窗过滤） |
| `GET /v1/windows` | 窗口指标（含各 revision） |
| `POST /v1/alerts` | 上报告警（同品类区域活跃告警自动去重） |
| `POST /v1/alerts/{merge,escalate,transfer,suppress,resolve,correct}` | 告警动作 |
| `POST /v1/incidents/{dispatch,resolve}` | 派单（幂等）/办结 |
| `POST /v1/reports` · `GET /v1/reports/{id}/reconcile` | 发布日报 / 逐项核对 |
| `POST /v1/whatif` | 沙盘推演（隔离，不写回） |
| `GET /v1/lineage?node=...` | 血缘追溯 |

### 调用示例

```bash
curl -sX POST localhost:8080/v1/rules -H 'Content-Type: application/json' -d '{
  "business_line":"aquatic","category":"fish","region":"A",
  "safe_stock":20.0,"warn_coverage_days":2.0,"critical_coverage_days":1.0,
  "throughput_per_hour":5.0,"valid_from":"2026-09-01T00:00:00+08:00"}'

curl -sX POST localhost:8080/v1/snapshots -H 'Content-Type: application/json' -d '{
  "business_line":"aquatic","category":"fish","region":"A",
  "observed_at":"2026-09-23T22:05:00+08:00",
  "recorded_at":"2026-09-23T23:30:00+08:00",
  "stock":8.0,"outbound":8.0,"source":"aqs-a01"}'
# -> {"late": true, "recalculated_windows": ["evt-window-...-r2"], ...}
```

所有命令均在项目根目录执行，仅依赖 Python 3.11+ 标准库。
