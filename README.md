# 多业态保供指挥台

果蔬、粮油、水产、休闲食品四类业态的带版本数据快照与业务事件汇入统一时间线，
按品类/区域/时间窗计算覆盖天数、吞吐压力与告警等级，并驱动告警处置、班次交接、
日报核对的保供指挥后端。仅依赖 Python 3.11 标准库与 SQLite。

## 核心约定

- **双时态**：每条事实同时记录业务时间 `occurred_at` 与入库时间 `recorded_at`。
  当前视图可被迟到数据校正；历史视图按 `recorded_at` 截止回放，夜班解除告警的依据随时可还原。
- **只追加，不覆盖**：快照带版本；窗口重算产生新版本；规则发布产生新版本；
  处置单保留原判断，迟到修正以 `corrected` 链节与处置单修订追加。
- **三类数据状态明确区分**：`ok` / `late`（数据迟到，可校正视图）/
  `missing`（数据缺失，不臆断供应异常）；迟到数据证实的短缺才落为真实告警。
- **幂等**：快照、事件、告警、处置单均带幂等键或指纹，重复上报不新增事件、不重复派单。
- **双人复核**：合并、升级、派单、转交、抑制、解除均需申请人与 commander 复核，
  且两人不得为同一人。
- **唯一责任链**：处置单责任人任一时刻唯一；班次交接只记录交接事实，不改变责任链。
- **隔离推演**：what-if 在数据库克隆上进行，主库不受影响。
- **恢复补算**：系统恢复后补算遗漏窗口，已存在窗口跳过，同一处置单不重复开具。

## 目录

- `domain/contract.json`：实体、状态、事件类型与时间/版本/复核/责任策略。
- `examples/events.json`：迟到快照—窗口重算—解除—修正—日报的时间线样例。
- `app/`：后端实现
  - `clock.py`：可冻结时钟与带时区时间工具。
  - `store.py`：SQLite 双时态表结构、事件时间线、库克隆。
  - `rules.py`：阈值规则版本管理（支持业态专属规则）。
  - `service.py`：摄入、窗口指标、告警工作流、处置单/责任链、班次、日报、血缘、推演、恢复。
  - `api.py`：标准库 HTTP JSON API。
- `tools/validate_contract.py`：领域资料离线校验。
- `tools/demo.py`：夜班误判解除 → 早班迟到数据修正的完整情景演示。
- `tests/`：29 个单元/集成测试。

## 构建与测试

```bash
python3 -m compileall -q .
python3 -m unittest discover -s tests -v
python3 tools/validate_contract.py
```

## 情景演示

```bash
python3 tools/demo.py
```

演示水产采集中断时窗口判为“数据缺失”，夜班据此双人复核解除告警并发布日报；
凌晨积压快照迟到到达后，窗口校正为连续数小时 critical，处置单原判断保留并追加修正，
且可按解除时刻回放历史依据、逐项核对日报。

## HTTP 服务

```bash
python3 -m app.api --db command.db --port 8000
```

主要端点（均为 JSON）：

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/operators` | 注册操作员（operator / commander） |
| POST | `/snapshots` | 带版本快照入库（幂等键去重，迟到自动校正窗口） |
| POST | `/business-events` | 到货、调拨等业务事件入库 |
| POST | `/rules` | 发布阈值规则新版本 |
| POST | `/windows/compute` | 计算/重算窗口，追加新版本 |
| POST | `/alerts/open` | 手工开告警（指纹去重） |
| POST | `/alerts/{id}/{merge,escalate,suppress,resolve,assign}` | 双人复核动作 |
| POST | `/incidents/{id}/transfer` | 转交，责任链追加链节 |
| POST | `/shifts/{id}/{open,handover}` | 开班、交接 |
| POST | `/reports` | 按 as_of 发布日报 |
| GET  | `/reports/{id}/reconcile` | 日报逐项复算核对 |
| GET  | `/dispatches/reconcile` | 全部处置单逐项核对（冻结判断 vs 窗口留档） |
| GET  | `/windows/lineage/{window_id}` | 指标血缘（快照/事件/规则版本） |
| GET  | `/timeline` | 统一事件时间线 |
| POST | `/recovery` | 恢复后补算窗口（不重复派单） |
| POST | `/simulate` | 隔离 what-if 推演 |

请求体可带 `"at": "ISO-8601"` 冻结入库时钟，便于回放迟到与恢复场景。
