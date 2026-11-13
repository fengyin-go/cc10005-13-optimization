"""采集监控接口：维护监控指标，覆盖提交校验、启用监控、停用监控等动作。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.schemas import ActionResult, EntryPayload, PageResult
from app.services.alerting import SCENARIO_META, ReplayRequest, get_scenario, replay
from app.services.monitor import MonitorService

router = APIRouter(prefix="/api/monitor", tags=["采集监控"])

service = MonitorService()

LIST_FIELDS = ["指标编号", "关联任务", "指标名称", "预警阈值", "告警级别", "统计粒度", "通知渠道", "静默窗口"]
STATUSES = ["草稿", "待校验", "已启用", "已停用"]


@router.get("/causality/scenarios")
def list_causality_scenarios() -> dict[str, Any]:
    """通知因果视图的预置样本场景：短暂尖峰、持续越界、静默合并、迟到补入、阈值改版、回执失败、多指标合并。"""
    return {"items": SCENARIO_META}


@router.get("/causality/scenarios/{key}")
def get_causality_scenario(key: str) -> dict[str, Any]:
    """读取一个预置场景的完整回放输入（样本、规则版本、故障窗口）。"""
    scenario = get_scenario(key)
    if scenario is None:
        raise HTTPException(status_code=404, detail=f"场景 {key} 不存在，可先查询场景列表")
    return scenario


@router.post("/causality/replay")
def replay_causality(payload: ReplayRequest) -> dict[str, Any]:
    """回放一段作业指标样本：输出采样点、告警事件、通知回执三层时间轴与逐序号判定日志。"""
    if not payload.samples:
        raise HTTPException(status_code=400, detail="样本为空：请至少给一行 作业指标样本")
    if not payload.rule_versions:
        raise HTTPException(status_code=400, detail="缺少规则版本：至少给一个指标的阈值规则")
    return replay(payload)


@router.get("", response_model=PageResult[dict])
def list_entries(
    keyword: str | None = Query(default=None, description="按指标编号检索"),
    status: str | None = Query(default=None, description="草稿、待校验、已启用、已停用"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """按指标编号与状态过滤采集监控列表；没有数据时返回空页，不报错。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    items, total = service.list_entries(keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/export")
def export_entries() -> dict[str, Any]:
    """导出采集监控清单：返回当前过滤条件下的全量数据。

    必须声明在 /{entry_id} 之前，否则 "export" 会先撞上明细路由并被 int 校验拦成 422。
    """
    items, total = service.list_entries(page=1, size=10000)
    return {"module": "monitor", "total": total, "items": items}


@router.get("/{entry_id}", response_model=dict)
def get_entry(entry_id: int) -> dict:
    """读取单条监控指标明细；不存在时给出可读的错误说明。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"监控指标 {entry_id} 不存在或已归档")
    return entry


@router.post("", response_model=ActionResult)
def create_entry(payload: EntryPayload) -> ActionResult:
    """登记一条监控指标，缺字段时说明原因而不是静默丢弃。"""
    entry, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="监控指标已登记", entry=entry)


@router.post("/{entry_id}/actions", response_model=ActionResult)
def run_action(entry_id: int, payload: EntryPayload) -> ActionResult:
    """对单条监控指标执行提交校验、启用监控、停用监控；不允许的动作会被拦下并说明原因。"""
    action = str(payload.values.get("action") or "").strip()
    entry, message = service.run_action(entry_id, action)
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)
