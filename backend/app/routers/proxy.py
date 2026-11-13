"""代理池管理接口：维护代理节点，覆盖提交校验、启用节点、停用节点等动作。

调度类接口（裁决、申请、切换、排队、上报、健康校验）统一走 ProxyScheduler，
保证代理选择到重试规则取数用的是同一份冷却判断。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.schemas import ActionResult, EntryPayload, PageResult
from app.services.proxy import ProxyService
from app.services.scheduler import scheduler

router = APIRouter(prefix="/api/proxy", tags=["代理池管理"])

service = ProxyService()

LIST_FIELDS = ["节点编号", "代理地址", "代理类型", "地理位置", "响应延迟", "可用率", "切换策略", "节点状态"]
STATUSES = ["草稿", "待校验", "已启用", "已停用"]


@router.get("", response_model=PageResult[dict])
def list_entries(
    keyword: str | None = Query(default=None, description="按节点编号检索"),
    status: str | None = Query(default=None, description="草稿、待校验、已启用、已停用"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """按节点编号与状态过滤代理池管理列表；没有数据时返回空页，不报错。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    scheduler.sync()
    items, total = service.list_entries(keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/schedule")
def schedule_plan(plan_id: int = Query(..., description="采集计划 id")) -> dict[str, Any]:
    """统一裁决预览：延迟、可用率、最近失败与冷却时间一次算清；只读，不改排队顺序。"""
    return scheduler.adjudicate(plan_id)


@router.get("/queue")
def queue_view() -> dict[str, Any]:
    """待等待清单：当前排队的计划、位次与预计等待，只读。"""
    return {"ok": True, "items": scheduler.queue_view()}


@router.post("/acquire")
def acquire_node(payload: EntryPayload) -> dict[str, Any]:
    """申请节点：同一把锁内完成裁决与租约，最后一个节点只会租给一个计划。"""
    values = payload.values
    return scheduler.acquire(
        int(values.get("plan_id") or 0),
        values.get("operator"),
        values.get("role"),
    )


@router.post("/release")
def release_node(payload: EntryPayload) -> dict[str, Any]:
    """释放节点：归还租约后按优先级给排队计划补位。"""
    values = payload.values
    return scheduler.release(
        int(values.get("plan_id") or 0),
        values.get("operator"),
        values.get("role"),
    )


@router.post("/switch")
def switch_node(payload: EntryPayload) -> dict[str, Any]:
    """切换节点：走同一次裁决，刚失败冷却中的节点不会被换回来。"""
    values = payload.values
    return scheduler.switch(
        int(values.get("plan_id") or 0),
        values.get("operator"),
        values.get("role"),
    )


@router.get("/export")
def export_entries() -> dict[str, Any]:
    """导出代理池管理清单：返回当前过滤条件下的全量数据。"""
    scheduler.sync()
    items, total = service.list_entries(page=1, size=10000)
    return {"module": "proxy", "total": total, "items": items}


@router.get("/{entry_id}", response_model=dict)
def get_entry(entry_id: int) -> dict:
    """读取单条代理节点明细；不存在时给出可读的错误说明。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"代理节点 {entry_id} 不存在或已归档")
    return entry


@router.post("", response_model=ActionResult)
def create_entry(payload: EntryPayload) -> ActionResult:
    """登记一条代理节点，缺字段时说明原因而不是静默丢弃。"""
    entry, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="代理节点已登记", entry=entry)


@router.post("/{entry_id}/actions", response_model=ActionResult)
def run_action(entry_id: int, payload: EntryPayload) -> ActionResult:
    """对单条代理节点执行提交校验、启用节点、停用节点；不允许的动作会被拦下并说明原因。"""
    action = str(payload.values.get("action") or "").strip()
    entry, message = service.run_action(entry_id, action)
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)


@router.post("/{entry_id}/report")
def report_node(entry_id: int, payload: EntryPayload) -> dict[str, Any]:
    """上报节点结果：成功更新延迟与可用率，失败按重试规则进入冷却并改派在租计划。"""
    values = payload.values
    return scheduler.report(
        entry_id,
        result=values.get("result") or "success",
        latency_ms=values.get("latency_ms"),
        error_type=values.get("异常类型") or values.get("error_type"),
        operator=values.get("operator"),
        role=values.get("role"),
    )


@router.post("/{entry_id}/health-check")
def health_check_node(entry_id: int, payload: EntryPayload) -> dict[str, Any]:
    """健康校验：冷却结束的节点只允许通过一次校验再参与裁决。"""
    values = payload.values
    return scheduler.health_check(
        entry_id,
        passed=values.get("passed", True),
        operator=values.get("operator"),
        role=values.get("role"),
    )
