"""代理调度裁决引擎：代理选择、冷却判断、排队互斥与归属审计收在同一次裁决里。

设计约定：
- 响应延迟、可用率、最近失败时间与异常重试规则的冷却时间在 adjudicate() 一次算清；
- 节点顺序先按冷却结束时间、再按站点限制裁决，评分与公平性只在同级内排序；
- 申请、切换、释放都在同一把锁里完成，最后一个节点只会租给一个计划；
- 可用率双速平滑：历史可用率慢速漂移，单次失败抹不掉长期表现（如 PROX-0001）；
- 冷却结束的节点必须先通过一次健康校验，才重新参与裁决；
- 只有站点归属负责人能执行受控操作，所有受控操作都会写入审计日志。
"""
from __future__ import annotations

import threading
from datetime import datetime, timedelta, timezone
from typing import Any

from app.store import store

PROXY_MODULE = "proxy"
RETRY_MODULE = "retry"
PLAN_MODULE = "taskplan"
THROTTLE_MODULE = "throttle"
AUDIT_MODULE = "audit"

# 评分权重：可用率为主、延迟为辅；二者冲突时由冷却结束时间与站点限制先行裁决。
AVAILABILITY_WEIGHT = 0.65
LATENCY_WEIGHT = 0.35
# 双速平滑：近期可用率反应快，历史可用率慢速漂移，单次失败不会抹掉长期表现。
RECENT_ALPHA = 0.30
HISTORY_ALPHA = 0.05
LATENCY_ALPHA = 0.40
DEFAULT_COOLDOWN_SECONDS = 300
DEFAULT_LATENCY_MS = 800.0
MAX_LATENCY_MS = 5000.0
PRIORITY_WEIGHT = {"高": 3, "中": 2, "低": 1}
OWNER_ROLE = "负责人"

STATE_AVAILABLE = "空闲"
STATE_LEASED = "在租"
STATE_COOLING = "冷却中"
STATE_PENDING_VERIFY = "待校验"
STATE_DISABLED = "未启用"

ENABLED_STATUS = "已启用"


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(moment: datetime | None) -> str | None:
    if moment is None:
        return None
    return moment.astimezone(timezone.utc).isoformat(timespec="seconds")


def _parse_float(value: Any, default: float) -> float:
    try:
        return float(str(value).strip().rstrip("%"))
    except (TypeError, ValueError):
        return default


def _parse_ratio(value: Any, default: float) -> float:
    number = _parse_float(value, default)
    if number > 1.5:  # 兼容 97 或 97% 的写法
        number = number / 100.0
    return min(max(number, 0.0), 1.0)


def _parse_int(value: Any, default: int) -> int:
    try:
        return int(float(str(value).strip()))
    except (TypeError, ValueError):
        return default


def _parse_cooldown_seconds(value: Any) -> int | None:
    """冷却时间只接受秒数；日期之类的写法返回 None，让调用方走默认值。"""
    text = str(value or "").strip()
    if not text:
        return None
    try:
        seconds = int(float(text))
    except ValueError:
        return None
    return max(seconds, 0)


class NodeRuntime:
    """节点运行态：只保存在内存里，业务字段仍落在代理池清单上。"""

    def __init__(self, row: dict[str, Any]) -> None:
        self.node_id = int(row.get("id", 0))
        self.latency_ms = _parse_float(row.get("响应延迟"), DEFAULT_LATENCY_MS)
        self.availability_recent = _parse_ratio(row.get("可用率"), 0.9)
        self.availability_history = self.availability_recent
        self.last_failure_at: datetime | None = None
        self.cooldown_until: datetime | None = None
        self.needs_health_check = False
        self.lease: dict[str, Any] | None = None
        self.last_leased_at: datetime | None = None
        self.last_error_type = ""
        self.success_total = 0
        self.failure_total = 0


class ProxyScheduler:
    """代理选择到重试规则取数的统一裁决入口，所有计划共享同一份冷却判断。"""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._runtime: dict[int, NodeRuntime] = {}
        self._queue: list[dict[str, Any]] = []

    # ------------------------------------------------------------------
    # 基础取数
    # ------------------------------------------------------------------
    def _runtime_for(self, row: dict[str, Any]) -> NodeRuntime:
        node_id = int(row.get("id", 0))
        runtime = self._runtime.get(node_id)
        if runtime is None:
            runtime = NodeRuntime(row)
            self._runtime[node_id] = runtime
        return runtime

    def _proxy_rows(self) -> list[dict[str, Any]]:
        return store.rows(PROXY_MODULE)

    def _proxy_by_code(self, code: str) -> dict[str, Any] | None:
        for row in self._proxy_rows():
            if str(row.get("节点编号")) == code:
                return row
        return None

    def _plan(self, plan_id: int) -> dict[str, Any] | None:
        return store.find(PLAN_MODULE, plan_id)

    def _state_of(self, row: dict[str, Any], runtime: NodeRuntime, now: datetime) -> str:
        if str(row.get("status")) != ENABLED_STATUS:
            return STATE_DISABLED
        if runtime.lease is not None:
            return STATE_LEASED
        if runtime.cooldown_until is not None and now < runtime.cooldown_until:
            return STATE_COOLING
        if runtime.needs_health_check:
            return STATE_PENDING_VERIFY
        return STATE_AVAILABLE

    def _sync_row(self, row: dict[str, Any], runtime: NodeRuntime, state: str) -> None:
        """把运行态回写到清单字段，列表页看到的延迟、可用率、节点状态与裁决一致。"""
        row["节点状态"] = state
        row["响应延迟"] = int(round(runtime.latency_ms))
        row["可用率"] = round(0.5 * (runtime.availability_history + runtime.availability_recent), 4)

    def sync(self) -> None:
        """列表页刷新用：把冷却到期等时间驱动的状态变化落到清单上。"""
        now = _now()
        with self._lock:
            for row in self._proxy_rows():
                runtime = self._runtime_for(row)
                self._sync_row(row, runtime, self._state_of(row, runtime, now))

    # ------------------------------------------------------------------
    # 冷却判断：全系统共享同一份结论
    # ------------------------------------------------------------------
    def cooldown_policy(self) -> list[dict[str, Any]]:
        """已启用重试规则换算成的冷却口径，裁决、协同清单都从这里取数。"""
        policy: list[dict[str, Any]] = []
        for row in store.rows(RETRY_MODULE):
            if str(row.get("status")) != ENABLED_STATUS:
                continue
            seconds = _parse_cooldown_seconds(row.get("冷却时间"))
            policy.append({
                "规则编号": row.get("规则编号"),
                "异常类型": str(row.get("异常类型") or "").strip(),
                "冷却秒数": seconds if seconds is not None else DEFAULT_COOLDOWN_SECONDS,
                "重试上限": row.get("重试上限"),
                "退避策略": row.get("退避策略"),
            })
        return policy

    def _cooldown_seconds_for(self, error_type: str) -> int:
        policy = self.cooldown_policy()
        matched = [item["冷却秒数"] for item in policy if item["异常类型"] and item["异常类型"] == error_type]
        if matched:
            return max(matched)
        fallback = [item["冷却秒数"] for item in policy]
        return max(fallback) if fallback else DEFAULT_COOLDOWN_SECONDS

    def cooldown_watch(self, now: datetime | None = None) -> list[dict[str, Any]]:
        with self._lock:
            now = now or _now()
            watch: list[dict[str, Any]] = []
            for row in self._proxy_rows():
                runtime = self._runtime_for(row)
                state = self._state_of(row, runtime, now)
                remaining = 0
                if runtime.cooldown_until is not None and now < runtime.cooldown_until:
                    remaining = int((runtime.cooldown_until - now).total_seconds())
                watch.append({
                    "节点编号": row.get("节点编号"),
                    "服务站点": row.get("服务站点"),
                    "状态": state,
                    "冷却中": state == STATE_COOLING,
                    "剩余冷却秒数": remaining,
                    "冷却结束时间": _iso(runtime.cooldown_until) if state == STATE_COOLING else None,
                    "最近失败时间": _iso(runtime.last_failure_at),
                    "最近异常类型": runtime.last_error_type or None,
                })
            return watch

    # ------------------------------------------------------------------
    # 站点限制与归属
    # ------------------------------------------------------------------
    def _site_limit(self, site: str) -> int | None:
        limits: list[int] = []
        for row in store.rows(THROTTLE_MODULE):
            if str(row.get("目标站点") or "").strip() != site:
                continue
            if str(row.get("是否启用") or "").strip() not in ("是", "启用", "已启用", "true", "1"):
                continue
            limits.append(_parse_int(row.get("并发数量"), 0))
        return min(limits) if limits else None

    def _site_in_use(self, site: str) -> int:
        count = 0
        for row in self._proxy_rows():
            lease = self._runtime_for(row).lease
            if lease and str(lease.get("目标站点") or "") == site:
                count += 1
        return count

    def _site_owners(self, site: str) -> set[str]:
        owners: set[str] = set()
        for row in store.rows(PROXY_MODULE):
            if str(row.get("服务站点") or "").strip() == site:
                owner = str(row.get("归属负责人") or "").strip()
                if owner:
                    owners.add(owner)
        for row in store.rows(PLAN_MODULE):
            if str(row.get("目标站点") or "").strip() == site:
                owner = str(row.get("负责人") or "").strip()
                if owner:
                    owners.add(owner)
        return owners

    def _permission_denied(
        self,
        site: str,
        plan: dict[str, Any] | None,
        operator: Any,
        role: Any,
        action: str,
        now: datetime,
    ) -> dict[str, Any] | None:
        operator_text = str(operator or "").strip()
        role_text = str(role or "").strip()
        owners = self._site_owners(site)
        if not operator_text:
            reason = f"归属不明的请求不能{action}：请提供操作人，排队顺序保持不变"
        elif role_text != OWNER_ROLE:
            reason = f"角色「{role_text or '未声明'}」只读，只有站点归属负责人可以{action}，排队顺序保持不变"
        elif not owners:
            reason = f"站点 {site or '未标注'} 归属不明，{action}已驳回，排队顺序保持不变"
        elif operator_text not in owners:
            reason = (
                f"越权：{operator_text} 不是站点 {site or '未标注'} 的归属负责人"
                f"（{'、'.join(sorted(owners))}），{action}已驳回"
            )
        else:
            return None
        self._audit(action, plan, operator_text or "未知", f"已驳回：{reason}", "无变更", "已驳回", now)
        return {"ok": False, "denied": True, "message": reason}

    # ------------------------------------------------------------------
    # 统一裁决
    # ------------------------------------------------------------------
    def _score(self, runtime: NodeRuntime) -> float:
        blended = 0.5 * runtime.availability_history + 0.5 * runtime.availability_recent
        latency_score = max(0.0, 1.0 - runtime.latency_ms / MAX_LATENCY_MS)
        return round(AVAILABILITY_WEIGHT * blended + LATENCY_WEIGHT * latency_score, 4)

    def _node_reason(
        self,
        *,
        pinned: bool,
        excluded: bool,
        state: str,
        site_allowed: bool,
        node_site: str,
        site: str,
        capacity_open: bool,
        limit: int | None,
        in_use: int,
        remaining: int,
        runtime: NodeRuntime,
        score: float,
    ) -> str:
        if pinned:
            return "当前持有节点，健康则保持，避免频繁切换"
        if excluded:
            return "本次切换的源节点，暂不参与"
        if not site_allowed:
            return f"服务站点 {node_site or '未标注'} 与计划站点 {site or '未标注'} 不符"
        if state == STATE_DISABLED:
            return "节点未启用，不参与裁决"
        if state == STATE_COOLING:
            return f"冷却中，{remaining} 秒后结束（{runtime.last_error_type or '异常'}），恢复需健康校验"
        if state == STATE_PENDING_VERIFY:
            return "冷却已结束，需通过一次健康校验再参与"
        if state == STATE_LEASED:
            lease = runtime.lease or {}
            return f"在租给 {lease.get('计划编号', '其他计划')}"
        if not capacity_open:
            return f"站点并发已达上限（{in_use}/{limit}）"
        return f"可立即执行，评分 {score}"

    def adjudicate(
        self,
        plan_id: int,
        now: datetime | None = None,
        exclude_node_id: int | None = None,
    ) -> dict[str, Any]:
        """同一次裁决算清延迟、可用率、最近失败与冷却，给出可执行节点顺序。"""
        with self._lock:
            now = now or _now()
            plan = self._plan(plan_id)
            if plan is None:
                return {"ok": False, "message": f"采集计划 {plan_id} 不存在或已归档"}
            site = str(plan.get("目标站点") or "").strip()
            priority = str(plan.get("优先级") or "").strip()
            limit = self._site_limit(site)
            in_use = self._site_in_use(site)
            capacity_open = limit is None or in_use < limit
            held = self._lease_of_plan(plan_id)
            held_id = int(held[0].get("id", 0)) if held else None

            items: list[dict[str, Any]] = []
            for row in self._proxy_rows():
                node_id = int(row.get("id", 0))
                runtime = self._runtime_for(row)
                state = self._state_of(row, runtime, now)
                self._sync_row(row, runtime, state)
                node_site = str(row.get("服务站点") or "").strip()
                site_allowed = not node_site or not site or node_site == site
                score = self._score(runtime)
                remaining = 0
                if runtime.cooldown_until is not None and now < runtime.cooldown_until:
                    remaining = int((runtime.cooldown_until - now).total_seconds())
                pinned = held_id == node_id
                excluded = exclude_node_id is not None and node_id == exclude_node_id
                executable = (
                    not pinned
                    and not excluded
                    and site_allowed
                    and state == STATE_AVAILABLE
                    and capacity_open
                )
                if pinned:
                    group = 0
                elif executable:
                    group = 1
                elif state == STATE_COOLING and site_allowed:
                    group = 2
                elif state == STATE_PENDING_VERIFY and site_allowed:
                    group = 3
                elif state == STATE_LEASED and site_allowed:
                    group = 4
                else:
                    group = 5
                items.append({
                    "节点编号": row.get("节点编号"),
                    "代理地址": row.get("代理地址"),
                    "服务站点": node_site or None,
                    "归属负责人": row.get("归属负责人"),
                    "状态": state,
                    "评分": score,
                    "响应延迟ms": int(round(runtime.latency_ms)),
                    "近期可用率": runtime.availability_recent,
                    "历史可用率": runtime.availability_history,
                    "最近失败时间": _iso(runtime.last_failure_at),
                    "冷却结束时间": _iso(runtime.cooldown_until) if state == STATE_COOLING else None,
                    "剩余冷却秒数": remaining,
                    "站点受限": not site_allowed,
                    "可立即执行": executable,
                    "在租计划": (runtime.lease or {}).get("计划编号"),
                    "说明": self._node_reason(
                        pinned=pinned,
                        excluded=excluded,
                        state=state,
                        site_allowed=site_allowed,
                        node_site=node_site,
                        site=site,
                        capacity_open=capacity_open,
                        limit=limit,
                        in_use=in_use,
                        remaining=remaining,
                        runtime=runtime,
                        score=score,
                    ),
                    "_group": group,
                    "_cooldown_ts": runtime.cooldown_until.timestamp() if state == STATE_COOLING and runtime.cooldown_until else 0.0,
                    "_fairness_ts": runtime.last_leased_at.timestamp() if runtime.last_leased_at else 0.0,
                })
            # 冲突裁决：先冷却结束时间、再站点限制（分组已体现），同级内评分与公平性排序。
            items.sort(key=lambda item: (
                item["_group"],
                item["_cooldown_ts"],
                -item["评分"],
                item["_fairness_ts"],
                str(item["节点编号"]),
            ))
            executable_codes = [item["节点编号"] for item in items if item["可立即执行"]]
            wait_advice = None
            if not executable_codes and held is None:
                wait_advice = self._wait_advice(
                    site=site,
                    items=[item for item in items if not item["站点受限"]],
                    limit=limit,
                    in_use=in_use,
                    queued_count=len(self._queue),
                )
            for item in items:
                item.pop("_group", None)
                item.pop("_cooldown_ts", None)
                item.pop("_fairness_ts", None)
            return {
                "ok": True,
                "计划编号": plan.get("计划编号"),
                "目标站点": site,
                "优先级": priority,
                "优先级权重": PRIORITY_WEIGHT.get(priority, 0),
                "站点并发上限": limit,
                "站点在租": in_use,
                "generated_at": _iso(now),
                "冷却规则": self.cooldown_policy(),
                "节点顺序": items,
                "可执行节点": executable_codes,
                "额度耗尽": not executable_codes and held is None,
                "等待建议": wait_advice,
            }

    def _wait_advice(
        self,
        *,
        site: str,
        items: list[dict[str, Any]],
        limit: int | None,
        in_use: int,
        queued_count: int,
    ) -> dict[str, Any]:
        """额度耗尽时的可操作等待建议：告诉调用方等多久、等什么、能做什么。"""
        actions: list[str] = []
        wait_seconds: int | None = None
        pending = [item for item in items if item["状态"] == STATE_PENDING_VERIFY]
        cooling = [item for item in items if item["状态"] == STATE_COOLING]
        if pending:
            codes = "、".join(str(item["节点编号"]) for item in pending)
            actions.append(f"节点 {codes} 冷却已结束，执行一次健康校验即可重新参与裁决")
            wait_seconds = 0
        if cooling:
            soonest = min(cooling, key=lambda item: int(item["剩余冷却秒数"]))
            remaining = int(soonest["剩余冷却秒数"])
            actions.append(
                f"最早 {remaining} 秒后节点 {soonest['节点编号']} 冷却结束"
                f"（{soonest['冷却结束时间']}），恢复前还需一次健康校验"
            )
            wait_seconds = remaining if wait_seconds is None else min(wait_seconds, remaining)
        if limit is not None and in_use >= limit:
            actions.append(
                f"站点 {site or '未标注'} 并发已达上限（{in_use}/{limit}），"
                "可等待在租计划释放，或到采集频率控制上调该站点的并发数量"
            )
        if queued_count:
            actions.append(f"当前还有 {queued_count} 个计划排队，按优先级与入队时间依次补位")
        if not actions:
            actions.append("该站点暂无可用代理节点，请先在代理池登记并启用节点")
        return {"wait_seconds": wait_seconds, "summary": actions[0], "actions": actions}

    # ------------------------------------------------------------------
    # 租约与排队
    # ------------------------------------------------------------------
    def _lease_of_plan(self, plan_id: int) -> tuple[dict[str, Any], NodeRuntime] | None:
        for row in self._proxy_rows():
            runtime = self._runtime_for(row)
            if runtime.lease and int(runtime.lease.get("plan_id", 0)) == int(plan_id):
                return row, runtime
        return None

    def _queue_entry(self, plan_id: int) -> dict[str, Any] | None:
        for entry in self._queue:
            if int(entry["plan_id"]) == int(plan_id):
                return entry
        return None

    def _lease_to(
        self,
        row: dict[str, Any],
        runtime: NodeRuntime,
        plan: dict[str, Any],
        operator: str,
        now: datetime,
    ) -> None:
        runtime.lease = {
            "plan_id": int(plan.get("id", 0)),
            "计划编号": plan.get("计划编号"),
            "目标站点": plan.get("目标站点"),
            "operator": operator,
            "since": _iso(now),
        }
        runtime.last_leased_at = now
        self._sync_row(row, runtime, STATE_LEASED)

    def _enqueue(self, plan: dict[str, Any], operator: str, reason: str, now: datetime) -> dict[str, Any]:
        entry = {
            "plan_id": int(plan.get("id", 0)),
            "计划编号": plan.get("计划编号"),
            "目标站点": plan.get("目标站点"),
            "优先级": plan.get("优先级"),
            "operator": operator,
            "enqueued_at": _iso(now),
            "reason": reason,
        }
        self._queue.append(entry)
        return entry

    def _queue_order(self) -> list[dict[str, Any]]:
        return sorted(
            self._queue,
            key=lambda entry: (-PRIORITY_WEIGHT.get(str(entry.get("优先级") or ""), 0), str(entry.get("enqueued_at") or "")),
        )

    def queue_view(self, now: datetime | None = None) -> list[dict[str, Any]]:
        """待等待清单：每次读取都按当前裁决重算位次与预计等待。"""
        with self._lock:
            now = now or _now()
            view: list[dict[str, Any]] = []
            for index, entry in enumerate(self._queue_order()):
                decision = self.adjudicate(int(entry["plan_id"]), now)
                advice = decision.get("等待建议") or {}
                view.append({
                    "排队位次": index + 1,
                    "计划编号": entry.get("计划编号"),
                    "目标站点": entry.get("目标站点"),
                    "优先级": entry.get("优先级"),
                    "操作人": entry.get("operator"),
                    "入队时间": entry.get("enqueued_at"),
                    "排队原因": entry.get("reason"),
                    "预计等待秒数": advice.get("wait_seconds"),
                    "等待建议": advice.get("summary"),
                })
            return view

    def _drain_queue(self, now: datetime) -> None:
        """容量空出来后按优先级补位；每次补位都会改变容量，逐个重新裁决。"""
        progressed = True
        while progressed:
            progressed = False
            for entry in self._queue_order():
                decision = self.adjudicate(int(entry["plan_id"]), now)
                executable = decision.get("可执行节点") or []
                if not executable:
                    continue
                row = self._proxy_by_code(str(executable[0]))
                plan = self._plan(int(entry["plan_id"]))
                if row is None or plan is None:
                    continue
                runtime = self._runtime_for(row)
                self._lease_to(row, runtime, plan, str(entry.get("operator") or ""), now)
                self._queue.remove(entry)
                self._audit(
                    "排队补位",
                    plan,
                    str(entry.get("operator") or ""),
                    f"排队结束，租得节点 {row.get('节点编号')}",
                    f"{row.get('节点编号')} 空闲 -> 在租",
                    "已生效",
                    now,
                )
                progressed = True
                break

    # ------------------------------------------------------------------
    # 受控操作：申请、释放、切换、上报、健康校验
    # ------------------------------------------------------------------
    def acquire(self, plan_id: int, operator: Any, role: Any, now: datetime | None = None) -> dict[str, Any]:
        """申请节点：锁内完成裁决与租约，最后一个节点只会租给一个计划。"""
        with self._lock:
            now = now or _now()
            plan = self._plan(plan_id)
            if plan is None:
                return {"ok": False, "message": f"采集计划 {plan_id} 不存在或已归档"}
            site = str(plan.get("目标站点") or "").strip()
            denied = self._permission_denied(site, plan, operator, role, "申请节点", now)
            if denied:
                return denied
            held = self._lease_of_plan(plan_id)
            if held is not None:
                row, runtime = held
                return {
                    "ok": True,
                    "message": f"计划已持有节点 {row.get('节点编号')}，无需重复申请",
                    "节点编号": row.get("节点编号"),
                    "lease": dict(runtime.lease or {}),
                }
            existing = self._queue_entry(plan_id)
            if existing is not None:
                view = [item for item in self.queue_view(now) if item["计划编号"] == existing.get("计划编号")]
                return {
                    "ok": True,
                    "message": "计划已在排队，同一计划只保留一个排队结果",
                    "queued": view[0] if view else None,
                }
            decision = self.adjudicate(plan_id, now)
            executable = decision.get("可执行节点") or []
            if executable:
                code = str(executable[0])
                row = self._proxy_by_code(code)
                if row is not None:
                    runtime = self._runtime_for(row)
                    self._lease_to(row, runtime, plan, str(operator).strip(), now)
                    self._audit("申请节点", plan, str(operator).strip(), f"租得节点 {code}", f"{code} 空闲 -> 在租", "已生效", now)
                    return {
                        "ok": True,
                        "message": f"已租得节点 {code}",
                        "节点编号": code,
                        "lease": dict(runtime.lease or {}),
                        "裁决": decision,
                    }
            reason = "节点额度耗尽，进入排队"
            self._enqueue(plan, str(operator).strip(), reason, now)
            self._audit("申请节点", plan, str(operator).strip(), reason, "无节点变更", "已生效", now)
            return {"ok": True, "queued": True, "message": f"{reason}，详见等待建议", "等待建议": decision.get("等待建议")}

    def release(self, plan_id: int, operator: Any, role: Any, now: datetime | None = None) -> dict[str, Any]:
        with self._lock:
            now = now or _now()
            plan = self._plan(plan_id)
            if plan is None:
                return {"ok": False, "message": f"采集计划 {plan_id} 不存在或已归档"}
            site = str(plan.get("目标站点") or "").strip()
            denied = self._permission_denied(site, plan, operator, role, "释放节点", now)
            if denied:
                return denied
            held = self._lease_of_plan(plan_id)
            if held is None:
                return {"ok": False, "message": "计划当前没有持有节点，无需释放"}
            row, runtime = held
            code = str(row.get("节点编号"))
            runtime.lease = None
            self._sync_row(row, runtime, self._state_of(row, runtime, now))
            self._audit("释放节点", plan, str(operator).strip(), f"释放节点 {code}", f"{code} 在租 -> 空闲", "已生效", now)
            self._drain_queue(now)
            return {"ok": True, "message": f"节点 {code} 已释放，排队计划已按优先级补位"}

    def switch(self, plan_id: int, operator: Any, role: Any, now: datetime | None = None) -> dict[str, Any]:
        """切换节点：走同一次裁决，刚失败冷却中的节点不会被换回来。"""
        with self._lock:
            now = now or _now()
            plan = self._plan(plan_id)
            if plan is None:
                return {"ok": False, "message": f"采集计划 {plan_id} 不存在或已归档"}
            site = str(plan.get("目标站点") or "").strip()
            denied = self._permission_denied(site, plan, operator, role, "切换节点", now)
            if denied:
                return denied
            held = self._lease_of_plan(plan_id)
            if held is None:
                return self.acquire(plan_id, operator, role, now)
            current_row, current_runtime = held
            current_code = str(current_row.get("节点编号"))
            current_runtime.lease = None
            decision = self.adjudicate(plan_id, now, exclude_node_id=int(current_row.get("id", 0)))
            executable = decision.get("可执行节点") or []
            if executable:
                best_code = str(executable[0])
                row = self._proxy_by_code(best_code)
                if row is not None:
                    runtime = self._runtime_for(row)
                    self._lease_to(row, runtime, plan, str(operator).strip(), now)
                    self._audit(
                        "切换节点",
                        plan,
                        str(operator).strip(),
                        f"{current_code} -> {best_code}",
                        f"{current_code} 空闲；{best_code} 在租",
                        "已生效",
                        now,
                    )
                    self._drain_queue(now)
                    return {"ok": True, "message": f"已切换：{current_code} → {best_code}", "节点编号": best_code, "裁决": decision}
            state = self._state_of(current_row, current_runtime, now)
            if state == STATE_AVAILABLE:
                # 没有更优节点时回切原节点，避免为切换而切换（切换时机的取舍）。
                self._lease_to(current_row, current_runtime, plan, str(operator).strip(), now)
                self._audit("切换节点", plan, str(operator).strip(), "无更优节点，保持当前节点", "无变更", "已生效", now)
                return {"ok": True, "message": "暂无更优节点，保持当前节点不变", "节点编号": current_code}
            self._enqueue(plan, str(operator).strip(), "切换时无可用节点，进入排队", now)
            self._audit("切换节点", plan, str(operator).strip(), "切换时无可用节点，进入排队", f"{current_code} 不再持有", "已生效", now)
            self._drain_queue(now)
            return {"ok": True, "queued": True, "message": "无可用节点，已进入排队", "等待建议": decision.get("等待建议")}

    def report(
        self,
        node_id: int,
        *,
        result: Any,
        latency_ms: Any = None,
        error_type: Any = "",
        operator: Any,
        role: Any,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        """上报节点结果：成功更新延迟与可用率，失败进入冷却并改派在租计划。"""
        with self._lock:
            now = now or _now()
            row = store.find(PROXY_MODULE, node_id)
            if row is None:
                return {"ok": False, "message": f"代理节点 {node_id} 不存在或已归档"}
            site = str(row.get("服务站点") or "").strip()
            denied = self._permission_denied(site, None, operator, role, "上报结果", now)
            if denied:
                return denied
            runtime = self._runtime_for(row)
            code = str(row.get("节点编号"))
            success = str(result).strip().lower() in ("success", "ok", "true", "成功")
            if success:
                sample = _parse_float(latency_ms, runtime.latency_ms)
                runtime.latency_ms = round(LATENCY_ALPHA * sample + (1 - LATENCY_ALPHA) * runtime.latency_ms, 1)
                runtime.availability_recent = round(RECENT_ALPHA + (1 - RECENT_ALPHA) * runtime.availability_recent, 4)
                runtime.availability_history = round(HISTORY_ALPHA + (1 - HISTORY_ALPHA) * runtime.availability_history, 4)
                runtime.success_total += 1
                self._sync_row(row, runtime, self._state_of(row, runtime, now))
                self._audit(
                    "上报成功",
                    None,
                    str(operator).strip(),
                    f"节点 {code} 成功样本，延迟 {sample:.0f}ms",
                    f"{code} 近期可用率 {runtime.availability_recent}",
                    "已生效",
                    now,
                )
                return {"ok": True, "message": f"节点 {code} 成功样本已记录", "runtime": self._runtime_view(row, runtime, now)}
            # 失败：双速平滑只让近期可用率快降，历史可用率慢速漂移，单次失败抹不掉长期表现。
            error_text = str(error_type or "").strip() or "未分类异常"
            seconds = self._cooldown_seconds_for(error_text)
            runtime.last_failure_at = now
            runtime.cooldown_until = now + timedelta(seconds=seconds)
            runtime.needs_health_check = True
            runtime.availability_recent = round((1 - RECENT_ALPHA) * runtime.availability_recent, 4)
            runtime.availability_history = round((1 - HISTORY_ALPHA) * runtime.availability_history, 4)
            runtime.failure_total += 1
            runtime.last_error_type = error_text
            displaced: str | None = None
            lease = runtime.lease
            if lease is not None:
                runtime.lease = None
                plan = self._plan(int(lease.get("plan_id", 0)))
                if plan is not None and self._queue_entry(int(plan.get("id", 0))) is None:
                    self._enqueue(plan, str(lease.get("operator") or ""), f"节点 {code} 失败，自动改派排队", now)
                    displaced = str(plan.get("计划编号"))
                    self._audit(
                        "自动改派",
                        plan,
                        str(lease.get("operator") or ""),
                        f"节点 {code} 失败，计划改入排队",
                        f"{code} 在租 -> 冷却中",
                        "已生效",
                        now,
                    )
            self._sync_row(row, runtime, self._state_of(row, runtime, now))
            self._audit(
                "上报失败",
                None,
                str(operator).strip(),
                f"节点 {code} 失败（{error_text}），冷却 {seconds} 秒",
                f"{code} 进入冷却，{displaced or '无在租计划'}受影响",
                "已生效",
                now,
            )
            self._drain_queue(now)
            return {
                "ok": True,
                "message": f"节点 {code} 已进入冷却 {seconds} 秒，恢复需先通过一次健康校验",
                "displaced_plan": displaced,
                "cooldown_until": _iso(runtime.cooldown_until),
                "runtime": self._runtime_view(row, runtime, now),
            }

    def health_check(self, node_id: int, *, passed: Any, operator: Any, role: Any, now: datetime | None = None) -> dict[str, Any]:
        """健康校验：冷却结束的节点只允许通过一次校验再参与裁决。"""
        with self._lock:
            now = now or _now()
            row = store.find(PROXY_MODULE, node_id)
            if row is None:
                return {"ok": False, "message": f"代理节点 {node_id} 不存在或已归档"}
            site = str(row.get("服务站点") or "").strip()
            denied = self._permission_denied(site, None, operator, role, "健康校验", now)
            if denied:
                return denied
            runtime = self._runtime_for(row)
            code = str(row.get("节点编号"))
            state = self._state_of(row, runtime, now)
            if state == STATE_COOLING:
                remaining = int((runtime.cooldown_until - now).total_seconds()) if runtime.cooldown_until else 0
                return {"ok": False, "message": f"节点 {code} 仍在冷却中，剩余 {remaining} 秒，暂不能健康校验"}
            if not runtime.needs_health_check:
                return {"ok": False, "message": f"节点 {code} 当前无需健康校验"}
            if bool(passed):
                runtime.needs_health_check = False
                runtime.cooldown_until = None
                self._sync_row(row, runtime, self._state_of(row, runtime, now))
                self._audit("健康校验", None, str(operator).strip(), f"节点 {code} 校验通过，重新参与裁决", f"{code} 待校验 -> 空闲", "已生效", now)
                self._drain_queue(now)
                return {"ok": True, "message": f"节点 {code} 健康校验通过，已重新参与裁决"}
            seconds = self._cooldown_seconds_for(runtime.last_error_type)
            runtime.last_failure_at = now
            runtime.cooldown_until = now + timedelta(seconds=seconds)
            self._sync_row(row, runtime, self._state_of(row, runtime, now))
            self._audit("健康校验", None, str(operator).strip(), f"节点 {code} 校验未通过，重新冷却 {seconds} 秒", f"{code} 待校验 -> 冷却中", "已生效", now)
            return {"ok": False, "message": f"节点 {code} 健康校验未通过，已重新进入冷却 {seconds} 秒"}

    # ------------------------------------------------------------------
    # 协同清单与待办队列
    # ------------------------------------------------------------------
    def coordination(self, now: datetime | None = None) -> dict[str, Any]:
        """协同清单：重试规则清单、共享冷却判断与待等待清单一次给出。"""
        with self._lock:
            now = now or _now()
            self.sync()
            return {
                "ok": True,
                "generated_at": _iso(now),
                "重试规则清单": self.cooldown_policy(),
                "共享冷却判断": self.cooldown_watch(now),
                "待等待清单": self.queue_view(now),
            }

    def todo_queue(self, now: datetime | None = None) -> dict[str, Any]:
        """计划待办队列：跟着裁决结果重排，计划在租、可执行、排队、等冷却一目了然。"""
        with self._lock:
            now = now or _now()
            queue_positions = {item["计划编号"]: item["排队位次"] for item in self.queue_view(now)}
            items: list[dict[str, Any]] = []
            for plan in store.rows(PLAN_MODULE):
                if str(plan.get("status")) == "已完成":
                    continue
                plan_id = int(plan.get("id", 0))
                held = self._lease_of_plan(plan_id)
                queued = self._queue_entry(plan_id)
                decision = self.adjudicate(plan_id, now)
                advice = decision.get("等待建议") or {}
                if held is not None:
                    row, _runtime = held
                    group, status_text, node, wait, note = 0, "在租执行", row.get("节点编号"), 0, "持有节点执行中"
                elif queued is not None:
                    position = queue_positions.get(plan.get("计划编号"))
                    group, status_text, node = 2, "排队中", None
                    wait = advice.get("wait_seconds")
                    note = f"排队第 {position} 位：{queued.get('reason')}"
                elif decision.get("可执行节点"):
                    group, status_text, wait = 1, "可立即执行", 0
                    node = decision["可执行节点"][0]
                    note = "裁决通过，可申请节点"
                else:
                    group, status_text, node = 3, "等待冷却", None
                    wait = advice.get("wait_seconds")
                    note = advice.get("summary") or "暂无可用节点"
                items.append({
                    "计划编号": plan.get("计划编号"),
                    "目标站点": plan.get("目标站点"),
                    "优先级": plan.get("优先级"),
                    "计划状态": plan.get("status"),
                    "调度状态": status_text,
                    "建议节点": node,
                    "预计等待秒数": wait,
                    "说明": note,
                    "_group": group,
                })
            items.sort(key=lambda item: (
                item["_group"],
                -PRIORITY_WEIGHT.get(str(item.get("优先级") or ""), 0),
                item["预计等待秒数"] if item["预计等待秒数"] is not None else 10 ** 9,
                str(item["计划编号"]),
            ))
            for item in items:
                item.pop("_group", None)
            return {"ok": True, "generated_at": _iso(now), "items": items}

    # ------------------------------------------------------------------
    # 审计：受控操作留下归属记录
    # ------------------------------------------------------------------
    def _audit(
        self,
        action: str,
        plan: dict[str, Any] | None,
        operator: str,
        summary: str,
        change: str,
        review: str,
        now: datetime,
    ) -> None:
        rows = store.rows(AUDIT_MODULE)
        next_id = max((int(row.get("id", 0)) for row in rows), default=0) + 1
        seq = 0
        for row in rows:
            code = str(row.get("记录编号") or "")
            if code.startswith("AUDI-"):
                try:
                    seq = max(seq, int(code.split("-", 1)[1]))
                except (IndexError, ValueError):
                    continue
        rows.append({
            "id": next_id,
            "status": "已记录",
            "pending": True,
            "abnormal": review == "已驳回",
            "记录编号": f"AUDI-{seq + 1:04d}",
            "操作类型": action,
            "采集计划": (plan or {}).get("计划编号") or "—",
            "执行人": operator or "未知",
            "执行时间": now.strftime("%Y-%m-%d %H:%M:%S"),
            "结果摘要": summary,
            "变更内容": change,
            "复核状态": review,
        })

    def _runtime_view(self, row: dict[str, Any], runtime: NodeRuntime, now: datetime) -> dict[str, Any]:
        state = self._state_of(row, runtime, now)
        self._sync_row(row, runtime, state)
        return {
            "节点编号": row.get("节点编号"),
            "状态": state,
            "响应延迟ms": round(runtime.latency_ms, 1),
            "近期可用率": runtime.availability_recent,
            "历史可用率": runtime.availability_history,
            "最近失败时间": _iso(runtime.last_failure_at),
            "冷却结束时间": _iso(runtime.cooldown_until) if state == STATE_COOLING else None,
            "累计成功": runtime.success_total,
            "累计失败": runtime.failure_total,
        }


scheduler = ProxyScheduler()
