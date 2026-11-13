"""告警回放引擎：采样点 → 告警事件 → 通知回执 三层因果推演。

纯函数实现：输入一段按到达顺序排列的作业指标样本、规则版本与故障窗口，
输出三层时间轴（采样点、告警事件、通知回执）以及逐序号判定日志。
不读写任何全局状态，同一输入永远得到同一时间轴，前端拖动游标即可重演每一次判定。

核心语义：
- 短暂尖峰抑制：连续越界次数未达 trigger_count 就回落，计数归零，不产生事件与通知；
- 事件身份：持续越界建立事件后，升级沿用原事件身份，达到恢复条件才封口，
  再次越界另开新事件；
- 静默合并：同作业同渠道的静默窗口内，普通候选通知并入摘要通知，
  严重级别提升可打断静默，普通重复样本不能打断；
- 迟到采样：仅补入对应业务时段，不参与判定，不倒改已经发出的通知；
- 阈值改版：规则按版本生效，未封口事件可选择接续旧版本或切换新版本，
  每条通知都带参数快照，历史通知可重演当时决定；
- 回执失败：传输失败只记录在通知的回执层，不反过来创建第二个告警事件。
"""
from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field

SEVERITY_RANK = {"提示": 1, "一般": 2, "严重": 3, "紧急": 4}
SEVERITIES = list(SEVERITY_RANK)
CHANNELS = ["邮件", "短信", "Webhook"]


class ReplaySample(BaseModel):
    """一个采样点：ts 是业务采样时刻，到达顺序由数组下标决定（决策序号从 1 起）。"""

    ts: int = Field(description="业务采样时刻（秒）")
    metric: str = Field(description="指标编号")
    value: float


class RuleVersion(BaseModel):
    """监控规则的某个版本；from_seq 表示从第几个到达样本起生效。"""

    version: int
    metric: str
    threshold: float
    direction: Literal["above", "below"] = "above"  # above 高于阈值越界 / below 低于阈值越界
    trigger_count: int = Field(default=3, ge=1, description="连续越界多少次建立事件")
    recover_count: int = Field(default=2, ge=1, description="连续恢复多少次封口")
    severity: str = "一般"
    silence_seconds: int = Field(default=600, ge=0, description="静默窗口秒数")
    channel: str = "邮件"
    from_seq: int = Field(default=1, ge=1)
    open_event_policy: Literal["keep", "adopt"] = "keep"  # 未封口事件接续旧版 / 切换新版


class FailWindow(BaseModel):
    """通知渠道故障窗口：落在窗口内发送的通知，首次回执记为失败。"""

    channel: str
    from_seq: int = Field(ge=1)
    to_seq: int = Field(ge=1)


class ReplayRequest(BaseModel):
    job: str = Field(description="作业编号")
    samples: list[ReplaySample]
    rule_versions: list[RuleVersion]
    fail_windows: list[FailWindow] = Field(default_factory=list)


def _is_breach(value: float, rule: RuleVersion) -> bool:
    return value > rule.threshold if rule.direction == "above" else value < rule.threshold


def _snapshot(rule: RuleVersion, event: dict[str, Any] | None) -> dict[str, Any]:
    """通知发送当时的参数快照：历史通知重演当时决定就靠它。"""
    snap: dict[str, Any] = {
        "rule_version": rule.version,
        "metric": rule.metric,
        "threshold": rule.threshold,
        "direction": "高于阈值越界" if rule.direction == "above" else "低于阈值越界",
        "trigger_count": rule.trigger_count,
        "recover_count": rule.recover_count,
        "severity": rule.severity,
        "silence_seconds": rule.silence_seconds,
        "channel": rule.channel,
    }
    if event is not None:
        snap["event_rule_version"] = event["active_rule_version"]
        snap["event_policy_note"] = event.get("policy_note") or "事件按建立时的规则版本评估"
    return snap


def replay(req: ReplayRequest) -> dict[str, Any]:
    samples = req.samples
    n = len(samples)

    versions_by_metric: dict[str, list[RuleVersion]] = {}
    for v in req.rule_versions:
        versions_by_metric.setdefault(v.metric, []).append(v)
    for lst in versions_by_metric.values():
        lst.sort(key=lambda v: (v.from_seq, v.version))

    def version_at(metric: str, seq: int) -> RuleVersion | None:
        current: RuleVersion | None = None
        for v in versions_by_metric.get(metric, []):
            if v.from_seq <= seq:
                current = v
        return current

    def version_by_id(metric: str, version: int) -> RuleVersion | None:
        for v in versions_by_metric.get(metric, []):
            if v.version == version:
                return v
        return None

    sample_rows: list[dict[str, Any]] = []
    events: list[dict[str, Any]] = []
    notifications: list[dict[str, Any]] = []
    decisions: list[dict[str, Any]] = []
    states: list[dict[str, Any]] = []

    watermark: dict[str, int] = {}
    streams: dict[str, dict[str, Any]] = {}
    open_events: dict[str, dict[str, Any]] = {}
    silence: dict[str, dict[str, Any]] = {}
    spikes_suppressed = 0

    def log(seq: int, ts: int, kind: str, message: str, **refs: Any) -> None:
        decisions.append({"seq": seq, "ts": ts, "kind": kind, "message": message, **refs})

    def ts_at(seq: int) -> int:
        seq = max(1, min(seq, n))
        return samples[seq - 1].ts if n else 0

    def emit_receipts(notif: dict[str, Any], seq: int, ts: int) -> None:
        """发送通知并记录回执；失败只落在回执层，绝不新建告警事件。"""
        windows = [
            w for w in req.fail_windows
            if w.channel == notif["channel"] and w.from_seq <= seq <= w.to_seq
        ]
        if not windows:
            notif["receipts"].append(
                {"attempt": 1, "seq": seq, "ts": ts, "status": "成功", "detail": "渠道回执正常"}
            )
            return
        notif["receipts"].append({
            "attempt": 1,
            "seq": seq,
            "ts": ts,
            "status": "失败",
            "detail": f"渠道 {notif['channel']} 处于故障窗口",
        })
        retry_seq = min(max(w.to_seq for w in windows) + 1, max(n, 1))
        notif["receipts"].append({
            "attempt": 2,
            "seq": retry_seq,
            "ts": ts_at(retry_seq),
            "status": "成功",
            "detail": "故障恢复后自动重试，仍归属原通知",
        })
        log(seq, ts, "回执失败",
            f"通知 {notif['id']} 传输失败，已记录在回执层：不新建告警事件，也不产生第二条通知",
            notification=notif["id"])
        log(retry_seq, ts_at(retry_seq), "回执成功",
            f"通知 {notif['id']} 自动重试成功，告警事件仍只有 {len(notif['event_ids'])} 个关联事件",
            notification=notif["id"])

    def notify(kind: str, event: dict[str, Any], rule: RuleVersion,
               seq: int, ts: int, sample_ids: list[int]) -> None:
        notif: dict[str, Any] = {
            "id": f"N-{len(notifications) + 1}",
            "kind": kind,
            "seq": seq,
            "ts": ts,
            "event_ids": [event["id"]],
            "metric": event["metric"],
            "severity": rule.severity,
            "channel": rule.channel,
            "disposition": "已发送",
            "merged_into": None,
            "merged_children": [],
            "merge_note": "",
            "silence_broken": False,
            "sample_ids": list(sample_ids),
            "param_snapshot": _snapshot(rule, event),
            "receipts": [],
        }
        channel = rule.channel
        sev_rank = SEVERITY_RANK.get(rule.severity, 0)
        sil = silence.get(channel)
        sil_active = bool(sil and sil["until_ts"] > ts)

        if kind == "恢复":
            log(seq, ts, "通知发送",
                f"恢复通知 {notif['id']} 直接发送（恢复通知不参与静默合并）",
                event=event["id"], notification=notif["id"])
            emit_receipts(notif, seq, ts)
        elif kind == "升级":
            if sil_active:
                notif["silence_broken"] = True
                log(seq, ts, "通知发送",
                    f"升级通知 {notif['id']} 严重级别提升为{rule.severity}，打断静默窗口直接发送",
                    event=event["id"], notification=notif["id"])
            else:
                log(seq, ts, "通知发送", f"升级通知 {notif['id']} 发送",
                    event=event["id"], notification=notif["id"])
            emit_receipts(notif, seq, ts)
            silence[channel] = {
                "until_ts": ts + rule.silence_seconds,
                "anchor_id": notif["id"],
                "severity_rank": sev_rank,
            }
        else:  # 触发
            if sil_active and sev_rank <= sil["severity_rank"]:
                anchor_id = sil["anchor_id"]
                anchor = next((x for x in notifications if x["id"] == anchor_id), None)
                notif["disposition"] = "静默合并"
                notif["merged_into"] = anchor_id
                cross_metric = anchor is not None and anchor["metric"] != notif["metric"]
                boundary = (
                    f"指标 {anchor['metric']} 与 {notif['metric']} 同指作业 {req.job}，"
                    if cross_metric and anchor else f"作业 {req.job} 同渠道 {channel} "
                )
                notif["merge_note"] = (
                    f"{boundary}按 作业+渠道+静默窗口 裁决合并边界："
                    f"窗口至 {sil['until_ts']}s，级别未超窗口级别，并入摘要通知 {anchor_id}"
                )
                if anchor is not None:
                    anchor["merged_children"].append(notif["id"])
                    anchor["event_ids"] = sorted(set(anchor["event_ids"]) | {event["id"]})
                log(seq, ts, "静默合并",
                    f"候选通知 {notif['id']} 落在静默窗口内（至 {sil['until_ts']}s），"
                    f"并入摘要通知 {anchor_id}，不单独发送",
                    event=event["id"], notification=notif["id"])
            else:
                if sil_active:
                    notif["silence_broken"] = True
                    log(seq, ts, "通知发送",
                        f"触发通知 {notif['id']} 级别 {rule.severity} 高于窗口级别，打断静默发送",
                        event=event["id"], notification=notif["id"])
                else:
                    log(seq, ts, "通知发送", f"触发通知 {notif['id']} 发送",
                        event=event["id"], notification=notif["id"])
                emit_receipts(notif, seq, ts)
                silence[channel] = {
                    "until_ts": ts + rule.silence_seconds,
                    "anchor_id": notif["id"],
                    "severity_rank": sev_rank,
                }
        notifications.append(notif)

    for i, sample in enumerate(samples, start=1):
        metric = sample.metric
        stream = streams.setdefault(
            metric, {"breach": 0, "normal": 0, "breach_samples": [], "normal_samples": []}
        )

        # 1) 规则版本在此序号生效；未封口事件按策略决定接续旧版还是切换新版
        for v in versions_by_metric.get(metric, []):
            if v.from_seq == i:
                ev = open_events.get(metric)
                if v.from_seq <= 1:
                    log(i, sample.ts, "规则装载",
                        f"指标 {metric} 装载规则 v{v.version}（阈值 {v.threshold}，"
                        f"连续越界 {v.trigger_count} 次建立事件，级别 {v.severity}）")
                elif ev is not None and v.open_event_policy == "adopt":
                    ev["active_rule_version"] = v.version
                    ev["policy_note"] = f"规则 v{v.version} 生效时事件未封口，按策略切换至新版本评估"
                    log(i, sample.ts, "阈值改版",
                        f"规则 v{v.version} 生效（阈值 {v.threshold}）：未封口事件 {ev['id']} "
                        f"按策略切换至新版本评估", event=ev["id"])
                elif ev is not None:
                    ev["policy_note"] = f"规则 v{v.version} 生效时事件未封口，按策略接续旧版本评估"
                    log(i, sample.ts, "阈值改版",
                        f"规则 v{v.version} 生效（阈值 {v.threshold}）：未封口事件 {ev['id']} "
                        f"接续旧版本 v{ev['active_rule_version']} 评估，新版本仅作用于后续新事件",
                        event=ev["id"])
                else:
                    log(i, sample.ts, "阈值改版",
                        f"规则 v{v.version} 生效（阈值 {v.threshold}，级别 {v.severity}），"
                        f"当前无未封口事件")

        # 2) 迟到采样：只补入对应业务时段，不参与判定，不倒改已发通知
        wm = watermark.get(metric)
        late = wm is not None and sample.ts < wm
        slot_after_seq: int | None = None
        if late:
            slot_candidates = [
                row["seq"] for row in sample_rows
                if row["metric"] == metric and not row["late"] and row["ts"] <= sample.ts
            ]
            slot_after_seq = max(slot_candidates) if slot_candidates else None
            sample_rows.append({
                "seq": i, "ts": sample.ts, "metric": metric, "value": sample.value,
                "late": True, "evaluated": False, "breach": None,
                "threshold": None, "rule_version": None, "slot_after_seq": slot_after_seq,
            })
            log(i, sample.ts, "迟到补入",
                f"采样点迟到（业务时刻 {sample.ts}s 早于已处理水位 {wm}s）："
                f"仅补入对应时段，不参与判定，不倒改已发通知")
        else:
            watermark[metric] = sample.ts
            rule = version_at(metric, i)
            if rule is None:
                sample_rows.append({
                    "seq": i, "ts": sample.ts, "metric": metric, "value": sample.value,
                    "late": False, "evaluated": False, "breach": None,
                    "threshold": None, "rule_version": None, "slot_after_seq": None,
                })
                log(i, sample.ts, "无规则", f"指标 {metric} 没有生效规则，样本仅记录不判定")
            else:
                ev = open_events.get(metric)
                ev_rule = version_by_id(metric, ev["active_rule_version"]) if ev else None
                judge_rule = ev_rule or rule
                breach = _is_breach(sample.value, judge_rule)
                sample_rows.append({
                    "seq": i, "ts": sample.ts, "metric": metric, "value": sample.value,
                    "late": False, "evaluated": True, "breach": breach,
                    "threshold": judge_rule.threshold, "rule_version": judge_rule.version,
                    "slot_after_seq": None,
                })

                if ev is not None and ev_rule is not None:
                    ev["sample_ids"].append(i)
                    if breach:
                        stream["breach"] += 1
                        stream["normal"] = 0
                        stream["breach_samples"].append(i)
                        stream["normal_samples"] = []
                        if SEVERITY_RANK.get(ev_rule.severity, 0) > SEVERITY_RANK.get(ev["severity"], 0):
                            old = ev["severity"]
                            ev["severity"] = ev_rule.severity
                            ev["escalations"].append(
                                {"seq": i, "ts": sample.ts, "from": old, "to": ev_rule.severity}
                            )
                            log(i, sample.ts, "事件升级",
                                f"事件 {ev['id']} 级别 {old} → {ev_rule.severity}，沿用原事件身份",
                                event=ev["id"])
                            notify("升级", ev, ev_rule, i, sample.ts, [i])
                        else:
                            log(i, sample.ts, "重复越界",
                                f"事件 {ev['id']} 持续越界（第 {stream['breach']} 次），"
                                f"普通重复样本不打扰静默", event=ev["id"])
                    else:
                        stream["normal"] += 1
                        stream["breach"] = 0
                        stream["normal_samples"].append(i)
                        stream["breach_samples"] = []
                        if stream["normal"] >= ev_rule.recover_count:
                            ev["status"] = "已封口"
                            ev["closed_seq"] = i
                            ev["closed_ts"] = sample.ts
                            ev["recovery_sample_ids"] = list(
                                stream["normal_samples"][-ev_rule.recover_count:]
                            )
                            del open_events[metric]
                            log(i, sample.ts, "事件恢复",
                                f"连续 {stream['normal']} 次恢复正常，事件 {ev['id']} "
                                f"达到恢复条件封口", event=ev["id"])
                            notify("恢复", ev, ev_rule, i, sample.ts, ev["recovery_sample_ids"])
                        else:
                            log(i, sample.ts, "恢复观察",
                                f"样本回落，事件 {ev['id']} 恢复计数 "
                                f"{stream['normal']}/{ev_rule.recover_count}", event=ev["id"])
                elif breach:
                    stream["breach"] += 1
                    stream["normal"] = 0
                    stream["breach_samples"].append(i)
                    stream["normal_samples"] = []
                    if stream["breach"] >= rule.trigger_count:
                        ev = {
                            "id": f"E-{len(events) + 1}",
                            "metric": metric,
                            "status": "进行中",
                            "severity": rule.severity,
                            "opened_seq": i,
                            "opened_ts": sample.ts,
                            "closed_seq": None,
                            "closed_ts": None,
                            "open_rule_version": rule.version,
                            "active_rule_version": rule.version,
                            "policy_note": "事件按建立时的规则版本评估",
                            "escalations": [],
                            "trigger_sample_ids": list(
                                stream["breach_samples"][-rule.trigger_count:]
                            ),
                            "recovery_sample_ids": [],
                            "sample_ids": list(stream["breach_samples"]),
                        }
                        events.append(ev)
                        open_events[metric] = ev
                        log(i, sample.ts, "事件建立",
                            f"连续越界 {stream['breach']} 次达到建立条件，"
                            f"建立事件 {ev['id']}（级别 {rule.severity}）", event=ev["id"])
                        notify("触发", ev, rule, i, sample.ts, ev["trigger_sample_ids"])
                    else:
                        log(i, sample.ts, "越界计数",
                            f"样本越界，连续计数 {stream['breach']}/{rule.trigger_count}，"
                            f"未达建立条件")
                else:
                    if stream["breach"] > 0:
                        spikes_suppressed += 1
                        log(i, sample.ts, "尖峰抑制",
                            f"短暂尖峰被抑制：连续越界 {stream['breach']} 次未达 "
                            f"{rule.trigger_count} 次即回落，计数归零，不产生事件与通知")
                    else:
                        log(i, sample.ts, "采样正常", "样本在阈值内")
                    stream["breach"] = 0
                    stream["normal"] += 1
                    stream["breach_samples"] = []
                    stream["normal_samples"] = [i]

        # 3) 每个序号结束后的流状态快照，供游标面板展示
        states.append({
            "seq": i,
            "ts": sample.ts,
            "streams": [
                {
                    "metric": m,
                    "breach": s["breach"],
                    "normal": s["normal"],
                    "open_event": (open_events.get(m) or {}).get("id"),
                    "severity": (open_events.get(m) or {}).get("severity"),
                }
                for m, s in streams.items()
            ],
            "silence": [
                {"channel": c, "until_ts": sil["until_ts"], "anchor": sil["anchor_id"]}
                for c, sil in silence.items()
            ],
        })

    decisions.sort(key=lambda d: d["seq"])
    sent = [x for x in notifications if x["disposition"] == "已发送"]
    merged = [x for x in notifications if x["disposition"] == "静默合并"]
    stats = {
        "samples": n,
        "late": sum(1 for row in sample_rows if row["late"]),
        "spikes_suppressed": spikes_suppressed,
        "events": len(events),
        "escalations": sum(len(e["escalations"]) for e in events),
        "notifications_sent": len(sent),
        "notifications_merged": len(merged),
        "receipts_failed": sum(
            1 for x in notifications for r in x["receipts"] if r["status"] == "失败"
        ),
    }
    return {
        "job": req.job,
        "seq_count": n,
        "samples": sample_rows,
        "events": events,
        "notifications": notifications,
        "decisions": decisions,
        "states": states,
        "stats": stats,
    }


def _s(ts: int, metric: str, value: float) -> dict[str, Any]:
    return {"ts": ts, "metric": metric, "value": value}


def _v(version: int, metric: str, threshold: float, **kw: Any) -> dict[str, Any]:
    return {"version": version, "metric": metric, "threshold": threshold, **kw}


M1 = "MONI-0001"
M2 = "MONI-0002"
JOB = "SPID-0003"

SCENARIOS: dict[str, dict[str, Any]] = {
    "spike": {
        "name": "短暂尖峰被抑制",
        "desc": "两次孤立尖峰都未凑满连续越界次数即回落，计数归零，不产生事件与通知。",
        "payload": {
            "job": JOB,
            "samples": [
                _s(0, M1, 200), _s(60, M1, 240), _s(120, M1, 260), _s(180, M1, 950),
                _s(240, M1, 280), _s(300, M1, 300), _s(360, M1, 900), _s(420, M1, 880),
                _s(480, M1, 260), _s(540, M1, 240),
            ],
            "rule_versions": [
                _v(1, M1, 800, trigger_count=3, recover_count=2,
                   severity="一般", silence_seconds=600, channel="邮件"),
            ],
            "fail_windows": [],
        },
    },
    "sustained": {
        "name": "持续越界建事件·升级·恢复",
        "desc": "持续越界建立事件；阈值改版后未封口事件切换新版并升级（沿用原身份），"
                "恢复封口后再次越界另开新事件。",
        "payload": {
            "job": JOB,
            "samples": [
                _s(0, M1, 200), _s(60, M1, 260), _s(120, M1, 900), _s(180, M1, 950),
                _s(240, M1, 1000), _s(300, M1, 1050), _s(360, M1, 1100), _s(420, M1, 1150),
                _s(480, M1, 1200), _s(540, M1, 300), _s(600, M1, 260), _s(660, M1, 900),
                _s(720, M1, 950), _s(780, M1, 1000), _s(840, M1, 300), _s(900, M1, 280),
            ],
            "rule_versions": [
                _v(1, M1, 800, trigger_count=3, recover_count=2,
                   severity="一般", silence_seconds=300, channel="邮件"),
                _v(2, M1, 800, trigger_count=3, recover_count=2, severity="严重",
                   silence_seconds=300, channel="邮件", from_seq=9, open_event_policy="adopt"),
            ],
            "fail_windows": [],
        },
    },
    "silence": {
        "name": "静默合并与升级打断",
        "desc": "静默窗口内的新触发被并入摘要通知；严重级别提升打断静默直接发送。",
        "payload": {
            "job": JOB,
            "samples": [
                _s(0, M1, 200), _s(60, M1, 900), _s(120, M1, 950), _s(180, M1, 400),
                _s(240, M1, 380), _s(300, M1, 900), _s(360, M1, 920), _s(420, M1, 880),
                _s(480, M1, 860), _s(540, M1, 840), _s(600, M1, 900), _s(660, M1, 300),
                _s(720, M1, 280),
            ],
            "rule_versions": [
                _v(1, M1, 800, trigger_count=2, recover_count=2,
                   severity="一般", silence_seconds=600, channel="邮件"),
                _v(2, M1, 800, trigger_count=2, recover_count=2, severity="紧急",
                   silence_seconds=600, channel="邮件", from_seq=11, open_event_policy="adopt"),
            ],
            "fail_windows": [],
        },
    },
    "late": {
        "name": "迟到采样只补入",
        "desc": "一个超高值采样迟到到达：只补入对应业务时段，不参与判定，不倒改已发通知。",
        "payload": {
            "job": JOB,
            "samples": [
                _s(0, M1, 200), _s(60, M1, 240), _s(120, M1, 260), _s(180, M1, 300),
                _s(240, M1, 280), _s(300, M1, 260), _s(120, M1, 1500), _s(360, M1, 240),
                _s(420, M1, 220),
            ],
            "rule_versions": [
                _v(1, M1, 800, trigger_count=3, recover_count=2,
                   severity="一般", silence_seconds=600, channel="邮件"),
            ],
            "fail_windows": [],
        },
    },
    "threshold": {
        "name": "阈值改版·未封口事件接续旧版",
        "desc": "事件未封口时阈值从 800 改为 500：按策略接续旧版评估至封口，"
                "新事件才用新阈值；可切换策略对比。",
        "payload": {
            "job": JOB,
            "samples": [
                _s(0, M1, 300), _s(60, M1, 900), _s(120, M1, 950), _s(180, M1, 1000),
                _s(240, M1, 1050), _s(300, M1, 700), _s(360, M1, 720), _s(420, M1, 600),
                _s(480, M1, 620), _s(540, M1, 300), _s(600, M1, 280),
            ],
            "rule_versions": [
                _v(1, M1, 800, trigger_count=2, recover_count=2,
                   severity="一般", silence_seconds=300, channel="邮件"),
                _v(2, M1, 500, trigger_count=2, recover_count=2, severity="一般",
                   silence_seconds=300, channel="邮件", from_seq=6, open_event_policy="keep"),
            ],
            "fail_windows": [],
        },
    },
    "receipt": {
        "name": "通知发送失败只记回执",
        "desc": "触发通知撞上渠道故障窗口：回执层记录失败并自动重试，"
                "不反过来创建第二个告警事件。",
        "payload": {
            "job": JOB,
            "samples": [
                _s(0, M1, 200), _s(60, M1, 900), _s(120, M1, 950), _s(180, M1, 1000),
                _s(240, M1, 1100), _s(300, M1, 300), _s(360, M1, 280),
            ],
            "rule_versions": [
                _v(1, M1, 800, trigger_count=2, recover_count=2,
                   severity="一般", silence_seconds=300, channel="邮件"),
            ],
            "fail_windows": [{"channel": "邮件", "from_seq": 3, "to_seq": 5}],
        },
    },
    "merge": {
        "name": "多指标同作业合并",
        "desc": "两个指标同时指向同一作业并先后越界：按 作业+渠道+静默窗口 裁决合并边界，"
                "第二条触发并入摘要通知。",
        "payload": {
            "job": JOB,
            "samples": [
                _s(0, M1, 300), _s(0, M2, 99), _s(60, M1, 900), _s(60, M2, 80),
                _s(120, M1, 950), _s(120, M2, 70), _s(180, M1, 400), _s(180, M2, 99),
                _s(240, M1, 380), _s(240, M2, 98),
            ],
            "rule_versions": [
                _v(1, M1, 800, trigger_count=2, recover_count=2,
                   severity="一般", silence_seconds=600, channel="邮件"),
                _v(1, M2, 90, direction="below", trigger_count=2, recover_count=2,
                   severity="一般", silence_seconds=600, channel="邮件"),
            ],
            "fail_windows": [],
        },
    },
}

SCENARIO_META = [
    {"key": key, "name": item["name"], "desc": item["desc"]}
    for key, item in SCENARIOS.items()
]


def get_scenario(key: str) -> dict[str, Any] | None:
    item = SCENARIOS.get(key)
    if item is None:
        return None
    return {"key": key, "name": item["name"], "desc": item["desc"], "payload": item["payload"]}
