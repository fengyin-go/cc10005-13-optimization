import { useCallback, useEffect, useMemo, useRef, useState } from 'react'

import { request } from '@/api/client'

const ENDPOINT = '/api/monitor/causality'

type ScenarioMeta = { key: string; name: string; desc: string }

type RuleVersionJson = {
  version: number
  metric: string
  threshold: number
  direction: 'above' | 'below'
  trigger_count: number
  recover_count: number
  severity: string
  silence_seconds: number
  channel: string
  from_seq: number
  open_event_policy: 'keep' | 'adopt'
}

type ScenarioPayload = {
  job: string
  samples: { ts: number; metric: string; value: number }[]
  rule_versions: RuleVersionJson[]
  fail_windows: { channel: string; from_seq: number; to_seq: number }[]
}

type SamplePoint = {
  seq: number
  ts: number
  metric: string
  value: number
  late: boolean
  evaluated: boolean
  breach: boolean | null
  threshold: number | null
  rule_version: number | null
  slot_after_seq: number | null
}

type AlertEvent = {
  id: string
  metric: string
  status: string
  severity: string
  opened_seq: number
  opened_ts: number
  closed_seq: number | null
  closed_ts: number | null
  open_rule_version: number
  active_rule_version: number
  escalations: { seq: number; ts: number; from: string; to: string }[]
  trigger_sample_ids: number[]
  recovery_sample_ids: number[]
  sample_ids: number[]
}

type Receipt = { attempt: number; seq: number; ts: number; status: string; detail: string }

type NotificationItem = {
  id: string
  kind: string
  seq: number
  ts: number
  event_ids: string[]
  metric: string
  severity: string
  channel: string
  disposition: string
  merged_into: string | null
  merged_children: string[]
  merge_note: string
  silence_broken: boolean
  sample_ids: number[]
  param_snapshot: Record<string, string | number>
  receipts: Receipt[]
}

type Decision = {
  seq: number
  ts: number
  kind: string
  message: string
  event?: string
  notification?: string
}

type StreamState = {
  seq: number
  ts: number
  streams: { metric: string; breach: number; normal: number; open_event: string | null; severity: string | null }[]
  silence: { channel: string; until_ts: number; anchor: string }[]
}

type ReplayResult = {
  job: string
  seq_count: number
  samples: SamplePoint[]
  events: AlertEvent[]
  notifications: NotificationItem[]
  decisions: Decision[]
  states: StreamState[]
  stats: Record<string, number>
}

type ReplayInput = {
  job: string
  samples: { ts: number; metric: string; value: number }[]
  rule_versions: RuleVersionJson[]
  fail_windows: { channel: string; from_seq: number; to_seq: number }[]
}

type RuleForm = {
  metric: string
  threshold: string
  direction: 'above' | 'below'
  triggerCount: string
  recoverCount: string
  severity: string
  silenceSeconds: string
  channel: string
  v2Enabled: boolean
  v2FromSeq: string
  v2Threshold: string
  v2Severity: string
  v2Policy: 'keep' | 'adopt'
}

const SEVERITIES = ['提示', '一般', '严重', '紧急']
const CHANNELS = ['邮件', '短信', 'Webhook']
const SEVERITY_COLORS: Record<string, string> = { 提示: '#3b82f6', 一般: '#d97706', 严重: '#ea580c', 紧急: '#dc2626' }
const KIND_COLORS: Record<string, string> = { 触发: '#dc2626', 升级: '#ea580c', 恢复: '#16a34a' }
const DECISION_COLORS: Record<string, string> = {
  采样正常: '#16a34a', 越界计数: '#d97706', 尖峰抑制: '#7c3aed', 事件建立: '#dc2626',
  事件升级: '#ea580c', 事件恢复: '#16a34a', 重复越界: '#d97706', 恢复观察: '#0ea5e9',
  通知发送: '#1f6feb', 静默合并: '#64748b', 迟到补入: '#7c3aed', 阈值改版: '#0d9488',
  规则装载: '#0d9488', 回执失败: '#dc2626', 回执成功: '#16a34a', 无规则: '#94a3b8',
}
const SNAPSHOT_LABELS: Record<string, string> = {
  rule_version: '规则版本', metric: '指标编号', threshold: '预警阈值', direction: '越界方向',
  trigger_count: '连续越界建立', recover_count: '连续恢复封口', severity: '告警级别',
  silence_seconds: '静默窗口(秒)', channel: '通知渠道',
  event_rule_version: '事件评估版本', event_policy_note: '未封口事件版本策略',
}
const STAT_LABELS: [string, string][] = [
  ['samples', '采样点'], ['late', '迟到补入'], ['spikes_suppressed', '被抑制尖峰'],
  ['events', '告警事件'], ['escalations', '升级次数'], ['notifications_sent', '已发通知'],
  ['notifications_merged', '静默合并'], ['receipts_failed', '回执失败'],
]

function emptyRule(metric: string): RuleForm {
  return {
    metric, threshold: '800', direction: 'above', triggerCount: '3', recoverCount: '2',
    severity: '一般', silenceSeconds: '600', channel: '邮件',
    v2Enabled: false, v2FromSeq: '', v2Threshold: '', v2Severity: '严重', v2Policy: 'keep',
  }
}

function parseInput(job: string, sampleText: string, rules: RuleForm[], failText: string): ReplayInput {
  const samples: ReplayInput['samples'] = []
  sampleText.split('\n').forEach((line, idx) => {
    const text = line.trim()
    if (!text || text.startsWith('#')) return
    const parts = text.split(',').map((p) => p.trim())
    if (parts.length !== 3) throw new Error(`样本第 ${idx + 1} 行格式应为：时刻,指标编号,数值`)
    const [ts, metric, value] = [Number(parts[0]), parts[1], Number(parts[2])]
    if (!Number.isFinite(ts) || !metric || !Number.isFinite(value)) {
      throw new Error(`样本第 ${idx + 1} 行无法解析：${text}`)
    }
    samples.push({ ts, metric, value })
  })
  if (samples.length === 0) throw new Error('请至少给一行作业指标样本')

  const ruleVersions: RuleVersionJson[] = []
  rules.forEach((rule) => {
    if (!rule.metric.trim()) throw new Error('存在未填写指标编号的规则')
    const base: RuleVersionJson = {
      version: 1,
      metric: rule.metric.trim(),
      threshold: Number(rule.threshold),
      direction: rule.direction,
      trigger_count: Number(rule.triggerCount),
      recover_count: Number(rule.recoverCount),
      severity: rule.severity,
      silence_seconds: Number(rule.silenceSeconds),
      channel: rule.channel,
      from_seq: 1,
      open_event_policy: 'keep',
    }
    if (!Number.isFinite(base.threshold)) throw new Error(`指标 ${base.metric} 的阈值不是数字`)
    ruleVersions.push(base)
    if (rule.v2Enabled) {
      const fromSeq = Number(rule.v2FromSeq)
      if (!Number.isInteger(fromSeq) || fromSeq < 2) {
        throw new Error(`指标 ${base.metric} 的改版生效序号需是不小于 2 的整数`)
      }
      ruleVersions.push({
        ...base,
        version: 2,
        threshold: Number(rule.v2Threshold),
        severity: rule.v2Severity,
        from_seq: fromSeq,
        open_event_policy: rule.v2Policy,
      })
    }
  })
  if (ruleVersions.length === 0) throw new Error('请至少配置一个指标的阈值规则')

  const failWindows: ReplayInput['fail_windows'] = []
  failText.split('\n').forEach((line, idx) => {
    const text = line.trim()
    if (!text || text.startsWith('#')) return
    const parts = text.split(',').map((p) => p.trim())
    if (parts.length !== 3) throw new Error(`故障窗口第 ${idx + 1} 行格式应为：渠道,起始序号,结束序号`)
    failWindows.push({ channel: parts[0], from_seq: Number(parts[1]), to_seq: Number(parts[2]) })
  })
  return { job: job.trim() || 'SPID-0003', samples, rule_versions: ruleVersions, fail_windows: failWindows }
}

export default function CausalityPanel() {
  const [scenarios, setScenarios] = useState<ScenarioMeta[]>([])
  const [scenarioKey, setScenarioKey] = useState('')
  const [scenarioDesc, setScenarioDesc] = useState('')
  const [job, setJob] = useState('SPID-0003')
  const [sampleText, setSampleText] = useState('')
  const [rules, setRules] = useState<RuleForm[]>([])
  const [failText, setFailText] = useState('')
  const [result, setResult] = useState<ReplayResult | null>(null)
  const [cursor, setCursor] = useState(1)
  const [selectedNotif, setSelectedNotif] = useState<string | null>(null)
  const [errorMessage, setErrorMessage] = useState('')
  const [dirty, setDirty] = useState(false)
  const logRef = useRef<HTMLDivElement | null>(null)

  const markDirty = useCallback(() => setDirty(true), [])

  const runReplay = useCallback(async (input: ReplayInput) => {
    setErrorMessage('')
    try {
      const response = await request(`${ENDPOINT}/replay`, {
        method: 'POST',
        body: JSON.stringify(input),
      })
      if (!response.ok) {
        const detail = await response.json().catch(() => null)
        throw new Error(detail?.detail ?? '回放请求未生效，请检查样本与规则')
      }
      const payload = (await response.json()) as ReplayResult
      setResult(payload)
      setCursor(1)
      setSelectedNotif(null)
      setDirty(false)
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '回放失败')
    }
  }, [])

  const loadScenario = useCallback(async (key: string) => {
    setErrorMessage('')
    try {
      const response = await request(`${ENDPOINT}/scenarios/${key}`)
      if (!response.ok) throw new Error('场景读取失败')
      const data = (await response.json()) as { name: string; desc: string; payload: ScenarioPayload }
      const payload = data.payload
      setScenarioDesc(data.desc)
      setJob(payload.job)
      setSampleText(payload.samples.map((s) => `${s.ts},${s.metric},${s.value}`).join('\n'))
      const byMetric = new Map<string, RuleVersionJson[]>()
      payload.rule_versions.forEach((v) => {
        byMetric.set(v.metric, [...(byMetric.get(v.metric) ?? []), v])
      })
      setRules([...byMetric.entries()].map(([metric, vs]) => {
        const sorted = [...vs].sort((a, b) => a.from_seq - b.from_seq)
        const v1 = sorted[0]
        const v2 = sorted[1]
        return {
          metric,
          threshold: String(v1.threshold),
          direction: v1.direction,
          triggerCount: String(v1.trigger_count),
          recoverCount: String(v1.recover_count),
          severity: v1.severity,
          silenceSeconds: String(v1.silence_seconds),
          channel: v1.channel,
          v2Enabled: Boolean(v2),
          v2FromSeq: v2 ? String(v2.from_seq) : '',
          v2Threshold: v2 ? String(v2.threshold) : '',
          v2Severity: v2 ? v2.severity : '严重',
          v2Policy: v2 ? v2.open_event_policy : 'keep',
        }
      }))
      setFailText(payload.fail_windows.map((w) => `${w.channel},${w.from_seq},${w.to_seq}`).join('\n'))
      await runReplay(payload)
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '场景载入失败')
    }
  }, [runReplay])

  useEffect(() => {
    void (async () => {
      try {
        const response = await request(`${ENDPOINT}/scenarios`)
        if (!response.ok) throw new Error('场景列表读取失败')
        const payload = (await response.json()) as { items: ScenarioMeta[] }
        setScenarios(payload.items)
        const first = payload.items.find((item) => item.key === 'sustained') ?? payload.items[0]
        if (first) {
          setScenarioKey(first.key)
          await loadScenario(first.key)
        }
      } catch (error) {
        setErrorMessage(error instanceof Error ? error.message : '场景列表读取失败')
      }
    })()
  }, [loadScenario])

  useEffect(() => {
    if (logRef.current) logRef.current.scrollTop = logRef.current.scrollHeight
  }, [cursor, result])

  const handleReplay = useCallback(() => {
    try {
      void runReplay(parseInput(job, sampleText, rules, failText))
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '输入解析失败')
    }
  }, [job, sampleText, rules, failText, runReplay])

  const selected = useMemo(
    () => result?.notifications.find((item) => item.id === selectedNotif) ?? null,
    [result, selectedNotif],
  )

  const updateRule = (index: number, patch: Partial<RuleForm>) => {
    setRules((prev) => prev.map((rule, i) => (i === index ? { ...rule, ...patch } : rule)))
    markDirty()
  }

  return (
    <div className="causality" data-module="monitor-causality">
      <div className="panel-card">
        <div className="panel-head">
          <div>
            <h3>通知因果视图</h3>
            <p className="page-desc">
              给一段作业指标样本，拖动回放游标：看短暂尖峰为何被抑制、持续越界何时建立事件、静默期内哪些通知被合并。
            </p>
          </div>
          <div className="scenario-picker">
            <select
              value={scenarioKey}
              onChange={(event) => {
                setScenarioKey(event.target.value)
                void loadScenario(event.target.value)
              }}
            >
              {scenarios.map((item) => (
                <option key={item.key} value={item.key}>{item.name}</option>
              ))}
            </select>
            <button className="btn primary" type="button" onClick={handleReplay}>开始回放</button>
          </div>
        </div>
        {scenarioDesc && <p className="scenario-desc">场景说明：{scenarioDesc}</p>}

        <div className="editor-grid">
          <div className="editor-col">
            <label className="editor-label">
              <span>作业指标样本（每行：时刻秒,指标编号,数值；按到达顺序排列，时刻早于已处理水位即判迟到）</span>
              <textarea
                className="sample-textarea"
                rows={10}
                value={sampleText}
                onChange={(event) => { setSampleText(event.target.value); markDirty() }}
                spellCheck={false}
              />
            </label>
            <label className="editor-label">
              <span>作业编号</span>
              <input value={job} onChange={(event) => { setJob(event.target.value); markDirty() }} />
            </label>
            <label className="editor-label">
              <span>渠道故障窗口（每行：渠道,起始序号,结束序号；失败只记回执层）</span>
              <textarea
                className="sample-textarea small"
                rows={2}
                value={failText}
                placeholder="邮件,3,5"
                onChange={(event) => { setFailText(event.target.value); markDirty() }}
                spellCheck={false}
              />
            </label>
          </div>
          <div className="editor-col">
            {rules.map((rule, index) => (
              <div key={index} className="rule-card">
                <div className="rule-head">
                  <strong>指标规则 · {rule.metric || '未命名'}</strong>
                  <button className="link" type="button" onClick={() => { setRules((prev) => prev.filter((_, i) => i !== index)); markDirty() }}>
                    移除
                  </button>
                </div>
                <div className="rule-grid">
                  <label><span>指标编号</span>
                    <input value={rule.metric} onChange={(e) => updateRule(index, { metric: e.target.value })} />
                  </label>
                  <label><span>预警阈值</span>
                    <input value={rule.threshold} onChange={(e) => updateRule(index, { threshold: e.target.value })} />
                  </label>
                  <label><span>越界方向</span>
                    <select value={rule.direction} onChange={(e) => updateRule(index, { direction: e.target.value as 'above' | 'below' })}>
                      <option value="above">高于阈值越界</option>
                      <option value="below">低于阈值越界</option>
                    </select>
                  </label>
                  <label><span>连续越界建立</span>
                    <input value={rule.triggerCount} onChange={(e) => updateRule(index, { triggerCount: e.target.value })} />
                  </label>
                  <label><span>连续恢复封口</span>
                    <input value={rule.recoverCount} onChange={(e) => updateRule(index, { recoverCount: e.target.value })} />
                  </label>
                  <label><span>告警级别</span>
                    <select value={rule.severity} onChange={(e) => updateRule(index, { severity: e.target.value })}>
                      {SEVERITIES.map((s) => <option key={s} value={s}>{s}</option>)}
                    </select>
                  </label>
                  <label><span>静默窗口(秒)</span>
                    <input value={rule.silenceSeconds} onChange={(e) => updateRule(index, { silenceSeconds: e.target.value })} />
                  </label>
                  <label><span>通知渠道</span>
                    <select value={rule.channel} onChange={(e) => updateRule(index, { channel: e.target.value })}>
                      {CHANNELS.map((c) => <option key={c} value={c}>{c}</option>)}
                    </select>
                  </label>
                </div>
                <label className="rule-v2-toggle">
                  <input
                    type="checkbox"
                    checked={rule.v2Enabled}
                    onChange={(e) => updateRule(index, { v2Enabled: e.target.checked })}
                  />
                  <span>中途修改预警阈值（产生规则 v2）</span>
                </label>
                {rule.v2Enabled && (
                  <div className="rule-grid v2">
                    <label><span>v2 生效序号</span>
                      <input value={rule.v2FromSeq} placeholder="如 6" onChange={(e) => updateRule(index, { v2FromSeq: e.target.value })} />
                    </label>
                    <label><span>v2 预警阈值</span>
                      <input value={rule.v2Threshold} onChange={(e) => updateRule(index, { v2Threshold: e.target.value })} />
                    </label>
                    <label><span>v2 告警级别</span>
                      <select value={rule.v2Severity} onChange={(e) => updateRule(index, { v2Severity: e.target.value })}>
                        {SEVERITIES.map((s) => <option key={s} value={s}>{s}</option>)}
                      </select>
                    </label>
                    <label><span>未封口事件</span>
                      <select value={rule.v2Policy} onChange={(e) => updateRule(index, { v2Policy: e.target.value as 'keep' | 'adopt' })}>
                        <option value="keep">接续旧版本评估</option>
                        <option value="adopt">切换新版本评估</option>
                      </select>
                    </label>
                  </div>
                )}
              </div>
            ))}
            <button className="btn ghost" type="button" onClick={() => { setRules((prev) => [...prev, emptyRule(`MONI-000${prev.length + 1}`)]); markDirty() }}>
              添加指标规则
            </button>
          </div>
        </div>
        {errorMessage && <p className="error-text">{errorMessage}</p>}
        {dirty && result && <p className="dirty-hint">配置已变更，点击「开始回放」刷新时间轴。</p>}
      </div>

      {result && (
        <>
          <div className="stat-row">
            {STAT_LABELS.map(([key, label]) => (
              <article key={key} className="stat-card">
                <span className="stat-label">{label}</span>
                <strong className="stat-value">{result.stats[key] ?? 0}</strong>
              </article>
            ))}
          </div>

          <div className="timeline-layout">
            <div className="panel-card timeline-card">
              <Timeline
                result={result}
                cursor={cursor}
                onCursor={setCursor}
                selectedNotif={selectedNotif}
                onSelectNotif={setSelectedNotif}
              />
              <div className="cursor-row">
                <input
                  type="range"
                  min={1}
                  max={Math.max(result.seq_count, 1)}
                  value={cursor}
                  onChange={(event) => setCursor(Number(event.target.value))}
                />
                <span className="cursor-label">回放游标 #{cursor} / {result.seq_count}</span>
              </div>
              <div className="legend">
                <span className="legend-item"><i className="dot normal" />正常样本</span>
                <span className="legend-item"><i className="dot breach" />越界样本</span>
                <span className="legend-item"><i className="dot late" />迟到补入</span>
                <span className="legend-item"><i className="swatch event" />告警事件（色随级别）</span>
                <span className="legend-item"><i className="triangle" />已发通知</span>
                <span className="legend-item"><i className="dot merged" />静默合并</span>
              </div>
            </div>

            <aside className="panel-card decision-panel">
              <h4>游标处判定 · #{cursor}</h4>
              <CursorState result={result} cursor={cursor} />
              <div className="decision-log" ref={logRef}>
                {result.decisions.filter((d) => d.seq <= cursor).map((d, idx) => (
                  <div key={idx} className={`decision-item${d.seq === cursor ? ' active' : ''}`}>
                    <span className="decision-kind" style={{ background: DECISION_COLORS[d.kind] ?? '#64748b' }}>
                      {d.kind}
                    </span>
                    <span className="decision-msg">
                      <em>#{d.seq} · {d.ts}s</em>{d.message}
                    </span>
                  </div>
                ))}
              </div>
            </aside>
          </div>

          {selected && (
            <NotificationDrawer
              notification={selected}
              result={result}
              onSelect={setSelectedNotif}
              onClose={() => setSelectedNotif(null)}
            />
          )}
        </>
      )}
    </div>
  )
}

function CursorState({ result, cursor }: { result: ReplayResult; cursor: number }) {
  const state = result.states[cursor - 1]
  if (!state) return null
  return (
    <div className="state-chips">
      {state.streams.map((s) => (
        <span key={s.metric} className="chip">
          {s.metric}：连续越界 {s.breach} · 恢复计数 {s.normal}
          {s.open_event ? ` · 未封口 ${s.open_event}（${s.severity}）` : ' · 无未封口事件'}
        </span>
      ))}
      {state.silence.map((sil) => (
        <span key={sil.channel} className={`chip${sil.until_ts > state.ts ? ' warn' : ''}`}>
          {sil.channel}静默{sil.until_ts > state.ts ? `至 ${sil.until_ts}s（锚点 ${sil.anchor}）` : '已结束'}
        </span>
      ))}
    </div>
  )
}

function Timeline({ result, cursor, onCursor, selectedNotif, onSelectNotif }: {
  result: ReplayResult
  cursor: number
  onCursor: (seq: number) => void
  selectedNotif: string | null
  onSelectNotif: (id: string) => void
}) {
  const svgRef = useRef<SVGSVGElement | null>(null)
  const n = Math.max(result.seq_count, 1)
  const metrics = useMemo(() => [...new Set(result.samples.map((s) => s.metric))], [result])
  const highlightSamples = useMemo(() => {
    const notif = result.notifications.find((item) => item.id === selectedNotif)
    return new Set(notif?.sample_ids ?? [])
  }, [result, selectedNotif])

  const W = 1000
  const PADL = 132
  const PADR = 36
  const AXIS_H = 26
  const SAMPLE_H = 78
  const EVENT_H = 30
  const NOTIF_H = 92
  const METRIC_GAP = 14
  const metricBlock = SAMPLE_H + EVENT_H + METRIC_GAP
  const H = AXIS_H + metrics.length * metricBlock + NOTIF_H + 8
  const span = W - PADL - PADR
  const x = (seq: number) => (n <= 1 ? PADL + span / 2 : PADL + ((seq - 1) / (n - 1)) * span)

  const domains = useMemo(() => {
    const map = new Map<string, { min: number; max: number }>()
    metrics.forEach((metric) => {
      const values: number[] = []
      result.samples.forEach((s) => {
        if (s.metric !== metric) return
        values.push(s.value)
        if (s.threshold !== null) values.push(s.threshold)
      })
      let min = Math.min(...values)
      let max = Math.max(...values)
      if (min === max) { min -= 1; max += 1 }
      const pad = (max - min) * 0.15
      map.set(metric, { min: min - pad, max: max + pad })
    })
    return map
  }, [metrics, result])

  const notifLaneTop = AXIS_H + metrics.length * metricBlock + 4

  const moveCursor = useCallback((clientX: number) => {
    const svg = svgRef.current
    if (!svg) return
    const rect = svg.getBoundingClientRect()
    const scale = rect.width / W
    const px = (clientX - rect.left) / scale
    const seq = n <= 1 ? 1 : Math.round(((px - PADL) / span) * (n - 1)) + 1
    onCursor(Math.min(n, Math.max(1, seq)))
  }, [n, onCursor, span, W, PADL])

  const notifById = useMemo(
    () => new Map(result.notifications.map((item) => [item.id, item])),
    [result],
  )

  return (
    <svg
      ref={svgRef}
      className="timeline-svg"
      viewBox={`0 0 ${W} ${H}`}
      onPointerDown={(event) => {
        event.currentTarget.setPointerCapture(event.pointerId)
        moveCursor(event.clientX)
      }}
      onPointerMove={(event) => {
        if (event.buttons === 1) moveCursor(event.clientX)
      }}
    >
      {/* 顶部序号轴 */}
      {Array.from({ length: n }, (_, i) => i + 1).map((seq) => (
        <g key={seq}>
          <line x1={x(seq)} y1={AXIS_H - 8} x2={x(seq)} y2={AXIS_H} stroke="#cbd5e1" />
          <text x={x(seq)} y={AXIS_H - 11} textAnchor="middle" fontSize="9" fill="#64748b">#{seq}</text>
        </g>
      ))}

      {metrics.map((metric, mIdx) => {
        const top = AXIS_H + mIdx * metricBlock
        const eventTop = top + SAMPLE_H + 4
        const domain = domains.get(metric) ?? { min: 0, max: 1 }
        const yValue = (v: number) => top + SAMPLE_H - 8 - ((v - domain.min) / (domain.max - domain.min)) * (SAMPLE_H - 16)
        const points = result.samples.filter((s) => s.metric === metric)
        const linePoints = points.filter((s) => !s.late)
        const metricEvents = result.events.filter((e) => e.metric === metric)
        return (
          <g key={metric}>
            <text x={8} y={top + 14} fontSize="11" fill="#334155">采样点 · {metric}</text>
            <rect x={PADL} y={top} width={span} height={SAMPLE_H} fill="#f8fafc" stroke="#e2e8f0" />
            {/* 阈值阶梯线：随规则版本变化 */}
            {linePoints.map((s) => {
              if (s.threshold === null || s.seq >= n) return null
              return (
                <line
                  key={`th-${s.seq}`}
                  x1={x(s.seq)} y1={yValue(s.threshold)} x2={x(s.seq + 1)} y2={yValue(s.threshold)}
                  stroke="#dc2626" strokeDasharray="4 3" strokeWidth="1" opacity="0.7"
                />
              )
            })}
            {/* 到达顺序连线 */}
            <polyline
              fill="none" stroke="#94a3b8" strokeWidth="1.2"
              points={linePoints.map((s) => `${x(s.seq)},${yValue(s.value)}`).join(' ')}
            />
            {points.map((s) => {
              const cx = x(s.seq)
              const cy = yValue(s.value)
              if (s.late) {
                const ghostX = s.slot_after_seq !== null ? x(s.slot_after_seq) : null
                return (
                  <g key={s.seq}>
                    {ghostX !== null && (
                      <>
                        <line x1={ghostX} y1={cy} x2={cx} y2={cy} stroke="#7c3aed" strokeDasharray="2 3" />
                        <circle cx={ghostX} cy={cy} r="4" fill="none" stroke="#7c3aed" strokeDasharray="2 2" />
                      </>
                    )}
                    <circle cx={cx} cy={cy} r="5" fill="#ede9fe" stroke="#7c3aed" strokeDasharray="2 2" />
                    <text x={cx} y={cy - 9} textAnchor="middle" fontSize="9" fill="#7c3aed">迟到</text>
                    <title>{`#${s.seq} ${s.ts}s 迟到补入：仅补入对应时段，不参与判定`}</title>
                  </g>
                )
              }
              const color = !s.evaluated ? '#94a3b8' : s.breach ? '#dc2626' : '#16a34a'
              return (
                <g key={s.seq}>
                  <circle cx={cx} cy={cy} r="4.5" fill={color} />
                  {highlightSamples.has(s.seq) && (
                    <circle cx={cx} cy={cy} r="8" fill="none" stroke="#1f6feb" strokeWidth="1.6" />
                  )}
                  <title>{`#${s.seq} ${s.ts}s ${s.metric}=${s.value}${s.breach ? '（越界）' : ''}`}</title>
                </g>
              )
            })}

            {/* 告警事件层 */}
            <text x={8} y={eventTop + 13} fontSize="11" fill="#334155">告警事件</text>
            <rect x={PADL} y={eventTop} width={span} height={EVENT_H - 8} fill="#f8fafc" stroke="#e2e8f0" />
            {metricEvents.map((ev) => {
              const x1 = x(ev.opened_seq)
              const x2 = ev.closed_seq !== null ? x(ev.closed_seq) : x(n) + 6
              const color = SEVERITY_COLORS[ev.severity] ?? '#d97706'
              return (
                <g key={ev.id}>
                  <rect x={x1} y={eventTop + 3} width={Math.max(x2 - x1, 8)} height={EVENT_H - 14} rx="3"
                    fill={color} opacity="0.85" />
                  <text x={x1 + 4} y={eventTop + 14} fontSize="9" fill="#fff" pointerEvents="none">
                    {`${ev.id}·${ev.severity}${ev.closed_seq === null ? '·未封口' : ''}`}
                  </text>
                  {ev.escalations.map((esc) => (
                    <g key={esc.seq}>
                      <path
                        d={`M ${x(esc.seq)} ${eventTop - 1} l 4 6 l -8 0 Z`}
                        fill="#ea580c" stroke="#fff" strokeWidth="0.6"
                      />
                      <title>{`#${esc.seq} 事件升级 ${esc.from} → ${esc.to}，沿用原身份`}</title>
                    </g>
                  ))}
                  <title>{`${ev.id} ${ev.metric} ${ev.opened_ts}s 建立${ev.closed_ts !== null ? `，${ev.closed_ts}s 封口` : '，未封口'}`}</title>
                </g>
              )
            })}
          </g>
        )
      })}

      {/* 通知回执层 */}
      <text x={8} y={notifLaneTop + 14} fontSize="11" fill="#334155">通知回执</text>
      <rect x={PADL} y={notifLaneTop} width={span} height={NOTIF_H - 16} fill="#f8fafc" stroke="#e2e8f0" />
      {result.notifications.map((notif) => {
        const cx = x(notif.seq)
        const cy = notifLaneTop + 34
        const isSelected = selectedNotif === notif.id
        if (notif.disposition === '静默合并') {
          const anchor = notif.merged_into ? notifById.get(notif.merged_into) : undefined
          return (
            <g key={notif.id} style={{ cursor: 'pointer' }} onClick={() => onSelectNotif(notif.id)}>
              {anchor && (
                <line x1={x(anchor.seq)} y1={cy - 12} x2={cx} y2={cy - 12}
                  stroke="#94a3b8" strokeDasharray="2 3" />
              )}
              <circle cx={cx} cy={cy} r="5" fill="#fff" stroke="#64748b" strokeWidth="1.5" />
              {isSelected && <circle cx={cx} cy={cy} r="9" fill="none" stroke="#1f6feb" strokeWidth="1.6" />}
              <title>{`${notif.id} 静默合并 → ${notif.merged_into ?? ''}`}</title>
            </g>
          )
        }
        const color = KIND_COLORS[notif.kind] ?? '#1f6feb'
        const failed = notif.receipts.some((r) => r.status === '失败')
        return (
          <g key={notif.id} style={{ cursor: 'pointer' }} onClick={() => onSelectNotif(notif.id)}>
            <path d={`M ${cx} ${cy - 8} l 8 14 l -16 0 Z`} fill={color} />
            {failed && <circle cx={cx} cy={cy} r="12" fill="none" stroke="#dc2626" strokeWidth="1.6" strokeDasharray="3 2" />}
            {notif.silence_broken && <text x={cx} y={cy - 14} textAnchor="middle" fontSize="10" fill="#ea580c">⚡</text>}
            {notif.merged_children.length > 0 && (
              <text x={cx + 10} y={cy - 6} fontSize="9" fill="#64748b">+{notif.merged_children.length}</text>
            )}
            {isSelected && <circle cx={cx} cy={cy} r="13" fill="none" stroke="#1f6feb" strokeWidth="1.6" />}
            <title>{`${notif.id} ${notif.kind} · ${notif.channel}${failed ? ' · 回执失败已重试' : ''}`}</title>
          </g>
        )
      })}

      {/* 未来遮罩与回放游标 */}
      <rect x={x(cursor)} y={0} width={W - x(cursor)} height={H} fill="#ffffff" opacity="0.55" pointerEvents="none" />
      <line x1={x(cursor)} y1={0} x2={x(cursor)} y2={H} stroke="#1f6feb" strokeWidth="1.6" pointerEvents="none" />
      <circle cx={x(cursor)} cy={6} r="5" fill="#1f6feb" pointerEvents="none" />
    </svg>
  )
}

function NotificationDrawer({ notification, result, onSelect, onClose }: {
  notification: NotificationItem
  result: ReplayResult
  onSelect: (id: string) => void
  onClose: () => void
}) {
  const failed = notification.receipts.some((r) => r.status === '失败')
  const samples = notification.sample_ids
    .map((seq) => result.samples.find((s) => s.seq === seq))
    .filter((s): s is SamplePoint => Boolean(s))
  return (
    <div className="panel-card notif-drawer">
      <div className="panel-head">
        <h3>
          通知 {notification.id} · {notification.kind} · {notification.disposition}
          <span className="badge" style={{ background: KIND_COLORS[notification.kind] ?? '#1f6feb' }}>
            {notification.severity}
          </span>
        </h3>
        <button className="btn ghost" type="button" onClick={onClose}>收起</button>
      </div>

      {failed && (
        <p className="banner danger">
          传输失败仅记录在回执层：未创建第二个告警事件，自动重试仍归属本通知。
        </p>
      )}
      {notification.disposition === '静默合并' && (
        <p className="banner muted">{notification.merge_note}</p>
      )}
      {notification.silence_broken && (
        <p className="banner warn">严重级别提升打断静默窗口，本通知直接发送。</p>
      )}
      <p className="banner info">参数快照保留发送当时的规则版本，历史通知可重演当时决定。</p>

      <div className="drawer-grid">
        <section>
          <h4>参与计算的采样点</h4>
          <table className="data-table">
            <thead>
              <tr><th>序号</th><th>时刻</th><th>指标</th><th>数值</th><th>判定</th></tr>
            </thead>
            <tbody>
              {samples.map((s) => (
                <tr key={s.seq}>
                  <td>#{s.seq}</td>
                  <td>{s.ts}s</td>
                  <td>{s.metric}</td>
                  <td>{s.value}</td>
                  <td>{s.late ? '迟到补入' : s.breach === null ? '未判定' : s.breach ? '越界' : '正常'}</td>
                </tr>
              ))}
              {samples.length === 0 && (
                <tr><td className="empty-state" colSpan={5}>无关联采样点</td></tr>
              )}
            </tbody>
          </table>
          <p className="drawer-note">
            关联事件：{notification.event_ids.join('、')} · 渠道 {notification.channel} · 发送于 #{notification.seq}（{notification.ts}s）
          </p>
          {notification.merged_children.length > 0 && (
            <p className="drawer-note">
              本通知为静默窗口摘要，合并了：
              {notification.merged_children.map((id) => (
                <button key={id} className="link" type="button" onClick={() => onSelect(id)}>{id}</button>
              ))}
            </p>
          )}
          {notification.merged_into && (
            <p className="drawer-note">
              并入摘要通知：
              <button className="link" type="button" onClick={() => onSelect(notification.merged_into as string)}>
                {notification.merged_into}
              </button>
            </p>
          )}
        </section>

        <section>
          <h4>参数快照（发送当时）</h4>
          <table className="data-table">
            <tbody>
              {Object.entries(notification.param_snapshot).map(([key, value]) => (
                <tr key={key}>
                  <td>{SNAPSHOT_LABELS[key] ?? key}</td>
                  <td>{value}</td>
                </tr>
              ))}
            </tbody>
          </table>
          <h4>通知回执</h4>
          <table className="data-table">
            <thead>
              <tr><th>次数</th><th>序号</th><th>时刻</th><th>状态</th><th>说明</th></tr>
            </thead>
            <tbody>
              {notification.receipts.map((r) => (
                <tr key={r.attempt}>
                  <td>第 {r.attempt} 次</td>
                  <td>#{r.seq}</td>
                  <td>{r.ts}s</td>
                  <td className={r.status === '失败' ? 'error-text' : ''}>{r.status}</td>
                  <td>{r.detail}</td>
                </tr>
              ))}
              {notification.receipts.length === 0 && (
                <tr><td className="empty-state" colSpan={5}>未单独发送，无回执</td></tr>
              )}
            </tbody>
          </table>
        </section>
      </div>
    </div>
  )
}
