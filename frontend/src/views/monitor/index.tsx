import { useCallback, useEffect, useMemo, useState } from 'react'

import { request } from '@/api/client'

import CausalityPanel from './causality'

const ENDPOINT = '/api/monitor'
const columns = [
  "指标编号",
  "关联任务",
  "指标名称",
  "预警阈值",
  "告警级别",
  "统计粒度",
  "通知渠道",
  "静默窗口"
]
const actions = [
  "提交校验",
  "启用监控",
  "停用监控"
]
const statuses = [
  "草稿",
  "待校验",
  "已启用",
  "已停用"
]
const stats = [
  {
    "label": "监控指标总数",
    "value": 0
  },
  {
    "label": "已启用指标",
    "value": 0
  },
  {
    "label": "待校验指标",
    "value": 0
  }
]

type Row = Record<string, string | number | null>

export default function MonitorView() {
  const [tab, setTab] = useState<'entries' | 'causality'>('entries')
  const [rows, setRows] = useState<Row[]>([])
  const [total, setTotal] = useState(0)
  const [errorMessage, setErrorMessage] = useState('')
  const [filters, setFilters] = useState<Record<string, string>>({})
  const filterFields = useMemo(() => columns.slice(0, 3), [])

  const reload = useCallback(async () => {
    setErrorMessage('')
    const query = new URLSearchParams(filters).toString()
    try {
      const response = await request(`${ENDPOINT}?${query}`)
      if (!response.ok) {
        throw new Error('监控指标列表读取失败')
      }
      const payload = await response.json()
      setRows(payload.items ?? [])
      setTotal(payload.total ?? 0)
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '采集监控列表读取失败')
    }
  }, [filters])

  useEffect(() => {
    void reload()
  }, [reload])

  async function runAction(action: string, row: Row) {
    setErrorMessage('')
    try {
      const response = await request(`${ENDPOINT}/${row.id}/actions`, {
        method: 'POST',
        body: JSON.stringify({ action }),
      })
      if (!response.ok) {
        throw new Error('采集监控动作未生效，请稍后重试')
      }
      await reload()
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '采集监控操作失败')
    }
  }

  return (
    <section className="page" data-module="monitor">
      <header className="page-head">
        <div>
          <h2>采集监控管理</h2>
          <p className="page-desc">维护采集监控指标的输入、执行约束与结果记录，为采集监控提供可验证的数据工程流程。</p>
        </div>
        <div className="page-actions">
          <button className="btn primary" type="button" onClick={() => setErrorMessage('监控指标登记入口尚未接入审批流')}>
            登记监控指标
          </button>
          <button className="btn" type="button" onClick={() => window.open(`${ENDPOINT}/export`, '_blank')}>
            导出采集监控清单
          </button>
        </div>
      </header>

      <div className="tab-bar">
        <button
          className={`tab-btn${tab === 'entries' ? ' active' : ''}`}
          type="button"
          onClick={() => setTab('entries')}
        >
          指标管理
        </button>
        <button
          className={`tab-btn${tab === 'causality' ? ' active' : ''}`}
          type="button"
          onClick={() => setTab('causality')}
        >
          通知因果视图
        </button>
      </div>

      {tab === 'causality' ? (
        <CausalityPanel />
      ) : (
        <>
      <div className="stat-row">
        {stats.map((item) => (
          <article key={item.label} className="stat-card">
            <span className="stat-label">{item.label}</span>
            <strong className="stat-value">{item.value}</strong>
          </article>
        ))}
      </div>

      <form className="filter-bar" onSubmit={(event) => { event.preventDefault(); void reload() }}>
        {filterFields.map((field) => (
          <label key={field} className="filter-item">
            <span>{field}</span>
            <input
              value={filters[field] ?? ''}
              placeholder={`按${field}检索`}
              onChange={(event) => setFilters((prev) => ({ ...prev, [field]: event.target.value }))}
            />
          </label>
        ))}
        <button className="btn" type="submit">查询</button>
        <button className="btn ghost" type="button" onClick={() => setFilters({})}>重置条件</button>
      </form>

      <table className="data-table">
        <thead>
          <tr>
            {columns.map((column) => <th key={column}>{column}</th>)}
            <th>可执行动作</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={String(row.id)}>
              {columns.map((column) => <td key={column}>{row[column] ?? '—'}</td>)}
              <td className="row-actions">
                {actions.map((action) => (
                  <button key={action} className="link" type="button" onClick={() => void runAction(action, row)}>
                    {action}
                  </button>
                ))}
              </td>
            </tr>
          ))}
          {rows.length === 0 && (
            <tr>
              <td className="empty-state" colSpan={columns.length + 1}>暂无采集监控数据，可先登记监控指标</td>
            </tr>
          )}
        </tbody>
      </table>

      <footer className="page-foot">
        <span>共 {total} 条采集监控记录</span>
        {errorMessage && <span className="error-text">{errorMessage}</span>}
      </footer>
        </>
      )}
    </section>
  )
}
