import { useEffect, useState } from 'react'
import { getAggregateMetrics } from '../api/client'
import type { ClusterOverview } from '../api/client'
import {
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer
} from 'recharts'

interface MetricsPoint {
  time: string
  term: number
  commitIndex: number
  logEntries: number
}

interface MetricsDashboardProps {
  overview: ClusterOverview | null
}

function parsePrometheusValue(text: string, metricName: string): number {
  const lines = text.split('\n')
  for (const line of lines) {
    if (line.startsWith('#') || !line.trim()) continue
    if (line.startsWith(metricName + ' ') || line.startsWith(metricName + '{')) {
      const parts = line.split(' ')
      const val = parseFloat(parts[parts.length - 1])
      if (!isNaN(val)) return val
    }
  }
  return 0
}

export function MetricsDashboard({ overview }: MetricsDashboardProps) {
  const [history, setHistory] = useState<MetricsPoint[]>([])
  const [rawMetrics, setRawMetrics] = useState('')

  // Use SSE overview as the primary data source for chart
  useEffect(() => {
    if (!overview) return
    const leaderNode = overview.nodes.find(n => n.role === 'leader')
    if (!leaderNode) return

    setHistory(prev => {
      const point: MetricsPoint = {
        time: new Date().toLocaleTimeString(),
        term: leaderNode.current_term,
        commitIndex: leaderNode.commit_index,
        logEntries: leaderNode.log_length,
      }
      return [...prev, point].slice(-60) // keep last 60 points
    })
  }, [overview])

  // Fetch raw prometheus text separately
  useEffect(() => {
    const id = setInterval(async () => {
      try {
        const text = await getAggregateMetrics()
        setRawMetrics(text)
      } catch {}
    }, 5000)
    return () => clearInterval(id)
  }, [])

  const chartLines = [
    { key: 'term',        name: 'Current Term',   color: '#f59e0b' },
    { key: 'commitIndex', name: 'Commit Index',    color: '#10b981' },
    { key: 'logEntries',  name: 'Log Length',      color: '#4facfe' },
  ]

  return (
    <div className="metrics-dashboard">
      <h2>Metrics Dashboard</h2>
      <p className="section-desc">Live consensus state trends, fed by the gateway SSE stream.</p>

      <div className="chart-card">
        <h3>Consensus Progress</h3>
        <ResponsiveContainer width="100%" height={280}>
          <LineChart data={history}>
            <CartesianGrid strokeDasharray="3 3" stroke="#ffffff11" />
            <XAxis dataKey="time" tick={{ fill: '#9ca3af', fontSize: 11 }} />
            <YAxis tick={{ fill: '#9ca3af', fontSize: 11 }} />
            <Tooltip
              contentStyle={{ background: '#0b0f19', border: '1px solid #4facfe33', borderRadius: 8 }}
              labelStyle={{ color: '#f3f4f6' }}
            />
            <Legend wrapperStyle={{ color: '#9ca3af' }} />
            {chartLines.map(({ key, name, color }) => (
              <Line
                key={key}
                type="monotone"
                dataKey={key}
                name={name}
                stroke={color}
                strokeWidth={2}
                dot={false}
                isAnimationActive={false}
              />
            ))}
          </LineChart>
        </ResponsiveContainer>
      </div>

      {rawMetrics && (
        <div className="raw-metrics-card">
          <h3>Prometheus Export <span className="badge-tag">/metrics/aggregate</span></h3>
          <pre className="metrics-text">{rawMetrics}</pre>
        </div>
      )}
    </div>
  )
}
