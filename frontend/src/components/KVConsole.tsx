import { useState, useRef } from 'react'
import { kvGet, kvPut, kvDelete } from '../api/client'

interface LogEntry {
  id: number
  ts: string
  op: string
  key: string
  value?: string
  status: 'ok' | 'error'
  response: string
  durationMs: number
}

interface KVConsoleProps {
  token?: string
}

let _logId = 0

export function KVConsole({ token }: KVConsoleProps) {
  const [key, setKey]     = useState('demo-key')
  const [value, setValue] = useState('Hello Raft Consensus!')
  const [loading, setLoading] = useState(false)
  const [log, setLog]     = useState<LogEntry[]>([])
  const logRef            = useRef<HTMLDivElement>(null)

  function appendLog(entry: Omit<LogEntry, 'id' | 'ts'>) {
    const full: LogEntry = {
      ...entry,
      id: ++_logId,
      ts: new Date().toLocaleTimeString(),
    }
    setLog(prev => [full, ...prev].slice(0, 50))
  }

  async function exec(op: 'GET' | 'PUT' | 'DELETE') {
    if (!key.trim()) return
    setLoading(true)
    const t0 = performance.now()
    try {
      let response = ''
      if (op === 'GET') {
        const r = await kvGet(key.trim(), token)
        response = JSON.stringify(r, null, 2)
      } else if (op === 'PUT') {
        const r = await kvPut(key.trim(), value, token)
        response = JSON.stringify(r, null, 2)
      } else {
        const r = await kvDelete(key.trim(), token)
        response = JSON.stringify(r, null, 2)
      }
      appendLog({ op, key: key.trim(), value, status: 'ok', response, durationMs: performance.now() - t0 })
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : String(e)
      appendLog({ op, key: key.trim(), value, status: 'error', response: msg, durationMs: performance.now() - t0 })
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="kv-console">
      <h2>KV Console</h2>
      <p className="section-desc">
        PUT/GET/DELETE keys — routed through the gateway to the current Raft leader automatically.
      </p>

      <div className="form-row">
        <div className="form-group">
          <label htmlFor="kv-key">Key</label>
          <input
            id="kv-key"
            value={key}
            onChange={e => setKey(e.target.value)}
            placeholder="my-key"
            disabled={loading}
          />
        </div>
        <div className="form-group flex-grow">
          <label htmlFor="kv-value">Value (for PUT)</label>
          <input
            id="kv-value"
            value={value}
            onChange={e => setValue(e.target.value)}
            placeholder="my-value"
            disabled={loading}
          />
        </div>
      </div>

      <div className="btn-row">
        <button
          id="btn-put"
          className="btn-primary"
          onClick={() => exec('PUT')}
          disabled={loading || !key.trim()}
        >
          {loading ? '…' : 'PUT'}
        </button>
        <button
          id="btn-get"
          className="btn-secondary"
          onClick={() => exec('GET')}
          disabled={loading || !key.trim()}
        >
          GET
        </button>
        <button
          id="btn-delete"
          className="btn-danger"
          onClick={() => exec('DELETE')}
          disabled={loading || !key.trim()}
        >
          DELETE
        </button>
      </div>

      <div className="request-log" ref={logRef}>
        {log.length === 0 ? (
          <span className="log-placeholder">No requests yet. Try a PUT or GET above.</span>
        ) : (
          log.map(entry => (
            <div key={entry.id} className={`log-entry log-${entry.status}`}>
              <span className="log-ts">{entry.ts}</span>
              <span className="log-op">{entry.op}</span>
              <span className="log-key">/{entry.key}</span>
              <span className={`log-status ${entry.status === 'ok' ? 'log-ok' : 'log-error'}`}>
                {entry.status === 'ok' ? '✓' : '✗'}
              </span>
              <span className="log-dur">{entry.durationMs.toFixed(0)}ms</span>
              <pre className="log-response">{entry.response}</pre>
            </div>
          ))
        )}
      </div>
    </div>
  )
}
