import { useState } from 'react'
import { adminAddNode, adminRemoveNode } from '../api/client'

interface AdminPanelProps {
  token?: string
}

export function AdminPanel({ token }: AdminPanelProps) {
  const [addId, setAddId] = useState('node4')
  const [grpcAddr, setGrpcAddr] = useState('127.0.0.1:9004')
  const [httpAddr, setHttpAddr] = useState('127.0.0.1:8004')
  const [removeId, setRemoveId] = useState('')
  const [msg, setMsg] = useState<{ type: 'ok' | 'err'; text: string } | null>(null)
  const [loading, setLoading] = useState(false)

  async function handleAdd(e: React.FormEvent) {
    e.preventDefault()
    setLoading(true)
    setMsg(null)
    try {
      const res = await adminAddNode(addId, grpcAddr, httpAddr, token)
      setMsg({ type: 'ok', text: `Node ${addId} addition initiated: ${JSON.stringify(res)}` })
    } catch (err: unknown) {
      setMsg({ type: 'err', text: err instanceof Error ? err.message : String(err) })
    } finally {
      setLoading(false)
    }
  }

  async function handleRemove(e: React.FormEvent) {
    e.preventDefault()
    setLoading(true)
    setMsg(null)
    try {
      const res = await adminRemoveNode(removeId, token)
      setMsg({ type: 'ok', text: `Node ${removeId} removal initiated: ${JSON.stringify(res)}` })
    } catch (err: unknown) {
      setMsg({ type: 'err', text: err instanceof Error ? err.message : String(err) })
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="admin-panel">
      <h2>Cluster Membership Admin</h2>
      <p className="section-desc">
        Trigger §6 Raft Joint Consensus membership modifications via the Gateway.
      </p>

      {msg && (
        <div className={`admin-alert alert-${msg.type}`}>
          {msg.text}
        </div>
      )}

      <div className="admin-grid">
        <form className="admin-card" onSubmit={handleAdd}>
          <h3>Add Cluster Member</h3>
          <div className="form-group">
            <label htmlFor="add-id">Node ID</label>
            <input id="add-id" value={addId} onChange={e => setAddId(e.target.value)} required />
          </div>
          <div className="form-group">
            <label htmlFor="add-grpc">gRPC Target Address</label>
            <input id="add-grpc" value={grpcAddr} onChange={e => setGrpcAddr(e.target.value)} required />
          </div>
          <div className="form-group">
            <label htmlFor="add-http">HTTP API Address</label>
            <input id="add-http" value={httpAddr} onChange={e => setHttpAddr(e.target.value)} required />
          </div>
          <button type="submit" className="btn-primary" disabled={loading}>
            {loading ? 'Processing…' : 'Add Node to Cluster'}
          </button>
        </form>

        <form className="admin-card" onSubmit={handleRemove}>
          <h3>Remove Cluster Member</h3>
          <div className="form-group">
            <label htmlFor="rem-id">Node ID to Evict</label>
            <input id="rem-id" value={removeId} onChange={e => setRemoveId(e.target.value)} placeholder="e.g. node3" required />
          </div>
          <button type="submit" className="btn-danger" disabled={loading}>
            {loading ? 'Processing…' : 'Evict Node from Cluster'}
          </button>
        </form>
      </div>
    </div>
  )
}
