import { useState, useEffect } from 'react'
import { useSSE } from './api/useSSE'
import { TopologyView } from './components/TopologyView'
import { KVConsole } from './components/KVConsole'
import { MetricsDashboard } from './components/MetricsDashboard'
import { EventLog } from './components/EventLog'
import { AdminPanel } from './components/AdminPanel'
import { Login } from './components/Login'

type Tab = 'topology' | 'kv' | 'metrics' | 'events' | 'admin'

export function App() {
  const { overview, events, connected } = useSSE()
  const [activeTab, setActiveTab] = useState<Tab>('topology')
  const [token, setToken] = useState<string>(() => sessionStorage.getItem('gw_token') || '')
  const [showAuthModal, setShowAuthModal] = useState(false)

  useEffect(() => {
    if (token) {
      sessionStorage.setItem('gw_token', token)
    } else {
      sessionStorage.removeItem('gw_token')
    }
  }, [token])

  return (
    <div className="app-container">
      <header className="app-header">
        <div className="brand">
          <div className="logo-icon">⚡</div>
          <div className="brand-text">
            <h1>Raft Consensus Gateway</h1>
            <span className="subtitle">Distributed Key-Value Engine & Cluster Observer</span>
          </div>
        </div>

        <div className="header-actions">
          <button
            className={`auth-btn ${token ? 'authenticated' : ''}`}
            onClick={() => setShowAuthModal(!showAuthModal)}
          >
            {token ? '🔒 Token Set' : '🔑 Auth Token'}
          </button>
        </div>
      </header>

      <nav className="nav-tabs">
        <button
          className={`tab-item ${activeTab === 'topology' ? 'active' : ''}`}
          onClick={() => setActiveTab('topology')}
        >
          🕸 Cluster Topology
        </button>
        <button
          className={`tab-item ${activeTab === 'kv' ? 'active' : ''}`}
          onClick={() => setActiveTab('kv')}
        >
          💾 KV Operations
        </button>
        <button
          className={`tab-item ${activeTab === 'metrics' ? 'active' : ''}`}
          onClick={() => setActiveTab('metrics')}
        >
          📊 Prometheus Metrics
        </button>
        <button
          className={`tab-item ${activeTab === 'events' ? 'active' : ''}`}
          onClick={() => setActiveTab('events')}
        >
          📡 SSE Event Log
        </button>
        <button
          className={`tab-item ${activeTab === 'admin' ? 'active' : ''}`}
          onClick={() => setActiveTab('admin')}
        >
          ⚙ Membership Admin
        </button>
      </nav>

      {showAuthModal && (
        <Login
          currentToken={token}
          onLogin={t => {
            setToken(t)
            setShowAuthModal(false)
          }}
          onClearToken={() => {
            setToken('')
            setShowAuthModal(false)
          }}
        />
      )}

      <main className="main-content">
        {activeTab === 'topology' && <TopologyView overview={overview} connected={connected} />}
        {activeTab === 'kv' && <KVConsole token={token} />}
        {activeTab === 'metrics' && <MetricsDashboard overview={overview} />}
        {activeTab === 'events' && <EventLog events={events} />}
        {activeTab === 'admin' && <AdminPanel token={token} />}
      </main>

      <footer className="app-footer">
        <span>Raft KV Store Full-Stack Architecture • Single Entry Gateway + Push SSE Protocol</span>
      </footer>
    </div>
  )
}

export default App
