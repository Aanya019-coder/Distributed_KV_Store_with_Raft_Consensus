import { useState } from 'react'

interface LoginProps {
  onLogin: (token: string) => void
  onClearToken: () => void
  currentToken?: string
}

export function Login({ onLogin, onClearToken, currentToken }: LoginProps) {
  const [tokenInput, setTokenInput] = useState(currentToken || '')

  function handleSubmit(e: React.FormEvent) {
    e.preventDefault()
    onLogin(tokenInput.trim())
  }

  return (
    <div className="login-modal">
      <div className="login-card">
        <h3>Gateway Authentication Token</h3>
        <p className="login-desc">
          Enter the Gateway Bearer Token to authorize KV writes and Admin actions. The gateway forwards internal cluster credentials server-side.
        </p>

        {currentToken ? (
          <div className="token-active-box">
            <span className="token-status">✓ Authenticated with Gateway Token</span>
            <button className="btn-secondary btn-sm" onClick={onClearToken}>
              Clear Token / Logout
            </button>
          </div>
        ) : (
          <form onSubmit={handleSubmit}>
            <div className="form-group">
              <label htmlFor="token-input">Bearer Token</label>
              <input
                id="token-input"
                type="password"
                value={tokenInput}
                onChange={e => setTokenInput(e.target.value)}
                placeholder="mysecrettoken"
              />
            </div>
            <button type="submit" className="btn-primary full-width">
              Save Authentication Token
            </button>
          </form>
        )}
      </div>
    </div>
  )
}
