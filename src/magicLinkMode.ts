// Magic-link mode: when active, the player came in via a /play/{code} URL,
// the SPA never holds Microsoft/Xbox/MC credentials, and all session state
// lives on the proxy keyed by ProxySessionId. State here is in-memory only
// — never localStorage — so it dies with the tab. The deploy-specific
// backend host that issues the codes is configured via AppConfig
// (magicLinkBackend); see appConfig.ts and index.ts.

export interface MagicLinkServer {
  Name: string
  Host: string
  Port: number
  ProxyURL: string
}

export interface MagicLinkState {
  code: string
  username: string  // gamertag from mc-frontend
  servers: MagicLinkServer[]
  expiresAt: string // ISO timestamp
  proxySessionId?: string // populated after /api/vm/net/auth completes
}

let state: MagicLinkState | null = null

export const magicLinkMode = {
  set (next: MagicLinkState) {
    state = next
  },

  isActive (): boolean {
    return state !== null
  },

  get (): MagicLinkState | null {
    return state
  },

  // Called by microsoftAuthflow.ts after the proxy's /auth response includes
  // proxySessionId — needed by /session and /connect later in the flow.
  setProxySessionId (id: string) {
    if (state) state.proxySessionId = id
  },

  // The token we feed mineflayer in magic-link mode. mineflayer/yggdrasil
  // sends this as accessToken on session.join — the proxy recognizes the
  // "magic:" sentinel prefix and uses the suffix as the ProxySessionId
  // lookup key into its MagicSessionStore.
  magicAccessToken (): string | undefined {
    if (!state?.proxySessionId) return undefined
    return 'magic:' + state.proxySessionId
  },

  clear () {
    state = null
  },
}
