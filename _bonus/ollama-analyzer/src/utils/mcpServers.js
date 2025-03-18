import axios from 'axios'

// Default MCP server configuration
const defaultMcpConfig = {
  servers: [
    {
      name: 'Local MCP',
      url: 'http://localhost:11434',
      type: 'ollama',
      active: true,
    },
    {
      name: 'Development MCP',
      url: 'http://localhost:11435',
      type: 'ollama',
      active: false,
    }
  ]
}

// Load MCP configuration from localStorage or use defaults
export const loadMcpConfig = () => {
  const storedConfig = localStorage.getItem('mcpConfig')
  return storedConfig ? JSON.parse(storedConfig) : defaultMcpConfig
}

// Save MCP configuration to localStorage
export const saveMcpConfig = (config) => {
  localStorage.setItem('mcpConfig', JSON.stringify(config))
}

// Test MCP server connection
export const testMcpServer = async (serverUrl) => {
  try {
    const response = await axios.get(`${serverUrl}/api/health`)
    return response.status === 200
  } catch (error) {
    console.error('MCP server test failed:', error)
    return false
  }
}

// Get active MCP server
export const getActiveMcpServer = () => {
  const config = loadMcpConfig()
  return config.servers.find(server => server.active)
}

// Set active MCP server
export const setActiveMcpServer = (serverName) => {
  const config = loadMcpConfig()
  config.servers = config.servers.map(server => ({
    ...server,
    active: server.name === serverName
  }))
  saveMcpConfig(config)
  return config
}

// Add new MCP server
export const addMcpServer = (server) => {
  const config = loadMcpConfig()
  config.servers.push({
    ...server,
    active: false
  })
  saveMcpConfig(config)
  return config
}

// Remove MCP server
export const removeMcpServer = (serverName) => {
  const config = loadMcpConfig()
  config.servers = config.servers.filter(server => server.name !== serverName)
  saveMcpConfig(config)
  return config
}

// Update MCP server
export const updateMcpServer = (serverName, updates) => {
  const config = loadMcpConfig()
  config.servers = config.servers.map(server => 
    server.name === serverName ? { ...server, ...updates } : server
  )
  saveMcpConfig(config)
  return config
} 