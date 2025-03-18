import { useState, useEffect } from 'react'
import {
  Box,
  Typography,
  Paper,
  TextField,
  Button,
  Switch,
  FormControlLabel,
  Divider,
  Alert,
  List,
  ListItem,
  ListItemText,
  ListItemSecondaryAction,
  IconButton,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  CircularProgress,
} from '@mui/material'
import AddIcon from '@mui/icons-material/Add'
import DeleteIcon from '@mui/icons-material/Delete'
import EditIcon from '@mui/icons-material/Edit'
import CheckCircleIcon from '@mui/icons-material/CheckCircle'
import ErrorIcon from '@mui/icons-material/Error'
import {
  loadMcpConfig,
  saveMcpConfig,
  testMcpServer,
  addMcpServer,
  removeMcpServer,
  updateMcpServer,
  setActiveMcpServer,
} from '../utils/mcpServers'

export default function Settings() {
  const [settings, setSettings] = useState({
    enableMetrics: true,
    enableLogging: true,
    maxConcurrentRequests: '5',
  })
  const [mcpServers, setMcpServers] = useState([])
  const [saved, setSaved] = useState(false)
  const [dialogOpen, setDialogOpen] = useState(false)
  const [editServer, setEditServer] = useState(null)
  const [newServer, setNewServer] = useState({
    name: '',
    url: '',
    type: 'ollama',
  })
  const [serverStatuses, setServerStatuses] = useState({})
  const [testingServer, setTestingServer] = useState('')

  useEffect(() => {
    const config = loadMcpConfig()
    setMcpServers(config.servers)
    // Test all servers on load
    config.servers.forEach(server => {
      testServer(server.url)
    })
  }, [])

  const handleChange = (field) => (event) => {
    const value = event.target.type === 'checkbox' ? event.target.checked : event.target.value
    setSettings((prev) => ({
      ...prev,
      [field]: value,
    }))
    setSaved(false)
  }

  const handleSave = () => {
    // Save general settings
    console.log('Saving settings:', settings)
    setSaved(true)
    setTimeout(() => setSaved(false), 3000)
  }

  const handleServerDialogOpen = (server = null) => {
    if (server) {
      setEditServer(server)
      setNewServer(server)
    } else {
      setEditServer(null)
      setNewServer({
        name: '',
        url: '',
        type: 'ollama',
      })
    }
    setDialogOpen(true)
  }

  const handleServerDialogClose = () => {
    setDialogOpen(false)
    setEditServer(null)
    setNewServer({
      name: '',
      url: '',
      type: 'ollama',
    })
  }

  const testServer = async (url) => {
    setTestingServer(url)
    setServerStatuses(prev => ({ ...prev, [url]: 'testing' }))
    const isConnected = await testMcpServer(url)
    setServerStatuses(prev => ({ ...prev, [url]: isConnected ? 'connected' : 'error' }))
    setTestingServer('')
  }

  const handleServerSave = () => {
    if (editServer) {
      const updatedServers = updateMcpServer(editServer.name, newServer).servers
      setMcpServers(updatedServers)
    } else {
      const updatedServers = addMcpServer(newServer).servers
      setMcpServers(updatedServers)
    }
    handleServerDialogClose()
    // Test the new/updated server
    testServer(newServer.url)
  }

  const handleServerDelete = (serverName) => {
    const updatedServers = removeMcpServer(serverName).servers
    setMcpServers(updatedServers)
  }

  const handleSetActive = (serverName) => {
    const updatedServers = setActiveMcpServer(serverName).servers
    setMcpServers(updatedServers)
  }

  return (
    <Box>
      <Typography variant="h4" gutterBottom>
        Settings
      </Typography>
      <Typography variant="body1" color="text.secondary" paragraph>
        Configure your Ollama Analyzer settings and preferences.
      </Typography>

      <Paper sx={{ p: 3, mt: 3 }}>
        <Typography variant="h6" gutterBottom>
          MCP Servers
        </Typography>
        <List>
          {mcpServers.map((server) => (
            <ListItem
              key={server.name}
              sx={{
                bgcolor: server.active ? 'action.selected' : 'transparent',
                borderRadius: 1,
                mb: 1,
              }}
            >
              <ListItemText
                primary={server.name}
                secondary={server.url}
                sx={{
                  '& .MuiListItemText-primary': {
                    fontWeight: server.active ? 600 : 400,
                  },
                }}
              />
              <ListItemSecondaryAction sx={{ display: 'flex', gap: 1, alignItems: 'center' }}>
                {serverStatuses[server.url] === 'testing' ? (
                  <CircularProgress size={20} />
                ) : serverStatuses[server.url] === 'connected' ? (
                  <CheckCircleIcon color="success" />
                ) : serverStatuses[server.url] === 'error' ? (
                  <ErrorIcon color="error" />
                ) : null}
                <Button
                  size="small"
                  variant={server.active ? 'contained' : 'outlined'}
                  onClick={() => handleSetActive(server.name)}
                  sx={{ mr: 1 }}
                >
                  {server.active ? 'Active' : 'Set Active'}
                </Button>
                <IconButton
                  edge="end"
                  onClick={() => handleServerDialogOpen(server)}
                  sx={{ mr: 1 }}
                >
                  <EditIcon />
                </IconButton>
                <IconButton
                  edge="end"
                  onClick={() => handleServerDelete(server.name)}
                  disabled={server.active}
                >
                  <DeleteIcon />
                </IconButton>
              </ListItemSecondaryAction>
            </ListItem>
          ))}
        </List>
        <Button
          startIcon={<AddIcon />}
          variant="outlined"
          onClick={() => handleServerDialogOpen()}
          sx={{ mt: 2 }}
        >
          Add Server
        </Button>

        <Divider sx={{ my: 4 }} />

        <Typography variant="h6" gutterBottom>
          Analysis Settings
        </Typography>
        <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
          <FormControlLabel
            control={
              <Switch
                checked={settings.enableMetrics}
                onChange={handleChange('enableMetrics')}
                color="primary"
              />
            }
            label="Enable Performance Metrics"
          />
          <FormControlLabel
            control={
              <Switch
                checked={settings.enableLogging}
                onChange={handleChange('enableLogging')}
                color="primary"
              />
            }
            label="Enable Detailed Logging"
          />
          <Box sx={{ maxWidth: 200 }}>
            <TextField
              fullWidth
              type="number"
              label="Max Concurrent Requests"
              value={settings.maxConcurrentRequests}
              onChange={handleChange('maxConcurrentRequests')}
              inputProps={{ min: 1, max: 10 }}
            />
          </Box>
        </Box>

        {saved && (
          <Alert severity="success" sx={{ mt: 3 }}>
            Settings saved successfully!
          </Alert>
        )}

        <Button
          variant="contained"
          onClick={handleSave}
          sx={{ mt: 4 }}
        >
          Save Settings
        </Button>
      </Paper>

      <Dialog open={dialogOpen} onClose={handleServerDialogClose}>
        <DialogTitle>{editServer ? 'Edit Server' : 'Add Server'}</DialogTitle>
        <DialogContent>
          <Box sx={{ pt: 2, display: 'flex', flexDirection: 'column', gap: 2 }}>
            <TextField
              fullWidth
              label="Server Name"
              value={newServer.name}
              onChange={(e) => setNewServer(prev => ({ ...prev, name: e.target.value }))}
            />
            <TextField
              fullWidth
              label="Server URL"
              value={newServer.url}
              onChange={(e) => setNewServer(prev => ({ ...prev, url: e.target.value }))}
            />
            <TextField
              fullWidth
              label="Server Type"
              value={newServer.type}
              onChange={(e) => setNewServer(prev => ({ ...prev, type: e.target.value }))}
            />
          </Box>
        </DialogContent>
        <DialogActions>
          <Button onClick={handleServerDialogClose}>Cancel</Button>
          <Button
            onClick={handleServerSave}
            variant="contained"
            disabled={!newServer.name || !newServer.url}
          >
            {editServer ? 'Update' : 'Add'}
          </Button>
        </DialogActions>
      </Dialog>
    </Box>
  )
} 