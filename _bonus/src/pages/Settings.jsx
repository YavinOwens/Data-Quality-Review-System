import { useState } from 'react'
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
} from '@mui/material'

export default function Settings() {
  const [settings, setSettings] = useState({
    apiEndpoint: 'http://localhost:11434',
    enableMetrics: true,
    enableLogging: true,
    maxConcurrentRequests: '5',
  })
  const [saved, setSaved] = useState(false)

  const handleChange = (field) => (event) => {
    const value = event.target.type === 'checkbox' ? event.target.checked : event.target.value
    setSettings((prev) => ({
      ...prev,
      [field]: value,
    }))
    setSaved(false)
  }

  const handleSave = () => {
    // TODO: Implement actual settings save
    console.log('Saving settings:', settings)
    setSaved(true)
    setTimeout(() => setSaved(false), 3000)
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
          API Configuration
        </Typography>
        <Box sx={{ maxWidth: 400 }}>
          <TextField
            fullWidth
            label="Ollama API Endpoint"
            value={settings.apiEndpoint}
            onChange={handleChange('apiEndpoint')}
            margin="normal"
          />
        </Box>

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
    </Box>
  )
} 