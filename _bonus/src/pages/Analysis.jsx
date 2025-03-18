import { useState } from 'react'
import {
  Box,
  Typography,
  Paper,
  TextField,
  Button,
  CircularProgress,
  Alert,
} from '@mui/material'

export default function Analysis() {
  const [modelName, setModelName] = useState('')
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)

  const handleAnalyze = async () => {
    if (!modelName.trim()) {
      setError('Please enter a model name')
      return
    }

    setLoading(true)
    setError(null)

    try {
      // TODO: Implement actual model analysis
      await new Promise(resolve => setTimeout(resolve, 2000))
      
      // Placeholder for analysis logic
      console.log('Analyzing model:', modelName)
      
    } catch (err) {
      setError(err.message || 'Failed to analyze model')
    } finally {
      setLoading(false)
    }
  }

  return (
    <Box>
      <Typography variant="h4" gutterBottom>
        Model Analysis
      </Typography>
      <Typography variant="body1" color="text.secondary" paragraph>
        Enter your Ollama model name to begin the analysis.
      </Typography>

      <Paper sx={{ p: 3, mt: 3 }}>
        <Box sx={{ maxWidth: 400 }}>
          <TextField
            fullWidth
            label="Model Name"
            value={modelName}
            onChange={(e) => setModelName(e.target.value)}
            placeholder="e.g. llama2"
            disabled={loading}
          />
          
          {error && (
            <Alert severity="error" sx={{ mt: 2 }}>
              {error}
            </Alert>
          )}

          <Button
            variant="contained"
            onClick={handleAnalyze}
            disabled={loading}
            sx={{ mt: 3 }}
          >
            {loading ? (
              <>
                <CircularProgress size={20} sx={{ mr: 1 }} />
                Analyzing...
              </>
            ) : (
              'Start Analysis'
            )}
          </Button>
        </Box>
      </Paper>

      {/* Placeholder for analysis results */}
      <Paper sx={{ p: 3, mt: 3, display: loading ? 'none' : 'block' }}>
        <Typography variant="h6" gutterBottom>
          Analysis Results
        </Typography>
        <Typography variant="body2" color="text.secondary">
          Analysis results will appear here once the process is complete.
        </Typography>
      </Paper>
    </Box>
  )
} 