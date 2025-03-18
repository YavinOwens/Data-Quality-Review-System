import { Box, Typography, Paper, Grid, Button } from '@mui/material'
import AnalyticsIcon from '@mui/icons-material/Analytics'
import SettingsIcon from '@mui/icons-material/Settings'
import { useNavigate } from 'react-router-dom'

export default function Home() {
  const navigate = useNavigate()

  return (
    <Box>
      <Typography variant="h4" gutterBottom>
        Welcome to Ollama Analyzer
      </Typography>
      <Typography variant="body1" color="text.secondary" paragraph>
        Analyze and optimize your Ollama models with ease. Get started by selecting one of the options below.
      </Typography>

      <Grid container spacing={3} sx={{ mt: 2 }}>
        <Grid item xs={12} md={6}>
          <Paper
            sx={{
              p: 3,
              height: '100%',
              display: 'flex',
              flexDirection: 'column',
              alignItems: 'flex-start',
              gap: 2,
            }}
          >
            <AnalyticsIcon color="primary" sx={{ fontSize: 40 }} />
            <Typography variant="h6">Model Analysis</Typography>
            <Typography variant="body2" color="text.secondary">
              Analyze your Ollama models' performance, memory usage, and response times.
            </Typography>
            <Button
              variant="contained"
              onClick={() => navigate('/data-quality')}
              sx={{ mt: 'auto' }}
            >
              Start Analysis
            </Button>
          </Paper>
        </Grid>

        <Grid item xs={12} md={6}>
          <Paper
            sx={{
              p: 3,
              height: '100%',
              display: 'flex',
              flexDirection: 'column',
              alignItems: 'flex-start',
              gap: 2,
            }}
          >
            <SettingsIcon color="primary" sx={{ fontSize: 40 }} />
            <Typography variant="h6">Settings</Typography>
            <Typography variant="body2" color="text.secondary">
              Configure your Ollama instance, API endpoints, and analysis preferences.
            </Typography>
            <Button
              variant="outlined"
              onClick={() => navigate('/settings')}
              sx={{ mt: 'auto' }}
            >
              Configure
            </Button>
          </Paper>
        </Grid>
      </Grid>
    </Box>
  )
} 