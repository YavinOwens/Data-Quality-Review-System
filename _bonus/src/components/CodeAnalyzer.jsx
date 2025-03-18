import { useState, useEffect } from 'react';
import { Typography, Box, TextField, Button, CircularProgress } from '@mui/material';
import { CodeAnalysisPresenter } from '../presenters/CodeAnalysisPresenter';

export function CodeAnalyzer() {
  const [presenter] = useState(() => new CodeAnalysisPresenter());
  const [state, setState] = useState(presenter.getState());

  useEffect(() => {
    const updateState = () => {
      setState(presenter.getState());
    };

    // Subscribe to state changes
    presenter.onStateChange = updateState;
    return () => {
      presenter.onStateChange = null;
    };
  }, [presenter]);

  const handleCodeChange = (e) => {
    presenter.setCode(e.target.value);
  };

  const handleAnalyze = () => {
    presenter.analyzeCode();
  };

  return (
    <Box>
      <Typography variant="h4" component="h1" gutterBottom>
        Code Analysis Tool
      </Typography>
      
      <Box sx={{ mb: 3 }}>
        <TextField
          fullWidth
          multiline
          rows={10}
          value={state.code}
          onChange={handleCodeChange}
          placeholder="Enter your code here..."
          variant="outlined"
        />
      </Box>

      <Box sx={{ mb: 3 }}>
        <Button
          variant="contained"
          onClick={handleAnalyze}
          disabled={state.loading}
        >
          {state.loading ? 'Analyzing...' : 'Analyze Code'}
        </Button>
      </Box>

      {state.error && (
        <Typography color="error" sx={{ mb: 2 }}>
          Error: {state.error}
        </Typography>
      )}

      {state.loading && (
        <Box display="flex" justifyContent="center" my={4}>
          <CircularProgress />
        </Box>
      )}

      {state.result && (
        <Box sx={{ mt: 3 }}>
          <Typography variant="h6" gutterBottom>
            Analysis Result:
          </Typography>
          <Typography component="pre" sx={{ 
            whiteSpace: 'pre-wrap',
            backgroundColor: '#f5f5f5',
            p: 2,
            borderRadius: 1
          }}>
            {state.result}
          </Typography>
        </Box>
      )}
    </Box>
  );
} 