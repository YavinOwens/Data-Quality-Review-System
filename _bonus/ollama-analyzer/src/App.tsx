import { useState, useEffect } from 'react';
import { Box, Container, CssBaseline, ThemeProvider, createTheme } from '@mui/material';
import { Drawer, List, ListItem, ListItemIcon, ListItemText, AppBar, Toolbar, Typography } from '@mui/material';
import { Schema } from '@mui/icons-material';
import FileUpload from './components/FileUpload';
import SchemaViewer from './components/SchemaViewer';
import { getSchemaDefinitions } from './api/dataQualityApi';

const drawerWidth = 240;

function App() {
  const [schemaDefinitions, setSchemaDefinitions] = useState<Record<string, any> | null>(null);

  const theme = createTheme({
    palette: {
      mode: 'light',
      primary: {
        main: '#1976d2',
      },
    },
  });

  useEffect(() => {
    const loadSchemas = async () => {
      try {
        const schemas = await getSchemaDefinitions();
        setSchemaDefinitions(schemas);
      } catch (error) {
        console.error('Error loading schema definitions:', error);
      }
    };
    loadSchemas();
  }, []);

  return (
    <ThemeProvider theme={theme}>
      <CssBaseline />
      <Box sx={{ display: 'flex' }}>
        <AppBar position="fixed" sx={{ zIndex: (theme) => theme.zIndex.drawer + 1 }}>
          <Toolbar>
            <Typography variant="h6" noWrap component="div">
              Data Quality Analyzer
            </Typography>
          </Toolbar>
        </AppBar>
        <Box
          component="main"
          sx={{
            flexGrow: 1,
            p: 3,
            marginTop: '64px',
          }}
        >
          <Container maxWidth="xl">
            <SchemaViewer schemas={schemaDefinitions} />
          </Container>
        </Box>
      </Box>
    </ThemeProvider>
  );
}

export default App; 