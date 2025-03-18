import { useState, useCallback, useEffect } from 'react'
import {
  Box,
  Typography,
  Paper,
  Button,
  Grid,
  List,
  ListItem,
  ListItemText,
  ListItemIcon,
  CircularProgress,
  Alert,
  Tabs,
  Tab,
  Divider,
  IconButton,
  Dialog,
  DialogTitle,
  DialogContent,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  ListItemButton,
} from '@mui/material'
import {
  Upload as UploadIcon,
  Assessment as AssessmentIcon,
  ShowChart as ShowChartIcon,
  Schema as SchemaIcon,
  Check as CheckIcon,
  Error as ErrorIcon,
  Close as CloseIcon,
  AccountTree as AccountTreeIcon,
  Folder as FolderIcon,
} from '@mui/icons-material'
import { validateFile, generateProfile, getErDiagram, getSchemaDefinitions } from '../api/dataQualityApi'

const SUPPORTED_FILES = ['workers.csv', 'assignments.csv', 'communications.csv', 'addresses.csv']

const LOCAL_FILES = [
  { name: 'workers.csv', path: '/Users/yavinowens/Documents/Python_offline/hr_core_validation/data_sources/workers.csv' },
  { name: 'assignments.csv', path: '/Users/yavinowens/Documents/Python_offline/hr_core_validation/data_sources/assignments.csv' },
  { name: 'communications.csv', path: '/Users/yavinowens/Documents/Python_offline/hr_core_validation/data_sources/communications.csv' },
  { name: 'addresses.csv', path: '/Users/yavinowens/Documents/Python_offline/hr_core_validation/data_sources/addresses.csv' },
]

export default function DataQuality() {
  const [selectedTab, setSelectedTab] = useState(0)
  const [files, setFiles] = useState({})
  const [validationResults, setValidationResults] = useState({})
  const [profileReports, setProfileReports] = useState({})
  const [loading, setLoading] = useState({})
  const [error, setError] = useState(null)
  const [erDiagramOpen, setErDiagramOpen] = useState(false)
  const [schemaDefinitions, setSchemaDefinitions] = useState({})
  const [erDiagramUrl, setErDiagramUrl] = useState(null)
  const [selectedFile, setSelectedFile] = useState(null)
  const [validationResult, setValidationResult] = useState(null)
  const [profileResult, setProfileResult] = useState(null)
  const [erDiagram, setErDiagram] = useState(null)
  const [activeTab, setActiveTab] = useState(0)

  useEffect(() => {
    const fetchSchemaDefinitions = async () => {
      try {
        const schemas = await getSchemaDefinitions()
        setSchemaDefinitions(schemas)
      } catch (err) {
        setError(`Failed to fetch schema definitions: ${err.message}`)
      }
    }

    fetchSchemaDefinitions()
  }, [])

  const handleFileUpload = (event) => {
    const file = event.target.files[0]
    if (file) {
      setSelectedFile(file)
      setValidationResult(null)
      setProfileResult(null)
      setErDiagram(null)
      setError(null)
    }
  }

  const handleLocalFileSelect = (file) => {
    setSelectedFile(file)
    setValidationResult(null)
    setProfileResult(null)
    setErDiagram(null)
    setError(null)
  }

  const handleValidate = async () => {
    if (!selectedFile) {
      setError('Please select a file first')
      return
    }

    setLoading(true)
    setError(null)

    try {
      const formData = new FormData()
      formData.append('file', selectedFile)
      const result = await validateFile(formData)
      setValidationResult(result)
    } catch (err) {
      setError(err.message || 'Error validating file')
    } finally {
      setLoading(false)
    }
  }

  const handleGenerateProfile = async () => {
    if (!selectedFile) {
      setError('Please select a file first')
      return
    }

    setLoading(true)
    setError(null)

    try {
      const formData = new FormData()
      formData.append('file', selectedFile)
      const result = await generateProfile(formData)
      setProfileResult(result)
    } catch (err) {
      setError(err.message || 'Error generating profile')
    } finally {
      setLoading(false)
    }
  }

  const handleGenerateErDiagram = async () => {
    if (!selectedFile) {
      setError('Please select a file first')
      return
    }

    setLoading(true)
    setError(null)

    try {
      const result = await getErDiagram()
      setErDiagram(result)
    } catch (err) {
      setError(err.message || 'Error generating ER diagram')
    } finally {
      setLoading(false)
    }
  }

  const handleTabChange = (event, newValue) => {
    setSelectedTab(newValue)
  }

  return (
    <Box>
      <Typography variant="h4" gutterBottom>
        Data Quality Assessment
      </Typography>
      
      <Grid container spacing={3}>
        <Grid item xs={12} md={4}>
          <Paper sx={{ p: 3, mb: 3 }}>
            <Typography variant="h6" gutterBottom>
              Select Data Source
            </Typography>
            
            <Tabs value={activeTab} onChange={(event, newValue) => {
              setActiveTab(newValue)
              setSelectedTab(0)
            }} sx={{ mb: 2 }}>
              <Tab label="Upload File" icon={<UploadIcon />} />
              <Tab label="Local Files" icon={<FolderIcon />} />
            </Tabs>

            {activeTab === 0 && (
              <Box>
                <input
                  accept=".csv"
                  style={{ display: 'none' }}
                  id="file-upload"
                  type="file"
                  onChange={handleFileUpload}
                />
                <label htmlFor="file-upload">
                  <Button
                    variant="contained"
                    component="span"
                    fullWidth
                    sx={{ mb: 2 }}
                  >
                    Choose File
                  </Button>
                </label>
                {selectedFile && (
                  <Typography variant="body2" color="text.secondary">
                    Selected: {selectedFile.name}
                  </Typography>
                )}
              </Box>
            )}

            {activeTab === 1 && (
              <List>
                {LOCAL_FILES.map((file) => (
                  <ListItem key={file.name} disablePadding>
                    <ListItemButton
                      selected={selectedFile?.name === file.name}
                      onClick={() => handleLocalFileSelect(file)}
                    >
                      <ListItemText primary={file.name} />
                    </ListItemButton>
                  </ListItem>
                ))}
              </List>
            )}

            <Divider sx={{ my: 2 }} />

            <Button
              variant="contained"
              onClick={handleValidate}
              disabled={!selectedFile || loading[selectedFile.name]}
              fullWidth
              sx={{ mb: 1 }}
            >
              Validate Data
            </Button>
            <Button
              variant="contained"
              onClick={handleGenerateProfile}
              disabled={!selectedFile || loading[selectedFile.name]}
              fullWidth
              sx={{ mb: 1 }}
            >
              Generate Profile
            </Button>
            <Button
              variant="contained"
              onClick={handleGenerateErDiagram}
              disabled={!selectedFile || loading[selectedFile.name]}
              fullWidth
            >
              Generate ER Diagram
            </Button>
          </Paper>
        </Grid>

        <Grid item xs={12} md={8}>
          <Paper sx={{ p: 3 }}>
            <Box sx={{ borderBottom: 1, borderColor: 'divider', mb: 2 }}>
              <Tabs value={selectedTab} onChange={handleTabChange}>
                <Tab label="Validation" icon={<AssessmentIcon />} iconPosition="start" />
                <Tab label="Profiling" icon={<ShowChartIcon />} iconPosition="start" />
                <Tab label="Schema" icon={<SchemaIcon />} iconPosition="start" />
              </Tabs>
            </Box>

            {/* Validation Tab */}
            {selectedTab === 0 && (
              <Box>
                <Typography variant="h6" gutterBottom>
                  Validation Results
                </Typography>
                {loading && (
                  <Box display="flex" justifyContent="center" my={4}>
                    <CircularProgress />
                  </Box>
                )}
                {validationResult && (
                  <Box>
                    <Alert 
                      severity={validationResult.details.validation_passed ? "success" : "error"}
                      sx={{ mb: 2 }}
                    >
                      {validationResult.message}
                    </Alert>
                    {!validationResult.details.validation_passed && (
                      <TableContainer component={Paper} variant="outlined">
                        <Table size="small">
                          <TableHead>
                            <TableRow>
                              <TableCell>Error Details</TableCell>
                            </TableRow>
                          </TableHead>
                          <TableBody>
                            <TableRow>
                              <TableCell style={{ whiteSpace: 'pre-wrap' }}>
                                {validationResult.details.errors}
                              </TableCell>
                            </TableRow>
                          </TableBody>
                        </Table>
                      </TableContainer>
                    )}
                  </Box>
                )}
              </Box>
            )}

            {/* Profiling Tab */}
            {selectedTab === 1 && (
              <Box>
                <Typography variant="h6" gutterBottom>
                  Data Profile Report
                </Typography>
                {loading && (
                  <Box display="flex" justifyContent="center" my={4}>
                    <CircularProgress />
                  </Box>
                )}
                {profileResult && (
                  <Box>
                    <Alert severity="info" sx={{ mb: 2 }}>
                      Profile generated for {selectedFile.name}
                    </Alert>
                    <iframe
                      src={profileResult.details.profile_url}
                      style={{ width: '100%', height: '600px', border: 'none' }}
                      title={`Profile Report for ${selectedFile.name}`}
                    />
                  </Box>
                )}
              </Box>
            )}

            {/* Schema Tab */}
            {selectedTab === 2 && (
              <Box>
                <Typography variant="h6" gutterBottom>
                  Schema Information
                </Typography>
                <Button
                  variant="outlined"
                  startIcon={<AccountTreeIcon />}
                  onClick={() => setErDiagramOpen(true)}
                  sx={{ mb: 2 }}
                >
                  View ER Diagram
                </Button>
                {selectedFile && schemaDefinitions[selectedFile.name] && (
                  <Box>
                    <Typography variant="subtitle1" color="primary" gutterBottom sx={{ mt: 2 }}>
                      {selectedFile.name} Schema
                    </Typography>
                    <TableContainer component={Paper} variant="outlined">
                      <Table size="small">
                        <TableHead>
                          <TableRow>
                            <TableCell>Column</TableCell>
                            <TableCell>Type</TableCell>
                            <TableCell>Nullable</TableCell>
                            <TableCell>Checks</TableCell>
                          </TableRow>
                        </TableHead>
                        <TableBody>
                          {Object.entries(schemaDefinitions[selectedFile.name].columns || {}).map(([column, details]) => (
                            <TableRow key={column}>
                              <TableCell>{column}</TableCell>
                              <TableCell>{details.type}</TableCell>
                              <TableCell>{details.nullable ? 'Yes' : 'No'}</TableCell>
                              <TableCell>{details.checks?.join(', ') || '-'}</TableCell>
                            </TableRow>
                          ))}
                        </TableBody>
                      </Table>
                    </TableContainer>
                  </Box>
                )}
              </Box>
            )}
          </Paper>
        </Grid>
      </Grid>

      {error && (
        <Alert 
          severity="error" 
          sx={{ mt: 2 }}
          action={
            <IconButton
              aria-label="close"
              color="inherit"
              size="small"
              onClick={() => setError(null)}
            >
              <CloseIcon fontSize="inherit" />
            </IconButton>
          }
        >
          {error}
        </Alert>
      )}

      {/* ER Diagram Dialog */}
      <Dialog
        open={erDiagramOpen}
        onClose={() => setErDiagramOpen(false)}
        maxWidth="lg"
        fullWidth
      >
        <DialogTitle>
          ER Diagram
          <IconButton
            aria-label="close"
            onClick={() => setErDiagramOpen(false)}
            sx={{ position: 'absolute', right: 8, top: 8 }}
          >
            <CloseIcon />
          </IconButton>
        </DialogTitle>
        <DialogContent>
          {erDiagram && (
            <img 
              src={erDiagram} 
              alt="ER Diagram" 
              style={{ width: '100%', height: 'auto' }}
            />
          )}
        </DialogContent>
      </Dialog>
    </Box>
  )
} 