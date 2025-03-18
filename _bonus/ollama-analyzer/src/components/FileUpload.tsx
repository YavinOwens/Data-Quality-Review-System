import { ChangeEvent } from 'react';
import { Box, Button, Typography } from '@mui/material';
import { Upload, Assessment } from '@mui/icons-material';

interface FileUploadProps {
  onFileUpload: (files: FileList) => void;
  onFullAssessment: (files: FileList) => void;
}

function FileUpload({ onFileUpload, onFullAssessment }: FileUploadProps) {
  const handleFileChange = (event: ChangeEvent<HTMLInputElement>) => {
    const files = event.target.files;
    if (files && files.length > 0) {
      onFileUpload(files);
    }
  };

  const handleFullAssessment = (event: ChangeEvent<HTMLInputElement>) => {
    const files = event.target.files;
    if (files && files.length > 0) {
      onFullAssessment(files);
    }
  };

  return (
    <Box sx={{ 
      display: 'flex', 
      flexDirection: 'column', 
      gap: 2,
      width: '100%',
      mb: 4 
    }}>
      <Typography variant="h5" gutterBottom>
        Select Data Source
      </Typography>
      <Box sx={{ 
        display: 'flex', 
        gap: 2,
        alignItems: 'center' 
      }}>
        <Button
          variant="contained"
          component="label"
          startIcon={<Upload />}
        >
          Upload File
          <input
            type="file"
            hidden
            onChange={handleFileChange}
            accept=".csv"
          />
        </Button>
        <Button
          variant="contained"
          component="label"
          startIcon={<Assessment />}
          color="secondary"
        >
          Full Assessment
          <input
            type="file"
            hidden
            onChange={handleFullAssessment}
            accept=".csv"
          />
        </Button>
      </Box>
    </Box>
  );
}

export default FileUpload; 