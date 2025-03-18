import { Grid, Paper, Typography, Box } from '@mui/material';
import {
  Assessment as AssessmentIcon,
  Warning as WarningIcon,
  CheckCircle as CheckCircleIcon,
  Error as ErrorIcon,
} from '@mui/icons-material';

const StatCard = ({ title, value, icon, color }) => (
  <Paper
    sx={{
      p: 2,
      display: 'flex',
      flexDirection: 'column',
      height: 140,
    }}
  >
    <Box sx={{ display: 'flex', alignItems: 'center', mb: 2 }}>
      {icon}
      <Typography variant="h6" component="div" sx={{ ml: 1 }}>
        {title}
      </Typography>
    </Box>
    <Typography component="p" variant="h4" sx={{ color }}>
      {value}
    </Typography>
  </Paper>
);

export function Dashboard() {
  return (
    <Box>
      <Typography variant="h4" gutterBottom>
        Data Quality Dashboard
      </Typography>
      <Grid container spacing={3}>
        <Grid item xs={12} sm={6} md={3}>
          <StatCard
            title="Total Records"
            value="1,234"
            icon={<AssessmentIcon color="primary" />}
            color="primary.main"
          />
        </Grid>
        <Grid item xs={12} sm={6} md={3}>
          <StatCard
            title="Issues Found"
            value="45"
            icon={<WarningIcon color="warning" />}
            color="warning.main"
          />
        </Grid>
        <Grid item xs={12} sm={6} md={3}>
          <StatCard
            title="Valid Records"
            value="1,189"
            icon={<CheckCircleIcon color="success" />}
            color="success.main"
          />
        </Grid>
        <Grid item xs={12} sm={6} md={3}>
          <StatCard
            title="Critical Errors"
            value="12"
            icon={<ErrorIcon color="error" />}
            color="error.main"
          />
        </Grid>
      </Grid>
    </Box>
  );
} 