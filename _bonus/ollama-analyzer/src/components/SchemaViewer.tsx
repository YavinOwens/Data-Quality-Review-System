import { Box, Typography, Paper, Tabs, Tab, Card, CardContent, Table, TableBody, TableCell, TableContainer, TableHead, TableRow } from '@mui/material';
import { useState } from 'react';

interface SchemaField {
  type: string;
  nullable: boolean;
  required: boolean;
  regex?: string;
  allowed_values?: string[];
  unique?: boolean;
  min_value?: number;
  max_value?: number;
}

interface SchemaDefinition {
  [key: string]: SchemaField;
}

interface SchemaViewerProps {
  schemas: Record<string, SchemaDefinition> | null;
}

function SchemaViewer({ schemas }: SchemaViewerProps) {
  const [selectedTab, setSelectedTab] = useState(0);

  if (!schemas) {
    return (
      <Box sx={{ p: 3 }}>
        <Typography>Loading schemas...</Typography>
      </Box>
    );
  }

  const schemaNames = Object.keys(schemas);

  const handleTabChange = (event: React.SyntheticEvent, newValue: number) => {
    setSelectedTab(newValue);
  };

  const renderFieldConstraints = (field: SchemaField) => {
    const constraints = [];
    if (field.unique) constraints.push('Unique');
    if (!field.nullable) constraints.push('Required');
    if (field.regex) constraints.push(`Pattern: ${field.regex}`);
    if (field.allowed_values) constraints.push(`Values: ${field.allowed_values.join(', ')}`);
    if (field.min_value !== undefined) constraints.push(`Min: ${field.min_value}`);
    if (field.max_value !== undefined) constraints.push(`Max: ${field.max_value}`);
    return constraints.join(', ');
  };

  return (
    <Box sx={{ width: '100%' }}>
      <Typography variant="h5" gutterBottom>
        Schema Definitions
      </Typography>
      <Paper sx={{ width: '100%', mb: 2 }}>
        <Tabs
          value={selectedTab}
          onChange={handleTabChange}
          indicatorColor="primary"
          textColor="primary"
          variant="scrollable"
          scrollButtons="auto"
        >
          {schemaNames.map((name, index) => (
            <Tab key={name} label={name} id={`schema-tab-${index}`} />
          ))}
        </Tabs>
      </Paper>
      {schemaNames.map((name, index) => (
        <Box
          key={name}
          role="tabpanel"
          hidden={selectedTab !== index}
          id={`schema-tabpanel-${index}`}
        >
          {selectedTab === index && (
            <Card>
              <CardContent>
                <Typography variant="h6" gutterBottom>
                  {name} Schema
                </Typography>
                <TableContainer>
                  <Table>
                    <TableHead>
                      <TableRow>
                        <TableCell>Field Name</TableCell>
                        <TableCell>Type</TableCell>
                        <TableCell>Constraints</TableCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {Object.entries(schemas[name]).map(([fieldName, field]) => (
                        <TableRow key={fieldName}>
                          <TableCell>{fieldName}</TableCell>
                          <TableCell>{field.type}</TableCell>
                          <TableCell>{renderFieldConstraints(field)}</TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </TableContainer>
              </CardContent>
            </Card>
          )}
        </Box>
      ))}
    </Box>
  );
}

export default SchemaViewer; 