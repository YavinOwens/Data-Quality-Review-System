import axios from 'axios';

const api = axios.create({
  baseURL: import.meta.env.VITE_API_BASE_URL || 'http://localhost:5001',
});

// Add request interceptor for logging
api.interceptors.request.use(
  (config) => {
    console.log('API Request:', config.method?.toUpperCase(), config.url);
    return config;
  },
  (error) => {
    console.error('API Request Error:', error);
    return Promise.reject(error);
  }
);

// Add response interceptor for error handling
api.interceptors.response.use(
  (response) => {
    console.log('API Response:', response.status, response.data);
    return response;
  },
  (error) => {
    console.error('API Response Error:', error.response?.data || error.message);
    return Promise.reject(error);
  }
);

export const validateFile = async (file: File) => {
  const formData = new FormData();
  formData.append('file', file);
  const response = await api.post('/validate', formData);
  return response.data;
};

export const generateProfile = async (file: File) => {
  const formData = new FormData();
  formData.append('file', file);
  const response = await api.post('/profile', formData);
  return response.data;
};

export const getErDiagram = async () => {
  const response = await api.get('/er-diagram');
  return response.data;
};

export const getSchemaDefinitions = async () => {
  const response = await api.get('/schema-definitions');
  return response.data;
}; 