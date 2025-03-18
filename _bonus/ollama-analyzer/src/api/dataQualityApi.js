import axios from 'axios'

// Create axios instance with default config
const api = axios.create({
  baseURL: import.meta.env.VITE_API_BASE_URL || 'http://localhost:5001',
  headers: {
    'Content-Type': 'application/json',
  },
})

// Add request interceptor for error handling
api.interceptors.request.use(
  (config) => {
    // Don't modify content-type for FormData
    if (config.data instanceof FormData) {
      delete config.headers['Content-Type']
    }
    console.log('API Request:', config.method.toUpperCase(), config.url)
    return config
  },
  (error) => {
    console.error('API Request Error:', error)
    return Promise.reject(error)
  }
)

// Add response interceptor for error handling
api.interceptors.response.use(
  (response) => {
    console.log('API Response:', response.status, response.config.url)
    return response
  },
  (error) => {
    console.error('API Response Error:', error.response?.status, error.response?.data)
    if (!error.response) {
      throw new Error('Network Error: Unable to connect to the server. Please ensure the backend server is running.')
    }
    return Promise.reject(error)
  }
)

export const validateFile = async (formData) => {
  try {
    const response = await api.post('/api/validate', formData)
    return response.data
  } catch (error) {
    if (!error.response) {
      throw new Error('Network Error: Unable to connect to the server. Please ensure the backend server is running.')
    }
    throw new Error(`Validation failed: ${error.response?.data?.detail || error.message}`)
  }
}

export const generateProfile = async (formData) => {
  try {
    const response = await api.post('/api/profile', formData)
    return response.data
  } catch (error) {
    if (!error.response) {
      throw new Error('Network Error: Unable to connect to the server. Please ensure the backend server is running.')
    }
    throw new Error(`Profile generation failed: ${error.response?.data?.detail || error.message}`)
  }
}

export const getErDiagram = async () => {
  try {
    const response = await api.get('/api/er-diagram')
    return response.data
  } catch (error) {
    if (!error.response) {
      throw new Error('Network Error: Unable to connect to the server. Please ensure the backend server is running.')
    }
    throw new Error(`Failed to fetch ER diagram: ${error.response?.data?.detail || error.message}`)
  }
}

export const getSchemaDefinitions = async () => {
  try {
    const response = await api.get('/api/schemas')
    return response.data
  } catch (error) {
    if (!error.response) {
      throw new Error('Network Error: Unable to connect to the server. Please ensure the backend server is running.')
    }
    throw new Error(`Failed to fetch schema definitions: ${error.response?.data?.detail || error.message}`)
  }
} 