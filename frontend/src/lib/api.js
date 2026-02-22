import axios from 'axios';

const BACKEND_URL = process.env.REACT_APP_BACKEND_URL;
const API = `${BACKEND_URL}/api`;

const getAuthHeaders = () => {
  const token = localStorage.getItem('crop_optimizer_token');
  return token ? { Authorization: `Bearer ${token}` } : {};
};

export const api = {
  // Analysis endpoints
  createAnalysis: async (farmProfile) => {
    const response = await axios.post(
      `${API}/analysis/create`,
      { farm_profile: farmProfile },
      { headers: getAuthHeaders() }
    );
    return response.data;
  },

  getAnalysis: async (analysisId) => {
    const response = await axios.get(`${API}/analysis/${analysisId}`, {
      headers: getAuthHeaders()
    });
    return response.data;
  },

  getAnalyses: async () => {
    const response = await axios.get(`${API}/analysis`, {
      headers: getAuthHeaders()
    });
    return response.data;
  }
};

export default api;
