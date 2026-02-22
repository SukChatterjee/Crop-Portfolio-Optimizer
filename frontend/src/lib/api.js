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
    const selectedCrops = farmProfile?.selected_crops || [];
    const legacyCropUniverse = ['Corn', 'Wheat', 'Soybeans', 'Rice', 'Cotton', 'Alfalfa', 'Sorghum', 'Sunflower'];
    const selectedSet = new Set(selectedCrops.map(c => String(c).toLowerCase()));
    const legacyCropConstraints = legacyCropUniverse.filter(c => !selectedSet.has(c.toLowerCase()));

    const payload = {
      farm_profile: {
        location: {
          lat: farmProfile?.location?.lat,
          lng: farmProfile?.location?.lng,
          address: farmProfile?.location?.address || '',
          county: farmProfile?.location?.county || '',
          state: farmProfile?.location?.state || '',
        },
        acres: farmProfile?.acres,
        has_irrigation: farmProfile?.has_irrigation,
        soil_type: farmProfile?.soil_type,
        selected_crops: selectedCrops,
        // Backward-compatible fields for older backend instances still expecting legacy schema.
        soil_ph: 6.5,
        crop_constraints: legacyCropConstraints,
        risk_preference: farmProfile?.risk_preference,
        goal: farmProfile?.goal,
      },
    };

    const response = await axios.post(
      `${API}/analysis/create`,
      payload,
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
