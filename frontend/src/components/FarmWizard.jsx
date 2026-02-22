import React, { useState, useEffect, useRef } from 'react';
import { MapContainer, TileLayer, Marker, useMapEvents } from 'react-leaflet';
import L from 'leaflet';
import 'leaflet/dist/leaflet.css';
import { Button } from './ui/button';
import { Input } from './ui/input';
import { Label } from './ui/label';
import { Switch } from './ui/switch';
import { RadioGroup, RadioGroupItem } from './ui/radio-group';
import { Card, CardContent } from './ui/card';
import { 
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from './ui/select';
import { 
  MapPin, 
  Sprout, 
  Droplets, 
  Target, 
  ChevronRight, 
  ChevronLeft,
  Check,
  X
} from 'lucide-react';

// Fix Leaflet default marker icon issue
delete L.Icon.Default.prototype._getIconUrl;
L.Icon.Default.mergeOptions({
  iconRetinaUrl: 'https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.9.4/images/marker-icon-2x.png',
  iconUrl: 'https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.9.4/images/marker-icon.png',
  shadowUrl: 'https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.9.4/images/marker-shadow.png',
});

const customIcon = new L.Icon({
  iconUrl: 'https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.9.4/images/marker-icon.png',
  iconRetinaUrl: 'https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.9.4/images/marker-icon-2x.png',
  shadowUrl: 'https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.9.4/images/marker-shadow.png',
  iconSize: [25, 41],
  iconAnchor: [12, 41],
  popupAnchor: [1, -34],
  shadowSize: [41, 41]
});

const STEPS = [
  { id: 1, title: 'Location', icon: MapPin, description: 'Select your farm location' },
  { id: 2, title: 'Farm Details', icon: Sprout, description: 'Enter farm specifications' },
  { id: 3, title: 'Soil Info', icon: Droplets, description: 'Soil type and conditions' },
  { id: 4, title: 'Crops', icon: X, description: 'Choose crops to include' },
  { id: 5, title: 'Goals', icon: Target, description: 'Define your objectives' },
];

const SOIL_TYPES = [
  'Clay',
  'Clay Loam',
  'Loam',
  'Sandy Loam',
  'Sandy Clay',
  'Silt Loam',
  'Silty Clay',
];

const CROPS = [
  'Corn',
  'Wheat',
  'Soybeans',
  'Rice',
  'Cotton',
  'Tomatoes',
  'Potatoes',
  'Onions',
  'Apples',
  'Lettuce',
];

// Map click handler component
const LocationMarker = ({ position, setPosition }) => {
  useMapEvents({
    click(e) {
      setPosition([e.latlng.lat, e.latlng.lng]);
    },
  });

  return position ? <Marker position={position} icon={customIcon} /> : null;
};

export const FarmWizard = ({ onComplete, onCancel }) => {
  const [currentStep, setCurrentStep] = useState(1);
  const [formData, setFormData] = useState({
    location: { lat: 39.8283, lng: -98.5795, address: '', county: '', state: '' },
    acres: 100,
    has_irrigation: false,
    soil_type: '',
    selected_crops: [],
    other_crop_text: '',
    risk_preference: 'moderate',
    goal: 'balanced',
  });
  const [markerPosition, setMarkerPosition] = useState([39.8283, -98.5795]);
  const [locationQuery, setLocationQuery] = useState('');
  const [locationSearchError, setLocationSearchError] = useState('');
  const [isSearchingLocation, setIsSearchingLocation] = useState(false);
  const mapRef = useRef(null);

  useEffect(() => {
    if (markerPosition) {
      setFormData(prev => ({
        ...prev,
        location: {
          ...prev.location,
          lat: markerPosition[0],
          lng: markerPosition[1],
        }
      }));
    }
  }, [markerPosition]);

  const handleLocationSearch = async () => {
    const query = locationQuery.trim();
    if (!query) {
      setLocationSearchError('Please enter a location to search.');
      return;
    }

    setIsSearchingLocation(true);
    setLocationSearchError('');

    try {
      const url = `https://nominatim.openstreetmap.org/search?format=json&q=${encodeURIComponent(query)}&limit=1`;
      // Browsers typically block overriding User-Agent; send standard headers.
      const response = await fetch(url, {
        method: 'GET',
        headers: {
          Accept: 'application/json',
        },
      });

      if (!response.ok) {
        throw new Error('Failed to search location.');
      }

      const results = await response.json();
      if (!results || results.length === 0) {
        setLocationSearchError('No location found. Try a more specific search.');
        return;
      }

      const lat = parseFloat(results[0].lat);
      const lng = parseFloat(results[0].lon);
      if (Number.isNaN(lat) || Number.isNaN(lng)) {
        setLocationSearchError('Invalid coordinates returned for that location.');
        return;
      }

      setMarkerPosition([lat, lng]);
      setFormData(prev => ({
        ...prev,
        location: {
          ...prev.location,
          address: query,
          lat,
          lng,
        },
      }));

      if (mapRef.current && typeof mapRef.current.flyTo === 'function') {
        mapRef.current.flyTo([lat, lng], 12);
      }
    } catch (error) {
      setLocationSearchError('Location search failed. Please try again.');
    } finally {
      setIsSearchingLocation(false);
    }
  };

  const updateFormData = (field, value) => {
    setFormData(prev => ({ ...prev, [field]: value }));
  };

  const toggleSelectedCrop = (crop) => {
    setFormData(prev => ({
      ...prev,
      selected_crops: prev.selected_crops.includes(crop)
        ? prev.selected_crops.filter(c => c !== crop)
        : [...prev.selected_crops, crop]
    }));
  };

  const handleAddOtherCrop = () => {
    const crop = formData.other_crop_text.trim();
    if (!crop) {
      return;
    }
    const isInDefaultList = CROPS.some(c => c.toLowerCase() === crop.toLowerCase());
    if (!isInDefaultList && !formData.selected_crops.some(c => c.toLowerCase() === crop.toLowerCase())) {
      setFormData(prev => ({
        ...prev,
        selected_crops: [...prev.selected_crops, crop],
        other_crop_text: '',
      }));
      return;
    }
    setFormData(prev => ({
      ...prev,
      other_crop_text: '',
    }));
  };

  const canProceed = () => {
    switch (currentStep) {
      case 1:
        return markerPosition !== null;
      case 2:
        return formData.acres > 0;
      case 3:
        return formData.soil_type !== '';
      case 4:
        return formData.selected_crops.length > 0;
      case 5:
        return formData.risk_preference && formData.goal;
      default:
        return false;
    }
  };

  const handleNext = () => {
    if (currentStep < 5) {
      setCurrentStep(currentStep + 1);
    } else {
      onComplete(formData);
    }
  };

  const handleBack = () => {
    if (currentStep > 1) {
      setCurrentStep(currentStep - 1);
    }
  };

  return (
    <div className="fixed inset-0 z-50 bg-slate-900/50 backdrop-blur-sm flex items-center justify-center p-4" data-testid="farm-wizard-overlay">
      <div className="bg-white rounded-2xl shadow-2xl w-full max-w-4xl max-h-[90vh] overflow-hidden flex flex-col" data-testid="farm-wizard-modal">
        {/* Header */}
        <div className="px-6 py-4 border-b border-slate-100 flex items-center justify-between">
          <div>
            <h2 className="font-display text-xl font-bold text-emerald-950">Farm Profile Setup</h2>
            <p className="text-sm text-slate-500">Step {currentStep} of 5</p>
          </div>
          <button
            onClick={onCancel}
            className="p-2 hover:bg-slate-100 rounded-lg transition-colors"
            data-testid="wizard-close-btn"
          >
            <X className="w-5 h-5 text-slate-400" />
          </button>
        </div>

        {/* Progress Steps */}
        <div className="px-6 py-4 bg-slate-50 border-b border-slate-100">
          <div className="flex items-center justify-between">
            {STEPS.map((step, index) => (
              <div key={step.id} className="flex items-center">
                <div className={`flex items-center ${index > 0 ? 'ml-4' : ''}`}>
                  <div
                    className={`w-8 h-8 rounded-full flex items-center justify-center text-sm font-medium transition-colors ${
                      currentStep > step.id
                        ? 'bg-lime-500 text-white'
                        : currentStep === step.id
                        ? 'bg-emerald-950 text-white'
                        : 'bg-slate-200 text-slate-500'
                    }`}
                    data-testid={`step-indicator-${step.id}`}
                  >
                    {currentStep > step.id ? <Check className="w-4 h-4" /> : step.id}
                  </div>
                  <span className={`ml-2 text-sm font-medium hidden sm:block ${
                    currentStep >= step.id ? 'text-emerald-950' : 'text-slate-400'
                  }`}>
                    {step.title}
                  </span>
                </div>
                {index < STEPS.length - 1 && (
                  <div className={`w-8 lg:w-16 h-0.5 ml-4 ${
                    currentStep > step.id ? 'bg-lime-500' : 'bg-slate-200'
                  }`} />
                )}
              </div>
            ))}
          </div>
        </div>

        {/* Content */}
        <div className="flex-1 overflow-y-auto p-6">
          {/* Step 1: Location */}
          {currentStep === 1 && (
            <div className="space-y-6" data-testid="step-1-content">
              <div>
                <h3 className="font-display text-lg font-semibold text-emerald-950 mb-2">
                  Select Your Farm Location
                </h3>
                <p className="text-sm text-slate-500 mb-4">
                  Click on the map to pinpoint your farm's location
                </p>
              </div>

              <div>
                <Label htmlFor="location-search" className="text-slate-600 mb-2 block">
                  Search location
                </Label>
                <div className="flex gap-2">
                  <Input
                    id="location-search"
                    value={locationQuery}
                    onChange={(e) => setLocationQuery(e.target.value)}
                    placeholder="City, county, or address"
                    data-testid="location-search-input"
                  />
                  <Button
                    type="button"
                    onClick={handleLocationSearch}
                    disabled={isSearchingLocation}
                    className="bg-emerald-950 hover:bg-emerald-900 text-white"
                    data-testid="location-search-btn"
                  >
                    {isSearchingLocation ? 'Searching...' : 'Search'}
                  </Button>
                </div>
                {locationSearchError && (
                  <p className="mt-2 text-sm text-red-600" data-testid="location-search-error">
                    {locationSearchError}
                  </p>
                )}
              </div>

              <div className="h-[350px] rounded-xl overflow-hidden border border-slate-200">
                <MapContainer
                  center={markerPosition || [39.8283, -98.5795]}
                  zoom={4}
                  style={{ height: '100%', width: '100%' }}
                  ref={mapRef}
                >
                  <TileLayer
                    attribution='&copy; <a href="https://carto.com/">CARTO</a>'
                    url="https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png"
                  />
                  <LocationMarker position={markerPosition} setPosition={setMarkerPosition} />
                </MapContainer>
              </div>

              {markerPosition && (
                <div className="grid grid-cols-2 gap-4">
                  <div>
                    <Label className="text-slate-600">Latitude</Label>
                    <Input 
                      value={markerPosition[0].toFixed(6)} 
                      readOnly 
                      className="bg-slate-50"
                      data-testid="location-lat-input"
                    />
                  </div>
                  <div>
                    <Label className="text-slate-600">Longitude</Label>
                    <Input 
                      value={markerPosition[1].toFixed(6)} 
                      readOnly 
                      className="bg-slate-50"
                      data-testid="location-lng-input"
                    />
                  </div>
                </div>
              )}
            </div>
          )}

          {/* Step 2: Farm Details */}
          {currentStep === 2 && (
            <div className="space-y-6" data-testid="step-2-content">
              <div>
                <h3 className="font-display text-lg font-semibold text-emerald-950 mb-2">
                  Farm Specifications
                </h3>
                <p className="text-sm text-slate-500 mb-4">
                  Enter details about your farm's size and infrastructure
                </p>
              </div>

              <div className="space-y-6">
                <div>
                  <Label htmlFor="acres" className="text-slate-600 mb-2 block">
                    Total Acreage
                  </Label>
                  <div className="flex items-center gap-4">
                    <Input
                      id="acres"
                      type="number"
                      min={1}
                      value={formData.acres}
                      onChange={(e) => updateFormData('acres', parseFloat(e.target.value) || 0)}
                      className="flex-1"
                      data-testid="acres-input"
                    />
                    <span className="text-sm text-slate-500 font-medium">acres</span>
                  </div>
                </div>

                <Card className="border-slate-200">
                  <CardContent className="p-4">
                    <div className="flex items-center justify-between">
                      <div className="flex items-center gap-3">
                        <div className="w-10 h-10 bg-sky-50 rounded-lg flex items-center justify-center">
                          <Droplets className="w-5 h-5 text-sky-600" />
                        </div>
                        <div>
                          <p className="font-medium text-emerald-950">Irrigation System</p>
                          <p className="text-sm text-slate-500">Do you have irrigation infrastructure?</p>
                        </div>
                      </div>
                      <Switch
                        checked={formData.has_irrigation}
                        onCheckedChange={(checked) => updateFormData('has_irrigation', checked)}
                        data-testid="irrigation-switch"
                      />
                    </div>
                  </CardContent>
                </Card>
              </div>
            </div>
          )}

          {/* Step 3: Soil Info */}
          {currentStep === 3 && (
            <div className="space-y-6" data-testid="step-3-content">
              <div>
                <h3 className="font-display text-lg font-semibold text-emerald-950 mb-2">
                  Soil Information
                </h3>
                <p className="text-sm text-slate-500 mb-4">
                  Provide details about your soil composition
                </p>
              </div>

              <div className="space-y-6">
                <div>
                  <Label className="text-slate-600 mb-2 block">Soil Type</Label>
                  <Select
                    value={formData.soil_type}
                    onValueChange={(value) => updateFormData('soil_type', value)}
                  >
                    <SelectTrigger className="w-full" data-testid="soil-type-select">
                      <SelectValue placeholder="Select soil type" />
                    </SelectTrigger>
                    <SelectContent>
                      {SOIL_TYPES.map((type) => (
                        <SelectItem key={type} value={type} data-testid={`soil-option-${type.toLowerCase().replace(' ', '-')}`}>
                          {type}
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>
              </div>
            </div>
          )}

          {/* Step 4: Crops */}
          {currentStep === 4 && (
            <div className="space-y-6" data-testid="step-4-content">
              <div>
                <h3 className="font-display text-lg font-semibold text-emerald-950 mb-2">
                  Select Crops
                </h3>
                <p className="text-sm text-slate-500 mb-4">
                  Choose one or more crops to include in your analysis
                </p>
              </div>

              <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
                {CROPS.map((crop) => {
                  const isSelected = formData.selected_crops.includes(crop);
                  const cropTestId = `crop-select-${crop.toLowerCase().replace(/\s+/g, '-')}`;
                  return (
                    <button
                      key={crop}
                      onClick={() => toggleSelectedCrop(crop)}
                      className={`p-4 rounded-xl border-2 text-left transition-all ${
                        isSelected
                          ? 'border-emerald-300 bg-emerald-50 text-emerald-700'
                          : 'border-slate-200 bg-white hover:border-emerald-300 text-slate-700'
                      }`}
                      data-testid={cropTestId}
                    >
                      <div className="flex items-center justify-between">
                        <span className="font-medium">{crop}</span>
                        {isSelected && <Check className="w-4 h-4 text-emerald-500" />}
                      </div>
                      <span className="text-xs text-slate-400">
                        {isSelected ? 'Selected' : 'Not selected'}
                      </span>
                    </button>
                  );
                })}
              </div>

              <div>
                <Label htmlFor="other-crop" className="text-slate-600 mb-2 block">Other crop</Label>
                <div className="flex gap-2">
                  <Input
                    id="other-crop"
                    value={formData.other_crop_text}
                    onChange={(e) => updateFormData('other_crop_text', e.target.value)}
                    placeholder="Type crop name"
                    data-testid="other-crop-input"
                  />
                  <Button
                    type="button"
                    onClick={handleAddOtherCrop}
                    className="bg-emerald-950 hover:bg-emerald-900 text-white"
                    data-testid="other-crop-add-btn"
                  >
                    Add
                  </Button>
                </div>
              </div>

              <p className="text-sm text-slate-500" data-testid="selected-crops-count">
                {formData.selected_crops.length} crop(s) selected
              </p>
            </div>
          )}

          {/* Step 5: Goals */}
          {currentStep === 5 && (
            <div className="space-y-8" data-testid="step-5-content">
              <div>
                <h3 className="font-display text-lg font-semibold text-emerald-950 mb-2">
                  Define Your Goals
                </h3>
                <p className="text-sm text-slate-500 mb-4">
                  Set your risk tolerance and optimization objective
                </p>
              </div>

              <div>
                <Label className="text-slate-600 mb-4 block">Risk Preference</Label>
                <RadioGroup
                  value={formData.risk_preference}
                  onValueChange={(value) => updateFormData('risk_preference', value)}
                  className="grid grid-cols-3 gap-4"
                >
                  {[
                    { value: 'conservative', label: 'Conservative', desc: 'Lower risk, stable returns' },
                    { value: 'moderate', label: 'Moderate', desc: 'Balanced approach' },
                    { value: 'aggressive', label: 'Aggressive', desc: 'Higher risk, higher potential' },
                  ].map((option) => (
                    <Label
                      key={option.value}
                      htmlFor={option.value}
                      className={`flex flex-col items-center p-4 rounded-xl border-2 cursor-pointer transition-all ${
                        formData.risk_preference === option.value
                          ? 'border-emerald-500 bg-emerald-50'
                          : 'border-slate-200 hover:border-slate-300'
                      }`}
                      data-testid={`risk-option-${option.value}`}
                    >
                      <RadioGroupItem value={option.value} id={option.value} className="sr-only" />
                      <span className="font-medium text-emerald-950">{option.label}</span>
                      <span className="text-xs text-slate-500 text-center mt-1">{option.desc}</span>
                    </Label>
                  ))}
                </RadioGroup>
              </div>

              <div>
                <Label className="text-slate-600 mb-4 block">Optimization Goal</Label>
                <RadioGroup
                  value={formData.goal}
                  onValueChange={(value) => updateFormData('goal', value)}
                  className="grid grid-cols-3 gap-4"
                >
                  {[
                    { value: 'maximize_profit', label: 'Maximize Profit', desc: 'Focus on highest returns' },
                    { value: 'balanced', label: 'Balanced', desc: 'Profit with risk management' },
                    { value: 'minimize_risk', label: 'Minimize Risk', desc: 'Prioritize stability' },
                  ].map((option) => (
                    <Label
                      key={option.value}
                      htmlFor={`goal-${option.value}`}
                      className={`flex flex-col items-center p-4 rounded-xl border-2 cursor-pointer transition-all ${
                        formData.goal === option.value
                          ? 'border-emerald-500 bg-emerald-50'
                          : 'border-slate-200 hover:border-slate-300'
                      }`}
                      data-testid={`goal-option-${option.value}`}
                    >
                      <RadioGroupItem value={option.value} id={`goal-${option.value}`} className="sr-only" />
                      <span className="font-medium text-emerald-950">{option.label}</span>
                      <span className="text-xs text-slate-500 text-center mt-1">{option.desc}</span>
                    </Label>
                  ))}
                </RadioGroup>
              </div>
            </div>
          )}
        </div>

        {/* Footer */}
        <div className="px-6 py-4 border-t border-slate-100 flex items-center justify-between bg-slate-50">
          <Button
            variant="outline"
            onClick={handleBack}
            disabled={currentStep === 1}
            className="gap-2"
            data-testid="wizard-back-btn"
          >
            <ChevronLeft className="w-4 h-4" />
            Back
          </Button>

          <Button
            onClick={handleNext}
            disabled={!canProceed()}
            className="bg-emerald-950 hover:bg-emerald-900 text-white gap-2"
            data-testid="wizard-next-btn"
          >
            {currentStep === 5 ? 'Run Analysis' : 'Continue'}
            <ChevronRight className="w-4 h-4" />
          </Button>
        </div>
      </div>
    </div>
  );
};

export default FarmWizard;
