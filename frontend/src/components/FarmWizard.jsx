import React, { useState, useEffect, useRef } from 'react';
import { MapContainer, TileLayer, Marker, useMapEvents } from 'react-leaflet';
import L from 'leaflet';
import 'leaflet/dist/leaflet.css';
import { Button } from './ui/button';
import { Input } from './ui/input';
import { Label } from './ui/label';
import { Switch } from './ui/switch';
import { Slider } from './ui/slider';
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
  { id: 4, title: 'Constraints', icon: X, description: 'Crop exclusions' },
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
  'Soybeans',
  'Wheat',
  'Cotton',
  'Rice',
  'Alfalfa',
  'Sorghum',
  'Sunflower',
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
    soil_ph: 6.5,
    crop_constraints: [],
    risk_preference: 'moderate',
    goal: 'balanced',
  });
  const [markerPosition, setMarkerPosition] = useState([39.8283, -98.5795]);
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

  const updateFormData = (field, value) => {
    setFormData(prev => ({ ...prev, [field]: value }));
  };

  const toggleCropConstraint = (crop) => {
    setFormData(prev => ({
      ...prev,
      crop_constraints: prev.crop_constraints.includes(crop)
        ? prev.crop_constraints.filter(c => c !== crop)
        : [...prev.crop_constraints, crop]
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
        return true;
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

                <div>
                  <Label className="text-slate-600 mb-2 block">
                    Soil pH Level: <span className="font-semibold text-emerald-950">{formData.soil_ph.toFixed(1)}</span>
                  </Label>
                  <div className="pt-2 pb-4">
                    <Slider
                      value={[formData.soil_ph]}
                      onValueChange={([value]) => updateFormData('soil_ph', value)}
                      min={4}
                      max={9}
                      step={0.1}
                      className="w-full"
                      data-testid="soil-ph-slider"
                    />
                    <div className="flex justify-between mt-2 text-xs text-slate-400">
                      <span>4.0 (Acidic)</span>
                      <span>7.0 (Neutral)</span>
                      <span>9.0 (Alkaline)</span>
                    </div>
                  </div>
                </div>
              </div>
            </div>
          )}

          {/* Step 4: Constraints */}
          {currentStep === 4 && (
            <div className="space-y-6" data-testid="step-4-content">
              <div>
                <h3 className="font-display text-lg font-semibold text-emerald-950 mb-2">
                  Crop Constraints
                </h3>
                <p className="text-sm text-slate-500 mb-4">
                  Select any crops you want to exclude from the analysis
                </p>
              </div>

              <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
                {CROPS.map((crop) => {
                  const isExcluded = formData.crop_constraints.includes(crop);
                  return (
                    <button
                      key={crop}
                      onClick={() => toggleCropConstraint(crop)}
                      className={`p-4 rounded-xl border-2 text-left transition-all ${
                        isExcluded
                          ? 'border-red-300 bg-red-50 text-red-700'
                          : 'border-slate-200 bg-white hover:border-lime-300 text-slate-700'
                      }`}
                      data-testid={`crop-constraint-${crop.toLowerCase()}`}
                    >
                      <div className="flex items-center justify-between">
                        <span className="font-medium">{crop}</span>
                        {isExcluded && <X className="w-4 h-4 text-red-500" />}
                      </div>
                      <span className="text-xs text-slate-400">
                        {isExcluded ? 'Excluded' : 'Included'}
                      </span>
                    </button>
                  );
                })}
              </div>

              <p className="text-sm text-slate-400">
                {formData.crop_constraints.length === 0
                  ? 'All crops will be considered in the analysis'
                  : `${formData.crop_constraints.length} crop(s) excluded`}
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
