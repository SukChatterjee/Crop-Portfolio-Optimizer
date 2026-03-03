import React, { useState } from 'react';
import CountUp from 'react-countup';
import { 
  BarChart, 
  Bar, 
  XAxis, 
  YAxis, 
  Tooltip, 
  ResponsiveContainer,
  LineChart,
  Line,
  Area,
  AreaChart,
  Cell
} from 'recharts';
import { Card, CardContent, CardHeader, CardTitle } from './ui/card';
import { Tabs, TabsContent, TabsList, TabsTrigger } from './ui/tabs';
import { Button } from './ui/button';
import { Progress } from './ui/progress';
import { 
  TrendingUp, 
  DollarSign, 
  Wheat, 
  AlertTriangle, 
  ChevronRight,
  Cloud,
  BarChart3,
  Layers,
  Download,
  Plus
} from 'lucide-react';

const CHART_COLORS = {
  profit: '#10b981',
  risk: '#f43f5e',
  yield: '#0ea5e9',
  soil: '#d97706',
};

const CustomTooltip = ({ active, payload, label }) => {
  if (active && payload && payload.length) {
    return (
      <div className="bg-white px-3 py-2 rounded-lg shadow-lg border border-slate-200">
        <p className="font-medium text-slate-900">{label}</p>
        {payload.map((entry, index) => (
          <p key={index} style={{ color: entry.color }} className="text-sm">
            {entry.name}: ${entry.value?.toLocaleString()}
          </p>
        ))}
      </div>
    );
  }
  return null;
};

const CropCard = ({ crop, rank, isSelected, onClick }) => {
  const getRiskColor = (level) => {
    switch (level) {
      case 'Low': return 'text-emerald-600 bg-emerald-50';
      case 'Medium': return 'text-amber-600 bg-amber-50';
      case 'High': return 'text-red-600 bg-red-50';
      default: return 'text-slate-600 bg-slate-50';
    }
  };

  return (
    <Card 
      className={`cursor-pointer transition-all ${
        isSelected 
          ? 'border-emerald-500 shadow-md ring-2 ring-emerald-500/20' 
          : 'border-slate-200 hover:border-slate-300 hover:shadow-sm'
      }`}
      onClick={onClick}
      data-testid={`crop-card-${crop.crop_name.toLowerCase()}`}
    >
      <CardContent className="p-4">
        <div className="flex items-start justify-between mb-3">
          <div className="flex items-center gap-2">
            <div className={`w-8 h-8 rounded-lg flex items-center justify-center ${
              rank === 1 ? 'bg-lime-500 text-white' : 'bg-slate-100 text-slate-600'
            }`}>
              <span className="font-bold text-sm">#{rank}</span>
            </div>
            <div>
              <h3 className="font-display font-semibold text-emerald-950">{crop.crop_name}</h3>
              <span className={`text-xs px-2 py-0.5 rounded-full ${getRiskColor(crop.risk_level)}`}>
                {crop.risk_level} Risk
              </span>
            </div>
          </div>
          <ChevronRight className={`w-5 h-5 transition-transform ${isSelected ? 'rotate-90 text-emerald-600' : 'text-slate-400'}`} />
        </div>

        <div className="space-y-2">
          <div className="flex justify-between items-center">
            <span className="text-sm text-slate-500">Expected Profit</span>
            <span className="font-display font-bold text-emerald-600">
              $<CountUp end={crop.expected_profit} duration={1.5} separator="," decimals={0} />
            </span>
          </div>
          <div className="flex justify-between items-center">
            <span className="text-sm text-slate-500">Soil Compatibility</span>
            <div className="flex items-center gap-2">
              <Progress value={crop.soil_compatibility} className="w-16 h-1.5" />
              <span className="text-sm font-medium text-slate-700">{crop.soil_compatibility}%</span>
            </div>
          </div>
        </div>
      </CardContent>
    </Card>
  );
};

export const ResultsDashboard = ({ analysis, onNewAnalysis }) => {
  const [selectedCrop, setSelectedCrop] = useState(analysis?.results?.[0] || null);

  if (!analysis || !analysis.results) {
    return (
      <div className="flex items-center justify-center min-h-[400px]">
        <p className="text-slate-500">No analysis data available</p>
      </div>
    );
  }

  const topCrop = analysis.results[0];
  const profitDistributionData = analysis.results.map(crop => ({
    name: crop.crop_name,
    p10: crop.profit_p10,
    p50: crop.profit_p50,
    p90: crop.profit_p90,
  }));

  const yieldComparisonData = analysis.results.map(crop => ({
    name: crop.crop_name,
    yield: crop.yield_forecast,
    price: crop.price_forecast * 100, // Scale for visibility
  }));

  return (
    <div className="space-y-8" data-testid="results-dashboard">
      {/* Header */}
      <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
        <div>
          <h1 className="font-display text-2xl font-bold text-emerald-950">Analysis Results</h1>
          <p className="text-slate-500">
            {analysis.farm_profile.acres} acres · {analysis.farm_profile.soil_type} soil · 
            {analysis.farm_profile.has_irrigation ? ' Irrigated' : ' Non-irrigated'}
          </p>
        </div>
        <div className="flex gap-3">
          <Button variant="outline" className="gap-2" data-testid="download-report-btn">
            <Download className="w-4 h-4" />
            Export Report
          </Button>
          <Button 
            onClick={onNewAnalysis}
            className="bg-emerald-950 hover:bg-emerald-900 text-white gap-2"
            data-testid="new-analysis-btn"
          >
            <Plus className="w-4 h-4" />
            New Analysis
          </Button>
        </div>
      </div>

      {/* KPI Cards */}
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
        <Card className="bg-gradient-to-br from-emerald-50 to-white border-emerald-100">
          <CardContent className="p-5">
            <div className="flex items-center gap-3 mb-3">
              <div className="w-10 h-10 bg-emerald-100 rounded-lg flex items-center justify-center">
                <TrendingUp className="w-5 h-5 text-emerald-600" />
              </div>
              <span className="text-sm font-medium text-slate-600">Top Recommendation</span>
            </div>
            <p className="font-display text-2xl font-bold text-emerald-950">{topCrop.crop_name}</p>
            <p className="text-sm text-emerald-600 font-medium">Highest expected profit</p>
          </CardContent>
        </Card>

        <Card className="border-slate-200">
          <CardContent className="p-5">
            <div className="flex items-center gap-3 mb-3">
              <div className="w-10 h-10 bg-lime-50 rounded-lg flex items-center justify-center">
                <DollarSign className="w-5 h-5 text-lime-600" />
              </div>
              <span className="text-sm font-medium text-slate-600">Expected Profit</span>
            </div>
            <p className="font-display text-2xl font-bold text-emerald-950">
              $<CountUp end={topCrop.expected_profit} duration={2} separator="," decimals={0} />
            </p>
            <p className="text-sm text-slate-500">P50 estimate</p>
          </CardContent>
        </Card>

        <Card className="border-slate-200">
          <CardContent className="p-5">
            <div className="flex items-center gap-3 mb-3">
              <div className="w-10 h-10 bg-sky-50 rounded-lg flex items-center justify-center">
                <Wheat className="w-5 h-5 text-sky-600" />
              </div>
              <span className="text-sm font-medium text-slate-600">Yield Forecast</span>
            </div>
            <p className="font-display text-2xl font-bold text-emerald-950">
              <CountUp end={topCrop.yield_forecast} duration={2} separator="," decimals={1} />
            </p>
            <p className="text-sm text-slate-500">bu/acre projected</p>
          </CardContent>
        </Card>

        <Card className="border-slate-200">
          <CardContent className="p-5">
            <div className="flex items-center gap-3 mb-3">
              <div className="w-10 h-10 bg-amber-50 rounded-lg flex items-center justify-center">
                <AlertTriangle className="w-5 h-5 text-amber-600" />
              </div>
              <span className="text-sm font-medium text-slate-600">Risk Score</span>
            </div>
            <p className="font-display text-2xl font-bold text-emerald-950">
              <CountUp end={topCrop.risk_score} duration={2} decimals={1} />
              <span className="text-lg text-slate-400">/100</span>
            </p>
            <p className="text-sm text-slate-500">{topCrop.risk_level} risk level</p>
          </CardContent>
        </Card>
      </div>

      {/* Main Content Grid */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* Crop Rankings */}
        <div className="lg:col-span-1 space-y-4">
          <h2 className="font-display text-lg font-semibold text-emerald-950 flex items-center gap-2">
            <BarChart3 className="w-5 h-5 text-lime-500" />
            Crop Rankings
          </h2>
          <div className="space-y-3">
            {analysis.results.map((crop, index) => (
              <CropCard
                key={crop.crop_name}
                crop={crop}
                rank={index + 1}
                isSelected={selectedCrop?.crop_name === crop.crop_name}
                onClick={() => setSelectedCrop(crop)}
              />
            ))}
          </div>
        </div>

        {/* Charts & Details */}
        <div className="lg:col-span-2 space-y-6">
          <Tabs defaultValue="distribution" className="w-full">
            <TabsList className="bg-slate-100 p-1">
              <TabsTrigger value="distribution" data-testid="tab-distribution">Profit Distribution</TabsTrigger>
              <TabsTrigger value="comparison" data-testid="tab-comparison">Yield Comparison</TabsTrigger>
              <TabsTrigger value="details" data-testid="tab-details">Crop Details</TabsTrigger>
            </TabsList>

            <TabsContent value="distribution" className="mt-4">
              <Card>
                <CardHeader className="pb-2">
                  <CardTitle className="text-base font-medium">Profit Distribution (P10 / P50 / P90)</CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="h-72">
                    <ResponsiveContainer width="100%" height="100%">
                      <BarChart data={profitDistributionData} margin={{ top: 20, right: 30, left: 20, bottom: 5 }}>
                        <XAxis dataKey="name" tick={{ fill: '#64748b', fontSize: 12 }} axisLine={false} tickLine={false} />
                        <YAxis tick={{ fill: '#64748b', fontSize: 12 }} axisLine={false} tickLine={false} tickFormatter={(value) => `$${(value / 1000).toFixed(0)}k`} />
                        <Tooltip content={<CustomTooltip />} />
                        <Bar dataKey="p10" name="P10 (Worst)" fill="#fca5a5" radius={[4, 4, 0, 0]} />
                        <Bar dataKey="p50" name="P50 (Expected)" fill={CHART_COLORS.profit} radius={[4, 4, 0, 0]} />
                        <Bar dataKey="p90" name="P90 (Best)" fill="#86efac" radius={[4, 4, 0, 0]} />
                      </BarChart>
                    </ResponsiveContainer>
                  </div>
                </CardContent>
              </Card>
            </TabsContent>

            <TabsContent value="comparison" className="mt-4">
              <Card>
                <CardHeader className="pb-2">
                  <CardTitle className="text-base font-medium">Yield & Price Forecast</CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="h-72">
                    <ResponsiveContainer width="100%" height="100%">
                      <AreaChart data={yieldComparisonData} margin={{ top: 20, right: 30, left: 20, bottom: 5 }}>
                        <XAxis dataKey="name" tick={{ fill: '#64748b', fontSize: 12 }} axisLine={false} tickLine={false} />
                        <YAxis tick={{ fill: '#64748b', fontSize: 12 }} axisLine={false} tickLine={false} />
                        <Tooltip />
                        <Area type="monotone" dataKey="yield" name="Yield (bu/acre)" stroke={CHART_COLORS.yield} fill={CHART_COLORS.yield} fillOpacity={0.3} />
                      </AreaChart>
                    </ResponsiveContainer>
                  </div>
                </CardContent>
              </Card>
            </TabsContent>

            <TabsContent value="details" className="mt-4">
              {selectedCrop && (
                <Card>
                  <CardHeader>
                    <CardTitle className="flex items-center gap-2">
                      <Wheat className="w-5 h-5 text-lime-500" />
                      {selectedCrop.crop_name} Details
                    </CardTitle>
                  </CardHeader>
                  <CardContent className="space-y-6">
                    {/* Profit Range */}
                    <div>
                      <h4 className="text-sm font-medium text-slate-600 mb-3">Profit Range</h4>
                      <div className="grid grid-cols-3 gap-4">
                        <div className="text-center p-3 bg-red-50 rounded-lg">
                          <p className="text-xs text-red-600 mb-1">Worst Case (P10)</p>
                          <p className="font-display font-bold text-red-700">${selectedCrop.profit_p10.toLocaleString()}</p>
                        </div>
                        <div className="text-center p-3 bg-emerald-50 rounded-lg">
                          <p className="text-xs text-emerald-600 mb-1">Expected (P50)</p>
                          <p className="font-display font-bold text-emerald-700">${selectedCrop.profit_p50.toLocaleString()}</p>
                        </div>
                        <div className="text-center p-3 bg-green-50 rounded-lg">
                          <p className="text-xs text-green-600 mb-1">Best Case (P90)</p>
                          <p className="font-display font-bold text-green-700">${selectedCrop.profit_p90.toLocaleString()}</p>
                        </div>
                      </div>
                    </div>

                    {/* Metrics */}
                    <div className="grid grid-cols-2 gap-4">
                      <div>
                        <h4 className="text-sm font-medium text-slate-600 mb-2">Yield Forecast</h4>
                        <p className="font-display text-xl font-bold text-emerald-950">{selectedCrop.yield_forecast.toLocaleString()} bu/acre</p>
                      </div>
                      <div>
                        <h4 className="text-sm font-medium text-slate-600 mb-2">Price Forecast</h4>
                        <p className="font-display text-xl font-bold text-emerald-950">${selectedCrop.price_forecast.toFixed(2)}/bu</p>
                      </div>
                      <div>
                        <h4 className="text-sm font-medium text-slate-600 mb-2">Cost per Acre</h4>
                        <p className="font-display text-xl font-bold text-emerald-950">${(selectedCrop.cost_per_acre ?? 0).toLocaleString()}</p>
                        <p className="text-xs text-slate-500">Source: {selectedCrop.cost_source || 'api_or_default'}</p>
                      </div>
                      <div>
                        <h4 className="text-sm font-medium text-slate-600 mb-2">Forecast Source</h4>
                        <p className="font-display text-sm font-bold text-emerald-950">{selectedCrop.forecast_source || 'deterministic_fallback'}</p>
                        <p className="text-xs text-slate-500">Confidence: {((selectedCrop.forecast_confidence ?? 0) * 100).toFixed(0)}%</p>
                      </div>
                    </div>

                    {/* Soil Compatibility */}
                    <div>
                      <h4 className="text-sm font-medium text-slate-600 mb-2 flex items-center gap-2">
                        <Layers className="w-4 h-4" />
                        Soil Compatibility
                      </h4>
                      <div className="flex items-center gap-3 mb-2">
                        <Progress value={selectedCrop.soil_compatibility} className="flex-1 h-2" />
                        <span className="font-display font-bold text-emerald-950">{selectedCrop.soil_compatibility}%</span>
                      </div>
                      <p className="text-sm text-slate-500 leading-relaxed">{selectedCrop.soil_explanation}</p>
                    </div>

                    {/* Risk Assessment */}
                    <div>
                      <h4 className="text-sm font-medium text-slate-600 mb-2 flex items-center gap-2">
                        <AlertTriangle className="w-4 h-4" />
                        Risk Assessment
                      </h4>
                      <div className="flex items-center gap-3">
                        <div className={`px-3 py-1 rounded-full text-sm font-medium ${
                          selectedCrop.risk_level === 'Low' ? 'bg-emerald-100 text-emerald-700' :
                          selectedCrop.risk_level === 'Medium' ? 'bg-amber-100 text-amber-700' :
                          'bg-red-100 text-red-700'
                        }`}>
                          {selectedCrop.risk_level} Risk
                        </div>
                        <span className="text-sm text-slate-500">Score: {selectedCrop.risk_score.toFixed(1)}/100</span>
                      </div>
                    </div>
                  </CardContent>
                </Card>
              )}
            </TabsContent>
          </Tabs>

          {/* Weather & Market Summary */}
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
            <Card className="border-sky-100 bg-gradient-to-br from-sky-50 to-white">
              <CardContent className="p-5">
                <div className="flex items-center gap-3 mb-3">
                  <div className="w-10 h-10 bg-sky-100 rounded-lg flex items-center justify-center">
                    <Cloud className="w-5 h-5 text-sky-600" />
                  </div>
                  <span className="font-medium text-slate-700">Weather Summary</span>
                </div>
                <p className="text-sm text-slate-600 leading-relaxed">{analysis.weather_summary}</p>
              </CardContent>
            </Card>

            <Card className="border-amber-100 bg-gradient-to-br from-amber-50 to-white">
              <CardContent className="p-5">
                <div className="flex items-center gap-3 mb-3">
                  <div className="w-10 h-10 bg-amber-100 rounded-lg flex items-center justify-center">
                    <DollarSign className="w-5 h-5 text-amber-600" />
                  </div>
                  <span className="font-medium text-slate-700">Market Outlook</span>
                </div>
                <p className="text-sm text-slate-600 leading-relaxed">{analysis.market_outlook}</p>
              </CardContent>
            </Card>
          </div>
        </div>
      </div>
    </div>
  );
};

export default ResultsDashboard;
