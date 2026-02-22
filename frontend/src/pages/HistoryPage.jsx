import React, { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { useAuth } from '../context/AuthContext';
import { Header } from '../components/Header';
import { ResultsDashboard } from '../components/ResultsDashboard';
import { Card, CardContent } from '../components/ui/card';
import { Button } from '../components/ui/button';
import { api } from '../lib/api';
import { 
  History, 
  Sprout, 
  MapPin, 
  Layers, 
  Calendar,
  ArrowLeft,
  Plus,
  Loader2
} from 'lucide-react';

export const HistoryPage = () => {
  const { user, loading: authLoading } = useAuth();
  const navigate = useNavigate();
  const [analyses, setAnalyses] = useState([]);
  const [loading, setLoading] = useState(true);
  const [selectedAnalysis, setSelectedAnalysis] = useState(null);

  useEffect(() => {
    if (!authLoading && !user) {
      navigate('/login');
    }
  }, [user, authLoading, navigate]);

  useEffect(() => {
    const fetchAnalyses = async () => {
      try {
        const data = await api.getAnalyses();
        setAnalyses(data);
      } catch (error) {
        console.error('Failed to fetch analyses:', error);
      } finally {
        setLoading(false);
      }
    };

    if (user) {
      fetchAnalyses();
    }
  }, [user]);

  if (authLoading || loading) {
    return (
      <div className="min-h-screen bg-mesh-gradient flex items-center justify-center">
        <Loader2 className="w-8 h-8 animate-spin text-emerald-600" />
      </div>
    );
  }

  if (selectedAnalysis) {
    return (
      <div className="min-h-screen bg-mesh-gradient">
        <Header />
        <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
          <Button 
            variant="ghost" 
            className="mb-6 gap-2"
            onClick={() => setSelectedAnalysis(null)}
            data-testid="back-to-history-btn"
          >
            <ArrowLeft className="w-4 h-4" />
            Back to History
          </Button>
          <ResultsDashboard 
            analysis={selectedAnalysis} 
            onNewAnalysis={() => navigate('/dashboard')} 
          />
        </main>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-mesh-gradient" data-testid="history-page">
      <Header />
      
      <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        {/* Header */}
        <div className="flex items-center justify-between mb-8">
          <div>
            <h1 className="font-display text-2xl font-bold text-emerald-950 mb-2 flex items-center gap-2">
              <History className="w-6 h-6 text-slate-400" />
              Analysis History
            </h1>
            <p className="text-slate-500">
              View and compare your past crop analyses
            </p>
          </div>
          <Button 
            onClick={() => navigate('/dashboard')}
            className="bg-emerald-950 hover:bg-emerald-900 text-white gap-2"
            data-testid="new-analysis-btn"
          >
            <Plus className="w-4 h-4" />
            New Analysis
          </Button>
        </div>

        {/* Analyses List */}
        {analyses.length === 0 ? (
          <Card className="border-slate-200">
            <CardContent className="p-12 text-center">
              <div className="w-16 h-16 bg-slate-100 rounded-2xl flex items-center justify-center mx-auto mb-4">
                <History className="w-8 h-8 text-slate-400" />
              </div>
              <h3 className="font-display text-xl font-semibold text-emerald-950 mb-2">
                No Analyses Yet
              </h3>
              <p className="text-slate-500 mb-6 max-w-md mx-auto">
                Run your first farm analysis to get AI-powered crop recommendations
              </p>
              <Button 
                onClick={() => navigate('/dashboard')}
                className="bg-emerald-950 hover:bg-emerald-900 text-white"
              >
                Start Your First Analysis
              </Button>
            </CardContent>
          </Card>
        ) : (
          <div className="space-y-4">
            {analyses.map((analysis) => (
              <Card 
                key={analysis.id}
                className="border-slate-200 hover:border-slate-300 cursor-pointer transition-colors"
                onClick={() => setSelectedAnalysis(analysis)}
                data-testid={`history-item-${analysis.id}`}
              >
                <CardContent className="p-6">
                  <div className="flex flex-col md:flex-row md:items-center md:justify-between gap-4">
                    {/* Left: Basic Info */}
                    <div className="flex items-start gap-4">
                      <div className="w-12 h-12 bg-lime-50 rounded-xl flex items-center justify-center flex-shrink-0">
                        <Sprout className="w-6 h-6 text-lime-600" />
                      </div>
                      <div>
                        <h3 className="font-display font-semibold text-lg text-emerald-950 mb-1">
                          {analysis.results?.[0]?.crop_name || 'Farm Analysis'}
                          <span className="ml-2 text-sm font-normal text-slate-400">
                            (Top Recommendation)
                          </span>
                        </h3>
                        <div className="flex flex-wrap items-center gap-4 text-sm text-slate-500">
                          <span className="flex items-center gap-1">
                            <Calendar className="w-4 h-4" />
                            {new Date(analysis.created_at).toLocaleDateString('en-US', {
                              year: 'numeric',
                              month: 'short',
                              day: 'numeric'
                            })}
                          </span>
                          <span className="flex items-center gap-1">
                            <MapPin className="w-4 h-4" />
                            {analysis.farm_profile.acres} acres
                          </span>
                          <span className="flex items-center gap-1">
                            <Layers className="w-4 h-4" />
                            {analysis.farm_profile.soil_type}
                          </span>
                        </div>
                      </div>
                    </div>

                    {/* Right: Profit Info */}
                    <div className="flex items-center gap-6 md:border-l md:border-slate-100 md:pl-6">
                      <div className="text-right">
                        <p className="text-sm text-slate-400 mb-1">Expected Profit</p>
                        <p className="font-display text-xl font-bold text-emerald-600">
                          ${analysis.results?.[0]?.expected_profit?.toLocaleString() || '0'}
                        </p>
                      </div>
                      <div className={`px-3 py-1 rounded-full text-sm font-medium ${
                        analysis.results?.[0]?.risk_level === 'Low' 
                          ? 'bg-emerald-100 text-emerald-700'
                          : analysis.results?.[0]?.risk_level === 'Medium'
                          ? 'bg-amber-100 text-amber-700'
                          : 'bg-red-100 text-red-700'
                      }`}>
                        {analysis.results?.[0]?.risk_level || 'N/A'} Risk
                      </div>
                    </div>
                  </div>

                  {/* Crops Preview */}
                  <div className="mt-4 pt-4 border-t border-slate-100">
                    <p className="text-xs text-slate-400 uppercase tracking-wider mb-2">
                      All Recommendations
                    </p>
                    <div className="flex flex-wrap gap-2">
                      {analysis.results?.slice(0, 6).map((crop, index) => (
                        <span 
                          key={crop.crop_name}
                          className={`px-3 py-1 rounded-full text-sm ${
                            index === 0 
                              ? 'bg-lime-100 text-lime-700 font-medium'
                              : 'bg-slate-100 text-slate-600'
                          }`}
                        >
                          #{index + 1} {crop.crop_name}
                        </span>
                      ))}
                    </div>
                  </div>
                </CardContent>
              </Card>
            ))}
          </div>
        )}
      </main>
    </div>
  );
};

export default HistoryPage;
