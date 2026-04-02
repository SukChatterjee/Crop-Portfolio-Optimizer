import React, { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { useAuth } from '../context/AuthContext';
import { Header } from '../components/Header';
import { FarmWizard } from '../components/FarmWizard';
import { ProcessingScreen } from '../components/ProcessingScreen';
import { ResultsDashboard } from '../components/ResultsDashboard';
import { Button } from '../components/ui/button';
import { Card, CardContent } from '../components/ui/card';
import { api } from '../lib/api';
import { toast } from 'sonner';
import { 
  Plus, 
  History, 
  Sprout, 
  TrendingUp,
  MapPin,
  Layers
} from 'lucide-react';

export const DashboardPage = () => {
  const { user, loading: authLoading } = useAuth();
  const navigate = useNavigate();
  const [showWizard, setShowWizard] = useState(false);
  const [processing, setProcessing] = useState(false);
  const [currentAnalysis, setCurrentAnalysis] = useState(null);
  const [farmProfile, setFarmProfile] = useState(null);
  const [analysisJob, setAnalysisJob] = useState(null);
  const [recentAnalyses, setRecentAnalyses] = useState([]);
  const [loadingHistory, setLoadingHistory] = useState(true);

  useEffect(() => {
    if (!authLoading && !user) {
      navigate('/login');
    }
  }, [user, authLoading, navigate]);

  useEffect(() => {
    const fetchRecentAnalyses = async () => {
      try {
        const analyses = await api.getAnalyses();
        setRecentAnalyses(analyses.slice(0, 3));
      } catch (error) {
        console.error('Failed to fetch analyses:', error);
      } finally {
        setLoadingHistory(false);
      }
    };

    if (user) {
      fetchRecentAnalyses();
    }
  }, [user]);

  useEffect(() => {
    if (!processing || !analysisJob?.job_id) {
      return undefined;
    }

    let cancelled = false;
    const poll = async () => {
      try {
        const nextJob = await api.getAnalysisJob(analysisJob.job_id);
        if (cancelled) {
          return;
        }
        setAnalysisJob(nextJob);
        if (nextJob.status === 'completed' && nextJob.result) {
          setCurrentAnalysis(nextJob.result);
          setProcessing(false);
          setAnalysisJob(null);
          toast.success('Analysis complete!');
          return;
        }
        if (nextJob.status === 'failed') {
          setProcessing(false);
          setAnalysisJob(null);
          toast.error(nextJob.error || 'Analysis failed. Please try again.');
          return;
        }
      } catch (error) {
        if (!cancelled) {
          console.error('Analysis polling failed:', error);
          setProcessing(false);
          setAnalysisJob(null);
          toast.error('Analysis failed. Please try again.');
        }
      }
    };

    poll();
    const intervalId = setInterval(poll, 1000);
    return () => {
      cancelled = true;
      clearInterval(intervalId);
    };
  }, [processing, analysisJob?.job_id]);

  const handleWizardComplete = async (profile) => {
    setFarmProfile(profile);
    setShowWizard(false);
    setProcessing(true);
    try {
      const job = await api.startAnalysis(profile);
      setAnalysisJob(job);
    } catch (error) {
      console.error('Analysis failed:', error);
      toast.error('Analysis failed. Please try again.');
      setProcessing(false);
    }
  };

  const handleNewAnalysis = () => {
    setCurrentAnalysis(null);
    setShowWizard(true);
  };

  const handleViewAnalysis = (analysis) => {
    setCurrentAnalysis(analysis);
  };

  if (authLoading) {
    return (
      <div className="min-h-screen bg-mesh-gradient flex items-center justify-center">
        <div className="animate-pulse text-slate-500">Loading...</div>
      </div>
    );
  }

  // Processing Screen
  if (processing) {
    return <ProcessingScreen farmProfile={farmProfile} analysisJob={analysisJob} />;
  }

  // Results View
  if (currentAnalysis) {
    return (
      <div className="min-h-screen bg-mesh-gradient">
        <Header />
        <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
          <ResultsDashboard analysis={currentAnalysis} onNewAnalysis={handleNewAnalysis} />
        </main>
      </div>
    );
  }

  // Dashboard Home
  return (
    <div className="min-h-screen bg-mesh-gradient" data-testid="dashboard-page">
      <Header />
      
      <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        {/* Welcome Section */}
        <div className="mb-8">
          <h1 className="font-display text-2xl font-bold text-emerald-950 mb-2">
            Welcome back, {user?.name?.split(' ')[0]}!
          </h1>
          <p className="text-slate-500">Ready to optimize your crop portfolio?</p>
        </div>

        {/* Action Cards */}
        <div className="grid md:grid-cols-2 gap-6 mb-10">
          {/* New Analysis Card */}
          <Card className="border-2 border-dashed border-emerald-200 bg-gradient-to-br from-emerald-50 to-white hover:border-emerald-400 transition-colors cursor-pointer group"
                onClick={() => setShowWizard(true)}
                data-testid="new-analysis-card">
            <CardContent className="p-8 flex flex-col items-center text-center">
              <div className="w-16 h-16 bg-emerald-100 rounded-2xl flex items-center justify-center mb-4 group-hover:scale-110 transition-transform">
                <Plus className="w-8 h-8 text-emerald-600" />
              </div>
              <h3 className="font-display text-xl font-semibold text-emerald-950 mb-2">
                New Farm Analysis
              </h3>
              <p className="text-slate-500 mb-4">
                Get AI-powered crop recommendations based on your farm's profile
              </p>
              <Button className="bg-emerald-950 hover:bg-emerald-900 text-white" data-testid="start-analysis-btn">
                Run My Farm Analysis
              </Button>
            </CardContent>
          </Card>

          {/* Quick Stats Card */}
          <Card className="border-slate-200">
            <CardContent className="p-8">
              <div className="flex items-center gap-3 mb-6">
                <div className="w-10 h-10 bg-lime-50 rounded-lg flex items-center justify-center">
                  <TrendingUp className="w-5 h-5 text-lime-600" />
                </div>
                <h3 className="font-display text-lg font-semibold text-emerald-950">
                  Your Analysis Stats
                </h3>
              </div>

              <div className="grid grid-cols-2 gap-4">
                <div className="bg-slate-50 rounded-lg p-4">
                  <p className="text-2xl font-display font-bold text-emerald-950">
                    {recentAnalyses.length}
                  </p>
                  <p className="text-sm text-slate-500">Total Analyses</p>
                </div>
                <div className="bg-slate-50 rounded-lg p-4">
                  <p className="text-2xl font-display font-bold text-emerald-950">
                    {recentAnalyses.length > 0 
                      ? recentAnalyses[0]?.results?.[0]?.crop_name || 'N/A'
                      : 'N/A'}
                  </p>
                  <p className="text-sm text-slate-500">Last Recommendation</p>
                </div>
              </div>

              <Button 
                variant="outline" 
                className="w-full mt-4 gap-2"
                onClick={() => navigate('/history')}
                data-testid="view-history-btn"
              >
                <History className="w-4 h-4" />
                View Full History
              </Button>
            </CardContent>
          </Card>
        </div>

        {/* Recent Analyses */}
        {recentAnalyses.length > 0 && (
          <div>
            <h2 className="font-display text-lg font-semibold text-emerald-950 mb-4 flex items-center gap-2">
              <History className="w-5 h-5 text-slate-400" />
              Recent Analyses
            </h2>
            <div className="grid md:grid-cols-3 gap-4">
              {recentAnalyses.map((analysis) => (
                <Card 
                  key={analysis.id}
                  className="border-slate-200 hover:border-slate-300 cursor-pointer transition-colors"
                  onClick={() => handleViewAnalysis(analysis)}
                  data-testid={`recent-analysis-${analysis.id}`}
                >
                  <CardContent className="p-5">
                    <div className="flex items-start justify-between mb-3">
                      <div className="w-10 h-10 bg-lime-50 rounded-lg flex items-center justify-center">
                        <Sprout className="w-5 h-5 text-lime-600" />
                      </div>
                      <span className="text-xs text-slate-400">
                        {new Date(analysis.created_at).toLocaleDateString()}
                      </span>
                    </div>
                    
                    <h3 className="font-display font-semibold text-emerald-950 mb-1">
                      {analysis.results?.[0]?.crop_name || 'Analysis'}
                    </h3>
                    
                    <div className="flex items-center gap-4 text-sm text-slate-500">
                      <span className="flex items-center gap-1">
                        <MapPin className="w-3 h-3" />
                        {analysis.farm_profile.acres} acres
                      </span>
                      <span className="flex items-center gap-1">
                        <Layers className="w-3 h-3" />
                        {analysis.farm_profile.soil_type}
                      </span>
                    </div>

                    <div className="mt-3 pt-3 border-t border-slate-100">
                      <span className="text-emerald-600 font-medium">
                        ${analysis.results?.[0]?.expected_profit?.toLocaleString() || '0'}
                      </span>
                      <span className="text-sm text-slate-400 ml-1">expected profit</span>
                    </div>
                  </CardContent>
                </Card>
              ))}
            </div>
          </div>
        )}
      </main>

      {/* Wizard Modal */}
      {showWizard && (
        <FarmWizard 
          onComplete={handleWizardComplete}
          onCancel={() => setShowWizard(false)}
        />
      )}
    </div>
  );
};

export default DashboardPage;
