import React from 'react';
import { useNavigate } from 'react-router-dom';
import { Button } from './ui/button';
import { useAuth } from '../context/AuthContext';
import { 
  TrendingUp, 
  CloudRain, 
  BarChart3, 
  Shield, 
  ChevronRight,
  Database,
  Cpu
} from 'lucide-react';

const dataSourceBadges = [
  { name: 'USDA NASS', desc: 'Yield Data' },
  { name: 'NOAA', desc: 'Climate History' },
  { name: 'NRCS', desc: 'Soil Data' },
  { name: 'ERS', desc: 'Cost Analysis' },
  { name: 'Market Models', desc: 'Price Baselines' },
];

const features = [
  {
    icon: TrendingUp,
    title: 'Profit Optimization',
    description: 'AI-driven analysis to maximize your farm\'s revenue potential'
  },
  {
    icon: CloudRain,
    title: 'Weather Intelligence',
    description: '30-year climate data integration for accurate forecasting'
  },
  {
    icon: BarChart3,
    title: 'Market Analysis',
    description: 'Real-time commodity pricing and trend predictions'
  },
  {
    icon: Shield,
    title: 'Risk Assessment',
    description: 'Comprehensive risk modeling with probability distributions'
  },
];

export const HeroSection = () => {
  const navigate = useNavigate();
  const { isAuthenticated } = useAuth();

  const handleCTA = () => {
    if (isAuthenticated) {
      navigate('/dashboard');
    } else {
      navigate('/register');
    }
  };

  return (
    <section className="relative overflow-hidden" data-testid="hero-section">
      {/* Background */}
      <div className="absolute inset-0 bg-hero-gradient" />
      <div className="absolute inset-0 bg-[url('data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iNjAiIGhlaWdodD0iNjAiIHZpZXdCb3g9IjAgMCA2MCA2MCIgeG1sbnM9Imh0dHA6Ly93d3cudzMub3JnLzIwMDAvc3ZnIj48ZyBmaWxsPSJub25lIiBmaWxsLXJ1bGU9ImV2ZW5vZGQiPjxnIGZpbGw9IiM4NGNjMTYiIGZpbGwtb3BhY2l0eT0iMC4wNCI+PGNpcmNsZSBjeD0iMzAiIGN5PSIzMCIgcj0iMiIvPjwvZz48L2c+PC9zdmc+')] opacity-50" />
      
      <div className="relative max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-20 lg:py-32">
        <div className="grid lg:grid-cols-2 gap-12 lg:gap-16 items-center">
          {/* Left Content */}
          <div className="stagger-children">
            {/* Badge */}
            <div className="inline-flex items-center gap-2 px-4 py-2 bg-white/80 backdrop-blur border border-lime-200 rounded-full mb-6 animate-fade-in-up" data-testid="hero-badge">
              <Cpu className="w-4 h-4 text-lime-600" />
              <span className="text-sm font-medium text-emerald-950">AI-Powered Agricultural Intelligence</span>
            </div>

            {/* Headline */}
            <h1 className="font-display text-4xl sm:text-5xl lg:text-6xl font-extrabold text-emerald-950 tracking-tight leading-tight mb-6 animate-fade-in-up" data-testid="hero-headline">
              AI-Powered
              <span className="block text-transparent bg-clip-text bg-gradient-to-r from-emerald-600 to-lime-500">
                Crop Profit Optimization
              </span>
            </h1>

            {/* Subheading */}
            <p className="text-lg text-slate-600 leading-relaxed mb-8 max-w-xl animate-fade-in-up" data-testid="hero-subheading">
              Leverage multi-source data intelligence from USDA, NOAA, and NRCS to forecast 
              the most profitable crops for your farm. Make data-driven decisions with confidence.
            </p>

            {/* CTA Buttons */}
            <div className="flex flex-col sm:flex-row gap-4 mb-12 animate-fade-in-up">
              <Button 
                onClick={handleCTA}
                className="bg-emerald-950 hover:bg-emerald-900 text-white rounded-lg px-8 py-6 text-base font-semibold shadow-lg shadow-emerald-900/20 transition-transform active:scale-95"
                data-testid="hero-cta-button"
              >
                Run My Farm Analysis
                <ChevronRight className="w-5 h-5 ml-1" />
              </Button>
              <Button 
                variant="outline"
                onClick={() => document.getElementById('features')?.scrollIntoView({ behavior: 'smooth' })}
                className="border-slate-200 hover:bg-slate-50 rounded-lg px-6 py-6 text-base"
                data-testid="hero-learn-more-button"
              >
                Learn How It Works
              </Button>
            </div>

            {/* Data Sources */}
            <div className="animate-fade-in-up">
              <p className="text-sm font-medium text-slate-500 uppercase tracking-wider mb-3 flex items-center gap-2">
                <Database className="w-4 h-4" />
                Integrated Data Sources
              </p>
              <div className="flex flex-wrap gap-2">
                {dataSourceBadges.map((source) => (
                  <div
                    key={source.name}
                    className="px-3 py-1.5 bg-white border border-slate-200 rounded-lg text-xs font-medium text-slate-700 hover:border-lime-300 transition-colors"
                    data-testid={`data-source-${source.name.toLowerCase().replace(' ', '-')}`}
                  >
                    <span className="font-semibold">{source.name}</span>
                    <span className="text-slate-400 ml-1">· {source.desc}</span>
                  </div>
                ))}
              </div>
            </div>
          </div>

          {/* Right Content - Feature Cards */}
          <div className="grid grid-cols-2 gap-4" data-testid="feature-cards">
            {features.map((feature, index) => (
              <div
                key={feature.title}
                className="bg-white border border-slate-100 rounded-xl p-6 shadow-sm hover:shadow-md hover:border-lime-200/50 transition-all animate-fade-in-up card-hover"
                style={{ animationDelay: `${index * 0.1}s` }}
                data-testid={`feature-card-${index}`}
              >
                <div className="w-10 h-10 bg-lime-50 rounded-lg flex items-center justify-center mb-4">
                  <feature.icon className="w-5 h-5 text-lime-600" />
                </div>
                <h3 className="font-display font-semibold text-emerald-950 mb-2">
                  {feature.title}
                </h3>
                <p className="text-sm text-slate-500 leading-relaxed">
                  {feature.description}
                </p>
              </div>
            ))}
          </div>
        </div>
      </div>

      {/* Bottom Wave */}
      <div className="absolute bottom-0 left-0 right-0">
        <svg viewBox="0 0 1440 100" fill="none" xmlns="http://www.w3.org/2000/svg" className="w-full">
          <path d="M0 100V50C240 16.7 480 0 720 0C960 0 1200 16.7 1440 50V100H0Z" fill="#f8fafc" />
        </svg>
      </div>
    </section>
  );
};

export default HeroSection;
