import React from 'react';
import { HeroSection } from '../components/HeroSection';
import { 
  Cpu, 
  Shield, 
  TrendingUp, 
  Database,
  CheckCircle,
  ArrowRight
} from 'lucide-react';
import { useNavigate } from 'react-router-dom';
import { useAuth } from '../context/AuthContext';
import { Button } from '../components/ui/button';

const howItWorks = [
  {
    step: 1,
    title: 'Input Farm Data',
    description: 'Select your location, enter acreage, soil type, and farming preferences',
    icon: Database,
  },
  {
    step: 2,
    title: 'AI Analysis',
    description: 'Our agent processes USDA, NOAA, and market data in real-time',
    icon: Cpu,
  },
  {
    step: 3,
    title: 'Get Recommendations',
    description: 'Receive ranked crop suggestions with profit projections',
    icon: TrendingUp,
  },
];

const benefits = [
  'Integration with 5+ federal agricultural data sources',
  '30-year climate history analysis',
  'Monte Carlo profit simulations',
  'Risk-adjusted crop rankings',
  'Soil compatibility scoring',
  'Market price forecasting',
];

export const HomePage = () => {
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
    <div className="min-h-screen bg-mesh-gradient" data-testid="home-page">
      <HeroSection />

      {/* How It Works Section */}
      <section id="features" className="py-20 lg:py-28 bg-slate-50" data-testid="how-it-works-section">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="text-center mb-16">
            <h2 className="font-display text-3xl md:text-4xl font-bold text-emerald-950 mb-4">
              How It Works
            </h2>
            <p className="text-lg text-slate-600 max-w-2xl mx-auto">
              Get data-driven crop recommendations in three simple steps
            </p>
          </div>

          <div className="grid md:grid-cols-3 gap-8">
            {howItWorks.map((item, index) => (
              <div 
                key={item.step}
                className="relative bg-white rounded-2xl p-8 border border-slate-100 shadow-sm hover:shadow-md transition-shadow"
                data-testid={`how-it-works-step-${item.step}`}
              >
                <div className="absolute -top-4 left-8">
                  <div className="w-8 h-8 bg-emerald-950 text-white rounded-lg flex items-center justify-center font-display font-bold">
                    {item.step}
                  </div>
                </div>
                <div className="mt-4">
                  <div className="w-14 h-14 bg-lime-50 rounded-xl flex items-center justify-center mb-6">
                    <item.icon className="w-7 h-7 text-lime-600" />
                  </div>
                  <h3 className="font-display text-xl font-semibold text-emerald-950 mb-3">
                    {item.title}
                  </h3>
                  <p className="text-slate-600 leading-relaxed">
                    {item.description}
                  </p>
                </div>
                {index < howItWorks.length - 1 && (
                  <div className="hidden md:block absolute top-1/2 -right-4 transform -translate-y-1/2">
                    <ArrowRight className="w-6 h-6 text-slate-300" />
                  </div>
                )}
              </div>
            ))}
          </div>
        </div>
      </section>

      {/* Benefits Section */}
      <section className="py-20 lg:py-28" data-testid="benefits-section">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="grid lg:grid-cols-2 gap-12 items-center">
            <div>
              <h2 className="font-display text-3xl md:text-4xl font-bold text-emerald-950 mb-6">
                Powered by Agricultural Intelligence
              </h2>
              <p className="text-lg text-slate-600 mb-8 leading-relaxed">
                Our AI agent combines multiple federal data sources to provide the most 
                comprehensive crop profit analysis available to farmers.
              </p>

              <div className="grid sm:grid-cols-2 gap-4">
                {benefits.map((benefit, index) => (
                  <div 
                    key={index}
                    className="flex items-start gap-3"
                    data-testid={`benefit-${index}`}
                  >
                    <CheckCircle className="w-5 h-5 text-lime-500 flex-shrink-0 mt-0.5" />
                    <span className="text-slate-700">{benefit}</span>
                  </div>
                ))}
              </div>

              <div className="mt-10">
                <Button 
                  onClick={handleCTA}
                  className="bg-emerald-950 hover:bg-emerald-900 text-white rounded-lg px-8 py-6 text-base font-semibold shadow-lg shadow-emerald-900/20"
                  data-testid="benefits-cta-btn"
                >
                  Start Your Analysis
                  <ArrowRight className="w-5 h-5 ml-2" />
                </Button>
              </div>
            </div>

            <div className="relative">
              <div className="bg-white rounded-2xl shadow-xl p-6 border border-slate-100">
                <img 
                  src="https://images.unsplash.com/photo-1592209176240-e5b170e8474c?crop=entropy&cs=srgb&fm=jpg&q=85&w=600" 
                  alt="Farmer using tablet for crop analysis"
                  className="rounded-xl w-full h-auto"
                />
              </div>
              <div className="absolute -bottom-6 -left-6 bg-lime-500 text-white p-4 rounded-xl shadow-lg">
                <div className="flex items-center gap-3">
                  <Shield className="w-8 h-8" />
                  <div>
                    <p className="font-display font-bold text-lg">Data-Driven</p>
                    <p className="text-sm text-lime-100">Decisions</p>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>
      </section>

      {/* Footer */}
      <footer className="bg-emerald-950 text-white py-12" data-testid="footer">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="flex flex-col md:flex-row items-center justify-between gap-6">
            <div className="flex items-center gap-3">
              <div className="w-10 h-10 bg-lime-500 rounded-lg flex items-center justify-center">
                <Cpu className="w-5 h-5 text-white" />
              </div>
              <span className="font-display font-bold text-lg">Crop Portfolio Optimizer</span>
            </div>
            <p className="text-slate-400 text-sm">
              © 2026 Crop Portfolio Optimizer. Data sourced from USDA, NOAA, and NRCS.
            </p>
          </div>
        </div>
      </footer>
    </div>
  );
};

export default HomePage;
