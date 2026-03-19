import React, { useState, useEffect, useRef } from 'react';
import { Progress } from './ui/progress';
import { 
  Database, 
  Cloud, 
  BarChart3, 
  DollarSign, 
  CheckCircle,
  Cpu,
  Tractor
} from 'lucide-react';

const PROCESSING_STEPS = [
  {
    id: 'data_ingestion',
    title: 'Data Ingestion',
    icon: Database,
    thoughts: [
      'Connecting to USDA NASS database...',
      'Fetching regional yield statistics...',
      'Loading 10-year historical crop data... [OK]',
      'Querying NRCS soil survey API...',
      'Processing soil composition data... [OK]',
      'Connecting to USDA ERS cost database...',
      'Loading production cost estimates... [OK]',
    ]
  },
  {
    id: 'weather_analysis',
    title: 'Weather Analysis',
    icon: Cloud,
    thoughts: [
      'Accessing NOAA climate data servers...',
      'Downloading 30-year precipitation records...',
      'Analyzing seasonal temperature patterns... [OK]',
      'Calculating frost-free growing days...',
      'Processing drought probability indices... [OK]',
      'Evaluating extreme weather risks...',
      'Climate analysis complete... [OK]',
    ]
  },
  {
    id: 'yield_modeling',
    title: 'Yield Modeling',
    icon: BarChart3,
    thoughts: [
      'Initializing crop yield prediction model...',
      'Adjusting for soil pH compatibility...',
      'Factoring irrigation availability...',
      'Running Monte Carlo simulations... (1000 iterations)',
      'Calculating yield distributions... [OK]',
      'Validating predictions against historical data...',
      'Yield models calibrated... [OK]',
    ]
  },
  {
    id: 'market_forecast',
    title: 'Market Forecasting',
    icon: DollarSign,
    thoughts: [
      'Building commodity price baselines...',
      'Applying crop-specific market priors...',
      'Analyzing futures market trends... [OK]',
      'Processing export demand indicators...',
      'Evaluating supply chain factors...',
      'Calculating price volatility indices... [OK]',
      'Market forecast complete... [OK]',
    ]
  },
  {
    id: 'profit_simulation',
    title: 'Profit Simulation',
    icon: CheckCircle,
    thoughts: [
      'Combining yield and price models...',
      'Calculating production cost scenarios...',
      'Running profit distribution analysis...',
      'Computing risk-adjusted returns... [OK]',
      'Generating P10/P50/P90 estimates...',
      'Ranking crops by expected profit...',
      'Optimization complete... [OK]',
    ]
  },
];

export const ProcessingScreen = ({ farmProfile, onComplete }) => {
  const [currentStepIndex, setCurrentStepIndex] = useState(0);
  const [stepProgress, setStepProgress] = useState(0);
  const [logs, setLogs] = useState([]);
  const [completedSteps, setCompletedSteps] = useState([]);
  const logsEndRef = useRef(null);
  const thoughtIndexRef = useRef(0);

  useEffect(() => {
    logsEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [logs]);

  useEffect(() => {
    const currentStep = PROCESSING_STEPS[currentStepIndex];
    if (!currentStep) {
      // All steps complete
      setTimeout(() => {
        onComplete();
      }, 1000);
      return;
    }

    const thoughts = currentStep.thoughts;
    thoughtIndexRef.current = 0;

    const thoughtInterval = setInterval(() => {
      if (thoughtIndexRef.current < thoughts.length) {
        const thought = thoughts[thoughtIndexRef.current];
        setLogs(prev => [...prev, { 
          step: currentStep.title, 
          text: thought, 
          isOk: thought.includes('[OK]') 
        }]);
        setStepProgress(((thoughtIndexRef.current + 1) / thoughts.length) * 100);
        thoughtIndexRef.current++;
      } else {
        clearInterval(thoughtInterval);
        setCompletedSteps(prev => [...prev, currentStep.id]);
        
        setTimeout(() => {
          setCurrentStepIndex(prev => prev + 1);
          setStepProgress(0);
        }, 500);
      }
    }, 400);

    return () => clearInterval(thoughtInterval);
  }, [currentStepIndex, onComplete]);

  const overallProgress = ((currentStepIndex + stepProgress / 100) / PROCESSING_STEPS.length) * 100;

  return (
    <div className="fixed inset-0 z-50 bg-emerald-950 flex items-center justify-center" data-testid="processing-screen">
      <div className="w-full max-w-4xl px-6 py-8">
        {/* Header */}
        <div className="text-center mb-8">
          <div className="inline-flex items-center gap-3 mb-4">
            <div className="w-12 h-12 bg-lime-500/20 rounded-xl flex items-center justify-center animate-pulse-subtle">
              <Tractor className="w-6 h-6 text-lime-400" />
            </div>
            <h2 className="font-display text-2xl font-bold text-white">
              Analyzing Your Farm
            </h2>
          </div>
          <p className="text-slate-400">
            Processing {farmProfile?.acres || 100} acres at coordinates ({farmProfile?.location?.lat?.toFixed(4) || '39.8283'}, {farmProfile?.location?.lng?.toFixed(4) || '-98.5795'})
          </p>
        </div>

        {/* Overall Progress */}
        <div className="mb-8">
          <div className="flex items-center justify-between mb-2">
            <span className="text-sm text-slate-400">Overall Progress</span>
            <span className="text-sm font-mono text-lime-400">{Math.round(overallProgress)}%</span>
          </div>
          <Progress value={overallProgress} className="h-2 bg-slate-800" />
        </div>

        {/* Steps */}
        <div className="grid grid-cols-5 gap-2 mb-8">
          {PROCESSING_STEPS.map((step, index) => {
            const isCompleted = completedSteps.includes(step.id);
            const isCurrent = index === currentStepIndex;
            const Icon = step.icon;

            return (
              <div
                key={step.id}
                className={`p-3 rounded-lg text-center transition-all ${
                  isCompleted
                    ? 'bg-lime-500/20 border border-lime-500/30'
                    : isCurrent
                    ? 'bg-slate-800 border border-slate-700'
                    : 'bg-slate-900/50 border border-slate-800'
                }`}
                data-testid={`processing-step-${step.id}`}
              >
                <Icon className={`w-5 h-5 mx-auto mb-1 ${
                  isCompleted ? 'text-lime-400' : isCurrent ? 'text-white' : 'text-slate-600'
                }`} />
                <span className={`text-xs font-medium ${
                  isCompleted ? 'text-lime-400' : isCurrent ? 'text-white' : 'text-slate-600'
                }`}>
                  {step.title}
                </span>
              </div>
            );
          })}
        </div>

        {/* Terminal/Log Window */}
        <div className="bg-slate-900 rounded-xl border border-slate-800 overflow-hidden" data-testid="agent-terminal">
          <div className="px-4 py-2 bg-slate-800 border-b border-slate-700 flex items-center gap-2">
            <Cpu className="w-4 h-4 text-lime-400" />
            <span className="text-sm font-mono text-slate-300">Farm Analysis Agent</span>
            <div className="ml-auto flex gap-1.5">
              <div className="w-3 h-3 rounded-full bg-red-500" />
              <div className="w-3 h-3 rounded-full bg-yellow-500" />
              <div className="w-3 h-3 rounded-full bg-green-500" />
            </div>
          </div>

          <div className="h-64 overflow-y-auto p-4 font-mono text-sm" data-testid="agent-logs">
            {logs.map((log, index) => (
              <div
                key={index}
                className="mb-1 animate-fade-in-up"
                style={{ animationDelay: `${index * 0.05}s` }}
              >
                <span className="text-slate-500">[{log.step}]</span>{' '}
                <span className={log.isOk ? 'text-lime-400' : 'text-slate-300'}>
                  {log.text}
                </span>
              </div>
            ))}
            {currentStepIndex < PROCESSING_STEPS.length && (
              <div className="text-lime-400 terminal-cursor">
                <span className="text-slate-500">{'>'}</span>{' '}
              </div>
            )}
            <div ref={logsEndRef} />
          </div>
        </div>

        {/* Current Step Progress */}
        {currentStepIndex < PROCESSING_STEPS.length && (
          <div className="mt-4">
            <div className="flex items-center justify-between mb-2">
              <span className="text-sm text-slate-400">
                {PROCESSING_STEPS[currentStepIndex]?.title}
              </span>
              <span className="text-sm font-mono text-lime-400">{Math.round(stepProgress)}%</span>
            </div>
            <Progress value={stepProgress} className="h-1.5 bg-slate-800" />
          </div>
        )}
      </div>
    </div>
  );
};

export default ProcessingScreen;
