import { useState, lazy, Suspense } from 'react';
import { 
  SidebarProvider, 
  SidebarInset,
  SidebarTrigger
} from "@/components/ui/sidebar";
import { Toaster } from "@/components/ui/sonner";
import { useDataflow } from './hooks/useDataflow';
import { AppSidebar } from './components/layout/AppSidebar';

// Lazy load pages for code splitting
const Dashboard = lazy(() => import('./pages/Dashboard').then(m => ({ default: m.Dashboard })));
const Pipelines = lazy(() => import('./pages/Pipelines').then(m => ({ default: m.Pipelines })));
const Sources = lazy(() => import('./pages/Sources').then(m => ({ default: m.Sources })));
const Sinks = lazy(() => import('./pages/Sinks').then(m => ({ default: m.Sinks })));
const CronJobs = lazy(() => import('./pages/CronJobs').then(m => ({ default: m.CronJobs })));

type View = 'dashboard' | 'sources' | 'sinks' | 'pipelines' | 'cronjobs';

export default function App() {
  const [activeView, setActiveView] = useState<View>('dashboard');
  
  const {
    stats,
    config,
    history,
    selectedPipeline,
    setSelectedPipeline,
    loading,
    runPipeline,
    deleteItem,
    saveItem
  } = useDataflow();

  const renderView = () => {
    switch (activeView) {
      case 'dashboard':
        return (
          <Dashboard 
            stats={stats}
            history={history}
            pipelines={config.pipelines}
            selectedPipeline={selectedPipeline}
            setSelectedPipeline={setSelectedPipeline}
            loading={loading}
            runPipeline={runPipeline}
          />
        );
      case 'pipelines':
        return (
          <Pipelines 
            pipelines={config.pipelines}
            sources={config.sources}
            sinks={config.sinks}
            onSave={(p) => saveItem('pipelines', p)}
            onDelete={(name) => deleteItem('pipelines', name)}
            onRun={runPipeline}
            loading={loading}
          />
        );
      case 'sources':
        return (
          <Sources 
            sources={config.sources}
            onSave={(s) => saveItem('sources', s)}
            onDelete={(name) => deleteItem('sources', name)}
          />
        );
      case 'sinks':
        return (
          <Sinks 
            sinks={config.sinks}
            onSave={(s) => saveItem('sinks', s)}
            onDelete={(name) => deleteItem('sinks', name)}
          />
        );
      case 'cronjobs':
        return (
          <CronJobs 
            cronjobs={config.cronjobs}
            pipelines={config.pipelines}
            onSave={(c) => saveItem('cronjobs', c)}
            onDelete={(name) => deleteItem('cronjobs', name)}
          />
        );
      default:
        return null;
    }
  };

  return (
    <SidebarProvider defaultOpen={true}>
      <div className="flex min-h-screen w-full bg-background selection:bg-teal-500/30 selection:text-teal-200">
        <AppSidebar activeView={activeView} setActiveView={setActiveView} />
        <SidebarInset className="flex flex-col bg-background/30 backdrop-blur-3xl">
          <header className="flex h-16 items-center border-b border-border/50 px-6 sticky top-0 bg-background/80 backdrop-blur-md z-40">
            <SidebarTrigger className="h-9 w-9 rounded-xl hover:bg-muted/50 transition-colors mr-4" />
            <div className="h-4 w-[1px] bg-border/50 mx-2" />
            <div className="flex items-center gap-2 ml-4">
              <span className="text-[10px] font-black uppercase tracking-[0.2em] text-muted-foreground opacity-40">Current View</span>
              <span className="text-xs font-bold text-teal-400 capitalize tracking-tight px-2 py-1 rounded-md bg-teal-500/5 border border-teal-500/10">
                {activeView === 'cronjobs' ? 'Schedules' : activeView}
              </span>
            </div>
          </header>
          <main className="flex-1 p-6 lg:p-10 max-w-[1600px] mx-auto w-full">
            <Suspense fallback={
              <div className="h-full w-full flex items-center justify-center min-h-[400px]">
                <div className="animate-spin h-8 w-8 border-4 border-teal-500 border-t-transparent rounded-full" />
              </div>
            }>
              {renderView()}
            </Suspense>
          </main>
        </SidebarInset>
        <Toaster position="bottom-right" theme="dark" />
      </div>
    </SidebarProvider>
  );
}
