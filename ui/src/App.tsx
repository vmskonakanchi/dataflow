import { useState, useEffect } from 'react';
import { 
  BarChart3, 
  Play, 
  ArrowRightLeft,
  ChevronRight,
  Plus,
  Trash2,
  Edit2,
  HardDrive,
  Cloud,
  Layers,
  Clock,
  Zap,
  Database
} from "lucide-react";

import { 
  Card, 
  CardContent, 
  CardHeader, 
  CardTitle 
} from "@/components/ui/card";
import { 
  Table, 
  TableBody, 
  TableCell, 
  TableHead, 
  TableHeader, 
  TableRow 
} from "@/components/ui/table";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import {
  Sidebar,
  SidebarContent,
  SidebarGroup,
  SidebarGroupContent,
  SidebarGroupLabel,
  SidebarHeader,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
  SidebarProvider,
  SidebarTrigger,
  SidebarInset
} from "@/components/ui/sidebar";
import { Input } from "@/components/ui/input";
import { 
  Dialog, 
  DialogContent, 
  DialogDescription, 
  DialogFooter, 
  DialogHeader, 
  DialogTitle, 
  DialogTrigger 
} from "@/components/ui/dialog";
import { Label } from "@/components/ui/label";
import { 
  Select, 
  SelectContent, 
  SelectItem, 
  SelectTrigger, 
  SelectValue 
} from "@/components/ui/select";
import { Toaster } from "@/components/ui/sonner";
import { toast } from "sonner";

const API_BASE = "http://localhost:8000";

interface RunHistory {
  id: number;
  pipeline_name: string;
  status: string;
  started_at: string;
  finished_at: string | null;
  rows_extracted: number | null;
  rows_written: number | null;
  error_message: string | null;
}

interface Stats {
  total_runs: number;
  success: number;
  failed: number;
  started: number;
}

interface Pipeline {
  name: string;
  description?: string;
  source: string;
  source_query: string;
  sink: string;
  sink_table: string;
  sink_mode: 'append' | 'upsert' | 'replace';
  sink_key?: string;
  batch_size?: number;
  transforms: any[];
  alerts: {
    on_failure: 'email' | 'none';
    email?: string;
    on_row_count_below?: number;
  };
  originalName?: string;
}

interface Cronjob {
  name: string;
  pipeline: string;
  schedule: string;
  timezone: string;
  enabled: boolean;
  retry: {
    max_attempts: number;
    delay_seconds: number;
  };
  originalName?: string;
}

interface Source {
  name: string;
  type: string;
  host?: string;
  port?: number;
  database?: string;
  file_path?: string;
  url?: string;
  query?: string;
  bucket?: string;
  key?: string;
  region?: string;
  originalName?: string;
}

interface Sink {
  name: string;
  type: string;
  file_path?: string;
  bucket?: string;
  key?: string;
  region?: string;
  host?: string;
  port?: number;
  database?: string;
  table_name?: string;
  originalName?: string;
}

export default function App() {
  const [stats, setStats] = useState<Stats>({ total_runs: 0, success: 0, failed: 0, started: 0 });
  const [pipelines, setPipelines] = useState<Pipeline[]>([]);
  const [history, setHistory] = useState<RunHistory[]>([]);
  const [selectedPipeline, setSelectedPipeline] = useState<string | null>(null);
  const [loading, setLoading] = useState<string | null>(null);
  const [activeView, setActiveView] = useState<'dashboard' | 'sources' | 'sinks' | 'pipelines' | 'cronjobs'>('dashboard');
  const [config, setConfig] = useState<{sources: Source[], sinks: Sink[], pipelines: Pipeline[], cronjobs: Cronjob[]}>({sources: [], sinks: [], pipelines: [], cronjobs: []});

  // Modal states
  const [sourceEditing, setSourceEditing] = useState<Source | null>(null);
  const [sinkEditing, setSinkEditing] = useState<Sink | null>(null);
  const [pipelineEditing, setPipelineEditing] = useState<Pipeline | null>(null);
  const [cronjobEditing, setCronjobEditing] = useState<Cronjob | null>(null);
  const [isSourceModalOpen, setIsSourceModalOpen] = useState(false);
  const [isSinkModalOpen, setIsSinkModalOpen] = useState(false);
  const [isPipelineModalOpen, setIsPipelineModalOpen] = useState(false);
  const [isCronjobModalOpen, setIsCronjobModalOpen] = useState(false);

  useEffect(() => {
    fetchStats();
    fetchConfig();
  }, []);

  const fetchStats = async () => {
    try {
      const res = await fetch(`${API_BASE}/stats`);
      const data = await res.json();
      setStats(data);
    } catch (e) { console.error(e); }
  };

  const fetchConfig = async () => {
    try {
      const res = await fetch(`${API_BASE}/config`);
      const data = await res.json();
      setPipelines(data.pipelines);
      setConfig({ 
        sources: data.sources, 
        sinks: data.sinks, 
        pipelines: data.pipelines, 
        cronjobs: data.cronjobs 
      });
      if (data.pipelines.length > 0 && !selectedPipeline) {
        setSelectedPipeline(data.pipelines[0].name);
        fetchHistory(data.pipelines[0].name);
      }
    } catch (e) { console.error(e); }
  };

  const fetchHistory = async (name: string) => {
    try {
      const res = await fetch(`${API_BASE}/pipelines/${name}/history`);
      if (res.ok) {
        const data = await res.json();
        if (Array.isArray(data)) {
          setHistory(data);
        } else {
          setHistory([]);
          console.error("History data is not an array:", data);
        }
      } else {
        setHistory([]);
      }
    } catch (e) { 
      console.error(e); 
      setHistory([]);
    }
  };

  const runPipeline = async (name: string) => {
    setLoading(name);
    try {
      await fetch(`${API_BASE}/pipelines/${name}/run`, { method: 'POST' });
      toast.success(`Started pipeline: ${name}`);
      setTimeout(() => {
        fetchStats();
        fetchHistory(name);
        setLoading(null);
      }, 2000);
    } catch (e) {
      console.error(e);
      setLoading(null);
      toast.error("Failed to trigger pipeline");
    }
  };

  const handleSaveSource = async () => {
    if (!sourceEditing) return;
    const method = config.sources.find(s => s.name === sourceEditing.originalName) ? 'PUT' : 'POST';
    const url = method === 'PUT' ? `${API_BASE}/sources/${sourceEditing.originalName}` : `${API_BASE}/sources`;
    
    try {
      const res = await fetch(url, {
        method,
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(sourceEditing)
      });
      if (res.ok) {
        toast.success(`Source ${method === 'PUT' ? 'updated' : 'added'} successfully`);
        setIsSourceModalOpen(false);
        fetchConfig();
      } else {
        toast.error("Error saving source");
      }
    } catch (e) {
      toast.error("Network error");
    }
  };

  const handleDeleteSource = async (name: string) => {
    if (!confirm(`Delete source ${name}?`)) return;
    try {
      const res = await fetch(`${API_BASE}/sources/${name}`, { method: 'DELETE' });
      if (res.ok) {
        toast.success("Source deleted");
        fetchConfig();
      }
    } catch (e) { toast.error("Error deleting source"); }
  };

  const handleSaveSink = async () => {
    if (!sinkEditing) return;
    const method = config.sinks.find(s => s.name === sinkEditing.originalName) ? 'PUT' : 'POST';
    const url = method === 'PUT' ? `${API_BASE}/sinks/${sinkEditing.originalName}` : `${API_BASE}/sinks`;
    
    try {
      const res = await fetch(url, {
        method,
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(sinkEditing)
      });
      if (res.ok) {
        toast.success(`Sink ${method === 'PUT' ? 'updated' : 'added'} successfully`);
        setIsSinkModalOpen(false);
        fetchConfig();
      } else {
        toast.error("Error saving sink");
      }
    } catch (e) {
      toast.error("Network error");
    }
  };

  const handleDeleteSink = async (name: string) => {
    if (!confirm(`Delete sink ${name}?`)) return;
    try {
      const res = await fetch(`${API_BASE}/sinks/${name}`, { method: 'DELETE' });
      if (res.ok) {
        toast.success("Sink deleted");
        fetchConfig();
      }
    } catch (e) { toast.error("Error deleting sink"); }
  };

  const handleSavePipeline = async () => {
    if (!pipelineEditing) return;
    const isEdit = config.pipelines.find(p => p.name === pipelineEditing.originalName);
    const method = isEdit ? 'PUT' : 'POST';
    const url = isEdit ? `${API_BASE}/pipelines/${pipelineEditing.originalName}` : `${API_BASE}/pipelines`;
    
    try {
      const res = await fetch(url, {
        method,
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(pipelineEditing)
      });
      if (res.ok) {
        toast.success(`Pipeline ${isEdit ? 'updated' : 'added'} successfully`);
        setIsPipelineModalOpen(false);
        fetchConfig();
      } else {
        toast.error("Error saving pipeline");
      }
    } catch (e) { toast.error("Network error"); }
  };

  const handleDeletePipeline = async (name: string) => {
    if (!confirm(`Delete pipeline ${name}?`)) return;
    try {
      const res = await fetch(`${API_BASE}/pipelines/${name}`, { method: 'DELETE' });
      if (res.ok) {
        toast.success("Pipeline deleted");
        fetchConfig();
      }
    } catch (e) { toast.error("Error deleting pipeline"); }
  };

  const handleSaveCronjob = async () => {
    if (!cronjobEditing) return;
    const isEdit = config.cronjobs.find(c => c.name === cronjobEditing.originalName);
    const method = isEdit ? 'PUT' : 'POST';
    const url = isEdit ? `${API_BASE}/cronjobs/${cronjobEditing.originalName}` : `${API_BASE}/cronjobs`;
    
    try {
      const res = await fetch(url, {
        method,
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(cronjobEditing)
      });
      if (res.ok) {
        toast.success(`Cronjob ${isEdit ? 'updated' : 'added'} successfully`);
        setIsCronjobModalOpen(false);
        fetchConfig();
      } else {
        toast.error("Error saving cronjob");
      }
    } catch (e) { toast.error("Network error"); }
  };

  const handleDeleteCronjob = async (name: string) => {
    if (!confirm(`Delete cronjob ${name}?`)) return;
    try {
      const res = await fetch(`${API_BASE}/cronjobs/${name}`, { method: 'DELETE' });
      if (res.ok) {
        toast.success("Cronjob deleted");
        fetchConfig();
      }
    } catch (e) { toast.error("Error deleting cronjob"); }
  };

  const getStatusBadge = (status: string) => {
    switch (status) {
      case 'success': return <Badge variant="outline" className="border-teal-400/50 text-teal-400 bg-teal-400/10">Success</Badge>;
      case 'failed': return <Badge variant="outline" className="border-amber-400/50 text-amber-400 bg-amber-400/10">Failed</Badge>;
      case 'started': return <Badge variant="outline" className="border-cyan-400/50 text-cyan-400 bg-cyan-400/10 animate-pulse">Running</Badge>;
      default: return <Badge variant="secondary">{status}</Badge>;
    }
  };

  return (
    <SidebarProvider>
      <Toaster position="top-right" theme="dark" />
      <div className="flex min-h-screen w-full bg-background">
        <Sidebar className="border-r border-border/40">
          <SidebarHeader className="p-6">
            <div className="flex items-center gap-3 px-2">
              <div className="flex h-9 w-9 items-center justify-center rounded-xl bg-primary/15 text-primary ring-1 ring-primary/30">
                <Database className="h-5 w-5" />
              </div>
              <div className="flex flex-col">
                <span className="text-base font-extrabold tracking-tight">Dataflow</span>
                <span className="text-[10px] text-muted-foreground uppercase tracking-[0.2em]">Pipeline Engine</span>
              </div>
            </div>
          </SidebarHeader>
          <SidebarContent>
            <SidebarGroup>
              <SidebarGroupLabel className="px-6 text-xs font-semibold uppercase tracking-wider text-muted-foreground">Topology</SidebarGroupLabel>
              <SidebarGroupContent className="px-4 py-2">
                <SidebarMenu>
                  <SidebarMenuItem>
                    <SidebarMenuButton 
                      onClick={() => setActiveView('dashboard')}
                      isActive={activeView === 'dashboard'}
                    >
                      <BarChart3 className="h-4 w-4" />
                      <span>Dashboard</span>
                    </SidebarMenuButton>
                  </SidebarMenuItem>
                  <SidebarMenuItem>
                    <SidebarMenuButton 
                      onClick={() => setActiveView('sources')}
                      isActive={activeView === 'sources'}
                    >
                      <HardDrive className="h-4 w-4" />
                      <span>Sources</span>
                    </SidebarMenuButton>
                  </SidebarMenuItem>
                  <SidebarMenuItem>
                    <SidebarMenuButton 
                      onClick={() => setActiveView('sinks')}
                      isActive={activeView === 'sinks'}
                    >
                      <Cloud className="h-4 w-4" />
                      <span>Sinks</span>
                    </SidebarMenuButton>
                  </SidebarMenuItem>
                  <SidebarMenuItem>
                    <SidebarMenuButton 
                      onClick={() => setActiveView('pipelines')}
                      isActive={activeView === 'pipelines'}
                    >
                      <Layers className="h-4 w-4" />
                      <span>Pipelines</span>
                    </SidebarMenuButton>
                  </SidebarMenuItem>
                  <SidebarMenuItem>
                    <SidebarMenuButton 
                      onClick={() => setActiveView('cronjobs')}
                      isActive={activeView === 'cronjobs'}
                    >
                      <Clock className="h-4 w-4" />
                      <span>Schedules</span>
                    </SidebarMenuButton>
                  </SidebarMenuItem>
                </SidebarMenu>
              </SidebarGroupContent>
            </SidebarGroup>
          </SidebarContent>
        </Sidebar>

        <SidebarInset className="flex flex-col">
          <header className="flex h-16 shrink-0 items-center justify-between border-b border-border/40 px-8">
            <div className="flex items-center gap-4">
              <SidebarTrigger className="-ml-1" />
              <div className="h-4 w-px bg-border/40" />
              <h2 className="text-sm font-medium text-muted-foreground capitalize">{activeView} Layer</h2>
            </div>
          </header>

          <main className="flex-1 overflow-y-auto p-8 lg:p-12">
            {activeView === 'dashboard' && (
              <div className="grid gap-8 animate-in fade-in duration-500">
                <div className="flex flex-col gap-1">
                  <h1 className="text-3xl font-bold tracking-tight">System Operational Overview</h1>
                  <p className="text-muted-foreground">Monitoring active ingestion streams and environment health.</p>
                </div>

                <div className="grid gap-6 md:grid-cols-3">
                  <Card className="bg-card/60 border-border/50 shadow-lg shadow-black/10">
                    <CardHeader className="pb-2">
                      <CardTitle className="text-[10px] font-bold uppercase tracking-[0.15em] text-muted-foreground">Total Runs</CardTitle>
                    </CardHeader>
                    <CardContent>
                      <div className="text-4xl font-black tracking-tight">{stats.total_runs}</div>
                    </CardContent>
                  </Card>
                  <Card className="bg-teal-500/5 border-teal-500/20 shadow-lg shadow-teal-500/5">
                    <CardHeader className="pb-2">
                      <CardTitle className="text-[10px] font-bold uppercase tracking-[0.15em] text-teal-400/80">Successful</CardTitle>
                    </CardHeader>
                    <CardContent>
                      <div className="text-4xl font-black text-teal-400 tracking-tight">{stats.success}</div>
                    </CardContent>
                  </Card>
                   <Card className="bg-amber-500/5 border-amber-500/20 shadow-lg shadow-amber-500/5">
                    <CardHeader className="pb-2">
                      <CardTitle className="text-[10px] font-bold uppercase tracking-[0.15em] text-amber-400/80">Failures</CardTitle>
                    </CardHeader>
                    <CardContent>
                      <div className="text-4xl font-black text-amber-400 tracking-tight">{stats.failed}</div>
                    </CardContent>
                  </Card>
                </div>

                <div className="grid gap-6 md:grid-cols-4">
                  <Card className="bg-primary/5 border-primary/20">
                    <CardHeader className="pb-2">
                       <CardTitle className="text-[10px] font-bold uppercase tracking-wider text-primary/80">Active Pipelines</CardTitle>
                    </CardHeader>
                    <CardContent>
                      <div className="text-2xl font-bold">{config.pipelines.length}</div>
                    </CardContent>
                  </Card>
                  <Card className="bg-primary/5 border-primary/20">
                    <CardHeader className="pb-2">
                       <CardTitle className="text-[10px] font-bold uppercase tracking-wider text-primary/80">Sources</CardTitle>
                    </CardHeader>
                    <CardContent>
                      <div className="text-2xl font-bold">{config.sources.length}</div>
                    </CardContent>
                  </Card>
                  <Card className="bg-primary/5 border-primary/20">
                    <CardHeader className="pb-2">
                       <CardTitle className="text-[10px] font-bold uppercase tracking-wider text-primary/80">Sinks</CardTitle>
                    </CardHeader>
                    <CardContent>
                      <div className="text-2xl font-bold">{config.sinks.length}</div>
                    </CardContent>
                  </Card>
                  <Card className="bg-primary/5 border-primary/20">
                    <CardHeader className="pb-2">
                       <CardTitle className="text-[10px] font-bold uppercase tracking-wider text-primary/80">Cron Jobs</CardTitle>
                    </CardHeader>
                    <CardContent>
                      <div className="text-2xl font-bold">{config.cronjobs.length}</div>
                    </CardContent>
                  </Card>
                </div>

                <section>
                  <h2 className="text-xl font-bold mb-6">Execution Streams</h2>
                  <div className="grid gap-6 md:grid-cols-2 lg:grid-cols-3">
                    {pipelines.map(p => (
                      <Card 
                        key={p.name}
                        className={`cursor-pointer transition-all duration-200 hover:border-primary/50 hover:shadow-lg hover:shadow-primary/5 ${selectedPipeline === p.name ? 'ring-1 ring-primary/60 border-primary/50 bg-primary/[0.04] shadow-lg shadow-primary/5' : 'bg-card/40 border-border/50'}`}
                        onClick={() => { setSelectedPipeline(p.name); fetchHistory(p.name); }}
                      >
                        <CardHeader className="flex flex-row items-center justify-between pb-2">
                          <CardTitle className="text-base font-bold truncate pr-4">{p.name}</CardTitle>
                          <Button 
                            variant="secondary" 
                            size="icon" 
                            className="h-8 w-8 rounded-lg"
                            disabled={loading === p.name}
                            onClick={(e) => { e.stopPropagation(); runPipeline(p.name); }}
                          >
                            <Play className={`h-4 w-4 ${loading === p.name ? 'animate-spin' : 'fill-current'}`} />
                          </Button>
                        </CardHeader>
                        <CardContent>
                          <div className="flex items-center gap-2 text-xs font-mono text-muted-foreground">
                            <Badge variant="secondary" className="font-mono text-[10px]">{p.source}</Badge>
                            <ChevronRight className="h-3 w-3" />
                            <Badge variant="secondary" className="font-mono text-[10px]">{p.sink}</Badge>
                          </div>
                        </CardContent>
                      </Card>
                    ))}
                  </div>
                </section>

                {selectedPipeline && (
                  <section className="animate-in slide-in-from-bottom-4 duration-700">
                    <h2 className="text-xl font-bold mb-6">Streaming History: {selectedPipeline}</h2>
                    <div className="rounded-xl border border-border/50 overflow-hidden bg-card/40 backdrop-blur-sm shadow-xl shadow-black/10">
                      <Table>
                        <TableHeader className="bg-muted/50">
                          <TableRow className="hover:bg-transparent border-border/60">
                            <TableHead className="py-4 px-6 text-[10px] font-bold uppercase tracking-widest">Run Reference</TableHead>
                            <TableHead className="py-4 px-6 text-[10px] font-bold uppercase tracking-widest">Status</TableHead>
                            <TableHead className="py-4 px-6 text-[10px] font-bold uppercase tracking-widest">Extraction Event</TableHead>
                            <TableHead className="py-4 px-6 text-[10px] font-bold uppercase tracking-widest text-right">In</TableHead>
                            <TableHead className="py-4 px-6 text-[10px] font-bold uppercase tracking-widest text-right">Out</TableHead>
                          </TableRow>
                        </TableHeader>
                        <TableBody>
                          {history.map(h => (
                            <TableRow key={h.id} className="border-border/30 hover:bg-primary/[0.03] transition-colors">
                              <TableCell className="py-4 px-6 font-mono text-xs text-muted-foreground tracking-tighter">#{h.id}</TableCell>
                              <TableCell className="py-4 px-6">{getStatusBadge(h.status)}</TableCell>
                              <TableCell className="py-4 px-6 font-mono text-xs opacity-70">{new Date(h.started_at).toLocaleString()}</TableCell>
                              <TableCell className="py-4 px-6 text-right font-mono font-medium">{h.rows_extracted || 0}</TableCell>
                              <TableCell className="py-4 px-6 text-right font-mono font-medium">{h.rows_written || 0}</TableCell>
                            </TableRow>
                          ))}
                        </TableBody>
                      </Table>
                    </div>
                  </section>
                )}
              </div>
            )}

            {activeView === 'sources' && (
              <div className="grid gap-8 animate-in fade-in duration-500">
                <div className="flex items-center justify-between">
                  <div className="flex flex-col gap-1">
                    <h1 className="text-3xl font-bold tracking-tight">Origin Sources</h1>
                    <p className="text-muted-foreground">Manage your database, file, and API endpoints.</p>
                  </div>
                  <Dialog open={isSourceModalOpen} onOpenChange={setIsSourceModalOpen}>
                    <DialogTrigger asChild>
                      <Button onClick={() => setSourceEditing({ name: '', type: 'postgres' })} className="gap-2">
                        <Plus className="h-4 w-4" /> Add Source
                      </Button>
                    </DialogTrigger>
                    <DialogContent className="max-w-md bg-card">
                      <DialogHeader>
                        <DialogTitle>{(sourceEditing as any)?.originalName ? 'Edit' : 'Add'} Data Source</DialogTitle>
                        <DialogDescription>Configure connection parameters for this source.</DialogDescription>
                      </DialogHeader>
                      <div className="grid gap-6 py-4">
                        <div className="grid gap-2">
                          <Label>Internal Name</Label>
                          <Input 
                            value={sourceEditing?.name || ''} 
                            onChange={e => setSourceEditing(s => s ? {...s, name: e.target.value} : null)} 
                            placeholder="e.g. pg_master"
                          />
                        </div>
                        <div className="grid gap-2">
                          <Label>Connection Type</Label>
                          <Select 
                            value={sourceEditing?.type} 
                            onValueChange={v => setSourceEditing(s => s ? {...s, type: v} : null)}
                          >
                            <SelectTrigger>
                              <SelectValue placeholder="Select type" />
                            </SelectTrigger>
                            <SelectContent>
                              <SelectItem value="postgres">PostgreSQL</SelectItem>
                              <SelectItem value="mysql">MySQL</SelectItem>
                              <SelectItem value="local_file">Local File</SelectItem>
                              <SelectItem value="s3">AWS S3</SelectItem>
                              <SelectItem value="duckdb">DuckDB</SelectItem>
                            </SelectContent>
                          </Select>
                        </div>

                        {/* Conditional Fields based on Source Type */}
                        {(sourceEditing?.type === 'postgres' || sourceEditing?.type === 'mysql') && (
                          <div className="grid grid-cols-2 gap-4">
                            <div className="grid gap-2">
                              <Label>Host</Label>
                              <Input value={(sourceEditing as any).host || ''} onChange={e => setSourceEditing(s => s ? {...s, host: e.target.value} : null)} />
                            </div>
                            <div className="grid gap-2">
                              <Label>Port</Label>
                              <Input type="number" value={(sourceEditing as any).port || (sourceEditing?.type === 'mysql' ? 3306 : 5432)} onChange={e => setSourceEditing(s => s ? {...s, port: parseInt(e.target.value)} : null)} />
                            </div>
                            <div className="grid gap-2">
                              <Label>Database</Label>
                              <Input value={(sourceEditing as any).database || ''} onChange={e => setSourceEditing(s => s ? {...s, database: e.target.value} : null)} />
                            </div>
                            <div className="grid gap-2">
                              <Label>Username</Label>
                              <Input value={(sourceEditing as any).username || ''} onChange={e => setSourceEditing(s => s ? {...s, username: e.target.value} : null)} />
                            </div>
                            <div className="grid gap-2">
                              <Label>Password</Label>
                              <Input type="password" value={(sourceEditing as any).password || ''} onChange={e => setSourceEditing(s => s ? {...s, password: e.target.value} : null)} />
                            </div>
                            {sourceEditing?.type === 'postgres' && (
                              <div className="grid gap-2">
                                <Label>Schema</Label>
                                <Input value={(sourceEditing as any).schema || 'public'} onChange={e => setSourceEditing(s => s ? {...s, schema: e.target.value} : null)} />
                              </div>
                            )}
                          </div>
                        )}

                        {(sourceEditing?.type === 'local_file' || sourceEditing?.type === 'duckdb') && (
                          <div className="grid gap-4">
                            <div className="grid gap-2">
                              <Label>File Path</Label>
                              <Input value={(sourceEditing as any).file_path || ''} onChange={e => setSourceEditing(s => s ? {...s, file_path: e.target.value} : null)} />
                            </div>
                            {sourceEditing?.type === 'local_file' && (
                              <div className="grid gap-2">
                                <Label>Format</Label>
                                <Select value={(sourceEditing as any).file_format || 'parquet'} onValueChange={v => setSourceEditing(s => s ? {...s, file_format: v} : null)}>
                                  <SelectTrigger><SelectValue /></SelectTrigger>
                                  <SelectContent>
                                    <SelectItem value="parquet">Parquet</SelectItem>
                                    <SelectItem value="csv">CSV</SelectItem>
                                    <SelectItem value="json">JSON</SelectItem>
                                  </SelectContent>
                                </Select>
                              </div>
                            )}
                          </div>
                        )}

                        {sourceEditing?.type === 's3' && (
                          <div className="grid grid-cols-2 gap-4">
                            <div className="grid gap-2 col-span-2">
                              <Label>Bucket</Label>
                              <Input value={(sourceEditing as any).bucket || ''} onChange={e => setSourceEditing(s => s ? {...s, bucket: e.target.value} : null)} />
                            </div>
                            <div className="grid gap-2">
                              <Label>Key / Prefix</Label>
                              <Input value={(sourceEditing as any).key || ''} onChange={e => setSourceEditing(s => s ? {...s, key: e.target.value} : null)} />
                            </div>
                            <div className="grid gap-2">
                              <Label>Region</Label>
                              <Input value={(sourceEditing as any).region || 'us-east-1'} onChange={e => setSourceEditing(s => s ? {...s, region: e.target.value} : null)} />
                            </div>
                            <div className="grid gap-2">
                              <Label>Access Key ID</Label>
                              <Input value={(sourceEditing as any).access_key || ''} onChange={e => setSourceEditing(s => s ? {...s, access_key: e.target.value} : null)} />
                            </div>
                            <div className="grid gap-2">
                              <Label>Secret Access Key</Label>
                              <Input type="password" value={(sourceEditing as any).secret_key || ''} onChange={e => setSourceEditing(s => s ? {...s, secret_key: e.target.value} : null)} />
                            </div>
                          </div>
                        )}
                      </div>
                      <DialogFooter>
                        <Button variant="outline" onClick={() => setIsSourceModalOpen(false)}>Cancel</Button>
                        <Button onClick={handleSaveSource}>Save Definition</Button>
                      </DialogFooter>
                    </DialogContent>
                  </Dialog>
                </div>

                <div className="grid gap-6 md:grid-cols-2 lg:grid-cols-3">
                  {config.sources.map(s => (
                    <Card key={s.name} className="bg-card/30 border-border/60 group relative h-fit">
                      <CardHeader className="p-6">
                        <div className="flex justify-between items-start">
                          <div className="flex flex-col gap-1">
                            <div className="flex items-center gap-2">
                              <CardTitle className="text-lg font-bold">{s.name}</CardTitle>
                              <Badge variant="outline" className="text-[9px] uppercase font-black opacity-60">{s.type}</Badge>
                            </div>
                            <span className="text-xs font-mono text-muted-foreground truncate max-w-[150px]">
                              {s.host || s.url || s.file_path || "local"}
                            </span>
                          </div>
                          <div className="flex gap-1 opacity-0 group-hover:opacity-100 transition-opacity">
                            <Button 
                              variant="ghost" 
                              size="icon" 
                              className="h-8 w-8 text-muted-foreground hover:text-primary"
                              onClick={() => { setSourceEditing({...s, originalName: s.name} as any); setIsSourceModalOpen(true); }}
                            >
                              <Edit2 className="h-4 w-4" />
                            </Button>
                            <Button 
                              variant="ghost" 
                              size="icon" 
                              className="h-8 w-8 text-muted-foreground hover:text-destructive"
                              onClick={() => handleDeleteSource(s.name)}
                            >
                              <Trash2 className="h-4 w-4" />
                            </Button>
                          </div>
                        </div>
                      </CardHeader>
                    </Card>
                  ))}
                </div>
              </div>
            )}

            {activeView === 'sinks' && (
              <div className="grid gap-8 animate-in fade-in duration-500">
                <div className="flex items-center justify-between">
                  <div className="flex flex-col gap-1">
                    <h1 className="text-3xl font-bold tracking-tight">Destination Sinks</h1>
                    <p className="text-muted-foreground">Manage your targets for processed data.</p>
                  </div>
                  <Dialog open={isSinkModalOpen} onOpenChange={setIsSinkModalOpen}>
                    <DialogTrigger asChild>
                      <Button onClick={() => setSinkEditing({ name: '', type: 'csv' })} className="gap-2">
                        <Plus className="h-4 w-4" /> Add Sink
                      </Button>
                    </DialogTrigger>
                    <DialogContent className="max-w-md bg-card">
                      <DialogHeader>
                        <DialogTitle>{(sinkEditing as any)?.originalName ? 'Edit' : 'Add'} Target Sink</DialogTitle>
                        <DialogDescription>Set the output location for your pipelines.</DialogDescription>
                      </DialogHeader>
                      <div className="grid gap-6 py-4">
                        <div className="grid gap-2">
                          <Label>Sink Name</Label>
                          <Input 
                            value={sinkEditing?.name || ''} 
                            onChange={e => setSinkEditing(s => s ? {...s, name: e.target.value} : null)} 
                          />
                        </div>
                        <div className="grid gap-2">
                          <Label>Output Type</Label>
                          <Select 
                            value={sinkEditing?.type} 
                            onValueChange={v => setSinkEditing(s => s ? {...s, type: v} : null)}
                          >
                            <SelectTrigger>
                              <SelectValue placeholder="Select type" />
                            </SelectTrigger>
                            <SelectContent>
                              <SelectItem value="local_file">Local File</SelectItem>
                              <SelectItem value="postgres">PostgreSQL</SelectItem>
                              <SelectItem value="mysql">MySQL</SelectItem>
                              <SelectItem value="s3">AWS S3</SelectItem>
                              <SelectItem value="duckdb">DuckDB</SelectItem>
                            </SelectContent>
                          </Select>
                        </div>

                        {(sinkEditing?.type === 'postgres' || sinkEditing?.type === 'mysql') && (
                          <div className="grid grid-cols-2 gap-4">
                            <div className="grid gap-2">
                              <Label>Host</Label>
                              <Input value={(sinkEditing as any).host || ''} onChange={e => setSinkEditing(s => s ? {...s, host: e.target.value} : null)} />
                            </div>
                            <div className="grid gap-2">
                              <Label>Port</Label>
                              <Input type="number" value={(sinkEditing as any).port || (sinkEditing?.type === 'mysql' ? 3306 : 5432)} onChange={e => setSinkEditing(s => s ? {...s, port: parseInt(e.target.value)} : null)} />
                            </div>
                            <div className="grid gap-2">
                              <Label>Database</Label>
                              <Input value={(sinkEditing as any).database || ''} onChange={e => setSinkEditing(s => s ? {...s, database: e.target.value} : null)} />
                            </div>
                            <div className="grid gap-2">
                              <Label>Username</Label>
                              <Input value={(sinkEditing as any).username || ''} onChange={e => setSinkEditing(s => s ? {...s, username: e.target.value} : null)} />
                            </div>
                            <div className="grid gap-2">
                              <Label>Password</Label>
                              <Input type="password" value={(sinkEditing as any).password || ''} onChange={e => setSinkEditing(s => s ? {...s, password: e.target.value} : null)} />
                            </div>
                            {sinkEditing?.type === 'postgres' && (
                              <div className="grid gap-2">
                                <Label>Schema</Label>
                                <Input value={(sinkEditing as any).schema || 'public'} onChange={e => setSinkEditing(s => s ? {...s, schema: e.target.value} : null)} />
                              </div>
                            )}
                          </div>
                        )}

                        {(sinkEditing?.type === 'local_file' || sinkEditing?.type === 'duckdb') && (
                          <div className="grid gap-4">
                            {sinkEditing?.type === 'local_file' && (
                              <div className="grid gap-2">
                                <Label>Output Mode</Label>
                                <Select 
                                  value={(sinkEditing as any).file_path ? 'fixed' : 'versioned'} 
                                  onValueChange={v => {
                                    if (v === 'fixed') {
                                      setSinkEditing(s => s ? {...s, file_path: (s as any).directory || '', directory: undefined} : null);
                                    } else {
                                      setSinkEditing(s => s ? {...s, directory: (s as any).file_path || '', file_path: undefined} : null);
                                    }
                                  }}
                                >
                                  <SelectTrigger><SelectValue /></SelectTrigger>
                                  <SelectContent>
                                    <SelectItem value="versioned">Versioned (Directory)</SelectItem>
                                    <SelectItem value="fixed">Fixed Path (Single File)</SelectItem>
                                  </SelectContent>
                                </Select>
                              </div>
                            )}
                            <div className="grid gap-2">
                              <Label>{(sinkEditing as any).file_path ? 'File Path' : 'Directory'}</Label>
                              <Input 
                                value={(sinkEditing as any).file_path || (sinkEditing as any).directory || ''} 
                                onChange={e => setSinkEditing(s => s ? ( (s as any).file_path !== undefined ? {...s, file_path: e.target.value} : {...s, directory: e.target.value}) : null)} 
                              />
                            </div>
                            {sinkEditing?.type === 'local_file' && (
                              <>
                                <div className="grid gap-2">
                                  <Label>Format</Label>
                                  <Select value={(sinkEditing as any).file_format || 'parquet'} onValueChange={v => setSinkEditing(s => s ? {...s, file_format: v} : null)}>
                                    <SelectTrigger><SelectValue /></SelectTrigger>
                                    <SelectContent>
                                      <SelectItem value="parquet">Parquet</SelectItem>
                                      <SelectItem value="csv">CSV</SelectItem>
                                      <SelectItem value="json">JSON</SelectItem>
                                    </SelectContent>
                                  </Select>
                                </div>
                                <div className="grid gap-2">
                                  <Label>Write Mode</Label>
                                  <Select value={(sinkEditing as any).mode || 'replace'} onValueChange={v => setSinkEditing(s => s ? {...s, mode: v} : null)}>
                                    <SelectTrigger><SelectValue /></SelectTrigger>
                                    <SelectContent>
                                      <SelectItem value="replace">Replace (Overwrite)</SelectItem>
                                      <SelectItem value="append">Append (Add to end)</SelectItem>
                                    </SelectContent>
                                  </Select>
                                </div>
                              </>
                            )}
                          </div>
                        )}

                        {sinkEditing?.type === 's3' && (
                          <div className="grid grid-cols-2 gap-4">
                            <div className="grid gap-2 col-span-2">
                              <Label>Bucket</Label>
                              <Input value={(sinkEditing as any).bucket || ''} onChange={e => setSinkEditing(s => s ? {...s, bucket: e.target.value} : null)} />
                            </div>
                            <div className="grid gap-2">
                              <Label>Prefix</Label>
                              <Input value={(sinkEditing as any).prefix || ''} onChange={e => setSinkEditing(s => s ? {...s, prefix: e.target.value} : null)} />
                            </div>
                            <div className="grid gap-2">
                              <Label>Region</Label>
                              <Input value={(sinkEditing as any).region || 'us-east-1'} onChange={e => setSinkEditing(s => s ? {...s, region: e.target.value} : null)} />
                            </div>
                            <div className="grid gap-2">
                              <Label>Access Key ID</Label>
                              <Input value={(sinkEditing as any).access_key || ''} onChange={e => setSinkEditing(s => s ? {...s, access_key: e.target.value} : null)} />
                            </div>
                            <div className="grid gap-2">
                              <Label>Secret Access Key</Label>
                              <Input type="password" value={(sinkEditing as any).secret_key || ''} onChange={e => setSinkEditing(s => s ? {...s, secret_key: e.target.value} : null)} />
                            </div>
                          </div>
                        )}
                      </div>
                      <DialogFooter>
                        <Button variant="outline" onClick={() => setIsSinkModalOpen(false)}>Cancel</Button>
                        <Button onClick={handleSaveSink}>Save Sink</Button>
                      </DialogFooter>
                    </DialogContent>
                  </Dialog>
                </div>

                <div className="grid gap-6 md:grid-cols-2 lg:grid-cols-3">
                  {config.sinks.map(s => (
                    <Card key={s.name} className="bg-card/30 border-border/60 group relative h-fit">
                      <CardHeader className="p-6">
                        <div className="flex justify-between items-start">
                          <div className="flex flex-col gap-1">
                            <div className="flex items-center gap-2">
                              <CardTitle className="text-lg font-bold">{s.name}</CardTitle>
                              <Badge variant="outline" className="text-[9px] uppercase font-black opacity-70 text-teal-400 border-teal-400/30 bg-teal-400/5">{s.type}</Badge>
                            </div>
                            <span className="text-xs font-mono text-muted-foreground truncate max-w-[150px]">
                              {s.file_path || s.bucket || s.database || "local"}
                            </span>
                          </div>
                          <div className="flex gap-1 opacity-0 group-hover:opacity-100 transition-opacity">
                            <Button 
                              variant="ghost" 
                              size="icon" 
                              className="h-8 w-8 text-muted-foreground hover:text-primary"
                              onClick={() => { setSinkEditing({...s, originalName: s.name} as any); setIsSinkModalOpen(true); }}
                            >
                              <Edit2 className="h-4 w-4" />
                            </Button>
                            <Button 
                              variant="ghost" 
                              size="icon" 
                              className="h-8 w-8 text-muted-foreground hover:text-destructive"
                              onClick={() => handleDeleteSink(s.name)}
                            >
                              <Trash2 className="h-4 w-4" />
                            </Button>
                          </div>
                        </div>
                      </CardHeader>
                    </Card>
                  ))}
                </div>
              </div>
            )}

            {activeView === 'pipelines' && (
              <div className="grid gap-8 animate-in fade-in duration-500">
                <div className="flex items-center justify-between">
                  <div className="flex flex-col gap-1">
                    <h1 className="text-3xl font-bold tracking-tight">Data Pipelines</h1>
                    <p className="text-muted-foreground">Orchestrate data movement and transformation logic.</p>
                   </div>
                   <Dialog open={isPipelineModalOpen} onOpenChange={setIsPipelineModalOpen}>
                     <DialogTrigger asChild>
                       <Button onClick={() => setPipelineEditing({
                         name: '', source: '', source_query: '', sink: '', sink_table: '', sink_mode: 'replace',
                         transforms: [], alerts: { on_failure: 'none' }
                       } as any)} className="gap-2">
                         <Plus className="h-4 w-4" /> Add Pipeline
                       </Button>
                     </DialogTrigger>
                     <DialogContent className="max-w-2xl bg-card max-h-[90vh] overflow-y-auto">
                        <DialogHeader>
                          <DialogTitle>{pipelineEditing?.originalName ? 'Edit' : 'Add'} Pipeline</DialogTitle>
                          <DialogDescription>Define your ETL stream logic.</DialogDescription>
                        </DialogHeader>
                        <div className="grid gap-6 py-4">
                          <div className="grid grid-cols-2 gap-4">
                            <div className="grid gap-2">
                              <Label>Name</Label>
                              <Input value={pipelineEditing?.name || ''} onChange={e => setPipelineEditing(p => p ? {...p, name: e.target.value} : null)} />
                            </div>
                            <div className="grid gap-2">
                              <Label>Description</Label>
                              <Input value={pipelineEditing?.description || ''} onChange={e => setPipelineEditing(p => p ? {...p, description: e.target.value} : null)} />
                            </div>
                          </div>
                          <div className="grid grid-cols-2 gap-4">
                            <div className="grid gap-2">
                              <Label>Source</Label>
                              <Select value={pipelineEditing?.source} onValueChange={v => setPipelineEditing(p => p ? {...p, source: v} : null)}>
                                <SelectTrigger><SelectValue placeholder="Select Source" /></SelectTrigger>
                                <SelectContent>{config.sources.map(s => <SelectItem key={s.name} value={s.name}>{s.name} ({s.type})</SelectItem>)}</SelectContent>
                              </Select>
                            </div>
                            <div className="grid gap-2">
                              <Label>Sink</Label>
                              <Select value={pipelineEditing?.sink} onValueChange={v => setPipelineEditing(p => p ? {...p, sink: v} : null)}>
                                <SelectTrigger><SelectValue placeholder="Select Sink" /></SelectTrigger>
                                <SelectContent>{config.sinks.map(s => <SelectItem key={s.name} value={s.name}>{s.name} ({s.type})</SelectItem>)}</SelectContent>
                              </Select>
                            </div>
                          </div>
                          <div className="grid gap-2">
                            <Label>Source Query (use {"{{last_run}}"} for incrementals)</Label>
                            <textarea 
                              className="flex min-h-[80px] w-full rounded-md border border-input bg-transparent px-3 py-2 text-sm shadow-sm placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring disabled:cursor-not-allowed disabled:opacity-50 font-mono"
                              value={pipelineEditing?.source_query || ''} 
                              onChange={e => setPipelineEditing(p => p ? {...p, source_query: e.target.value} : null)} 
                            />
                          </div>
                          <div className="grid grid-cols-3 gap-4">
                            <div className="grid gap-2">
                              <Label>Sink Table</Label>
                              <Input value={pipelineEditing?.sink_table || ''} onChange={e => setPipelineEditing(p => p ? {...p, sink_table: e.target.value} : null)} />
                            </div>
                            <div className="grid gap-2">
                              <Label>Sink Mode</Label>
                              <Select value={pipelineEditing?.sink_mode} onValueChange={v => setPipelineEditing(p => p ? {...p, sink_mode: v as any} : null)}>
                                <SelectTrigger><SelectValue /></SelectTrigger>
                                <SelectContent>
                                  <SelectItem value="replace">Replace</SelectItem>
                                  <SelectItem value="append">Append</SelectItem>
                                  <SelectItem value="upsert">Upsert</SelectItem>
                                </SelectContent>
                              </Select>
                            </div>
                            <div className="grid gap-2">
                              <Label>Sink Key (Upsert only)</Label>
                              <Input value={pipelineEditing?.sink_key || ''} onChange={e => setPipelineEditing(p => p ? {...p, sink_key: e.target.value} : null)} disabled={pipelineEditing?.sink_mode !== 'upsert'} />
                            </div>
                          </div>
                          <div className="grid gap-2">
                            <Label>Batch Size (min 1000, optional)</Label>
                            <Input type="number" value={pipelineEditing?.batch_size || ''} onChange={e => setPipelineEditing(p => p ? {...p, batch_size: e.target.value ? parseInt(e.target.value) : undefined} : null)} placeholder="e.g. 5000" />
                          </div>
                          <div className="grid gap-2">
                            <Label>Transforms (JSON Array)</Label>
                            <textarea 
                              className="flex min-h-[120px] w-full rounded-md border border-input bg-transparent px-3 py-2 text-sm shadow-sm placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring font-mono"
                              value={JSON.stringify(pipelineEditing?.transforms || [], null, 2)} 
                              onChange={e => {
                                try {
                                  const val = JSON.parse(e.target.value);
                                  if (Array.isArray(val)) setPipelineEditing(p => p ? {...p, transforms: val} : null);
                                } catch (err) {}
                              }} 
                              placeholder='[{"type": "filter", "condition": "amount > 0"}]'
                            />
                          </div>
                          <div className="grid gap-2">
                            <Label>Alert Email (optional)</Label>
                            <Input value={pipelineEditing?.alerts?.email || ''} onChange={e => setPipelineEditing(p => p ? {...p, alerts: {...p.alerts, email: e.target.value, on_failure: e.target.value ? 'email' : 'none'}} : null)} placeholder="alerts@example.com" />
                          </div>
                        </div>
                        <DialogFooter>
                          <Button variant="outline" onClick={() => setIsPipelineModalOpen(false)}>Cancel</Button>
                          <Button onClick={handleSavePipeline}>Save Pipeline</Button>
                        </DialogFooter>
                     </DialogContent>
                   </Dialog>
                </div>

                <div className="grid gap-6">
                  {config.pipelines.map(p => (
                    <Card key={p.name} className="bg-card/30 border-border/60 group">
                      <CardHeader className="p-6">
                        <div className="flex justify-between items-start">
                          <div className="space-y-1">
                            <div className="flex items-center gap-3">
                              <CardTitle className="text-xl font-bold">{p.name}</CardTitle>
                              <Badge variant="outline" className="font-mono text-[10px]">{p.sink_mode}</Badge>
                            </div>
                            <p className="text-sm text-muted-foreground">{p.description || 'No description provided.'}</p>
                          </div>
                          <div className="flex gap-2">
                             <Button variant="outline" size="sm" onClick={() => runPipeline(p.name)} disabled={loading === p.name}>
                               <Play className={`h-4 w-4 mr-2 ${loading === p.name ? 'animate-spin' : ''}`} /> Run
                             </Button>
                             <Button variant="ghost" size="icon" onClick={() => { setPipelineEditing({...p, originalName: p.name}); setIsPipelineModalOpen(true); }}>
                               <Edit2 className="h-4 w-4" />
                             </Button>
                             <Button variant="ghost" size="icon" className="hover:text-destructive" onClick={() => handleDeletePipeline(p.name)}>
                               <Trash2 className="h-4 w-4" />
                             </Button>
                          </div>
                        </div>
                      </CardHeader>
                      <CardContent className="px-6 pb-6 pt-0">
                         <div className="flex items-center gap-6 text-xs font-mono text-muted-foreground bg-black/20 p-3 rounded-lg border border-border/40">
                            <div className="flex items-center gap-2">
                               <HardDrive className="h-3 w-3" /> {p.source}
                            </div>
                            <ArrowRightLeft className="h-3 w-3 opacity-40" />
                            <div className="flex items-center gap-2">
                               <Cloud className="h-3 w-3" /> {p.sink} <span className="opacity-40">➔</span> {p.sink_table}
                            </div>
                         </div>
                      </CardContent>
                    </Card>
                  ))}
                </div>
              </div>
            )}

            {activeView === 'cronjobs' && (
              <div className="grid gap-8 animate-in fade-in duration-500">
                <div className="flex items-center justify-between">
                  <div className="flex flex-col gap-1">
                    <h1 className="text-3xl font-bold tracking-tight">Scheduled Jobs</h1>
                    <p className="text-muted-foreground">Automate your pipelines with cron expressions.</p>
                  </div>
                  <Dialog open={isCronjobModalOpen} onOpenChange={setIsCronjobModalOpen}>
                    <DialogTrigger asChild>
                       <Button onClick={() => setCronjobEditing({
                         name: '', pipeline: '', schedule: '0 * * * *', timezone: 'UTC', enabled: true,
                         retry: { max_attempts: 3, delay_seconds: 60 }
                       } as any)} className="gap-2">
                         <Plus className="h-4 w-4" /> Add Schedule
                       </Button>
                    </DialogTrigger>
                    <DialogContent className="max-w-md bg-card">
                       <DialogHeader>
                         <DialogTitle>{cronjobEditing?.originalName ? 'Edit' : 'Add'} Schedule</DialogTitle>
                         <DialogDescription>Set frequency and execution parameters.</DialogDescription>
                       </DialogHeader>
                       <div className="grid gap-6 py-4">
                          <div className="grid gap-2">
                            <Label>Job Name</Label>
                            <Input value={cronjobEditing?.name || ''} onChange={e => setCronjobEditing(c => c ? {...c, name: e.target.value} : null)} />
                          </div>
                          <div className="grid gap-2">
                            <Label>Target Pipeline</Label>
                            <Select value={cronjobEditing?.pipeline} onValueChange={v => setCronjobEditing(c => c ? {...c, pipeline: v} : null)}>
                                <SelectTrigger><SelectValue placeholder="Select Pipeline" /></SelectTrigger>
                                <SelectContent>{config.pipelines.map(p => <SelectItem key={p.name} value={p.name}>{p.name}</SelectItem>)}</SelectContent>
                            </Select>
                          </div>
                          <div className="grid grid-cols-2 gap-4">
                            <div className="grid gap-2">
                               <Label>Cron Expression</Label>
                               <Input value={cronjobEditing?.schedule || ''} onChange={e => setCronjobEditing(c => c ? {...c, schedule: e.target.value} : null)} />
                            </div>
                            <div className="grid gap-2">
                               <Label>Timezone</Label>
                               <Input value={cronjobEditing?.timezone || 'UTC'} onChange={e => setCronjobEditing(c => c ? {...c, timezone: e.target.value} : null)} />
                            </div>
                          </div>
                          <div className="flex items-center gap-4 py-2">
                             <Label>Enabled</Label>
                             <input type="checkbox" checked={cronjobEditing?.enabled} onChange={e => setCronjobEditing(c => c ? {...c, enabled: e.target.checked} : null)} className="h-4 w-4 accent-primary" />
                          </div>
                       </div>
                       <DialogFooter>
                         <Button variant="outline" onClick={() => setIsCronjobModalOpen(false)}>Cancel</Button>
                         <Button onClick={handleSaveCronjob}>Save Schedule</Button>
                       </DialogFooter>
                    </DialogContent>
                  </Dialog>
                </div>

                <div className="grid gap-6">
                  {config.cronjobs.map(c => (
                    <Card key={c.name} className="bg-card/30 border-border/60">
                       <CardHeader className="p-6">
                         <div className="flex justify-between items-center">
                            <div className="flex items-center gap-4">
                               <div className={`h-2.5 w-2.5 rounded-full ${c.enabled ? 'bg-emerald-500 animate-pulse' : 'bg-muted'}`} />
                               <div className="space-y-1">
                                  <CardTitle className="text-xl font-bold">{c.name}</CardTitle>
                                  <div className="flex items-center gap-2 text-xs text-muted-foreground font-mono">
                                     <Zap className="h-3 w-3" /> {c.pipeline}
                                     <span className="opacity-40">|</span>
                                     <Clock className="h-3 w-3" /> {c.schedule} ({c.timezone})
                                  </div>
                               </div>
                            </div>
                            <div className="flex gap-2">
                               <Button variant="ghost" size="icon" onClick={() => { setCronjobEditing({...c, originalName: c.name}); setIsCronjobModalOpen(true); }}>
                                  <Edit2 className="h-4 w-4" />
                               </Button>
                               <Button variant="ghost" size="icon" className="hover:text-destructive" onClick={() => handleDeleteCronjob(c.name)}>
                                  <Trash2 className="h-4 w-4" />
                               </Button>
                            </div>
                         </div>
                       </CardHeader>
                    </Card>
                  ))}
                </div>
              </div>
            )}

          </main>
        </SidebarInset>
      </div>
    </SidebarProvider>
  );
}
