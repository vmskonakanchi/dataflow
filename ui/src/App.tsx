import { useState, useEffect } from 'react';
import { 
  BarChart3, 
  Settings, 
  Play, 
  AlertCircle,
  ArrowRightLeft,
  ChevronRight,
  Plus,
  Trash2,
  Edit2,
  HardDrive,
  Cloud
} from "lucide-react";

import { 
  Card, 
  CardContent, 
  CardDescription, 
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
  source: string;
  sink: string;
  description?: string;
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
  originalName?: string; // For tracking name changes during edit
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
  originalName?: string; // For tracking name changes during edit
}

export default function App() {
  const [stats, setStats] = useState<Stats>({ total_runs: 0, success: 0, failed: 0, started: 0 });
  const [pipelines, setPipelines] = useState<Pipeline[]>([]);
  const [history, setHistory] = useState<RunHistory[]>([]);
  const [selectedPipeline, setSelectedPipeline] = useState<string | null>(null);
  const [loading, setLoading] = useState<string | null>(null);
  const [activeView, setActiveView] = useState<'dashboard' | 'sources' | 'sinks' | 'settings'>('dashboard');
  const [config, setConfig] = useState<{sources: Source[], sinks: Sink[]}>({sources: [], sinks: []});

  // Modal states
  const [sourceEditing, setSourceEditing] = useState<Source | null>(null);
  const [sinkEditing, setSinkEditing] = useState<Sink | null>(null);
  const [isSourceModalOpen, setIsSourceModalOpen] = useState(false);
  const [isSinkModalOpen, setIsSinkModalOpen] = useState(false);

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
      setConfig({ sources: data.sources, sinks: data.sinks });
      if (data.pipelines.length > 0 && !selectedPipeline) {
        setSelectedPipeline(data.pipelines[0].name);
        fetchHistory(data.pipelines[0].name);
      }
    } catch (e) { console.error(e); }
  };

  const fetchHistory = async (name: string) => {
    try {
      const res = await fetch(`${API_BASE}/history/${name}`);
      const data = await res.json();
      setHistory(data);
    } catch (e) { console.error(e); }
  };

  const runPipeline = async (name: string) => {
    setLoading(name);
    try {
      await fetch(`${API_BASE}/run/${name}`, { method: 'POST' });
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

  const getStatusBadge = (status: string) => {
    switch (status) {
      case 'success': return <Badge variant="outline" className="border-green-500/50 text-green-500 bg-green-500/10">Success</Badge>;
      case 'failed': return <Badge variant="outline" className="border-red-500/50 text-red-500 bg-red-500/10">Failed</Badge>;
      case 'started': return <Badge variant="outline" className="border-blue-500/50 text-blue-500 animate-pulse">Running</Badge>;
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
              <div className="flex h-8 w-8 items-center justify-center rounded-lg bg-primary text-primary-foreground">
                <ArrowRightLeft className="h-5 w-5" />
              </div>
              <span className="text-xl font-bold tracking-tight">Dataflow</span>
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
                      onClick={() => setActiveView('settings')}
                      isActive={activeView === 'settings'}
                    >
                      <Settings className="h-4 w-4" />
                      <span>Settings</span>
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
                  <Card className="bg-card/40 border-border/60 shadow-sm">
                    <CardHeader className="pb-2">
                      <CardTitle className="text-xs font-bold uppercase tracking-wider text-muted-foreground">Total Runs</CardTitle>
                    </CardHeader>
                    <CardContent>
                      <div className="text-4xl font-black">{stats.total_runs}</div>
                    </CardContent>
                  </Card>
                  <Card className="bg-emerald-500/5 border-emerald-500/20">
                    <CardHeader className="pb-2">
                      <CardTitle className="text-xs font-bold uppercase tracking-wider text-emerald-500/80">Success Rate</CardTitle>
                    </CardHeader>
                    <CardContent>
                      <div className="text-4xl font-black text-emerald-500">{stats.success}</div>
                    </CardContent>
                  </Card>
                  <Card className="bg-rose-500/5 border-rose-500/20">
                    <CardHeader className="pb-2">
                      <CardTitle className="text-xs font-bold uppercase tracking-wider text-rose-500/80">Failures</CardTitle>
                    </CardHeader>
                    <CardContent>
                      <div className="text-4xl font-black text-rose-500">{stats.failed}</div>
                    </CardContent>
                  </Card>
                </div>

                <section>
                  <h2 className="text-xl font-bold mb-6">Execution Streams</h2>
                  <div className="grid gap-6 md:grid-cols-2 lg:grid-cols-3">
                    {pipelines.map(p => (
                      <Card 
                        key={p.name}
                        className={`cursor-pointer transition-all border-border/60 hover:border-primary/40 ${selectedPipeline === p.name ? 'ring-1 ring-primary border-primary/50 bg-primary/[0.02]' : 'bg-card/30'}`}
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
                    <div className="rounded-xl border border-border/60 overflow-hidden bg-card/20 backdrop-blur-sm shadow-xl">
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
                            <TableRow key={h.id} className="border-border/40 hover:bg-white/[0.02]">
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
                              <SelectItem value="csv">CSV File</SelectItem>
                              <SelectItem value="local_file">Local File (Parquet/CSV/JSON)</SelectItem>
                              <SelectItem value="s3">AWS S3</SelectItem>
                            </SelectContent>
                          </Select>
                        </div>
                        {sourceEditing?.type === 'csv' ? (
                          <div className="grid gap-2">
                            <Label>Host File Path</Label>
                            <Input 
                              value={sourceEditing?.file_path || ''} 
                              onChange={e => setSourceEditing(s => s ? {...s, file_path: e.target.value} : null)} 
                            />
                          </div>
                        ) : (
                          <div className="grid gap-2">
                            <Label>Hostname / Endpoint</Label>
                            <Input 
                              value={sourceEditing?.host || sourceEditing?.url || ''} 
                              onChange={e => setSourceEditing(s => s ? (s.type === 'rest_api' ? {...s, url: e.target.value} : {...s, host: e.target.value}) : null)} 
                            />
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
                              <SelectItem value="local_file">Local File (Parquet/CSV/JSON)</SelectItem>
                              <SelectItem value="postgres">PostgreSQL</SelectItem>
                              <SelectItem value="s3">AWS S3</SelectItem>
                              <SelectItem value="duckdb">DuckDB File</SelectItem>
                            </SelectContent>
                          </Select>
                        </div>
                        <div className="grid gap-2">
                          <Label>{sinkEditing?.type === 'csv' || sinkEditing?.type === 'duckdb' ? 'File Path' : 'Target Path / Bucket'}</Label>
                          <Input 
                            value={sinkEditing?.file_path || sinkEditing?.bucket || sinkEditing?.database || ''} 
                            onChange={e => setSinkEditing(s => s ? (s.type === 's3' ? {...s, bucket: e.target.value} : {...s, file_path: e.target.value}) : null)} 
                          />
                        </div>
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
                              <Badge variant="outline" className="text-[9px] uppercase font-black opacity-60 text-emerald-500 border-emerald-500/30 bg-emerald-500/5">{s.type}</Badge>
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

            {activeView === 'settings' && (
              <div className="max-w-3xl animate-in fade-in duration-500">
                <div className="flex flex-col gap-1 mb-10">
                  <h1 className="text-3xl font-bold tracking-tight">Platform Core Configuration</h1>
                  <p className="text-muted-foreground">Underlying engine parameters and environment state.</p>
                </div>

                <div className="grid gap-10">
                  <Card className="border-border/60 bg-card/20 backdrop-blur-md">
                    <CardHeader>
                      <CardTitle className="text-lg font-bold tracking-tight">Engine Environment</CardTitle>
                      <CardDescription>File-backed configuration and telemetry parameters.</CardDescription>
                    </CardHeader>
                    <CardContent className="grid gap-8">
                      <div className="space-y-2">
                        <label className="text-[10px] font-black uppercase text-muted-foreground tracking-widest pl-1">Config Mount Point</label>
                        <Input readOnly value="./configs" className="bg-muted/40 border-border/40 font-mono text-xs h-11" />
                      </div>
                      <div className="space-y-2">
                        <label className="text-[10px] font-black uppercase text-muted-foreground tracking-widest pl-1">Telemetry DB (SQLite)</label>
                        <Input readOnly value="dataflow_runs.db" className="bg-muted/40 border-border/40 font-mono text-xs h-11" />
                      </div>
                    </CardContent>
                  </Card>

                  <div className="bg-primary/[0.03] border border-primary/20 rounded-2xl p-8 flex gap-6 items-start">
                    <div className="h-10 w-10 shrink-0 rounded-xl bg-primary/10 flex items-center justify-center text-primary">
                      <AlertCircle className="h-6 w-6" />
                    </div>
                    <div className="space-y-2">
                      <h4 className="font-bold text-sm tracking-tight">Active Persistence Mode</h4>
                      <p className="text-sm text-muted-foreground leading-relaxed">
                        Changes to Sources and Sinks are persisted immediately to the underlying JSON configuration files. Ensure the system has write permissions to the <code className="text-primary font-bold">./configs</code> directory.
                      </p>
                    </div>
                  </div>
                </div>
              </div>
            )}
          </main>
        </SidebarInset>
      </div>
    </SidebarProvider>
  );
}
