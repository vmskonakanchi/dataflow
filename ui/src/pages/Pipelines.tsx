import { useState } from 'react';
import { 
  Plus, 
  Trash2, 
  Edit2, 
  Play,
  ArrowRightLeft
} from "lucide-react";
import { 
  Card, 
  CardTitle 
} from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Label } from "@/components/ui/label";
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
import { 
  Select, 
  SelectContent, 
  SelectItem, 
  SelectTrigger, 
  SelectValue 
} from "@/components/ui/select";
import { 
  Tabs, 
  TabsContent, 
  TabsList, 
  TabsTrigger 
} from "@/components/ui/tabs";
import { 
  Sheet, 
  SheetContent
} from "@/components/ui/sheet";
import { SqlEditor } from '../components/editors/SqlEditor';
import { JsonEditor } from '../components/editors/JsonEditor';
import { TransformGuide } from '../components/editors/TransformGuide';
import { HelpCircle } from "lucide-react";
import type { Pipeline, Source, Sink } from '../types';

interface PipelinesProps {
  pipelines: Pipeline[];
  sources: Source[];
  sinks: Sink[];
  onSave: (pipeline: Pipeline) => Promise<boolean>;
  onDelete: (name: string) => void;
  onRun: (name: string) => void;
  loading: string | null;
}

export function Pipelines({ 
  pipelines, 
  sources, 
  sinks, 
  onSave, 
  onDelete, 
  onRun,
  loading 
}: PipelinesProps) {
  const [editing, setEditing] = useState<Pipeline | null>(null);
  const [isOpen, setIsOpen] = useState(false);
  const [showGuide, setShowGuide] = useState(false);

  const handleCreate = () => {
    setEditing({
      name: '', 
      source: '', 
      source_query: '', 
      sink: '', 
      sink_table: '', 
      sink_mode: 'replace',
      transforms: [], 
      alerts: { on_failure: 'none' }
    });
    setIsOpen(true);
  };

  const handleEdit = (pipeline: Pipeline) => {
    setEditing({ ...pipeline, originalName: pipeline.name });
    setIsOpen(true);
  };

  const handleSave = async () => {
    if (editing) {
      const success = await onSave(editing);
      if (success) setIsOpen(false);
    }
  };

  return (
    <div className="grid gap-8 animate-in fade-in duration-500">
      <Sheet open={showGuide} onOpenChange={setShowGuide}>
        <SheetContent side="right" className="w-[400px] p-0 border-l border-border/50 shadow-2xl">
          <TransformGuide />
        </SheetContent>
      </Sheet>

      <div className="flex items-center justify-between">
        <div className="flex flex-col gap-1">
          <h1 className="text-3xl font-bold tracking-tight text-foreground">Data Pipelines</h1>
          <p className="text-muted-foreground">Orchestrate data movement and transformation logic.</p>
        </div>
        <Dialog open={isOpen} onOpenChange={setIsOpen}>
          <DialogTrigger asChild>
            <Button onClick={handleCreate} className="gap-2 bg-teal-500 hover:bg-teal-600 shadow-lg shadow-teal-500/20 rounded-xl px-5 transition-all">
              <Plus className="h-4 w-4" /> Add Pipeline
            </Button>
          </DialogTrigger>
          <DialogContent className="w-[90vw] max-w-[1200px] bg-card max-h-[90vh] overflow-y-auto">
            <DialogHeader>
              <DialogTitle>{editing?.originalName ? 'Edit' : 'Add'} Pipeline</DialogTitle>
              <DialogDescription>Define your ETL stream logic.</DialogDescription>
            </DialogHeader>
            <Tabs defaultValue="general" className="w-full py-4">
              <TabsList className="grid w-full grid-cols-3 mb-6 bg-muted/50 p-1 rounded-xl">
                <TabsTrigger value="general" className="rounded-lg data-[state=active]:bg-background data-[state=active]:shadow-sm">1. General Setup</TabsTrigger>
                <TabsTrigger value="query" className="rounded-lg data-[state=active]:bg-background data-[state=active]:shadow-sm">2. Query & Logic</TabsTrigger>
                <TabsTrigger value="target" className="rounded-lg data-[state=active]:bg-background data-[state=active]:shadow-sm">3. Target Destination</TabsTrigger>
              </TabsList>

              <TabsContent value="general" className="grid gap-6 mt-0">
                <div className="grid grid-cols-2 gap-6">
                  <div className="grid gap-2">
                    <Label>Pipeline Name</Label>
                    <Input value={editing?.name || ''} onChange={e => setEditing(p => p ? {...p, name: e.target.value} : null)} placeholder="e.g. daily_transactions" className="h-10 border-border/50" />
                  </div>
                  <div className="grid gap-2">
                    <Label>Description</Label>
                    <Input value={editing?.description || ''} onChange={e => setEditing(p => p ? {...p, description: e.target.value} : null)} placeholder="What does this pipeline do?" className="h-10 border-border/50" />
                  </div>
                </div>
                <div className="grid grid-cols-2 gap-6">
                  <div className="grid gap-2">
                    <Label>Origin Source</Label>
                    <Select value={editing?.source} onValueChange={v => setEditing(p => p ? {...p, source: v} : null)}>
                      <SelectTrigger className="h-10"><SelectValue placeholder="Select Source" /></SelectTrigger>
                      <SelectContent>{sources.map(s => <SelectItem key={s.name} value={s.name}>{s.name} ({s.type})</SelectItem>)}</SelectContent>
                    </Select>
                  </div>
                  <div className="grid gap-2">
                    <Label>Target Sink</Label>
                    <Select value={editing?.sink} onValueChange={v => setEditing(p => p ? {...p, sink: v} : null)}>
                      <SelectTrigger className="h-10"><SelectValue placeholder="Select Sink" /></SelectTrigger>
                      <SelectContent>{sinks.map(s => <SelectItem key={s.name} value={s.name}>{s.name} ({s.type})</SelectItem>)}</SelectContent>
                    </Select>
                  </div>
                </div>
                <div className="grid grid-cols-2 gap-6">
                  <div className="grid gap-2">
                    <Label>Batch Size (min 1000, optional)</Label>
                    <Input type="number" value={editing?.batch_size || ''} onChange={e => setEditing(p => p ? {...p, batch_size: e.target.value ? parseInt(e.target.value) : undefined} : null)} placeholder="e.g. 5000" className="h-10" />
                  </div>
                  <div className="grid gap-2">
                    <Label>Alert Email (optional)</Label>
                    <Input value={editing?.alerts?.email || ''} onChange={e => setEditing(p => p ? {...p, alerts: {...p.alerts, email: e.target.value, on_failure: e.target.value ? 'email' : 'none'}} : null)} placeholder="alerts@example.com" className="h-10" />
                  </div>
                </div>
              </TabsContent>

              <TabsContent value="query" className="flex flex-col gap-6 mt-0">
                <div className="grid gap-2">
                  <Label className="flex items-center gap-2 px-2 text-muted-foreground/80">
                    Source Query (use {"{{last_run}}"} for incrementals)
                  </Label>
                  <SqlEditor 
                    value={editing?.source_query || ''} 
                    onChange={val => setEditing(p => p ? {...p, source_query: val} : null)} 
                  />
                </div>
                <div className="grid gap-2">
                  <Label className="flex justify-between items-center bg-muted/30 p-2 rounded-lg border border-border/50">
                    <div className="flex items-center gap-2 px-1">
                      <span>Transforms (JSON Array)</span>
                      <Badge variant="secondary" className="text-[10px] opacity-70">Optional</Badge>
                    </div>
                    <Button 
                      variant="ghost" 
                      size="sm" 
                      onClick={() => setShowGuide(true)}
                      className="h-7 gap-1.5 px-2 text-[11px] font-bold uppercase tracking-tight text-muted-foreground hover:text-teal-400 transition-all"
                    >
                      <HelpCircle className="h-3 w-3" />
                      Show Guide
                    </Button>
                  </Label>
                  <JsonEditor 
                    value={editing?.transforms || []} 
                    onChange={val => setEditing(p => p ? {...p, transforms: val} : null)} 
                  />
                </div>
              </TabsContent>

              <TabsContent value="target" className="grid gap-6 mt-0">
                <div className="grid gap-2">
                  <Label>Sink Table / Destination Key</Label>
                  <Input value={editing?.sink_table || ''} onChange={e => setEditing(p => p ? {...p, sink_table: e.target.value} : null)} placeholder="e.g. core_transactions_v2" className="h-10" />
                  <p className="text-[10px] text-muted-foreground mt-1">The exact table or file prefix where data will be loaded.</p>
                </div>
                <div className="grid grid-cols-2 gap-6">
                  <div className="grid gap-2">
                    <Label>Write Mode</Label>
                    <Select value={editing?.sink_mode} onValueChange={v => setEditing(p => p ? {...p, sink_mode: v as any} : null)}>
                      <SelectTrigger className="h-10 font-bold"><SelectValue /></SelectTrigger>
                      <SelectContent>
                        <SelectItem value="replace">Replace (Overwrite)</SelectItem>
                        <SelectItem value="append">Append</SelectItem>
                        <SelectItem value="upsert">Upsert (Merge)</SelectItem>
                      </SelectContent>
                    </Select>
                  </div>
                  <div className="grid gap-2">
                    <Label>Upsert Key (Primary Key)</Label>
                    <Input 
                      value={editing?.sink_key || ''} 
                      onChange={e => setEditing(p => p ? {...p, sink_key: e.target.value} : null)} 
                      disabled={editing?.sink_mode !== 'upsert'} 
                      placeholder={editing?.sink_mode !== 'upsert' ? 'Only applicable for Upsert mode' : 'e.g. id'}
                      className={editing?.sink_mode !== 'upsert' ? "h-10 bg-muted/50 text-muted-foreground" : "h-10"}
                    />
                  </div>
                </div>
              </TabsContent>
            </Tabs>
            <DialogFooter>
              <Button variant="outline" onClick={() => setIsOpen(false)} className="rounded-xl px-6">Cancel</Button>
              <Button onClick={handleSave} className="bg-teal-500 hover:bg-teal-600 shadow-lg shadow-teal-500/20 rounded-xl px-10">Save Pipeline</Button>
            </DialogFooter>
          </DialogContent>
        </Dialog>
      </div>

      <div className="grid gap-6">
        {pipelines.map(p => (
          <Card key={p.name} className="bg-card/30 border-border/60 group hover:shadow-xl hover:shadow-teal-500/5 transition-all">
            <div className="p-6">
              <div className="flex justify-between items-start">
                <div className="space-y-2">
                  <div className="flex items-center gap-3">
                    <CardTitle className="text-xl font-bold tracking-tight text-foreground">{p.name}</CardTitle>
                    <Badge variant="outline" className="font-mono text-[9px] uppercase font-black bg-teal-500/5 text-teal-400 border-teal-500/20">
                      {p.sink_mode}
                    </Badge>
                  </div>
                  <p className="text-sm text-muted-foreground opacity-80">{p.description || 'No description provided.'}</p>
                  <div className="flex items-center gap-4 mt-4">
                    <div className="flex flex-col gap-0.5">
                      <span className="text-[9px] uppercase font-black text-muted-foreground/50 tracking-widest">Source</span>
                      <span className="text-xs font-bold text-foreground flex items-center gap-1.5 underline underline-offset-4 decoration-teal-500/30">
                        <ArrowRightLeft className="h-3 w-3 text-teal-400" /> {p.source}
                      </span>
                    </div>
                    <div className="w-8 h-[1px] bg-border/50 mt-4 opacity-50" />
                    <div className="flex flex-col gap-0.5">
                      <span className="text-[9px] uppercase font-black text-muted-foreground/50 tracking-widest">Target</span>
                      <span className="text-xs font-bold text-foreground">
                        {p.sink} &rarr; {p.sink_table}
                      </span>
                    </div>
                  </div>
                </div>
                <div className="flex gap-2">
                  <Button 
                    variant="outline" 
                    size="sm" 
                    onClick={() => onRun(p.name)} 
                    disabled={loading === p.name}
                    className="h-10 px-4 bg-teal-500/5 hover:bg-teal-500 text-teal-400 hover:text-white border-teal-500/20 rounded-xl transition-all font-bold"
                  >
                    {loading === p.name ? <div className="animate-spin h-3 w-3 border-2 border-current border-t-transparent rounded-full mr-2" /> : <Play className="h-3 w-1.5 fill-current mr-2" />}
                    Execute
                  </Button>
                  <Button variant="ghost" size="icon" className="h-10 w-10 text-muted-foreground hover:text-teal-400 rounded-xl" onClick={() => handleEdit(p)}>
                    <Edit2 className="h-4 w-4" />
                  </Button>
                  <Button variant="ghost" size="icon" className="h-10 w-10 text-muted-foreground hover:text-rose-400 rounded-xl" onClick={() => onDelete(p.name)}>
                    <Trash2 className="h-4 w-4" />
                  </Button>
                </div>
              </div>
            </div>
          </Card>
        ))}
      </div>
    </div>
  );
}
