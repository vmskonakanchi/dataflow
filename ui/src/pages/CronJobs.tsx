import { useState } from 'react';
import { 
  Plus, 
  Trash2, 
  Edit2, 
  Clock
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
import type { Cronjob, Pipeline } from '../types';

interface CronJobsProps {
  cronjobs: Cronjob[];
  pipelines: Pipeline[];
  onSave: (cronjob: Cronjob) => Promise<boolean>;
  onDelete: (name: string) => void;
}

export function CronJobs({ cronjobs, pipelines, onSave, onDelete }: CronJobsProps) {
  const [editing, setEditing] = useState<Cronjob | null>(null);
  const [isOpen, setIsOpen] = useState(false);

  const handleCreate = () => {
    setEditing({
      name: '', 
      pipeline: '', 
      schedule: '0 * * * *', 
      timezone: 'UTC', 
      enabled: true,
      retry: { max_attempts: 3, delay_seconds: 60 }
    });
    setIsOpen(true);
  };

  const handleEdit = (cronjob: Cronjob) => {
    setEditing({ ...cronjob, originalName: cronjob.name });
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
      <div className="flex items-center justify-between">
        <div className="flex flex-col gap-1">
          <h1 className="text-3xl font-bold tracking-tight text-foreground">Scheduled Jobs</h1>
          <p className="text-muted-foreground">Automate your data pipelines with deterministic cron expressions.</p>
        </div>
        <Dialog open={isOpen} onOpenChange={setIsOpen}>
          <DialogTrigger asChild>
            <Button onClick={handleCreate} className="gap-2 bg-teal-500 hover:bg-teal-600 shadow-lg shadow-teal-500/20 rounded-xl px-5 transition-all">
              <Plus className="h-4 w-4" /> Add Schedule
            </Button>
          </DialogTrigger>
          <DialogContent className="w-[90vw] max-w-[800px] bg-card">
            <DialogHeader>
              <DialogTitle>{editing?.originalName ? 'Edit' : 'Add'} Schedule</DialogTitle>
              <DialogDescription>Set high-frequency or batch execution parameters.</DialogDescription>
            </DialogHeader>
            <div className="grid gap-6 py-4">
              <div className="grid grid-cols-2 gap-4">
                <div className="grid gap-2">
                  <Label>Job Name</Label>
                  <Input value={editing?.name || ''} onChange={e => setEditing(s => s ? {...s, name: e.target.value} : null)} placeholder="e.g. daily_sync_1" className="h-10 border-border/50" />
                </div>
                <div className="grid gap-2">
                  <Label>Target Pipeline</Label>
                  <Select value={editing?.pipeline} onValueChange={v => setEditing(s => s ? {...s, pipeline: v} : null)}>
                    <SelectTrigger className="h-10"><SelectValue placeholder="Select Pipeline" /></SelectTrigger>
                    <SelectContent>
                      {pipelines.map(p => <SelectItem key={p.name} value={p.name}>{p.name}</SelectItem>)}
                    </SelectContent>
                  </Select>
                </div>
              </div>
              <div className="grid grid-cols-2 gap-4">
                <div className="grid gap-2 font-mono">
                  <Label className="font-sans">Cron Schedule</Label>
                  <Input value={editing?.schedule || ''} onChange={e => setEditing(s => s ? {...s, schedule: e.target.value} : null)} placeholder="0 * * * *" className="h-10" />
                </div>
                <div className="grid gap-2">
                  <Label>Timezone</Label>
                  <Input value={editing?.timezone || ''} onChange={e => setEditing(s => s ? {...s, timezone: e.target.value} : null)} placeholder="UTC" className="h-10" />
                </div>
              </div>
            </div>
            <DialogFooter>
              <Button variant="outline" onClick={() => setIsOpen(false)} className="rounded-xl px-6">Cancel</Button>
              <Button onClick={handleSave} className="bg-teal-500 hover:bg-teal-600 shadow-lg shadow-teal-500/20 rounded-xl px-10">Save Schedule</Button>
            </DialogFooter>
          </DialogContent>
        </Dialog>
      </div>

      <div className="grid gap-6">
        {cronjobs.map(c => (
          <Card key={c.name} className="bg-card/30 border-border/60 group hover:shadow-xl hover:shadow-teal-500/5 transition-all p-6">
            <div className="flex justify-between items-center">
              <div className="flex flex-col gap-2">
                <div className="flex items-center gap-3">
                  <CardTitle className="text-xl font-bold tracking-tight text-foreground">{c.name}</CardTitle>
                  <Badge variant={c.enabled ? "default" : "secondary"} className={c.enabled ? "bg-teal-500/10 text-teal-400 border-teal-500/20" : "opacity-50"}>
                    {c.enabled ? 'Active' : 'Disabled'}
                  </Badge>
                </div>
                <div className="flex items-center gap-6 mt-1 text-xs">
                  <div className="flex items-center gap-1.5 text-muted-foreground font-mono">
                    <Clock className="h-3 w-3 text-teal-400 opacity-60" />
                    <span>{c.schedule}</span>
                  </div>
                  <div className="flex items-center gap-1.5 text-muted-foreground font-bold">
                    <span className="text-[10px] uppercase opacity-40 font-black tracking-widest">Pipeline:</span>
                    <span className="underline underline-offset-4 decoration-teal-500/20">{c.pipeline}</span>
                  </div>
                </div>
              </div>
              <div className="flex gap-2">
                <Button variant="ghost" size="icon" className="h-10 w-10 text-muted-foreground hover:text-teal-400 rounded-xl" onClick={() => handleEdit(c)}>
                  <Edit2 className="h-4 w-4" />
                </Button>
                <Button variant="ghost" size="icon" className="h-10 w-10 text-muted-foreground hover:text-rose-400 rounded-xl" onClick={() => onDelete(c.name)}>
                  <Trash2 className="h-4 w-4" />
                </Button>
              </div>
            </div>
          </Card>
        ))}
        {cronjobs.length === 0 && (
          <div className="h-[300px] flex flex-col items-center justify-center border-2 border-dashed border-border/50 rounded-3xl opacity-50 grayscale transition-all hover:grayscale-0 hover:opacity-100 hover:border-teal-500/20">
            <Clock className="h-12 w-12 text-teal-400 mb-4 opacity-50" />
            <p className="text-sm font-bold tracking-tight text-muted-foreground">No schedules configured yet.</p>
            <p className="text-[10px] font-black uppercase tracking-widest text-muted-foreground/30 mt-1">Add a schedule to automate your pipelines</p>
          </div>
        )}
      </div>
    </div>
  );
}
