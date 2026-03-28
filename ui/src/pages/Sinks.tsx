import { useState } from 'react';
import { 
  Plus, 
  Trash2, 
  Edit2, 
  Cloud
} from "lucide-react";
import { 
  Card, 
  CardHeader, 
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
import type { Sink } from '../types';

interface SinksProps {
  sinks: Sink[];
  onSave: (sink: Sink) => Promise<boolean>;
  onDelete: (name: string) => void;
}

export function Sinks({ sinks, onSave, onDelete }: SinksProps) {
  const [editing, setEditing] = useState<Sink | null>(null);
  const [isOpen, setIsOpen] = useState(false);

  const handleCreate = () => {
    setEditing({ name: '', type: 'csv' });
    setIsOpen(true);
  };

  const handleEdit = (sink: Sink) => {
    setEditing({ ...sink, originalName: sink.name });
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
          <h1 className="text-3xl font-bold tracking-tight text-foreground">Destination Sinks</h1>
          <p className="text-muted-foreground">Manage your targets for processed and transformed data.</p>
        </div>
        <Dialog open={isOpen} onOpenChange={setIsOpen}>
          <DialogTrigger asChild>
            <Button onClick={handleCreate} className="gap-2 bg-teal-500 hover:bg-teal-600 shadow-lg shadow-teal-500/20 rounded-xl px-5 transition-all">
              <Plus className="h-4 w-4" /> Add Sink
            </Button>
          </DialogTrigger>
          <DialogContent className="w-[90vw] max-w-[800px] bg-card">
            <DialogHeader>
              <DialogTitle>{editing?.originalName ? 'Edit' : 'Add'} Target Sink</DialogTitle>
              <DialogDescription>Set the output location for your processed data.</DialogDescription>
            </DialogHeader>
            <div className="grid gap-6 py-4">
              <div className="grid gap-2">
                <Label>Internal Name</Label>
                <Input value={editing?.name || ''} onChange={e => setEditing(s => s ? {...s, name: e.target.value} : null)} placeholder="e.g. s3_datalake" className="rounded-lg h-10 border-border/50 bg-background/50 focus:bg-background transition-all" />
              </div>
              <div className="grid grid-cols-2 gap-4">
                <div className="grid gap-2">
                  <Label>Target Type</Label>
                  <Select value={editing?.type} onValueChange={v => setEditing(s => s ? {...s, type: v} : null)}>
                    <SelectTrigger className="rounded-lg h-10"><SelectValue /></SelectTrigger>
                    <SelectContent>
                      <SelectItem value="csv">Local CSV</SelectItem>
                      <SelectItem value="parquet">Local Parquet</SelectItem>
                      <SelectItem value="postgres">PostgreSQL Table</SelectItem>
                      <SelectItem value="mysql">MySQL Table</SelectItem>
                      <SelectItem value="s3">AWS S3 Folder</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
                {editing?.type !== 's3' && !editing?.type?.includes('postgres') && !editing?.type?.includes('mysql') && (
                  <div className="grid gap-2">
                    <Label>Local Directory</Label>
                    <Input value={editing?.file_path || ''} onChange={e => setEditing(s => s ? {...s, file_path: e.target.value} : null)} placeholder="/data/output/" className="rounded-lg h-10" />
                  </div>
                )}
              </div>
              {editing?.type === 's3' && (
                <div className="grid grid-cols-2 gap-4">
                  <div className="grid gap-2">
                    <Label>Bucket Name</Label>
                    <Input value={editing?.bucket || ''} onChange={e => setEditing(s => s ? {...s, bucket: e.target.value} : null)} placeholder="my-dataflow-bucket" className="rounded-lg h-10" />
                  </div>
                  <div className="grid gap-2">
                    <Label>S3 Region</Label>
                    <Input value={editing?.region || ''} onChange={e => setEditing(s => s ? {...s, region: e.target.value} : null)} placeholder="us-east-1" className="rounded-lg h-10" />
                  </div>
                </div>
              )}
              {(editing?.type === 'postgres' || editing?.type === 'mysql') && (
                <div className="grid grid-cols-2 gap-4">
                  <div className="grid gap-2">
                    <Label>Host</Label>
                    <Input value={editing?.host || ''} onChange={e => setEditing(s => s ? {...s, host: e.target.value} : null)} placeholder="localhost" className="rounded-lg h-10" />
                  </div>
                  <div className="grid gap-2">
                    <Label>Database</Label>
                    <Input value={editing?.database || ''} onChange={e => setEditing(s => s ? {...s, database: e.target.value} : null)} placeholder="analytics_db" className="rounded-lg h-10" />
                  </div>
                </div>
              )}
            </div>
            <DialogFooter>
              <Button variant="outline" onClick={() => setIsOpen(false)} className="rounded-xl px-6">Cancel</Button>
              <Button onClick={handleSave} className="bg-teal-500 hover:bg-teal-600 rounded-xl px-10">Save Sink</Button>
            </DialogFooter>
          </DialogContent>
        </Dialog>
      </div>

      <div className="grid gap-6 md:grid-cols-2 lg:grid-cols-3">
        {sinks.map(s => (
          <Card key={s.name} className="bg-card/30 border-border/60 group relative h-fit hover:shadow-xl hover:shadow-teal-500/5 transition-all">
            <CardHeader className="p-6">
              <div className="flex justify-between items-start">
                <div className="flex flex-col gap-2">
                  <div className="flex items-center gap-2">
                    <CardTitle className="text-xl font-bold tracking-tight">{s.name}</CardTitle>
                    <Badge variant="outline" className="text-[9px] uppercase font-black opacity-70 text-teal-400 border-teal-400/30 bg-teal-400/5">{s.type}</Badge>
                  </div>
                  <div className="flex items-center gap-2 text-xs font-mono text-muted-foreground opacity-60">
                    <Cloud className="h-3 w-3" />
                    <span className="truncate max-w-[150px]">{s.file_path || s.bucket || s.database || "local"}</span>
                  </div>
                </div>
                <div className="flex gap-1 opacity-0 group-hover:opacity-100 transition-opacity">
                  <Button variant="ghost" size="icon" className="h-9 w-9 text-muted-foreground hover:text-teal-400 rounded-lg" onClick={() => handleEdit(s)}>
                    <Edit2 className="h-4 w-4" />
                  </Button>
                  <Button variant="ghost" size="icon" className="h-9 w-9 text-muted-foreground hover:text-rose-400 rounded-lg" onClick={() => onDelete(s.name)}>
                    <Trash2 className="h-4 w-4" />
                  </Button>
                </div>
              </div>
            </CardHeader>
          </Card>
        ))}
      </div>
    </div>
  );
}
