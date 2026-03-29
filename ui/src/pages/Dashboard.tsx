import { 
  BarChart3,
  Zap, 
  Clock, 
  ArrowRightLeft,
  Play,
  AlertCircle
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
  Tooltip, 
  TooltipContent, 
  TooltipProvider, 
  TooltipTrigger 
} from "@/components/ui/tooltip";
import type { Stats, RunHistory, Pipeline } from '../types';

interface DashboardProps {
  stats: Stats;
  history: RunHistory[];
  pipelines: Pipeline[];
  selectedPipeline: string | null;
  setSelectedPipeline: (name: string) => void;
  loading: string | null;
  runPipeline: (name: string) => void;
}

export function Dashboard({ 
  stats, 
  history, 
  pipelines, 
  selectedPipeline, 
  setSelectedPipeline, 
  loading,
  runPipeline 
}: DashboardProps) {
  return (
    <div className="grid gap-8 animate-in fade-in duration-500">
      <div className="flex flex-col gap-1">
        <h1 className="text-3xl font-bold tracking-tight">System Dashboard</h1>
        <p className="text-muted-foreground">Real-time monitoring and execution metrics.</p>
      </div>

      <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
        {[
          { title: "Total Executions", value: stats.total_runs, icon: Zap, color: "text-teal-400" },
          { title: "Successful", value: stats.success, icon: BarChart3, color: "text-emerald-400" },
          { title: "Failure Rate", value: `${stats.total_runs ? Math.round((stats.failed/stats.total_runs)*100) : 0}%`, icon: Zap, color: "text-rose-400" },
          { title: "Active Runs", value: stats.started, icon: Clock, color: "text-blue-400" },
        ].map((stat, i) => (
          <Card key={i} className="bg-card/40 border-border/60 transition-all hover:bg-card/60 hover:shadow-lg hover:shadow-teal-500/5 group">
            <CardHeader className="flex flex-row items-center justify-between pb-2">
              <CardTitle className="text-xs font-black uppercase tracking-widest text-muted-foreground group-hover:text-teal-400 transition-colors opacity-70">{stat.title}</CardTitle>
              <stat.icon className={`h-4 w-4 ${stat.color} opacity-70`} />
            </CardHeader>
            <CardContent>
              <div className="text-3xl font-black text-foreground">{stat.value}</div>
            </CardContent>
          </Card>
        ))}
      </div>

      <div className="grid gap-8 lg:grid-cols-7">
        <Card className="lg:col-span-4 bg-card/20 border-border/50 backdrop-blur-md">
          <CardHeader className="flex flex-row items-center justify-between px-6 py-4 border-b border-border/50">
            <CardTitle className="text-sm font-bold flex items-center gap-2">
              <Clock className="h-4 w-4 text-teal-400" /> Recent Execution History
            </CardTitle>
            <Select value={selectedPipeline || ''} onValueChange={setSelectedPipeline}>
              <SelectTrigger className="w-[200px] h-9 bg-card shadow-sm border-border/50">
                <SelectValue placeholder="Select Pipeline" />
              </SelectTrigger>
              <SelectContent>
                {pipelines.map(p => <SelectItem key={p.name} value={p.name}>{p.name}</SelectItem>)}
              </SelectContent>
            </Select>
          </CardHeader>
          <CardContent className="p-0">
            <div className="overflow-x-auto min-h-[400px]">
              <Table>
                <TableHeader className="bg-muted/30">
                  <TableRow className="border-border/50 hover:bg-transparent">
                    <TableHead className="w-[80px] text-[10px] font-black uppercase text-muted-foreground px-6 py-4">ID</TableHead>
                    <TableHead className="text-[10px] font-black uppercase text-muted-foreground">Status</TableHead>
                    <TableHead className="text-[10px] font-black uppercase text-muted-foreground">Extracted</TableHead>
                    <TableHead className="text-[10px] font-black uppercase text-muted-foreground">Written</TableHead>
                    <TableHead className="text-[10px] font-black uppercase text-muted-foreground text-right px-6">Finished</TableHead>
                  </TableRow>
                </TableHeader>
                    <TableBody>
                      {history.map((run) => (
                        <TableRow key={run.id} className="border-border/40 hover:bg-muted/20 transition-colors">
                          <TableCell className="font-mono text-xs px-6 py-4 text-muted-foreground">#{run.id}</TableCell>
                          <TableCell>
                            <TooltipProvider>
                              <Tooltip>
                                <TooltipTrigger asChild>
                                  <div className="flex items-center gap-2 cursor-help">
                                    <Badge 
                                      variant={run.status === 'success' ? 'default' : 'destructive'} 
                                      className={`uppercase text-[9px] font-black tracking-tighter ${
                                        run.status === 'success' 
                                          ? 'bg-emerald-500/10 text-emerald-400 border-emerald-500/20' 
                                          : 'bg-rose-500/10 text-rose-400 border-rose-500/20'
                                      }`}
                                    >
                                      {run.status}
                                    </Badge>
                                    {run.status === 'failed' && <AlertCircle className="h-3.5 w-3.5 text-rose-400 opacity-70" />}
                                  </div>
                                </TooltipTrigger>
                                {run.status === 'failed' && run.error_message && (
                                  <TooltipContent side="right" className="max-w-[300px] break-words bg-rose-950 text-rose-100 border-rose-500/50 p-3 shadow-xl">
                                    <p className="font-bold mb-1 text-[10px] uppercase tracking-widest text-rose-400">Error Details</p>
                                    <p className="font-mono text-[11px] leading-relaxed">{run.error_message}</p>
                                  </TooltipContent>
                                )}
                              </Tooltip>
                            </TooltipProvider>
                          </TableCell>
                          <TableCell className="font-mono text-sm">{run.rows_extracted || 0}</TableCell>
                          <TableCell className="font-mono text-sm">{run.rows_written || 0}</TableCell>
                          <TableCell className="text-right font-mono text-xs text-muted-foreground px-6 py-4">
                            {run.finished_at ? new Date(run.finished_at).toLocaleTimeString() : '---'}
                          </TableCell>
                        </TableRow>
                      ))}
                  {history.length === 0 && (
                    <TableRow>
                      <TableCell colSpan={5} className="h-[300px] text-center text-muted-foreground italic font-mono text-sm opacity-50">
                        No run history found for {selectedPipeline || 'any pipeline'}.
                      </TableCell>
                    </TableRow>
                  )}
                </TableBody>
              </Table>
            </div>
          </CardContent>
        </Card>

        <Card className="lg:col-span-3 bg-card/20 border-border/50 overflow-hidden">
          <CardHeader className="px-6 py-4 border-b border-border/50 bg-muted/20">
            <CardTitle className="text-sm font-black uppercase tracking-widest text-teal-400 flex items-center gap-2">
              <ArrowRightLeft className="h-4 w-4" /> Quick Run
            </CardTitle>
          </CardHeader>
          <CardContent className="p-4 flex flex-col gap-3">
            {pipelines.map(p => (
              <div key={p.name} className="flex items-center justify-between p-4 rounded-xl border border-border/40 bg-card/30 hover:bg-card/50 transition-all group shadow-sm hover:shadow-md">
                <div className="flex flex-col gap-1">
                  <span className="font-bold tracking-tight text-sm text-foreground">{p.name}</span>
                  <span className="text-[10px] uppercase font-black text-muted-foreground opacity-70 font-mono tracking-tighter">
                    {p.source} &rarr; {p.sink}
                  </span>
                </div>
                <Button 
                  size="sm" 
                  onClick={() => runPipeline(p.name)} 
                  disabled={loading === p.name}
                  className="rounded-lg h-9 w-9 p-0 bg-teal-500/10 hover:bg-teal-500 text-teal-400 hover:text-white border border-teal-500/20 transition-all duration-300 shadow-sm"
                >
                  {loading === p.name ? <div className="animate-spin h-3 w-3 border-2 border-current border-t-transparent rounded-full" /> : <Play className="h-3 w-3 fill-current" />}
                </Button>
              </div>
            ))}
          </CardContent>
        </Card>
      </div>
    </div>
  );
}

// Added internal Selective for use within Dashboard.tsx
import { 
  Select, 
  SelectContent, 
  SelectItem, 
  SelectTrigger, 
  SelectValue 
} from "@/components/ui/select";
