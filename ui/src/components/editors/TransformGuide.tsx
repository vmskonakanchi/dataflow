import { 
  Filter, 
  Type, 
  Layers, 
  Shuffle, 
  Copy, 
  Info 
} from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { toast } from "sonner";

export function TransformGuide() {
  const copyToClipboard = (text: string) => {
    navigator.clipboard.writeText(text);
    toast.success("Example copied to clipboard!");
  };

  const transforms = [
    {
      type: "filter",
      icon: Filter,
      title: "Column Filter",
      description: "Keep only rows that match a specific SQL-like condition.",
      example: JSON.stringify({
        type: "filter",
        condition: "status == 'completed' AND amount > 100"
      }, null, 2)
    },
    {
      type: "rename",
      icon: Type,
      title: "Rename Column",
      description: "Change the name of a column from 'from' to 'to'.",
      example: JSON.stringify({
        type: "rename",
        from: "old_column_name",
        to: "new_column_name"
      }, null, 2)
    },
    {
      type: "aggregate",
      icon: Layers,
      title: "Aggregation",
      description: "Group data and apply functions like SUM, COUNT, AVG, MIN, or MAX.",
      example: JSON.stringify({
        type: "aggregate",
        group_by: ["category", "date"],
        agg: {
          "price": "SUM",
          "id": "COUNT"
        }
      }, null, 2)
    },
    {
      type: "join",
      icon: Shuffle,
      title: "Source Join",
      description: "Enrich data by joining with another defined source.",
      example: JSON.stringify({
        type: "join",
        right_source: "users_dim",
        right_query: "SELECT id, email, country FROM users",
        join_type: "left",
        on: "user_id"
      }, null, 2)
    }
  ];

  return (
    <div className="flex flex-col h-full bg-muted/30 border-l border-border/50 animate-in slide-in-from-right-10 duration-300">
      <div className="p-4 border-b border-border/50 bg-muted/50 flex items-center gap-2">
        <Info className="h-4 w-4 text-teal-400" />
        <h3 className="text-sm font-black uppercase tracking-widest">Transform Guide</h3>
      </div>
      
      <div className="flex-1 overflow-y-auto p-4 space-y-6 custom-scrollbar">
        {transforms.map((t) => (
          <div key={t.type} className="space-y-3 group">
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-2">
                <div className="p-1.5 rounded-lg bg-teal-500/10 text-teal-400">
                  <t.icon className="h-4 w-4" />
                </div>
                <span className="text-xs font-bold text-foreground uppercase tracking-tight">{t.title}</span>
              </div>
              <Badge variant="outline" className="text-[8px] font-black tracking-widest opacity-50 uppercase">{t.type}</Badge>
            </div>
            
            <p className="text-[11px] text-muted-foreground leading-relaxed">
              {t.description}
            </p>

            <div className="relative group/code">
              <pre className="p-4 rounded-xl bg-zinc-950 border border-border/40 text-[10.5px] font-mono overflow-x-auto text-zinc-100 leading-relaxed mb-1 shadow-inner">
                {t.example}
              </pre>
              <button 
                onClick={() => copyToClipboard(t.example)}
                className="absolute top-2 right-2 p-1.5 rounded-md bg-zinc-800 hover:bg-teal-500 hover:text-white opacity-0 group-hover/code:opacity-100 transition-all shadow-md border border-zinc-700"
              >
                <Copy className="h-3 w-3" />
              </button>
            </div>
          </div>
        ))}
        
        <div className="pt-4 border-t border-border/40">
          <p className="text-[9px] text-muted-foreground/60 italic leading-snug">
            💡 Tip: Multiple transforms can be added to the array and will be executed sequentially.
          </p>
        </div>
      </div>
    </div>
  );
}
