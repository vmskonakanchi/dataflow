import React from 'react';
import CodeMirror from "@uiw/react-codemirror";
import { json } from "@codemirror/lang-json";
import { Sparkles } from "lucide-react";
import { Button } from "../ui/button";

interface JsonEditorProps {
  value: any[];
  onChange: (value: any[]) => void;
  height?: string;
}

export function JsonEditor({ value, onChange, height = "280px" }: JsonEditorProps) {
  const [local, setLocal] = React.useState(() => JSON.stringify(value || [], null, 2));
  
  // Only sync from external prop if the data is functionally different
  React.useEffect(() => {
    try {
      if (JSON.stringify(JSON.parse(local)) === JSON.stringify(value)) return;
    } catch (e) {}
    
    setLocal(JSON.stringify(value || [], null, 2));
  }, [value]);

  const handleFormat = () => {
    try {
      const parsed = JSON.parse(local);
      setLocal(JSON.stringify(parsed, null, 2));
    } catch(e) {
      alert("Invalid JSON - please fix it before formatting!");
    }
  };

  return (
    <div className="relative rounded-xl border border-border/50 shadow-inner overflow-hidden focus-within:ring-1 focus-within:ring-teal-500">
      <div className="absolute right-2 top-2 z-20">
        <Button 
          variant="secondary" 
          size="sm" 
          onClick={handleFormat}
          title="Format Code"
          className="h-7 gap-1.5 bg-background/80 backdrop-blur-sm border border-border/50 shadow-sm hover:bg-teal-500/10 hover:text-teal-400 transition-all rounded-md px-2 text-[10px] font-bold uppercase tracking-wider"
        >
          <Sparkles className="h-3 w-3" />
          Format
        </Button>
      </div>

      <CodeMirror
        value={local}
        height={height}
        theme="dark"
        extensions={[json()]}
        className="text-sm font-mono"
        onChange={(val) => {
          setLocal(val);
          try {
            const parsed = JSON.parse(val);
            if (Array.isArray(parsed)) {
              // We pass the parsed data back to the parent, but we DON'T 
              // update 'local' here to avoid snapping the cursor.
              onChange(parsed);
            }
          } catch(e) { /* typing... */ }
        }}
      />
    </div>
  );
}
