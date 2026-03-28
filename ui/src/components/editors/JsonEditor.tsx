import React from 'react';
import CodeMirror from "@uiw/react-codemirror";
import { json } from "@codemirror/lang-json";

interface JsonEditorProps {
  value: any[];
  onChange: (value: any[]) => void;
  height?: string;
}

export function JsonEditor({ value, onChange, height = "280px" }: JsonEditorProps) {
  const [local, setLocal] = React.useState(() => JSON.stringify(value || [], null, 2));
  
  React.useEffect(() => {
    // Only resync if the value is functionally different (e.g., loaded from a different pipeline)
    const stringified = JSON.stringify(value || [], null, 2);
    if (stringified !== local) {
      setLocal(stringified);
    }
  }, [value]);

  return (
    <div className="rounded-xl border border-border/50 shadow-inner overflow-hidden focus-within:ring-1 focus-within:ring-teal-500">
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
              onChange(parsed);
            }
          } catch(e) {
            // Silently ignore invalid JSON while typing
          }
        }}
      />
    </div>
  );
}
