import CodeMirror from "@uiw/react-codemirror";
import { sql } from "@codemirror/lang-sql";

interface SqlEditorProps {
  value: string;
  onChange: (value: string) => void;
  height?: string;
}

export function SqlEditor({ value, onChange, height = "220px" }: SqlEditorProps) {
  return (
    <div className="rounded-xl border border-border/50 shadow-inner overflow-hidden focus-within:ring-1 focus-within:ring-teal-500">
      <CodeMirror
        value={value}
        height={height}
        theme="dark"
        extensions={[sql()]}
        onChange={onChange}
        className="text-sm font-mono"
      />
    </div>
  );
}
