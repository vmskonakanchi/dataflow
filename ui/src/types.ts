export const API_BASE = "http://localhost:8000";

export interface RunHistory {
  id: number;
  pipeline_name: string;
  status: string;
  started_at: string;
  finished_at: string | null;
  rows_extracted: number | null;
  rows_written: number | null;
  error_message: string | null;
}

export interface Stats {
  total_runs: number;
  success: number;
  failed: number;
  started: number;
}

export interface Pipeline {
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

export interface Cronjob {
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

export interface Source {
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

export interface Sink {
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

export interface DataflowConfig {
  sources: Source[];
  sinks: Sink[];
  pipelines: Pipeline[];
  cronjobs: Cronjob[];
}
