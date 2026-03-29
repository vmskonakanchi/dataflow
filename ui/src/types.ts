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
  username?: string;
  password?: string;
  schema?: string;
  file_path?: string;
  file_format?: 'parquet' | 'csv' | 'json';
  delimiter?: string;
  has_header?: boolean;
  bucket?: string;
  key?: string; // used for s3 source
  region?: string;
  access_key?: string;
  secret_key?: string;
  public?: boolean;
  originalName?: string;
}

export interface Sink {
  name: string;
  type: string;
  host?: string;
  port?: number;
  database?: string;
  username?: string;
  password?: string;
  schema?: string;
  file_path?: string;
  directory?: string; // local_file sink
  file_format?: 'parquet' | 'csv' | 'json';
  mode?: 'replace' | 'append';
  delimiter?: string;
  bucket?: string;
  prefix?: string; // used for s3 sink
  region?: string;
  access_key?: string;
  secret_key?: string;
  originalName?: string;
}

export interface DataflowConfig {
  sources: Source[];
  sinks: Sink[];
  pipelines: Pipeline[];
  cronjobs: Cronjob[];
}
