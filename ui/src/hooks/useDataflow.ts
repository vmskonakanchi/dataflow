import { useState, useEffect, useCallback } from 'react';
import { toast } from "sonner";
import { 
  API_BASE 
} from '../types';
import type { 
  Stats, 
  RunHistory, 
  DataflowConfig 
} from '../types';

export function useDataflow() {
  const [stats, setStats] = useState<Stats>({ total_runs: 0, success: 0, failed: 0, started: 0 });
  const [config, setConfig] = useState<DataflowConfig>({
    sources: [],
    sinks: [],
    pipelines: [],
    cronjobs: []
  });
  const [history, setHistory] = useState<RunHistory[]>([]);
  const [selectedPipeline, setSelectedPipeline] = useState<string | null>(null);
  const [loading, setLoading] = useState<string | null>(null);

  const fetchStats = useCallback(async () => {
    try {
      const res = await fetch(`${API_BASE}/stats`);
      const data = await res.json();
      setStats(data);
    } catch (e) {
      console.error("Failed to fetch stats:", e);
    }
  }, []);

  const fetchConfig = useCallback(async () => {
    try {
      const res = await fetch(`${API_BASE}/config`);
      const data = await res.json();
      setConfig({
        sources: data.sources || [],
        sinks: data.sinks || [],
        pipelines: data.pipelines || [],
        cronjobs: data.cronjobs || []
      });
      
      // Select first pipeline by default if none selected
      if (data.pipelines?.length > 0 && !selectedPipeline) {
        setSelectedPipeline(data.pipelines[0].name);
      }
    } catch (e) {
      console.error("Failed to fetch config:", e);
    }
  }, [selectedPipeline]);

  const fetchHistory = useCallback(async (name: string) => {
    try {
      const res = await fetch(`${API_BASE}/pipelines/${name}/history`);
      if (res.ok) {
        const data = await res.json();
        setHistory(Array.isArray(data) ? data : []);
      } else {
        setHistory([]);
      }
    } catch (e) {
      console.error(`Failed to fetch history for ${name}:`, e);
      setHistory([]);
    }
  }, []);

  useEffect(() => {
    fetchStats();
    fetchConfig();
  }, [fetchStats, fetchConfig]);

  useEffect(() => {
    if (selectedPipeline) {
      fetchHistory(selectedPipeline);
    }
  }, [selectedPipeline, fetchHistory]);

  const runPipeline = async (name: string) => {
    setLoading(name);
    try {
      const res = await fetch(`${API_BASE}/pipelines/${name}/run`, { method: 'POST' });
      if (res.ok) {
        toast.success(`Started pipeline: ${name}`);
        // Refresh after a delay to allow the run to start/progress
        setTimeout(() => {
          fetchStats();
          if (selectedPipeline === name) fetchHistory(name);
          setLoading(null);
        }, 2000);
      } else {
        throw new Error("Failed to start pipeline");
      }
    } catch (e) {
      console.error(e);
      setLoading(null);
      toast.error(`Failed to trigger pipeline ${name}`);
    }
  };

  const deleteItem = async (type: 'sources' | 'sinks' | 'pipelines' | 'cronjobs', name: string) => {
    if (!window.confirm(`Are you sure you want to delete ${name}?`)) return;
    try {
      const res = await fetch(`${API_BASE}/${type}/${name}`, { method: 'DELETE' });
      if (res.ok) {
        toast.success(`Deleted ${name}`);
        fetchConfig();
        if (type === 'pipelines' && selectedPipeline === name) {
          setSelectedPipeline(null);
          setHistory([]);
        }
      } else {
        toast.error(`Failed to delete ${name}`);
      }
    } catch (e) {
      console.error(e);
      toast.error(`Error deleting ${name}`);
    }
  };

  const saveItem = async (type: 'sources' | 'sinks' | 'pipelines' | 'cronjobs', item: any) => {
    const isEdit = !!item.originalName;
    const method = isEdit ? 'PUT' : 'POST';
    const url = isEdit ? `${API_BASE}/${type}/${item.originalName}` : `${API_BASE}/${type}`;
    
    try {
      const res = await fetch(url, {
        method,
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(item)
      });
      
      if (res.ok) {
        toast.success(`${isEdit ? 'Updated' : 'Created'} ${item.name}`);
        fetchConfig();
        return true;
      } else {
        const err = await res.json();
        toast.error(err.detail || `Failed to save ${item.name}`);
        return false;
      }
    } catch (e) {
      console.error(e);
      toast.error(`Error saving ${item.name}`);
      return false;
    }
  };

  return {
    stats,
    config,
    history,
    selectedPipeline,
    setSelectedPipeline,
    loading,
    fetchStats,
    fetchConfig,
    fetchHistory,
    runPipeline,
    deleteItem,
    saveItem
  };
}
