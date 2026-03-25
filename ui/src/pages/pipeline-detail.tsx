import { useEffect, useState, useCallback } from "react";
import { useParams, useNavigate } from "react-router-dom";
import type { PipelineInfo } from "@/lib/api";
import { getPipeline } from "@/lib/api";
import { PipelineDetail } from "@/components/pipeline-detail";
import { Button } from "@/components/ui/button";
import { ArrowLeft } from "lucide-react";

export function PipelineDetailPage() {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const [pipeline, setPipeline] = useState<PipelineInfo | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const refresh = useCallback(async () => {
    if (!id) return;
    try {
      const data = await getPipeline(id);
      setPipeline(data);
      setError(null);
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setLoading(false);
    }
  }, [id]);

  useEffect(() => {
    refresh();
    const interval = setInterval(refresh, 3000);
    return () => clearInterval(interval);
  }, [refresh]);

  if (loading) {
    return (
      <div className="flex h-64 items-center justify-center text-muted-foreground">
        Loading...
      </div>
    );
  }

  if (error || !pipeline) {
    return (
      <div className="space-y-4">
        <Button variant="ghost" size="sm" onClick={() => navigate("/")}>
          <ArrowLeft className="mr-1 h-4 w-4" />
          Back
        </Button>
        <div className="flex h-64 items-center justify-center text-muted-foreground">
          {error ?? "Pipeline not found."}
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      <Button variant="ghost" size="sm" onClick={() => navigate("/")}>
        <ArrowLeft className="mr-1 h-4 w-4" />
        Back to Pipelines
      </Button>
      <PipelineDetail
        pipeline={pipeline}
        onRefresh={refresh}
        onDeleted={() => navigate("/")}
      />
    </div>
  );
}
