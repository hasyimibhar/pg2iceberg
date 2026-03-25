import type { PipelineInfo } from "@/lib/api";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent } from "@/components/ui/card";
import { ScrollArea } from "@/components/ui/scroll-area";
import { cn } from "@/lib/utils";

function statusVariant(
  status: string
): "default" | "secondary" | "destructive" | "outline" {
  switch (status) {
    case "running":
      return "default";
    case "error":
      return "destructive";
    case "stopped":
    case "stopping":
      return "secondary";
    default:
      return "outline";
  }
}

interface PipelineListProps {
  pipelines: PipelineInfo[];
  selected: string | null;
  onSelect: (id: string) => void;
  loading: boolean;
}

export function PipelineList({
  pipelines,
  selected,
  onSelect,
  loading,
}: PipelineListProps) {
  if (loading) {
    return (
      <div className="space-y-2">
        {[1, 2, 3].map((i) => (
          <Card key={i}>
            <CardContent className="p-4">
              <div className="h-4 w-32 animate-pulse rounded bg-muted" />
              <div className="mt-2 h-3 w-20 animate-pulse rounded bg-muted" />
            </CardContent>
          </Card>
        ))}
      </div>
    );
  }

  if (pipelines.length === 0) {
    return (
      <div className="py-8 text-center text-sm text-muted-foreground">
        No pipelines
      </div>
    );
  }

  return (
    <ScrollArea className="h-[calc(100vh-8rem)]">
      <div className="space-y-2 pr-4">
        {pipelines.map((p) => (
          <Card
            key={p.id}
            className={cn(
              "cursor-pointer transition-colors hover:bg-accent",
              selected === p.id && "border-primary bg-accent"
            )}
            onClick={() => onSelect(p.id)}
          >
            <CardContent className="p-4">
              <div className="flex items-center justify-between">
                <span className="font-medium">{p.id}</span>
                <Badge variant={statusVariant(p.status)}>{p.status}</Badge>
              </div>
              <p className="mt-1 text-xs text-muted-foreground">
                {p.config.source.postgres.host}:{p.config.source.postgres.port}/
                {p.config.source.postgres.database}
              </p>
            </CardContent>
          </Card>
        ))}
      </div>
    </ScrollArea>
  );
}
