import { useState } from "react";
import type { PipelineConfig } from "@/lib/api";
import { createPipeline } from "@/lib/api";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Separator } from "@/components/ui/separator";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Textarea } from "@/components/ui/textarea";
import { toast } from "sonner";

interface CreatePipelineDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onCreated: () => void;
}

const defaultConfig: PipelineConfig = {
  source: {
    mode: "logical",
    postgres: {
      host: "localhost",
      port: 5432,
      database: "",
      user: "postgres",
      password: "",
    },
    logical: {
      publication_name: "pg2iceberg_pub",
      slot_name: "pg2iceberg_slot",
      tables: [],
    },
  },
  sink: {
    catalog_uri: "",
    warehouse: "s3://warehouse/",
    namespace: "default",
    s3_endpoint: "",
    s3_access_key: "",
    s3_secret_key: "",
    s3_region: "us-east-1",
    flush_interval: "10s",
    flush_rows: 1000,
  },
  state: {
    path: "/data/pg2iceberg-state.json",
  },
};

export function CreatePipelineDialog({
  open,
  onOpenChange,
  onCreated,
}: CreatePipelineDialogProps) {
  const [id, setId] = useState("");
  const [config, setConfig] = useState<PipelineConfig>(
    structuredClone(defaultConfig)
  );
  const [tables, setTables] = useState("");
  const [creating, setCreating] = useState(false);

  function updatePostgres(field: string, value: string | number) {
    setConfig((prev) => ({
      ...prev,
      source: {
        ...prev.source,
        postgres: { ...prev.source.postgres, [field]: value },
      },
    }));
  }

  function updateLogical(field: string, value: string) {
    setConfig((prev) => ({
      ...prev,
      source: {
        ...prev.source,
        logical: { ...prev.source.logical!, [field]: value },
      },
    }));
  }

  function updateSink(field: string, value: string | number) {
    setConfig((prev) => ({
      ...prev,
      sink: { ...prev.sink, [field]: value },
    }));
  }

  async function handleCreate() {
    if (!id.trim()) {
      toast.error("Pipeline ID is required");
      return;
    }

    setCreating(true);
    try {
      const finalConfig = structuredClone(config);
      if (finalConfig.source.mode === "logical" && finalConfig.source.logical) {
        finalConfig.source.logical.tables = tables
          .split("\n")
          .map((t) => t.trim())
          .filter(Boolean);
      }

      await createPipeline(id.trim(), finalConfig);
      toast.success(`Pipeline "${id.trim()}" created`);

      // Reset form
      setId("");
      setConfig(structuredClone(defaultConfig));
      setTables("");
      onCreated();
    } catch (e) {
      toast.error(`Failed to create: ${(e as Error).message}`);
    } finally {
      setCreating(false);
    }
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-h-[90vh] overflow-y-auto sm:max-w-2xl">
        <DialogHeader>
          <DialogTitle>Create Pipeline</DialogTitle>
          <DialogDescription>
            Set up a new Postgres to Iceberg replication pipeline.
          </DialogDescription>
        </DialogHeader>

        <div className="space-y-6 py-4">
          {/* Pipeline ID */}
          <div className="space-y-2">
            <Label htmlFor="pipeline-id">Pipeline ID</Label>
            <Input
              id="pipeline-id"
              placeholder="my-pipeline"
              value={id}
              onChange={(e) => setId(e.target.value)}
            />
          </div>

          <Separator />

          {/* Source */}
          <div className="space-y-4">
            <h3 className="text-sm font-medium">Source (PostgreSQL)</h3>

            <div className="grid grid-cols-2 gap-4">
              <div className="space-y-2">
                <Label htmlFor="pg-host">Host</Label>
                <Input
                  id="pg-host"
                  value={config.source.postgres.host}
                  onChange={(e) => updatePostgres("host", e.target.value)}
                />
              </div>
              <div className="space-y-2">
                <Label htmlFor="pg-port">Port</Label>
                <Input
                  id="pg-port"
                  type="number"
                  value={config.source.postgres.port}
                  onChange={(e) =>
                    updatePostgres("port", parseInt(e.target.value) || 5432)
                  }
                />
              </div>
            </div>

            <div className="space-y-2">
              <Label htmlFor="pg-database">Database</Label>
              <Input
                id="pg-database"
                value={config.source.postgres.database}
                onChange={(e) => updatePostgres("database", e.target.value)}
              />
            </div>

            <div className="grid grid-cols-2 gap-4">
              <div className="space-y-2">
                <Label htmlFor="pg-user">User</Label>
                <Input
                  id="pg-user"
                  value={config.source.postgres.user}
                  onChange={(e) => updatePostgres("user", e.target.value)}
                />
              </div>
              <div className="space-y-2">
                <Label htmlFor="pg-password">Password</Label>
                <Input
                  id="pg-password"
                  type="password"
                  value={config.source.postgres.password}
                  onChange={(e) => updatePostgres("password", e.target.value)}
                />
              </div>
            </div>

            <div className="space-y-2">
              <Label>Replication Mode</Label>
              <Select
                value={config.source.mode}
                onValueChange={(v) => {
                  if (v)
                    setConfig((prev) => ({
                      ...prev,
                      source: { ...prev.source, mode: v },
                    }));
                }}
              >
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="logical">Logical Replication</SelectItem>
                  <SelectItem value="query">Query-based</SelectItem>
                </SelectContent>
              </Select>
            </div>

            {config.source.mode === "logical" && (
              <>
                <div className="grid grid-cols-2 gap-4">
                  <div className="space-y-2">
                    <Label htmlFor="pub-name">Publication Name</Label>
                    <Input
                      id="pub-name"
                      value={config.source.logical?.publication_name ?? ""}
                      onChange={(e) =>
                        updateLogical("publication_name", e.target.value)
                      }
                    />
                  </div>
                  <div className="space-y-2">
                    <Label htmlFor="slot-name">Slot Name</Label>
                    <Input
                      id="slot-name"
                      value={config.source.logical?.slot_name ?? ""}
                      onChange={(e) =>
                        updateLogical("slot_name", e.target.value)
                      }
                    />
                  </div>
                </div>
                <div className="space-y-2">
                  <Label htmlFor="tables">Tables (one per line)</Label>
                  <Textarea
                    id="tables"
                    placeholder={"public.users\npublic.orders"}
                    value={tables}
                    onChange={(e) => setTables(e.target.value)}
                    rows={4}
                  />
                </div>
              </>
            )}
          </div>

          <Separator />

          {/* Sink */}
          <div className="space-y-4">
            <h3 className="text-sm font-medium">Sink (Iceberg)</h3>

            <div className="space-y-2">
              <Label htmlFor="catalog-uri">Catalog URI</Label>
              <Input
                id="catalog-uri"
                placeholder="http://iceberg-rest:8181"
                value={config.sink.catalog_uri}
                onChange={(e) => updateSink("catalog_uri", e.target.value)}
              />
            </div>

            <div className="grid grid-cols-2 gap-4">
              <div className="space-y-2">
                <Label htmlFor="warehouse">Warehouse</Label>
                <Input
                  id="warehouse"
                  value={config.sink.warehouse}
                  onChange={(e) => updateSink("warehouse", e.target.value)}
                />
              </div>
              <div className="space-y-2">
                <Label htmlFor="namespace">Namespace</Label>
                <Input
                  id="namespace"
                  value={config.sink.namespace}
                  onChange={(e) => updateSink("namespace", e.target.value)}
                />
              </div>
            </div>

            <div className="space-y-2">
              <Label htmlFor="s3-endpoint">S3 Endpoint</Label>
              <Input
                id="s3-endpoint"
                placeholder="http://minio:9000"
                value={config.sink.s3_endpoint}
                onChange={(e) => updateSink("s3_endpoint", e.target.value)}
              />
            </div>

            <div className="grid grid-cols-2 gap-4">
              <div className="space-y-2">
                <Label htmlFor="s3-access">S3 Access Key</Label>
                <Input
                  id="s3-access"
                  value={config.sink.s3_access_key}
                  onChange={(e) => updateSink("s3_access_key", e.target.value)}
                />
              </div>
              <div className="space-y-2">
                <Label htmlFor="s3-secret">S3 Secret Key</Label>
                <Input
                  id="s3-secret"
                  type="password"
                  value={config.sink.s3_secret_key}
                  onChange={(e) => updateSink("s3_secret_key", e.target.value)}
                />
              </div>
            </div>

            <div className="grid grid-cols-3 gap-4">
              <div className="space-y-2">
                <Label htmlFor="s3-region">S3 Region</Label>
                <Input
                  id="s3-region"
                  value={config.sink.s3_region}
                  onChange={(e) => updateSink("s3_region", e.target.value)}
                />
              </div>
              <div className="space-y-2">
                <Label htmlFor="flush-interval">Flush Interval</Label>
                <Input
                  id="flush-interval"
                  value={config.sink.flush_interval}
                  onChange={(e) => updateSink("flush_interval", e.target.value)}
                />
              </div>
              <div className="space-y-2">
                <Label htmlFor="flush-rows">Flush Rows</Label>
                <Input
                  id="flush-rows"
                  type="number"
                  value={config.sink.flush_rows}
                  onChange={(e) =>
                    updateSink("flush_rows", parseInt(e.target.value) || 1000)
                  }
                />
              </div>
            </div>
          </div>
        </div>

        <DialogFooter>
          <Button variant="outline" onClick={() => onOpenChange(false)}>
            Cancel
          </Button>
          <Button onClick={handleCreate} disabled={creating}>
            {creating ? "Creating..." : "Create Pipeline"}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
