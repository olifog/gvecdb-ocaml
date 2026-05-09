import { useCallback, useEffect, useMemo, useRef } from "react";
import { Crosshair } from "@phosphor-icons/react";
import ForceGraph2D from "react-force-graph-2d";
import type { GraphData, SearchFilters } from "@/api/types";
import { Button } from "@/components/ui/button";

interface GraphVisualisationProps {
  data: GraphData;
  filters: SearchFilters;
  width?: number;
  height?: number;
  selectedNodeId?: number;
  onNodeClick: (nodeId: number, nodeType: string) => void;
  onVisibleCountChange?: (nodes: number, edges: number) => void;
}

interface FGNode {
  id: number;
  type: string;
  label: string;
  metadata: Record<string, string | number>;
  x?: number;
  y?: number;
  fx?: number | null;
  fy?: number | null;
}

interface FGLink {
  source: number | FGNode;
  target: number | FGNode;
  type: string;
}

const PAPER_COLOR = "#3b82f6";
const AUTHOR_COLOR = "#22c55e";
const CITES_COLOR = "#64748b";
const AUTHORED_COLOR = "#f97316";

function getNodeColour(node: FGNode): string {
  if (node.type === "paper") return PAPER_COLOR;
  if (node.type === "author") return AUTHOR_COLOR;
  return "#94a3b8";
}

function getEdgeColour(link: FGLink): string {
  if (link.type === "cites") return CITES_COLOR;
  if (link.type === "authored") return AUTHORED_COLOR;
  return "#475569";
}

export default function GraphVisualisation({
  data,
  filters,
  width,
  height,
  selectedNodeId,
  onNodeClick,
  onVisibleCountChange,
}: GraphVisualisationProps) {
  const fgRef = useRef<{
    d3ReheatSimulation?: () => void;
    zoomToFit?: (ms?: number, padding?: number) => void;
    centerAt?: (x?: number, y?: number, ms?: number) => void;
    zoom?: (zoom: number, ms?: number) => void;
    d3Force?: (name: string, force?: unknown) => unknown;
  }>(undefined);
  const initialFitDone = useRef(false);
  const pendingCentreRef = useRef<number | undefined>(undefined);
  const forcesInitialized = useRef(false);

  const filteredData = useMemo(() => {
    let nodes = data.nodes;
    let edges = data.edges;

    const hidden = new Set<number>();

    // Year filter
    if (filters.yearMin != null || filters.yearMax != null) {
      nodes = nodes.filter((n) => {
        if (n.type !== "paper") return true;
        if (selectedNodeId != null && n.id === selectedNodeId) return true;
        const year = Number(n.metadata.year);
        if (!year) return true;
        if (filters.yearMin != null && year < filters.yearMin) { hidden.add(n.id); return false; }
        if (filters.yearMax != null && year > filters.yearMax) { hidden.add(n.id); return false; }
        return true;
      });
    }

    // Category filter — exact token match
    if (filters.category) {
      const cats = filters.category.split(",").map((c) => c.trim().toLowerCase());
      nodes = nodes.filter((n) => {
        if (n.type !== "paper") return true;
        if (selectedNodeId != null && n.id === selectedNodeId) return true;
        const nodeCats = String(n.metadata.categories || "").toLowerCase().split(/\s+/);
        if (!cats.some((c) => nodeCats.includes(c))) { hidden.add(n.id); return false; }
        return true;
      });
    }

    // Published only filter
    if (filters.publishedOnly) {
      nodes = nodes.filter((n) => {
        if (n.type !== "paper") return true;
        if (selectedNodeId != null && n.id === selectedNodeId) return true;
        const doi = n.metadata.doi;
        if (!doi) { hidden.add(n.id); return false; }
        return true;
      });
    }

    // Remove edges connected to hidden nodes
    if (hidden.size > 0) {
      edges = edges.filter((e) => !hidden.has(e.source) && !hidden.has(e.target));
    }

    // Remove orphan nodes (no remaining edges) — both authors and papers
    const connectedIds = new Set<number>();
    edges.forEach((e) => { connectedIds.add(e.source); connectedIds.add(e.target); });
    nodes = nodes.filter((n) => {
      if (selectedNodeId != null && n.id === selectedNodeId) return true;
      return connectedIds.has(n.id);
    });

    return { nodes, edges };
  }, [data, filters.yearMin, filters.yearMax, filters.category, filters.publishedOnly, selectedNodeId]);

  // Report visible counts to parent
  useEffect(() => {
    onVisibleCountChange?.(filteredData.nodes.length, filteredData.edges.length);
  }, [filteredData.nodes.length, filteredData.edges.length, onVisibleCountChange]);

  const nodeMapRef = useRef<Map<number, FGNode>>(new Map());

  const graphData = useMemo(() => {
    const prevMap = nodeMapRef.current;
    const nextMap = new Map<number, FGNode>();
    const nodes = filteredData.nodes.map((n) => {
      const existing = prevMap.get(n.id);
      if (existing) {
        existing.label = n.label;
        existing.type = n.type;
        existing.metadata = n.metadata;
        nextMap.set(n.id, existing);
        return existing;
      }
      const fresh = { ...n } as FGNode;
      nextMap.set(n.id, fresh);
      return fresh;
    });
    nodeMapRef.current = nextMap;
    return {
      nodes,
      links: filteredData.edges.map((e) => ({
        source: e.source,
        target: e.target,
        type: e.type,
      })) as FGLink[],
    };
  }, [filteredData]);


  useEffect(() => {
    if (!fgRef.current?.d3Force || forcesInitialized.current) return;
    const charge = fgRef.current.d3Force("charge") as { strength?: (v: number) => void; distanceMax?: (v: number) => void } | undefined;
    if (charge?.strength) {
      charge.strength(-40);
      charge.distanceMax?.(200);
    }
    const link = fgRef.current.d3Force("link") as { distance?: (v: number) => void } | undefined;
    if (link?.distance) {
      link.distance(60);
    }
    forcesInitialized.current = true;
  }, [graphData]);

  const prevNodeCountRef = useRef(0);

  useEffect(() => {
    const prevCount = prevNodeCountRef.current;
    const newCount = data.nodes.length;
    prevNodeCountRef.current = newCount;
    if (newCount > prevCount && selectedNodeId != null) {
      pendingCentreRef.current = selectedNodeId;
    }
  }, [data.nodes.length, selectedNodeId]);

  const handleEngineStop = useCallback(() => {
    if (!initialFitDone.current && fgRef.current?.zoomToFit && data.nodes.length > 0) {
      fgRef.current.zoomToFit(300, 40);
      initialFitDone.current = true;
      return;
    }

    const targetId = pendingCentreRef.current;
    if (targetId != null && fgRef.current?.centerAt) {
      const node = graphData.nodes.find((n) => n.id === targetId) as FGNode | undefined;
      if (node?.x != null && node?.y != null) {
        fgRef.current.centerAt(node.x, node.y, 400);
      }
      pendingCentreRef.current = undefined;
    }
  }, [data.nodes.length, graphData.nodes]);

  const handleNodeClick = useCallback(
    (node: FGNode) => {
      onNodeClick(node.id, node.type);
    },
    [onNodeClick],
  );


  const focusSelected = useCallback(() => {
    if (selectedNodeId == null || !fgRef.current?.centerAt) return;
    const node = graphData.nodes.find((n) => n.id === selectedNodeId) as FGNode | undefined;
    if (node?.x != null && node?.y != null) {
      fgRef.current.centerAt(node.x, node.y, 400);
      fgRef.current.zoom?.(2, 400);
    }
  }, [selectedNodeId, graphData.nodes]);

  const paintNode = useCallback(
    (node: FGNode, ctx: CanvasRenderingContext2D) => {
      const r = node.type === "paper" ? 5 : 4;
      const colour = getNodeColour(node);
      const selected = node.id === selectedNodeId;

      if (selected) {
        ctx.beginPath();
        ctx.arc(node.x ?? 0, node.y ?? 0, r + 3, 0, 2 * Math.PI);
        ctx.strokeStyle = "#ffffff";
        ctx.lineWidth = 1.5;
        ctx.stroke();
      }

      ctx.beginPath();
      ctx.arc(node.x ?? 0, node.y ?? 0, r, 0, 2 * Math.PI);
      ctx.fillStyle = colour;
      ctx.fill();
      if (!selected) {
        ctx.strokeStyle = "rgba(255,255,255,0.1)";
        ctx.lineWidth = 0.5;
        ctx.stroke();
      }

      ctx.font = "3px 'JetBrains Mono Variable', monospace";
      ctx.textAlign = "center";
      ctx.fillStyle = selected ? "#ffffff" : "rgba(255,255,255,0.6)";
      const label =
        node.label.length > 30
          ? `${node.label.slice(0, 28)}...`
          : node.label;
      ctx.fillText(label, node.x ?? 0, (node.y ?? 0) + r + 4);
    },
    [selectedNodeId],
  );

  const paintLink = useCallback(
    (link: FGLink, ctx: CanvasRenderingContext2D) => {
      const src = link.source as FGNode;
      const tgt = link.target as FGNode;

      ctx.beginPath();
      ctx.moveTo(src.x ?? 0, src.y ?? 0);
      ctx.lineTo(tgt.x ?? 0, tgt.y ?? 0);
      ctx.strokeStyle = getEdgeColour(link);
      ctx.lineWidth = 0.5;
      ctx.stroke();
    },
    [],
  );

  return (
    <div className="relative w-full h-full">
      <ForceGraph2D
        ref={fgRef}
        graphData={graphData}
        width={width}
        height={height}
        nodeCanvasObject={paintNode}
        linkCanvasObject={paintLink}
        onNodeClick={handleNodeClick}
        onEngineStop={handleEngineStop}
        cooldownTicks={60}
        d3AlphaDecay={0.08}
        d3VelocityDecay={0.6}
        enableZoomInteraction
        enablePanInteraction
        backgroundColor="transparent"
      />
      <div className="absolute bottom-3 right-3 flex gap-2">
        {selectedNodeId != null && (
          <Button variant="secondary" size="icon-sm" onClick={focusSelected} title="Focus selected node">
            <Crosshair className="size-4" />
          </Button>
        )}
      </div>
    </div>
  );
}
