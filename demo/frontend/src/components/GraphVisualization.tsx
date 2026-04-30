import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Crosshair } from "@phosphor-icons/react";
import ForceGraph2D from "react-force-graph-2d";
import type { GraphData } from "@/api/types";
import { Button } from "@/components/ui/button";
import GraphControls, { type GraphSettings, DEFAULT_GRAPH_SETTINGS } from "@/components/GraphControls";

interface GraphVisualisationProps {
  data: GraphData;
  width?: number;
  height?: number;
  selectedNodeId?: number;
  onNodeClick: (nodeId: number, nodeType: string) => void;
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

function getNodeColour(node: FGNode, settings: GraphSettings): string {
  if (settings.colorBy === "type") {
    if (node.type === "paper") return settings.paperColor;
    if (node.type === "author") return settings.authorColor;
    return "#94a3b8";
  }
  if (settings.colorBy === "year" && node.type === "paper") {
    const year = Number(node.metadata.year) || 2020;
    const t = Math.max(0, Math.min(1, (year - 1990) / 35));
    const r = Math.round(59 + t * (59 - 59));
    const g = Math.round(130 + t * (130 - 130));
    const b = Math.round(246 * (0.3 + t * 0.7));
    return `rgb(${r},${g},${b})`;
  }
  if (settings.colorBy === "paperCount" && node.type === "author") {
    const count = Number(node.metadata.paper_count) || 1;
    const t = Math.min(1, count / 50);
    const r = Math.round(34 + t * (220 - 34));
    const g = Math.round(197 - t * (197 - 50));
    const b = Math.round(94 - t * (94 - 50));
    return `rgb(${r},${g},${b})`;
  }
  if (node.type === "paper") return settings.paperColor;
  if (node.type === "author") return settings.authorColor;
  return "#94a3b8";
}

function getEdgeColour(link: FGLink, settings: GraphSettings): string {
  if (link.type === "cites") return settings.citesColor;
  if (link.type === "authored") return settings.authoredColor;
  return "#475569";
}

export default function GraphVisualisation({
  data,
  width,
  height,
  selectedNodeId,
  onNodeClick,
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
  const [settings, setSettings] = useState<GraphSettings>(DEFAULT_GRAPH_SETTINGS);

  const filteredData = useMemo(() => {
    let nodes = data.nodes;
    let edges = data.edges;
    if (!settings.showPapers) {
      const hidden = new Set(nodes.filter((n) => n.type === "paper").map((n) => n.id));
      nodes = nodes.filter((n) => n.type !== "paper");
      edges = edges.filter((e) => !hidden.has(e.source) && !hidden.has(e.target));
    }
    if (!settings.showAuthors) {
      const hidden = new Set(nodes.filter((n) => n.type === "author").map((n) => n.id));
      nodes = nodes.filter((n) => n.type !== "author");
      edges = edges.filter((e) => !hidden.has(e.source) && !hidden.has(e.target));
    }
    if (!settings.showCites) {
      edges = edges.filter((e) => e.type !== "cites");
    }
    if (!settings.showAuthored) {
      edges = edges.filter((e) => e.type !== "authored");
    }
    return { nodes, edges };
  }, [data, settings.showPapers, settings.showAuthors, settings.showCites, settings.showAuthored]);

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
    if (!fgRef.current?.d3Force) return;
    const charge = fgRef.current.d3Force("charge") as { strength?: (v: number) => void; distanceMax?: (v: number) => void } | undefined;
    if (charge?.strength) {
      charge.strength(-8);
      charge.distanceMax?.(80);
    }
    const link = fgRef.current.d3Force("link") as { distance?: (v: number) => void } | undefined;
    if (link?.distance) {
      link.distance(30);
    }
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
      const colour = getNodeColour(node, settings);
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
    [selectedNodeId, settings],
  );

  const paintLink = useCallback(
    (link: FGLink, ctx: CanvasRenderingContext2D) => {
      const src = link.source as FGNode;
      const tgt = link.target as FGNode;
      ctx.beginPath();
      ctx.moveTo(src.x ?? 0, src.y ?? 0);
      ctx.lineTo(tgt.x ?? 0, tgt.y ?? 0);
      ctx.strokeStyle = getEdgeColour(link, settings);
      ctx.lineWidth = 0.5;
      ctx.stroke();
    },
    [settings],
  );

  const getNodeLabel = useCallback((node: FGNode) => node.label, []);

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
        nodeLabel={getNodeLabel}
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
      <div className="absolute top-3 right-3">
        <GraphControls settings={settings} onChange={setSettings} />
      </div>
    </div>
  );
}
