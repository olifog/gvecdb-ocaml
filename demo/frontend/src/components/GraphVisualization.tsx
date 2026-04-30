import { useCallback, useEffect, useMemo, useRef } from "react";
import ForceGraph2D from "react-force-graph-2d";
import type { GraphData } from "@/api/types";

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
}

interface FGLink {
  source: number | FGNode;
  target: number | FGNode;
  type: string;
}

const NODE_COLOURS: Record<string, string> = {
  paper: "#3b82f6",
  author: "#22c55e",
};

const EDGE_COLOURS: Record<string, string> = {
  cites: "#64748b",
  authored: "#f97316",
};

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
  }>(undefined);
  const initialFitDone = useRef(false);
  const pendingCentreRef = useRef<number | undefined>(undefined);

  const graphData = useMemo(
    () => ({
      nodes: data.nodes.map((n) => ({ ...n })) as FGNode[],
      links: data.edges.map((e) => ({
        source: e.source,
        target: e.target,
        type: e.type,
      })) as FGLink[],
    }),
    [data],
  );

  useEffect(() => {
    if (selectedNodeId != null) {
      pendingCentreRef.current = selectedNodeId;
    }
  }, [selectedNodeId]);

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

  const paintNode = useCallback(
    (node: FGNode, ctx: CanvasRenderingContext2D) => {
      const r = node.type === "paper" ? 5 : 4;
      const colour = NODE_COLOURS[node.type] ?? "#94a3b8";
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
      ctx.strokeStyle = EDGE_COLOURS[link.type] ?? "#475569";
      ctx.lineWidth = 0.5;
      ctx.stroke();
    },
    [],
  );

  const getNodeLabel = useCallback((node: FGNode) => node.label, []);

  return (
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
      cooldownTicks={40}
      d3AlphaDecay={0.05}
      d3VelocityDecay={0.3}
      enableZoomInteraction
      enablePanInteraction
      backgroundColor="transparent"
    />
  );
}
