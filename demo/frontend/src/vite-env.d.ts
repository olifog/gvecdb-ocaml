/// <reference types="vite/client" />

declare module "react-force-graph-2d" {
  interface ForceGraphProps {
    graphData?: { nodes: object[]; links: object[] };
    width?: number;
    height?: number;
    backgroundColor?: string;
    nodeCanvasObject?: (node: never, ctx: CanvasRenderingContext2D, globalScale: number) => void;
    linkCanvasObject?: (link: never, ctx: CanvasRenderingContext2D, globalScale: number) => void;
    onNodeClick?: (node: never, event: MouseEvent) => void;
    onEngineStop?: () => void;
    nodeLabel?: string | ((node: never) => string);
    cooldownTicks?: number;
    enableZoomInteraction?: boolean;
    enablePanInteraction?: boolean;
    [key: string]: unknown;
  }

  const ForceGraph2D: React.ForwardRefExoticComponent<ForceGraphProps & React.RefAttributes<unknown>>;
  export default ForceGraph2D;
}
