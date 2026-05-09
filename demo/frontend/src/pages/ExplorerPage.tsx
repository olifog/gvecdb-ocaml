import { MagnifyingGlass } from "@phosphor-icons/react";
import { useCallback, useEffect, useRef, useState } from "react";
import { useNavigate, useParams } from "react-router-dom";
import { useNeighborhood, useSearch } from "@/api/client";
import type { GraphData, SearchFilters } from "@/api/types";
import { Button } from "@/components/ui/button";
import FilterPanel from "@/components/FilterPanel";
import GraphVisualization from "@/components/GraphVisualization";
import NodeSidebar from "@/components/NodeSidebar";
import PaperCard from "@/components/PaperCard";
import SearchBar from "@/components/SearchBar";

const MAX_GRAPH_NODES = 500;
type VisibleCounts = { nodes: number; edges: number };

export default function ExplorerPage() {
  const { nodeId: paramNodeId } = useParams<{ nodeId: string }>();
  const navigate = useNavigate();

  const [focusNodeId, setFocusNodeId] = useState<number | undefined>(
    paramNodeId ? Number(paramNodeId) : undefined,
  );
  const [selectedNode, setSelectedNode] = useState<{
    id: number;
    type: string;
  } | null>(null);
  const [searchQuery, setSearchQuery] = useState("");
  const [searchActive, setSearchActive] = useState(!paramNodeId);
  const [filters, setFilters] = useState<SearchFilters>({
    publishedOnly: false,
  });
  const [mergedGraph, setMergedGraph] = useState<GraphData>({
    nodes: [],
    edges: [],
  });
  const containerRef = useRef<HTMLDivElement>(null);
  const [dimensions, setDimensions] = useState({ width: 800, height: 600 });
  const [visibleCounts, setVisibleCounts] = useState<VisibleCounts>({ nodes: 0, edges: 0 });

  const { data: neighbourhood } = useNeighborhood(focusNodeId, 1);
  const { data: searchResults } = useSearch(searchQuery, 20, "abstract_embedding", filters);

  useEffect(() => {
    if (!neighbourhood || !focusNodeId) return;
    if (sidebarDismissedRef.current === focusNodeId) return;
    const node = neighbourhood.nodes.find((n) => n.id === focusNodeId);
    if (node && (!selectedNode || selectedNode.id !== focusNodeId)) {
      setSelectedNode({ id: node.id, type: node.type });
    }
  }, [neighbourhood, focusNodeId, selectedNode]);

  useEffect(() => {
    if (!neighbourhood) return;
    setMergedGraph((prev) => {
      const existingNodeMap = new Map(prev.nodes.map((n) => [n.id, n]));
      const existingEdgeIds = new Set(prev.edges.map((e) => e.id));
      const newNodes = neighbourhood.nodes.filter(
        (n) => !existingNodeMap.has(n.id),
      );
      const newEdges = neighbourhood.edges.filter(
        (e) => !existingEdgeIds.has(e.id),
      );
      if (newNodes.length === 0 && newEdges.length === 0) return prev;

      const anchorNode = focusNodeId != null ? existingNodeMap.get(focusNodeId) : undefined;
      const positioned = newNodes.map((n) => {
        const anchor = anchorNode as Record<string, unknown> | undefined;
        if (anchor?.x != null && anchor?.y != null) {
          return { ...n, x: (anchor.x as number) + (Math.random() - 0.5) * 60, y: (anchor.y as number) + (Math.random() - 0.5) * 60 };
        }
        return n;
      });

      let allNodes = [...prev.nodes, ...positioned];
      let allEdges = [...prev.edges, ...newEdges];

      if (allNodes.length > MAX_GRAPH_NODES) {
        allNodes = allNodes.slice(allNodes.length - MAX_GRAPH_NODES);
        const nodeIds = new Set(allNodes.map((n) => n.id));
        allEdges = allEdges.filter(
          (e) => nodeIds.has(e.source) && nodeIds.has(e.target),
        );
      }

      return { nodes: allNodes, edges: allEdges };
    });
  }, [neighbourhood, focusNodeId]);

  useEffect(() => {
    const el = containerRef.current;
    if (!el) return;
    const ro = new ResizeObserver((entries) => {
      const entry = entries[0];
      if (entry) {
        setDimensions({
          width: entry.contentRect.width,
          height: entry.contentRect.height,
        });
      }
    });
    ro.observe(el);
    return () => ro.disconnect();
  }, []);

  const selectNode = useCallback(
    (nodeId: number) => {
      setFocusNodeId(nodeId);
      setSearchActive(false);
      setSearchQuery("");
      navigate(`/explorer/${nodeId}`, { replace: true });

      const existing = mergedGraph.nodes.find((n) => n.id === nodeId);
      if (existing) {
        setSelectedNode({ id: existing.id, type: existing.type });
      }
    },
    [navigate, mergedGraph.nodes],
  );

  const handleNodeClick = useCallback(
    (nodeId: number, nodeType: string) => {
      sidebarDismissedRef.current = null;
      setFocusNodeId(nodeId);
      setSelectedNode({ id: nodeId, type: nodeType });
      setSearchActive(false);
      setSearchQuery("");
      navigate(`/explorer/${nodeId}`, { replace: true });
    },
    [navigate],
  );

  const handleSearch = useCallback((_q: string, _t: string) => {
    setSearchQuery(_q);
    setSearchActive(true);
  }, []);

  const handleReset = useCallback(() => {
    setMergedGraph({ nodes: [], edges: [] });
    setFocusNodeId(undefined);
    setSelectedNode(null);
    setSearchActive(true);
    navigate("/explorer", { replace: true });
  }, [navigate]);

  const sidebarDismissedRef = useRef<number | null>(null);

  const handleCloseSidebar = useCallback(() => {
    if (selectedNode) sidebarDismissedRef.current = selectedNode.id;
    setSelectedNode(null);
  }, [selectedNode]);

  const handleVisibleCountChange = useCallback((nodes: number, edges: number) => {
    setVisibleCounts({ nodes, edges });
  }, []);

  const sidebarWidth = selectedNode ? 440 : 0;
  const graphWidth = dimensions.width - sidebarWidth;

  return (
    <div className="flex h-screen flex-col">
      <div ref={containerRef} className="relative flex-1 flex overflow-hidden">
        <div className="flex-1 relative">
          {mergedGraph.nodes.length > 0 ? (
            <GraphVisualization
              data={mergedGraph}
              filters={filters}
              width={graphWidth > 0 ? graphWidth : dimensions.width}
              height={dimensions.height}
              selectedNodeId={selectedNode?.id}
              onNodeClick={handleNodeClick}
              onVisibleCountChange={handleVisibleCountChange}
            />
          ) : (
            <div className="flex h-full items-center justify-center">
              <div className="text-center">
                <MagnifyingGlass className="mx-auto mb-2 size-6 text-muted-foreground/30" />
                <p className="text-xs text-muted-foreground">
                  Search for a paper to start exploring
                </p>
              </div>
            </div>
          )}

          <div className="absolute top-3 left-3 right-3 z-10 pointer-events-none">
            <div className="mx-auto max-w-xl pointer-events-auto">
              <div className="flex items-center gap-2">
                <div className="flex-1">
                  <SearchBar onSearch={handleSearch} />
                </div>
                {mergedGraph.nodes.length > 0 && (
                  <Button variant="outline" size="sm" onClick={handleReset}>
                    Reset
                  </Button>
                )}
              </div>
              <div className="mt-1.5">
                <FilterPanel filters={filters} onChange={setFilters} />
              </div>

              {searchActive &&
                searchResults &&
                searchResults.results.length > 0 && (
                  <div className="mt-2 max-h-[60vh] overflow-y-auto border bg-background/90 backdrop-blur-md p-2 space-y-1.5">
                    <p className="text-[10px] text-muted-foreground px-1">
                      {searchResults.results.length} results
                    </p>
                    {searchResults.results.map((p) => (
                      <PaperCard
                        key={p.node_id}
                        paper={p}
                        onNavigate={selectNode}
                      />
                    ))}
                  </div>
                )}
            </div>
          </div>
        </div>

        {selectedNode && (
          <div className="w-[440px] shrink-0 h-full overflow-hidden">
            <NodeSidebar
              key={selectedNode.id}
              nodeId={selectedNode.id}
              nodeType={selectedNode.type}
              onClose={handleCloseSidebar}
              onNavigate={selectNode}
            />
          </div>
        )}
      </div>

      <div className="border-t bg-card/50 px-4 py-1.5 shrink-0">
        <div className="flex items-center gap-3 text-[10px] text-muted-foreground">
          <span className="tabular-nums">
            {visibleCounts.nodes} nodes, {visibleCounts.edges} edges
          </span>
          <span className="flex items-center gap-1">
            <span className="inline-block h-2 w-2 rounded-full bg-[#3b82f6]" />
            Paper
          </span>
          <span className="flex items-center gap-1">
            <span className="inline-block h-2 w-2 rounded-full bg-[#22c55e]" />
            Author
          </span>
          <span className="flex items-center gap-1">
            <span className="inline-block h-1 w-3 rounded bg-[#64748b]" />
            Cites
          </span>
          <span className="flex items-center gap-1">
            <span className="inline-block h-1 w-3 rounded bg-[#f97316]" />
            Authored
          </span>
        </div>
      </div>
    </div>
  );
}
