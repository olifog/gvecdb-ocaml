import { X } from "@phosphor-icons/react";
import { useAuthor, usePaper, useSimilar } from "@/api/client";
import { Button } from "@/components/ui/button";
import { Separator } from "@/components/ui/separator";
import AuthorDetailView from "@/components/AuthorDetail";
import PaperCard from "@/components/PaperCard";
import PaperDetailView from "@/components/PaperDetail";

interface NodeSidebarProps {
  nodeId: number;
  nodeType: string;
  onClose: () => void;
  onNavigate: (nodeId: number) => void;
}

export default function NodeSidebar({
  nodeId,
  nodeType,
  onClose,
  onNavigate,
}: NodeSidebarProps) {
  const paperQuery = usePaper(nodeType === "paper" ? nodeId : undefined);
  const authorQuery = useAuthor(nodeType === "author" ? nodeId : undefined);
  const similarQuery = useSimilar(
    nodeType === "paper" ? nodeId : undefined,
    6,
  );

  const isLoading =
    (nodeType === "paper" && paperQuery.isLoading) ||
    (nodeType === "author" && authorQuery.isLoading);

  return (
    <div className="flex h-full flex-col border-l bg-background/95 backdrop-blur-sm">
      <div className="flex items-center justify-between border-b px-3 py-2 shrink-0">
        <span className="text-[10px] font-semibold uppercase tracking-widest text-muted-foreground">
          {nodeType}
        </span>
        <Button variant="ghost" size="icon-xs" onClick={onClose}>
          <X />
        </Button>
      </div>

      <div className="flex-1 overflow-y-auto px-3 py-3">
        {isLoading && (
          <div className="flex items-center justify-center py-12">
            <div className="h-4 w-4 animate-spin border-2 border-foreground border-t-transparent" />
          </div>
        )}

        {nodeType === "paper" && paperQuery.data && (
          <div className="space-y-4">
            <PaperDetailView
              paper={paperQuery.data}
              onNavigate={onNavigate}
            />
            {similarQuery.data && similarQuery.data.length > 0 && (
              <>
                <Separator />
                <div>
                  <h2 className="mb-1.5 text-[10px] font-semibold uppercase tracking-widest text-muted-foreground">
                    Similar Papers
                  </h2>
                  <div className="space-y-1.5">
                    {similarQuery.data.map((p) => (
                      <PaperCard
                        key={p.node_id}
                        paper={p}
                        onNavigate={onNavigate}
                      />
                    ))}
                  </div>
                </div>
              </>
            )}
          </div>
        )}

        {nodeType === "author" && authorQuery.data && (
          <AuthorDetailView
            author={authorQuery.data}
            onNavigate={onNavigate}
          />
        )}
      </div>
    </div>
  );
}
