import { Badge } from "@/components/ui/badge";
import { Card, CardContent } from "@/components/ui/card";
import type { AuthorDetail as AuthorDetailType } from "@/api/types";

interface AuthorDetailProps {
  author: AuthorDetailType;
  onNavigate?: (nodeId: number) => void;
}

export default function AuthorDetailView({
  author,
  onNavigate,
}: AuthorDetailProps) {
  return (
    <div className="space-y-4">
      <div>
        <h1 className="text-sm font-bold">{author.name}</h1>
        <p className="mt-0.5 text-xs text-muted-foreground">
          {author.paper_count} paper{author.paper_count !== 1 && "s"}
        </p>
      </div>

      <div>
        <h2 className="mb-1.5 text-[10px] font-semibold uppercase tracking-widest text-muted-foreground">
          Papers
        </h2>
        <div className="space-y-1.5">
          {author.papers.map((paper) => (
            <button
              key={paper.node_id}
              onClick={() => onNavigate?.(paper.node_id)}
              className="w-full text-left"
            >
              <Card
                className="transition-colors hover:bg-muted/50"
                size="sm"
              >
                <CardContent>
                  <h3 className="text-xs font-medium">{paper.title}</h3>
                  <div className="mt-1 flex items-center gap-1.5">
                    <Badge variant="outline">{paper.year}</Badge>
                    <span className="font-mono text-[10px] text-muted-foreground">
                      {paper.arxiv_id}
                    </span>
                  </div>
                </CardContent>
              </Card>
            </button>
          ))}
        </div>
      </div>
    </div>
  );
}
