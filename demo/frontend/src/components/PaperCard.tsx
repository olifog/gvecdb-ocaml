import { BookOpen, FileText, GitBranch } from "@phosphor-icons/react";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent } from "@/components/ui/card";
import type { PaperSummary } from "@/api/types";

interface PaperCardProps {
  paper: PaperSummary;
  onNavigate?: (nodeId: number) => void;
}

export default function PaperCard({ paper, onNavigate }: PaperCardProps) {
  const cats = paper.categories.split(/\s+/).slice(0, 4);
  const scorePercent = Math.round(paper.score * 100);

  return (
    <button
      className="w-full text-left"
      onClick={() => onNavigate?.(paper.node_id)}
    >
      <Card className="transition-colors hover:bg-muted/50" size="sm">
        <CardContent className="flex items-start justify-between gap-4">
          <div className="min-w-0 flex-1">
            <h3 className="text-xs font-semibold leading-snug">
              {paper.title}
            </h3>
            <p className="mt-1 text-xs text-muted-foreground">
              {paper.authors.slice(0, 5).join(", ")}
              {paper.authors.length > 5 &&
                ` +${paper.authors.length - 5} more`}
            </p>
            <div className="mt-2 flex flex-wrap items-center gap-1.5">
              <Badge variant="outline">{paper.year}</Badge>
              {cats.map((cat) => (
                <Badge key={cat} variant="secondary">
                  {cat}
                </Badge>
              ))}
              {paper.doi && <Badge variant="secondary">DOI</Badge>}
              {paper.journal_ref && (
                <Badge variant="outline">
                  {paper.journal_ref.split(/[,.(]/)[0]?.trim()}
                </Badge>
              )}
            </div>
            <div className="mt-1.5 flex items-center gap-3 text-[10px] text-muted-foreground">
              <span className="font-mono">{paper.arxiv_id}</span>
              {paper.page_count > 0 && (
                <span className="flex items-center gap-0.5">
                  <FileText className="size-2.5" />
                  {paper.page_count}p
                </span>
              )}
              {paper.figure_count > 0 && (
                <span className="flex items-center gap-0.5">
                  <BookOpen className="size-2.5" />
                  {paper.figure_count} fig
                </span>
              )}
              {paper.version_count > 1 && (
                <span className="flex items-center gap-0.5">
                  <GitBranch className="size-2.5" />
                  v{paper.version_count}
                </span>
              )}
              {paper.submitted_date && <span>{paper.submitted_date}</span>}
            </div>
          </div>
          {paper.score > 0 && (
            <div className="flex flex-col items-center">
              <div className="flex h-9 w-9 items-center justify-center border text-xs font-bold tabular-nums">
                {scorePercent}
              </div>
              <span className="mt-0.5 text-[10px] text-muted-foreground">
                match
              </span>
            </div>
          )}
        </CardContent>
      </Card>
    </button>
  );
}
