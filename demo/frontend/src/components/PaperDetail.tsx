import {
  ArrowSquareOut,
  BookOpen,
  CalendarDots,
  FileText,
  GitBranch,
  LinkSimple,
} from "@phosphor-icons/react";
import { Badge } from "@/components/ui/badge";
import { Separator } from "@/components/ui/separator";
import type { PaperDetail as PaperDetailType } from "@/api/types";

interface PaperDetailProps {
  paper: PaperDetailType;
  onNavigate?: (nodeId: number) => void;
}

export default function PaperDetailView({
  paper,
  onNavigate,
}: PaperDetailProps) {
  const cats = paper.categories.split(/\s+/);

  return (
    <div className="space-y-4">
      <div>
        <h1 className="text-sm font-bold leading-tight">{paper.title}</h1>
        <div className="mt-2 flex flex-wrap gap-1.5">
          <Badge variant="outline">{paper.year}</Badge>
          {cats.map((cat) => (
            <Badge key={cat} variant="secondary">
              {cat}
            </Badge>
          ))}
          <a
            href={`https://arxiv.org/abs/${paper.arxiv_id}`}
            target="_blank"
            rel="noopener noreferrer"
            className="inline-flex items-center gap-1 text-xs text-muted-foreground hover:text-foreground transition-colors"
          >
            arXiv:{paper.arxiv_id}
            <ArrowSquareOut className="size-3" />
          </a>
        </div>
      </div>

      <div className="grid grid-cols-2 gap-x-4 gap-y-1.5 text-xs">
        {paper.submitted_date && (
          <div className="flex items-center gap-1.5 text-muted-foreground">
            <CalendarDots className="size-3 shrink-0" />
            <span>{paper.submitted_date}</span>
          </div>
        )}
        {paper.page_count > 0 && (
          <div className="flex items-center gap-1.5 text-muted-foreground">
            <FileText className="size-3 shrink-0" />
            <span>{paper.page_count} pages</span>
          </div>
        )}
        {paper.figure_count > 0 && (
          <div className="flex items-center gap-1.5 text-muted-foreground">
            <BookOpen className="size-3 shrink-0" />
            <span>{paper.figure_count} figures</span>
          </div>
        )}
        {paper.version_count > 0 && (
          <div className="flex items-center gap-1.5 text-muted-foreground">
            <GitBranch className="size-3 shrink-0" />
            <span>
              {paper.version_count} version
              {paper.version_count !== 1 && "s"}
            </span>
          </div>
        )}
        {paper.doi && (
          <div className="flex items-center gap-1.5 text-muted-foreground col-span-2">
            <LinkSimple className="size-3 shrink-0" />
            <a
              href={`https://doi.org/${paper.doi}`}
              target="_blank"
              rel="noopener noreferrer"
              className="truncate hover:text-foreground transition-colors"
            >
              {paper.doi}
            </a>
          </div>
        )}
        {paper.journal_ref && (
          <div className="flex items-center gap-1.5 text-muted-foreground col-span-2">
            <BookOpen className="size-3 shrink-0" />
            <span className="truncate">{paper.journal_ref}</span>
          </div>
        )}
      </div>

      {paper.comments && (
        <p className="text-xs text-muted-foreground italic">
          {paper.comments}
        </p>
      )}

      <div>
        <h2 className="mb-1.5 text-[10px] font-semibold uppercase tracking-widest text-muted-foreground">
          Authors
        </h2>
        <div className="flex flex-wrap gap-1">
          {paper.authors.map((a) => (
            <button
              key={a.node_id}
              onClick={() => onNavigate?.(a.node_id)}
              className="inline-flex h-5 items-center border px-1.5 text-[10px] font-medium text-muted-foreground hover:text-foreground hover:bg-muted transition-colors"
            >
              {a.name}
            </button>
          ))}
        </div>
      </div>

      <Separator />

      <div>
        <h2 className="mb-1.5 text-[10px] font-semibold uppercase tracking-widest text-muted-foreground">
          Abstract
        </h2>
        <p className="text-xs leading-relaxed text-muted-foreground">
          {paper.abstract}
        </p>
      </div>

      {paper.cites.length > 0 && (
        <>
          <Separator />
          <div>
            <h2 className="mb-1.5 text-[10px] font-semibold uppercase tracking-widest text-muted-foreground">
              References ({paper.cites.length})
            </h2>
            <div className="space-y-0.5">
              {paper.cites.map((ref) => (
                <button
                  key={ref.node_id}
                  onClick={() => onNavigate?.(ref.node_id)}
                  className="block w-full text-left px-2 py-1.5 text-xs transition-colors hover:bg-muted"
                >
                  {ref.title}{" "}
                  <span className="text-muted-foreground">({ref.year})</span>
                </button>
              ))}
            </div>
          </div>
        </>
      )}

      {paper.cited_by.length > 0 && (
        <>
          <Separator />
          <div>
            <h2 className="mb-1.5 text-[10px] font-semibold uppercase tracking-widest text-muted-foreground">
              Cited By ({paper.cited_by.length})
            </h2>
            <div className="space-y-0.5">
              {paper.cited_by.map((ref) => (
                <button
                  key={ref.node_id}
                  onClick={() => onNavigate?.(ref.node_id)}
                  className="block w-full text-left px-2 py-1.5 text-xs transition-colors hover:bg-muted"
                >
                  {ref.title}{" "}
                  <span className="text-muted-foreground">({ref.year})</span>
                </button>
              ))}
            </div>
          </div>
        </>
      )}
    </div>
  );
}
