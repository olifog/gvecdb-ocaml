import { Funnel } from "@phosphor-icons/react";
import { useState } from "react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import type { SearchFilters } from "@/api/types";

interface FilterPanelProps {
  filters: SearchFilters;
  onChange: (filters: SearchFilters) => void;
}

const CATEGORIES = [
  { value: "cs.AI", label: "AI" },
  { value: "cs.LG", label: "ML" },
  { value: "cs.CL", label: "NLP" },
  { value: "cs.CV", label: "Vision" },
  { value: "cs.IR", label: "IR" },
] as const;

export default function FilterPanel({ filters, onChange }: FilterPanelProps) {
  const [open, setOpen] = useState(false);

  const activeCount =
    (filters.yearMin != null ? 1 : 0) +
    (filters.yearMax != null ? 1 : 0) +
    (filters.category ? 1 : 0) +
    (filters.publishedOnly ? 1 : 0);

  const selectedCats = new Set(filters.category?.split(",").filter(Boolean) ?? []);

  const toggleCategory = (cat: string) => {
    const next = new Set(selectedCats);
    if (next.has(cat)) {
      next.delete(cat);
    } else {
      next.add(cat);
    }
    onChange({
      ...filters,
      category: next.size > 0 ? [...next].join(",") : undefined,
    });
  };

  return (
    <div>
      <Button
        variant={activeCount > 0 ? "secondary" : "outline"}
        size="sm"
        onClick={() => setOpen(!open)}
      >
        <Funnel data-icon="inline-start" />
        Filters
        {activeCount > 0 && (
          <Badge variant="default" className="ml-1 h-4 min-w-4 px-1">
            {activeCount}
          </Badge>
        )}
      </Button>

      {open && (
        <div className="mt-2 border bg-card p-3 space-y-3">
          <div className="flex items-center gap-2">
            <span className="text-[10px] font-semibold uppercase tracking-widest text-muted-foreground w-12 shrink-0">
              Year
            </span>
            <Input
              type="number"
              placeholder="From"
              value={filters.yearMin ?? ""}
              onChange={(e) =>
                onChange({
                  ...filters,
                  yearMin: e.target.value ? Number(e.target.value) : undefined,
                })
              }
              className="w-20"
            />
            <span className="text-muted-foreground text-xs">—</span>
            <Input
              type="number"
              placeholder="To"
              value={filters.yearMax ?? ""}
              onChange={(e) =>
                onChange({
                  ...filters,
                  yearMax: e.target.value ? Number(e.target.value) : undefined,
                })
              }
              className="w-20"
            />
          </div>

          <div className="flex items-center gap-2">
            <span className="text-[10px] font-semibold uppercase tracking-widest text-muted-foreground w-12 shrink-0">
              Area
            </span>
            <div className="flex flex-wrap gap-1">
              {CATEGORIES.map((cat) => (
                <button
                  key={cat.value}
                  onClick={() => toggleCategory(cat.value)}
                  className={
                    selectedCats.has(cat.value)
                      ? "inline-flex h-5 items-center px-2 text-xs font-medium bg-primary text-primary-foreground"
                      : "inline-flex h-5 items-center px-2 text-xs font-medium border text-muted-foreground hover:text-foreground"
                  }
                >
                  {cat.label}
                </button>
              ))}
            </div>
          </div>

          <div className="flex items-center gap-2">
            <span className="text-[10px] font-semibold uppercase tracking-widest text-muted-foreground w-12 shrink-0" />
            <button
              onClick={() =>
                onChange({ ...filters, publishedOnly: !filters.publishedOnly })
              }
              className={
                filters.publishedOnly
                  ? "inline-flex h-5 items-center px-2 text-xs font-medium bg-primary text-primary-foreground"
                  : "inline-flex h-5 items-center px-2 text-xs font-medium border text-muted-foreground hover:text-foreground"
              }
            >
              Published only (has DOI)
            </button>
            {activeCount > 0 && (
              <Button
                variant="ghost"
                size="xs"
                onClick={() =>
                  onChange({ publishedOnly: false })
                }
              >
                Clear all
              </Button>
            )}
          </div>
        </div>
      )}
    </div>
  );
}
