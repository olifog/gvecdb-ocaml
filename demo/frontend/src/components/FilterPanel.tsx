import { Funnel, X } from "@phosphor-icons/react";
import { useState } from "react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import type { SearchFilters } from "@/api/types";

interface FilterPanelProps {
  filters: SearchFilters;
  onChange: (filters: SearchFilters) => void;
}

export default function FilterPanel({ filters, onChange }: FilterPanelProps) {
  const [open, setOpen] = useState(false);
  const [catInput, setCatInput] = useState("");

  const activeCount =
    (filters.yearMin != null ? 1 : 0) +
    (filters.yearMax != null ? 1 : 0) +
    (filters.category ? 1 : 0) +
    (filters.publishedOnly ? 1 : 0);

  const selectedCats = filters.category?.split(",").filter(Boolean) ?? [];

  const addCategory = (cat: string) => {
    const trimmed = cat.trim();
    if (!trimmed || selectedCats.includes(trimmed)) return;
    const next = [...selectedCats, trimmed];
    onChange({ ...filters, category: next.join(",") });
    setCatInput("");
  };

  const removeCategory = (cat: string) => {
    const next = selectedCats.filter((c) => c !== cat);
    onChange({
      ...filters,
      category: next.length > 0 ? next.join(",") : undefined,
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

          <div className="flex items-start gap-2">
            <span className="text-[10px] font-semibold uppercase tracking-widest text-muted-foreground w-12 shrink-0 pt-1">
              Area
            </span>
            <div className="flex-1 space-y-1.5">
              <Input
                placeholder="e.g. cs.AI, math.CO, physics.optics"
                value={catInput}
                onChange={(e) => setCatInput(e.target.value)}
                onKeyDown={(e) => {
                  if (e.key === "Enter" || e.key === ",") {
                    e.preventDefault();
                    addCategory(catInput);
                  }
                }}
                className="h-7 text-xs"
              />
              {selectedCats.length > 0 && (
                <div className="flex flex-wrap gap-1">
                  {selectedCats.map((cat) => (
                    <button
                      key={cat}
                      onClick={() => removeCategory(cat)}
                      className="inline-flex h-5 items-center gap-0.5 bg-primary text-primary-foreground px-1.5 text-[10px] font-medium"
                    >
                      {cat}
                      <X className="size-2.5" />
                    </button>
                  ))}
                </div>
              )}
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
                onClick={() => onChange({ publishedOnly: false })}
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
