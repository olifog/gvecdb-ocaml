import { MagnifyingGlass } from "@phosphor-icons/react";
import { useCallback, useEffect, useRef, useState } from "react";
import { Input } from "@/components/ui/input";
import { cn } from "@/lib/utils";

interface SearchBarProps {
  onSearch: (query: string, tag: string) => void;
  initialQuery?: string;
  initialTag?: string;
}

const TAGS = [
  { value: "abstract_embedding", label: "Abstract" },
  { value: "title_embedding", label: "Title" },
] as const;

export default function SearchBar({
  onSearch,
  initialQuery = "",
  initialTag = "abstract_embedding",
}: SearchBarProps) {
  const [query, setQuery] = useState(initialQuery);
  const [tag, setTag] = useState(initialTag);
  const timerRef = useRef<ReturnType<typeof setTimeout>>(undefined);

  const triggerSearch = useCallback(
    (q: string, t: string) => {
      clearTimeout(timerRef.current);
      timerRef.current = setTimeout(() => {
        if (q.trim()) {
          onSearch(q.trim(), t);
        }
      }, 300);
    },
    [onSearch],
  );

  useEffect(() => () => clearTimeout(timerRef.current), []);

  return (
    <div className="flex gap-2">
      <div className="relative flex-1">
        <MagnifyingGlass className="absolute left-2.5 top-1/2 size-3.5 -translate-y-1/2 text-muted-foreground" />
        <Input
          value={query}
          onChange={(e) => {
            setQuery(e.target.value);
            triggerSearch(e.target.value, tag);
          }}
          onKeyDown={(e) => {
            if (e.key === "Enter" && query.trim()) {
              clearTimeout(timerRef.current);
              onSearch(query.trim(), tag);
            }
          }}
          placeholder="Search papers by concept, topic, or question..."
          className="pl-8"
          autoFocus
        />
      </div>
      <div className="flex overflow-hidden border">
        {TAGS.map((t) => (
          <button
            key={t.value}
            onClick={() => {
              setTag(t.value);
              if (query.trim()) {
                clearTimeout(timerRef.current);
                onSearch(query.trim(), t.value);
              }
            }}
            className={cn(
              "px-2.5 py-1.5 text-xs font-medium transition-colors",
              tag === t.value
                ? "bg-primary text-primary-foreground"
                : "bg-background text-muted-foreground hover:bg-muted hover:text-foreground",
            )}
          >
            {t.label}
          </button>
        ))}
      </div>
    </div>
  );
}
