import { Gear } from "@phosphor-icons/react";
import { useState } from "react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";

export interface GraphSettings {
  showPapers: boolean;
  showAuthors: boolean;
  showCites: boolean;
  showAuthored: boolean;
  colorBy: "type" | "year" | "paperCount";
  paperColor: string;
  authorColor: string;
  citesColor: string;
  authoredColor: string;
}

export const DEFAULT_GRAPH_SETTINGS: GraphSettings = {
  showPapers: true,
  showAuthors: true,
  showCites: true,
  showAuthored: true,
  colorBy: "type",
  paperColor: "#3b82f6",
  authorColor: "#22c55e",
  citesColor: "#64748b",
  authoredColor: "#f97316",
};

interface GraphControlsProps {
  settings: GraphSettings;
  onChange: (settings: GraphSettings) => void;
}

function Toggle({
  label,
  checked,
  color,
  onToggle,
}: {
  label: string;
  checked: boolean;
  color?: string;
  onToggle: () => void;
}) {
  return (
    <button onClick={onToggle} className="flex items-center gap-1.5 text-xs">
      <span
        className="inline-block h-2.5 w-2.5 rounded-sm border"
        style={{
          backgroundColor: checked ? (color ?? "#fff") : "transparent",
          borderColor: color ?? "#666",
        }}
      />
      <span className={checked ? "text-foreground" : "text-muted-foreground line-through"}>
        {label}
      </span>
    </button>
  );
}

export default function GraphControls({ settings, onChange }: GraphControlsProps) {
  const [open, setOpen] = useState(false);

  const set = <K extends keyof GraphSettings>(key: K, value: GraphSettings[K]) =>
    onChange({ ...settings, [key]: value });

  return (
    <div className="relative">
      <Button
        variant="secondary"
        size="icon-sm"
        onClick={() => setOpen(!open)}
        title="Graph settings"
      >
        <Gear className="size-4" />
      </Button>

      {open && (
        <div className="absolute top-full right-0 mt-1.5 border bg-card/95 backdrop-blur-sm p-2.5 space-y-2.5 w-48 z-20">
          <div className="space-y-1">
            <span className="text-[9px] font-semibold uppercase tracking-widest text-muted-foreground">
              Nodes
            </span>
            <Toggle label="Papers" checked={settings.showPapers} color={settings.paperColor} onToggle={() => set("showPapers", !settings.showPapers)} />
            <Toggle label="Authors" checked={settings.showAuthors} color={settings.authorColor} onToggle={() => set("showAuthors", !settings.showAuthors)} />
          </div>

          <div className="space-y-1">
            <span className="text-[9px] font-semibold uppercase tracking-widest text-muted-foreground">
              Edges
            </span>
            <Toggle label="Cites" checked={settings.showCites} color={settings.citesColor} onToggle={() => set("showCites", !settings.showCites)} />
            <Toggle label="Authored" checked={settings.showAuthored} color={settings.authoredColor} onToggle={() => set("showAuthored", !settings.showAuthored)} />
          </div>

          <div className="space-y-1">
            <span className="text-[9px] font-semibold uppercase tracking-widest text-muted-foreground">
              Color by
            </span>
            <div className="flex gap-1">
              {(["type", "year", "paperCount"] as const).map((mode) => (
                <button
                  key={mode}
                  onClick={() => set("colorBy", mode)}
                  className={
                    settings.colorBy === mode
                      ? "px-1.5 py-0.5 text-[10px] font-medium bg-primary text-primary-foreground"
                      : "px-1.5 py-0.5 text-[10px] font-medium border text-muted-foreground hover:text-foreground"
                  }
                >
                  {mode === "paperCount" ? "papers" : mode}
                </button>
              ))}
            </div>
          </div>

          <div className="space-y-1">
            <span className="text-[9px] font-semibold uppercase tracking-widest text-muted-foreground">
              Colors
            </span>
            <div className="grid grid-cols-2 gap-1">
              {([
                ["paperColor", "Paper"],
                ["authorColor", "Author"],
                ["citesColor", "Cites"],
                ["authoredColor", "Authored"],
              ] as const).map(([key, label]) => (
                <label key={key} className="flex items-center gap-1 text-[10px] text-muted-foreground">
                  <Input
                    type="color"
                    value={settings[key]}
                    onChange={(e) => set(key, e.target.value)}
                    className="h-4 w-6 p-0 border-0 cursor-pointer"
                  />
                  {label}
                </label>
              ))}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
