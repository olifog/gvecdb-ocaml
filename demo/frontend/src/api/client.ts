import { useQuery } from "@tanstack/react-query";
import type {
  AuthorDetail,
  GraphData,
  PaperDetail,
  PaperSummary,
  SearchFilters,
  SearchResponse,
} from "@/api/types";

const BASE = "/api";

async function fetchJson<T>(url: string): Promise<T> {
  const res = await fetch(url);
  if (!res.ok) {
    throw new Error(`API error: ${res.status} ${res.statusText}`);
  }
  return res.json() as Promise<T>;
}

export function useSearch(
  query: string,
  k = 20,
  tag = "abstract_embedding",
  filters?: SearchFilters,
) {
  return useQuery<SearchResponse>({
    queryKey: ["search", query, k, tag, filters],
    queryFn: () => {
      const params = new URLSearchParams({
        q: query,
        k: String(k),
        tag,
      });
      if (filters?.yearMin != null) params.set("year_min", String(filters.yearMin));
      if (filters?.yearMax != null) params.set("year_max", String(filters.yearMax));
      if (filters?.category) params.set("category", filters.category);
      if (filters?.publishedOnly) params.set("published_only", "true");
      return fetchJson(`${BASE}/search?${params}`);
    },
    enabled: query.length > 0,
  });
}

export function usePaper(nodeId: number | undefined) {
  return useQuery<PaperDetail>({
    queryKey: ["paper", nodeId],
    queryFn: () => fetchJson(`${BASE}/paper/by-node/${nodeId}`),
    enabled: nodeId !== undefined,
  });
}

export function useAuthor(nodeId: number | undefined) {
  return useQuery<AuthorDetail>({
    queryKey: ["author", nodeId],
    queryFn: () => fetchJson(`${BASE}/author/${nodeId}`),
    enabled: nodeId !== undefined,
  });
}

export function useNeighborhood(nodeId: number | undefined, depth = 1) {
  return useQuery<GraphData>({
    queryKey: ["neighbourhood", nodeId, depth],
    queryFn: () =>
      fetchJson(`${BASE}/graph/neighbourhood/${nodeId}?depth=${depth}`),
    enabled: nodeId !== undefined,
  });
}

export function useSimilar(
  nodeId: number | undefined,
  k = 10,
  tag = "abstract_embedding",
) {
  return useQuery<PaperSummary[]>({
    queryKey: ["similar", nodeId, k, tag],
    queryFn: () =>
      fetchJson(
        `${BASE}/similar/${nodeId}?k=${k}&tag=${encodeURIComponent(tag)}`,
      ),
    enabled: nodeId !== undefined,
  });
}
