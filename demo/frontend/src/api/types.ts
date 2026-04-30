export interface PaperSummary {
  node_id: number;
  arxiv_id: string;
  title: string;
  authors: string[];
  year: number;
  categories: string;
  score: number;
  doi: string;
  journal_ref: string;
  submitted_date: string;
  page_count: number;
  figure_count: number;
  version_count: number;
}

export interface AuthorRef {
  node_id: number;
  name: string;
  position: number;
}

export interface PaperRef {
  node_id: number;
  arxiv_id: string;
  title: string;
  year: number;
}

export interface PaperDetail {
  node_id: number;
  arxiv_id: string;
  title: string;
  abstract: string;
  authors: AuthorRef[];
  year: number;
  categories: string;
  cites: PaperRef[];
  cited_by: PaperRef[];
  doi: string;
  journal_ref: string;
  submitted_date: string;
  page_count: number;
  figure_count: number;
  version_count: number;
  comments: string;
}

export interface AuthorDetail {
  node_id: number;
  name: string;
  paper_count: number;
  papers: PaperRef[];
}

export interface GraphNode {
  id: number;
  type: string;
  label: string;
  metadata: Record<string, string | number>;
}

export interface GraphEdge {
  id: number;
  source: number;
  target: number;
  type: string;
}

export interface GraphData {
  nodes: GraphNode[];
  edges: GraphEdge[];
}

export interface SearchResponse {
  query: string;
  tag: string;
  results: PaperSummary[];
}

export interface SearchFilters {
  yearMin?: number;
  yearMax?: number;
  category?: string;
  publishedOnly: boolean;
}

export interface PaperStats {
  node_id: number;
  citation_count: number;
  cited_by_count: number;
  author_count: number;
  categories: string[];
}

export interface DiscoveryResult {
  cited: PaperSummary[];
  undiscovered: PaperSummary[];
}

