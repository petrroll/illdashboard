export interface LabFile {
  id: number;
  filename: string;
  filepath: string;
  mime_type: string;
  uploaded_at: string;
  ocr_raw: string | null;
  ocr_text_raw: string | null;
  ocr_text_english: string | null;
  ocr_summary_english: string | null;
  lab_date: string | null;
  tags: string[];
}

export interface Measurement {
  id: number;
  lab_file_id: number;
  lab_file_filename?: string | null;
  lab_file_source_tag?: string | null;
  marker_name: string;
  value: number | null;
  qualitative_value: string | null;
  unit: string | null;
  reference_low: number | null;
  reference_high: number | null;
  measured_at: string | null;
  page_number: number | null;
}

export interface MarkerOverviewItem {
  marker_name: string;
  group_name: string;
  latest_measurement: Measurement;
  previous_measurement: Measurement | null;
  status: "low" | "high" | "in_range" | "no_range";
  range_position: number | null;
  total_count: number;
  value_min: number | null;
  value_max: number | null;
  tags: string[];
  marker_tags: string[];
  file_tags: string[];
}

export interface MarkerOverviewGroup {
  group_name: string;
  markers: MarkerOverviewItem[];
}

export interface MarkerDetailResponse {
  marker_name: string;
  group_name: string;
  latest_measurement: Measurement;
  previous_measurement: Measurement | null;
  status: "low" | "high" | "in_range" | "no_range";
  range_position: number | null;
  measurements: Measurement[];
  explanation: string | null;
  explanation_cached: boolean;
  tags: string[];
  marker_tags: string[];
  file_tags: string[];
}

export interface MarkerInsightResponse {
  marker_name: string;
  explanation: string;
  explanation_cached: boolean;
}

export interface ExplainRequest {
  marker_name: string;
  value?: number | null;
  qualitative_value?: string | null;
  unit?: string | null;
  reference_low?: number | null;
  reference_high?: number | null;
}

export interface ExplainResponse {
  explanation: string;
}

export interface NormalizeMarkersResponse {
  updated: number;
}

export interface SearchSnippet {
  source: string;
  text: string;
}

export interface SearchResult {
  file_id: number;
  filename: string;
  uploaded_at: string;
  lab_date: string | null;
  tags: string[];
  marker_names: string[];
  snippets: SearchSnippet[];
}
