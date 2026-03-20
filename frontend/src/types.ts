export interface FileProgress {
  measurement_pages_done: number;
  measurement_pages_total: number;
  text_pages_done: number;
  text_pages_total: number;
  ready_measurements: number;
  total_measurements: number;
  summary_ready: boolean;
  source_ready: boolean;
  search_ready: boolean;
  measurement_error_count: number;
  is_complete: boolean;
}

export interface LabFile {
  id: number;
  filename: string;
  filepath: string;
  mime_type: string;
  page_count: number;
  status: "uploaded" | "queued" | "processing" | "complete" | "error";
  processing_error: string | null;
  uploaded_at: string;
  ocr_raw: string | null;
  ocr_text_raw: string | null;
  ocr_text_english: string | null;
  ocr_summary_english: string | null;
  lab_date: string | null;
  source_name: string | null;
  text_assembled_at: string | null;
  summary_generated_at: string | null;
  source_resolved_at: string | null;
  search_indexed_at: string | null;
  tags: string[];
  progress: FileProgress;
}

export interface Measurement {
  id: number;
  lab_file_id: number;
  lab_file_filename?: string | null;
  lab_file_source_tag?: string | null;
  marker_name: string;
  canonical_unit?: string | null;
  canonical_value: number | null;
  original_value?: number | null;
  original_qualitative_value?: string | null;
  qualitative_bool?: boolean | null;
  qualitative_value: string | null;
  original_unit?: string | null;
  unit_conversion_missing?: boolean;
  canonical_reference_low: number | null;
  canonical_reference_high: number | null;
  original_reference_low?: number | null;
  original_reference_high?: number | null;
  measured_at: string | null;
  page_number: number | null;
}

export interface MarkerOverviewItem {
  marker_name: string;
  group_name: string;
  canonical_unit?: string | null;
  latest_measurement: Measurement;
  previous_measurement: Measurement | null;
  reference_low: number | null;
  reference_high: number | null;
  status: "low" | "high" | "in_range" | "no_range" | "positive" | "negative";
  range_position: number | null;
  has_numeric_history: boolean;
  has_qualitative_trend: boolean;
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
  canonical_unit?: string | null;
  latest_measurement: Measurement;
  previous_measurement: Measurement | null;
  reference_low: number | null;
  reference_high: number | null;
  status: "low" | "high" | "in_range" | "no_range" | "positive" | "negative";
  range_position: number | null;
  has_numeric_history: boolean;
  has_qualitative_trend: boolean;
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

export interface RescalingRule {
  id: number;
  original_unit: string;
  canonical_unit: string;
  scale_factor: number | null;
  marker_name: string | null;
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
