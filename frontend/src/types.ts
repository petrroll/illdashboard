export interface LabFile {
  id: number;
  filename: string;
  filepath: string;
  mime_type: string;
  uploaded_at: string;
  ocr_raw: string | null;
  lab_date: string | null;
}

export interface Measurement {
  id: number;
  lab_file_id: number;
  marker_name: string;
  value: number;
  unit: string | null;
  reference_low: number | null;
  reference_high: number | null;
  measured_at: string | null;
}

export interface ExplainRequest {
  marker_name: string;
  value: number;
  unit?: string | null;
  reference_low?: number | null;
  reference_high?: number | null;
}

export interface ExplainResponse {
  explanation: string;
}
