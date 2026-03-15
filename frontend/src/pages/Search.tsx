import { useDeferredValue, useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { fetchFileTags, searchFiles } from "../api";
import TagFilter from "../components/TagFilter";
import type { SearchResult } from "../types";
import { formatDate, formatDateTime } from "../utils/measurements";

const MIN_SEARCH_LENGTH = 2;

function getSnippetLabel(source: string | null) {
  switch (source) {
    case "summary":
      return "English summary";
    case "translated_text":
      return "English OCR text";
    case "raw_text":
      return "Raw OCR text";
    case "measurements":
      return "Measurements";
    case "tags":
      return "Tags";
    case "filename":
      return "Filename";
    default:
      return "Match";
  }
}

export default function Search() {
  const [query, setQuery] = useState("");
  const [results, setResults] = useState<SearchResult[]>([]);
  const [allTags, setAllTags] = useState<string[]>([]);
  const [filterTags, setFilterTags] = useState<string[]>([]);
  const [loading, setLoading] = useState(false);
  const deferredQuery = useDeferredValue(query.trim());

  useEffect(() => {
    fetchFileTags().then(setAllTags);
  }, []);

  useEffect(() => {
    let cancelled = false;

    const loadResults = async () => {
      if (deferredQuery.length < MIN_SEARCH_LENGTH) {
        setResults([]);
        setLoading(false);
        return;
      }

      setLoading(true);
      try {
        const response = await searchFiles(deferredQuery, filterTags);
        if (!cancelled) {
          setResults(response);
        }
      } finally {
        if (!cancelled) {
          setLoading(false);
        }
      }
    };

    void loadResults();
    return () => {
      cancelled = true;
    };
  }, [deferredQuery, filterTags]);

  const showGuidance = deferredQuery.length < MIN_SEARCH_LENGTH;

  return (
    <div className="search-page">
      <div className="search-hero card">
        <h2>Search</h2>
        <p className="search-subtitle">
          Search across file tags, English summaries, OCR text, and extracted measurements.
        </p>

        <div className="search-form-shell">
          <input
            className="search-input"
            type="search"
            value={query}
            onChange={(event) => setQuery(event.target.value)}
            placeholder="Ferritin, fasting, administrative review..."
          />

          {allTags.length > 0 && (
            <div className="search-tag-filter-row">
              <span className="search-tag-filter-label">Restrict to tags</span>
              <TagFilter
                selected={filterTags}
                allTags={allTags}
                onChange={setFilterTags}
                label="Add file tag…"
              />
            </div>
          )}
        </div>
      </div>

      {showGuidance ? (
        <div className="card search-empty-state">
          Enter at least {MIN_SEARCH_LENGTH} characters to search OCR text, tags, and measurements.
        </div>
      ) : loading ? (
        <div className="card search-empty-state">
          <span className="spinner" /> Searching…
        </div>
      ) : results.length === 0 ? (
        <div className="card search-empty-state">No files match this search.</div>
      ) : (
        <div className="search-results">
          {results.map((result) => (
            <article key={result.file_id} className="search-result-card card">
              <div className="search-result-head">
                <div>
                  <Link className="search-result-title" to={`/files/${result.file_id}`}>
                    {result.filename}
                  </Link>
                  <p className="search-result-meta">
                    Uploaded {formatDateTime(result.uploaded_at)}
                    {result.lab_date && ` · Lab date ${formatDate(result.lab_date)}`}
                  </p>
                </div>
                <Link className="btn btn-outline btn-sm" to={`/files/${result.file_id}`}>
                  Open file
                </Link>
              </div>

              {result.tags.length > 0 && (
                <div className="tag-list search-result-tags">
                  {result.tags.map((tag) => (
                    <span key={tag} className="tag-pill">
                      {tag}
                    </span>
                  ))}
                </div>
              )}

              {result.snippets.length > 0 &&
                result.snippets.map((snippet, index) => (
                  <div key={index} className="search-result-snippet">
                    <span className="search-result-snippet-label">
                      {getSnippetLabel(snippet.source)}
                    </span>
                    <p>{snippet.text}</p>
                  </div>
                ))}

              {result.marker_names.length > 0 && (
                <div className="search-result-markers">
                  <span className="search-result-markers-label">Related biomarkers</span>
                  <div className="search-result-marker-links">
                    {result.marker_names.map((markerName) => (
                      <Link key={markerName} className="tag-pill tag-pill-link" to={`/charts?marker=${encodeURIComponent(markerName)}`}>
                        {markerName}
                      </Link>
                    ))}
                  </div>
                </div>
              )}
            </article>
          ))}
        </div>
      )}
    </div>
  );
}