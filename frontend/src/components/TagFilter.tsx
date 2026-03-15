import { useEffect, useRef, useState } from "react";

interface TagFilterProps {
  selected: string[];
  allTags: string[];
  onChange: (tags: string[]) => void;
  label?: string;
}

export default function TagFilter({
  selected,
  allTags,
  onChange,
  label = "Filter by tags",
}: TagFilterProps) {
  const [input, setInput] = useState("");
  const [showSuggestions, setShowSuggestions] = useState(false);
  const [highlightIndex, setHighlightIndex] = useState(-1);
  const wrapperRef = useRef<HTMLDivElement>(null);

  const suggestions = input.trim()
    ? allTags.filter(
        (t) => t.toLowerCase().includes(input.toLowerCase()) && !selected.includes(t),
      )
    : allTags.filter((t) => !selected.includes(t));

  useEffect(() => {
    const handleClick = (e: MouseEvent) => {
      if (wrapperRef.current && !wrapperRef.current.contains(e.target as Node)) {
        setShowSuggestions(false);
      }
    };
    document.addEventListener("mousedown", handleClick);
    return () => document.removeEventListener("mousedown", handleClick);
  }, []);

  const add = (tag: string) => {
    if (!selected.includes(tag)) {
      onChange([...selected, tag]);
    }
    setInput("");
    setHighlightIndex(-1);
    setShowSuggestions(true);
  };

  const remove = (tag: string) => {
    onChange(selected.filter((t) => t !== tag));
  };

  const highlightedSuggestion =
    showSuggestions && highlightIndex >= 0 ? suggestions[highlightIndex] : undefined;

  const exactMatch = input.trim()
    ? allTags.find(
        (tag) => tag.toLowerCase() === input.trim().toLowerCase() && !selected.includes(tag),
      )
    : undefined;

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === "Enter") {
      e.preventDefault();
      if (highlightedSuggestion) {
        add(highlightedSuggestion);
      } else if (exactMatch) {
        add(exactMatch);
      }
    } else if (e.key === "Backspace" && !input && selected.length > 0) {
      remove(selected[selected.length - 1]);
    } else if (e.key === "ArrowDown") {
      e.preventDefault();
      setHighlightIndex((i) => Math.min(i + 1, suggestions.length - 1));
    } else if (e.key === "ArrowUp") {
      e.preventDefault();
      setHighlightIndex((i) => Math.max(i - 1, 0));
    } else if (e.key === "Escape") {
      setShowSuggestions(false);
      setHighlightIndex(-1);
    }
  };

  return (
    <div className="tag-filter-wrapper" ref={wrapperRef}>
      <div className="tag-input-box">
        {selected.map((tag) => (
          <span key={tag} className="tag-pill tag-pill-filter">
            {tag}
            <button type="button" onClick={() => remove(tag)} aria-label={`Remove ${tag}`}>
              ×
            </button>
          </span>
        ))}
        <input
          type="text"
          value={input}
          onChange={(e) => {
            setInput(e.target.value);
            setShowSuggestions(true);
            setHighlightIndex(-1);
          }}
          onFocus={() => setShowSuggestions(true)}
          onKeyDown={handleKeyDown}
          placeholder={selected.length === 0 ? label : ""}
          className="tag-input-field"
        />
      </div>
      {showSuggestions && suggestions.length > 0 && (
        <ul className="tag-suggestions">
          {suggestions.map((s, i) => (
            <li
              key={s}
              className={i === highlightIndex ? "highlighted" : ""}
              onMouseDown={(e) => {
                e.preventDefault();
                add(s);
              }}
            >
              {s}
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}
