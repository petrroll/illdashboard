import { useEffect, useMemo, useRef, useState } from "react";

interface TagSelectorProps {
  tags: string[];
  allTags: string[];
  onChange: (tags: string[]) => void;
  placeholder: string;
  variant?: "default" | "filter";
  allowCreate?: boolean;
}

export default function TagSelector({
  tags,
  allTags,
  onChange,
  placeholder,
  variant = "default",
  allowCreate = true,
}: TagSelectorProps) {
  const [input, setInput] = useState("");
  const [showSuggestions, setShowSuggestions] = useState(false);
  const [highlightIndex, setHighlightIndex] = useState(-1);
  const wrapperRef = useRef<HTMLDivElement>(null);
  const normalizedInput = input.trim().toLowerCase();

  const suggestions = useMemo(() => {
    return allTags.filter((tag) => {
      const matchesSearch = normalizedInput
        ? tag.toLowerCase().includes(normalizedInput)
        : true;
      return matchesSearch && !tags.includes(tag);
    });
  }, [allTags, normalizedInput, tags]);

  useEffect(() => {
    const handleClick = (event: MouseEvent) => {
      if (wrapperRef.current && !wrapperRef.current.contains(event.target as Node)) {
        setShowSuggestions(false);
      }
    };

    document.addEventListener("mousedown", handleClick);
    return () => document.removeEventListener("mousedown", handleClick);
  }, []);

  const highlightedSuggestion =
    showSuggestions && highlightIndex >= 0 ? suggestions[highlightIndex] : undefined;

  const exactMatch = normalizedInput
    ? allTags.find(
        (tag) => tag.toLowerCase() === normalizedInput && !tags.includes(tag),
      )
    : undefined;

  const addTag = (nextTag: string) => {
    const trimmedTag = nextTag.trim();
    if (!trimmedTag || tags.includes(trimmedTag)) {
      setInput("");
      setHighlightIndex(-1);
      return;
    }

    onChange([...tags, trimmedTag]);
    setInput("");
    setHighlightIndex(-1);
    setShowSuggestions(true);
  };

  const removeTag = (tagToRemove: string) => {
    onChange(tags.filter((tag) => tag !== tagToRemove));
  };

  const handleEnter = () => {
    if (highlightedSuggestion) {
      addTag(highlightedSuggestion);
      return;
    }

    if (exactMatch) {
      addTag(exactMatch);
      return;
    }

    if (allowCreate && input.trim()) {
      addTag(input);
    }
  };

  const handleKeyDown = (event: React.KeyboardEvent<HTMLInputElement>) => {
    switch (event.key) {
      case "Enter":
        event.preventDefault();
        handleEnter();
        break;
      case "Backspace":
        if (!input && tags.length > 0) {
          removeTag(tags[tags.length - 1]);
        }
        break;
      case "ArrowDown":
        event.preventDefault();
        setHighlightIndex((currentIndex) =>
          Math.min(currentIndex + 1, suggestions.length - 1),
        );
        break;
      case "ArrowUp":
        event.preventDefault();
        setHighlightIndex((currentIndex) => Math.max(currentIndex - 1, 0));
        break;
      case "Escape":
        setShowSuggestions(false);
        setHighlightIndex(-1);
        break;
    }
  };

  return (
    <div
      className={variant === "filter" ? "tag-filter-wrapper" : "tag-input-wrapper"}
      ref={wrapperRef}
    >
      <div className="tag-input-box">
        {tags.map((tag) => (
          <span
            key={tag}
            className={`tag-pill${variant === "filter" ? " tag-pill-filter" : ""}`}
          >
            {tag}
            <button type="button" onClick={() => removeTag(tag)} aria-label={`Remove ${tag}`}>
              ×
            </button>
          </span>
        ))}
        <input
          type="text"
          value={input}
          onChange={(event) => {
            setInput(event.target.value);
            setShowSuggestions(true);
            setHighlightIndex(-1);
          }}
          onFocus={() => setShowSuggestions(true)}
          onKeyDown={handleKeyDown}
          placeholder={tags.length === 0 ? placeholder : ""}
          className="tag-input-field"
        />
      </div>
      {showSuggestions && suggestions.length > 0 && (
        <ul className="tag-suggestions">
          {suggestions.map((suggestion, index) => (
            <li
              key={suggestion}
              className={index === highlightIndex ? "highlighted" : ""}
              onMouseDown={(event) => {
                event.preventDefault();
                addTag(suggestion);
              }}
            >
              {suggestion}
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}