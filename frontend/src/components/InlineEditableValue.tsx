import { isAxiosError } from "axios";
import {
  type FocusEvent,
  type KeyboardEvent,
  type MouseEvent,
  type ReactNode,
  useEffect,
  useRef,
  useState,
} from "react";

const EDITED_VALUE_TOOLTIP = "Value has been changed. Click to reset to original value.";
const EDITED_VALUE_READONLY_TOOLTIP = "Value has been changed.";

function getErrorMessage(error: unknown) {
  if (isAxiosError(error)) {
    const detail = error.response?.data?.detail;
    if (typeof detail === "string") {
      return detail;
    }
    if (Array.isArray(detail)) {
      const messages = detail
        .map((entry) => (entry && typeof entry === "object" && "msg" in entry ? entry.msg : null))
        .filter((message): message is string => typeof message === "string");
      if (messages.length > 0) {
        return messages.join(" ");
      }
    }
  }

  return error instanceof Error ? error.message : "Something went wrong.";
}

interface InlineEditableValueProps {
  display: ReactNode;
  editValue: string;
  onSave: (nextValue: string) => Promise<void>;
  onReset?: () => Promise<void>;
  edited?: boolean;
  readOnly?: boolean;
  inputType?: "text" | "date";
  placeholder?: string;
  ariaLabel: string;
  hint?: ReactNode;
  title?: string;
  monospace?: boolean;
  align?: "left" | "right";
  onViewClick?: () => void;
}

export default function InlineEditableValue({
  display,
  editValue,
  onSave,
  onReset,
  edited = false,
  readOnly = false,
  inputType = "text",
  placeholder,
  ariaLabel,
  hint,
  title,
  monospace = false,
  align = "left",
  onViewClick,
}: InlineEditableValueProps) {
  const [draft, setDraft] = useState(editValue);
  const [editing, setEditing] = useState(false);
  const [saving, setSaving] = useState(false);
  const [resetting, setResetting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const containerRef = useRef<HTMLDivElement | null>(null);
  const inputRef = useRef<HTMLInputElement | null>(null);
  const clickTimeoutRef = useRef<number | null>(null);

  useEffect(() => {
    if (!editing) {
      setDraft(editValue);
    }
  }, [editValue, editing]);

  useEffect(() => {
    return () => {
      if (clickTimeoutRef.current !== null) {
        window.clearTimeout(clickTimeoutRef.current);
      }
    };
  }, []);

  useEffect(() => {
    if (!editing) {
      return;
    }
    const frame = window.requestAnimationFrame(() => {
      inputRef.current?.focus();
      inputRef.current?.select();
    });
    return () => window.cancelAnimationFrame(frame);
  }, [editing]);

  const beginEditing = () => {
    if (readOnly || saving || resetting) {
      return;
    }
    if (clickTimeoutRef.current !== null) {
      window.clearTimeout(clickTimeoutRef.current);
      clickTimeoutRef.current = null;
    }
    setDraft(editValue);
    setError(null);
    setEditing(true);
  };

  const cancelEditing = () => {
    if (saving) {
      return;
    }
    setDraft(editValue);
    setError(null);
    setEditing(false);
  };

  const commitChanges = async () => {
    if (saving || resetting) {
      return;
    }
    setSaving(true);
    setError(null);
    try {
      await onSave(draft);
      setEditing(false);
    } catch (saveError) {
      setError(getErrorMessage(saveError));
    } finally {
      setSaving(false);
    }
  };

  const resetChanges = async () => {
    if (!onReset || saving || resetting) {
      return;
    }
    setResetting(true);
    setError(null);
    try {
      await onReset();
      setEditing(false);
    } catch (resetError) {
      setError(getErrorMessage(resetError));
    } finally {
      setResetting(false);
    }
  };

  const handleBlur = (event: FocusEvent<HTMLDivElement>) => {
    if (!editing || saving || resetting) {
      return;
    }
    const nextTarget = event.relatedTarget;
    if (nextTarget instanceof Node && containerRef.current?.contains(nextTarget)) {
      return;
    }
    void commitChanges();
  };

  const handleInputKeyDown = (event: KeyboardEvent<HTMLInputElement>) => {
    if (event.key === "Enter") {
      event.preventDefault();
      void commitChanges();
      return;
    }
    if (event.key === "Escape") {
      event.preventDefault();
      cancelEditing();
    }
  };

  const handleViewKeyDown = (event: KeyboardEvent<HTMLDivElement>) => {
    if (event.key !== "Enter" && event.key !== " " && event.key !== "F2") {
      return;
    }
    event.preventDefault();
    beginEditing();
  };

  const handleViewClick = (event: MouseEvent<HTMLDivElement>) => {
    if (!onViewClick) {
      return;
    }
    const target = event.target;
    if (target instanceof HTMLElement && target.closest("a, button, input, textarea, select, label")) {
      return;
    }
    if (clickTimeoutRef.current !== null) {
      window.clearTimeout(clickTimeoutRef.current);
    }
    clickTimeoutRef.current = window.setTimeout(() => {
      clickTimeoutRef.current = null;
      onViewClick();
    }, 220);
  };

  return (
    <div
      ref={containerRef}
      className={`inline-edit${editing ? " inline-edit--editing" : ""}`}
      onBlur={handleBlur}
    >
      {editing ? (
        <div className="inline-edit-editor">
          <input
            ref={inputRef}
            type={inputType}
            className={[
              "inline-edit-input",
              monospace ? "inline-edit-input--monospace" : "",
              align === "right" ? "inline-edit-input--right" : "",
            ]
              .filter(Boolean)
              .join(" ")}
            value={draft}
            onChange={(event) => setDraft(event.target.value)}
            onKeyDown={handleInputKeyDown}
            placeholder={placeholder}
            aria-label={ariaLabel}
            disabled={saving || resetting}
          />
          <div className="inline-edit-actions">
            <button
              type="button"
              className="btn btn-outline btn-sm inline-edit-action"
              onClick={() => void commitChanges()}
              disabled={saving || resetting}
            >
              {saving ? "Saving…" : "Save"}
            </button>
            <button
              type="button"
              className="btn btn-outline btn-sm inline-edit-action"
              onClick={cancelEditing}
              disabled={saving || resetting}
            >
              Cancel
            </button>
            {edited && onReset && (
              <button
                type="button"
                className="btn btn-outline btn-sm inline-edit-action"
                onClick={() => void resetChanges()}
                disabled={saving || resetting}
              >
                {resetting ? "Resetting…" : "Reset"}
              </button>
            )}
          </div>
          {(hint || error) && (
            <div className="inline-edit-meta">
              {hint && <div className="inline-edit-hint">{hint}</div>}
              {error && <div className="inline-edit-error">{error}</div>}
            </div>
          )}
        </div>
      ) : (
        <div
          className={`inline-edit-view${readOnly ? " inline-edit-view--readonly" : ""}`}
          onClick={handleViewClick}
          onDoubleClick={beginEditing}
          onKeyDown={readOnly ? undefined : handleViewKeyDown}
          tabIndex={readOnly ? undefined : 0}
          role={readOnly ? undefined : "button"}
          title={readOnly ? undefined : title ?? "Double-click to edit"}
        >
          <div className="inline-edit-display-group">
            <div className="inline-edit-display">{display}</div>
            {edited && onReset && !readOnly ? (
              <button
                type="button"
                className="inline-edit-badge inline-edit-badge--resettable"
                title={resetting ? "Resetting original value..." : EDITED_VALUE_TOOLTIP}
                aria-label={resetting ? "Resetting original value" : EDITED_VALUE_TOOLTIP}
                aria-busy={resetting}
                onClick={(event) => {
                  event.preventDefault();
                  event.stopPropagation();
                  void resetChanges();
                }}
                disabled={resetting}
              >
                *
              </button>
            ) : edited ? (
              <span className="inline-edit-badge" title={EDITED_VALUE_READONLY_TOOLTIP}>
                *
              </span>
            ) : null}
          </div>
        </div>
      )}
    </div>
  );
}
