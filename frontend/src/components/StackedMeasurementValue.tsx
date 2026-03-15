import type { ReactNode } from "react";

interface StackedMeasurementValueProps {
  primary: ReactNode;
  secondary?: ReactNode;
}

export default function StackedMeasurementValue({
  primary,
  secondary,
}: StackedMeasurementValueProps) {
  return (
    <div className="history-measurement-cell">
      <span className="history-measurement-primary">{primary}</span>
      {secondary != null && secondary !== "" && (
        <span className="history-measurement-secondary">{secondary}</span>
      )}
    </div>
  );
}