import TagSelector from "./TagSelector";

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
  return (
    <TagSelector
      tags={selected}
      allTags={allTags}
      onChange={onChange}
      placeholder={label}
      variant="filter"
      allowCreate={false}
    />
  );
}
