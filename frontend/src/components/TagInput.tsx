import TagSelector from "./TagSelector";

interface TagInputProps {
  tags: string[];
  allTags: string[];
  onChange: (tags: string[]) => void;
  placeholder?: string;
}

export default function TagInput({ tags, allTags, onChange, placeholder = "Add tag…" }: TagInputProps) {
  return (
    <TagSelector
      tags={tags}
      allTags={allTags}
      onChange={onChange}
      placeholder={placeholder}
      allowCreate
    />
  );
}
