import React, { useMemo } from "react";
import { marked } from "marked";

export default function MarkdownTable({ markdown }) {
  const html = useMemo(() => {
    if (!markdown) return "";
    return marked.parse(markdown);
  }, [markdown]);

  if (!markdown) return null;

  return (
    <div className="tableWrap" dangerouslySetInnerHTML={{ __html: html }} />
  );
}
