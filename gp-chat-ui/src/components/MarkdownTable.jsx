import React, { useMemo } from "react";
import { marked } from "marked";
import DOMPurify from "dompurify";

export default function MarkdownTable({ markdown }) {
  const html = useMemo(() => {
    if (!markdown) return "";
    const raw = marked.parse(markdown);
    return DOMPurify.sanitize(raw, {
      ALLOWED_TAGS: ["table", "thead", "tbody", "tr", "th", "td", "p",
                      "strong", "em", "br", "code", "pre", "span", "div"],
      ALLOWED_ATTR: ["class"],
    });
  }, [markdown]);

  if (!markdown) return null;

  return (
    <div className="tableWrap" role="region" aria-label="Data table" dangerouslySetInnerHTML={{ __html: html }} />
  );
}
