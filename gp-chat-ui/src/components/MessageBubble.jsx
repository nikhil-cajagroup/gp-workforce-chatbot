import React, { useMemo } from "react";
import { marked } from "marked";
import DOMPurify from "dompurify";

export default function MessageBubble({ role, content }) {
  const isUser = role === "user";

  const html = useMemo(() => {
    if (isUser || !content) return null;
    const raw = marked.parse(content);
    return DOMPurify.sanitize(raw, {
      ALLOWED_TAGS: ["p", "br", "strong", "em", "b", "i", "ul", "ol", "li",
                      "code", "pre", "table", "thead", "tbody", "tr", "th",
                      "td", "h1", "h2", "h3", "h4", "h5", "h6", "a", "span", "div"],
      ALLOWED_ATTR: ["href", "target", "rel", "class"],
    });
  }, [content, isUser]);

  return (
    <div className={`msgRow ${isUser ? "right" : "left"}`}>
      <div className={`bubble ${isUser ? "user" : "bot"}`}>
        <div className="roleTag" aria-hidden="true">
          {isUser ? "You" : "GP Workforce Bot"}
        </div>
        {isUser ? (
          <div className="msgText">{content}</div>
        ) : (
          <div
            className="msgText botMarkdown"
            dangerouslySetInnerHTML={{ __html: html }}
          />
        )}
      </div>
    </div>
  );
}
