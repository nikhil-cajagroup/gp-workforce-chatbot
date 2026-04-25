import React, { useMemo } from "react";
import { marked } from "marked";
import DOMPurify from "dompurify";

function BotAvatarIcon() {
  return (
    <svg width="14" height="14" viewBox="0 0 24 24" fill="none">
      <path d="M22 12h-4l-3 9L9 3l-3 9H2" stroke="white" strokeWidth="2.2"
        strokeLinecap="round" strokeLinejoin="round"/>
    </svg>
  );
}

export default function MessageBubble({ role, content }) {
  const isUser = role === "user";

  const html = useMemo(() => {
    if (isUser || !content) return null;
    const raw = marked.parse(content);
    return DOMPurify.sanitize(raw, {
      ALLOWED_TAGS: [
        "p", "br", "strong", "em", "b", "i",
        "ul", "ol", "li", "code", "pre",
        "table", "thead", "tbody", "tr", "th", "td",
        "h1", "h2", "h3", "h4", "h5", "h6",
        "a", "span", "div", "blockquote",
      ],
      ALLOWED_ATTR: ["href", "target", "rel", "class"],
    });
  }, [content, isUser]);

  if (isUser) {
    return (
      <div className="msgRow right">
        <div className="bubble user">
          <div className="roleTag">You</div>
          <div className="msgText">{content}</div>
        </div>
      </div>
    );
  }

  return (
    <div className="msgRow left">
      <div className="botAvatar" aria-hidden="true">
        <BotAvatarIcon />
      </div>
      <div className="bubble bot">
        <div className="roleTag">InsightsQI</div>
        <div
          className="msgText botMarkdown"
          dangerouslySetInnerHTML={{ __html: html }}
        />
      </div>
    </div>
  );
}
