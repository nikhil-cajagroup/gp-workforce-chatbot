import React from "react";

export default function MessageBubble({ role, content }) {
  const isUser = role === "user";

  return (
    <div className={`msgRow ${isUser ? "right" : "left"}`}>
      <div className={`bubble ${isUser ? "user" : "bot"}`}>
        <div className="roleTag">{isUser ? "You" : "GP Workforce Bot"}</div>
        <div className="msgText">{content}</div>
      </div>
    </div>
  );
}
