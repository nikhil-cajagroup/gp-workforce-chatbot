import React, { useEffect, useMemo, useRef, useState } from "react";
import { sendChat } from "./api";
import MessageBubble from "./components/MessageBubble";
import MarkdownTable from "./components/MarkdownTable";

function randomSessionId() {
  return "sess_" + Math.random().toString(36).slice(2, 10);
}

export default function App() {
  const [sessionId] = useState(() => randomSessionId());
  const [input, setInput] = useState("");
  const [messages, setMessages] = useState([
    {
      role: "bot",
      content:
        "Hi! Ask me anything about GP Workforce (ICB/Sub-ICB/practice, FTE/headcount trends, demographics etc.)",
    },
  ]);

  const [loading, setLoading] = useState(false);
  const [lastResponse, setLastResponse] = useState(null);
  const [showDebug, setShowDebug] = useState(false);
  const [error, setError] = useState("");

  const scrollRef = useRef(null);

  useEffect(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, [messages, loading, lastResponse]);

  const canSend = useMemo(() => input.trim().length > 0 && !loading, [input, loading]);

  async function onSend() {
    const question = input.trim();
    if (!question) return;

    setError("");
    setInput("");
    setLoading(true);

    setMessages((prev) => [...prev, { role: "user", content: question }]);

    try {
      const data = await sendChat({ sessionId, question });
      setLastResponse(data);
      setMessages((prev) => [...prev, { role: "bot", content: data.answer }]);
    } catch (e) {
      setError(e.message || "Something went wrong.");
    } finally {
      setLoading(false);
    }
  }

  function onKeyDown(e) {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      if (canSend) onSend();
    }
  }

  return (
    <div className="page">
      <header className="topbar">
        <div className="title">
          GP Workforce Chatbot <span className="pill">Athena + Nova Pro</span>
        </div>

        <div className="topRight">
          <button
            className={`btn ${showDebug ? "btnActive" : ""}`}
            onClick={() => setShowDebug((s) => !s)}
          >
            {showDebug ? "Hide Debug" : "Show Debug"}
          </button>
        </div>
      </header>

      <main className="main">
        <section className="chatPanel">
          <div className="chatArea" ref={scrollRef}>
            {messages.map((m, idx) => (
              <MessageBubble key={idx} role={m.role} content={m.content} />
            ))}

            {loading && (
              <div className="msgRow left">
                <div className="bubble bot">
                  <div className="roleTag">GP Workforce Bot</div>
                  <div className="msgText typing">Thinking…</div>
                </div>
              </div>
            )}
          </div>

          <div className="composer">
            <textarea
              className="input"
              placeholder="Ask: Top 10 ICBs by GP FTE in latest month…"
              value={input}
              onChange={(e) => setInput(e.target.value)}
              onKeyDown={onKeyDown}
              rows={2}
            />
            <button className="sendBtn" onClick={onSend} disabled={!canSend}>
              Send
            </button>
          </div>

          {error && <div className="errorBox">⚠️ {error}</div>}
        </section>

        <section className="resultPanel">
          <div className="panelHeader">
            <div className="panelTitle">Results</div>
            <div className="sessionTag">Session: {sessionId}</div>
          </div>

          {!lastResponse ? (
            <div className="emptyState">
              Ask a question and your result table will show here.
              <div className="exampleBox">
                Try:
                <ul>
                  <li>Top 10 ICBs by GP FTE in the latest month</li>
                  <li>Trend of GP FTE over last 12 months for NHS Greater Manchester ICB</li>
                  <li>Gender breakdown of GP FTE by ICB latest month</li>
                </ul>
              </div>
            </div>
          ) : (
            <>
              <div className="card">
                <div className="cardTitle">Preview Table</div>
                <MarkdownTable markdown={lastResponse.preview_markdown} />
              </div>

              {showDebug && (
                <div className="card">
                  <div className="cardTitle">Debug</div>

                  <div className="debugBlock">
                    <div className="debugLabel">SQL</div>
                    <pre className="code">{lastResponse.sql}</pre>
                  </div>

                  <div className="debugBlock">
                    <div className="debugLabel">Meta</div>
                    <pre className="code">
                      {JSON.stringify(lastResponse.meta, null, 2)}
                    </pre>
                  </div>
                </div>
              )}
            </>
          )}
        </section>
      </main>

      <footer className="footer">
        Built for NHS open datasets • Athena = source of truth • Debug shows planner + SQL
      </footer>
    </div>
  );
}
