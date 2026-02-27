import React, { useEffect, useMemo, useRef, useState, useCallback } from "react";
import { streamChat, fetchSuggestions } from "./api";
import MessageBubble from "./components/MessageBubble";
import MarkdownTable from "./components/MarkdownTable";

function randomSessionId() {
  // Use crypto.randomUUID for high-entropy session IDs (no collisions)
  if (typeof crypto !== "undefined" && crypto.randomUUID) {
    return "sess_" + crypto.randomUUID().replace(/-/g, "").slice(0, 16);
  }
  // Fallback for older browsers
  return "sess_" + Date.now().toString(36) + Math.random().toString(36).slice(2, 10);
}

export default function App() {
  const [sessionId] = useState(() => randomSessionId());
  const [input, setInput] = useState("");
  const [messages, setMessages] = useState([
    {
      role: "bot",
      content:
        "Hi! Ask me anything about **GP Workforce** data — ICB/Sub-ICB/practice lookups, FTE/headcount trends, demographics, staff breakdowns, and more. Try one of the suggestions below to get started.",
    },
  ]);

  const [loading, setLoading] = useState(false);
  const [progress, setProgress] = useState(null);
  const [lastResponse, setLastResponse] = useState(null);
  const [showDebug, setShowDebug] = useState(false);
  const [error, setError] = useState("");
  const [lastFailedQuestion, setLastFailedQuestion] = useState("");
  const [starterSuggestions, setStarterSuggestions] = useState([]);

  const scrollRef = useRef(null);
  const inputRef = useRef(null);
  const abortRef = useRef(null);

  // Fetch starter suggestions on mount
  useEffect(() => {
    fetchSuggestions().then(setStarterSuggestions);
  }, []);

  useEffect(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, [messages, loading, lastResponse, progress]);

  // Focus input after bot responds
  useEffect(() => {
    if (!loading && inputRef.current) {
      inputRef.current.focus();
    }
  }, [loading]);

  const canSend = useMemo(
    () => input.trim().length > 0 && !loading,
    [input, loading]
  );

  // The current follow-up suggestions from the last bot response
  const followUpSuggestions = lastResponse?.suggestions || [];

  // Show starter suggestions only if no conversation yet (only welcome message)
  const showStarters = messages.length <= 1 && starterSuggestions.length > 0;

  const onSend = useCallback(function onSend(questionOverride) {
    const question = (questionOverride || input).trim();
    if (!question) return;

    setError("");
    setLastFailedQuestion("");
    setInput("");
    setLoading(true);
    setProgress(null);

    // Cancel any in-flight request
    if (abortRef.current) {
      abortRef.current.abort();
    }
    const controller = new AbortController();
    abortRef.current = controller;

    setMessages((prev) => [...prev, { role: "user", content: question }]);

    streamChat(
      { sessionId, question },
      // onProgress
      (progressData) => {
        setProgress(progressData);
      },
      // onComplete
      (data) => {
        setLastResponse(data);
        setMessages((prev) => [...prev, { role: "bot", content: data.answer }]);
        setLoading(false);
        setProgress(null);
        abortRef.current = null;
      },
      // onError
      (errorMsg) => {
        setError(errorMsg || "Something went wrong.");
        setLastFailedQuestion(question);
        setLoading(false);
        setProgress(null);
        abortRef.current = null;
      },
      controller.signal
    );
  }, [input, sessionId]);

  function onKeyDown(e) {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      if (canSend) onSend();
    }
  }

  function onSuggestionClick(text) {
    if (!loading) {
      setInput("");
      onSend(text);
    }
  }

  function onRetry() {
    if (lastFailedQuestion) {
      onSend(lastFailedQuestion);
    }
  }

  return (
    <div className="page">
      <header className="topbar" role="banner">
        <div className="title">
          GP Workforce Chatbot{" "}
          <span className="pill">Athena + Nova Pro v5.8</span>
        </div>

        <div className="topRight">
          <button
            className={`btn ${showDebug ? "btnActive" : ""}`}
            onClick={() => setShowDebug((s) => !s)}
            aria-pressed={showDebug}
            aria-label={showDebug ? "Hide debug panel" : "Show debug panel"}
          >
            {showDebug ? "Hide Debug" : "Show Debug"}
          </button>
        </div>
      </header>

      <main className="main" role="main">
        <section className="chatPanel" aria-label="Chat conversation">
          <div className="chatArea" ref={scrollRef} role="log" aria-live="polite" aria-label="Message history">
            {messages.map((m, idx) => (
              <MessageBubble key={idx} role={m.role} content={m.content} />
            ))}

            {/* Starter suggestions */}
            {showStarters && (
              <div className="suggestionsRow" role="group" aria-label="Suggested questions">
                {starterSuggestions.slice(0, 6).map((s, i) => (
                  <button
                    key={i}
                    className="suggestionChip"
                    onClick={() => onSuggestionClick(s)}
                    disabled={loading}
                    aria-label={`Ask: ${s}`}
                  >
                    {s}
                  </button>
                ))}
              </div>
            )}

            {/* Follow-up suggestions */}
            {!showStarters &&
              !loading &&
              followUpSuggestions.length > 0 && (
                <div className="suggestionsRow" role="group" aria-label="Follow-up suggestions">
                  <div className="suggestionsLabel">Follow up:</div>
                  {followUpSuggestions.map((s, i) => (
                    <button
                      key={i}
                      className="suggestionChip followUp"
                      onClick={() => onSuggestionClick(s)}
                      disabled={loading}
                      aria-label={`Follow up: ${s}`}
                    >
                      {s}
                    </button>
                  ))}
                </div>
              )}

            {/* Streaming progress indicator */}
            {loading && (
              <div className="msgRow left" role="status" aria-label="Loading response">
                <div className="bubble bot">
                  <div className="roleTag" aria-hidden="true">GP Workforce Bot</div>
                  {progress ? (
                    <div className="progressContainer">
                      <div className="progressHeader">
                        <span className="progressLabel">{progress.label}</span>
                        <span className="progressStep">{progress.step}/{progress.total}</span>
                      </div>
                      <div className="progressBarTrack">
                        <div
                          className="progressBarFill"
                          style={{ width: `${(progress.step / progress.total) * 100}%` }}
                        />
                      </div>
                      <div className="progressDetail">{progress.detail}</div>
                    </div>
                  ) : (
                    <div className="msgText typing">Connecting...</div>
                  )}
                </div>
              </div>
            )}
          </div>

          <div className="composer">
            <label htmlFor="chatInput" className="srOnly">
              Type your question
            </label>
            <textarea
              id="chatInput"
              className="input"
              placeholder="Ask: Top 10 ICBs by GP FTE in latest month..."
              value={input}
              onChange={(e) => setInput(e.target.value)}
              onKeyDown={onKeyDown}
              rows={2}
              ref={inputRef}
              maxLength={1000}
              aria-label="Type your question about GP workforce data"
            />
            <button
              className="sendBtn"
              onClick={() => onSend()}
              disabled={!canSend}
              aria-label="Send message"
            >
              Send
            </button>
          </div>

          {error && (
            <div className="errorBox" role="alert">
              <span>{error}</span>
              {lastFailedQuestion && (
                <button className="retryBtn" onClick={onRetry} aria-label="Retry last question">
                  Retry
                </button>
              )}
            </div>
          )}
        </section>

        <section className="resultPanel" aria-label="Query results">
          <div className="panelHeader">
            <div className="panelTitle">Results</div>
            <div className="sessionTag" aria-label={`Session ID: ${sessionId}`}>
              Session: {sessionId.slice(0, 14)}…
            </div>
          </div>

          {!lastResponse ? (
            <div className="emptyState">
              Ask a question and your result table will show here.
              <div className="exampleBox">
                Try:
                <ul>
                  <li>Top 10 ICBs by GP FTE in the latest month</li>
                  <li>
                    Trend of GP FTE over last 12 months for NHS Greater
                    Manchester ICB
                  </li>
                  <li>Gender breakdown of GP FTE by ICB latest month</li>
                  <li>Staff breakdown at Keele Practice</li>
                  <li>Patients per GP ratio</li>
                </ul>
              </div>
            </div>
          ) : (
            <>
              {lastResponse.preview_markdown && (
                <div className="card">
                  <div className="cardTitle">
                    Preview Table
                    {lastResponse.meta?.rows_returned != null && (
                      <span className="rowCount">
                        {" "}
                        ({lastResponse.meta.rows_returned} rows)
                      </span>
                    )}
                  </div>
                  <MarkdownTable markdown={lastResponse.preview_markdown} />
                </div>
              )}

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

      <footer className="footer" role="contentinfo">
        Built for NHS open datasets | Athena = source of truth | v5.8 with
        streaming progress + multi-turn clarification
      </footer>
    </div>
  );
}
