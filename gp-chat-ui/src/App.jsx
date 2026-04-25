import React, { useEffect, useMemo, useRef, useState, useCallback } from "react";
import { streamChat, fetchSuggestions } from "./api";
import MessageBubble from "./components/MessageBubble";
import MarkdownTable from "./components/MarkdownTable";

function randomSessionId() {
  if (typeof crypto !== "undefined" && crypto.randomUUID) {
    return "sess_" + crypto.randomUUID().replace(/-/g, "").slice(0, 16);
  }
  return "sess_" + Date.now().toString(36) + Math.random().toString(36).slice(2, 10);
}

const WELCOME_MESSAGE = {
  role: "bot",
  content:
    "Hi! Ask me anything about **GP Workforce** or **GP Appointments** data — ICB, region, practice, or PCN level. FTE, headcount, DNA rates, appointment modes, trends, and more. Try one of the suggestions below.",
};

const EXAMPLE_QUESTIONS = [
  "Top 10 ICBs by GP FTE",
  "DNA rate by region",
  "Pharmacists by ICB",
  "Same-day appointments share",
  "Patients per GP nationally",
];

/* ── Icons ── */
function LogoIcon() {
  return (
    <svg width="18" height="18" viewBox="0 0 24 24" fill="none">
      <path d="M22 12h-4l-3 9L9 3l-3 9H2" stroke="white" strokeWidth="2.2"
        strokeLinecap="round" strokeLinejoin="round"/>
    </svg>
  );
}
function SendIcon() {
  return (
    <svg width="18" height="18" viewBox="0 0 24 24" fill="currentColor">
      <path d="M2.01 21L23 12 2.01 3 2 10l15 2-15 2z"/>
    </svg>
  );
}
function TrashIcon() {
  return (
    <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor"
      strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <polyline points="3 6 5 6 21 6"/>
      <path d="M19 6l-1 14a2 2 0 01-2 2H8a2 2 0 01-2-2L5 6"/>
      <path d="M10 11v6M14 11v6M9 6V4a2 2 0 012-2h2a2 2 0 012 2v2"/>
    </svg>
  );
}
function CodeIcon() {
  return (
    <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor"
      strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <polyline points="16 18 22 12 16 6"/>
      <polyline points="8 6 2 12 8 18"/>
    </svg>
  );
}
function ResultsIcon() {
  return (
    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor"
      strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <rect x="3" y="3" width="18" height="18" rx="2"/>
      <path d="M3 9h18M9 21V9"/>
    </svg>
  );
}

export default function App() {
  const [sessionId, setSessionId] = useState(() => randomSessionId());
  const [input, setInput] = useState("");
  const [messages, setMessages] = useState([WELCOME_MESSAGE]);
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

  useEffect(() => { fetchSuggestions().then(setStarterSuggestions); }, []);

  useEffect(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, [messages, loading, lastResponse, progress]);

  useEffect(() => {
    if (!loading && inputRef.current) inputRef.current.focus();
  }, [loading]);

  const canSend = useMemo(() => input.trim().length > 0 && !loading, [input, loading]);
  const followUpSuggestions = lastResponse?.suggestions || [];
  const showStarters = messages.length <= 1 && starterSuggestions.length > 0;
  const charCount = input.length;
  const nearLimit = charCount > 850;

  const onSend = useCallback(function onSend(questionOverride) {
    const question = (questionOverride || input).trim();
    if (!question) return;

    setError(""); setLastFailedQuestion(""); setInput("");
    setLoading(true); setProgress(null);

    if (abortRef.current) abortRef.current.abort();
    const controller = new AbortController();
    abortRef.current = controller;

    setMessages((prev) => [...prev, { role: "user", content: question }]);

    streamChat(
      { sessionId, question },
      (progressData) => setProgress(progressData),
      (data) => {
        setLastResponse(data);
        setMessages((prev) => [...prev, { role: "bot", content: data.answer }]);
        setLoading(false); setProgress(null);
        abortRef.current = null;
      },
      (errorMsg) => {
        setError(errorMsg || "Something went wrong.");
        setLastFailedQuestion(question);
        setLoading(false); setProgress(null);
        abortRef.current = null;
      },
      controller.signal
    );
  }, [input, sessionId]);

  function onKeyDown(e) {
    if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); if (canSend) onSend(); }
  }
  function onSuggestionClick(text) { if (!loading) { setInput(""); onSend(text); } }
  function onRetry() { if (lastFailedQuestion) onSend(lastFailedQuestion); }

  const onClearChat = useCallback(() => {
    if (abortRef.current) { abortRef.current.abort(); abortRef.current = null; }
    setMessages([WELCOME_MESSAGE]);
    setLastResponse(null); setInput(""); setError("");
    setLastFailedQuestion(""); setProgress(null); setLoading(false);
    setSessionId(randomSessionId());
    if (inputRef.current) inputRef.current.focus();
  }, []);

  return (
    <div className="page">
      {/* ── Topbar ── */}
      <header className="topbar" role="banner">
        <div className="topbarBrand">
          <div className="topbarLogo" aria-hidden="true">
            <LogoIcon />
          </div>
          <div className="topbarText">
            <div className="title">InsightsQI Assistant</div>
            <div className="titleSub">NHS GP Data · Powered by Amazon Bedrock</div>
          </div>
          <span className="pill">Beta</span>
        </div>

        <div className="topRight">
          <button className="btn" onClick={onClearChat}
            aria-label="Clear chat and start a new session" title="Clear chat">
            <TrashIcon /><span>Clear</span>
          </button>
          <button className={`btn ${showDebug ? "btnActive" : ""}`}
            onClick={() => setShowDebug((s) => !s)}
            aria-pressed={showDebug}
            aria-label={showDebug ? "Hide debug panel" : "Show debug panel"}>
            <CodeIcon /><span>Debug</span>
          </button>
        </div>
      </header>

      <main className="main" role="main">
        {/* ── Chat Panel ── */}
        <section className="chatPanel" aria-label="Chat conversation">
          <div className="chatArea" ref={scrollRef} role="log" aria-live="polite">
            {messages.map((m, idx) => (
              <MessageBubble key={idx} role={m.role} content={m.content} />
            ))}

            {/* Starter suggestions */}
            {showStarters && (
              <div className="suggestionsRow" role="group" aria-label="Suggested questions">
                {starterSuggestions.slice(0, 6).map((s, i) => (
                  <button key={i} className="suggestionChip"
                    onClick={() => onSuggestionClick(s)} disabled={loading}
                    aria-label={`Ask: ${s}`}>
                    {s}
                  </button>
                ))}
              </div>
            )}

            {/* Follow-up suggestions */}
            {!showStarters && !loading && followUpSuggestions.length > 0 && (
              <div className="suggestionsRow" role="group" aria-label="Follow-up suggestions">
                <div className="suggestionsLabel">Follow up:</div>
                {followUpSuggestions.map((s, i) => (
                  <button key={i} className="suggestionChip followUp"
                    onClick={() => onSuggestionClick(s)} disabled={loading}
                    aria-label={`Follow up: ${s}`}>
                    {s}
                  </button>
                ))}
              </div>
            )}

            {/* Loading indicator */}
            {loading && (
              <div className="msgRow left" role="status" aria-label="Loading response">
                <div className="botAvatar" aria-hidden="true">
                  <LogoIcon />
                </div>
                <div className="bubble bot">
                  <div className="roleTag">InsightsQI</div>
                  {progress ? (
                    <div className="progressContainer">
                      <div className="progressHeader">
                        <span className="progressLabel">{progress.label}</span>
                        <span className="progressStep">{progress.step}/{progress.total}</span>
                      </div>
                      <div className="progressBarTrack">
                        <div className="progressBarFill"
                          style={{ width: `${(progress.step / progress.total) * 100}%` }} />
                      </div>
                      <div className="progressDetail">{progress.detail}</div>
                    </div>
                  ) : (
                    <div className="typingDots" aria-label="Thinking">
                      <span/><span/><span/>
                    </div>
                  )}
                </div>
              </div>
            )}
          </div>

          {/* ── Composer ── */}
          <div className="composer">
            <div className="composerInner">
              <label htmlFor="chatInput" className="srOnly">Type your question</label>
              <div className="inputWrap">
                <textarea
                  id="chatInput"
                  className="input"
                  placeholder="Ask: Top 10 ICBs by GP FTE in the latest month…"
                  value={input}
                  onChange={(e) => setInput(e.target.value)}
                  onKeyDown={onKeyDown}
                  rows={2}
                  ref={inputRef}
                  maxLength={1000}
                  aria-label="Type your question about GP data"
                />
              </div>
              <button className="sendBtn" onClick={() => onSend()}
                disabled={!canSend} aria-label="Send message">
                <SendIcon />
              </button>
            </div>
            {charCount > 0 && (
              <div className={`charCount ${nearLimit ? "nearLimit" : ""}`}>
                {charCount} / 1000
              </div>
            )}
          </div>

          {error && (
            <div className="errorBox" role="alert">
              <span>{error}</span>
              {lastFailedQuestion && (
                <button className="retryBtn" onClick={onRetry} aria-label="Retry">Retry</button>
              )}
            </div>
          )}
        </section>

        {/* ── Results Panel ── */}
        <section className="resultPanel" aria-label="Query results">
          <div className="panelHeader">
            <div className="panelTitle" style={{ display: "flex", alignItems: "center", gap: 7 }}>
              <ResultsIcon /> Results
            </div>
            <div className="sessionTag" aria-label={`Session: ${sessionId}`}>
              {sessionId.slice(0, 14)}…
            </div>
          </div>

          {!lastResponse ? (
            <div className="emptyState">
              <div className="emptyStateIcon" aria-hidden="true">
                <ResultsIcon />
              </div>
              <div>
                <div className="emptyStateTitle">Your results will appear here</div>
                <div className="emptyStateSub">Ask a question to run a live query against NHS data</div>
              </div>
              <div className="exampleChips">
                {EXAMPLE_QUESTIONS.map((q, i) => (
                  <div key={i} className="exampleChip">{q}</div>
                ))}
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
                        {lastResponse.meta.rows_returned} rows
                      </span>
                    )}
                    {lastResponse.meta?.elapsed_seconds != null && (
                      <span className="rowCount" style={{ marginLeft: 8 }}>
                        · {lastResponse.meta.elapsed_seconds.toFixed(1)}s
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
                    <pre className="code">{JSON.stringify(lastResponse.meta, null, 2)}</pre>
                  </div>
                </div>
              )}
            </>
          )}
        </section>
      </main>

      <footer className="footer" role="contentinfo">
        <span>InsightsQI</span>
        <span className="footerDot">·</span>
        <span>NHS GP Open Data</span>
        <span className="footerDot">·</span>
        <span>Caja Group © 2026</span>
      </footer>
    </div>
  );
}
