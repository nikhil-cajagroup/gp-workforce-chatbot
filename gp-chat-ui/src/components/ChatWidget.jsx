import React, { useState, useEffect, useMemo, useRef, useCallback } from "react";
import { streamChat, fetchSuggestions } from "../api";
import MessageBubble from "./MessageBubble";
import MarkdownTable from "./MarkdownTable";
import "./ChatWidget.css";

function randomSessionId() {
  if (typeof crypto !== "undefined" && crypto.randomUUID) {
    return "sess_" + crypto.randomUUID().replace(/-/g, "").slice(0, 16);
  }
  return "sess_" + Date.now().toString(36) + Math.random().toString(36).slice(2, 10);
}

export default function ChatWidget() {
  const [isOpen, setIsOpen] = useState(false);
  const [isMaximised, setIsMaximised] = useState(false);
  const [hasUnread, setHasUnread] = useState(false);
  const [showDebug, setShowDebug] = useState(false);
  const [sessionId] = useState(() => randomSessionId());
  const [input, setInput] = useState("");
  const [messages, setMessages] = useState([
    {
      role: "bot",
      content:
        "Hi! Ask me anything about **GP Workforce** data — ICB/Sub-ICB/practice lookups, FTE/headcount trends, demographics, staff breakdowns, and more.",
    },
  ]);
  const [loading, setLoading] = useState(false);
  const [progress, setProgress] = useState(null);
  const [lastResponse, setLastResponse] = useState(null);
  const [error, setError] = useState("");
  const [lastFailedQuestion, setLastFailedQuestion] = useState("");
  const [starterSuggestions, setStarterSuggestions] = useState([]);

  const scrollRef = useRef(null);
  const inputRef = useRef(null);
  const abortRef = useRef(null);

  useEffect(() => {
    fetchSuggestions().then(setStarterSuggestions);
  }, []);

  useEffect(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, [messages, loading, progress]);

  useEffect(() => {
    if (isOpen && !loading && inputRef.current) {
      inputRef.current.focus();
    }
  }, [loading, isOpen]);

  // Close maximised on Escape
  useEffect(() => {
    function handleEsc(e) {
      if (e.key === "Escape") {
        if (isMaximised) setIsMaximised(false);
        else if (isOpen) setIsOpen(false);
      }
    }
    document.addEventListener("keydown", handleEsc);
    return () => document.removeEventListener("keydown", handleEsc);
  }, [isOpen, isMaximised]);

  const canSend = useMemo(
    () => input.trim().length > 0 && !loading,
    [input, loading]
  );

  const followUpSuggestions = lastResponse?.suggestions || [];
  const showStarters = messages.length <= 1 && starterSuggestions.length > 0;

  const toggleOpen = useCallback(() => {
    setIsOpen((prev) => {
      if (!prev) setHasUnread(false);
      return !prev;
    });
  }, []);

  const toggleMaximise = useCallback(() => {
    setIsMaximised((prev) => !prev);
  }, []);

  const onSend = useCallback(
    function onSend(questionOverride) {
      const question = (questionOverride || input).trim();
      if (!question) return;

      setError("");
      setLastFailedQuestion("");
      setInput("");
      setLoading(true);
      setProgress(null);
      // Reset textarea height after clearing
      if (inputRef.current) inputRef.current.style.height = "auto";

      if (abortRef.current) {
        abortRef.current.abort();
      }
      const controller = new AbortController();
      abortRef.current = controller;

      setMessages((prev) => [...prev, { role: "user", content: question }]);

      streamChat(
        { sessionId, question },
        (progressData) => {
          setProgress(progressData);
        },
        (data) => {
          setLastResponse(data);
          setMessages((prev) => [...prev, { role: "bot", content: data.answer }]);
          setLoading(false);
          setProgress(null);
          abortRef.current = null;
          setHasUnread((prev) => !document.querySelector(".cw-popup.open"));
        },
        (errorMsg) => {
          setError(errorMsg || "Something went wrong.");
          setLastFailedQuestion(question);
          setLoading(false);
          setProgress(null);
          abortRef.current = null;
        },
        controller.signal
      );
    },
    [input, sessionId]
  );

  const onCancel = useCallback(() => {
    if (abortRef.current) {
      abortRef.current.abort();
      abortRef.current = null;
    }
    setLoading(false);
    setProgress(null);
    setMessages((prev) => [
      ...prev,
      { role: "bot", content: "_Query cancelled._" },
    ]);
  }, []);

  /** Auto-grow textarea to fit content, up to CSS max-height */
  function autoGrow(el) {
    if (!el) return;
    el.style.height = "auto";
    el.style.height = Math.min(el.scrollHeight, 100) + "px";
  }

  function handleInputChange(e) {
    setInput(e.target.value);
    autoGrow(e.target);
  }

  function onKeyDown(e) {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      if (canSend) onSend();
    }
    if (e.key === "Escape" && loading) {
      onCancel();
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

  // ─── Maximised full-screen layout ───
  if (isMaximised && isOpen) {
    return (
      <div className="cw-container">
        {/* Backdrop */}
        <div className="cw-backdrop" onClick={toggleMaximise} />

        {/* Full-screen panel */}
        <div className="cw-fullscreen" role="dialog" aria-label="GP Workforce Chatbot — Expanded">
          {/* Header */}
          <div className="cw-header">
            <div className="cw-header-left">
              <div className="cw-avatar">
                <svg width="20" height="20" viewBox="0 0 24 24" fill="currentColor">
                  <path d="M20 2H4c-1.1 0-2 .9-2 2v18l4-4h14c1.1 0 2-.9 2-2V4c0-1.1-.9-2-2-2z"/>
                </svg>
              </div>
              <div>
                <div className="cw-header-title">GP Workforce Bot</div>
                <div className="cw-header-sub">
                  {loading ? "Thinking..." : "Ask about GP data"}
                </div>
              </div>
            </div>
            <div className="cw-header-actions">
              <button
                className={`cw-icon-btn ${showDebug ? "active" : ""}`}
                onClick={() => setShowDebug((s) => !s)}
                title={showDebug ? "Hide debug" : "Show debug"}
                aria-label="Toggle debug panel"
              >
                <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round">
                  <path d="M12 20h9"/><path d="M16.376 3.622a1 1 0 013.002 3.002L7.368 18.635a2 2 0 01-.855.506l-2.872.838.838-2.872a2 2 0 01.506-.854z"/>
                </svg>
              </button>
              {/* Minimise back to popup */}
              <button
                className="cw-icon-btn"
                onClick={toggleMaximise}
                title="Minimise to popup"
                aria-label="Minimise to popup"
              >
                {/* shrink icon */}
                <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round">
                  <polyline points="4 14 10 14 10 20"/>
                  <polyline points="20 10 14 10 14 4"/>
                  <line x1="14" y1="10" x2="21" y2="3"/>
                  <line x1="3" y1="21" x2="10" y2="14"/>
                </svg>
              </button>
              {/* Close */}
              <button
                className="cw-icon-btn"
                onClick={() => { setIsMaximised(false); setIsOpen(false); }}
                title="Close"
                aria-label="Close chatbot"
              >
                <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round">
                  <line x1="18" y1="6" x2="6" y2="18" />
                  <line x1="6" y1="6" x2="18" y2="18" />
                </svg>
              </button>
            </div>
          </div>

          {/* Two-panel body */}
          <div className="cw-full-body">
            {/* LEFT: Chat */}
            <div className="cw-full-chat">
              <div className="cw-messages" ref={scrollRef}>
                {messages.map((m, idx) => (
                  <MessageBubble key={idx} role={m.role} content={m.content} />
                ))}

                {loading && (
                  <div className="msgRow left">
                    <div className="bubble bot">
                      <div className="roleTag">GP Workforce Bot</div>
                      {progress ? (
                        <div className="cw-progress">
                          <div className="cw-progress-header">
                            <span className="cw-progress-label">{progress.label}</span>
                            <span className="cw-progress-step">{progress.step}/{progress.total}</span>
                          </div>
                          <div className="cw-progress-track">
                            <div className="cw-progress-fill" style={{ width: `${(progress.step / progress.total) * 100}%` }} />
                          </div>
                          <div className="cw-progress-detail">{progress.detail}</div>
                        </div>
                      ) : (
                        <div className="msgText typing">Connecting...</div>
                      )}
                    </div>
                  </div>
                )}
              </div>

              {/* Suggestions — pinned above composer */}
              {showStarters && (
                <div className="cw-suggestions cw-suggestions-bottom">
                  {starterSuggestions.slice(0, 6).map((s, i) => (
                    <button key={i} className="cw-chip" onClick={() => onSuggestionClick(s)} disabled={loading}>
                      {s}
                    </button>
                  ))}
                </div>
              )}

              {!showStarters && !loading && followUpSuggestions.length > 0 && (
                <div className="cw-suggestions cw-suggestions-bottom">
                  <div className="cw-suggestions-label">Follow up:</div>
                  {followUpSuggestions.map((s, i) => (
                    <button key={i} className="cw-chip followup" onClick={() => onSuggestionClick(s)} disabled={loading}>
                      {s}
                    </button>
                  ))}
                </div>
              )}

              {error && (
                <div className="cw-error">
                  <span>{error}</span>
                  {lastFailedQuestion && <button className="cw-retry" onClick={onRetry}>Retry</button>}
                </div>
              )}

              <div className="cw-composer">
                <textarea
                  className="cw-input"
                  placeholder="Ask about GP workforce data..."
                  value={input}
                  onChange={handleInputChange}
                  onKeyDown={onKeyDown}
                  rows={1}
                  ref={inputRef}
                  maxLength={1000}
                  aria-label="Type your question"
                />
                {loading ? (
                  <button className="cw-send cw-cancel" onClick={onCancel} aria-label="Cancel">
                    <svg width="20" height="20" viewBox="0 0 24 24" fill="currentColor">
                      <rect x="4" y="4" width="16" height="16" rx="2"/>
                    </svg>
                  </button>
                ) : (
                  <button className="cw-send" onClick={() => onSend()} disabled={!canSend} aria-label="Send">
                    <svg width="20" height="20" viewBox="0 0 24 24" fill="currentColor">
                      <path d="M2.01 21L23 12 2.01 3 2 10l15 2-15 2z"/>
                    </svg>
                  </button>
                )}
              </div>
            </div>

            {/* RIGHT: Results + Debug */}
            <div className="cw-full-results">
              <div className="cw-results-header">
                <span className="cw-results-title">Results</span>
                <span className="cw-session-tag">Session: {sessionId.slice(0, 14)}…</span>
              </div>

              {!lastResponse ? (
                <div className="cw-empty-state">
                  <p>Ask a question and your result table will show here.</p>
                  <div className="cw-example-box">
                    Try:
                    <ul>
                      <li>Top 10 ICBs by GP FTE in the latest month</li>
                      <li>Trend of GP FTE over last 12 months for NHS Greater Manchester ICB</li>
                      <li>Gender breakdown of GP FTE by ICB latest month</li>
                      <li>Staff breakdown at Keele Practice</li>
                    </ul>
                  </div>
                </div>
              ) : (
                <div className="cw-results-content">
                  {lastResponse.preview_markdown && (
                    <div className="cw-result-card">
                      <div className="cw-card-title">
                        Preview Table
                        {lastResponse.meta?.rows_returned != null && (
                          <span className="cw-row-count"> ({lastResponse.meta.rows_returned} rows)</span>
                        )}
                      </div>
                      <div className="cw-result-table-scroll">
                        <MarkdownTable markdown={lastResponse.preview_markdown} />
                      </div>
                    </div>
                  )}

                  {showDebug && (
                    <div className="cw-result-card">
                      <div className="cw-card-title">Debug</div>
                      <div className="cw-debug-block">
                        <div className="cw-debug-label">SQL</div>
                        <pre className="cw-code">{lastResponse.sql}</pre>
                      </div>
                      <div className="cw-debug-block">
                        <div className="cw-debug-label">Meta</div>
                        <pre className="cw-code">{JSON.stringify(lastResponse.meta, null, 2)}</pre>
                      </div>
                    </div>
                  )}
                </div>
              )}
            </div>
          </div>
        </div>
      </div>
    );
  }

  // ─── Normal popup / closed state ───
  return (
    <div className="cw-container">
      {/* ── Floating Button ── */}
      <button
        className={`cw-fab ${isOpen ? "cw-fab-active" : ""}`}
        onClick={toggleOpen}
        aria-label={isOpen ? "Close chatbot" : "Open GP Workforce Chatbot"}
        title="GP Workforce Chatbot"
      >
        {isOpen ? (
          <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round">
            <line x1="18" y1="6" x2="6" y2="18" />
            <line x1="6" y1="6" x2="18" y2="18" />
          </svg>
        ) : (
          <>
            <svg width="26" height="26" viewBox="0 0 24 24" fill="currentColor">
              <path d="M20 2H4c-1.1 0-2 .9-2 2v18l4-4h14c1.1 0 2-.9 2-2V4c0-1.1-.9-2-2-2zm0 14H6l-2 2V4h16v12z"/>
              <circle cx="8" cy="10" r="1.2"/>
              <circle cx="12" cy="10" r="1.2"/>
              <circle cx="16" cy="10" r="1.2"/>
            </svg>
            {hasUnread && <span className="cw-unread-dot" />}
          </>
        )}
      </button>

      {/* ── Chat Popup (compact) ── */}
      <div className={`cw-popup ${isOpen ? "open" : ""}`} role="dialog" aria-label="GP Workforce Chatbot">
        {/* Header */}
        <div className="cw-header">
          <div className="cw-header-left">
            <div className="cw-avatar">
              <svg width="20" height="20" viewBox="0 0 24 24" fill="currentColor">
                <path d="M20 2H4c-1.1 0-2 .9-2 2v18l4-4h14c1.1 0 2-.9 2-2V4c0-1.1-.9-2-2-2z"/>
              </svg>
            </div>
            <div>
              <div className="cw-header-title">GP Workforce Bot</div>
              <div className="cw-header-sub">
                {loading ? "Thinking..." : "Ask about GP data"}
              </div>
            </div>
          </div>
          <div className="cw-header-actions">
            {/* Maximise button */}
            <button
              className="cw-icon-btn"
              onClick={toggleMaximise}
              title="Expand full screen"
              aria-label="Expand to full screen"
            >
              {/* expand icon */}
              <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round">
                <polyline points="15 3 21 3 21 9"/>
                <polyline points="9 21 3 21 3 15"/>
                <line x1="21" y1="3" x2="14" y2="10"/>
                <line x1="3" y1="21" x2="10" y2="14"/>
              </svg>
            </button>
            {/* Minimise */}
            <button
              className="cw-icon-btn"
              onClick={toggleOpen}
              aria-label="Minimise chatbot"
              title="Minimise"
            >
              <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round">
                <line x1="5" y1="12" x2="19" y2="12" />
              </svg>
            </button>
          </div>
        </div>

        {/* Messages */}
        <div className="cw-messages" ref={scrollRef}>
          {messages.map((m, idx) => (
            <MessageBubble key={idx} role={m.role} content={m.content} />
          ))}

          {loading && (
            <div className="msgRow left">
              <div className="bubble bot">
                <div className="roleTag">GP Workforce Bot</div>
                {progress ? (
                  <div className="cw-progress">
                    <div className="cw-progress-header">
                      <span className="cw-progress-label">{progress.label}</span>
                      <span className="cw-progress-step">{progress.step}/{progress.total}</span>
                    </div>
                    <div className="cw-progress-track">
                      <div className="cw-progress-fill" style={{ width: `${(progress.step / progress.total) * 100}%` }} />
                    </div>
                    <div className="cw-progress-detail">{progress.detail}</div>
                  </div>
                ) : (
                  <div className="msgText typing">Connecting...</div>
                )}
              </div>
            </div>
          )}
        </div>

        {/* Suggestions — pinned above composer */}
        {showStarters && (
          <div className="cw-suggestions cw-suggestions-bottom">
            {starterSuggestions.slice(0, 4).map((s, i) => (
              <button key={i} className="cw-chip" onClick={() => onSuggestionClick(s)} disabled={loading}>
                {s}
              </button>
            ))}
          </div>
        )}

        {!showStarters && !loading && followUpSuggestions.length > 0 && (
          <div className="cw-suggestions cw-suggestions-bottom">
            <div className="cw-suggestions-label">Follow up:</div>
            {followUpSuggestions.map((s, i) => (
              <button key={i} className="cw-chip followup" onClick={() => onSuggestionClick(s)} disabled={loading}>
                {s}
              </button>
            ))}
          </div>
        )}

        {error && (
          <div className="cw-error">
            <span>{error}</span>
            {lastFailedQuestion && <button className="cw-retry" onClick={onRetry}>Retry</button>}
          </div>
        )}

        {/* Composer */}
        <div className="cw-composer">
          <textarea
            className="cw-input"
            placeholder="Ask about GP workforce data..."
            value={input}
            onChange={handleInputChange}
            onKeyDown={onKeyDown}
            rows={1}
            ref={inputRef}
            maxLength={1000}
            aria-label="Type your question"
          />
          {loading ? (
            <button className="cw-send cw-cancel" onClick={onCancel} aria-label="Cancel">
              <svg width="20" height="20" viewBox="0 0 24 24" fill="currentColor">
                <rect x="4" y="4" width="16" height="16" rx="2"/>
              </svg>
            </button>
          ) : (
            <button className="cw-send" onClick={() => onSend()} disabled={!canSend} aria-label="Send">
              <svg width="20" height="20" viewBox="0 0 24 24" fill="currentColor">
                <path d="M2.01 21L23 12 2.01 3 2 10l15 2-15 2z"/>
              </svg>
            </button>
          )}
        </div>
      </div>
    </div>
  );
}
