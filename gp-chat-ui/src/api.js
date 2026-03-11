const API_BASE = import.meta.env.VITE_API_BASE || "http://localhost:8000";
const API_KEY = import.meta.env.VITE_API_KEY || "";

/**
 * Build common headers for all API requests.
 * Includes API key when configured (production).
 */
function getHeaders() {
  const h = { "Content-Type": "application/json" };
  if (API_KEY) h["X-API-Key"] = API_KEY;
  return h;
}

/**
 * Send a chat message via SSE streaming.
 * Uses fetch + ReadableStream to consume Server-Sent Events from POST /chat/stream.
 *
 * @param {{ sessionId: string, question: string }} params
 * @param {(progress: {step:number, total:number, label:string, detail:string, elapsed:number, node:string}) => void} onProgress
 * @param {(result: object) => void} onComplete
 * @param {(error: string) => void} onError
 * @param {AbortSignal} [signal] — optional AbortController signal
 */
export async function streamChat({ sessionId, question }, onProgress, onComplete, onError, signal) {
  try {
    const res = await fetch(`${API_BASE}/chat/stream`, {
      method: "POST",
      headers: getHeaders(),
      body: JSON.stringify({ session_id: sessionId, question }),
      signal,
    });

    if (!res.ok) {
      let errorMsg = `Request failed (${res.status})`;
      try {
        const data = await res.json();
        errorMsg = data.error || errorMsg;
      } catch {
        errorMsg = res.statusText || errorMsg;
      }
      onError(errorMsg);
      return;
    }

    const reader = res.body.getReader();
    const decoder = new TextDecoder();
    let buffer = "";

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;

      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split("\n");
      buffer = lines.pop() || ""; // keep incomplete line in buffer

      let currentEvent = null;
      for (const line of lines) {
        if (line.startsWith("event:")) {
          currentEvent = line.slice(6).trim();
        } else if (line.startsWith("data:") && currentEvent) {
          const dataStr = line.slice(5).trim();
          if (!dataStr) continue;
          try {
            const data = JSON.parse(dataStr);
            if (currentEvent === "progress") {
              onProgress(data);
            } else if (currentEvent === "complete") {
              onComplete(data);
            } else if (currentEvent === "error") {
              onError(data.error || "Unknown error");
            }
          } catch {
            // skip malformed JSON
          }
          currentEvent = null;
        } else if (line === "") {
          currentEvent = null;
        }
      }
    }
  } catch (e) {
    if (e.name === "AbortError") return; // cancelled by user
    onError(e.message || "Connection failed");
  }
}

/**
 * Fallback: Send a chat message (non-streaming).
 */
export async function sendChat({ sessionId, question }, signal) {
  const res = await fetch(`${API_BASE}/chat`, {
    method: "POST",
    headers: getHeaders(),
    body: JSON.stringify({ session_id: sessionId, question }),
    signal,
  });

  if (!res.ok) {
    let errorMsg = `Request failed (${res.status})`;
    try {
      const data = await res.json();
      errorMsg = data.error || errorMsg;
    } catch {
      errorMsg = res.statusText || errorMsg;
    }
    throw new Error(errorMsg);
  }

  return res.json();
}

export async function fetchSuggestions() {
  try {
    const h = {};
    if (API_KEY) h["X-API-Key"] = API_KEY;
    const res = await fetch(`${API_BASE}/suggestions`, { headers: h });
    if (!res.ok) return [];
    const data = await res.json();
    return data.suggestions || [];
  } catch {
    return [];
  }
}
