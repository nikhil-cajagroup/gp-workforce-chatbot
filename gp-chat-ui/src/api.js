const API_BASE = import.meta.env.VITE_API_BASE || "http://localhost:8000";

/**
 * Send a chat message. Supports AbortController signal for cancellation.
 * @param {{ sessionId: string, question: string }} params
 * @param {AbortSignal} [signal] — optional AbortController signal
 * @returns {Promise<object>}
 */
export async function sendChat({ sessionId, question }, signal) {
  const res = await fetch(`${API_BASE}/chat`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ session_id: sessionId, question }),
    signal,
  });

  if (!res.ok) {
    let errorMsg = `Request failed (${res.status})`;
    try {
      const data = await res.json();
      errorMsg = data.error || errorMsg;
    } catch {
      // If response isn't JSON, use the status text
      errorMsg = res.statusText || errorMsg;
    }
    throw new Error(errorMsg);
  }

  return res.json();
}

export async function fetchSuggestions() {
  try {
    const res = await fetch(`${API_BASE}/suggestions`);
    if (!res.ok) return [];
    const data = await res.json();
    return data.suggestions || [];
  } catch {
    return [];
  }
}
