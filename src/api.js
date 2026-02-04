const API_BASE = import.meta.env.VITE_API_BASE || "http://localhost:8000";

export async function sendChat({ sessionId, question }) {
  const res = await fetch(`${API_BASE}/chat`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ session_id: sessionId, question }),
  });

  if (!res.ok) {
    const txt = await res.text();
    throw new Error(txt || `Request failed: ${res.status}`);
  }

  return res.json();
}
