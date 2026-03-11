import React from "react";
import ReactDOM from "react-dom/client";
import ChatWidget from "./components/ChatWidget";
import "./index.css";

/**
 * Dev mode: renders the widget on a sample page so you can test it.
 * Production: use `npm run build:widget` to get a single JS file for WordPress.
 */
ReactDOM.createRoot(document.getElementById("root")).render(
  <React.StrictMode>
    {/* Sample host page to test the widget overlay */}
    <div style={{
      minHeight: "100vh",
      padding: "40px",
      background: "#1a1a2e",
      color: "#e8eefc",
      fontFamily: "system-ui, sans-serif",
    }}>
      <h1 style={{ marginBottom: 16 }}>GP Practice Workforce Dashboard</h1>
      <p style={{ color: "#a9b3c7", maxWidth: 600 }}>
        This simulates your WordPress dashboard.
        The chatbot widget appears in the bottom-right corner.
        Click the blue chat button to open it.
      </p>
      <div style={{
        marginTop: 30,
        padding: 20,
        background: "rgba(255,255,255,0.05)",
        borderRadius: 12,
        border: "1px solid rgba(255,255,255,0.1)",
      }}>
        <p>[ Your existing dashboard content, maps, tables etc. would be here ]</p>
      </div>
    </div>

    {/* The floating chat widget */}
    <ChatWidget />
  </React.StrictMode>
);
