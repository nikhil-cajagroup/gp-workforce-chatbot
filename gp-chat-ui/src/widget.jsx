/**
 * Widget entry point — renders the floating chat popup.
 * This file is the entry for the embeddable widget build.
 * Drop a <script> tag on any page (WordPress, etc.) and the widget appears.
 */
import React from "react";
import ReactDOM from "react-dom/client";
import ChatWidget from "./components/ChatWidget";
import "./widget-base.css";

// Create a shadow-friendly mount point
const mountId = "gp-workforce-chatbot-widget";

function mount() {
  let container = document.getElementById(mountId);
  if (!container) {
    container = document.createElement("div");
    container.id = mountId;
    document.body.appendChild(container);
  }
  ReactDOM.createRoot(container).render(
    <React.StrictMode>
      <ChatWidget />
    </React.StrictMode>
  );
}

// Mount immediately if DOM ready, else wait
if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", mount);
} else {
  mount();
}
