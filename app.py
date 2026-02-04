# app.py
# Streamlit frontend for GP Workforce Athena Chatbot backend (gp_workforce_chatbot_backend.py)
# ✅ No st.secrets usage — uses ONLY environment variables (same as backend)
#
# Run:
#   export AWS_PROFILE=chatbot
#   export AWS_REGION=eu-west-2
#   export ATHENA_DATABASE="test-gp-workforce"
#   export ATHENA_OUTPUT_S3="s3://test-athena-results-fingertips/"
#   export BEDROCK_CHAT_MODEL_ID="amazon.nova-pro-v1:0"
#
#   streamlit run app.py

from __future__ import annotations

import os
import uuid
import streamlit as st

import gp_workforce_chatbot_backend as demo


# -----------------------------
# Page setup
# -----------------------------
st.set_page_config(
    page_title="GP Workforce Chat",
    page_icon="🩺",
    layout="centered",
)

st.markdown(
    """
    <div style="margin-bottom: 0.25rem;">
      <h1 style="margin:0;">🩺 GP Workforce Data Chat</h1>
      <p style="margin:0.25rem 0 0 0; color: #666;">
        Ask things like “latest month total FTE by staff group”, “top roles by FTE”, “headcount by ICB”, etc.
      </p>
    </div>
    """,
    unsafe_allow_html=True,
)

with st.expander("⚙️ Tips (click)", expanded=False):
    st.markdown(
        """
- Use **latest** if you want the newest month available (bot will pick latest snapshot automatically).
- Examples:
  - “Show latest month total FTE by staff_group”
  - “Top 10 detailed_staff_role by FTE in latest month”
  - “Headcount by ICB in latest month”
  - “Trend of GP total FTE last 12 months”
        """.strip()
    )

with st.expander("🔎 Environment check (from your terminal exports)", expanded=False):
    st.code(
        "\n".join(
            [
                f"AWS_PROFILE={os.getenv('AWS_PROFILE', '(not set)')}",
                f"AWS_REGION={os.getenv('AWS_REGION', '(not set)')}",
                f"ATHENA_DATABASE={os.getenv('ATHENA_DATABASE', '(not set)')}",
                f"ATHENA_OUTPUT_S3={os.getenv('ATHENA_OUTPUT_S3', '(not set)')}",
                f"BEDROCK_CHAT_MODEL_ID={os.getenv('BEDROCK_CHAT_MODEL_ID', '(not set)')}",
            ]
        ),
        language="text",
    )


# -----------------------------
# Session state init
# -----------------------------
if "session_id" not in st.session_state:
    st.session_state.session_id = str(uuid.uuid4())

if "memory" not in st.session_state:
    # Uses your backend memory class
    st.session_state.memory = demo.SimpleChatMemory(max_turns=10)

if "chat_history" not in st.session_state:
    # list of dicts: {"role": "user"/"assistant", "content": "..."}
    st.session_state.chat_history = []


# -----------------------------
# Sidebar controls
# -----------------------------
with st.sidebar:
    st.header("Controls")

    max_retries = st.slider("Auto-fix retries", min_value=0, max_value=5, value=2, step=1)

    st.divider()
    st.caption("Current Bedrock Model (from env)")
    st.code(os.getenv("BEDROCK_CHAT_MODEL_ID", "amazon.nova-pro-v1:0"), language="text")

    if st.button("🧹 Clear chat"):
        st.session_state.chat_history = []
        st.session_state.memory = demo.SimpleChatMemory(max_turns=10)
        st.rerun()


# -----------------------------
# Render chat history
# -----------------------------
for msg in st.session_state.chat_history:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])


# -----------------------------
# Chat input
# -----------------------------
q = st.chat_input("Ask a question about GP workforce…")
if q:
    # Add user message
    st.session_state.chat_history.append({"role": "user", "content": q})
    with st.chat_message("user"):
        st.markdown(q)

    # Run backend
    with st.chat_message("assistant"):
        with st.spinner("Thinking + running Athena…"):
            try:
                out = demo.ask(
                    memory=st.session_state.memory,
                    question=q,
                    max_retries=max_retries,
                )

                answer = out.get("answer", "")
                st.markdown(answer if answer else "✅ Done (no text returned).")

                # Debug info (SQL + query id)
                with st.expander("🧾 SQL + Athena details", expanded=False):
                    st.markdown("**SQL generated**")
                    st.code(out.get("sql", ""), language="sql")
                    st.markdown("**QueryExecutionId**")
                    st.code(out.get("query_execution_id", ""), language="text")
                    st.markdown("**Attempts**")
                    st.code(str(out.get("attempts", "")), language="text")

                st.session_state.chat_history.append({"role": "assistant", "content": answer})

            except Exception as e:
                err = f"❌ **Error:** {e}"
                st.error(err)
                st.session_state.chat_history.append({"role": "assistant", "content": err})
