import streamlit as st
import os
from helpers.pipeline_utils import (
    process_pipeline_json_in_chunks, extract_json_from_zip
)

MODEL_ENDPOINT = os.getenv("MODEL_ENDPOINT", "https://<your-llm-endpoint>")
TOKEN_URL = os.getenv("TOKEN_URL", "https://<your-token-endpoint>")
USERNAME = os.getenv("USERNAME", "<username>")
PASSWORD = os.getenv("PASSWORD", "<password>")

st.title("AI StreamSets Pipeline Processor (Enterprise Edition)")
st.markdown("""
- Handles large JSON pipelines using chunking.
- Enter your instructions in plain English (e.g., "upgrade Kafka driver to 3.5.1").
- Shows progress in real time.
""")

chunk_size = st.slider("Chunk Size (KB)", min_value=256, max_value=2048, value=1024) * 1024

user_instruction = st.text_input(
    "What do you want to do with this pipeline?",
    placeholder="E.g. upgrade Kafka driver to 3.5.1 and replace deprecated processors")

input_mode = st.radio("Input pipeline format:", ["Upload JSON", "Upload ZIP", "Paste JSON"])
json_data = None

if input_mode == "Upload JSON":
    pipeline_file = st.file_uploader("Upload Pipeline JSON", type=["json"])
    if pipeline_file:
        json_data = pipeline_file.read().decode()
elif input_mode == "Upload ZIP":
    zip_file = st.file_uploader("Upload Pipeline ZIP", type=["zip"])
    if zip_file:
        json_data = extract_json_from_zip(zip_file.read())
        if json_data is None:
            st.warning("No JSON found in ZIP.")
elif input_mode == "Paste JSON":
    json_data = st.text_area("Paste Pipeline JSON Here")

if json_data and user_instruction and st.button("Process Pipeline (Chunked)"):
    progress_bar = st.progress(0)
    status_text = st.empty()

    def progress_callback(current, total, info):
        percent = (current + 1) / total
        progress_bar.progress(percent)
        status_text.info(info)

    try:
        status_text.info("Starting processing...")
        result = process_pipeline_json_in_chunks(
            json_data, chunk_size, user_instruction,
            TOKEN_URL, USERNAME, PASSWORD, MODEL_ENDPOINT,
            progress_callback
        )
        progress_bar.progress(1.0)
        status_text.success("Processing complete.")
        st.download_button("Download Modified Pipeline JSON", result, file_name="updated_pipeline.json")
        st.code(result, language="json")
    except Exception as e:
        status_text.error(f"Processing failed: {str(e)}")
        progress_bar.progress(0)
