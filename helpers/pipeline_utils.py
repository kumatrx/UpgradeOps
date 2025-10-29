import requests
import base64
import zipfile
import io
import time

def get_jwt(token_url, username, password):
    credentials = base64.b64encode(f"{username}:{password}".encode()).decode()
    headers = {'Authorization': f'Basic {credentials}'}
    response = requests.post(token_url, headers=headers)
    if response.status_code == 200:
        return response.text.strip()
    else:
        raise Exception(f"JWT failure: {response.text}")

def call_llm(prompt, token_url, username, password, model_endpoint, max_retries=5, base_delay=2):
    token = get_jwt(token_url, username, password)
    config = {"maxTokens": 1000, "temperature": 0.5}
    payload = {
        "messages": [{"role": "user", "content": {"text": prompt}}],
        "inferenceConfig": config
    }
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {token}"}

    for attempt in range(max_retries):
        try:
            resp = requests.post(model_endpoint, json=payload, headers=headers, timeout=120)
            resp.raise_for_status()
            result = resp.json()
            return result['output']['message'][0]['text']
        except requests.exceptions.HTTPError as e:
            if resp.status_code == 429 and attempt < max_retries - 1:
                delay = base_delay * (2 ** attempt)
                time.sleep(delay)
            else:
                raise

def chunk_text(text, chunk_size):
    return [text[i:i+chunk_size] for i in range(0, len(text), chunk_size)]

def build_enhanced_prompt(user_instruction, chunk, idx, total):
    return (
        f"You are an expert in StreamSets pipelines. The user wants the following update:\n"
        f"'{user_instruction}'\n"
        f"You are working on chunk {idx+1} of {total} for a large pipeline JSON. "
        "Modify only this chunk as needed. Always return valid JSON.\n\n"
        f"-----\nPipeline chunk:\n{chunk}\n-----"
    )

def process_pipeline_json_in_chunks(json_data, chunk_size, user_instruction, token_url, username, password, model_endpoint, progress_callback=None):
    chunks = chunk_text(json_data, chunk_size)
    processed_chunks = []
    total = len(chunks)
    for i, chunk in enumerate(chunks):
        current_info = f"Processing chunk {i+1} of {total}..."
        if progress_callback:
            progress_callback(i, total, current_info)
        prompt = build_enhanced_prompt(user_instruction, chunk, i, total)
        output = call_llm(prompt, token_url, username, password, model_endpoint)
        processed_chunks.append(output)
    return "".join(processed_chunks)

def extract_json_from_zip(zip_bytes):
    with zipfile.ZipFile(io.BytesIO(zip_bytes), 'r') as zipf:
        for name in zipf.namelist():
            if name.endswith('.json'):
                return zipf.read(name).decode()
    return None
