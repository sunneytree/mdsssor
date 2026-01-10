# client_test.py
import json
import requests
from typing import Dict

from src.core.http_utils import (
    generate_id,
    get_pow_token_mock,
    build_openai_sentinel_token,
)

def post_sentinel_req(base_url: str, flow: str, pow_token: str) -> Dict:
    url = f"{base_url}/backend-api/sentinel/req"
    payload = {"p": pow_token, "flow": flow, "id": generate_id()}

    r = requests.post(url, json=payload, timeout=10)
    r.raise_for_status()
    return r.json()

def main():
    base_url = "https://chatgpt.com/"
    flow = "sora_2_create_task"

    pow_token = get_pow_token_mock()
    resp = post_sentinel_req(base_url, flow, pow_token)
    openai_sentinel_token = build_openai_sentinel_token(flow, resp, pow_token)

    print("Mock /sentinel/req response:")
    print(json.dumps(resp, ensure_ascii=False, indent=2))
    print("\nOpenAI-Sentinel-Token (mock):")
    print(openai_sentinel_token)

if __name__ == "__main__":
    main()
