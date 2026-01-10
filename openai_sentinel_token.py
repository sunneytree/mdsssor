# client_test.py
import json

from src.core.http_utils import get_pow_token_mock, post_sentinel_req, build_openai_sentinel_token

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
