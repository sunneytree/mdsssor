import os
import json
import base64
import random
import urllib.request
import urllib.error
from datetime import datetime, timedelta, timezone

SORA_APP_USER_AGENT = "Sora/1.2026.007 (Android 15; 24122RKC7C; build 2600700)"

POW_MAX_ITERATION = 500000
POW_CORES = [8, 16, 24, 32]
POW_SCRIPTS = [
    "https://cdn.oaistatic.com/_next/static/cXh69klOLzS0Gy2joLDRS/_ssgManifest.js?dpl=453ebaec0d44c2decab71692e1bfe39be35a24b3"
]
POW_DPL = ["prod-f501fe933b3edf57aea882da888e1a544df99840"]
POW_NAVIGATOR_KEYS = [
    "registerProtocolHandler-function registerProtocolHandler() { [native code] }",
    "storage-[object StorageManager]",
    "locks-[object LockManager]",
    "appCodeName-Mozilla",
    "permissions-[object Permissions]",
    "webdriver-false",
    "vendor-Google Inc.",
    "mediaDevices-[object MediaDevices]",
    "cookieEnabled-true",
    "product-Gecko",
    "productSub-20030107",
    "hardwareConcurrency-32",
    "onLine-true",
]
POW_DOCUMENT_KEYS = ["_reactListeningo743lnnpvdg", "location"]
POW_WINDOW_KEYS = [
    "0", "window", "self", "document", "name", "location",
    "navigator", "screen", "innerWidth", "innerHeight",
    "localStorage", "sessionStorage", "crypto", "performance",
    "fetch", "setTimeout", "setInterval", "console",
]


def _get_header(headers, name):
    if not headers:
        return None
    for key, value in headers.items():
        if key.lower() == name.lower():
            return value
    return None


def generate_id():
    import uuid
    return str(uuid.uuid4())


def generate_device_id():
    import uuid
    return str(uuid.uuid4())


def get_pow_parse_time():
    now = datetime.now(timezone(timedelta(hours=-5)))
    return now.strftime("%a %b %d %Y %H:%M:%S") + " GMT-0500 (Eastern Standard Time)"


def get_pow_config(user_agent):
    import time
    import uuid
    return [
        random.choice([1920 + 1080, 2560 + 1440, 1920 + 1200, 2560 + 1600]),
        get_pow_parse_time(),
        4294705152,
        0,
        user_agent,
        random.choice(POW_SCRIPTS) if POW_SCRIPTS else "",
        random.choice(POW_DPL) if POW_DPL else None,
        "en-US",
        "en-US,es-US,en,es",
        0,
        random.choice(POW_NAVIGATOR_KEYS),
        random.choice(POW_DOCUMENT_KEYS),
        random.choice(POW_WINDOW_KEYS),
        time.perf_counter() * 1000,
        str(uuid.uuid4()),
        "",
        random.choice(POW_CORES),
        time.time() * 1000 - (time.perf_counter() * 1000),
    ]


def solve_pow(seed, difficulty, config):
    import hashlib
    diff_len = len(difficulty) // 2
    seed_encoded = seed.encode()
    target_diff = bytes.fromhex(difficulty)

    static_part1 = (json.dumps(config[:3], separators=(",", ":"), ensure_ascii=False)[:-1] + ",").encode()
    static_part2 = ("," + json.dumps(config[4:9], separators=(",", ":"), ensure_ascii=False)[1:-1] + ",").encode()
    static_part3 = ("," + json.dumps(config[10:], separators=(",", ":"), ensure_ascii=False)[1:]).encode()

    for i in range(POW_MAX_ITERATION):
        dynamic_i = str(i).encode()
        dynamic_j = str(i >> 1).encode()
        final_json = static_part1 + dynamic_i + static_part2 + dynamic_j + static_part3
        b64_encoded = base64.b64encode(final_json)

        hash_value = hashlib.sha3_512(seed_encoded + b64_encoded).digest()
        if hash_value[:diff_len] <= target_diff:
            return b64_encoded.decode(), True

    error_token = "wQ8Lk5FbGpA2NcR9dShT6gYjU7VxZ4D" + base64.b64encode(f"\"{seed}\"".encode()).decode()
    return error_token, False


def get_pow_token(user_agent):
    seed = format(random.random())
    difficulty = "0fffff"
    config = get_pow_config(user_agent)
    solution, _ = solve_pow(seed, difficulty, config)
    return "gAAAAAC" + solution


def build_openai_sentinel_token(flow, resp, pow_token, user_agent):
    final_pow_token = pow_token
    proofofwork = resp.get("proofofwork", {})
    if proofofwork.get("required"):
        seed = proofofwork.get("seed", "")
        difficulty = proofofwork.get("difficulty", "")
        if seed and difficulty:
            config = get_pow_config(user_agent)
            solution, _ = solve_pow(seed, difficulty, config)
            final_pow_token = "gAAAAAB" + solution

    token_payload = {
        "p": final_pow_token,
        "t": resp.get("turnstile", {}).get("dx", ""),
        "c": resp.get("token", ""),
        "id": generate_id(),
        "flow": flow,
    }
    return json.dumps(token_payload, ensure_ascii=False, separators=(",", ":"))


def post_json(url, payload, headers, timeout=20):
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, resp.read().decode("utf-8")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8")


def lambda_handler(event, context):
    headers = event.get("headers") or {}
    expected_key = os.environ.get("LAMBDA_SHARED_KEY")
    if expected_key:
        provided_key = _get_header(headers, "x-lambda-key")
        if provided_key != expected_key:
            return {"statusCode": 401, "body": json.dumps({"error": "invalid lambda key"})}

    body = event.get("body") or "{}"
    if event.get("isBase64Encoded"):
        body = base64.b64decode(body).decode("utf-8")
    data = json.loads(body) if body else {}

    token = data.get("token")
    payload = data.get("payload") or data.get("nf_create")
    if not token or not payload:
        return {"statusCode": 400, "body": json.dumps({"error": "token and payload required"})}

    user_agent = data.get("user_agent") or SORA_APP_USER_AGENT
    flow = data.get("flow") or "sora_2_create_task"

    sentinel_base = os.environ.get("SENTINEL_BASE_URL", "https://chatgpt.com")
    sora_base = os.environ.get("SORA_BASE_URL", "https://sora.chatgpt.com/backend")

    pow_token = get_pow_token(user_agent)
    sentinel_req_payload = {"p": pow_token, "flow": flow, "id": generate_id()}
    sentinel_headers = {
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json",
        "Origin": "https://chatgpt.com",
        "Referer": "https://chatgpt.com/",
        "User-Agent": user_agent,
        "Authorization": f"Bearer {token}",
    }
    sentinel_url = sentinel_base.rstrip("/") + "/backend-api/sentinel/req"
    status, sentinel_body = post_json(sentinel_url, sentinel_req_payload, sentinel_headers, timeout=10)
    if status != 200:
        return {"statusCode": status, "body": sentinel_body}

    sentinel_resp = json.loads(sentinel_body)
    sentinel_token = build_openai_sentinel_token(flow, sentinel_resp, pow_token, user_agent)

    sora_headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Origin": "https://sora.chatgpt.com",
        "Referer": "https://sora.chatgpt.com/",
        "User-Agent": user_agent,
        "Authorization": f"Bearer {token}",
        "oai-device-id": generate_device_id(),
        "openai-sentinel-token": sentinel_token,
        "oai-package-name": "com.openai.sora",
        "oai-client-type": "android",
    }

    sora_url = sora_base.rstrip("/") + "/nf/create"
    status, resp_body = post_json(sora_url, payload, sora_headers, timeout=20)
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json"},
        "body": resp_body,
    }
