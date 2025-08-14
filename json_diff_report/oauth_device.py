
from __future__ import annotations
import time
import json
import webbrowser
from typing import Dict, Optional
from urllib import request, parse, error

DEFAULT_UA = "json-diff-report/1.1"

class OAuthDeviceError(RuntimeError):
    pass

def _post_form(url: str, data: Dict[str, str], headers: Optional[Dict[str,str]] = None, timeout: int = 30) -> Dict:
    body = parse.urlencode(data).encode("utf-8")
    hdrs = {"User-Agent": DEFAULT_UA, "Accept": "application/json", "Content-Type": "application/x-www-form-urlencoded"}
    if headers:
        hdrs.update(headers)
    req = request.Request(url, data=body, headers=hdrs, method="POST")
    try:
        with request.urlopen(req, timeout=timeout) as resp:
            txt = resp.read().decode("utf-8", errors="replace")
            return json.loads(txt)
    except error.HTTPError as e:
        msg = e.read().decode("utf-8", errors="replace") if hasattr(e, "read") else str(e)
        raise OAuthDeviceError(f"HTTPError {e.code}: {msg}") from e
    except error.URLError as e:
        raise OAuthDeviceError(f"URLError: {e}") from e

def start_device_flow(client_id: str, scope: str, open_browser: bool = True, base_url: Optional[str] = None) -> Dict[str, str]:
    """Start device code flow. Returns dict with device_code, user_code, verification_uri, expires_in, interval.
    base_url: None for github.com; for GHES use e.g. 'https://ghe.example.com'
    """
    device_endpoint = (base_url.rstrip('/') + "/login/device/code") if base_url else "https://github.com/login/device/code"
    resp = _post_form(device_endpoint, {"client_id": client_id, "scope": scope})
    if open_browser and "verification_uri" in resp:
        try:
            webbrowser.open(resp["verification_uri"], new=1, autoraise=True)
        except Exception:
            pass
    return resp

def poll_for_token(client_id: str, device_code: str, interval: int = 5, timeout_seconds: int = 600, base_url: Optional[str] = None) -> str:
    """Poll token endpoint until access_token is returned or timeout. Returns access_token string."""
    token_endpoint = (base_url.rstrip('/') + "/login/oauth/access_token") if base_url else "https://github.com/login/oauth/access_token"
    start = time.time()
    while True:
        resp = _post_form(token_endpoint, {
            "client_id": client_id,
            "device_code": device_code,
            "grant_type": "urn:ietf:params:oauth:grant-type:device_code"
        })
        if "access_token" in resp:
            return resp["access_token"]
        err = resp.get("error")
        if err == "authorization_pending":
            time.sleep(interval)
            if time.time() - start > timeout_seconds:
                raise OAuthDeviceError("Device flow polling timed out.")
            continue
        if err == "slow_down":
            interval += 5
            time.sleep(interval)
            if time.time() - start > timeout_seconds:
                raise OAuthDeviceError("Device flow polling timed out.")
            continue
        if err in ("expired_token", "access_denied", "unsupported_grant_type", "incorrect_client_credentials"):
            raise OAuthDeviceError(f"OAuth device flow error: {err}")
        raise OAuthDeviceError(f"Unexpected token response: {resp}")
