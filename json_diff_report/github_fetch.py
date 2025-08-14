
from __future__ import annotations
import base64
import json
from typing import Optional
from urllib import request, error, parse

DEFAULT_UA = "json-diff-report/1.1"

class GitHubFetchError(RuntimeError):
    pass

def _http_get(url: str, headers: dict, timeout: int = 30) -> bytes:
    req = request.Request(url, headers=headers, method="GET")
    try:
        with request.urlopen(req, timeout=timeout) as resp:
            return resp.read()
    except error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace") if hasattr(e, "read") else ""
        raise GitHubFetchError(f"HTTPError {e.code} for {url}: {body}") from e
    except error.URLError as e:
        raise GitHubFetchError(f"URLError for {url}: {e}") from e

def fetch_text(
    owner: str,
    repo: str,
    branch: str,
    path: str,
    token: Optional[str] = None,
    timeout: int = 30,
    base_api: Optional[str] = None,
    base_raw: Optional[str] = None,
) -> str:
    """Fetch file content as text from GitHub.com or GitHub Enterprise.
    - If token is provided, prefer the Contents API (works for public/private).
    - If no token and base_api is provided, still try the Contents API unauthenticated.
    - If no token and base_api is None, fall back to raw host (public only).
      * Dotcom default raw host: https://raw.githubusercontent.com/{owner}/{repo}/{branch}/{path}
      * GHES typical raw path:   {base_raw}/{owner}/{repo}/raw/{branch}/{path}
    """
    # Defaults
    api = base_api or "https://api.github.com"
    raw_host = base_raw or "https://raw.githubusercontent.com"

    # Use API if token provided (or if caller passed base_api explicitly)
    if token or base_api:
        api_url = f"{api}/repos/{owner}/{repo}/contents/{parse.quote(path)}?ref={parse.quote(branch)}"
        headers = {
            "User-Agent": DEFAULT_UA,
            "Accept": "application/vnd.github+json",
        }
        if token:
            headers["Authorization"] = f"Bearer {token}"
        data = _http_get(api_url, headers, timeout=timeout)
        obj = json.loads(data.decode("utf-8", errors="replace"))
        if isinstance(obj, dict) and obj.get("encoding") == "base64" and "content" in obj:
            raw = base64.b64decode(obj["content"])
            return raw.decode("utf-8", errors="replace")
        if isinstance(obj, dict) and "download_url" in obj and obj["download_url"]:
            dl_headers = {"User-Agent": DEFAULT_UA}
            if token:
                dl_headers["Authorization"] = f"Bearer {token}"
            data2 = _http_get(obj["download_url"], dl_headers, timeout=timeout)
            return data2.decode("utf-8", errors="replace")
        raise GitHubFetchError("Unexpected API response; missing base64 content or download_url.")
    # Raw fallback (public only)
    if base_raw:
        raw_url = f"{raw_host}/{owner}/{repo}/raw/{branch}/{path}"
    else:
        raw_url = f"{raw_host}/{owner}/{repo}/{branch}/{path}"
    data = _http_get(raw_url, {"User-Agent": DEFAULT_UA}, timeout=timeout)
    return data.decode("utf-8", errors="replace")

def fetch_json(
    owner: str,
    repo: str,
    branch: str,
    path: str,
    token: Optional[str] = None,
    timeout: int = 30,
    base_api: Optional[str] = None,
    base_raw: Optional[str] = None,
):
    text = fetch_text(owner, repo, branch, path, token=token, timeout=timeout, base_api=base_api, base_raw=base_raw)
    return json.loads(text)
