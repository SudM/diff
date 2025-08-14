
from __future__ import annotations
import argparse
import os
from pathlib import Path

from .report import build_report, build_report_from_objects
from .github_fetch import fetch_json, GitHubFetchError
from .oauth_device import start_device_flow, poll_for_token, OAuthDeviceError

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Compare services (by name), checkPermissions (by action), getPermissions (by resourceType). "
                    "Rows coloured by status; modified values highlighted in bright red. "
                    "Can read from local files or fetch from GitHub (GitHub.com or GHES)."
    )
    # Local file mode (optional if using GitHub mode)
    p.add_argument("v1", nargs="?", type=Path, help="Path to V1 JSON (omit if using GitHub flags)")
    p.add_argument("v2", nargs="?", type=Path, help="Path to V2 JSON (omit if using GitHub flags)")

    # GitHub mode
    p.add_argument("--gh-owner", type=str, help="GitHub owner/org (e.g., my-org)")
    p.add_argument("--gh-repo", type=str, help="GitHub repository name (e.g., my-repo)")
    p.add_argument("--v1-branch", type=str, help="Branch name for V1 (e.g., main)")
    p.add_argument("--v1-path", type=str, help="File path in repo for V1 JSON (e.g., config/toggles.json)")
    p.add_argument("--v2-branch", type=str, help="Branch name for V2 (e.g., feature/foo)")
    p.add_argument("--v2-path", type=str, help="File path in repo for V2 JSON")
    p.add_argument("--gh-token", type=str, default=os.getenv("GITHUB_TOKEN"), help="GitHub token (or set env GITHUB_TOKEN)")
    p.add_argument("--gh-base-url", type=str, help="Base URL for GitHub Enterprise (e.g., https://ghe.example.com). If set, API assumed at /api/v3 and raw at host root.")

    # Device (SSO) OAuth flow
    p.add_argument("--oauth-device", action="store_true", help="Use OAuth device flow (SSO) to obtain a token for GitHub")
    p.add_argument("--oauth-client-id", type=str, help="GitHub OAuth App client_id for device flow")
    p.add_argument("--oauth-scope", type=str, default="repo read:org", help="OAuth scopes (default: 'repo read:org')")
    p.add_argument("--oauth-base-url", type=str, help="Base URL for OAuth endpoints (defaults to gh-base-url or github.com).")

    # Output
    p.add_argument("-o", "--output", type=Path, default=Path("Diff_Report.html"), help="Output HTML file")
    return p.parse_args()

def main() -> None:
    args = parse_args()

    # Compute base urls
    base_api = None
    base_raw = None
    if args.gh_base_url:
        gh = args.gh_base_url.rstrip('/')
        base_api = gh + '/api/v3'
        base_raw = gh  # GHES raw pattern: https://ghe/owner/repo/raw/branch/path

    # Device flow (SSO) if requested and no token supplied
    if not args.gh_token and args.oauth_device:
        if not args.oauth_client_id:
            raise SystemExit("When using --oauth-device you must provide --oauth-client-id (from your GitHub OAuth App).")
        try:
            oauth_base = args.oauth_base_url or args.gh_base_url
            info = start_device_flow(args.oauth_client_id, args.oauth_scope, open_browser=True, base_url=oauth_base)
        except OAuthDeviceError as e:
            raise SystemExit(f"Failed to start OAuth device flow: {e}")
        print("== GitHub Device Flow ==")
        print("Go to:", info.get("verification_uri"))
        print("Enter code:", info.get("user_code"))
        interval = int(info.get("interval", 5))
        try:
            token = poll_for_token(args.oauth_client_id, info["device_code"], interval=interval, base_url=oauth_base)
        except OAuthDeviceError as e:
            raise SystemExit(f"OAuth device flow failed: {e}")
        args.gh_token = token
        print("SSO/OAuth token acquired.")

    # Prefer local files if both provided
    if args.v1 and args.v2:
        if not args.v1.exists():
            raise SystemExit(f"Not found: {args.v1}")
        if not args.v2.exists():
            raise SystemExit(f"Not found: {args.v2}")
        html = build_report(args.v1, args.v2)
        args.output.write_text(html, encoding="utf-8")
        print("Report saved to:", args.output.resolve())
        return

    # Otherwise try GitHub mode
    required = [args.gh_owner, args.gh_repo, args.v1_branch, args.v1_path, args.v2_branch, args.v2_path]
    if all(required):
        try:
            j1 = fetch_json(args.gh_owner, args.gh_repo, args.v1_branch, args.v1_path, token=args.gh_token, base_api=base_api, base_raw=base_raw)
            j2 = fetch_json(args.gh_owner, args.gh_repo, args.v2_branch, args.v2_path, token=args.gh_token, base_api=base_api, base_raw=base_raw)
        except GitHubFetchError as e:
            raise SystemExit(f"GitHub fetch failed: {e}")
        label1 = f"{args.gh_owner}/{args.gh_repo}@{args.v1_branch}:{args.v1_path}"
        label2 = f"{args.gh_owner}/{args.gh_repo}@{args.v2_branch}:{args.v2_path}"
        html = build_report_from_objects(j1, j2, source1=label1, source2=label2)
        args.output.write_text(html, encoding="utf-8")
        print("Report saved to:", args.output.resolve())
        return

    raise SystemExit("Provide either local files (v1 v2) OR all GitHub flags (--gh-owner --gh-repo --v1-branch --v1-path --v2-branch --v2-path).")

if __name__ == "__main__":
    main()
