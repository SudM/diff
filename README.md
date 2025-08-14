
# json_diff_report (modular)

Diff report between two JSON files with bright red highlights for modified values.

## Features
- **arbitrary → services** by `name`
- **checkPermissions** by `action`
- **getPermissions** by `resourceType`
- Only **Added / Deleted / Modified** (unchanged omitted)
- **Row colours**: green (Added), red (Deleted), yellow (Modified)
- Modified rows show full V1/V2 with **exact changes in bright red**, plus a compact details table.

## Local files
```bash
python -m json_diff_report.cli "Toggle V1.json" "Toggle V2.json" -o "Diff_Report.html"
```

## GitHub mode (public or private)
Fetch by **branch** and **path**:

Public repos (no token needed for small files):
```bash
python -m json_diff_report.cli   --gh-owner your-org --gh-repo your-repo   --v1-branch main --v1-path config/Toggle\ V1.json   --v2-branch feature/xyz --v2-path config/Toggle\ V2.json   -o Diff_Report.html
```

Private repos (token required):
```bash
export GITHUB_TOKEN=ghp_xxx   # or set in your shell env
python -m json_diff_report.cli   --gh-owner your-org --gh-repo your-repo   --v1-branch main --v1-path config/Toggle\ V1.json   --v2-branch feature/xyz --v2-path config/Toggle\ V2.json   -o Diff_Report.html
```

The tool uses the **Contents API** when a token is provided (base64 decode handled automatically). Without a token, it tries the public **raw.githubusercontent.com** URL.

## SSO / OAuth Device Flow
Use your org's SSO without a PAT:
```bash
python -m json_diff_report.cli   --gh-owner your-org --gh-repo your-repo   --v1-branch main --v1-path config/Toggle\ V1.json   --v2-branch feature/xyz --v2-path config/Toggle\ V2.json   --oauth-device --oauth-client-id YOUR_CLIENT_ID   --oauth-scope "repo read:org"   -o Diff_Report.html
```

## GitHub Enterprise (custom domain)
```bash
python -m json_diff_report.cli   --gh-owner your-org --gh-repo your-repo   --v1-branch main --v1-path config/Toggle\ V1.json   --v2-branch feature/xyz --v2-path config/Toggle\ V2.json   --gh-base-url https://ghe.example.com   --oauth-device --oauth-client-id YOUR_CLIENT_ID   --oauth-scope "repo read:org"   -o Diff_Report.html
```
Assumes API at `https://ghe.example.com/api/v3` and raw files at `https://ghe.example.com/{owner}/{repo}/raw/{branch}/{path}`.
You can override OAuth endpoints with `--oauth-base-url` if needed.
