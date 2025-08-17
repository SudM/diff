from datetime import datetime
import html

def _badge(val: str) -> str:
    return '<span class="badge-on">ON</span>' if (val or "").upper() == "ON" else '<span class="badge-off">OFF</span>'

def build_html(rows, missing_by_env, env_names, title="Action → PolicySet → Toggle Report") -> str:
    style = "<style>table{border-collapse:collapse;width:100%}th,td{border:1px solid #ddd;padding:8px;}th{background:#f2f2f2;} .badge-on{background:#d4edda;color:#155724;padding:2px 5px} .badge-off{background:#f8d7da;color:#721c24;padding:2px 5px}</style>"
    html_out = f"<html><head><meta charset='utf-8'><title>{title}</title>{style}</head><body>"
    html_out += f"<h1>{title}</h1><div>Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</div><table><tr>"
    headers = ["Action", "Tenant", "Version"] + env_names
    html_out += "".join(f"<th>{html.escape(h)}</th>" for h in headers) + "</tr>"
    for r in rows:
        html_out += "<tr>" + "".join(f"<td>{html.escape(r.get(col, '')) if col not in env_names else _badge(r[col])}</td>" for col in headers) + "</tr>"
    return html_out