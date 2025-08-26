import pandas as pd
from .utils import logger

def export_to_excel(datasets: dict, filename: str = "All_Datasets.xlsx"):
    """Export multiple DataFrames into an Excel workbook."""
    with pd.ExcelWriter(filename, engine="xlsxwriter") as writer:
        for sheet, df in datasets.items():
            df.to_excel(writer, sheet_name=sheet, index=False)
    logger.info(f"Excel exported to {filename}")


def export_environment_drift_html(df, output_file="Environment Drift.html"):
    """Export drift results as interactive HTML with filters and row coloring."""
    df_html = df.copy().rename(columns={"Value_action": "Action"})
    base_cols = ["Position", "ID", "Policy FullPath", "Action"]
    env_cols = [c for c in df_html.columns if c.upper().startswith(("SIT", "UAT", "PROD"))]
    df_html = df_html[base_cols + env_cols]

    dropdown_values = {col: sorted(set(str(v).strip() for v in df_html[col].dropna().unique())) for col in ["Action"] + env_cols}

    rows = []
    for _, row in df_html.iterrows():
        vals = [str(row[c]).strip() for c in env_cols if pd.notna(row[c])]
        drift = len(set(vals)) > 1
        all_off = all(v.upper() == "OFF" for v in vals if v)
        is_root = str(row["Policy FullPath"]).strip() == "PolicySet:Root"
        row_style = ""
        if not is_root:
            if all_off:
                row_style = ' style="background-color:#FFCCCC"'  # light red
            elif drift:
                row_style = ' style="background-color:#FFBF00"'  # amber
        new_row = "".join([f"<td>{row[col]}</td>" for col in base_cols + env_cols])
        rows.append(f"<tr{row_style}>{new_row}</tr>")

    header_cells = []
    for col in base_cols + env_cols:
        if col == "Action" or col in env_cols:
            options = "".join([f"<option value='{val}'>{val}</option>" for val in dropdown_values.get(col, []) if val])
            dropdown = f"<br><select onchange=\"colFilter(this, {col!r})\"><option value=''>All</option>{options}</select>"
            header_cells.append(f"<th>{col}{dropdown}</th>")
        else:
            header_cells.append(f"<th>{col}</th>")
    header_html = "".join(header_cells)

    html = f"""
    <html>
    <head>
      <meta charset="utf-8">
      <title>Environment Drift</title>
      <style>
        table {{ border-collapse: collapse; width: 100%; }}
        th, td {{ border: 1px solid #ccc; padding: 6px; text-align: left; }}
        th {{ background-color: #f2f2f2; position: sticky; top: 0; }}
      </style>
      <script>
        function colFilter(selectElem, colName) {{
          var filter = selectElem.value.toLowerCase();
          var table = document.getElementById("driftTable");
          var colIndex = Array.from(table.rows[0].cells).findIndex(c => c.innerText.startsWith(colName));
          var trs = table.getElementsByTagName("tr");
          for (var i = 1; i < trs.length; i++) {{
            var td = trs[i].getElementsByTagName("td")[colIndex];
            if (td) {{
              if (filter === "" || td.innerText.toLowerCase() === filter) {{
                trs[i].style.display = "";
              }} else {{
                trs[i].style.display = "none";
              }}
            }}
          }}
        }}
      </script>
    </head>
    <body>
      <h2>Environment Drift Report</h2>
      <table id="driftTable">
        <tr>{header_html}</tr>
        {''.join(rows)}
      </table>
    </body>
    </html>
    """
    with open(output_file, "w", encoding="utf-8") as f:
        f.write(html)
    logger.info(f"Environment drift HTML exported to {output_file}")
