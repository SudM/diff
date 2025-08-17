import argparse
from pathlib import Path
from report_generator import generate_toggle_report

def parse_args():
    parser = argparse.ArgumentParser(description="Generate Toggle Report from Deployment Package and Toggle Files.")
    parser.add_argument(
        "--deployment-package", "-d", type=Path, required=True,
        help="Path to the deployment package (.json/.xml/.zip)"
    )
    parser.add_argument(
        "--toggle", "-t", nargs=2, action="append", metavar=("ENV_NAME", "FILE"),
        required=True, help="Environment name and path to toggle JSON. Repeat per environment."
    )
    parser.add_argument(
        "--output", "-o", type=Path, default=Path("Toggle_Report_Output.html"),
        help="Path to output HTML report"
    )
    return parser.parse_args()

def main():
    args = parse_args()
    generate_toggle_report(args.deployment_package, args.toggle, args.output)
    print(f"✅ Report generated: {args.output}")

if __name__ == "__main__":
    main()
