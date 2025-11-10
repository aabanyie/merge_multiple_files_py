import argparse
import glob
import json
import os
from pathlib import Path
import csv

import pandas as pd

def load_data(data_dir=None):
    if data_dir is None:
        default_dir = os.path.dirname(os.path.abspath(__file__))
        print()
        user_dir = input("Enter directory path to load data (leave blank for default): ").strip()
        data_dir = user_dir if user_dir else default_dir
    if not os.path.isdir(data_dir):
        print(f"Directory '{data_dir}' does not exist.")
        return None, None
    files = [f for f in os.listdir(data_dir)
             if os.path.isfile(os.path.join(data_dir, f))
             and not f.startswith('~$')
             and not f.startswith('.')]
    if not files:
        print("No files found in the folder.")
        return None, None
    print("\nAvailable files in the folder:")
    for idx, file in enumerate(files, 1):
        print(f"{idx}. {file}")
    try:
        choice = int(input("\nEnter the number of the file to load: "))
        file_name = files[choice - 1]
    except (ValueError, IndexError):
        print("Invalid selection.")
        return None, None
    file_path = os.path.join(data_dir, file_name)
    try:
        if file_name.lower().endswith('.csv'):
            # attempt sniffing delimiter for csv as well
            with open(file_path, 'rb') as f:
                sample = f.read(8192)
            delim = detect_delimiter(sample)
            df = pd.read_csv(file_path, delimiter=delim, encoding='utf-8', errors='ignore')
        elif file_name.lower().endswith(('.xls', '.xlsx')):
            df = pd.read_excel(file_path)
        elif file_name.lower().endswith('.txt'):
            # Inline delimiter detection with explicit encoding and fallback
            with open(file_path, 'rb') as f:
                sample = f.read(8192)
            delim = detect_delimiter(sample)
            print(f"Auto-detected delimiter: '{delim}'")
            df = pd.read_csv(file_path, delimiter=delim, encoding='utf-8', errors='ignore')
        else:
            print("Unsupported file format. Please provide a CSV, TXT, or Excel file.")
            return None, None
        print()
        print(f"Data loaded successfully with {len(df):,} records and {len(df.columns):,} columns.")
        return df, file_path
    except Exception as e:
        print(f"Error loading data: {e}")
        return None, None

def detect_delimiter(sample_bytes: bytes) -> str:
    try:
        sample = sample_bytes.decode(errors="ignore")
        dialect = csv.Sniffer().sniff(sample)
        return dialect.delimiter
    except Exception:
        return ","


def list_files(source: str, pattern: str = "*", recursive: bool = False):
    p = Path(source)
    if p.is_file():
        return [str(p)]
    if recursive:
        return [str(p) for p in p.rglob(pattern)]
    return [str(p) for p in p.glob(pattern)]


def merge_csv(files, output_path, index=False, infer_delimiter=True, **read_kwargs):
    dfs = []
    for f in files:
        if infer_delimiter:
            with open(f, "rb") as fh:
                delim = detect_delimiter(fh.read(4096))
            df = pd.read_csv(f, delimiter=delim, encoding=read_kwargs.get("encoding", "utf-8"), errors="ignore", **{k:v for k,v in read_kwargs.items() if k not in ("encoding",)})
        else:
            df = pd.read_csv(f, **read_kwargs)
        dfs.append(df)
    out = pd.concat(dfs, ignore_index=True, sort=False)
    out.to_csv(output_path, index=index, encoding='utf-8')
    return output_path


def merge_excel(files, output_path, index=False, sheet_name=None, **read_kwargs):
    dfs = []
    for f in files:
        df = pd.read_excel(f, sheet_name=sheet_name or 0, **read_kwargs)
        dfs.append(df)
    out = pd.concat(dfs, ignore_index=True)
    out.to_excel(output_path, index=index)
    return output_path


def merge_json(files, output_path):
    merged = []
    for f in files:
        try:
            with open(f, "r", encoding="utf-8") as fh:
                data = json.load(fh)
                if isinstance(data, list):
                    merged.extend(data)
                else:
                    merged.append(data)
        except json.JSONDecodeError:
            # try newline-delimited JSON (ndjson)
            with open(f, "r", encoding="utf-8", errors="ignore") as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        merged.append(json.loads(line))
                    except Exception:
                        # skip malformed lines
                        continue
    with open(output_path, "w", encoding="utf-8") as fh:
        json.dump(merged, fh, ensure_ascii=False, indent=2)
    return output_path


def merge_txt(files, output_path, encoding="utf-8"):
    with open(output_path, "w", encoding=encoding) as outfh:
        for f in files:
            with open(f, "r", encoding=encoding, errors="ignore") as fh:
                outfh.write(f"\n--- FILE: {os.path.basename(f)} ---\n")
                outfh.write(fh.read())
    return output_path


def merge_parquet(files, output_path):
    dfs = [pd.read_parquet(f) for f in files]
    out = pd.concat(dfs, ignore_index=True)
    out.to_parquet(output_path, index=False)
    return output_path


def main():
    parser = argparse.ArgumentParser(
        description="Merge multiple files of the same type (csv, xlsx, json, txt, parquet)."
    )
    # make source optional so load_data can be used interactively
    parser.add_argument("source", nargs='?', help="file or directory containing files to merge (leave blank for interactive file selection)", default=None)
    parser.add_argument(
        "--ext",
        help="file extension to merge (csv, xlsx, json, txt, parquet). If omitted, inferred from files.",
        default=None,
    )
    parser.add_argument("--pattern", help="glob pattern (e.g. '*.csv')", default="*")
    parser.add_argument("--recursive", help="search recursively", action="store_true")
    parser.add_argument("--output", help="output file path (defaults to merged.<ext>)", default=None)
    args = parser.parse_args()

    # If no source supplied, let user pick a file via load_data()
    if args.source is None:
        df, selected_path = load_data()
        if selected_path is None:
            raise SystemExit("No file selected.")
        # treat selected file as the single-source for merging
        files = [selected_path]
        ext = Path(selected_path).suffix.lower().lstrip(".")
        # default output in same parent directory
        base_dir = Path(selected_path).resolve().parent
        output = args.output or str(base_dir / f"merged.{ext}")
    else:
        files = list_files(args.source, pattern=args.pattern, recursive=args.recursive)
        if not files:
            raise SystemExit("No files found.")

        # filter by ext if provided
        if args.ext:
            ext = args.ext.lower().lstrip(".")
            files = [f for f in files if Path(f).suffix.lower().lstrip(".") == ext]
        else:
            # infer ext from first file
            ext = Path(files[0]).suffix.lower().lstrip(".")
            files = [f for f in files if Path(f).suffix.lower().lstrip(".") == ext]

        if not files:
            raise SystemExit("No files matching the chosen extension were found.")

        # compute default output path in parent dir if source is a file
        if args.output:
            output = args.output
        else:
            src_path = Path(args.source).resolve()
            base_dir = src_path.parent if src_path.is_file() else src_path
            output = str(base_dir / f"merged.{ext}")

    # exclude the output file from files list if it exists in the same dir
    files = [f for f in files if os.path.abspath(f) != os.path.abspath(output)]

    print(f"Merging {len(files)} .{ext} files -> {output}")

    if ext == "csv":
        merge_csv(files, output)
    elif ext in ("xls", "xlsx"):
        merge_excel(files, output)
    elif ext == "json":
        merge_json(files, output)
    elif ext == "txt":
        merge_txt(files, output)
    elif ext == "parquet":
        merge_parquet(files, output)
    else:
        raise SystemExit(f"Unsupported extension: {ext}")

    print("Done.")

if __name__ == "__main__":
    main()