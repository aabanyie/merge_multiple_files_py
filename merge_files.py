# MERGING MULTIPLE FILES

import os
import pandas as pd
import csv


def load_data(data_dir=None):
    allowed = ['.csv', '.txt', '.xls', '.xlsx']
    if data_dir is None:
        default_dir = os.path.dirname(os.path.abspath(__file__))
        print()
        user_dir = input("Enter directory path to load data (leave blank for default): ").strip()
        data_dir = user_dir if user_dir else default_dir
    if not os.path.isdir(data_dir):
        print(f"Directory '{data_dir}' does not exist.")
        return None

    # collect only allowed files
    files = []
    for fname in os.listdir(data_dir):
        path = os.path.join(data_dir, fname)
        if os.path.isfile(path):
            ext = os.path.splitext(fname)[1].lower()
            if ext in allowed and not fname.startswith('~$') and not fname.startswith('.'):
                files.append(fname)
    if not files:
        print("No supported files (.csv, .txt, .xls, .xlsx) found in the folder.")
        return None

    # group by extension
    exts = {}
    for f in files:
        ext = os.path.splitext(f)[1].lower()
        exts.setdefault(ext, []).append(f)

    while True:
        print("\nAvailable file types in the folder:")
        ext_keys = list(exts.keys())
        for idx, ext in enumerate(ext_keys, 1):
            print(f"{idx}. {ext} ({len(exts[ext])} files)")

        # prompt for extension selection
        choice = input("\nSelect file type by number or extension (e.g. 1 or .csv), or 'q' to quit: ").strip().lower()
        if choice in ('q', 'quit'):
            print("Selection cancelled by user.")
            return None
        try:
            if choice.isdigit():
                ext_choice = ext_keys[int(choice) - 1]
            else:
                ext_choice = choice if choice.startswith('.') else '.' + choice
                if ext_choice not in exts:
                    print("Invalid extension selected.")
                    continue
        except Exception:
            print("Invalid selection.")
            continue

        # list files of selected type
        candidates = exts[ext_choice]
        print(f"\nFiles with '{ext_choice}':")
        for idx, fname in enumerate(candidates, 1):
            print(f"{idx}. {fname}")

        # Ask user Y/N for each file. If 'y' -> offer combine option or load single file.
        file_name = None
        for fname in candidates:
            ans = input(f"\nAre these the expected files to load? (y/n) [q to quit]: ").strip().lower()
            if ans in ('y', 'yes'):
                print(f"\nYou selected all {len(candidates)} '{ext_choice}' files.")
                print()
                comb = input(f"Combine all {len(candidates)} '{ext_choice}' files into one file? (y/n): ").strip().lower()
                if comb in ('y', 'yes'):
                    merged_path = combine_and_save_files(data_dir, ext_choice, candidates)
                    if merged_path:
                        # load merged file
                        try:
                            if ext_choice in ('.csv', '.txt'):
                                with open(merged_path, 'rb') as fh:
                                    sample = fh.read(8192)
                                try:
                                    sample_text = sample.decode('utf-8')
                                except Exception:
                                    sample_text = sample.decode('latin1', errors='ignore')
                                try:
                                    dialect = csv.Sniffer().sniff(sample_text)
                                    delimiter = dialect.delimiter
                                except Exception:
                                    delimiter = ','
                                with open(merged_path, 'r', encoding='utf-8', errors='ignore') as tf:
                                    df = pd.read_csv(tf, delimiter=delimiter)
                            else:
                                df = pd.read_excel(merged_path)
                            print()
                            print(f"Loaded merged file '{os.path.basename(merged_path)}' with {len(df):,} records and {len(df.columns):,} columns.")
                            return df
                        except Exception as e:
                            print(f"Error loading merged file: {e}")
                            return None
                    else:
                        # merging aborted or failed — return to selection
                        file_name = None
                        break
                else:
                    file_name = fname
                    break
            elif ans in ('n', 'no'):
                continue
            elif ans in ('q', 'quit'):
                print("Selection cancelled by user.")
                return None
            else:
                print("Please answer 'y' or 'n' (or 'q' to quit).")
                continue

        if file_name is None:
            # user did not pick a single file (or chose to merge which handled above) -> restart selection
            continue



def combine_and_save_files(data_dir: str, ext_choice: str, candidates: list):
    """
    Combine all files in `candidates` (filenames) located in `data_dir` with extension `ext_choice`.
    Ask user for output filename, save merged file in the same folder, and return the full path
    (or None on abort/error).
    Reports per-file errors (filename + issue) and continues merging remaining files.
    """
    ext_choice = ext_choice.lower()
    out_name = input(f"\nEnter output filename (leave blank for merged{ext_choice}): ").strip()
    if not out_name:
        out_name = f"merged{ext_choice}"
    if not out_name.lower().endswith(ext_choice):
        out_name = out_name + ext_choice
    out_path = os.path.join(data_dir, out_name)
    if os.path.exists(out_path):
        ans = input(f"File '{out_name}' already exists. Overwrite? (y/n): ").strip().lower()
        if ans not in ('y', 'yes'):
            print("Aborting save.")
            return None

    def robust_read_file(fp, delimiter=None):
        # Try using detected delimiter with python engine and warn on bad lines
        try:
            if delimiter:
                with open(fp, 'r', encoding='utf-8', errors='ignore') as tf:
                    return pd.read_csv(tf, delimiter=delimiter, engine='python', on_bad_lines='warn')
            # fallback: let pandas auto-detect
            return pd.read_csv(fp, sep=None, engine='python', on_bad_lines='warn')
        except Exception:
            # last resort: use csv.reader and pad rows to same length
            rows = []
            with open(fp, 'r', encoding='utf-8', errors='ignore') as fh:
                reader = csv.reader(fh)
                for r in reader:
                    rows.append(r)
            if not rows:
                return pd.DataFrame()
            maxlen = max(len(r) for r in rows)
            cols = [f'col_{i+1}' for i in range(maxlen)]
            normalized = [r + [''] * (maxlen - len(r)) for r in rows]
            return pd.DataFrame(normalized, columns=cols)

    per_file_errors = []
    read_success = []
    try:
        if ext_choice in ('.csv', '.txt'):
            dfs = []
            for fname in candidates:
                fp = os.path.join(data_dir, fname)
                try:
                    # attempt delimiter sniff
                    delimiter = None
                    try:
                        with open(fp, 'rb') as fh:
                            sample = fh.read(8192)
                        try:
                            sample_text = sample.decode('utf-8')
                        except Exception:
                            sample_text = sample.decode('latin1', errors='ignore')
                        try:
                            dialect = csv.Sniffer().sniff(sample_text)
                            delimiter = dialect.delimiter
                        except Exception:
                            delimiter = None
                    except Exception:
                        delimiter = None
                    df = robust_read_file(fp, delimiter=delimiter)
                    if df is None:
                        raise ValueError("No data returned")
                    dfs.append(df)
                    read_success.append(fname)
                except Exception as e:
                    per_file_errors.append((fname, str(e)))
                    print(f"[ERROR] Failed to read '{fname}': {e}")
                    # continue with next file
                    continue

            if not dfs:
                print("No files could be read successfully. Aborting merge.")
                return None

            merged = pd.concat(dfs, ignore_index=True, sort=False)
            merged.to_csv(out_path, index=False, encoding='utf-8')
        else:  # .xls / .xlsx
            dfs = []
            for fname in candidates:
                fp = os.path.join(data_dir, fname)
                try:
                    df = pd.read_excel(fp)
                    dfs.append(df)
                    read_success.append(fname)
                except Exception as e:
                    per_file_errors.append((fname, str(e)))
                    print(f"[ERROR] Failed to read '{fname}': {e}")
                    continue
            if not dfs:
                print("No Excel files could be read successfully. Aborting merge.")
                return None
            merged = pd.concat(dfs, ignore_index=True, sort=False)
            merged.to_excel(out_path, index=False)

        print(f"\nMerged {len(read_success)} / {len(candidates)} files -> {out_path}")
        if per_file_errors:
            print("\nSome files failed to merge:")
            for fname, err in per_file_errors:
                print(f" - {fname}: {err}")
        return out_path
    except Exception as e:
        print(f"Error merging files: {e}")
        if per_file_errors:
            print("\nFiles with errors encountered during merge:")
            for fname, err in per_file_errors:
                print(f" - {fname}: {err}")
        return None




def show_problem_lines(file_path, context=2, max_report=20):
    """Print lines with odd number of double quotes (likely malformed)."""
    bad = []
    with open(file_path, 'r', encoding='utf-8', errors='ignore') as fh:
        lines = fh.readlines()
    for i, L in enumerate(lines, 1):
        if L.count('"') % 2 == 1:
            bad.append(i)
            if len(bad) >= max_report:
                break
    if not bad:
        print("No obvious unbalanced-quote lines found.")
        return
    print(f"Found {len(bad)} suspicious lines (showing up to {max_report}):")
    for ln in bad:
        start = max(0, ln-1-context)
        end = min(len(lines), ln-1+context+1)
        print(f"\n--- Context for line {ln} ---")
        for j in range(start, end):
            prefix = ">>" if (j+1)==ln else "  "
            print(f"{prefix} {j+1:4d}: {lines[j].rstrip()}")

def main():
    df = load_data()
    if df is not None:
        print("\nFirst 5 rows of the loaded data:")
        print(df.head())

if __name__ == "__main__":
    main()

