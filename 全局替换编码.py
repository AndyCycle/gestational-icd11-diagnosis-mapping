import argparse
import os
import re

import pandas as pd


ICD_CODE_COLUMN_PATTERN = re.compile(r"diagnosis\d+_ICD11_Code$", re.IGNORECASE)
LEGACY_ORIGINAL_COLS = ["产科合并症", "手术适应症", "孕期风险项"]


def read_table(file_path):
    _, extension = os.path.splitext(file_path.lower())
    if extension == ".csv":
        return pd.read_csv(file_path, dtype=str, low_memory=False)
    if extension in [".xlsx", ".xls"]:
        return pd.read_excel(file_path, dtype=str)
    raise ValueError(f"不支持的文件类型: {extension}")


def write_table(df, file_path):
    _, extension = os.path.splitext(file_path.lower())
    if extension == ".csv":
        df.to_csv(file_path, index=False, encoding="utf-8-sig")
        return
    if extension in [".xlsx", ".xls"]:
        df.to_excel(file_path, index=False)
        return
    raise ValueError(f"不支持的输出类型: {extension}")


def build_default_output_path(input_path):
    base, extension = os.path.splitext(input_path)
    return f"{base}-code-fixed{extension}"


def build_default_report_path(input_path):
    base, _ = os.path.splitext(input_path)
    return f"{base}-code_replace_report.csv"


def split_pipe(value):
    if value is None:
        return []
    text = str(value).strip()
    if text == "" or text.lower() == "nan":
        return []
    return [x.strip() for x in text.split("|")]


def detect_code_columns(df):
    columns = list(df.columns)
    code_cols = [c for c in columns if ICD_CODE_COLUMN_PATTERN.fullmatch(c)]
    if code_cols:
        return sorted(code_cols)

    for col in LEGACY_ORIGINAL_COLS:
        code_col = f"{col}_ICD11_Code"
        if code_col in columns:
            code_cols.append(code_col)
    return code_cols


def load_mapping_table(mapping_file):
    mapping_df = read_table(mapping_file)
    required_cols = {"from_code", "to_code"}
    missing_cols = required_cols - set(mapping_df.columns)
    if missing_cols:
        raise ValueError(f"映射文件缺少列: {sorted(missing_cols)}")

    mappings = []
    for _, row in mapping_df.iterrows():
        from_code = str(row.get("from_code", "")).strip()
        to_code = str(row.get("to_code", "")).strip()
        enabled = str(row.get("enabled", "Y")).strip().upper()
        if enabled == "N" or from_code == "" or to_code == "":
            continue
        mappings.append((from_code, to_code))
    return mappings


def replace_codes_in_value(code_value, mappings):
    codes = split_pipe(code_value)
    if not codes:
        return code_value, 0

    new_codes = []
    changed = 0
    for code in codes:
        new_code = code
        for from_code, to_code in mappings:
            if code == from_code:
                new_code = to_code
                break
        if new_code != code:
            changed += 1
        new_codes.append(new_code)

    if changed == 0:
        return code_value, 0
    return "|".join(new_codes), changed


def main():
    parser = argparse.ArgumentParser(description="按旧编码全局替换为新编码。")
    parser.add_argument(
        "-i",
        "--input",
        required=True,
        help="输入文件 CSV/Excel",
    )
    parser.add_argument(
        "-o",
        "--output",
        default="",
        help="输出文件；未提供时默认在输入文件名后追加 -code-fixed",
    )
    parser.add_argument(
        "-p",
        "--report",
        default="",
        help="替换报告 CSV；未提供时默认基于输入文件名生成",
    )
    parser.add_argument(
        "--from-code",
        default="",
        help="待替换的旧编码",
    )
    parser.add_argument(
        "--to-code",
        default="",
        help="替换成的新编码",
    )
    parser.add_argument(
        "-m",
        "--mapping-file",
        default="",
        help="批量替换映射文件，需包含 from_code,to_code 列",
    )
    parser.add_argument(
        "-c",
        "--columns",
        nargs="*",
        default=[],
        help="只处理指定编码列；未提供时自动检测全部 *_ICD11_Code 列",
    )
    args = parser.parse_args()

    if not args.output:
        args.output = build_default_output_path(args.input)
    if not args.report:
        args.report = build_default_report_path(args.input)

    mappings = []
    if args.mapping_file:
        mappings.extend(load_mapping_table(args.mapping_file))
    if args.from_code or args.to_code:
        if not args.from_code or not args.to_code:
            raise ValueError("--from-code 和 --to-code 必须同时提供。")
        mappings.append((args.from_code.strip(), args.to_code.strip()))
    if not mappings:
        raise ValueError("请提供 --from-code/--to-code，或提供 --mapping-file。")

    print(f"读取文件: {args.input}")
    df = read_table(args.input)

    code_columns = args.columns or detect_code_columns(df)
    if not code_columns:
        raise ValueError("未检测到可用编码列。")
    print(f"处理编码列: {code_columns}")
    print(f"加载替换规则数: {len(mappings)}")

    total_changed_rows = 0
    total_changed_items = 0
    report_rows = []

    for idx, row in df.iterrows():
        row_changed = False
        for code_col in code_columns:
            old_code = row.get(code_col, "")
            new_code, changed_items = replace_codes_in_value(old_code, mappings)
            if changed_items == 0:
                continue

            df.at[idx, code_col] = new_code
            total_changed_items += changed_items
            row_changed = True
            report_rows.append(
                {
                    "row_index": idx,
                    "column": code_col,
                    "old_code": old_code,
                    "new_code": new_code,
                    "changed_items": changed_items,
                }
            )
        if row_changed:
            total_changed_rows += 1

    print(f"替换完成: 修改行数={total_changed_rows}, 修改编码数={total_changed_items}")
    write_table(df, args.output)
    print(f"已保存结果: {args.output}")

    report_df = pd.DataFrame(report_rows)
    report_df.to_csv(args.report, index=False, encoding="utf-8-sig")
    print(f"已保存报告: {args.report}")

    if not report_df.empty:
        print("替换样例:")
        print(report_df.head(5).to_string(index=False))


if __name__ == "__main__":
    main()
