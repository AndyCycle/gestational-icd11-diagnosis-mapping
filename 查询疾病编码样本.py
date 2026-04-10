import argparse
import os
import re

import pandas as pd


DIAGNOSIS_COLUMN_PATTERN = re.compile(r"diagnosis\d+$", re.IGNORECASE)
LEGACY_ORIGINAL_COLS = ["产科合并症", "手术适应症", "孕期风险项"]
INVISIBLE_CHAR_PATTERN = re.compile(r"[\u200b-\u200f\u202a-\u202e\u2060\ufeff]")
MULTISPACE_PATTERN = re.compile(r"\s+")
EDGE_NOISE_PATTERN = re.compile(r"^[\s\.,;:，。；：、\)\]】）]+|[\s\.,;:，。；：、\(\[【（]+$")


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
    base, _ = os.path.splitext(input_path)
    return f"{base}-query_result.csv"


def split_pipe(value):
    if value is None:
        return []
    text = str(value).strip()
    if text == "" or text.lower() == "nan":
        return []
    return [x.strip() for x in text.split("|") if x.strip()]


def normalize_text(text):
    if text is None:
        return ""
    value = str(text).strip()
    if value == "" or value.lower() == "nan":
        return ""
    value = INVISIBLE_CHAR_PATTERN.sub("", value)
    value = MULTISPACE_PATTERN.sub(" ", value)

    previous = None
    while previous != value:
        previous = value
        value = EDGE_NOISE_PATTERN.sub("", value).strip()
    return value


def detect_column_pairs(df):
    columns = list(df.columns)
    pairs = []

    diagnosis_cols = sorted(
        [c for c in columns if DIAGNOSIS_COLUMN_PATTERN.fullmatch(c)],
        key=lambda x: int(re.search(r"(\d+)$", x).group(1)),
    )
    for col in diagnosis_cols:
        code_col = f"{col}_ICD11_Code"
        if code_col in columns:
            pairs.append((col, code_col))

    if pairs:
        return pairs

    for col in LEGACY_ORIGINAL_COLS:
        code_col = f"{col}_ICD11_Code"
        if col in columns and code_col in columns:
            pairs.append((col, code_col))
    return pairs


def keyword_match(term, keyword, mode):
    if keyword == "":
        return True
    normalized_term = normalize_text(term)
    normalized_keyword = normalize_text(keyword)
    if mode == "exact":
        return normalized_term == normalized_keyword
    return normalized_keyword in normalized_term


def code_match(code, target_code):
    if target_code == "":
        return True
    return str(code).strip() == target_code


def find_same_item_matches(row, column_pairs, target_code, keyword, keyword_mode):
    matches = []
    for term_col, code_col in column_pairs:
        terms = split_pipe(row.get(term_col, ""))
        codes = split_pipe(row.get(code_col, ""))

        if not terms and not codes:
            continue

        pair_count = min(len(terms), len(codes))
        for idx in range(pair_count):
            term = terms[idx]
            code = codes[idx]
            if not code_match(code, target_code):
                continue
            if not keyword_match(term, keyword, keyword_mode):
                continue
            matches.append(
                {
                    "matched_column": term_col,
                    "matched_term": term,
                    "matched_code": code,
                    "match_scope": "same_item",
                }
            )
    return matches


def find_same_row_matches(row, column_pairs, target_code, keyword, keyword_mode):
    row_terms = []
    row_codes = []
    matched_columns = []

    for term_col, code_col in column_pairs:
        term_value = row.get(term_col, "")
        code_value = row.get(code_col, "")
        terms = split_pipe(term_value)
        codes = split_pipe(code_value)
        if terms:
            row_terms.extend(terms)
        if codes:
            row_codes.extend(codes)
        if terms or codes:
            matched_columns.append(term_col)

    if not row_terms and not row_codes:
        return []

    has_keyword = any(keyword_match(term, keyword, keyword_mode) for term in row_terms)
    has_code = any(code_match(code, target_code) for code in row_codes)
    if not has_keyword or not has_code:
        return []

    matched_terms = [term for term in row_terms if keyword_match(term, keyword, keyword_mode)]
    matched_codes = [code for code in row_codes if code_match(code, target_code)]
    return [
        {
            "matched_column": "|".join(matched_columns),
            "matched_term": "|".join(matched_terms),
            "matched_code": "|".join(matched_codes),
            "match_scope": "same_row",
        }
    ]


def main():
    parser = argparse.ArgumentParser(description="按 ICD 编码和疾病关键词定位样本。")
    parser.add_argument("-i", "--input", required=True, help="输入文件 CSV/Excel")
    parser.add_argument("-o", "--output", default="", help="输出结果文件；默认基于输入文件名生成")
    parser.add_argument("-c", "--code", default="", help="指定待查询编码，例如 5B81.Z")
    parser.add_argument("-k", "--keyword", default="", help="指定疾病关键词，例如 妊娠合并肥胖症")
    parser.add_argument(
        "--keyword-mode",
        choices=["contains", "exact"],
        default="contains",
        help="疾病关键词匹配方式，默认 contains",
    )
    parser.add_argument(
        "--scope",
        choices=["same_item", "same_row"],
        default="same_item",
        help="same_item 表示同一诊断子项同时满足编码和关键词；same_row 表示整行同时满足",
    )
    args = parser.parse_args()

    if args.code == "" and args.keyword == "":
        raise ValueError("至少提供 --code 或 --keyword 其中之一。")

    if not args.output:
        args.output = build_default_output_path(args.input)

    print(f"读取文件: {args.input}")
    df = read_table(args.input)

    column_pairs = detect_column_pairs(df)
    if not column_pairs:
        raise ValueError("未找到可用列对，请确认存在 diagnosis*_ICD11_Code 或旧结构列。")
    print(f"检测到列对: {column_pairs}")

    result_rows = []
    finder = find_same_item_matches if args.scope == "same_item" else find_same_row_matches

    for idx, row in df.iterrows():
        matches = finder(row, column_pairs, args.code.strip(), args.keyword.strip(), args.keyword_mode)
        for match in matches:
            row_dict = row.to_dict()
            row_dict["matched_row_index"] = idx
            row_dict["matched_column"] = match["matched_column"]
            row_dict["matched_term"] = match["matched_term"]
            row_dict["matched_code"] = match["matched_code"]
            row_dict["match_scope"] = match["match_scope"]
            result_rows.append(row_dict)

    result_df = pd.DataFrame(result_rows)
    write_table(result_df, args.output)

    print(f"定位完成: 命中样本数={len(result_df)}")
    print(f"已保存结果: {args.output}")
    if not result_df.empty:
        print("命中样例:")
        print(result_df.head(5).to_string(index=False))


if __name__ == "__main__":
    main()
