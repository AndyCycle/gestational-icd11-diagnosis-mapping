import argparse
import os
import re

import pandas as pd


DIAGNOSIS_COLUMN_PATTERN = re.compile(r"diagnosis\d+$", re.IGNORECASE)
LEGACY_ORIGINAL_COLS = ["产科合并症", "手术适应症", "孕期风险项"]
INVISIBLE_CHAR_PATTERN = re.compile(r"[\u200b-\u200f\u202a-\u202e\u2060\ufeff]")
MULTISPACE_PATTERN = re.compile(r"\s+")
EDGE_NOISE_PATTERN = re.compile(r"^[\s\.,;:，。；：、\)\]】）]+|[\s\.,;:，。；：、\(\[【（]+$")
PURE_NOISE_PATTERN = re.compile(r"^[\W_]+$", re.UNICODE)
EXPLICIT_NOISE_TERMS = {"无"}


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
    return f"{base}-fixed{extension}"


def build_default_report_path(input_path):
    base, _ = os.path.splitext(input_path)
    return f"{base}-fix_apply_report.csv"


def split_pipe(value):
    if value is None:
        return []
    text = str(value).strip()
    if text == "" or text.lower() == "nan":
        return []
    return [x.strip() for x in text.split("|")]


def normalize_term(term):
    if term is None:
        return ""
    text = str(term).strip()
    if text == "" or text.lower() == "nan":
        return ""
    text = INVISIBLE_CHAR_PATTERN.sub("", text)
    text = MULTISPACE_PATTERN.sub(" ", text)

    previous = None
    while text != previous:
        previous = text
        text = EDGE_NOISE_PATTERN.sub("", text).strip()

    return text


def is_noise_term(term):
    normalized = normalize_term(term)
    if normalized == "":
        return True, normalized, "标准化后为空"
    if normalized in EXPLICIT_NOISE_TERMS:
        return True, normalized, "显式噪声词"
    if PURE_NOISE_PATTERN.fullmatch(normalized):
        return True, normalized, "仅包含标点或噪声字符"
    return False, normalized, ""


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


def normalize_rule_row(row, allow_empty_correct=False):
    enabled = str(row.get("enabled", "Y")).strip().upper() != "N"
    term_match_mode = str(row.get("term_match_mode", "exact")).strip().lower()
    term_keyword = str(row.get("term_keyword", "")).strip()
    wrong_code = str(row.get("wrong_code", "")).strip()
    correct_code = str(row.get("correct_code", "")).strip()
    column_scope = str(row.get("column_scope", "ALL")).strip()

    if term_keyword == "":
        return None
    if wrong_code.lower() == "nan":
        wrong_code = ""
    if correct_code.lower() == "nan":
        correct_code = ""
    if (not allow_empty_correct) and correct_code == "":
        return None
    if term_match_mode not in {"exact", "contains"}:
        term_match_mode = "exact"
    normalized_term_keyword = normalize_term(term_keyword)
    if normalized_term_keyword == "":
        return None

    return {
        "enabled": enabled,
        "term_match_mode": term_match_mode,
        "term_keyword": term_keyword,
        "normalized_term_keyword": normalized_term_keyword,
        "wrong_code": wrong_code,
        "correct_code": correct_code,
        "column_scope": column_scope,
    }


def load_rules(rule_file, allow_empty_correct=False):
    rule_df = read_table(rule_file)
    rules = []
    for _, row in rule_df.iterrows():
        parsed = normalize_rule_row(row, allow_empty_correct=allow_empty_correct)
        if parsed is None or not parsed["enabled"]:
            continue
        rules.append(parsed)
    return rules


def term_match(term, mode, keyword):
    normalized_term = normalize_term(term)
    normalized_keyword = normalize_term(keyword)
    if mode == "exact":
        return normalized_term == normalized_keyword
    return normalized_keyword in normalized_term


def remove_noise_from_pair(term_value, code_value):
    terms = split_pipe(term_value)
    codes = split_pipe(code_value)
    if len(terms) == 0 and len(codes) == 0:
        return term_value, code_value, 0, "EMPTY_PAIR", []
    if len(terms) == 0:
        return term_value, code_value, 0, "LENGTH_MISMATCH_OR_EMPTY", []

    if len(terms) == len(codes):
        kept_terms = []
        kept_codes = []
        removed_items = []
        for term, code in zip(terms, codes):
            is_noise, normalized_term, reason = is_noise_term(term)
            if is_noise:
                removed_items.append(
                    {
                        "term": term,
                        "normalized_term": normalized_term,
                        "code": code,
                        "reason": reason,
                    }
                )
                continue
            kept_terms.append(term)
            kept_codes.append(code)

        if not removed_items:
            return term_value, code_value, 0, "NO_NOISE", []
        return "|".join(kept_terms), "|".join(kept_codes), len(removed_items), "NOISE_REMOVED", removed_items

    filtered_terms = []
    removed_items = []
    for term in terms:
        is_noise, normalized_term, reason = is_noise_term(term)
        if is_noise:
            removed_items.append(
                {
                    "term": term,
                    "normalized_term": normalized_term,
                    "code": "",
                    "reason": reason,
                }
            )
            continue
        filtered_terms.append(term)

    if len(filtered_terms) == len(codes) and removed_items:
        return "|".join(filtered_terms), code_value, len(removed_items), "NOISE_REMOVED", removed_items

    return term_value, code_value, 0, "LENGTH_MISMATCH_OR_EMPTY", []


def apply_rules_to_pair(term_value, code_value, rules_for_col):
    terms = split_pipe(term_value)
    codes = split_pipe(code_value)
    if len(terms) == 0 or len(codes) == 0 or len(terms) != len(codes):
        return code_value, 0, "LENGTH_MISMATCH_OR_EMPTY"

    changed = 0
    new_codes = []
    for term, code in zip(terms, codes):
        new_code = code
        for rule in rules_for_col:
            if not term_match(term, rule["term_match_mode"], rule["normalized_term_keyword"]):
                continue
            wrong_code = rule["wrong_code"]
            if wrong_code != "" and code != wrong_code:
                continue
            new_code = rule["correct_code"]
            break
        if new_code != code:
            changed += 1
        new_codes.append(new_code)

    if changed == 0:
        return code_value, 0, "NO_CHANGE"
    return "|".join(new_codes), changed, "UPDATED"


def filter_rules_by_column(rules, term_col):
    col_rules = []
    for rule in rules:
        scope = rule["column_scope"]
        if scope.upper() == "ALL" or scope == term_col:
            col_rules.append(rule)
    return col_rules


def main():
    parser = argparse.ArgumentParser(description="根据规则文件批量替换 ICD11 编码。")
    parser.add_argument(
        "-i",
        "--input",
        default="nipt_disgnosis_20251030_icd11_mapped-20260403.csv",
        help="输入映射结果文件",
    )
    parser.add_argument(
        "-o",
        "--output",
        default="",
        help="输出文件；未提供时默认在输入文件名后追加 -fixed",
    )
    parser.add_argument(
        "-r",
        "--rules",
        default="fix_rules_template.csv",
        help="规则文件 CSV/Excel",
    )
    parser.add_argument(
        "-p",
        "--report",
        default="",
        help="修复明细报告 CSV；未提供时默认基于输入文件名生成",
    )
    parser.add_argument(
        "--allow-empty-correct",
        action="store_true",
        help="允许将编码替换为空（默认关闭，防止误清空编码）",
    )
    args = parser.parse_args()
    if not args.output:
        args.output = build_default_output_path(args.input)
    if not args.report:
        args.report = build_default_report_path(args.input)

    print(f"读取映射文件: {args.input}")
    df = read_table(args.input)

    pairs = detect_column_pairs(df)
    if not pairs:
        raise ValueError("未检测到可用列对。")
    print(f"检测到列对: {pairs}")

    print(f"读取规则文件: {args.rules}")
    rules = load_rules(args.rules, allow_empty_correct=args.allow_empty_correct)
    if len(rules) == 0:
        print("未加载到可用规则（可能是 correct_code 还未填写），仅执行纯噪声剔除。")
    else:
        print(f"有效规则数: {len(rules)}")

    total_changed_items = 0
    total_changed_rows = 0
    report_rows = []

    for idx, row in df.iterrows():
        row_changed = False
        for term_col, code_col in pairs:
            original_term = row.get(term_col, "")
            old_code = row.get(code_col, "")
            cleaned_term, cleaned_code, noise_removed_count, noise_status, removed_items = remove_noise_from_pair(
                original_term,
                old_code,
            )
            current_term = cleaned_term
            current_code = cleaned_code

            if noise_removed_count > 0:
                df.at[idx, term_col] = cleaned_term
                df.at[idx, code_col] = cleaned_code
                total_changed_items += noise_removed_count
                row_changed = True
                report_rows.append(
                    {
                        "row_index": idx,
                        "column": term_col,
                        "diagnosis_value": original_term,
                        "new_diagnosis_value": cleaned_term,
                        "old_code": old_code,
                        "new_code": cleaned_code,
                        "changed_items": noise_removed_count,
                        "note": "NOISE_REMOVED",
                        "removed_terms": "|".join(item["term"] for item in removed_items),
                        "removed_codes": "|".join(item["code"] for item in removed_items),
                    }
                )
            elif noise_status == "LENGTH_MISMATCH_OR_EMPTY":
                report_rows.append(
                    {
                        "row_index": idx,
                        "column": term_col,
                        "diagnosis_value": original_term,
                        "new_diagnosis_value": original_term,
                        "old_code": old_code,
                        "new_code": old_code,
                        "changed_items": 0,
                        "note": noise_status,
                    }
                )
                continue

            rules_for_col = filter_rules_by_column(rules, term_col)
            if len(rules_for_col) == 0:
                continue

            new_code, changed_items, status = apply_rules_to_pair(
                current_term,
                current_code,
                rules_for_col,
            )
            if changed_items > 0:
                df.at[idx, code_col] = new_code
                total_changed_items += changed_items
                row_changed = True
                report_rows.append(
                    {
                        "row_index": idx,
                        "column": term_col,
                        "diagnosis_value": current_term,
                        "new_diagnosis_value": current_term,
                        "old_code": current_code,
                        "new_code": new_code,
                        "changed_items": changed_items,
                        "note": "RULE_UPDATED",
                    }
                )
            elif status == "LENGTH_MISMATCH_OR_EMPTY":
                report_rows.append(
                    {
                        "row_index": idx,
                        "column": term_col,
                        "diagnosis_value": current_term,
                        "new_diagnosis_value": current_term,
                        "old_code": current_code,
                        "new_code": current_code,
                        "changed_items": 0,
                        "note": status,
                    }
                )
        if row_changed:
            total_changed_rows += 1

    print(f"修复完成: 修改行数={total_changed_rows}, 修改子项数={total_changed_items}")
    write_table(df, args.output)
    print(f"已保存修复结果: {args.output}")

    report_df = pd.DataFrame(report_rows)
    report_df.to_csv(args.report, index=False, encoding="utf-8-sig")
    print(f"已保存修复报告: {args.report}")

    if len(report_df) > 0:
        print("修复样例:")
        print(report_df.head(5).to_string(index=False))


if __name__ == "__main__":
    main()
