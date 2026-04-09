import argparse
import os
import re

import pandas as pd


DIAGNOSIS_COLUMN_PATTERN = re.compile(r"diagnosis\d+$", re.IGNORECASE)
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


def split_pipe(value):
    if value is None:
        return []
    text = str(value).strip()
    if text == "" or text.lower() == "nan":
        return []
    return [x.strip() for x in text.split("|")]


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

    return {
        "enabled": enabled,
        "term_match_mode": term_match_mode,
        "term_keyword": term_keyword,
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
    if mode == "exact":
        return term == keyword
    return keyword in term


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
            if not term_match(term, rule["term_match_mode"], rule["term_keyword"]):
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
    parser.add_argument("--input", default="nipt_disgnosis_20251030_icd11_mapped-20260403.csv", help="输入映射结果文件")
    parser.add_argument("--output", default="nipt_disgnosis_20251030_icd11_mapped-fixed.csv", help="输出文件")
    parser.add_argument("--rules", default="fix_rules_template.csv", help="规则文件 CSV/Excel")
    parser.add_argument("--report", default="fix_apply_report.csv", help="修复明细报告 CSV")
    parser.add_argument(
        "--allow-empty-correct",
        action="store_true",
        help="允许将编码替换为空（默认关闭，防止误清空编码）",
    )
    args = parser.parse_args()

    print(f"读取映射文件: {args.input}")
    df = read_table(args.input)

    pairs = detect_column_pairs(df)
    if not pairs:
        raise ValueError("未检测到可用列对。")
    print(f"检测到列对: {pairs}")

    print(f"读取规则文件: {args.rules}")
    rules = load_rules(args.rules, allow_empty_correct=args.allow_empty_correct)
    if len(rules) == 0:
        print("未加载到可用规则（可能是 correct_code 还未填写），不执行替换。")
        write_table(df, args.output)
        pd.DataFrame(
            [{"note": "NO_VALID_RULES", "detail": "规则为空或correct_code未填写"}]
        ).to_csv(args.report, index=False, encoding="utf-8-sig")
        print(f"已原样输出: {args.output}")
        print(f"已输出报告: {args.report}")
        return
    print(f"有效规则数: {len(rules)}")

    total_changed_items = 0
    total_changed_rows = 0
    report_rows = []

    for idx, row in df.iterrows():
        row_changed = False
        for term_col, code_col in pairs:
            rules_for_col = filter_rules_by_column(rules, term_col)
            if len(rules_for_col) == 0:
                continue

            old_code = row.get(code_col, "")
            new_code, changed_items, status = apply_rules_to_pair(
                row.get(term_col, ""),
                old_code,
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
                        "diagnosis_value": row.get(term_col, ""),
                        "old_code": old_code,
                        "new_code": new_code,
                        "changed_items": changed_items,
                    }
                )
            elif status == "LENGTH_MISMATCH_OR_EMPTY":
                report_rows.append(
                    {
                        "row_index": idx,
                        "column": term_col,
                        "diagnosis_value": row.get(term_col, ""),
                        "old_code": old_code,
                        "new_code": old_code,
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
