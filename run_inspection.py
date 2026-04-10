import argparse
import json
import os
import re
from collections import Counter, defaultdict

import pandas as pd


DIAGNOSIS_COLUMN_PATTERN = re.compile(r"diagnosis\d+$", re.IGNORECASE)
ICD_CODE_COLUMN_PATTERN = re.compile(r"diagnosis\d+_ICD11_Code$", re.IGNORECASE)
LEGACY_ORIGINAL_COLS = ["产科合并症", "手术适应症", "孕期风险项"]
INVISIBLE_CHAR_PATTERN = re.compile(r"[\u200b-\u200f\u202a-\u202e\u2060\ufeff]")
MULTISPACE_PATTERN = re.compile(r"\s+")
EDGE_NOISE_PATTERN = re.compile(r"^[\s\.,;:，。；：、\)\]】）]+|[\s\.,;:，。；：、\(\[【（]+$")
PURE_NOISE_PATTERN = re.compile(r"^[\W_]+$", re.UNICODE)
EXPLICIT_NOISE_TERMS = {"无"}


def read_data(file_path, sheet_name=0):
    _, extension = os.path.splitext(file_path.lower())
    if extension == ".csv":
        return pd.read_csv(file_path, dtype=str, low_memory=False)
    if extension in [".xlsx", ".xls"]:
        return pd.read_excel(file_path, dtype=str, sheet_name=sheet_name)
    raise ValueError(f"不支持的文件类型: {extension}")


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


def split_pipe(value):
    if value is None:
        return []
    text = str(value).strip()
    if text == "" or text.lower() == "nan":
        return []
    return [x.strip() for x in text.split("|") if x.strip()]


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


def flag_noise_terms(flags, terms, row_idx, row_key, row_admission, term_col, code_value=""):
    clean_terms = []
    for term in terms:
        is_noise, normalized_term, noise_reason = is_noise_term(term)
        if is_noise:
            flags.append(
                {
                    "row_index": row_idx,
                    "uuid": row_key,
                    "admission_date": row_admission,
                    "column": term_col,
                    "issue_type": "NOISE_TERM",
                    "detail": noise_reason,
                    "term_value": term,
                    "normalized_term": normalized_term,
                    "code_value": code_value,
                    "recommended_code": "",
                    "recommended_code_frequency": 0,
                }
            )
            continue
        clean_terms.append(term)
    return clean_terms


def get_recommended_code(term_key, term_to_codes):
    code_count_map = term_to_codes.get(term_key)
    if not code_count_map:
        return "", 0
    return code_count_map.most_common(1)[0]


def build_reverse_map_and_flags(df, column_pairs):
    reverse_map = defaultdict(lambda: defaultdict(int))
    pair_counter = Counter()
    term_to_codes = defaultdict(Counter)
    term_display_counter = defaultdict(Counter)
    flags = []

    for row_idx, row in df.iterrows():
        for term_col, code_col in column_pairs:
            terms = split_pipe(row.get(term_col))
            codes = split_pipe(row.get(code_col))

            if not terms and not codes:
                continue

            row_key = row.get("uuid", "")
            row_admission = row.get("admission_date", "")
            original_terms = terms

            if terms and not codes:
                terms = flag_noise_terms(flags, terms, row_idx, row_key, row_admission, term_col)
                if not terms:
                    continue
                flags.append(
                    {
                        "row_index": row_idx,
                        "uuid": row_key,
                        "admission_date": row_admission,
                        "column": term_col,
                        "issue_type": "MISSING_CODE",
                        "detail": "有诊断但编码为空",
                        "term_value": "|".join(terms),
                        "code_value": "",
                    }
                )
                continue

            if codes and not terms:
                flags.append(
                    {
                        "row_index": row_idx,
                        "uuid": row_key,
                        "admission_date": row_admission,
                        "column": term_col,
                        "issue_type": "MISSING_TERM",
                        "detail": "有编码但诊断为空",
                        "term_value": "",
                        "code_value": "|".join(codes),
                    }
                )
                continue

            if len(original_terms) != len(codes):
                terms = flag_noise_terms(flags, original_terms, row_idx, row_key, row_admission, term_col)
            if len(terms) != len(codes):
                flags.append(
                    {
                        "row_index": row_idx,
                        "uuid": row_key,
                        "admission_date": row_admission,
                        "column": term_col,
                        "issue_type": "TERM_CODE_LENGTH_MISMATCH",
                        "detail": f"terms={len(terms)}, codes={len(codes)}",
                        "term_value": "|".join(terms),
                        "code_value": "|".join(codes),
                    }
                )
                continue

            for term, code in zip(terms, codes):
                is_noise, normalized_term, noise_reason = is_noise_term(term)
                if is_noise:
                    flags.append(
                        {
                            "row_index": row_idx,
                            "uuid": row_key,
                            "admission_date": row_admission,
                            "column": term_col,
                            "issue_type": "NOISE_TERM",
                            "detail": noise_reason,
                            "term_value": term,
                            "normalized_term": normalized_term,
                            "code_value": code,
                            "recommended_code": "",
                            "recommended_code_frequency": 0,
                        }
                    )
                    continue
                if "ERROR" not in code and code.strip() != "" and code.lower() not in {"none", "nan"}:
                    term_to_codes[normalized_term][code] += 1
                    term_display_counter[normalized_term][term] += 1
                    pair_counter[(normalized_term, code)] += 1

                recommended_code, recommended_count = get_recommended_code(normalized_term, term_to_codes)

                if "ERROR" in code:
                    flags.append(
                        {
                            "row_index": row_idx,
                            "uuid": row_key,
                            "admission_date": row_admission,
                            "column": term_col,
                            "issue_type": "ERROR_CODE",
                            "detail": "编码结果包含 ERROR",
                            "term_value": term,
                            "normalized_term": normalized_term,
                            "code_value": code,
                            "recommended_code": recommended_code,
                            "recommended_code_frequency": recommended_count,
                        }
                    )
                    continue

                if code.strip() == "" or code.lower() in {"none", "nan"}:
                    flags.append(
                        {
                            "row_index": row_idx,
                            "uuid": row_key,
                            "admission_date": row_admission,
                            "column": term_col,
                            "issue_type": "EMPTY_OR_NONE_CODE",
                            "detail": "编码为空或None",
                            "term_value": term,
                            "normalized_term": normalized_term,
                            "code_value": code,
                            "recommended_code": recommended_code,
                            "recommended_code_frequency": recommended_count,
                        }
                    )
                    continue

                reverse_map[code][normalized_term] += 1

    for term, code_count_map in term_to_codes.items():
        if len(code_count_map) > 1:
            major_code, major_count = code_count_map.most_common(1)[0]
            display_term = term_display_counter[term].most_common(1)[0][0]
            for code, count in code_count_map.items():
                if code != major_code:
                    flags.append(
                        {
                            "row_index": "",
                            "uuid": "",
                            "admission_date": "",
                            "column": "ALL",
                            "issue_type": "TERM_MULTI_CODE",
                            "detail": (
                                f"term={term} 出现多编码；major={major_code}({major_count}) "
                                f"current={code}({count})"
                            ),
                            "term_value": display_term,
                            "normalized_term": term,
                            "code_value": code,
                            "recommended_code": major_code,
                            "recommended_code_frequency": major_count,
                        }
                    )

    return reverse_map, pair_counter, term_to_codes, flags


def make_term_code_stats(pair_counter):
    rows = []
    for (term, code), freq in pair_counter.items():
        rows.append({"normalized_term": term, "code": code, "frequency": freq})
    stats_df = pd.DataFrame(rows)
    if not stats_df.empty:
        stats_df = stats_df.sort_values(["frequency", "normalized_term"], ascending=[False, True])
    return stats_df


def make_fix_rules_template(flags):
    template_rows = []
    seen = set()
    for item in flags:
        issue_type = item["issue_type"]
        term = item.get("normalized_term") or item["term_value"]
        code = item["code_value"]
        recommended_code = item.get("recommended_code", "")

        if issue_type not in {"ERROR_CODE", "EMPTY_OR_NONE_CODE", "TERM_MULTI_CODE"}:
            continue
        if term == "":
            continue

        key = (term, code)
        if key in seen:
            continue
        seen.add(key)

        template_rows.append(
            {
                "enabled": "Y",
                "term_match_mode": "exact",
                "term_keyword": term,
                "wrong_code": code,
                "correct_code": recommended_code,
                "column_scope": "ALL",
                "note": issue_type,
            }
        )

    if not template_rows:
        template_rows.append(
            {
                "enabled": "Y",
                "term_match_mode": "exact",
                "term_keyword": "示例诊断词",
                "wrong_code": "示例错误编码",
                "correct_code": "示例正确编码",
                "column_scope": "ALL",
                "note": "请按需填写",
            }
        )
    return pd.DataFrame(template_rows)


def save_outputs(
    reverse_map,
    stats_df,
    flags_df,
    template_df,
    output_json,
    output_flags_csv,
    output_stats_csv,
    output_template_csv,
):
    final_report = {
        code: dict(sorted(terms.items(), key=lambda x: x[1], reverse=True))
        for code, terms in sorted(reverse_map.items())
    }
    with open(output_json, "w", encoding="utf-8") as f:
        json.dump(final_report, f, ensure_ascii=False, indent=2)

    flags_df.to_csv(output_flags_csv, index=False, encoding="utf-8-sig")
    stats_df.to_csv(output_stats_csv, index=False, encoding="utf-8-sig")
    template_df.to_csv(output_template_csv, index=False, encoding="utf-8-sig")


def main():
    parser = argparse.ArgumentParser(description="检查 ICD11 映射结果并生成补救规则模板。")
    parser.add_argument(
        "-i",
        "--input",
        default="nipt_disgnosis_20251030_icd11_mapped-20260403.csv",
        help="映射结果文件",
    )
    parser.add_argument("-s", "--sheet", default=0, help="Excel 工作表名称或索引(从0开始)")
    parser.add_argument("-j", "--output-json", default="inspection_report.json", help="反查报告 JSON")
    parser.add_argument("-f", "--output-flags", default="inspection_flags.csv", help="异常明细 CSV")
    parser.add_argument("-r", "--output-stats", default="inspection_term_code_stats.csv", help="term-code 频次 CSV")
    parser.add_argument("-t", "--output-template", default="fix_rules_template.csv", help="补救规则模板 CSV")
    args = parser.parse_args()

    print(f"读取文件: {args.input}")
    sheet_name = args.sheet
    if isinstance(sheet_name, str) and sheet_name.isdigit():
        sheet_name = int(sheet_name)
    df = read_data(args.input, sheet_name=sheet_name)
    column_pairs = detect_column_pairs(df)
    if not column_pairs:
        raise ValueError("未找到可用列对，请确认存在 diagnosis*_ICD11_Code 或旧结构列。")
    print(f"检测到列对: {column_pairs}")

    reverse_map, pair_counter, _, flags = build_reverse_map_and_flags(df, column_pairs)
    stats_df = make_term_code_stats(pair_counter)
    flags_df = pd.DataFrame(flags)
    template_df = make_fix_rules_template(flags)

    save_outputs(
        reverse_map,
        stats_df,
        flags_df,
        template_df,
        args.output_json,
        args.output_flags,
        args.output_stats,
        args.output_template,
    )

    print(f"已生成: {args.output_json}")
    print(f"已生成: {args.output_flags} (共 {len(flags_df)} 条异常)")
    print(f"已生成: {args.output_stats} (共 {len(stats_df)} 条 term-code 记录)")
    print(f"已生成: {args.output_template} (共 {len(template_df)} 条建议规则)")


if __name__ == "__main__":
    main()
