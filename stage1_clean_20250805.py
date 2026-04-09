import argparse
import re
from typing import List

import numpy as np
import pandas as pd

# Import processing functions from other scripts
from stage1_clean_obstetric_complications import process_obstetric_complications
from stage1_clean_pregnancy_risks import process_pregnancy_risks
from stage1_clean_surgical_indications import process_surgical_indications

LEGACY_COLUMNS_TO_PROCESS = {
    "手术适应症": process_surgical_indications,
    "孕期风险项": process_pregnancy_risks,
    "产科合并症": process_obstetric_complications,
}

MEANINGLESS_CELL_VALUES = [
    "/",
    "／",
    "其他",
    "其他 /",
    "无",
    "其它",
    "其它 /",
    "其他的",
    "无并发症",
    "nan",
    "None",
]

DIAGNOSIS_COLUMN_PATTERN = re.compile(r"diagnosis\d+$", re.IGNORECASE)

MEANINGLESS_DIAGNOSIS_TERMS = {
    "",
    "/",
    "／",
    "无",
    "其他",
    "其它",
    "其他 /",
    "其它 /",
    "其他的",
    "无并发症",
    "nan",
    "none",
}

NON_DISEASE_PATTERNS = [
    re.compile(r"^孕\d+(?:\+\d+)?周$"),
    re.compile(r"^孕\d+次$"),
    re.compile(r"^产\d+次$"),
    re.compile(r"^(?:单|双|多)胎活产$"),
    re.compile(r"^单一活产$"),
    re.compile(r"^头位顺产$"),
    re.compile(r"^单胎顺产$"),
    re.compile(r"^经(?:选择性|急症)?剖宫产术?的?分娩$"),
    re.compile(r"^经剖宫产术分娩$"),
    re.compile(r"^经剖宫产术的分娩$"),
    re.compile(r"^早产经剖宫产$"),
    re.compile(r"^产钳助产的单胎分娩$"),
    re.compile(r"^提前自然临产伴有足月产$"),
    re.compile(r"^提前自然临产伴有早产$"),
    re.compile(r"^早产伴分娩$"),
    re.compile(r"^高危妊娠监督$"),
    re.compile(r"^正常妊娠监督$"),
    re.compile(r"^具有.*妊娠监督$"),
    re.compile(r"^.*妊娠状态$"),
]

SUPERVISION_KEEP_PATTERNS = [
    re.compile(r"^高龄初孕妇的监督$"),
    re.compile(r"^高龄经产妇妊娠监督$"),
    re.compile(r"^高龄初孕.*监督$"),
    re.compile(r"^高龄经产.*监督$"),
]


def split_outside_parentheses(text: str, separators: str = ",，;；|、\n\r\t") -> List[str]:
    """
    仅在括号外部分割，避免将形如“早产儿(....,....)”错误拆开。
    """
    if not isinstance(text, str):
        return [text]

    parts: List[str] = []
    current: List[str] = []
    depth = 0
    opening = {"(", "（", "[", "【", "{"}
    closing = {")", "）", "]", "】", "}"}
    sep_set = set(separators)

    for ch in text:
        if ch in opening:
            depth += 1
            current.append(ch)
            continue

        if ch in closing:
            depth = max(depth - 1, 0)
            current.append(ch)
            continue

        if ch in sep_set and depth == 0:
            part = "".join(current).strip()
            if part:
                parts.append(part)
            current = []
            continue

        current.append(ch)

    if current:
        tail = "".join(current).strip()
        if tail:
            parts.append(tail)

    return parts


def is_non_disease_term(term: str) -> bool:
    normalized = term.strip()

    if normalized.lower() in MEANINGLESS_DIAGNOSIS_TERMS:
        return True

    for pattern in NON_DISEASE_PATTERNS:
        if pattern.fullmatch(normalized):
            return True

    # 仅保留“高龄初孕/高龄经产”监督项，其余监督类过滤
    if "监督" in normalized:
        if any(pattern.fullmatch(normalized) for pattern in SUPERVISION_KEEP_PATTERNS):
            return False
        return True

    return False


def process_diagnosis_text(text):
    """
    处理 diagnosis1~diagnosisN 结构：
    1. 安全分割（括号内不切分）
    2. 过滤孕周/孕产次/分娩方式/监督等无意义信息
    3. 去重并用 | 输出
    """
    if not isinstance(text, str):
        return text

    tokens = split_outside_parentheses(text)
    cleaned_terms: List[str] = []

    for token in tokens:
        term = token.strip()
        term = re.sub(r"^\d+[\.、]\s*", "", term)  # 删除序号前缀
        term = re.sub(r"^(?:其他|其它)\s*", "", term)  # 删除前置“其他/其它”
        term = term.strip(" ，,;；|")

        if not term:
            continue
        if is_non_disease_term(term):
            continue
        if term.isdigit():
            continue

        cleaned_terms.append(term)

    unique_terms = list(dict.fromkeys(cleaned_terms))
    if unique_terms:
        return "|".join(unique_terms)
    return np.nan


def process_legacy_columns(df: pd.DataFrame) -> None:
    for column, process_func in LEGACY_COLUMNS_TO_PROCESS.items():
        if column in df.columns:
            print(f"开始清洗列: {column}...")
            df[column] = df[column].replace(MEANINGLESS_CELL_VALUES, np.nan)
            df[column] = df[column].apply(lambda x: process_func(x) if pd.notna(x) else x)
            print(f"列 '{column}' 清洗完成。")
        else:
            print(f"提示: 未找到旧结构列 '{column}'，跳过。")


def sort_diagnosis_columns(columns: List[str]) -> List[str]:
    def key_func(col_name: str):
        match = re.search(r"(\d+)$", col_name)
        return int(match.group(1)) if match else 9999

    return sorted(columns, key=key_func)


def process_diagnosis_columns(df: pd.DataFrame) -> None:
    diagnosis_columns = sort_diagnosis_columns(
        [c for c in df.columns if DIAGNOSIS_COLUMN_PATTERN.fullmatch(c)]
    )

    if not diagnosis_columns:
        print("提示: 未检测到 diagnosis* 新结构列。")
        return

    print(f"检测到新结构诊断列: {diagnosis_columns}")
    for column in diagnosis_columns:
        print(f"开始清洗列: {column}...")
        df[column] = df[column].replace(MEANINGLESS_CELL_VALUES, np.nan)
        df[column] = df[column].apply(lambda x: process_diagnosis_text(x) if pd.notna(x) else x)
        print(f"列 '{column}' 清洗完成。")


def clean_phase_one(input_file, output_file):
    """
    第一阶段数据清洗：
    1. 兼容旧结构：手术适应症、孕期风险项、产科合并症
    2. 兼容新结构：diagnosis1~diagnosisN
    """
    print(f"正在从 {input_file} 读取数据 (强制所有类型为字符串)...")
    df = pd.read_csv(input_file, low_memory=False, dtype=str)
    print("数据读取完毕。")

    process_legacy_columns(df)
    process_diagnosis_columns(df)

    print(f"正在将清洗结果保存至 {output_file}...")
    df.to_csv(output_file, index=False, encoding="utf-8-sig")
    print("所有处理完成，文件已保存。")

    return df


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=(
            "对CSV文件进行第一阶段清洗，兼容旧结构"
            "(手术适应症/孕期风险项/产科合并症)与新结构(diagnosis1~N)。"
        )
    )
    parser.add_argument("input_file", help="输入CSV文件的路径。")
    parser.add_argument("output_file", help="输出CSV文件的路径。")
    args = parser.parse_args()

    clean_phase_one(args.input_file, args.output_file)
