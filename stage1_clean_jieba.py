import argparse
import os
import re
from typing import Callable, List

import numpy as np
import pandas as pd

try:
    import jieba
except Exception as exc:  # pragma: no cover
    raise RuntimeError("jieba 未安装，请先执行: python -m pip install jieba") from exc

from stage1_clean_pregnancy_risks import process_pregnancy_risks


DIAGNOSIS_COLUMN_PATTERN = re.compile(r"diagnosis\d+$", re.IGNORECASE)
JIEBA_LEGACY_COLUMNS = ["手术适应症", "产科合并症"]
RISK_COLUMN = "孕期风险项"

MEANINGLESS_CELL_VALUES = {
    "/",
    "／",
    "其他",
    "其他 /",
    "无",
    "其它",
    "其它 /",
    "其他的",
    "无并发症",
    "珍贵儿",
    "孕妇及家属要求",
    "要求手术",
    "nan",
    "none",
    "None",
}

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
    "珍贵儿",
    "孕妇及家属要求",
    "要求手术",
    "nan",
    "none",
    "妊娠",
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

CONNECTOR_TOKENS = {"并", "并发", "伴", "伴有", "合并", "及", "和", "与"}
REMOVABLE_PHRASES = ["孕妇及家属要求", "要求手术", "珍贵儿"]

MEDICAL_USER_WORDS = [
    "妊娠期糖尿病",
    "妊娠期高血压",
    "子痫前期",
    "重度子痫前期",
    "胎膜早破",
    "胎盘早剥",
    "胎儿窘迫",
    "胎儿生长受限",
    "急性绒毛膜羊膜炎",
    "前置胎盘",
    "甲状腺功能减退",
    "甲状腺功能亢进",
    "妊娠合并",
    "凶险性前置胎盘",
]

for word in MEDICAL_USER_WORDS:
    jieba.add_word(word)


def split_outside_parentheses(text: str, separators: str = ",，;；|、?？ \n\r\t") -> List[str]:
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

    if "监督" in normalized:
        if any(pattern.fullmatch(normalized) for pattern in SUPERVISION_KEEP_PATTERNS):
            return False
        return True

    return False


def remove_non_disease_phrases(term: str) -> str:
    cleaned = term
    for phrase in REMOVABLE_PHRASES:
        cleaned = cleaned.replace(phrase, "")
    return cleaned.strip()


def split_with_jieba_connectors(term: str) -> List[str]:
    tokens = [tok.strip() for tok in jieba.lcut(term, cut_all=False) if tok.strip()]
    if not tokens:
        return [term]

    chunks: List[str] = []
    current: List[str] = []
    for tok in tokens:
        if tok in CONNECTOR_TOKENS:
            if current:
                chunks.append("".join(current).strip())
                current = []
            continue
        current.append(tok)

    if current:
        chunks.append("".join(current).strip())

    if len(chunks) <= 1:
        return [term.strip()]

    cleaned_chunks = [
        chunk
        for chunk in chunks
        if chunk and chunk not in MEANINGLESS_DIAGNOSIS_TERMS and not chunk.isdigit()
    ]
    return cleaned_chunks or [term.strip()]


def process_diagnosis_text_with_jieba(text):
    if not isinstance(text, str):
        return text

    base_tokens = split_outside_parentheses(text)
    cleaned_terms: List[str] = []

    for token in base_tokens:
        term = token.strip()
        term = re.sub(r"^\d+[\.、]\s*", "", term)
        term = re.sub(r"^(?:其他|其它)\s*", "", term)
        term = re.sub(r"[?？]+", "", term)
        term = re.sub(r"[（(]\s*[）)]", "", term)
        term = remove_non_disease_phrases(term)
        term = term.strip(" ，,;；|")

        if not term or is_non_disease_term(term):
            continue

        for piece in split_with_jieba_connectors(term):
            piece = piece.strip(" ，,;；|")
            piece = re.sub(r"[?？]+", "", piece)
            piece = re.sub(r"[（(]\s*[）)]", "", piece).strip()
            piece = remove_non_disease_phrases(piece)
            if not piece:
                continue
            if is_non_disease_term(piece):
                continue
            if piece.isdigit():
                continue
            cleaned_terms.append(piece)

    unique_terms = list(dict.fromkeys(cleaned_terms))
    if unique_terms:
        return "|".join(unique_terms)
    return np.nan


def sort_diagnosis_columns(columns: List[str]) -> List[str]:
    def key_func(col_name: str):
        match = re.search(r"(\d+)$", col_name)
        return int(match.group(1)) if match else 9999

    return sorted(columns, key=key_func)


def clean_column(df: pd.DataFrame, column: str, processor: Callable) -> None:
    print(f"开始清洗列: {column}...")
    df[column] = df[column].astype(object).replace(list(MEANINGLESS_CELL_VALUES), np.nan)
    df[column] = df[column].apply(lambda x: processor(x) if pd.notna(x) else x)
    print(f"列 '{column}' 清洗完成。")


def process_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    for column in JIEBA_LEGACY_COLUMNS:
        if column in df.columns:
            clean_column(df, column, process_diagnosis_text_with_jieba)
        else:
            print(f"提示: 未找到旧结构列 '{column}'，跳过。")

    if RISK_COLUMN in df.columns:
        clean_column(df, RISK_COLUMN, process_pregnancy_risks)
    else:
        print(f"提示: 未找到旧结构列 '{RISK_COLUMN}'，跳过。")

    diagnosis_columns = sort_diagnosis_columns(
        [c for c in df.columns if DIAGNOSIS_COLUMN_PATTERN.fullmatch(str(c))]
    )
    if diagnosis_columns:
        print(f"检测到新结构诊断列: {diagnosis_columns}")
        for column in diagnosis_columns:
            clean_column(df, column, process_diagnosis_text_with_jieba)
    else:
        print("提示: 未检测到 diagnosis* 新结构列。")

    return df


def read_table(input_path: str) -> pd.DataFrame:
    ext = os.path.splitext(input_path.lower())[1]
    if ext == ".csv":
        return pd.read_csv(input_path, low_memory=False, dtype=str)
    if ext in {".xlsx", ".xls"}:
        return pd.read_excel(input_path, dtype=str)
    raise ValueError(f"不支持的输入文件类型: {ext}")


def write_table(df: pd.DataFrame, output_path: str) -> None:
    ext = os.path.splitext(output_path.lower())[1]
    if ext == ".csv":
        df.to_csv(output_path, index=False, encoding="utf-8-sig")
        return
    if ext in {".xlsx", ".xls"}:
        df.to_excel(output_path, index=False)
        return
    raise ValueError(f"不支持的输出文件类型: {ext}")


def clean_phase_one(input_file: str, output_file: str) -> pd.DataFrame:
    print(f"正在从 {input_file} 读取数据 (强制所有类型为字符串)...")
    df = read_table(input_file)
    print("数据读取完毕。")

    out_df = process_dataframe(df)

    print(f"正在将清洗结果保存至 {output_file}...")
    write_table(out_df, output_file)
    print("所有处理完成，文件已保存。")
    return out_df


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Step1 诊断分词清洗：手术适应症/产科合并症使用 jieba，"
            "孕期风险项保留原风险项清洗逻辑，并兼容 diagnosis1~N。"
        )
    )
    parser.add_argument("input_file", help="输入文件路径，支持 csv/xlsx/xls。")
    parser.add_argument("output_file", help="输出文件路径，支持 csv/xlsx/xls。")
    args = parser.parse_args()

    clean_phase_one(args.input_file, args.output_file)


if __name__ == "__main__":
    main()
