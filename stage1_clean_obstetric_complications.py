import pandas as pd
import re
import numpy as np
import argparse

def process_obstetric_complications(text):
    """
    处理产科合并症列的文本数据。
    该函数遵循用户提供的优化逻辑，包括：
    1. 删除"其他/其它"前缀和序号。
    2. 保护特定大小描述（如"0.5x0.5cm"）不被错误分割。
    3. 清理标点符号。
    4. 分词、恢复占位符并进行最终清理。
    5. 去重并将结果用'|'合并。
    """
    if not isinstance(text, str):
        return text

    text = re.sub(r'^其他\s*|^其它\s*', '', text)
    text = re.sub(r'\d+\.\s*', '', text)
    protected_text = text

    # 保护包含 "x" 的尺寸描述
    size_pattern = r'\d+(\.\d+)?x\d+(\.\d+)?cm'
    placeholders = {}
    matches = re.finditer(size_pattern, protected_text)
    for i, match in enumerate(matches):
        full_match = match.group(0)
        placeholder = f"SIZE_PLACEHOLDER_{i}"
        if full_match not in protected_text: continue
        protected_text = protected_text.replace(full_match, placeholder)
        placeholders[placeholder] = full_match

    protected_text = re.sub(r'。|\?|？', '', protected_text)

    # 处理数字+、或数字+.的序号分割 - 修改为全局替换
    protected_text = re.sub(r'(\d+)[、.]', '', protected_text)

    # 将顿号也作为分隔符处理
    terms = []
    for segment in re.split(r'[;；,，]', protected_text):
        # 进一步按顿号分割
        sub_segments = re.split(r'、', segment)
        terms.extend(sub_segments)

    filtered_terms = []
    meaningless_values = ['其他', '其它', '/', '其他 /', '其它 /', '']

    for term in terms:
        term = term.strip()
        if term and term not in meaningless_values:
            if not term.isdigit():
                for placeholder, original in placeholders.items():
                    if placeholder in term:
                        term = term.replace(placeholder, original)
                filtered_terms.append(term)

    final_terms = []
    for term in filtered_terms:
        # 进一步处理可能由空格导致的多余分割
        sub_terms = term.split()
        for st in sub_terms:
            # --- 新增功能：删除"妊娠合并"前缀 ---
            st = re.sub(r'^妊娠合并', '', st)
            # ---------------------------------

            if (len(st) > 1 or st.isalpha()) and st not in meaningless_values:
                final_terms.append(st)

    final_terms = [term for term in final_terms if len(term) > 1 or not term.isdigit()]
    final_terms = list(set(final_terms))

    if final_terms:
        return '|'.join(final_terms)
    else:
        return np.nan

def main(input_file, output_file):
    """主函数，读取、处理并保存数据。"""
    df = pd.read_csv(input_file, low_memory=False)
    column_to_clean = '产科合并症'
    meaningless_values = ['/', '其他', '其他 /', '无', '其它', '其它 /']

    if column_to_clean in df.columns:
        print(f"开始清洗列: {column_to_clean}...")
        df[column_to_clean] = df[column_to_clean].replace(meaningless_values, np.nan)
        df[column_to_clean] = df[column_to_clean].apply(lambda x: process_obstetric_complications(x) if pd.notna(x) else x)
        print("清洗完成。")
    else:
        print(f"错误: 在输入文件中未找到列 '{column_to_clean}'。")

    df.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"处理结果已保存至: {output_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="清洗CSV文件中的 '产科合并症' 列。")
    parser.add_argument("input_file", help="输入CSV文件的路径。")
    parser.add_argument("output_file", help="输出CSV文件的路径。")
    args = parser.parse_args()

    main(args.input_file, args.output_file)