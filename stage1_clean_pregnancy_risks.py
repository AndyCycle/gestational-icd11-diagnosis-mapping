import pandas as pd
import re
import numpy as np
import argparse

def process_pregnancy_risks(text):
    """
    处理孕期风险项列的文本数据。
    该函数遵循用户提供的优化逻辑，包括：
    1. 使用"(颜色)"标记作为主要分隔符来分割字符串。
    2. 对每个分割后的条目进行清理，包括去除系统疾病前缀（如"血液系统疾病："）。
    3. 过滤掉无意义的条目（如"其他"）。
    4. 去重并将结果用'|'合并，保留原始顺序。
    """
    if not isinstance(text, str):
        return text

    # 使用正则表达式按(颜色)标记分割字符串
    terms = re.split(r'\([^)]*色\)', text)

    prefixes_to_remove = [
        r'血液系统疾病：', r'内分泌系统疾病：', r'循环系统疾病：',
        r'呼吸系统疾病：', r'消化系统疾病：', r'泌尿系统疾病：',
        r'神经系统疾病：', r'其他系统疾病：'
    ]

    meaningless_values = ['其他', '其它', '/']

    cleaned_terms = []
    for term in terms:
        processed_term = term.strip()

        for prefix in prefixes_to_remove:
            processed_term = processed_term.replace(prefix, '', 1)

        processed_term = processed_term.strip()

        if processed_term and processed_term not in meaningless_values:
            cleaned_terms.append(processed_term)

    # 去除重复项，同时保留原始顺序
    unique_terms = list(dict.fromkeys(cleaned_terms))

    if unique_terms:
        return '|'.join(unique_terms)
    else:
        return np.nan

def main(input_file, output_file):
    """主函数，读取、处理并保存数据。"""
    df = pd.read_csv(input_file, low_memory=False)
    column_to_clean = '孕期风险项'
    meaningless_values = ['/', '其他', '其他 /', '无', '其它', '其它 /']

    if column_to_clean in df.columns:
        print(f"开始清洗列: {column_to_clean}...")
        df[column_to_clean] = df[column_to_clean].replace(meaningless_values, np.nan)
        df[column_to_clean] = df[column_to_clean].apply(lambda x: process_pregnancy_risks(x) if pd.notna(x) else x)
        print("清洗完成。")
    else:
        print(f"错误: 在输入文件中未找到列 '{column_to_clean}'。")

    df.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"处理结果已保存至: {output_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="清洗CSV文件中的 '孕期风险项' 列。")
    parser.add_argument("input_file", help="输入CSV文件的路径。")
    parser.add_argument("output_file", help="输出CSV文件的路径。")
    args = parser.parse_args()

    main(args.input_file, args.output_file)