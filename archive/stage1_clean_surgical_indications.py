import pandas as pd
import re
import numpy as np
import argparse

def process_surgical_indications(text):
    """
    处理手术适应症列的文本数据。
    该函数在原始逻辑基础上进行了优化，以修复已知的分割错误。

    修复点：
    1. (Case 4) 扩展了孕产信息的正则表达式，以匹配 '孕X+Y周(...)' 格式。
    2. (Case 1) 增加了预处理步骤，去除括号（全角/半角）前多余的空格。
    3. (Case 2) 将 '?' 和 '？' 替换为空格（而不是逗号），避免错误分割。
    4. (Case 2) 增加了清理步骤，移除分割后可能产生的空括号 '()' 或 '( )'。
    5. (Case 3) 扩展了最终的清理逻辑，以移除 '数字+周' (如 '3周') 格式的词条。
    """
    if not isinstance(text, str):
        return text

    text = text.strip()
    # 仅删除开头的"其他/其它"
    text = re.sub(r'^其他\s*|^其它\s*', '', text)
    protected_text = text

    # 保护脐带相关文本，直接作为整体保留
    cord_patterns = [
        r'(脐带绕颈\d+周)',
        r'(脐带扭转\d+周)'
    ]

    cord_matches = []
    for pattern in cord_patterns:
        matches = re.findall(pattern, protected_text)
        for match in matches:
            if match:
                replacement = f"CORD_PLACEHOLDER_{len(cord_matches)}"
                protected_text = protected_text.replace(match, replacement)
                cord_matches.append(match)

    # 保护孕产信息格式 - 扩展正则表达式以匹配更多变体
    pregnancy_patterns = [
        ## 修复 (Case 4): 增加对 '孕X+Y周(...)' 格式的匹配
        r'(孕\d+\+\d+周\(.*?\))',
        r'(孕\d+产\d+孕\d+\+\d+周.*?(?:左|右)(?:枕|骶)(?:前|后|横)(?:早产临产)?)',
        r'(孕\d+产\d+孕\d+\+\d+周.*?(?:单|双|多)活胎)',
        r'(孕\d+产\d+孕\d+\+\d+周.*?)(?=[,，;；]|$)'
    ]

    pregnancy_matches = []
    for pattern in pregnancy_patterns:
        # 使用 re.finditer 来处理重叠或连续的匹配
        # 并且从左到右替换，避免替换已替换内容
        matches_iter = list(re.finditer(pattern, protected_text))
        for match in matches_iter:
            match_str = match.group(0)
            if match_str and "PLACEHOLDER" not in match_str: # 确保不重复处理
                replacement = f"PREGNANCY_PLACEHOLDER_{len(pregnancy_matches)}"
                protected_text = protected_text.replace(match_str, replacement)
                pregnancy_matches.append(match_str)


    # 处理问题3和4：处理"查因："的情况
    cause_pattern = r'(.*?)查因[：:](.*?)(?=$|[,，;；])'
    cause_matches = re.findall(cause_pattern, protected_text)
    for i, (before, cause) in enumerate(cause_matches):
        original = f"{before}查因：{cause}"
        replacement = f"{cause}"
        protected_text = protected_text.replace(original, replacement)

    # 不移除括号内容

    ## 修复 (Case 1): 移除括号（全角/半角）前的空格，防止被错误分割
    # 例如："症状 （重度）" -> "症状（重度）"
    protected_text = re.sub(r'\s+([\(（])', r'\1', protected_text)

    # 处理问题1：将+号作为分隔符，但不处理已被保护的文本中的+号
    # (Case 4 的修复确保了 '孕34+4周' 不会执行到这一步)
    if not any(placeholder in protected_text for placeholder in ['PREGNANCY_PLACEHOLDER_']):
        protected_text = protected_text.replace('+', ',')

    ## 修复 (Case 2): 将问号替换为空格，而不是逗号，以避免破坏 '(...)' 结构
    protected_text = re.sub(r'[\?？]', ' ', protected_text)

    # 清理其他符号
    protected_text = re.sub(r'\d+\.|\.|\*|\\|//|/|、', ' ', protected_text)
    protected_text = re.sub(r'。', '', protected_text)

    # 移除非病症描述
    non_disease_patterns = [
        r'孕妇要求手术', r'患者要求手术', r'孕妇要求',
        r'患者要求', r'要求手术', r'孕妇选择'
    ]
    for pattern in non_disease_patterns:
        protected_text = re.sub(pattern, '', protected_text)

    # 处理文本中间的"其他/其它"
    protected_text = re.sub(r'\s+其他\s+|\s+其它\s+', ' ', protected_text)

    # 严格按分隔符分割
    terms = re.split(r'[;；,，]', protected_text)
    filtered_terms = []
    meaningless_terms = ['其他', '其它', '无']

    for term in terms:
        term = term.strip()

        ## 修复 (Case 2): 移除因 '?' 转换留下的空括号
        # 例如 "低置胎盘( )" -> "低置胎盘"
        term = re.sub(r'[\(（]\s*[\)）]', '', term)
        term = term.strip() # 再次 strip

        if term and term not in meaningless_terms:
            # 只有当不包含占位符时才进行空格分割
            if ' ' in term and not any(placeholder in term for placeholder in ['CORD_PLACEHOLDER_', 'PREGNANCY_PLACEHOLDER_']):
                sub_terms = term.split()
                filtered_terms.extend([t for t in sub_terms if t and not t.isdigit() and t not in meaningless_terms])
            else:
                filtered_terms.append(term)

    # 恢复占位符
    # (恢复逻辑保持不变)
    for i, match in enumerate(cord_matches):
        placeholder = f"CORD_PLACEHOLDER_{i}"
        for j, term in enumerate(filtered_terms):
            if placeholder in term:
                filtered_terms[j] = term.replace(placeholder, match)

    for i, match in enumerate(pregnancy_matches):
        placeholder = f"PREGNANCY_PLACEHOLDER_{i}"
        for j, term in enumerate(filtered_terms):
            if placeholder in term:
                filtered_terms[j] = term.replace(placeholder, match)

    # 最终的分割处理
    # (分割逻辑保持不变)
    final_terms = []
    all_protected_matches = cord_matches + pregnancy_matches
    all_protected_matches.sort(key=len, reverse=True)
    escaped_matches = [re.escape(m) for m in all_protected_matches]

    regex_parts = escaped_matches + [r'\S+']
    pattern = '|'.join(regex_parts)

    for term in filtered_terms:
        if term:
            final_terms.extend(re.findall(pattern, term))

    filtered_terms = final_terms

    ## 修复 (Case 3): 增加对 '数字+周' (如 '3周') 的过滤
    filtered_terms = [
        term for term in filtered_terms
        if not term.isdigit()
        and term not in ['周']
        and not re.fullmatch(r'\d+周', term) # 确保 '3周' 被过滤
    ]

    # 去重
    unique_terms = []
    seen = set()
    for term in filtered_terms:
        if term not in seen:
            unique_terms.append(term)
            seen.add(term)

    filtered_terms = unique_terms

    if filtered_terms:
        return '|'.join(filtered_terms)
    else:
        return np.nan

def main(input_file, output_file):
    """主函数，读取、处理并保存数据。"""
    df = pd.read_csv(input_file, low_memory=False)
    column_to_clean = '手术适应症'
    meaningless_values = ['/', '其他', '其他 /', '无', '其它', '其它 /', "／",'珍贵儿', "社会因素", "足月成熟儿"]

    if column_to_clean in df.columns:
        print(f"开始清洗列: {column_to_clean}...")
        df[column_to_clean] = df[column_to_clean].replace(meaningless_values, np.nan)

        # 调用新版本的函数
        df[column_to_clean] = df[column_to_clean].apply(
            lambda x: process_surgical_indications(x) if pd.notna(x) else x
        )
        print("清洗完成。")
    else:
        print(f"错误: 在输入文件中未找到列 '{column_to_clean}'。")

    df.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"处理结果已保存至: {output_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="清洗CSV文件中的 '手术适应症' 列。")
    parser.add_argument("input_file", help="输入CSV文件的路径。")
    parser.add_argument("output_file", help="输出CSV文件的路径。")
    args = parser.parse_args()

    main(args.input_file, args.output_file)