import pandas as pd
import os

# --- 配置区域 ---
INPUT_FILE = r'龙岗编码结果\补救.xlsx'
OUTPUT_FILE = r'龙岗编码结果\补救.xlsx'

COLUMN_PAIRS = [
    ("产科合并症", "产科合并症_ICD11_Code"),
    ("手术适应症", "手术适应症_ICD11_Code"),
    ("孕期风险项", "孕期风险项_ICD11_Code")
]

# 诊断名称：维持【包含匹配】
# 只要单元格文本中包含这个词（例如 "妊娠期胆积淤积综合征"）就算命中
TARGET_TERM_KEYWORD = "脐带扭转"

# 错误编码：恢复【严格匹配】
# 只有完全等于 "JA65.0" 才会被替换（"JA65.01" 或 "JA65.0/X" 不会被修改）
WRONG_CODE = "LB03.Y"
CORRECT_CODE = "KC20.Z"

# -------------------------------------------------
# 核心修正逻辑函数
# -------------------------------------------------
def fix_code_alignment(row, diag_col, code_col):
    # 1. 获取原始值
    raw_diag = row[diag_col]
    raw_code = row[code_col]

    # 严谨性检查：空值直接返回
    if pd.isna(raw_diag) or pd.isna(raw_code):
        return raw_code

    # 2. 转字符串并去空
    diag_str = str(raw_diag).strip()
    code_str = str(raw_code).strip()

    if not diag_str or not code_str:
        return raw_code

    # 3. 拆分处理（按竖线拆分）
    diags = [d.strip() for d in diag_str.split('|')]
    codes = [c.strip() for c in code_str.split('|')]

    # 严谨性检查：数量不一致跳过，防止错位
    if len(diags) != len(codes):
        return raw_code

    is_modified = False
    new_codes = []

    for i in range(len(diags)):
        d_term = diags[i]
        c_code = codes[i]

        # 4. 判定逻辑（关键修改处）
        # 条件A (诊断): 使用 in (包含匹配)，提高容错率
        # 条件B (编码): 使用 == (严格匹配)，确保精准
        if (TARGET_TERM_KEYWORD in d_term) and (c_code == WRONG_CODE):
            new_codes.append(CORRECT_CODE)
            is_modified = True
        else:
            new_codes.append(c_code)

    # 5. 返回结果
    if is_modified:
        return "|".join(new_codes)
    else:
        # 无修改则返回原始对象
        return raw_code

# -------------------------------------------------
# 主程序
# -------------------------------------------------
if __name__ == "__main__":
    print(f"正在读取文件: {INPUT_FILE}")

    try:
        df = pd.read_excel(INPUT_FILE)
        total_fixed_count = 0

        for diag_col, code_col in COLUMN_PAIRS:
            if diag_col not in df.columns or code_col not in df.columns:
                print(f"跳过不存在的列对: {diag_col} / {code_col}")
                continue

            print(f"正在检查列对: {diag_col} <-> {code_col} ...")

            original_codes = df[code_col].copy()

            # 应用修复
            df[code_col] = df.apply(lambda row: fix_code_alignment(row, diag_col, code_col), axis=1)

            # 统计差异 (使用 fillna 确保对比的严谨性)
            col_diff = (original_codes.fillna("##NULL##") != df[code_col].fillna("##NULL##"))
            count = col_diff.sum()
            total_fixed_count += count

            if count > 0:
                print(f"  -> 有效修正了 {count} 行数据")
                # 打印真实修改的样例
                example_indices = df[col_diff].index[:5]
                for idx in example_indices:
                    print(f"    样例 (Row {idx}):")
                    print(f"      诊断: {df.loc[idx, diag_col]}")
                    print(f"      原码: {original_codes[idx]}")
                    print(f"      新码: {df.loc[idx, code_col]}")
            else:
                print("  -> 未发现需要修正的数据")

        print(f"正在保存结果到: {OUTPUT_FILE}")
        df.to_excel(OUTPUT_FILE, index=False)
        print(f"完成！共修正了 {total_fixed_count} 处真实异常。")

    except Exception as e:
        print(f"发生错误: {e}")