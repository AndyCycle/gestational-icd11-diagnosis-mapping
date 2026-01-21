import pandas as pd
import os

# --- 配置区域 ---
INPUT_FILE = r'补救胎盘早剥.xlsx'
OUTPUT_FILE = r'补救胎盘早剥.xlsx'

COLUMN_PAIRS = [
    ("产科合并症", "产科合并症_ICD11_Code"),
    ("手术适应症", "手术适应症_ICD11_Code")
]

TARGET_TERM_KEYWORD = "胎盘早剥"
WRONG_CODE = "JA41.Z"
CORRECT_CODE = "JA8C.Z"

# -------------------------------------------------
# 核心修正逻辑函数 (已修复空值处理 bug)
# -------------------------------------------------
def fix_code_alignment(row, diag_col, code_col):
    # 1. 获取原始值
    raw_diag = row[diag_col]
    raw_code = row[code_col]

    # 【修复点】：如果诊断或编码原本就是空的(NaN)，直接返回原始值，不要动它
    # 这样就不会触发 (NaN != "") 的误判
    if pd.isna(raw_diag) or pd.isna(raw_code):
        return raw_code

    # 2. 只有非空时，才转成字符串处理
    diag_str = str(raw_diag).strip()
    code_str = str(raw_code).strip()

    # 如果转成字符串后是空的，也直接返回原始值
    if not diag_str or not code_str:
        return raw_code

    # 3. 拆分处理
    diags = [d.strip() for d in diag_str.split('|')]
    codes = [c.strip() for c in code_str.split('|')]

    # 安全检查：数量不一致跳过
    if len(diags) != len(codes):
        return raw_code

    is_modified = False
    new_codes = []

    for i in range(len(diags)):
        d_term = diags[i]
        c_code = codes[i]

        # 4. 判定逻辑
        if TARGET_TERM_KEYWORD in d_term and c_code == WRONG_CODE:
            new_codes.append(CORRECT_CODE)
            is_modified = True
        else:
            new_codes.append(c_code)

    # 5. 返回结果
    if is_modified:
        return "|".join(new_codes)
    else:
        # 如果没有发生实际内容的变动，一定要返回原始对象 raw_code
        # 这样能保证 NaN 还是 NaN，字符串还是字符串，不会触发 diff
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
                continue

            print(f"正在检查列对: {diag_col} <-> {code_col} ...")

            original_codes = df[code_col].copy()

            # 应用修复
            df[code_col] = df.apply(lambda row: fix_code_alignment(row, diag_col, code_col), axis=1)

            # 统计差异 (此时 NaN == NaN 为 True，不会被统计进去了)
            # 注意：pandas 的 equals 或 compare 处理 NaN 可能比较特殊
            # 这里使用 fillna 统一处理后再比对，确保万无一失
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