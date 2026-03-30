import polars as pl
import os

# --- 配置区域 ---
INPUT_FILE = r'0127新增_未清洗样本提取_清洗地址后-icd11_mapped.xlsx'
OUTPUT_FILE = r'0127新增_未清洗样本提取_清洗地址后-icd11_mapped.xlsx'

COLUMN_PAIRS = [
    ("产科合并症", "产科合并症_ICD11_Code"),
    ("手术适应症", "手术适应症_ICD11_Code"),
    ("孕期风险项", "孕期风险项_ICD11_Code")
]

# 诊断名称：维持【包含匹配】
TARGET_TERM_KEYWORD = "足月成熟儿"

# 错误编码：支持单个字符串（如 "3A50.Z"）或列表（如 ["MB23.Q", "5C53.Y"]）
WRONG_CODE = ["DD93.0","KA22.2","MB23.Q", "QA47.0Z","QA47.2","None","None of the provided candidates are relevant to 'term infant' (足月成熟儿). The candidates describe unrelated conditions or incorrect gestational age statuses."]
CORRECT_CODE = ""

# -------------------------------------------------
# 核心修正逻辑 (Polars 向量化自定义函数)
# -------------------------------------------------
def fix_code_logic(diag_str, code_str):
    if diag_str is None or code_str is None:
        return code_str
    
    diags = [d.strip() for d in str(diag_str).split('|')]
    codes = [c.strip() for c in str(code_str).split('|')]

    if len(diags) != len(codes):
        return code_str

    # 转为列表处理
    wrong_codes = [WRONG_CODE] if isinstance(WRONG_CODE, str) else WRONG_CODE

    modified = False
    new_codes = []
    for d, c in zip(diags, codes):
        if (TARGET_TERM_KEYWORD in d) and (c in wrong_codes):
            new_codes.append(CORRECT_CODE)
            modified = True
        else:
            new_codes.append(c)
    
    return "|".join(new_codes) if modified else code_str

# -------------------------------------------------
# 主程序
# -------------------------------------------------
if __name__ == "__main__":
    print(f"正在读取文件: {INPUT_FILE} (使用 Polars 引擎)...")

    try:
        # 1. 加载数据 (强制所有列读取为 String 避免类型冲突)
        # 先读取表头获取列名
        header = pl.read_excel(INPUT_FILE, read_options={"n_rows": 1})
        df = pl.read_excel(INPUT_FILE, schema_overrides={col: pl.String for col in header.columns})
        
        total_fixed_count = 0

        for diag_col, code_col in COLUMN_PAIRS:
            if diag_col not in df.columns or code_col not in df.columns:
                print(f"跳过不存在的列对: {diag_col} / {code_col}")
                continue

            print(f"正在检查列对: {diag_col} <-> {code_col} ...")

            # 2. 性能优化的关键：预过滤
            # 只有当编码列中确实包含 WRONG_CODE 中的任意一个时才进行复杂计算
            wrong_codes = [WRONG_CODE] if isinstance(WRONG_CODE, str) else WRONG_CODE
            
            # 使用 | 拼接成正则，如果是列表的话
            if isinstance(WRONG_CODE, list):
                # 对特殊字符进行简单转义处理
                import re as standard_re
                regex_pattern = "|".join([standard_re.escape(c) for c in WRONG_CODE])
                mask = df[code_col].str.contains(regex_pattern).fill_null(False)
            else:
                mask = df[code_col].str.contains(WRONG_CODE, literal=True).fill_null(False)
            
            if mask.any():
                # 记录原始值用于统计
                original_codes = df.filter(mask)[code_col]

                # 对选中的行执行修正逻辑
                updated_values = df.filter(mask).select([
                    pl.struct([diag_col, code_col]).map_elements(
                        lambda x: fix_code_logic(x[diag_col], x[code_col]),
                        return_dtype=pl.String
                    ).alias(code_col)
                ])[code_col]

                # 统计有效的修正数量
                # 注意：Polars map_elements 之后我们要对比具体的字符串变化
                effective_changes = (updated_values != original_codes).sum()
                total_fixed_count += effective_changes

                # 更新原 DataFrame
                # 使用 pl.when().then().otherwise() 实现按条件更新
                df = df.with_columns(
                    pl.when(mask)
                    .then(
                        pl.struct([diag_col, code_col]).map_elements(
                            lambda x: fix_code_logic(x[diag_col], x[code_col]),
                            return_dtype=pl.String
                        )
                    )
                    .otherwise(pl.col(code_col))
                    .alias(code_col)
                )

                if effective_changes > 0:
                    print(f"  -> 有效修正了 {effective_changes} 行数据")

                    # 展示修改样例
                    diff_mask = updated_values != original_codes
                    examples_df = df.filter(mask).filter(diff_mask).head(5)
                    
                    # 为了方便展示，我们将 original_codes 也加入到这个临时 DF 中
                    # 注意：Polars 这里的 filter(mask和diff_mask) 后索引会重排
                    # 但我们可以直接按行展示关键信息
                    for i in range(len(examples_df)):
                        print(f"    样例:")
                        print(f"      诊断: {examples_df[diag_col][i]}")
                        print(f"      原码: {original_codes.filter(diff_mask)[i]}")
                        print(f"      新码: {examples_df[code_col][i]}")
                else:
                    print("  -> 预过滤匹配成功但未发现符合条件的子项")
            else:
                print("  -> 未发现需要修正的数据")

        # 3. 保存
        print(f"正在保存结果到: {OUTPUT_FILE}")
        output_dir = os.path.dirname(OUTPUT_FILE)
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)
        
        df.write_excel(OUTPUT_FILE)
        print(f"完成！共修正了 {total_fixed_count} 处真实异常。")

    except Exception as e:
        print(f"发生错误: {e}")