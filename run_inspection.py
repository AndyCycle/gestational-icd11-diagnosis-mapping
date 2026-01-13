import polars as pl
import json
from collections import defaultdict
import os

# --- 配置 ---
MAPPED_FILE_PATH = r'icd11_mapped.xlsx'
# 必须包含所有参与编码的原始列
ORIGINAL_COL_NAMES = ["产科合并症", "手术适应症", "孕期风险项"]
OUTPUT_JSON = r'inspection_report.json'

def read_data_with_polars(file_path):
    _ , extension = os.path.splitext(file_path)
    try:
        if extension == '.csv':
            return pl.read_csv(file_path, encoding='utf-8-sig', infer_schema_length=0)
        elif extension in ['.xlsx', '.xls']:
            # 采用先读一行获取 Schema 的安全策略
            header_df = pl.read_excel(file_path, read_options={"n_rows": 1})
            overrides = {col: pl.String for col in header_df.columns}
            return pl.read_excel(file_path, schema_overrides=overrides)
    except Exception as e:
        print(f"读取失败: {e}")
        return None

def create_reverse_map():
    reverse_map = defaultdict(lambda: defaultdict(int))
    df = read_data_with_polars(MAPPED_FILE_PATH)
    if df is None: return

    # 预先构建映射列关系
    code_col_map = {orig: f"{orig}_ICD11_Code" for orig in ORIGINAL_COL_NAMES}

    # 过滤掉不存在的列
    valid_map = {o: c for o, c in code_col_map.items() if o in df.columns and c in df.columns}

    for row in df.iter_rows(named=True):
        for orig_col, code_col in valid_map.items():
            orig_val, code_val = row.get(orig_col), row.get(code_col)
            if not orig_val or not code_val: continue

            # 解析可能存在的多值（用 | 分割）
            u_terms = [t.strip() for t in str(orig_val).split('|') if t.strip()]
            u_codes = [c.strip() for c in str(code_val).split('|') if c.strip()]

            # 注意：由于“条件映射”可能导致 Term 和 Code 数量不一致（部分 Term 被排除）
            # 我们这里采用“包含匹配”或“对齐匹配”。为了统计准确，建议对齐
            if len(u_terms) == len(u_codes):
                for term, code in zip(u_terms, u_codes):
                    if "ERROR" not in code:
                        reverse_map[code][term] += 1

    # 排序并保存
    final_report = {code: dict(sorted(terms.items(), key=lambda x: x[1], reverse=True))
                    for code, terms in sorted(reverse_map.items())}

    os.makedirs(os.path.dirname(OUTPUT_JSON), exist_ok=True)
    with open(OUTPUT_JSON, 'w', encoding='utf-8') as f:
        json.dump(final_report, f, ensure_ascii=False, indent=2)
    print(f"检验报告已生成: {OUTPUT_JSON}")

if __name__ == "__main__":
    create_reverse_map()