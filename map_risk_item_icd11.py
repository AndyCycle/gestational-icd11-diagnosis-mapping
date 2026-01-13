import polars as pl
import pandas as pd
import csv
import os

# --- 1. 辅助函数：稳健的数据读取 ---

def read_data_robust(file_path):
    _ , extension = os.path.splitext(file_path)
    try:
        if extension == '.csv':
            return pl.read_csv(file_path, encoding='utf-8-sig', infer_schema_length=0)
        elif extension in ['.xlsx', '.xls']:
            # 使用 Pandas 读取以保证 Excel 结构的完整性
            pdf = pd.read_excel(file_path, engine='openpyxl', dtype=str)
            df = pl.from_pandas(pdf)
            return df.cast(pl.String)
        else:
            print(f"错误: 不支持的文件类型: '{extension}'")
            return None
    except Exception as e:
        print(f"读取文件 '{file_path}' 时发生严重错误: {e}")
        return None

def load_mapping_file(mapping_path):
    if not os.path.exists(mapping_path):
        raise FileNotFoundError(f"映射文件未找到: {mapping_path}")
    code_map = {}
    try:
        with open(mapping_path, 'r', encoding='utf-8-sig') as f:
            reader = csv.reader(f)
            for row in reader:
                if len(row) < 2: continue
                token, code = row[0].strip(), row[1].strip()
                if token and code:
                    code_map[token] = code
    except Exception as e:
        print(f"读取映射文件时出错: {e}")
        return None
    return code_map

# --- 2. 核心逻辑：带交叉列检查的编码函数 ---

def process_risk_with_exclusion_logic(row, code_map):
    """
    row: 包含多列数据的字典
    逻辑：针对特定风险项，若手术适应症或产科合并症包含“甲亢、甲减、垂体”，则不进行编码。
    """
    risk_val = row.get("孕期风险项")
    surgery_val = row.get("手术适应症")
    complication_val = row.get("产科合并症")

    # 预处理空值
    def clean_str(v):
        s = str(v or "").strip()
        return "" if s.lower() == "nan" else s

    risk_text = clean_str(risk_val)
    # 合并背景列文本用于检索
    context_text = clean_str(surgery_val) + clean_str(complication_val)

    if not risk_text:
        return ""

    # 定义业务规则：目标项与排除词
    special_targets = {
        "无需药物治疗的糖尿病、甲状腺疾病、垂体泌乳素瘤等",
        "需药物治疗的糖尿病、甲状腺疾病、垂体泌乳素瘤"
    }
    exclusion_keywords = ["甲亢", "甲减", "垂体", "甲"]

    tokens = [t.strip() for t in risk_text.split('|') if t.strip()]
    mapped_results = []

    for token in tokens:
        # 判定是否触发排除逻辑
        if token in special_targets:
            # 检查上下文中是否存在排除关键词
            has_exclusion = any(kw in context_text for kw in exclusion_keywords)
            if has_exclusion:
                # 触发排除：不进行编码，直接跳过该 token
                continue

        # 正常映射逻辑
        code = code_map.get(token)
        if code:
            mapped_results.append(code)
        else:
            # 如果映射表中不存在，标记错误
            mapped_results.append(f"ERROR: No mapping for '{token}'")

    return "|".join(mapped_results)

# --- 3. 主程序 ---

if __name__ == "__main__":
    # 配置路径
    DATA_FILE_PATH = r'分娩记录_编码后.xlsx'
    MAPPING_FILE_PATH = r'孕期风险项coding.csv'
    OUTPUT_FILE_PATH = r'分娩记录_编码后_riskmapped.xlsx'

    # 步骤 1: 加载规则
    mapping_dict = load_mapping_file(MAPPING_FILE_PATH)
    if not mapping_dict:
        print("映射表加载失败，请检查文件。")
        exit(1)

    # 步骤 2: 读取数据
    df = read_data_robust(DATA_FILE_PATH)
    if df is None: exit(1)

    # 步骤 3: 预检查列是否存在
    required_cols = ["孕期风险项", "手术适应症", "产科合并症"]
    for col in required_cols:
        if col not in df.columns:
            print(f"提示: 列 '{col}' 不存在，已自动补充空列。")
            df = df.with_columns(pl.lit("").alias(col))

    # 步骤 4: 执行优化后的映射逻辑
    print("正在执行交叉列校验编码逻辑...")
    try:
        df_processed = df.with_columns(
            pl.struct(required_cols)
            .map_elements(
                lambda x: process_risk_with_exclusion_logic(x, mapping_dict),
                return_dtype=pl.String
            ).alias("孕期风险项_ICD11_Code")
        )

        # 行数校验
        if df.height == df_processed.height:
            print(f"✅ 处理完成，行数一致: {df.height}")
        else:
            print("❌ 警告：处理前后行数不一致！")

        # 步骤 5: 保存结果
        os.makedirs(os.path.dirname(OUTPUT_FILE_PATH), exist_ok=True)
        df_processed.write_excel(OUTPUT_FILE_PATH)
        print(f"文件保存成功: {OUTPUT_FILE_PATH}")

    except Exception as e:
        print(f"处理过程中发生错误: {e}")