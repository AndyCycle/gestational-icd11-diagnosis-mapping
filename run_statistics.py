import polars as pl
import csv
from collections import defaultdict
import os

# --- 配置 ---

# 1. (输入) 映射后的文件路径 (.csv 或 .xlsx)
MAPPED_FILE_PATH = 'ICD11mapped-20260113.xlsx'
# 也可以是: 'mapped_part_1.xlsx'

# 2. (输入) 原始列名列表
ORIGINAL_COL_NAMES = ["产科合并症", "手术适应症","孕期风险项"]
# 例如: ["产科合并症", "手术适应症","孕期风险项"]

# 3. (输出) 统计报告的文件名
OUTPUT_CSV = 'statistics_report.csv'

# --- 脚本 ---

def read_data_with_polars(file_path):
    """
    使用 Polars 根据文件扩展名读取 CSV 或 Excel 文件,
    并强制将所有列读取为字符串 (String) 类型。
    """
    _ , extension = os.path.splitext(file_path)

    try:
        if extension == '.csv':
            # 关键修复 (CSV):
            # infer_schema_length=0 告诉 Polars 不要推断类型,
            # 默认将所有列读取为 pl.String, 解决了 'AJ1768982' 的问题。
            print(f"检测到 .csv, 正在使用 'infer_schema_length=0' (强制字符串) 读取...")
            df = pl.read_csv(file_path, encoding='utf-8-sig', infer_schema_length=0)

        elif extension in ['.xlsx', '.xls']:
            # 关键修复 (Excel):
            # Excel 读取器没有 'infer_schema_length=0'。
            # 我们必须先读取表头, 然后使用 schema_overrides 强制所有列为 String。

            print(f"检测到 .xlsx, 正在执行两步读取 (强制字符串)...")

            # 1. 先只读表头来获取列名 (n_rows=0 仅读取 schema)
            #    注意: read_options={"n_rows": 0} 在某些 polars 版本中可能不稳定
            #    如果失败，我们尝试 n_rows=1
            try:
                header = pl.read_excel(
                    file_path,
                    engine='openpyxl',
                    read_options={"n_rows": 0}
                ).columns
            except Exception:
                # print("n_rows=0 读取失败，尝试 n_rows=1 获取表头...")
                header = pl.read_excel(
                    file_path,
                    engine='openpyxl',
                    read_options={"n_rows": 1}
                ).columns


            if not header:
                raise Exception("无法从 Excel 文件读取表头。")

            # 2. 创建一个覆盖字典, 将所有列指定为 String
            overrides = {col: pl.String for col in header if col is not None}

            # 3. 使用覆盖字典重新读取整个文件
            df = pl.read_excel(
                file_path,
                engine='openpyxl',
                schema_overrides=overrides
            )

        else:
            print(f"错误: 不支持的文件类型: '{extension}'。只支持 .csv, .xlsx, .xls。")
            return None

        # 之前的 .cast(pl.String) 已不再需要, 因为读取时已完成。
        return df

    except Exception as e:
        print(f"使用 Polars 读取文件 '{file_path}' 时出错: {e}")
        return None

# --- [新增] 自定义大类规则 ---
# 键为大类名称，值为包含的编码前缀或特定代码列表
GROUP_RULES = {
    "胎膜早破 (PROM)": ["JA89.1", "JA89.Z"],
    "妊娠期高血压疾病": ["JA20", "JA21", "JA22", "JA23", "JA24", "JA25", "JA2Z"],
    "胎先露异常": ["JA82.1", "JA82.2", "JA82.3", "JA82.4", "JA82.5", "JA82.6", "JA82.Y", "JA82.Z"],
    "妊娠期糖尿病": ["JA63.2","JA63.Y"],
    "宫内生长受限": ["KA20.1Y", "KA20.10", "KA20.11", "KA20.12", "KA20.1Z"],
    "产后出血": ["JA43.1", "JA43.Y", "JA43.1/JB02.2"],
    "滞产": ["JB03.1", "JB03.0", "JB03.2", "JB03.Z"],
}

# --- [新增/修改] 统计控制开关 ---

# 1. 是否开启大类合并统计 (例如: 统计 "妊娠期高血压疾病" 这个整体)
ENABLE_GROUP_STATS = False

# 2. 是否保留原始明细编码统计 (例如: 统计 "JA24.z")
KEEP_INDIVIDUAL_STATS = True

# 3. [进阶] 是否仅对“未归类”的编码进行明细统计
# 如果设为 True，那么属于高血压大类的 JA24.z 将不再单独出现，只有不属于任何大类的编码才会出现。
ONLY_KEEP_UNGROUPED_INDIVIDUALS = False

# --- 脚本 ---

def get_group_name(code):
    """根据规则判断编码所属大类"""
    for group_label, members in GROUP_RULES.items():
        for m in members:
            # 匹配逻辑：如果 code 以规则中的字符串开头（如 JA20.1 匹配 JA20）
            if code.startswith(m):
                return group_label
    return None

def calculate_statistics():
    """
    核心统计函数：支持大类合并、明细保留及互斥模式的灵活切换。
    """
    # 存储结果的字典
    code_frequency = defaultdict(int)
    code_patients = defaultdict(set)

    # 1. 加载数据
    print(f"正在加载数据文件: {MAPPED_FILE_PATH}")
    df = read_data_with_polars(MAPPED_FILE_PATH)
    if df is None: return

    total_patients = df.height
    header = df.columns

    # 动态确定编码列和背景列
    code_cols = [f"{col}_ICD11_Code" for col in ORIGINAL_COL_NAMES if f"{col}_ICD11_Code" in header]
    has_parity_col = "产次" in header

    print(f"统计配置确认: \n - 开启大类统计: {ENABLE_GROUP_STATS} \n - 保留明细统计: {KEEP_INDIVIDUAL_STATS} \n - 互斥模式(仅保留未归类明细): {ONLY_KEEP_UNGROUPED_INDIVIDUALS}")

    # 2. 逐行迭代处理
    for i, row in enumerate(df.iter_rows(named=True)):
        # --- A. 基础数据提取 ---
        raw_codes = set()
        for col in code_cols:
            val = row.get(col)
            if val:
                # 拆分并清洗编码
                codes = [c.strip() for c in str(val).split('|') if c.strip() and "ERROR" not in c]
                raw_codes.update(codes)

        # --- B. 临床业务逻辑预处理 (如瘢痕子宫) ---
        processed_detail_codes = set()
        parity_val = str(row.get("产次") or "").strip() if has_parity_col else ""

        for c in raw_codes:
            if c == "JA84.2":
                # 根据产次重命名标签
                label = "JA84.2 & QA42.0 (非产源性瘢痕/初次妊娠)" if parity_val == "0" else "JA84.2 (剖宫产后瘢痕)"
                processed_detail_codes.add(label)
            else:
                processed_detail_codes.add(c)

        # --- C. 大类识别逻辑 ---
        matched_groups = set()      # 存储本行命中的大类标签
        captured_detail_codes = set() # 记录本行中哪些明细码被归入了任何大类

        for detail_code in processed_detail_codes:
            group_label = get_group_name(detail_code)
            if group_label:
                matched_groups.add(group_label)
                captured_detail_codes.add(detail_code)

        # --- D. 根据开关决定本行计入统计的最终项 (Final Items) ---
        final_items_to_count = set()

        # 逻辑 1: 处理大类项
        if ENABLE_GROUP_STATS:
            final_items_to_count.update(matched_groups)

        # 逻辑 2: 处理明细项
        if KEEP_INDIVIDUAL_STATS:
            if ONLY_KEEP_UNGROUPED_INDIVIDUALS:
                # 互斥模式：明细集合 减去 已被大类捕获的集合
                ungrouped_codes = processed_detail_codes - captured_detail_codes
                final_items_to_count.update(ungrouped_codes)
            else:
                # 全量模式：保留所有明细
                final_items_to_count.update(processed_detail_codes)

        # --- E. 执行计数更新 ---
        for item in final_items_to_count:
            code_frequency[item] += 1
            code_patients[item].add(i)

    # 3. 统计结果汇总导出
    print("正在生成汇总报告...")
    stats_results = []
    for item, freq in code_frequency.items():
        p_count = len(code_patients[item])
        prevalence = (p_count / total_patients) * 100
        stats_results.append({
            "ICD11_Code_Or_Group": item,
            "Total_Occurrences": freq,
            "Patient_Count": p_count,
            "Prevalence": f"{prevalence:.4f}%"
        })

    # 按患者人数降序排列，保证高频项在前
    stats_df = pl.DataFrame(stats_results).sort("Patient_Count", descending=True)

    # 自动在文件名中体现统计模式，防止结果覆盖
    mode_str = f"G{int(ENABLE_GROUP_STATS)}_I{int(KEEP_INDIVIDUAL_STATS)}_M{int(ONLY_KEEP_UNGROUPED_INDIVIDUALS)}"
    final_output_path = OUTPUT_CSV.replace(".csv", f"_{mode_str}.csv")

    os.makedirs(os.path.dirname(final_output_path), exist_ok=True)
    stats_df.write_csv(final_output_path)
    print(f"统计报告已保存至: {final_output_path}")

if __name__ == "__main__":
    calculate_statistics()