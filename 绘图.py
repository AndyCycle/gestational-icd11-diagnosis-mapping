import polars as pl
import matplotlib.pyplot as plt
import math
import os
import argparse
from matplotlib import font_manager

# 寻找系统中可用的中文字体
def get_chinese_font():
    # 常见的备选字体名
    options = ['SimHei', 'Microsoft YaHei', 'SimSun', 'STHeiti', 'Arial Unicode MS']
    for font in options:
        if font in [f.name for f in font_manager.fontManager.ttflist]:
            return font
    return None

found_font = get_chinese_font()
if found_font:
    plt.rcParams['font.sans-serif'] = [found_font]
    print(f"成功加载中文字体: {found_font}")
else:
    print("警告: 未找到中文字体，请手动指定 .ttf 文件路径")

plt.rcParams['axes.unicode_minus'] = False

def plot_icd_slice(data_slice: pl.DataFrame, filename: str, rank_start: int):
    icd_codes = data_slice.get_column('ICD11_Code_Or_Group').to_list()
    patient_counts = data_slice.get_column('Patient_Count').to_list()

    # 转换患病率数据
    prevalence_raw = data_slice.get_column('Prevalence').to_list()
    prevalence = [float(p.replace('%', '')) for p in prevalence_raw]

    x_ticks = range(len(icd_codes))
    fig, ax1 = plt.subplots(figsize=(16, 9))

    # --- 1. 绘制柱状图 (Patient Count) ---
    color_bar = 'skyblue'
    ax1.set_xlabel('ICD-11 Code & Description', fontsize=12)
    ax1.set_ylabel('Patient Count', color='steelblue', fontsize=12)
    bars = ax1.bar(x_ticks, patient_counts, color=color_bar, label='Patient Count', alpha=0.7)

    # --- 2. 绘制折线图 (Prevalence) ---
    ax2 = ax1.twinx()
    color_line = 'tab:red'
    ax2.set_ylabel('Prevalence (%)', color=color_line, fontsize=12)
    line = ax2.plot(x_ticks, prevalence, color=color_line, marker='o', linewidth=2, label='Prevalence (%)')

    # --- 3. 核心修改：添加数值附注 (Data Annotations) ---
    for i in x_ticks:
        # A. 在柱状图上方标注人数 (蓝色字体)
        ax1.text(
            i,
            patient_counts[i] * 1.02, # 向上 2%
            format(int(patient_counts[i]), ','),
            ha='center', va='bottom',
            color='steelblue', fontsize=8, fontweight='bold'
        )

        # B. 在折线图点上方标注百分比 (红色字体)
        ax2.text(
            i,
            prevalence[i] - (max(prevalence) * 0.02), # 稍微向上偏移
            f"{prevalence[i]:.2f}%",
            ha='center', va='top',
            color='tab:red', fontsize=8
        )

    # --- 4. 坐标轴与布局优化 ---
    ax1.set_xticks(x_ticks)
    ax1.set_xticklabels(icd_codes, rotation=45, ha='right', fontsize=9)

    # 设置 Y 轴范围，留出顶部空间给标注
    ax1.set_ylim(0, max(patient_counts) * 1.15)
    ax2.set_ylim(0, max(prevalence) * 1.25)

    fig.suptitle('ICD-11 Analysis: Patient Count & Prevalence with Annotations', fontsize=16)

    # 合并图例
    lns = [bars] + line
    labs = [l.get_label() for l in lns]
    ax1.legend(lns, labs, loc='upper right')

    plt.subplots_adjust(bottom=0.18, top=0.9, left=0.08, right=0.92)
    plt.savefig(filename, dpi=150)
    plt.close(fig)

def main():
    parser = argparse.ArgumentParser(description="Generate English-labeled plots from ICD statistical reports.")
    parser.add_argument("-i", "--input", required=True, help="Path to the statistics_report.csv")
    args = parser.parse_args()
    input_filename = args.input

    # --- Constants ---
    SLICE_SIZE = 30
    MIN_PATIENT_COUNT = 10
    OUTPUT_DIR = "icd_visual_reports_en"

    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    try:
        print(f"Loading data: {input_filename}...")
        df = pl.read_csv(input_filename)

        # Apply filtering and sorting
        df_filtered = df.filter(pl.col('Patient_Count') >= MIN_PATIENT_COUNT)
        df_sorted = df_filtered.sort('Patient_Count', descending=True)

        total_rows = df_sorted.height
        if total_rows == 0:
            print(f"No entries found with Patient_Count >= {MIN_PATIENT_COUNT}.")
            return

        num_slices = math.ceil(total_rows / SLICE_SIZE)
        print(f"Processing {total_rows} codes across {num_slices} output file(s).")

        for i in range(num_slices):
            start_index = i * SLICE_SIZE
            slice_length = min(SLICE_SIZE, total_rows - start_index)
            df_slice = df_sorted.slice(start_index, slice_length)

            rank_end = start_index + slice_length
            plot_filename = os.path.join(OUTPUT_DIR, f'icd_plot_ranks_{start_index + 1}_to_{rank_end}.png')

            plot_icd_slice(df_slice, plot_filename, rank_start=start_index)

        print(f"\nExecution complete. Plots saved in: '{OUTPUT_DIR}'")

    except Exception as e:
        print(f"Error occurred during execution: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()