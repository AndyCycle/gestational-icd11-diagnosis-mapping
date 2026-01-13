import pandas as pd
import re
import numpy as np
import argparse

# Import processing functions from other scripts
from stage1_clean_surgical_indications import process_surgical_indications
from stage1_clean_pregnancy_risks import process_pregnancy_risks
from stage1_clean_obstetric_complications import process_obstetric_complications

def clean_phase_one(input_file, output_file):
    """
    第一阶段数据清洗：读取CSV文件，按顺序对指定列调用外部清洗函数，并保存结果。
    """
    # 读取数据，设置low_memory=False解决DtypeWarning
    print(f"正在从 {input_file} 读取数据 (强制所有类型为字符串)...")
    df = pd.read_csv(input_file, low_memory=False, dtype=str)
    print("数据读取完毕。")

    # 需要清洗的列与对应的处理函数
    columns_to_process = {
        '手术适应症': process_surgical_indications,
        '孕期风险项': process_pregnancy_risks,
        '产科合并症': process_obstetric_complications
    }

    # 无意义值列表
    meaningless_values = ['/', '其他', '其他 /', '无', '其它', '其它 /']

    # 处理每一列
    for column, process_func in columns_to_process.items():
        if column in df.columns:
            print(f"开始清洗列: {column}...")
            # 替换无意义值为NaN
            df[column] = df[column].replace(meaningless_values, np.nan)

            # 应用相应的处理函数
            df[column] = df[column].apply(lambda x: process_func(x) if pd.notna(x) else x)
            print(f"列 '{column}' 清洗完成。")
        else:
            print(f"警告: 在输入文件中未找到列 '{column}'，跳过处理。")

    # 保存清洗后的数据
    print(f"正在将清洗结果保存至 {output_file}...")
    df.to_csv(output_file, index=False, encoding='utf-8-sig')
    print("所有处理完成，文件已保存。")

    return df

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="对CSV文件进行第一阶段清洗，处理'手术适应症', '孕期风险项', '产科合并症'列。")
    parser.add_argument("input_file", help="输入CSV文件的路径。")
    parser.add_argument("output_file", help="输出CSV文件的路径。")
    args = parser.parse_args()

    clean_phase_one(args.input_file, args.output_file)