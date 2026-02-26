import pandas as pd
import json
import os
import re
import math
import time
from openai import OpenAI
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- CONFIGURATION (配置部分) ---
VOLCENGINE_API_BASE_URL = "https://ark.cn-beijing.volces.com/api/v3"
VOLCENGINE_API_KEY_ENV_NAME = "ARK_API_KEY"
LLM_MODEL = "doubao-seed-1-6-flash-250828"

# 文件路径
INPUT_FILE = r'E:\文件\研究生\项目\宝安妇幼数据搜索\清洗任务\clean_test\初映射后LLM优化专家编码\龙岗分娩记录_最终合并后带uuid.xlsx'
OUTPUT_FILE = r'E:\文件\研究生\项目\宝安妇幼数据搜索\清洗任务\clean_test\初映射后LLM优化专家编码\龙岗清洗地址\龙岗分娩记录_地址清洗后.xlsx'
CACHE_DIR = r'E:\文件\研究生\项目\宝安妇幼数据搜索\清洗任务\clean_test\初映射后LLM优化专家编码\龙岗清洗地址\address_cache_temp'

TARGET_COLUMN = "地址信息"
NEW_COLUMN = "地址信息_清洗后"

# 并发设置 (根据 API 配额调整)
MAX_WORKERS = 40

# API 初始化
api_key = os.environ.get(VOLCENGINE_API_KEY_ENV_NAME)
if not api_key:
    raise ValueError(f"未找到 API Key，请设置环境变量 {VOLCENGINE_API_KEY_ENV_NAME}")

llm_client = OpenAI(base_url=VOLCENGINE_API_BASE_URL, api_key=api_key)

# -------------------------------------------------
# 1. 核心逻辑：带前缀缓存的 LLM 调用
# -------------------------------------------------

def clean_address_with_llm(raw_address):
    if not raw_address or pd.isna(raw_address) or str(raw_address).strip() == "":
        return None

    raw_address_str = str(raw_address).strip()

    # 固定前缀：包含规则和大量案例
    # 这部分内容将被火山引擎服务端缓存
    system_prompt = (
        "你是一位专业的数据清洗专家，专门负责处理中文地址数据。\n"
        "你的任务是：提取出规范的**路/街道/社区/小区/大厦级别的地址**，并剔除具体的门牌号、房号以及无关的业务备注信息。\n\n"
        "**清洗规则:**\n"
        "1. 去除地址开头的日期（如'2018-12-15'）及业务动作说明（如'迁入'）。\n"
        "2. 去除括号内的非地址备注信息。\n"
        "3. 剔除详细门牌号（如 '441-1', '5B2A', '301室'）及楼栋号。\n"
        "4. 保持层级完整（省市区街道社区）。\n\n"
        "**参考案例 (Few-Shot):**\n"
        "- 输入: \"2018-12-15迁入深圳市宝安区航城街道黄田社区\"\n"
        "  输出: {\"cleaned_address\": \"深圳市宝安区航城街道黄田社区\"}\n"
        "- 输入: \"深圳市宝安区新安街道大浪社区33区上川路441-1\"\n"
        "  输出: {\"cleaned_address\": \"深圳市宝安区新安街道大浪社区33区上川路\"}\n"
        "- 输入: \"（2019.11.29转来福中福）广东省深圳市宝安区新安街道办事处幸福海岸花园5B2A\"\n"
        "  输出: {\"cleaned_address\": \"广东省深圳市宝安区新安街道办事处幸福海岸花园\"}\n\n"
        "Respond ONLY with a JSON object: {\"cleaned_address\": \"string or null\"}"
    )

    try:
        response = llm_client.chat.completions.create(
            model=LLM_MODEL,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": f"Input Address: \"{raw_address_str}\""}
            ],
            temperature=0.1,
            # --- 优化点：启用前缀缓存 ---
            extra_body={
                "context_options": {
                    "mode": "common_prefix",
                    "ttl": 86400  # 缓存有效期设为24小时，适合大规模任务
                }
            }
        )
        # 提取结果
        content = response.choices[0].message.content

        # 简单的正则提取 JSON
        match = re.search(r'\{.*\}', content, re.DOTALL)
        if match:
            result = json.loads(match.group(0))
            return result.get("cleaned_address")
        return raw_address_str
    except Exception as e:
        # print(f"Error: {e}") # 高并发下建议减少 print 以免卡顿
        return raw_address_str

# -------------------------------------------------
# 2. 线程工作函数 (支持分片断点续传)
# -------------------------------------------------

def process_chunk(chunk_id, df_subset, cache_path, progress_bar=None):
    # 加载本分片已有的缓存
    local_cache = {}
    if os.path.exists(cache_path):
        with open(cache_path, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    data = json.loads(line)
                    local_cache[data['index']] = data['cleaned']
                except: continue

    # 逐行处理并实时存档
    with open(cache_path, 'a', encoding='utf-8', buffering=1) as f_out:
        for idx, row in df_subset.iterrows():
            if idx in local_cache:
                if progress_bar: progress_bar.update(1)
                continue

            cleaned_val = clean_address_with_llm(row[TARGET_COLUMN])

            # 记录格式: {"index": 原始行号, "cleaned": 结果}
            record = json.dumps({"index": idx, "cleaned": cleaned_val}, ensure_ascii=False)
            f_out.write(record + "\n")

            if progress_bar: progress_bar.update(1)

    return f"Chunk {chunk_id} done"

# -------------------------------------------------
# 3. 主程序：任务切分与结果合并
# -------------------------------------------------

if __name__ == '__main__':
    # 准备环境
    if not os.path.exists(CACHE_DIR): os.makedirs(CACHE_DIR)

    print(f"正在读取数据: {INPUT_FILE}")
    df = pd.read_excel(INPUT_FILE)
    total_rows = len(df)

    # 任务分片
    chunk_size = math.ceil(total_rows / MAX_WORKERS)
    futures = []

    print(f"并发数: {MAX_WORKERS} | 模式: 前缀缓存 + 独立分片存档")
    pbar = tqdm(total=total_rows, desc="地址清洗进度")

    start_time = time.time()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for i in range(MAX_WORKERS):
            start_idx = i * chunk_size
            end_idx = min((i + 1) * chunk_size, total_rows)
            if start_idx >= total_rows: break

            df_subset = df.iloc[start_idx:end_idx]
            thread_cache_file = os.path.join(CACHE_DIR, f"addr_cache_part_{i}.jsonl")

            futures.append(executor.submit(process_chunk, i, df_subset, thread_cache_file, pbar))

        for future in as_completed(futures):
            future.result()

    pbar.close()
    print(f"清洗完成，耗时: {time.time() - start_time:.2f}s")

    # 合并数据
    print("正在合并所有缓存分片...")
    final_results_map = {}
    for i in range(MAX_WORKERS):
        cache_file = os.path.join(CACHE_DIR, f"addr_cache_part_{i}.jsonl")
        if os.path.exists(cache_file):
            with open(cache_file, 'r', encoding='utf-8') as f:
                for line in f:
                    try:
                        data = json.loads(line)
                        final_results_map[int(data['index'])] = data['cleaned']
                    except: continue

    # 按原始索引顺序重组结果
    df[NEW_COLUMN] = [final_results_map.get(idx, None) for idx in df.index]

    # 保存
    try:
        df.to_excel(OUTPUT_FILE, index=False)
        print(f"🎉 任务成功完成！最终结果保存在: {OUTPUT_FILE}")
    except Exception as e:
        print(f"保存 Excel 失败，尝试保存为 CSV: {e}")
        df.to_csv(OUTPUT_FILE.replace('.xlsx', '.csv'), index=False, encoding='utf-8-sig')