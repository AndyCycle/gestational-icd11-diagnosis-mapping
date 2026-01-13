import requests
import json
import os
import re
import math
import time
import pandas as pd
from openai import OpenAI
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- CONFIGURATION (配置部分) ---
LOCAL_API_URL = "http://localhost:8382/icd/release/11/2025-01/mms/search"
VOLCENGINE_API_BASE_URL = "https://ark.cn-beijing.volces.com/api/v3"
VOLCENGINE_API_KEY_ENV_NAME = "ARK_API_KEY"
LLM_MODEL = "your_model_endpoint" #e.g. doubao-seed-1-6-flash-250828

# 文件路径配置 (请确认路径无误)
INPUT_CSV = r'分娩记录_stage1_cleaned.csv'
OUTPUT_CSV = '分娩记录_编码后.csv'
EXPERT_RULES_FILE = '专家coding校正.csv'
CACHE_DIR = r'temp_cache'

COLUMNS_TO_PROCESS = ["手术适应症", "产科合并症"]
MAX_WORKERS = 40  # 并发数

# API 初始化
api_key = os.environ.get(VOLCENGINE_API_KEY_ENV_NAME)
if not api_key:
    raise ValueError(f"未找到 API Key，请设置环境变量 {VOLCENGINE_API_KEY_ENV_NAME}")

llm_client = OpenAI(base_url=VOLCENGINE_API_BASE_URL, api_key=api_key)

# -------------------------------------------------
# 1. 基础工具函数
# -------------------------------------------------

def extract_json_from_text(text):
    if not text: return None
    match = re.search(r'```json\s*(\{.*?\})\s*```', text, re.DOTALL)
    if match:
        try: return json.loads(match.group(1))
        except: pass
    match = re.search(r'\{.*\}', text, re.DOTALL)
    if match:
        try: return json.loads(match.group(0))
        except: pass
    return None

def clean_html(raw_html):
    cleanr = re.compile('<.*?>')
    return re.sub(cleanr, '', raw_html)

def load_expert_rules(filepath):
    rules = []
    try:
        with open(filepath, 'r', encoding='utf-8-sig') as f:
            import csv
            reader = csv.reader(f)
            first_row = next(reader, None)
            if first_row:
                if "code" in str(first_row[1]).lower() or "编码" in str(first_row[1]):
                    pass
                else:
                    if len(first_row) >= 2:
                        rules.append(f"- Rule: If input implies '{first_row[0]}', map to '{first_row[1]}'")
            for row in reader:
                if len(row) >= 2:
                    term = row[0].strip()
                    code = row[1].strip()
                    if term and code:
                        rules.append(f"- Rule: If input implies '{term}', map to '{code}'")
        return "\n".join(rules)
    except Exception as e:
        print(f"Warning: Could not load expert rules: {e}")
        return ""

# -------------------------------------------------
# 2. 核心业务逻辑 (已集成前缀缓存)
# -------------------------------------------------

def check_expert_mapping_with_llm(term_cn, expert_rules_str):
    if not expert_rules_str: return None
    system_prompt = (
        "You are an expert Medical Coding Auditor.\n"
        "You have a list of **MANDATORY EXPERT RULES** below.\n"
        "Your task: Check if the user's input diagnosis is covered by any of these rules.\n\n"
        "**MATCHING LOGIC (STRICT):**\n"
        "1. **Semantic Equivalence:** Match if the input means the same thing as the rule (e.g., input '羊水3度' matches rule '羊水混浊').\n"
        "2. **Subtypes/Specifications:** Match if the input is a specific type/genotype of the rule (e.g., input 'Thalassemia (--SEA/aa)' matches rule 'Thalassemia').\n"
        "3. **DO NOT MATCH Composite Diagnoses:** If the input contains an **ADDITIONAL distinct medical condition** not covered by the rule, return NULL. \n"
        "   - Example: Rule is 'PROM'. Input is 'PROM with Cervical Immaturity'. Result -> NULL (Because 'Cervical Immaturity' would be lost).\n"
        "   - Example: Rule is 'Uterine Scar'. Input is 'Uterine Scar (C-section)'. Result -> MATCH (Because 'C-section' implies the scar).\n"
        "4. **Strict Output:** Return the target code directly if matched. Return null if not matched.\n\n"
        "**EXPERT RULES LIST:**\n"
        f"{expert_rules_str}\n\n"
        "**OUTPUT FORMAT:**\n"
        "Respond ONLY with a JSON object: {\"match_found\": boolean, \"target_code\": string or null}"
    )
    user_prompt = f"Diagnosis Input: \"{term_cn}\""

    try:
        response = llm_client.chat.completions.create(
            model=LLM_MODEL,
            messages=[{"role": "system", "content": system_prompt}, {"role": "user", "content": user_prompt}],
            temperature=0.0,
            # [关键修改] 启用前缀缓存
            extra_body={
                "context_options": {
                    "mode": "common_prefix",
                    "ttl": 86400
                }
            }
        )
        content = response.choices[0].message.content
        result = extract_json_from_text(content)
        if result and result.get("match_found") is True:
            return result.get("target_code")
        return None
    except Exception as e:
        return None

def get_split_and_translated_terms(term_cn):
    system_prompt = (
        "You are a clinical coding expert.\n"
        "Your task: Analyze the input Chinese clinical term.\n"
        "1. **Check for Compound Diagnoses:** If the term actually contains two or more distinct medical conditions merged together (e.g., 'Condition A with Condition B', 'A and B'), you MUST split them.\n"
        "2. **Translate:** Convert each distinct condition into its standard English medical term for ICD-11 search.\n"
        "3. **Refine:** Remove adjectives that are not clinically essential for the main code (e.g., 'history of', 'status post') unless it is the main diagnosis.\n\n"
        "**Examples:**\n"
        "- Input: '胎膜早破伴宫颈不成熟' -> Output: ['Premature rupture of membranes', 'Cervical immaturity']\n"
        "- Input: '瘢痕子宫' -> Output: ['Uterine scar from previous surgery'] (Single condition)\n"
        "- Input: 'GDM(A2级)' -> Output: ['Gestational diabetes mellitus'] (Grade is usually a modifier, keep core)\n"
        "- Input: '妊娠合并贫血及血小板减少' -> Output: ['Anemia in pregnancy', 'Thrombocytopenia']\n\n"
        "Respond ONLY with a JSON object: {\"english_terms\": [\"term1\", \"term2\", ...]}"
    )
    try:
        response = llm_client.chat.completions.create(
            model=LLM_MODEL,
            messages=[{"role": "system", "content": system_prompt}, {"role": "user", "content": f"Chinese Term: \"{term_cn}\""}],
            temperature=0.0,
             # [关键修改] 启用前缀缓存
            extra_body={
                "context_options": {
                    "mode": "common_prefix",
                    "ttl": 86400
                }
            }
        )
        content = response.choices[0].message.content
        result = extract_json_from_text(content)
        if result and "english_terms" in result:
            terms = result["english_terms"]
            if isinstance(terms, list): return terms, None
            else: return [str(terms)], None
        return None, "Splitting/Translation failed: Invalid JSON format."
    except Exception as e:
        return None, f"LLM error: {e}"

def search_local_icd11(term_en):
    headers = {"Accept-Language": "en", "API-Version": "v2", "Accept": "application/json"}
    payload = {'q': term_en, 'useFlexiSearch': 'true'}
    try:
        response = requests.post(LOCAL_API_URL, headers=headers, data=payload, timeout=10)
        response.raise_for_status()
        return response.json(), None
    except Exception as e:
        return None, str(e)

def select_best_code_with_llm(term_cn, term_en, api_response):
    candidates = []
    if not api_response or not api_response.get("destinationEntities"):
        return None, "No API results"
    for entity in api_response.get("destinationEntities", [])[:5]:
        candidates.append({
            "code": entity.get("theCode"),
            "title": clean_html(entity.get("title", "")),
            "chapter": entity.get("chapter")
        })
    system_prompt = (
        "Select the single best ICD-11 code for the Chinese term based on the candidates. "
        "Context: Obstetrics/Maternal Care. Prioritize the underlying condition over symptoms. "
        "Respond ONLY with JSON: {'best_match_code': '...'}"
    )
    user_prompt = f"Term: {term_cn} (Search: {term_en})\nCandidates: {json.dumps(candidates)}"
    try:
        response = llm_client.chat.completions.create(
            model=LLM_MODEL,
            messages=[{"role": "system", "content": system_prompt}, {"role": "user", "content": user_prompt}],
            temperature=0.1,
             # [关键修改] 启用前缀缓存
            extra_body={
                "context_options": {
                    "mode": "common_prefix",
                    "ttl": 86400
                }
            }
        )
        result = extract_json_from_text(response.choices[0].message.content)
        if result: return result, None
        return None, "Selection failed"
    except Exception as e:
        return None, str(e)

def process_cell_logic(cell_text, expert_rules_str):
    if not cell_text or str(cell_text).strip() == "": return None
    raw_diagnoses = [d.strip() for d in str(cell_text).split('|') if d.strip()]
    final_codes_for_cell = []

    for term_cn in raw_diagnoses:
        expert_code = check_expert_mapping_with_llm(term_cn, expert_rules_str)
        if expert_code:
            final_codes_for_cell.append(expert_code)
            continue
        english_terms_list, error = get_split_and_translated_terms(term_cn)
        if error:
            final_codes_for_cell.append(f"ERROR: {error}")
            continue
        sub_codes = []
        for term_en in english_terms_list:
            sub_expert_code = check_expert_mapping_with_llm(term_en, expert_rules_str)
            if sub_expert_code:
                sub_codes.append(sub_expert_code)
            else:
                api_response, error = search_local_icd11(term_en)
                if error:
                    sub_codes.append(f"ERROR: {error}")
                    continue
                llm_decision, error = select_best_code_with_llm(term_cn, term_en, api_response)
                sub_codes.append(llm_decision.get('best_match_code') if not error else f"ERROR: {error}")
        if sub_codes:
            final_codes_for_cell.append("|".join(sub_codes))
    return "|".join(final_codes_for_cell)

# -------------------------------------------------
# 3. 并行处理与线程工作函数
# -------------------------------------------------

def process_chunk(chunk_id, df_subset, cache_path, expert_rules_str, progress_bar=None):
    local_cache = {}
    if os.path.exists(cache_path):
        with open(cache_path, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    data = json.loads(line)
                    local_cache[data['index']] = data['results']
                except: continue

    with open(cache_path, 'a', encoding='utf-8', buffering=1) as f_out:
        for idx, row in df_subset.iterrows():
            if idx in local_cache:
                if progress_bar: progress_bar.update(1)
                continue
            row_results = {}
            for col in COLUMNS_TO_PROCESS:
                if col in row and pd.notna(row[col]):
                    cell_text = row[col]
                    mapped_code = process_cell_logic(cell_text, expert_rules_str)
                    row_results[col] = mapped_code
                else:
                    row_results[col] = None
            record = json.dumps({"index": idx, "results": row_results}, ensure_ascii=False)
            f_out.write(record + "\n")
            if progress_bar: progress_bar.update(1)
    return f"Chunk {chunk_id} completed."

# -------------------------------------------------
# 4. 主程序
# -------------------------------------------------

if __name__ == '__main__':
    output_dir = os.path.dirname(OUTPUT_CSV)
    if not os.path.exists(output_dir): os.makedirs(output_dir, exist_ok=True)
    if not os.path.exists(CACHE_DIR): os.makedirs(CACHE_DIR, exist_ok=True)

    print("正在加载专家映射规则...")
    expert_rules_str = load_expert_rules(EXPERT_RULES_FILE)
    if expert_rules_str: print(f"专家规则加载成功，长度: {len(expert_rules_str)}")

    print(f"正在读取文件: {INPUT_CSV} ...")
    df = pd.read_csv(INPUT_CSV, encoding='utf-8-sig')
    total_rows = len(df)

    chunk_size = math.ceil(total_rows / MAX_WORKERS)
    futures = []

    print(f"启动 {MAX_WORKERS} 个线程并行处理，已启用前缀缓存，缓存目录: {CACHE_DIR} ...")
    pbar = tqdm(total=total_rows, desc="并行映射中")
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for i in range(MAX_WORKERS):
            start_idx = i * chunk_size
            end_idx = min((i + 1) * chunk_size, total_rows)
            if start_idx >= total_rows: break

            df_subset = df.iloc[start_idx:end_idx]
            thread_cache_file = os.path.join(CACHE_DIR, f"mapping_cache_part_{i}.jsonl")

            future = executor.submit(process_chunk, i, df_subset, thread_cache_file, expert_rules_str, pbar)
            futures.append(future)

        for future in as_completed(futures):
            try: future.result()
            except Exception as e: print(f"线程异常: {e}")

    pbar.close()
    print(f"处理完成，耗时: {time.time() - start_time:.2f}秒")

    print("正在合并缓存数据...")
    full_results_map = {}
    for i in range(MAX_WORKERS):
        cache_file = os.path.join(CACHE_DIR, f"mapping_cache_part_{i}.jsonl")
        if os.path.exists(cache_file):
            print(f"读取分片: {cache_file}")
            with open(cache_file, 'r', encoding='utf-8') as f:
                for line in f:
                    try:
                        data = json.loads(line)
                        idx = int(data['index'])
                        full_results_map[idx] = data['results']
                    except: continue

    print("正在生成最终 CSV...")
    new_cols_data = {f"{col}_ICD11_Code": [] for col in COLUMNS_TO_PROCESS}
    for idx in df.index:
        res_dict = full_results_map.get(idx, {})
        for col in COLUMNS_TO_PROCESS:
            new_cols_data[f"{col}_ICD11_Code"].append(res_dict.get(col, None))

    for col_name, data_list in new_cols_data.items():
        df[col_name] = data_list

    try:
        df.to_csv(OUTPUT_CSV, index=False, encoding='utf-8-sig')
        print(f"成功！结果已保存至: {OUTPUT_CSV}")
    except Exception as e:
        print(f"保存 CSV 失败: {e}")