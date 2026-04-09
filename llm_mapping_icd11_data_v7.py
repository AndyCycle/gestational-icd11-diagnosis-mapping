import argparse
import csv
import hashlib
import json
import math
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests
from openai import OpenAI
from tqdm import tqdm

# --- CONFIGURATION (配置部分) ---
LOCAL_API_URL = "http://localhost:8382/icd/release/11/2025-01/mms/search"
VOLCENGINE_API_BASE_URL = "https://ark.cn-beijing.volces.com/api/v3"
VOLCENGINE_API_KEY_ENV_NAME = "ARK_API_KEY"
LLM_MODEL = "doubao-seed-1-6-flash-250828"

# 默认参数
DEFAULT_INPUT_CSV = r"分娩记录.xlsx"
DEFAULT_OUTPUT_CSV = r"分娩记录-icd11_mapped.xlsx"
DEFAULT_EXPERT_RULES_FILE = "专家coding校正.csv"
DEFAULT_CACHE_DIR = r"temp_cache"
DEFAULT_MAX_WORKERS = 40

LEGACY_COLUMNS_TO_PROCESS = ["手术适应症", "产科合并症"]
DIAGNOSIS_COLUMN_PATTERN = re.compile(r"diagnosis\d+$", re.IGNORECASE)

SYSTEM_PROMPT_EXPERT_AUDITOR = (
    "You are an expert Medical Coding Auditor.\n"
    "You have a list of **MANDATORY EXPERT RULES** below.\n"
    "Your task: Check if the user's input diagnosis is covered by any of these rules.\n\n"
    "**MATCHING LOGIC (STRICT):**\n"
    "1. **Semantic Equivalence:** Match if the input means the same thing as the rule (e.g., input '羊水3度' matches rule '羊水混浊').\n"
    "2. **Subtypes/Specifications:** Match if the input is a specific type/genotype of the rule (e.g., input 'Thalassemia (--SEA/aa)' matches rule 'Thalassemia').\n"
    "3. **DO NOT MATCH Composite Diagnoses:** If the input contains an **ADDITIONAL distinct medical condition** not covered by the rule, return NULL.\n"
    "   - Example: Rule is 'PROM'. Input is 'PROM with Cervical Immaturity'. Result -> NULL (Because 'Cervical Immaturity' would be lost).\n"
    "   - Example: Rule is 'Uterine Scar'. Input is 'Uterine Scar (C-section)'. Result -> MATCH (Because 'C-section' implies the scar).\n"
    "4. **Strict Output:** Return the target code directly if matched. Return null if not matched.\n\n"
    "**EXPERT RULES LIST:**\n"
)

SYSTEM_PROMPT_SPLIT_TRANSLATE = (
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

SYSTEM_PROMPT_SELECT_CODE = (
    "Select the single best ICD-11 code for the Chinese term based on the candidates. "
    "Context: Obstetrics/Maternal Care. Prioritize the underlying condition over symptoms. "
    "Respond ONLY with JSON: {'best_match_code': '...'}"
)

api_key = os.environ.get(VOLCENGINE_API_KEY_ENV_NAME)
if not api_key:
    raise ValueError(f"未找到 API Key，请设置环境变量 {VOLCENGINE_API_KEY_ENV_NAME}")

llm_client = OpenAI(base_url=VOLCENGINE_API_BASE_URL, api_key=api_key)


def extract_json_from_text(text):
    if not text:
        return None
    match = re.search(r"```json\s*(\{.*?\})\s*```", text, re.DOTALL)
    if match:
        try:
            return json.loads(match.group(1))
        except Exception:
            pass
    match = re.search(r"\{.*\}", text, re.DOTALL)
    if match:
        try:
            return json.loads(match.group(0))
        except Exception:
            pass
    return None


def clean_html(raw_html):
    cleanr = re.compile("<.*?>")
    return re.sub(cleanr, "", raw_html)


def sort_diagnosis_columns(columns):
    def key_func(col_name):
        match = re.search(r"(\d+)$", col_name)
        return int(match.group(1)) if match else 9999

    return sorted(columns, key=key_func)


def detect_columns_to_process(df, manual_columns=None):
    if manual_columns:
        cols = [c.strip() for c in manual_columns.split(",") if c.strip()]
        valid = [c for c in cols if c in df.columns]
        if not valid:
            raise ValueError(f"手动指定列无效。输入: {cols}，可选列: {list(df.columns)}")
        return valid

    diagnosis_cols = sort_diagnosis_columns([c for c in df.columns if DIAGNOSIS_COLUMN_PATTERN.fullmatch(c)])
    if diagnosis_cols:
        return diagnosis_cols

    legacy_cols = [c for c in LEGACY_COLUMNS_TO_PROCESS if c in df.columns]
    if legacy_cols:
        return legacy_cols

    raise ValueError("未检测到可处理列。期望 diagnosis1~N 或旧结构列(手术适应症/产科合并症)。")


def load_expert_rules(filepath):
    rules = []
    exact_map = {}
    try:
        with open(filepath, "r", encoding="utf-8-sig") as f:
            reader = csv.reader(f)
            first_row = next(reader, None)
            if first_row:
                header_like = len(first_row) >= 2 and (
                    "code" in str(first_row[1]).lower() or "编码" in str(first_row[1])
                )
                if not header_like and len(first_row) >= 2:
                    term = first_row[0].strip()
                    code = first_row[1].strip()
                    if term and code:
                        exact_map[term] = code
                        rules.append(f"- Rule: If input implies '{term}', map to '{code}'")
            for row in reader:
                if len(row) < 2:
                    continue
                term = row[0].strip()
                code = row[1].strip()
                if term and code:
                    exact_map[term] = code
                    rules.append(f"- Rule: If input implies '{term}', map to '{code}'")
        return "\n".join(rules), exact_map
    except Exception as e:
        print(f"Warning: Could not load expert rules: {e}")
        return "", {}


def safe_int(value, default=0):
    try:
        return int(value)
    except Exception:
        return default


def extract_cached_tokens_from_usage(usage):
    if usage is None:
        return 0, False
    for attr in ("input_tokens_details", "prompt_tokens_details"):
        details = getattr(usage, attr, None)
        if details is not None and hasattr(details, "cached_tokens"):
            return safe_int(getattr(details, "cached_tokens", None)), True
    return 0, False


class UsageTracker:
    def __init__(self, enabled=False, path=None):
        self.enabled = enabled
        self.path = path
        self.lock = threading.Lock()
        self.data = {
            "total_calls": 0,
            "calls_with_usage": 0,
            "calls_with_cached_tokens_field": 0,
            "calls_with_cache_hit": 0,
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "total_tokens": 0,
            "cached_tokens": 0,
            "by_label": {},
        }
        if self.enabled and self.path:
            with open(self.path, "w", encoding="utf-8") as f:
                f.write("")

    def record(self, label, usage, metadata):
        with self.lock:
            self.data["total_calls"] += 1
            bucket = self.data["by_label"].setdefault(
                label,
                {
                    "calls": 0,
                    "calls_with_usage": 0,
                    "calls_with_cached_tokens_field": 0,
                    "calls_with_cache_hit": 0,
                    "prompt_tokens": 0,
                    "completion_tokens": 0,
                    "total_tokens": 0,
                    "cached_tokens": 0,
                },
            )
            bucket["calls"] += 1

            record = {"label": label, **metadata}
            if usage is None:
                self._write_record(record)
                return

            self.data["calls_with_usage"] += 1
            bucket["calls_with_usage"] += 1

            prompt_tokens = safe_int(getattr(usage, "prompt_tokens", None))
            completion_tokens = safe_int(getattr(usage, "completion_tokens", None))
            total_tokens = safe_int(getattr(usage, "total_tokens", None))

            cached_tokens, cached_field_present = extract_cached_tokens_from_usage(usage)

            if cached_field_present:
                self.data["calls_with_cached_tokens_field"] += 1
                bucket["calls_with_cached_tokens_field"] += 1
            if cached_tokens > 0:
                self.data["calls_with_cache_hit"] += 1
                bucket["calls_with_cache_hit"] += 1

            self.data["prompt_tokens"] += prompt_tokens
            self.data["completion_tokens"] += completion_tokens
            self.data["total_tokens"] += total_tokens
            self.data["cached_tokens"] += cached_tokens

            bucket["prompt_tokens"] += prompt_tokens
            bucket["completion_tokens"] += completion_tokens
            bucket["total_tokens"] += total_tokens
            bucket["cached_tokens"] += cached_tokens

            record.update(
                {
                    "prompt_tokens": prompt_tokens,
                    "completion_tokens": completion_tokens,
                    "total_tokens": total_tokens,
                    "cached_tokens": cached_tokens,
                    "cached_tokens_field_present": cached_field_present,
                }
            )
            self._write_record(record)

    def _write_record(self, record):
        if not (self.enabled and self.path):
            return
        with open(self.path, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")

    def print_summary(self):
        summary = self.data
        print("\n=== LLM Cache Observability Summary ===")
        print(f"总调用数: {summary['total_calls']}")
        print(f"返回 usage 的调用数: {summary['calls_with_usage']}")
        print(f"返回 cached_tokens 字段的调用数: {summary['calls_with_cached_tokens_field']}")
        print(f"观测到缓存命中的调用数: {summary['calls_with_cache_hit']}")
        print(f"prompt_tokens 总计: {summary['prompt_tokens']}")
        print(f"completion_tokens 总计: {summary['completion_tokens']}")
        print(f"total_tokens 总计: {summary['total_tokens']}")
        print(f"cached_tokens 总计: {summary['cached_tokens']}")
        for label, bucket in summary["by_label"].items():
            print(
                f"- {label}: calls={bucket['calls']}, usage={bucket['calls_with_usage']}, "
                f"cached_field={bucket['calls_with_cached_tokens_field']}, "
                f"cache_hits={bucket['calls_with_cache_hit']}, cached_tokens={bucket['cached_tokens']}"
            )


class ThreadSafeMemo:
    def __init__(self):
        self._data = {}
        self._lock = threading.Lock()

    def get(self, key):
        with self._lock:
            return self._data.get(key)

    def set(self, key, value):
        with self._lock:
            self._data[key] = value


class PersistentJsonlCache:
    def __init__(self, path):
        self.path = path
        self._lock = threading.Lock()
        self._data = {}
        self._known_keys = set()
        if path and os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    try:
                        record = json.loads(line)
                        key = record["key"]
                        value = record["value"]
                        self._data[key] = value
                        self._known_keys.add(key)
                    except Exception:
                        continue

    def get(self, key):
        with self._lock:
            return self._data.get(key)

    def set(self, key, value):
        with self._lock:
            self._data[key] = value
            if key in self._known_keys:
                return
            self._known_keys.add(key)
            with open(self.path, "a", encoding="utf-8") as f:
                f.write(json.dumps({"key": key, "value": value}, ensure_ascii=False) + "\n")


usage_tracker = None
translation_cache = ThreadSafeMemo()
expert_llm_cache = ThreadSafeMemo()
selection_cache = ThreadSafeMemo()
diagnosis_result_cache = None


def make_extra_body(enable_common_prefix_cache, ttl_seconds):
    if not enable_common_prefix_cache:
        return None
    return {"context_options": {"mode": "common_prefix", "ttl": ttl_seconds}}


def chat_completion_json(
    *,
    label,
    system_prompt,
    user_prompt,
    temperature,
    enable_common_prefix_cache,
    ttl_seconds,
):
    kwargs = {
        "model": LLM_MODEL,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "temperature": temperature,
    }
    extra_body = make_extra_body(enable_common_prefix_cache, ttl_seconds)
    if extra_body:
        kwargs["extra_body"] = extra_body

    response = llm_client.chat.completions.create(**kwargs)
    content = response.choices[0].message.content
    if usage_tracker is not None:
        usage_tracker.record(
            label,
            getattr(response, "usage", None),
            {
                "system_prompt_chars": len(system_prompt),
                "user_prompt_chars": len(user_prompt),
                "common_prefix_cache_enabled": bool(extra_body),
            },
        )
    return extract_json_from_text(content), content


def build_expert_system_prompt(expert_rules_str):
    return (
        f"{SYSTEM_PROMPT_EXPERT_AUDITOR}"
        f"{expert_rules_str}\n\n"
        "**OUTPUT FORMAT:**\n"
        "Respond ONLY with a JSON object: {\"match_found\": boolean, \"target_code\": string or null}"
    )


def check_expert_mapping_with_llm(
    term_cn,
    expert_rules_str,
    enable_common_prefix_cache,
    ttl_seconds,
    bypass_memo=False,
):
    if not expert_rules_str:
        return None

    memo_key = ("expert_llm", term_cn)
    if not bypass_memo:
        cached = expert_llm_cache.get(memo_key)
        if cached is not None:
            return cached

    system_prompt = build_expert_system_prompt(expert_rules_str)
    user_prompt = f'Diagnosis Input: "{term_cn}"'

    try:
        result, _ = chat_completion_json(
            label="expert_match",
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            temperature=0.0,
            enable_common_prefix_cache=enable_common_prefix_cache,
            ttl_seconds=ttl_seconds,
        )
        value = None
        if result and result.get("match_found") is True:
            value = result.get("target_code")
        if not bypass_memo:
            expert_llm_cache.set(memo_key, value)
        return value
    except Exception:
        if not bypass_memo:
            expert_llm_cache.set(memo_key, None)
        return None


def get_split_and_translated_terms(term_cn, enable_common_prefix_cache, ttl_seconds):
    cached = translation_cache.get(term_cn)
    if cached is not None:
        return cached

    try:
        result, _ = chat_completion_json(
            label="split_translate",
            system_prompt=SYSTEM_PROMPT_SPLIT_TRANSLATE,
            user_prompt=f'Chinese Term: "{term_cn}"',
            temperature=0.0,
            enable_common_prefix_cache=enable_common_prefix_cache,
            ttl_seconds=ttl_seconds,
        )
        if result and "english_terms" in result:
            terms = result["english_terms"]
            value = (terms if isinstance(terms, list) else [str(terms)], None)
            translation_cache.set(term_cn, value)
            return value
        value = (None, "Splitting/Translation failed: Invalid JSON format.")
        translation_cache.set(term_cn, value)
        return value
    except Exception as e:
        value = (None, f"LLM error: {e}")
        translation_cache.set(term_cn, value)
        return value


def search_local_icd11(term_en):
    headers = {"Accept-Language": "en", "API-Version": "v2", "Accept": "application/json"}
    payload = {"q": term_en, "useFlexiSearch": "true"}
    try:
        response = requests.post(LOCAL_API_URL, headers=headers, data=payload, timeout=10)
        response.raise_for_status()
        return response.json(), None
    except Exception as e:
        return None, str(e)


def select_best_code_with_llm(term_cn, term_en, api_response, enable_common_prefix_cache, ttl_seconds):
    memo_key = ("select", term_cn, term_en)
    cached = selection_cache.get(memo_key)
    if cached is not None:
        return cached

    candidates = []
    if not api_response or not api_response.get("destinationEntities"):
        value = (None, "No API results")
        selection_cache.set(memo_key, value)
        return value

    for entity in api_response.get("destinationEntities", [])[:5]:
        candidates.append(
            {
                "code": entity.get("theCode"),
                "title": clean_html(entity.get("title", "")),
                "chapter": entity.get("chapter"),
            }
        )

    user_prompt = f"Term: {term_cn} (Search: {term_en})\nCandidates: {json.dumps(candidates, ensure_ascii=False)}"
    try:
        result, _ = chat_completion_json(
            label="select_code",
            system_prompt=SYSTEM_PROMPT_SELECT_CODE,
            user_prompt=user_prompt,
            temperature=0.1,
            enable_common_prefix_cache=enable_common_prefix_cache,
            ttl_seconds=ttl_seconds,
        )
        if result:
            value = (result, None)
            selection_cache.set(memo_key, value)
            return value
        value = (None, "Selection failed")
        selection_cache.set(memo_key, value)
        return value
    except Exception as e:
        value = (None, str(e))
        selection_cache.set(memo_key, value)
        return value


def process_cell_logic(
    cell_text,
    expert_rules_str,
    expert_rules_exact_map,
    enable_common_prefix_cache,
    ttl_seconds,
):
    if not cell_text or str(cell_text).strip() == "":
        return None

    raw_diagnoses = [d.strip() for d in str(cell_text).split("|") if d.strip()]
    final_codes_for_cell = []

    for term_cn in raw_diagnoses:
        if diagnosis_result_cache is not None:
            cached_final = diagnosis_result_cache.get(term_cn)
            if cached_final is not None:
                final_codes_for_cell.append(cached_final)
                continue

        # 本地专家规则精确匹配，优先于 LLM，直接减少长 prompt 发送次数
        exact_code = expert_rules_exact_map.get(term_cn)
        if exact_code:
            final_codes_for_cell.append(exact_code)
            if diagnosis_result_cache is not None:
                diagnosis_result_cache.set(term_cn, exact_code)
            continue

        expert_code = check_expert_mapping_with_llm(
            term_cn,
            expert_rules_str,
            enable_common_prefix_cache,
            ttl_seconds,
        )
        if expert_code:
            final_codes_for_cell.append(expert_code)
            if diagnosis_result_cache is not None:
                diagnosis_result_cache.set(term_cn, expert_code)
            continue

        english_terms_list, error = get_split_and_translated_terms(
            term_cn,
            enable_common_prefix_cache,
            ttl_seconds,
        )
        if error:
            final_codes_for_cell.append(f"ERROR: {error}")
            continue

        sub_codes = []
        for term_en in english_terms_list:
            sub_expert_code = check_expert_mapping_with_llm(
                term_en,
                expert_rules_str,
                enable_common_prefix_cache,
                ttl_seconds,
            )
            if sub_expert_code:
                sub_codes.append(sub_expert_code)
                continue

            api_response, error = search_local_icd11(term_en)
            if error:
                sub_codes.append(f"ERROR: {error}")
                continue

            llm_decision, error = select_best_code_with_llm(
                term_cn,
                term_en,
                api_response,
                enable_common_prefix_cache,
                ttl_seconds,
            )
            sub_codes.append(llm_decision.get("best_match_code") if not error else f"ERROR: {error}")

        if sub_codes:
            mapped_value = "|".join(sub_codes)
            final_codes_for_cell.append(mapped_value)
            if diagnosis_result_cache is not None and "ERROR:" not in mapped_value:
                diagnosis_result_cache.set(term_cn, mapped_value)

    return "|".join(final_codes_for_cell)


def process_chunk(
    chunk_id,
    df_subset,
    cache_path,
    expert_rules_str,
    expert_rules_exact_map,
    columns_to_process,
    enable_common_prefix_cache,
    ttl_seconds,
    progress_bar=None,
):
    local_cache = {}
    if os.path.exists(cache_path):
        with open(cache_path, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    data = json.loads(line)
                    local_cache[data["index"]] = data["results"]
                except Exception:
                    continue

    with open(cache_path, "a", encoding="utf-8", buffering=1) as f_out:
        for idx, row in df_subset.iterrows():
            if idx in local_cache:
                if progress_bar:
                    progress_bar.update(1)
                continue

            row_results = {}
            for col in columns_to_process:
                if col in row and pd.notna(row[col]):
                    row_results[col] = process_cell_logic(
                        row[col],
                        expert_rules_str,
                        expert_rules_exact_map,
                        enable_common_prefix_cache,
                        ttl_seconds,
                    )
                else:
                    row_results[col] = None

            record = json.dumps({"index": idx, "results": row_results}, ensure_ascii=False)
            f_out.write(record + "\n")
            if progress_bar:
                progress_bar.update(1)

    return f"Chunk {chunk_id} completed."


def run_cache_diagnostic(term, expert_rules_str, repeats, enable_common_prefix_cache, ttl_seconds):
    print("开始执行缓存诊断...")
    print(f"诊断术语: {term}")
    for i in range(repeats):
        result = check_expert_mapping_with_llm(
            term,
            expert_rules_str,
            enable_common_prefix_cache,
            ttl_seconds,
            bypass_memo=True,
        )
        print(f"第 {i + 1} 次 expert_match 返回: {result}")
    if usage_tracker is not None:
        usage_tracker.print_summary()


def run_responses_api_diagnostic(term, expert_rules_str, repeats):
    print("开始执行 Responses API 诊断...")
    instructions = build_expert_system_prompt(expert_rules_str)
    input_text = f'Diagnosis Input: "{term}"'

    def print_response_usage(label, response):
        usage = getattr(response, "usage", None)
        cached_tokens, cached_field_present = extract_cached_tokens_from_usage(usage)
        print(
            json.dumps(
                {
                    "label": label,
                    "id": getattr(response, "id", None),
                    "cached_tokens": cached_tokens,
                    "cached_tokens_field_present": cached_field_present,
                    "usage_repr": str(usage),
                },
                ensure_ascii=False,
            )
        )

    try:
        print("测试 1: Responses API 基础调用")
        for i in range(repeats):
            response = llm_client.responses.create(
                model=LLM_MODEL,
                instructions=instructions,
                input=input_text,
                temperature=0.0,
            )
            print_response_usage(f"responses_basic_{i + 1}", response)

        print("测试 2: Responses API + prompt_cache_key")
        for i in range(repeats):
            response = llm_client.responses.create(
                model=LLM_MODEL,
                instructions=instructions,
                input=input_text,
                temperature=0.0,
                prompt_cache_key="expert_rules_test_key_v1",
                prompt_cache_retention="24h",
            )
            print_response_usage(f"responses_prompt_cache_{i + 1}", response)

        print("测试 3: Responses API + previous_response_id")
        response = llm_client.responses.create(
            model=LLM_MODEL,
            instructions=instructions,
            input=input_text,
            temperature=0.0,
        )
        print_response_usage("responses_previous_1", response)
        prev_id = response.id
        for i in range(repeats - 1):
            response = llm_client.responses.create(
                model=LLM_MODEL,
                previous_response_id=prev_id,
                input=f"再次判断：{term}",
                temperature=0.0,
            )
            print_response_usage(f"responses_previous_{i + 2}", response)
            prev_id = response.id
    except Exception as e:
        print(f"Responses API 诊断失败: {e}")


def build_diagnosis_cache_path(cache_dir, model_name, expert_rules_str):
    rules_hash = hashlib.md5(expert_rules_str.encode("utf-8")).hexdigest()[:8] if expert_rules_str else "no_rules"
    safe_model = re.sub(r"[^A-Za-z0-9._-]+", "_", model_name)
    return os.path.join(cache_dir, f"diagnosis_term_cache__{safe_model}__{rules_hash}.jsonl")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="LLM ICD-11 映射脚本，兼容 diagnosis1~N 与旧结构列。")
    parser.add_argument("--input-file", dest="input_file", default=DEFAULT_INPUT_CSV, help="输入文件路径(支持.csv或.xlsx)")
    parser.add_argument("--output-file", dest="output_file", default=DEFAULT_OUTPUT_CSV, help="输出文件路径(支持.csv或.xlsx)")
    parser.add_argument("--expert-rules", default=DEFAULT_EXPERT_RULES_FILE, help="专家规则CSV路径")
    parser.add_argument("--cache-dir", default=DEFAULT_CACHE_DIR, help="缓存目录")
    parser.add_argument("--columns", default=None, help="手动指定处理列，逗号分隔；不传则自动检测")
    parser.add_argument("--max-workers", type=int, default=DEFAULT_MAX_WORKERS, help="并发线程数")
    parser.add_argument("--cache-ttl", type=int, default=86400, help="common_prefix TTL 秒数")
    parser.add_argument("--disable-common-prefix-cache", action="store_true", help="关闭 common_prefix 缓存参数")
    parser.add_argument("--usage-log", default=None, help="将每次调用的 usage/cached_tokens 写入 jsonl")
    parser.add_argument("--cache-diagnostic-term", default=None, help="对单个术语重复调用，观察 cached_tokens")
    parser.add_argument("--cache-diagnostic-repeats", type=int, default=3, help="缓存诊断重复次数")
    parser.add_argument("--responses-diagnostic-term", default=None, help="测试官方 Responses API / cached_tokens 链路")
    args = parser.parse_args()

    usage_tracker = UsageTracker(enabled=bool(args.usage_log), path=args.usage_log)

    input_file = args.input_file
    output_file = args.output_file
    expert_rules_file = args.expert_rules
    cache_dir = args.cache_dir
    max_workers = max(1, int(args.max_workers))
    enable_common_prefix_cache = not args.disable_common_prefix_cache
    ttl_seconds = max(1, int(args.cache_ttl))

    output_dir = os.path.dirname(output_file)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)
    if not os.path.exists(cache_dir):
        os.makedirs(cache_dir, exist_ok=True)

    print("正在加载专家映射规则...")
    expert_rules_str, expert_rules_exact_map = load_expert_rules(expert_rules_file)
    if expert_rules_str:
        print(f"专家规则加载成功，规则文本长度: {len(expert_rules_str)}")
    print(f"本地精确规则条数: {len(expert_rules_exact_map)}")
    print(f"common_prefix 缓存参数启用: {enable_common_prefix_cache}, ttl={ttl_seconds}")

    diagnosis_cache_path = build_diagnosis_cache_path(cache_dir, LLM_MODEL, expert_rules_str)
    diagnosis_result_cache = PersistentJsonlCache(diagnosis_cache_path)
    print(f"诊断术语持久缓存: {diagnosis_cache_path}")

    if args.cache_diagnostic_term:
        run_cache_diagnostic(
            args.cache_diagnostic_term,
            expert_rules_str,
            max(1, args.cache_diagnostic_repeats),
            enable_common_prefix_cache,
            ttl_seconds,
        )
        raise SystemExit(0)

    if args.responses_diagnostic_term:
        run_responses_api_diagnostic(
            args.responses_diagnostic_term,
            expert_rules_str,
            max(1, args.cache_diagnostic_repeats),
        )
        raise SystemExit(0)

    print(f"正在读取文件: {input_file} ...")
    if str(input_file).lower().endswith(('.xlsx', '.xls')):
        df = pd.read_excel(input_file, dtype=str)
    else:
        df = pd.read_csv(input_file, encoding="utf-8-sig", low_memory=False, dtype=str)
    columns_to_process = detect_columns_to_process(df, args.columns)
    print(f"本次处理列: {columns_to_process}")

    total_rows = len(df)
    chunk_size = math.ceil(total_rows / max_workers)
    futures = []

    print(f"启动 {max_workers} 个线程并行处理，缓存目录: {cache_dir} ...")
    pbar = tqdm(total=total_rows, desc="并行映射中")
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for i in range(max_workers):
            start_idx = i * chunk_size
            end_idx = min((i + 1) * chunk_size, total_rows)
            if start_idx >= total_rows:
                break

            df_subset = df.iloc[start_idx:end_idx]
            thread_cache_file = os.path.join(cache_dir, f"mapping_cache_part_{i}.jsonl")
            future = executor.submit(
                process_chunk,
                i,
                df_subset,
                thread_cache_file,
                expert_rules_str,
                expert_rules_exact_map,
                columns_to_process,
                enable_common_prefix_cache,
                ttl_seconds,
                pbar,
            )
            futures.append(future)

        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"线程异常: {e}")

    pbar.close()
    print(f"处理完成，耗时: {time.time() - start_time:.2f}秒")

    print("正在合并缓存数据...")
    full_results_map = {}
    for i in range(max_workers):
        cache_file = os.path.join(cache_dir, f"mapping_cache_part_{i}.jsonl")
        if os.path.exists(cache_file):
            with open(cache_file, "r", encoding="utf-8") as f:
                for line in f:
                    try:
                        data = json.loads(line)
                        idx = int(data["index"])
                        full_results_map[idx] = data["results"]
                    except Exception:
                        continue

    print("正在生成最终 CSV...")
    new_cols_data = {f"{col}_ICD11_Code": [] for col in columns_to_process}
    for idx in df.index:
        res_dict = full_results_map.get(idx, {})
        for col in columns_to_process:
            new_cols_data[f"{col}_ICD11_Code"].append(res_dict.get(col, None))

    for col_name, data_list in new_cols_data.items():
        df[col_name] = data_list

    if str(output_file).lower().endswith(('.xlsx', '.xls')):
        df.to_excel(output_file, index=False)
    else:
        df.to_csv(output_file, index=False, encoding="utf-8-sig")
    print(f"成功！结果已保存至: {output_file}")
    if usage_tracker is not None:
        usage_tracker.print_summary()
