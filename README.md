# gestational-icd11-diagnosis-mapping

本项目是一套面向产科临床文本的 ICD-11 编码流水线，覆盖：

1. 原始文本清洗
2. LLM + ICD-11 API 自动编码
3. inspection 异常巡检
4. 规则补救替换
5. 全局编码替换
6. 编码/疾病样本定位
7. 统计与绘图

当前脚本已经同时兼容两类列结构：

- 新结构：`diagnosis1 ~ diagnosisN` 及对应 `diagnosis1_ICD11_Code ~ diagnosisN_ICD11_Code`
- 旧结构：`产科合并症`、`手术适应症`、`孕期风险项` 及对应 `_ICD11_Code`

## 环境与依赖

需要的常用 Python 库：

- `pandas`
- `polars`
- `requests`
- `openai`
- `tqdm`
- `matplotlib`
- `openpyxl`

外部依赖：

1. 本地 ICD-11 API 服务  
默认按 WHO 本地部署方案运行。
2. LLM API Key  
当前映射脚本使用 `ARK_API_KEY`。

## 核心文件

### 映射与清洗

- [llm_mapping_icd11_data_v7.py](./llm_mapping_icd11_data_v7.py)  
主映射脚本，负责调用 LLM 和 ICD-11 API。
- `stage1_clean_20250805.py`  
统一预清洗入口。
- `stage1_clean_obstetric_complications.py`
- `stage1_clean_surgical_indications.py`
- `stage1_clean_pregnancy_risks.py`
- [map_risk_item_icd11.py](./map_risk_item_icd11.py)  
孕期风险项专项映射。

### 巡检与补救

- [run_inspection.py](./run_inspection.py)  
生成异常报告、term-code 统计、补救规则模板。
- [补救替换.py](./补救替换.py)  
按规则替换错误编码，支持噪声剔除。
- [全局替换编码.py](./全局替换编码.py)  
按旧编码全局替换为新编码。
- [查询疾病编码样本.py](./查询疾病编码样本.py)  
按编码和疾病关键词定位样本。

### 统计与展示

- `run_statistics.py`
- `绘图.py`

### 规则与映射表

- `专家coding校正.csv`  
LLM 映射前的专家强制规则。
- `孕期风险项coding.csv`  
孕期风险项专项映射表。
- `fix_rules_template.csv`  
inspection 生成后人工维护的补救替换规则。
- `code_full_replace_template.csv`  
全局编码替换映射表，可用于批量替换旧编码。

## 推荐流程

### 1. 数据预清洗

```bash
python stage1_clean_20250805.py 原始文件.csv 清洗后文件.csv
```

### 2. LLM 自动编码

```bash
python llm_mapping_icd11_data_v7.py --input-file 清洗后文件.csv --output-file 编码结果.csv
```

常用参数：

- `--expert-rules`：专家规则 CSV
- `--cache-dir`：缓存目录
- `--columns`：手动指定处理列，逗号分隔
- `--max-workers`：并发数
- `--usage-log`：输出 token/缓存统计日志

### 3. 生成 inspection 报告

```bash
python run_inspection.py -i 编码结果.csv
```

默认会输出：

- `inspection_report.json`
- `inspection_flags.csv`
- `inspection_term_code_stats.csv`
- `fix_rules_template.csv`

### 4. 规则补救替换

```bash
python 补救替换.py -i 编码结果.csv -r fix_rules_template.csv
```

默认会输出：

- `编码结果-fixed.csv`
- `编码结果-fix_apply_report.csv`

### 5. 全局编码替换

当你已经确认“某个旧编码在全表都应该改成另一个新编码”时，不建议再走 term 规则，直接用：

```bash
python 全局替换编码.py -i 编码结果.csv --from-code 5B81.Z --to-code JB64.2/5B81.Z
```

也支持批量映射表：

```bash
python 全局替换编码.py -i 编码结果.csv -m code_full_replace_template.csv
```

### 6. 查询样本

按编码和疾病词定位：

```bash
python 查询疾病编码样本.py -i 编码结果.csv -c JB64.2/5B81.Z -k 妊娠合并肥胖症
```

按编码查：

```bash
python 查询疾病编码样本.py -i 编码结果.csv -c 5B81.Z
```

按疾病词查：

```bash
python 查询疾病编码样本.py -i 编码结果.csv -k 胎盘血窦
```

### 7. 统计与绘图

```bash
python run_statistics.py
python 绘图.py -i 统计报告.csv
```

## fix_rules_template.csv 规则说明

字段含义：

| 列名 | 含义 |
| :--- | :--- |
| `enabled` | `Y` 生效，`N` 跳过 |
| `term_match_mode` | `exact` 或 `contains` |
| `term_keyword` | 待匹配的疾病词 |
| `wrong_code` | 需要被替换的旧编码；为空时表示允许补码 |
| `correct_code` | 替换后的新编码 |
| `column_scope` | `ALL` 或指定列名 |
| `note` | 备注 |

使用建议：

1. 能用 `exact` 就不要先用 `contains`
2. 能写 `wrong_code` 就尽量写，避免误改
3. inspection 推荐规则可以作为起点，但不等于一定能直接修完所有问题

## 常见问答

### Q1：为什么 inspection 产出的规则，补救替换时显示“有效规则数 > 0，但修改数 = 0”？

通常有 4 种原因：

1. 你补救后重新 inspection 的输入文件不是 `-fixed` 文件
2. 当前数据里没有真正命中该规则的记录
3. `wrong_code` 写得过严，实际错误码不是这一条
4. 该记录属于 `TERM_CODE_LENGTH_MISMATCH`，term 和 code 没有一一对齐，脚本无法直接替换

优先检查：

- 补救报告里有没有 `RULE_UPDATED`
- inspection 跑的是不是补救后的输出文件

### Q2：为什么 inspection 推荐了 `HBV` 规则，但补救替换没有命中？

因为 inspection 在统计时会先对 term 做标准化，而补救脚本现在也已经同步做了标准化匹配。  
如果仍然没命中，通常说明：

1. 当前输入文件里不存在 `wrong_code` 对应的那条错误编码
2. 这条记录并不是标准一一配对，而是错位或黏连场景

### Q3：`column_scope` 是什么？

它表示规则作用范围：

- `ALL`：所有诊断列都可应用
- 指定列名：只对该列生效，例如 `diagnosis2`

### Q4：`TERM_CODE_LENGTH_MISMATCH` 应该怎么理解？

它表示 term 数量和 code 数量不一致。常见原因：

1. 一个诊断被编码成两个 code
2. 一个诊断在 diagnosis 侧没切开，但在 code 侧被正确拆开
3. LLM 认为某个词不适合编码，所以 code 比 term 少

这类问题通常比普通替换更难，不能只靠简单规则解决。

### Q5：剩余大量 `TERM_CODE_LENGTH_MISMATCH` 应该怎么处理？

推荐顺序：

1. 先用 [查询疾病编码样本.py](./查询疾病编码样本.py) 定位高频问题样本
2. 对明确“全局都错”的旧编码，用 [全局替换编码.py](./全局替换编码.py)
3. 对 term 已经比较清楚、只是个别错误码的，用 [补救替换.py](./补救替换.py)
4. 对 diagnosis 串严重黏连的样本，优先回到预清洗/切分阶段修复

### Q6：什么时候该用“补救替换”，什么时候该用“全局替换编码”？

用 [补救替换.py](./补救替换.py)：

- 你希望“某个疾病词在某种错误码下改成正确码”
- 你需要 `exact/contains` + `wrong_code` 这类细粒度控制

用 [全局替换编码.py](./全局替换编码.py)：

- 你已经确定某个旧编码整体都是错的
- 不需要看 diagnosis 文本，直接全表替换

### Q7：如何快速定位某个编码为什么还残留？

用查询脚本直接查：

```bash
python 查询疾病编码样本.py -i 编码结果.csv -c 5B81.Z
```

或者编码 + 疾病词联合查：

```bash
python 查询疾病编码样本.py -i 编码结果.csv -c 5B81.Z -k 妊娠合并肥胖症
```

### Q8：为什么 `D-二聚体升高` 这类规则不会被加载？

如果 `correct_code` 为空，而你又没有加 `--allow-empty-correct`，该规则会被自动过滤，不会进入有效规则数。

### Q9：`补救替换.py` 输出文件名为什么自动变成 `-fixed`？

这是为了降低误操作风险。  
如果你不显式指定 `-o`，脚本会默认输出到“输入文件名 `-fixed`”，避免覆盖原始编码文件。

### Q10：我应该优先维护哪些模板文件？

优先级建议：

1. `专家coding校正.csv`：影响源头映射质量
2. `fix_rules_template.csv`：影响补救替换
3. `code_full_replace_template.csv`：适合维护全局旧码 -> 新码映射
4. `孕期风险项coding.csv`：只在风险项专项映射场景下重要

## 注意事项

1. 不要直接覆盖原始编码结果，优先保留 `-fixed`、`-code-fixed` 等版本
2. inspection、补救、全局替换最好始终围绕同一个最新输出文件继续迭代
3. 如果某个问题主要表现为 diagnosis 串黏连，不要只堆编码规则，应回到清洗或切分阶段修复
4. 若在 Linux 环境绘图，需确认系统安装了支持中文的字体
