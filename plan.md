# Variant Dual-Mode Tracking (Unified)

本文档合并原 `plan.md` 与 `design_notes.md`，用于持续追踪：
- 设计约束（我们怎么做）
- 当前进度（已经完成什么）
- 下一步计划（还要做什么）

---

## 1. Scope and Terminology

`VariantColumn` 支持两种执行形态：

- Row mode: `ObjectColumn<VariantRowValue>`
- Shredded family:
  - Base shredded: `metadata + remain + typed_columns`
  - Typed-only shredded: 仅使用 `typed_columns`（允许 `metadata/remain` 为空）

统一语义：

- `is_shredded_variant() == true` 表示进入 shredded family
- 判定规则：`!typed_columns.empty() || (metadata && remain)`

---

## 2. Locked Decisions

### 2.1 Correctness first

- 先保证行为正确和稳定，再做 typed 路径性能优化。

### 2.2 Typed/remain ownership

- 同一行同一路径只在一个位置生效：typed 或 remain（二选一，不双写）。
- typed 命中优先读取；typed 未命中或 typed 值为空时回退 remain/materialize。
- 当 `typed_columns[i]` 为 `TYPE_VARIANT` 时，cell 语义是 `VariantRowValue(metadata_i, value_i)`：
  - `value_i` 内 field-id 仅在 `metadata_i` 命名空间内有效（per-row local dictionary）。
  - 不允许将该 cell 的 raw dict-id 直接透传到其他 metadata 命名空间。
- 对 base shredded（`metadata + remain + typed_columns`）行值重建，最终完整值必须由三者合并得到：
  - `metadata` 提供 key dictionary / field-id 语义；
  - `remain` 提供未抽取骨架；
  - `typed_columns` 提供抽取路径值（覆盖/补全骨架）。
- 因此当 `metadata/remain` 存在且 `typed_columns` 非空时，不能仅返回 `VariantRowValue(metadata, remain)` 作为最终行值。
- 目标行 metadata 约束：
  - base shredded：使用该行 `metadata_column[row]` 作为目标命名空间。
  - typed-only：由本次 materialize/build 过程生成目标 metadata（非持久，除非后续规范化到 base shredded）。

### 2.3 Path convention for arrays

- 查询路径使用显式下标：`$.a.c[0].b`、`$.a.c[1].b`。
- typed key 使用数组父路径：例如 key=`a.c`，typed 列承载数组值。

### 2.4 Typed-only contract (updated)

`Typed-only` 语义更新如下：

1) 形态定义

- `typed_columns` 非空，且 `metadata/remain` 为空时即为 typed-only 形态。
- `shredded_paths.size() == shredded_types.size() == typed_columns.size()`
- `metadata_column == nullptr && remain_value_column == nullptr`

2) 类型约束

- `typed_columns[i]` 类型允许 scalar 与 complex type（`ARRAY/MAP/STRUCT`）。
- 支持如下两类 typed-only：
  - 根单列：`keys={""}` + 单 typed 列（根值可继续 drill down）
  - 多 key 对象：例如 `a` / `a.b` / `x.y.z` 等，要求 typed key 集足以表达完整对象

3) 非法组合

- `""` 与任何非空 key 共存：非法 schema。

4) 行为约束

- `get_row_value/serialize/debug` 采用“按行即时编码 typed datum 为 `VariantRowValue`”。
- 读取路径优先 typed fast-path；未命中时保持现有 fallback 规则。
- merge 时若遇到 typed-only 与 base shredded 混合，先补齐 `metadata/remain` 再进入现有 merge/alignment。
- `variant_typeof`：
  - `keys={""}` 时返回该根值类型
  - 多 key typed-only 时根类型视为 `OBJECT`

### 2.5 Merge/cast policy

- 只允许保守安全转换（同类安全转换 + 数值 widening）。
- 无法安全 cast 时不直接报错中断，优先 remain fallback（或 hoist `TYPE_VARIANT`）。

#### Row-level merge semantics (locked)

- 本文档中的 `merge` 指 schema 仲裁 + 行追加（concat/append）语义。
- 对输入列 `a`、`b`，目标列 `c` 的数据层语义是按输入顺序追加行，而不是逐行双输入融合。
- 明确排除：`c[i] = f(a[i], b[i])` 形式的 row-wise binary merge 不在本阶段范围。

### 2.6 Merge policy for row/shredded mix

- `VariantMerger::merge_into(dst, src)` 在 `dst` 为 unshredded 且 `dst.size() > 0`、`src` 为 shredded 时，必须返回 `InvalidArgument`。
- 禁止在该场景下隐式执行以下行为：
  - 重置已有 row mode 历史行后再切换 shredded。
  - 自动将历史 row 行 materialize/upgrade 到 shredded。
- 允许的场景：
  - `dst` 为空且 `src` 为 shredded：可初始化为 shredded 后合并。
  - `dst` 与 `src` 均为 shredded：按 schema prepare/alignment + arbitration 路径合并。
  - `dst` 为 shredded、`src` 为 row：按既有 append 语义逐行合并。
- 该策略是“列间拼接 + schema 仲裁”，不定义逐行二元融合函数语义。

### 2.7 Additional locked semantics (2026-02 alignment)

1) typed key 同构前提

- 运行时默认前提：typed key 集在列级别是同构输入，不考虑冲突型 typed key 组合。
- 非同构扩展由 schema 扩展流程处理，不在 query 语义层引入歧义分支。
- typed key 路径必须唯一；重复 path 视为非法 schema。

2) missing vs JSON null

- 当前阶段不区分 “路径不存在” 与 “值为 JSON null”。
- 对查询函数统一外部行为：返回 SQL `NULL`。

3) 路径/类型不匹配行为

- 对数组索引越界、路径类型不匹配（如对标量取下标）等 seek 失败场景，统一返回 SQL `NULL`。
- 不将该类场景升级为用户可见错误。

---

## 3. Core Invariants

### 3.1 Schema alignment invariant

- `shredded_paths.size() == shredded_types.size() == typed_columns.size()`
- 三者按 index 一一对应：
  - `shredded_paths[i] <-> shredded_types[i] <-> typed_columns[i]`

### 3.2 Row alignment invariant

- 所有“已存在”的子列（`metadata`、`remain`、各 `typed_columns`）行数必须对齐。

---

## 4. Implemented (Done)

### 4.1 Dual-mode execution compatibility

- 在 cast / variant functions / parquet level 构建等路径统一复用 `VariantColumn::get_row_value(...)`。
- 清理部分重复分支与冗余控制流（包括 cast 代码路径中的简化）。

### 4.2 Typed-path read optimization

- `variant_query/get_variant_*` 支持 typed prefix 命中后继续 follow suffix path。
- 支持对象前缀最长匹配（例如 typed=`a.b`，查询 `$.a.b.c`）。
- 支持 const path fast path（常量路径不再逐行重复匹配）。
- 支持 const-column 的正确 row index 访问（常量列读 row=0）。

### 4.3 Fallback behavior

- shredded mode 下：typed 未命中、typed 为 null、或 typed 后续 seek 失败时，回退到 remain/materialize。
- unshredded mode 下：直接读取 `ObjectColumn<VariantRowValue>` 行值。

### 4.4 Shredded schema alignment before append (newly implemented)

在 `VariantColumn` 新增专用对齐函数：

- `align_shredded_schema_with(const VariantColumn& src)`

行为：

1) 两侧均为 shredded 时，对齐目的列（dst）的 schema 以支持 append 快路径。

2) 对齐后的 canonical 顺序：

- 先按 source path 顺序
- 再追加 destination-only path（保持目的列原顺序）

3) 缺失 path 处理：

- 在 dst 侧补齐对应 typed 列（nullable），并按已有行数补 null，保持行对齐。

4) 重叠 path 的类型检查：

- 同路径类型不一致时返回 false（当前不在 append 热路径内做类型仲裁）。

5) append 快路径行为：

- 已存在于 src 的 path：按列 append
- dst-only path：append null

已接入位置：

- `append(...)`
- `append_selective(...)`
- `append_value_multiple_times(...)`

### 4.5 API and call-path cleanup

- 移除了 `is_container_variant()` 别名，统一使用 `is_shredded_variant()`。
- `VariantColumn::append` 保留 `const VariantRowValue*` + `const VariantRowValue&` 两个入口：
  - 指针入口承载 nullable 语义（`nullptr` => append null）。
  - 引用入口用于 `ColumnBuilder<TYPE_VARIANT>` 等泛型调用链。
- 已清理 `VariantRowValue&&` 重载，避免不必要重载分支。

### 4.6 Schema-alignment guard strengthening

- 在 shredded append 系列路径增加 presence guard：
  - `metadata_column` presence 必须匹配
  - `remain_value_column` presence 必须匹配
- `align_shredded_schema_with(...)` 新增 guard：
  - 若 `metadata/remain` presence 不一致，直接返回 `false`
- alignment 失败时追加 warning 日志，并在 append 路径回退到 row append（保证行为可持续）。
- 补充负向 UT：
  - 同路径类型冲突 -> 对齐失败
  - metadata presence 不匹配 -> 对齐失败
  - 对齐失败时 append 回退路径可用

### 4.7 VariantMerger Stage-2 (minimal arbitration)

新增：

- `be/src/column/variant_merger.h`
- `be/src/column/variant_merger.cpp`
- `be/test/column/variant_merger_test.cpp`

并接入构建：

- `be/src/column/CMakeLists.txt`
- `be/test/CMakeLists.txt`

当前能力（Stage-2 minimal）：

1) `VariantMerger::merge(const Columns&)`

- 合并多个 `VariantColumn`，按输入顺序拼接行。

2) `VariantMerger::merge_into(VariantColumn* dst, const VariantColumn& src)`

- 对 `dst(row mode) + src(shredded mode)` 场景由 merger 显式调用 schema init。
- 当 `dst` 非空 row mode 时，拒绝切换为 shredded（返回 `InvalidArgument`），避免隐式重置历史行。
- shredded 模式下先做显式 schema 预对齐检查，再执行 append。
- 对 overlapping path 的类型冲突先做最小仲裁：
  - 仅支持 numeric widening（整数提升、整数+浮点提升到 `DOUBLE`）
  - 非兼容冲突优先 hoist 到 `TYPE_VARIANT`（通过 variant encoding 保留值语义）
  - 仲裁后再做 schema 对齐 + append
- 对当前编码器不支持的类型组合仍返回 `NotSupported`。
- shredded schema 元信息已收敛为单一 `TypeDescriptor` 数组（成员名保持 `_shredded_types`），用于保留 decimal precision/scale，并在 schema compare/alignment 与 merger 冲突检测中统一使用 descriptor 级别比较。
- `VariantMerger::merge` 在输入包含 shredded 列时会优先初始化 shredded 目标列，消除“row 在前、shredded 在后”的顺序依赖，整体向 shredded 收敛。
- decimalv3 合并采用保守策略：仅同 scale 时允许 widening；scale 不一致则 hoist 到 `TYPE_VARIANT`。

3) 设计注释补充：

- 在头文件明确“为什么存在 + 输入输出契约 + Stage-1 边界”，为后续 widening/arbitration 扩展预留集中入口。

4) 当前接入状态（重要）：

- `VariantMerger` 当前主要用于能力验证与 UT 覆盖（`VariantMergerTest*`）。
- 线上主调用链仍以 `VariantColumn::append/append_selective/append_value_multiple_times` 为主。
- 因此 Stage-2 的 merge 语义先作为规范与实现基线，不代表已切换为生产主入口。

### 4.8 Read-path closure for dual storage (newly confirmed)

围绕 `typed_columns/remain + ObjectColumn<VariantRowValue>` 的读取与表达式路径已完成收口：

### 4.9 Type file naming migration (newly completed)

为降低 `VariantValue`（基础编码单元）与 `VariantRowValue`（行级封装）命名歧义，已完成文件级重命名：

- `be/src/types/variant.h` -> `be/src/types/variant_base.h`
- `be/src/types/variant.cpp` -> `be/src/types/variant_base.cpp`
- `be/src/types/variant_value.h` -> `be/src/types/variant_row_value.h`
- `be/src/types/variant_value.cpp` -> `be/src/types/variant_row_value.cpp`
- `be/test/types/variant_value_test.cpp` -> `be/test/types/variant_row_value_test.cpp`

并完成同步改动：

- 全量 `#include` 引用切换到新头文件名。
- `be/src/types/CMakeLists.txt` 与 `be/test/types/CMakeLists.txt` 源文件列表切换到新文件名。
- 本次仅做命名迁移，不引入行为语义变更。

### 4.10 VariantNode encoding path (in progress)

围绕 “内部不依赖 VPack 树语义” 的共识，`VariantBuilder` 已开始切到 `VariantNode` 原生二进制路径：

- decode: `Variant binary -> VariantNode`（已完成）
- overlay: 路径覆盖与 skipNull 语义（已完成）
- encode: `VariantNode -> Variant binary`（已接入首版）
  - 对象 key 来自节点语义 key；
  - encode 前先 collect key 并生成目标 metadata；
  - 再基于目标 metadata 的 dict-id 写回 value，避免跨 metadata raw id 透传。

当前状态：

- `VariantBuilder::build` 已不再依赖 `VPack -> VariantEncoder::encode_vslice_to_variant` 中间链路。
- `VariantNode` 目前仍为 builder 内部类型；后续是否下沉到 `types` 公共层（作为通用内存结构）待下一步设计收口。

1) 统一读取入口

- 统一通过 `VariantColumn::get_row_value(...)` 读取行值，对上层屏蔽 row/shredded 物理差异。

2) seek/query 语义

- `variant_query/get_variant_*` 已实现 typed 优先 + suffix follow + typed miss/null fallback remain 的完整链路。
- const path 与 const variant column 的 row index 处理已覆盖。

3) cast/function 语义

- `cast variant -> scalar/varchar/json/array/map/struct` 已支持 dual-mode 读取语义。
- const-column 在 cast 路径上的索引一致性（row=0）已修复并验证。

4) merger 与读取语义协同

- merger 阶段完成最小仲裁与 schema 对齐策略，读取侧不再依赖写时特殊分支。
- 当前 foundation 目标（读取侧）已达到可用闭环。
- 当前生产主路径仍是 `VariantColumn append*`，`VariantMerger` 处于“可用但未主链接入”状态。

### 4.9 Base shredded row reconstruction closure (newly implemented)

- `VariantColumn::try_materialize_row` 在 `metadata+remain` 存在且 `typed_columns` 非空时，
  已切换为三方合并重建：`metadata + remain + typed overlays`。
- 行值不再直接返回 `VariantRowValue(metadata, remain)` 基底，避免忽略 typed 抽取列。
- 当前实现采用 correctness-first 合并路径（含 JSON 文本桥接），后续可再做无 JSON 中间态优化。
- `typed_only -> base_shredded` promotion 语义已修正：
  - 历史行在 promotion 时仅补 `null-base` 的 `metadata/remain`；
  - `typed_columns` 继续作为历史行主数据来源，避免把 typed 内容重复写入 remain。

### 4.10 Consensus alignment update (newly completed)

- 已将“merge 语义”在文档中强收口为：schema 仲裁 + 行追加（concat/append）。
- 已明确排除逐行双输入融合语义：`c[i] = f(a[i], b[i])` 不在当前范围。
- 已锁定 `TYPE_VARIANT` typed slot 的 metadata 命名空间规则：
  - 每个 cell 的 dict-id 仅在其本地 metadata 内有效；
  - 行重建必须按目标 metadata 语义重编码，禁止跨 metadata raw dict-id 透传。
- 已补充目标行 metadata 来源：
  - base shredded 使用 `metadata_column[row]`；
  - typed-only 由本次 build/materialize 生成（非持久，除非后续规范化）。
- 已补充接入状态说明：
  - `VariantMerger` 目前主要用于 UT 与能力验证；
  - 线上主路径仍是 `VariantColumn::append/append_selective/append_value_multiple_times`。
- 已新增并验证 `TYPE_VARIANT` metadata 命名空间重编码语义 UT：
  - `test_base_shredded_materialization_typed_variant_overlay_dict_namespace_remap`
  - 覆盖“typed cell 源 metadata 与目标行 metadata 不同”场景，确保按 key 语义重编码而非透传 raw dict-id。

---

## 5. Tests and Validation

### 5.1 UT

当前回归状态（最近一次重跑）：

- `be-debug-ut` 全量编译与 UT 流程通过。
- `run-debug-ut --gtest_filter='VariantBuilderTest*:VariantColumnTest*:VariantFunctionsTest.*:VectorizedCastExprTest.*variant*'`：
  - `VariantBuilderTest*` 通过
  - `VariantColumnTest*` 通过
  - `VectorizedCastExprTest.*variant*` 通过
  - `VariantFunctionsTest.*` 有 4 个失败（typed miss/null fallback remain 相关）：
    - `get_variant_int_shredded_fallback_to_remain_when_typed_path_missing`
    - `get_variant_int_shredded_fallback_to_remain_when_typed_null`
    - `get_variant_string_shredded_fallback_to_remain_when_typed_null`
    - `get_variant_double_shredded_fallback_to_remain_when_typed_null`
- `VariantColumnTest*`（25 tests，含 typed-only/base_shredded 行重建、append fallback 一致性、schema 对齐正向+负向 UT）
- `VariantMergerTest*`（15 tests，含 union/冲突hoist-variant/numeric widening/decimalv3 widening/decimal scale mismatch hoist/row-first merge 收敛、duplicate path 拒绝）
- `VectorizedCastExprTest.*variant*`（10 tests，覆盖 variant cast 的 typed-only/base_shredded 关键路径）

远端一致性检查（持续进行中）：

- 使用 `local/agent.md` 命令在远端执行：
  - `be-debug-ut`
  - `run-debug-ut --gtest_filter='VariantMergerTest*:VariantColumnTest*:VariantFunctionsTest.*:VectorizedCastExprTest.*variant*'`
- SQL 回归：`sr-run-local-test --dir='sql/test_variant_cast'` 通过。

新增关键用例覆盖：

- const path + shredded 多行 fast path
- typed object-prefix + suffix follow（`a.b` -> `a.b.c`）
- typed miss/null fallback remain
- shredded append schema alignment（source/destination path 集合不一致）
- `append/append_selective/append_value_multiple_times` 对齐失败 fallback 语义一致
- 路径/类型不匹配、数组越界统一返回 `NULL`
- `missing` 与 `json null` 不区分（统一 `NULL`）
- typed path 唯一性约束与 duplicate path 拒绝（schema/merger）

### 5.2 SQL regression

已通过：

- `sql/test_variant_cast`
- `test_iceberg_variant_query_[0-5]`
- `test_iceberg_variant_json`
- `test_iceberg_variant_write_0`
- 全量 `test_iceberg_variant*`（8/8）

---

## 6. Remaining Work

### 6.1 Near-term (Next)

1. `VariantBuilder` 优化（保持语义不变）：
   - 去 JSON 中间态：将 overlay 应用与最终编码更紧耦合到 variant binary 构建流程。
   - 继续固定 `skipNull` 语义（typed null 不覆盖 base remain），不引入 policy 分叉。
2. `rebuild` 批量化设计与实现：
   - 在 `VariantColumn` 内增加 typed-columns 批处理重建路径，减少逐行重复 encode/build 开销。
   - 保留现有逐行路径作为 correctness fallback。
3. 语义收口与代码清理：
   - 清理历史命名/注释里 `full_shredded` 残留，统一为 `base_shredded`。
   - 检查 `typed_only` promotion 后单侧所有权约束（typed 为主、remain 仅承载 base）。
4. 回归矩阵补强（UT）：
   - 增加/补强批量 rebuild 与 promotion 相关用例（typed-only -> base_shredded 后历史行语义不变）。
   - 补充 TYPE_VARIANT overlay + 目标 metadata 重编码的边界用例（const/nullable）。

### 6.2 Mid-term

1. 将 schema alignment 从“append 局部能力”扩展为“merge/write 统一前置步骤”（如需进入写侧）。
2. 在 `VariantMerger` 骨架基础上补全更细粒度类型仲裁策略（safe cast / widening / hoist `TYPE_VARIANT`）。
3. 补齐 `VariantMerger` 在 merge/write 场景的 UT 与 SQL 回归矩阵（写侧阶段）。
4. 当前阶段暂缓 `VariantMerger` 主链路接入，优先完成读取与重建链路的性能/语义稳定性。

### 6.3 Long-term

1. typed 命中率 profile 指标。
2. typed 优化覆盖更多复杂类型。
3. row mode vs shredded mode 的 CPU/内存基准评估。

---

## 7. Notes for Future Changes

- append 快路径的前提是 schema 已对齐且 index 对齐关系稳定。
- 不在 append 热路径内引入复杂类型仲裁；复杂仲裁应前移到 merge/alignment 阶段。
- 保持 `VariantRowValue` 作为执行 facade，不引入额外 view 状态机。
- 禁止跨 metadata 命名空间复用 raw dict-id（二进制透传）作为目标行编码结果；必须按目标 metadata 语义重编码。
