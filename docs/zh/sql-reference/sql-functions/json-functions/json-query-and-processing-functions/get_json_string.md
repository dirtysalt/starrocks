---
displayed_sidebar: docs
---

# get_json_string,get_json_object

分析并从JSON字符串中指定路径 (`json_path`) 获取字符串。如果 `json_string` 或 `json_path` 的格式错误，或者没有找到匹配的值，该函数将返回 NULL。

:::tip
所有的 JSON 函数和操作符都列在导航栏和 [概览页面](../overview-of-json-functions-and-operators.md)

通过 [生成列](../../../sql-statements/generated_columns.md) 加速查询
:::

别名为 get_json_object。

## 语法

```Haskell
VARCHAR get_json_string(VARCHAR json_str, VARCHAR json_path)
```

## 参数

- `json_str`: JSON 字符串。支持的数据类型是 VARCHAR。
- `json_path`: JSON 路径。支持的数据类型是 VARCHAR。`json_path` 以 `$` 开头，并使用 `.` 作为路径分隔符。`[ ]` 用作数组下标，从 0 开始。例如，`$."my.key"[1]` 表示获取元素 `my.key` 的第二个值。

## 返回值

返回 VARCHAR 类型的值。如果没有找到匹配的对象，则返回 NULL。

## 示例

示例 1: 获取键为 `k1` 的值。

```Plain Text
MySQL > SELECT get_json_string('{"k1":"v1", "k2":"v2"}', "$.k1");
+---------------------------------------------------+
| get_json_string('{"k1":"v1", "k2":"v2"}', '$.k1') |
+---------------------------------------------------+
| v1                                                |
+---------------------------------------------------+
```

示例 2: 从第一个元素中获取键为 `a` 的值。

```Plain Text
MySQL > SELECT get_json_object('[{"a":"123", "b": "456"},{"a":"23", "b": "56"}]', '$[0].a');
+------------------------------------------------------------------------------+
| get_json_object('[{"a":"123", "b": "456"},{"a":"23", "b": "56"}]', '$[0].a') |
+------------------------------------------------------------------------------+
| 123                                                                          |
+------------------------------------------------------------------------------+
```

示例 3: 获取数组中键为 `my.key` 的第二个元素。

```Plain Text
MySQL > SELECT get_json_string('{"k1":"v1", "my.key":["e1", "e2", "e3"]}', '$."my.key"[1]');
+------------------------------------------------------------------------------+
| get_json_string('{"k1":"v1", "my.key":["e1", "e2", "e3"]}', '$."my.key"[1]') |
+------------------------------------------------------------------------------+
| e2                                                                           |
+------------------------------------------------------------------------------+
```

示例 4: 获取路径为 `k1.key -> k2` 的数组中的第一个元素。

```Plain Text
MySQL > SELECT get_json_string('{"k1.key":{"k2":["v1", "v2"]}}', '$."k1.key".k2[0]');
+-----------------------------------------------------------------------+
| get_json_string('{"k1.key":{"k2":["v1", "v2"]}}', '$."k1.key".k2[0]') |
+-----------------------------------------------------------------------+
| v1                                                                    |
+-----------------------------------------------------------------------+
```

示例 5: 获取数组中所有键为 `k1` 的值。

```Plain Text
MySQL > SELECT get_json_string('[{"k1":"v1"}, {"k2":"v2"}, {"k1":"v3"}, {"k1":"v4"}]', '$.k1');
+---------------------------------------------------------------------------------+
| get_json_string('[{"k1":"v1"}, {"k2":"v2"}, {"k1":"v3"}, {"k1":"v4"}]', '$.k1') |
+---------------------------------------------------------------------------------+
| ["v1","v3","v4"]                                                                |
+---------------------------------------------------------------------------------+
```

## 关键词

GET_JSON_STRING,GET,JSON,STRING