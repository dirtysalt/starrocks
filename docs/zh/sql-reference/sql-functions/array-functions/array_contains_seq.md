---
displayed_sidebar: docs
---

# array_contains_seq

## 描述

检查数组 `array2` 的所有元素是否以完全相同的顺序出现在数组 `array1` 中。当且仅当 `array1 = prefix + array2 + suffix` 时，函数返回 `1`。

举例：

- `select array_contains_seq([1,2,3,4], [1,2,3]);` 返回 1。
- `select array_contains_seq([1,2,3,4], [4,3]);` 返回 0。

该函数从 3.3 版本开始支持。

## 语法

~~~Haskell
BOOLEAN array_contains_seq(arr1, arr2)
~~~

## 参数

`arr`: 要比较的两个数组。此语法检查 `arr2` 是否是 `arr1` 的子集，并且元素的顺序完全相同。

两个数组中元素的数据类型必须相同。有关 StarRocks 支持的数组元素数据类型，请参阅 [ARRAY](../../../sql-reference/data-types/semi_structured/Array.md)。

## 返回值

返回 BOOLEAN 类型的值。

- 如果 `arr2` 是 `arr1` 的子集，并且元素的顺序完全相同，则返回 `1`。否则，返回 `0`。
- 空数组默认为所有数组的子集。因此，如果 `arr1` 为有效数组，而 `arr2` 为空，返回 `1`。
- 如果任何一个输入数组 为 NULL，返回 NULL。
- 数组中的 Null 值作为正常值处理，比如 `SELECT array_contains_seq([1, 2, NULL, 3, 4], [2,3])` 返回 0。 `SELECT array_contains_seq([1, 2, NULL, 3, 4], [2,NULL,3])` 返回 1。

## 示例

```Plaintext

MySQL > select array_contains_seq([1,2,3,4], [1,2,3]);
+---------------------------------------------+
| array_contains_seq([1, 2, 3, 4], [1, 2, 3]) |
+---------------------------------------------+
|                                           1 |
+---------------------------------------------+

MySQL > select array_contains_seq([1,2,3,4], [3,2]);
+------------------------------------------+
| array_contains_seq([1, 2, 3, 4], [3, 2]) |
+------------------------------------------+
|                                        0 |
+------------------------------------------+

MySQL > select array_contains_seq([1, 2, NULL, 3, 4], ['a']);
+-----------------------------------------------+
| array_contains_all([1, 2, NULL, 3, 4], ['a']) |
+-----------------------------------------------+
|                                             0 |
+-----------------------------------------------+

MySQL > select array_contains_seq([1,2,3,4,null], null);
+------------------------------------------+
| array_contains([1, 2, 3, 4, NULL], NULL) |
+------------------------------------------+
|                                     NULL |
+------------------------------------------+

MySQL > select array_contains_seq([1,2,3,4], []);
+--------------------------------------+
| array_contains_seq([1, 2, 3, 4], []) |
+--------------------------------------+
|                                    1 |
+--------------------------------------+
```
