---
displayed_sidebar: docs
---

# timestampdiff



返回 `datetime_expr2` 和 `datetime_expr1` 的差值，其中 `datetime_expr1` 和`datetime_expr2` 是日期或日期时间表达式。

结果(整数)的单位由 `unit` 参数给出。`interval` 的单位由 `unit` 参数给出，应该是下列值之一:

MILLISECOND（3.2 及以后），SECOND，MINUTE，HOUR，DAY，WEEK，MONTH，YEAR。

## 语法

```Haskell
BIGINT TIMESTAMPDIFF(unit, DATETIME datetime_expr1, DATETIME datetime_expr2)
```

## 参数说明

- `datetime_expr`: 要比较的日期或日期时间表达式。
- `unit`：时间差值的单位。支持的单位包括 MILLISECOND (3.2 及以后)，SECOND，MINUTE，HOUR，DAY，WEEK，MONTH，YEAR。

## 返回值说明

返回 BIGINT 类型的值。

## 示例

```plain text

MySQL > SELECT TIMESTAMPDIFF(MONTH,'2003-02-01','2003-05-01');
+--------------------------------------------------------------------+
| timestampdiff(MONTH, '2003-02-01 00:00:00', '2003-05-01 00:00:00') |
+--------------------------------------------------------------------+
|                                                                  3 |
+--------------------------------------------------------------------+

MySQL > SELECT TIMESTAMPDIFF(YEAR,'2002-05-01','2001-01-01');
+-------------------------------------------------------------------+
| timestampdiff(YEAR, '2002-05-01 00:00:00', '2001-01-01 00:00:00') |
+-------------------------------------------------------------------+
|                                                                -1 |
+-------------------------------------------------------------------+

MySQL > SELECT TIMESTAMPDIFF(MINUTE,'2003-02-01','2003-05-01 12:05:55');
+---------------------------------------------------------------------+
| timestampdiff(MINUTE, '2003-02-01 00:00:00', '2003-05-01 12:05:55') |
+---------------------------------------------------------------------+
|                                                              128885 |
+---------------------------------------------------------------------+

MySQL > SELECT TIMESTAMPDIFF(MILLISECOND,'2003-02-01','2003-05-01');
+--------------------------------------------------------+
| timestampdiff(MILLISECOND, '2003-02-01', '2003-05-01') |
+--------------------------------------------------------+
|                                             7689600000 |
+--------------------------------------------------------+
```
