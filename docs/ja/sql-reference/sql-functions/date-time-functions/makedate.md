---
displayed_sidebar: docs
---

# makedate

指定された年と年の日数に基づいて日付を作成し、返します。

この関数は v3.1 からサポートされています。

## 構文

```Haskell
DATE makedate(INT year, INT dayOfYear);
```

## パラメータ

- `year`: 0 から 9999 の範囲です。この範囲を超えると NULL が返されます。サポートされているデータ型は INT です。
- `dayOfYear`: 年の日数です。サポートされているデータ型は INT です。関数 [dayofyear](./dayofyear.md) と同じ意味を維持するために、この数が 366 を超えるか、閏年でない年に 366 である場合、それは年の日数ではありません。

## 戻り値

指定された年の dayOfYear 番目の日の日付を返します。

- `year` は [0,9999] の範囲内でなければなりません。それ以外の場合は NULL が返されます。
- `dayOfYear` は 1 からその年の日数（通常年は 365 日、閏年は 366 日）の間でなければなりません。それ以外の場合は NULL が返されます。
- どちらかの入力パラメータが NULL の場合も、結果は NULL です。

## 例

```Plain Text
mysql> select makedate(2023,0);
+-------------------+
| makedate(2023, 0) |
+-------------------+
| NULL              |
+-------------------+

mysql> select makedate(2023,32);
+--------------------+
| makedate(2023, 32) |
+--------------------+
| 2023-02-01         |
+--------------------+

mysql> select makedate(2023,365);
+---------------------+
| makedate(2023, 365) |
+---------------------+
| 2023-12-31          |
+---------------------+

mysql> select makedate(2023,366);
+---------------------+
| makedate(2023, 366) |
+---------------------+
| NULL                |
+---------------------+

mysql> select makedate(9999,365);
+---------------------+
| makedate(9999, 365) |
+---------------------+
| 9999-12-31          |
+---------------------+

mysql> select makedate(10000,1);
+--------------------+
| makedate(10000, 1) |
+--------------------+
| NULL               |
+--------------------+
```

## キーワード

MAKEDATE,MAKE