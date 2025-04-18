---
displayed_sidebar: docs
---

# array_slice

配列のスライスを返します。この関数は、`offset` で指定された位置から `input` から `length` 要素を切り取ります。

## 構文

```Haskell
array_slice(input, offset, length)
```

## パラメータ

- `input`: スライスしたい配列。この関数は、次のタイプの配列要素をサポートします: BOOLEAN, TINYINT, SMALLINT, INT, BIGINT, LARGEINT, FLOAT, DOUBLE, VARCHAR, DECIMALV2, DATETIME, DATE, および JSON。**JSON はバージョン 2.5 からサポートされています。**

- `offset`: 要素を切り取る開始位置。有効な値は `1` から始まります。BIGINT 値である必要があります。

- `length`: 切り取りたいスライスの長さ。BIGINT 値である必要があります。

## 戻り値

`input` パラメータで指定された配列と同じデータ型の配列を返します。

## 使用上の注意

- offset は 1 から始まります。
- 指定された長さが実際に切り取れる要素数を超える場合、一致するすべての要素が返されます。例 4 を参照してください。

## 例

例 1: 3 番目の要素から 2 つの要素を切り取ります。

```Plain
mysql> select array_slice([1,2,4,5,6], 3, 2) as res;
+-------+
| res   |
+-------+
| [4,5] |
+-------+
```

例 2: 最初の要素から 2 つの要素を切り取ります。

```Plain
mysql> select array_slice(["sql","storage","query","execute"], 1, 2) as res;
+-------------------+
| res               |
+-------------------+
| ["sql","storage"] |
+-------------------+
```

例 3: Null 要素は通常の値として扱われます。

```Plain
mysql> select array_slice([57.73,97.32,128.55,null,324.2], 3, 3) as res;
+---------------------+
| res                 |
+---------------------+
| [128.55,null,324.2] |
+---------------------+
```

例 4: 3 番目の要素から 5 つの要素を切り取ります。

この関数は 5 つの要素を切り取ることを意図していますが、3 番目の要素からは 3 つの要素しかありません。その結果、これらの 3 つの要素がすべて返されます。

```Plain
mysql> select array_slice([57.73,97.32,128.55,null,324.2], 3, 5) as res;
+---------------------+
| res                 |
+---------------------+
| [128.55,null,324.2] |
+---------------------+
```