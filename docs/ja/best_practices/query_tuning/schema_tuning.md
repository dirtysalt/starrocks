---
displayed_sidebar: docs
sidebar_position: 50
---

# スキーマチューニングレシピ

このドキュメントは、StarRocks における効果的なスキーマ設計と基礎的なテーブル選択を通じて、クエリパフォーマンスを最適化するための実用的なヒントとベストプラクティスを提供します。異なるテーブルタイプ、キー、分散戦略がクエリ実行にどのように影響するかを理解することで、速度とリソース効率の両方を大幅に向上させることができます。これらのガイドラインを使用して、スキーマ設計、テーブルタイプの選択、StarRocks 環境のチューニングにおいて情報に基づいた意思決定を行い、高性能な分析を実現してください。

## テーブルタイプの選択

StarRocks は、重複キーテーブル、集計テーブル、ユニークキーテーブル、主キーテーブルの4つのテーブルタイプをサポートしています。これらはすべて KEY によってソートされます。

- `AGGREGATE KEY`: 同じ AGGREGATE KEY を持つレコードが StarRocks にロードされると、古いレコードと新しいレコードが集計されます。現在、集計テーブルは以下の集計関数をサポートしています: SUM, MIN, MAX, REPLACE。集計テーブルはデータを事前に集計し、ビジネスステートメントや多次元分析を容易にします。
- `DUPLICATE KEY`: 重複キーテーブルにはソートキーを指定するだけで済みます。同じ DUPLICATE KEY を持つレコードは同時に存在します。事前にデータを集計しない分析に適しています。
- `UNIQUE KEY`: 同じ UNIQUE KEY を持つレコードが StarRocks にロードされると、新しいレコードが古いレコードを上書きします。ユニークキーテーブルは REPLACE 関数を持つ集計テーブルに似ています。どちらも定期的な更新を伴う分析に適しています。
- `PRIMARY KEY`: 主キーテーブルはレコードの一意性を保証し、リアルタイムの更新を可能にします。

```sql
CREATE TABLE site_visit
(
    siteid      INT,
    city        SMALLINT,
    username    VARCHAR(32),
    pv BIGINT   SUM DEFAULT '0'
)
AGGREGATE KEY(siteid, city, username)
DISTRIBUTED BY HASH(siteid);

CREATE TABLE session_data
(
    visitorid   SMALLINT,
    sessionid   BIGINT,
    visittime   DATETIME,
    city        CHAR(20),
    province    CHAR(20),
    ip          varchar(32),
    browser      CHAR(20),
    url         VARCHAR(1024)
)
DUPLICATE KEY(visitorid, sessionid)
DISTRIBUTED BY HASH(sessionid, visitorid);

CREATE TABLE sales_order
(
    orderid     BIGINT,
    status      TINYINT,
    username    VARCHAR(32),
    amount      BIGINT DEFAULT '0'
)
UNIQUE KEY(orderid)
DISTRIBUTED BY HASH(orderid);

CREATE TABLE sales_order
(
    orderid     BIGINT,
    status      TINYINT,
    username    VARCHAR(32),
    amount      BIGINT DEFAULT '0'
)
PRIMARY KEY(orderid)
DISTRIBUTED BY HASH(orderid);
```

## コロケートテーブル

クエリを高速化するために、同じ分散を持つテーブルは共通のバケッティングカラムを使用できます。この場合、`join` 操作中にデータがクラスター間で転送されることなく、ローカルでジョインできます。

```sql
CREATE TABLE colocate_table
(
    visitorid   SMALLINT,
    sessionid   BIGINT,
    visittime   DATETIME,
    city        CHAR(20),
    province    CHAR(20),
    ip          varchar(32),
    browser      CHAR(20),
    url         VARCHAR(1024)
)
DUPLICATE KEY(visitorid, sessionid)
DISTRIBUTED BY HASH(sessionid, visitorid)
PROPERTIES(
    "colocate_with" = "group1"
);
```

コロケートジョインとレプリカ管理の詳細については、 [Colocate join](../../using_starrocks/Colocate_join.md) を参照してください。

## フラットテーブルとスタースキーマ

StarRocks はスタースキーマをサポートしており、フラットテーブルよりも柔軟にモデリングできます。モデリング中にフラットテーブルを置き換えるビューを作成し、複数のテーブルからデータをクエリしてクエリを高速化できます。

フラットテーブルには以下の欠点があります：

- フラットテーブルは通常、多数のディメンションを含むため、ディメンションの更新コストが高い。ディメンションが更新されるたびに、テーブル全体を更新する必要があります。更新頻度が増すと状況は悪化します。
- フラットテーブルは追加の開発作業、ストレージスペース、データバックフィリング操作を必要とするため、メンテナンスコストが高い。
- フラットテーブルには多くのフィールドがあり、集計テーブルにはさらに多くのキー フィールドが含まれる可能性があるため、データ取り込みコストが高い。データロード中に、より多くのフィールドをソートする必要があり、データロードが長引きます。

クエリの同時実行性や低レイテンシーに対する要求が高い場合は、フラットテーブルを使用することもできます。

## パーティションとバケット

StarRocks は2レベルのパーティショニングをサポートしています。第1レベルは RANGE パーティションで、第2レベルは HASH バケットです。

- RANGE パーティション: RANGE パーティションはデータを異なる間隔に分割するために使用されます（元のテーブルを複数のサブテーブルに分割することと理解できます）。ほとんどのユーザーは時間でパーティションを設定することを選択し、次の利点があります：

  - ホットデータとコールドデータを区別しやすい
  - StarRocks の階層型ストレージ (SSD + SATA) を活用できる
  - パーティションによるデータ削除が高速

- HASH バケット: ハッシュ値に基づいてデータを異なるバケットに分割します。

  - データの偏りを避けるために、識別度の高い列をバケッティングに使用することをお勧めします。
  - データの復旧を容易にするために、各バケット内の圧縮データのサイズを 100 MB から 1 GB の間に保つことをお勧めします。テーブルを作成するかパーティションを追加する際に、適切なバケット数を設定することをお勧めします。
  - ランダムバケット法は推奨されません。テーブルを作成する際には、HASH バケッティングカラムを明示的に指定する必要があります。

## スパースインデックスとブルームフィルターインデックス

StarRocks はデータを順序付けて保存し、1024 行の粒度でスパースインデックスを構築します。

StarRocks はスキーマ内の固定長プレフィックス（現在は 36 バイト）をスパースインデックスとして選択します。

テーブルを作成する際には、一般的なフィルターフィールドをスキーマ宣言の先頭に配置することをお勧めします。識別度とクエリ頻度が最も高いフィールドを最初に配置する必要があります。

VARCHAR フィールドはスパースインデックスの最後に配置する必要があります。インデックスは VARCHAR フィールドから切り捨てられるためです。VARCHAR フィールドが最初に現れると、インデックスは 36 バイト未満になる可能性があります。

上記の `site_visit` テーブルを例にとります。このテーブルには 4 つの列があります: `siteid, city, username, pv`。ソートキーには `siteid, city, username` の 3 つの列が含まれ、それぞれ 4, 2, 32 バイトを占めます。したがって、プレフィックスインデックス（スパースインデックス）は `siteid + city + username` の最初の 30 バイトになります。

スパースインデックスに加えて、StarRocks はブルームフィルターインデックスも提供しており、識別度の高い列をフィルタリングするのに効果的です。VARCHAR フィールドを他のフィールドの前に配置したい場合は、ブルームフィルターインデックスを作成できます。

## インバーテッドインデックス

StarRocks はビットマップインデックス技術を採用しており、重複キーテーブルのすべての列と集計テーブルおよびユニークキーテーブルのキー列に適用できるインバーテッドインデックスをサポートしています。ビットマップインデックスは、性別、都市、州などの小さな値範囲を持つ列に適しています。範囲が拡大するにつれて、ビットマップインデックスも並行して拡大します。

## マテリアライズドビュー（ロールアップ）

ロールアップは、元のテーブル（ベーステーブル）のマテリアライズドインデックスです。ロールアップを作成する際には、ベーステーブルの一部の列のみをスキーマとして選択でき、スキーマ内のフィールドの順序はベーステーブルとは異なる場合があります。以下はロールアップの使用例です：

- ベーステーブルのデータ集約が高くない場合、ベーステーブルには識別度の高いフィールドが含まれているためです。この場合、一部の列を選択してロールアップを作成することを検討できます。上記の `site_visit` テーブルを例にとります：

  ```sql
  site_visit(siteid, city, username, pv)
  ```

  `siteid` はデータ集約が不十分になる可能性があります。都市ごとに PV を頻繁に計算する必要がある場合は、`city` と `pv` のみを含むロールアップを作成できます。

  ```sql
  ALTER TABLE site_visit ADD ROLLUP rollup_city(city, pv);
  ```

- ベーステーブルのプレフィックスインデックスがヒットしない場合、ベーステーブルの構築方法がすべてのクエリパターンをカバーできないためです。この場合、列の順序を調整するためにロールアップを作成することを検討できます。上記の `session_data` テーブルを例にとります：

  ```sql
  session_data(visitorid, sessionid, visittime, city, province, ip, browser, url)
  ```

  `visitorid` に加えて `browser` と `province` で訪問を分析する必要がある場合は、別のロールアップを作成できます：

  ```sql
  ALTER TABLE session_data
  ADD ROLLUP rollup_browser(browser,province,ip,url)
  DUPLICATE KEY(browser,province);
  ```

## スキーマ変更

StarRocks では、スキーマを変更する方法が3つあります：ソート済みスキーマ変更、直接スキーマ変更、リンクスキーマ変更。

- ソート済みスキーマ変更: 列のソートを変更し、データを再配置します。たとえば、ソート済みスキーマで列を削除するとデータが再配置されます。

  `ALTER TABLE site_visit DROP COLUMN city;`

- 直接スキーマ変更: データを再配置せずに変換します。たとえば、列の型を変更したり、スパースインデックスに列を追加したりします。

  `ALTER TABLE site_visit MODIFY COLUMN username varchar(64);`

- リンクスキーマ変更: データを変換せずに変更を完了します。たとえば、列を追加します。

  `ALTER TABLE site_visit ADD COLUMN click bigint SUM default '0';`

  テーブルを作成する際には、スキーマ変更を加速するために適切なスキーマを選択することをお勧めします。