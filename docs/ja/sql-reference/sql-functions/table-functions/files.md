---
displayed_sidebar: docs
---

# FILES

リモートストレージ内のデータファイルを定義します。以下の用途に使用できます:

- [リモートストレージシステムからデータをロードまたはクエリする](#files-for-loading)
- [データをリモートストレージシステムにアンロードする](#files-for-unloading)

現在、FILES() 関数は以下のデータソースとファイル形式をサポートしています:

- **データソース:**
  - HDFS
  - AWS S3
  - Google Cloud Storage
  - その他の S3 互換ストレージシステム
  - Microsoft Azure Blob Storage
  - NFS(NAS)
- **ファイル形式:**
  - Parquet
  - ORC (バージョン 3.3 以降にサポート)
  - CSV (バージョン 3.3 以降にサポート)
  - Avro (バージョン 3.4.4 以降にサポートされ、ローディングのみ)

バージョン 3.2 以降、FILES() は基本データ型に加えて、ARRAY、JSON、MAP、STRUCT などの複雑なデータ型もサポートしています。

## FILES() for loading

バージョン 3.1.0 以降、StarRocks はテーブル関数 FILES() を使用してリモートストレージに読み取り専用ファイルを定義することをサポートしています。ファイルのパス関連プロパティを使用してリモートストレージにアクセスし、ファイル内のデータのテーブルスキーマを推測し、データ行を返します。[SELECT](../../sql-statements/table_bucket_part_index/SELECT.md) を使用してデータ行を直接クエリしたり、[INSERT](../../sql-statements/loading_unloading/INSERT.md) を使用して既存のテーブルにデータ行をロードしたり、[CREATE TABLE AS SELECT](../../sql-statements/table_bucket_part_index/CREATE_TABLE_AS_SELECT.md) を使用して新しいテーブルを作成し、データ行をロードすることができます。バージョン 3.3.4 以降、FILES() を使用して [DESC](../../sql-statements/table_bucket_part_index/DESCRIBE.md) を使用してデータファイルのスキーマを表示することもできます。

### 構文

```SQL
FILES( data_location , [data_format] [, schema_detect ] [, StorageCredentialParams ] [, columns_from_path ] [, list_files_only ] [, list_recursively])
```

### パラメータ

すべてのパラメータは `"key" = "value"` ペアで指定します。

#### data_location

ファイルにアクセスするための URI です。

パスまたはファイルを指定できます。たとえば、このパラメータを `"hdfs://<hdfs_host>:<hdfs_port>/user/data/tablename/20210411"` と指定して、HDFS サーバー上のパス `/user/data/tablename` から `20210411` という名前のデータファイルをロードできます。

また、ワイルドカード `?`, `*`, `[]`, `{}`, または `^` を使用して複数のデータファイルの保存パスとしてこのパラメータを指定することもできます。たとえば、このパラメータを `"hdfs://<hdfs_host>:<hdfs_port>/user/data/tablename/*/*"` または `"hdfs://<hdfs_host>:<hdfs_port>/user/data/tablename/dt=202104*/*"` と指定して、HDFS サーバー上のパス `/user/data/tablename` のすべてのパーティションまたは `202104` パーティションのみからデータファイルをロードできます。

:::note

ワイルドカードは中間パスを指定するためにも使用できます。

:::

- HDFS にアクセスするには、このパラメータを次のように指定する必要があります:

  ```SQL
  "path" = "hdfs://<hdfs_host>:<hdfs_port>/<hdfs_path>"
  -- 例: "path" = "hdfs://127.0.0.1:9000/path/file.parquet"
  ```

- AWS S3 にアクセスするには:

  - S3 プロトコルを使用する場合、このパラメータを次のように指定する必要があります:

    ```SQL
    "path" = "s3://<s3_path>"
    -- 例: "path" = "s3://path/file.parquet"
    ```

  - S3A プロトコルを使用する場合、このパラメータを次のように指定する必要があります:

    ```SQL
    "path" = "s3a://<s3_path>"
    -- 例: "path" = "s3a://path/file.parquet"
    ```

- Google Cloud Storage にアクセスするには、このパラメータを次のように指定する必要があります:

  ```SQL
  "path" = "s3a://<gcs_path>"
  -- 例: "path" = "s3a://path/file.parquet"
  ```

- Azure Blob Storage にアクセスするには:

  - ストレージアカウントが HTTP 経由でのアクセスを許可している場合、このパラメータを次のように指定する必要があります:

    ```SQL
    "path" = "wasb://<container>@<storage_account>.blob.core.windows.net/<blob_path>"
    -- 例: "path" = "wasb://testcontainer@testaccount.blob.core.windows.net/path/file.parquet"
    ```
  
  - ストレージアカウントが HTTPS 経由でのアクセスを許可している場合、このパラメータを次のように指定する必要があります:

    ```SQL
    "path" = "wasbs://<container>@<storage_account>.blob.core.windows.net/<blob_path>"
    -- 例: "path" = "wasbs://testcontainer@testaccount.blob.core.windows.net/path/file.parquet"
    ```

- NFS(NAS) にアクセスするには:

  ```SQL
  "path" = "file:///<absolute_path>"
  -- 例: "path" = "file:///home/ubuntu/parquetfile/file.parquet"
  ```

  :::note

  `file://` プロトコルを介して NFS 内のファイルにアクセスするには、各 BE または CN ノードの同じディレクトリに NAS デバイスを NFS としてマウントする必要があります。

  :::

#### data_format

データファイルの形式です。有効な値:
- `parquet`
- `orc` (バージョン 3.3 以降にサポート)
- `csv` (バージョン 3.3 以降にサポート)
- `avro` (バージョン 3.4.4 以降にサポートされ、ローディングのみ)

特定のデータファイル形式に対して詳細なオプションを設定する必要があります。

`list_files_only` が `true` に設定されている場合、`data_format` を指定する必要はありません。

##### Parquet

Parquet フォーマットの例：

```SQL
"format"="parquet",
"parquet.use_legacy_encoding" = "true",   -- アンロード専用
"parquet.version" = "2.6"                 -- アンロード専用
```

###### parquet.use_legacy_encoding

DATETIME および DECIMAL データ型に使用されるエンコード技術を制御する。有効な値： 有効な値: `true` および `false` (デフォルト)。このプロパティはデータのアンロードでのみサポートされる。

この項目が `true` に設定されている場合：

- DATETIME 型の場合、システムは `INT96` エンコーディングを使用する。
- DECIMAL 型の場合、システムは `fixed_len_byte_array` エンコーディングを使用する。

この項目が `false` に設定されている場合：

- DATETIME 型の場合、システムは `INT64` エンコーディングを使用する。
- DECIMAL 型の場合、システムは `INT32` または `INT64` エンコーディングを使用する。

:::note

DECIMAL 128 データ型では、`fixed_len_byte_array` エンコーディングのみが使用可能です。`parquet.use_legacy_encoding` は有効になりません。

:::

###### parquet.version

システムがデータをアンロードする Parquet のバージョンを制御します。v3.4.6 以降でサポートされています。有効な値：`1.0`、`2.4`、および`2.6`（デフォルト）。このプロパティはデータのアンロードでのみサポートされる。

##### CSV

CSV 形式の例:

```SQL
"format"="csv",
"csv.column_separator"="\\t",
"csv.enclose"='"',
"csv.skip_header"="1",
"csv.escape"="\\"
```

###### csv.column_separator

データファイルが CSV 形式の場合に使用される列区切り文字を指定します。このパラメータを指定しない場合、デフォルトでタブ (`\\t`) が使用されます。このパラメータで指定する列区切り文字は、実際にデータファイルで使用されている列区切り文字と同じでなければなりません。そうでない場合、データ品質が不十分なためロードジョブが失敗します。

Files() を使用するタスクは MySQL プロトコルに従って送信されます。StarRocks と MySQL はどちらもロードリクエスト内の文字をエスケープします。したがって、列区切り文字がタブのような不可視文字の場合、列区切り文字の前にバックスラッシュ (`\`) を追加する必要があります。たとえば、列区切り文字が `\t` の場合は `\\t` を入力し、列区切り文字が `\n` の場合は `\\n` を入力する必要があります。Apache Hive™ ファイルは `\x01` を列区切り文字として使用するため、データファイルが Hive からのものである場合は `\\x01` を入力する必要があります。

> **NOTE**
>
> - CSV データの場合、長さが 50 バイトを超えない UTF-8 文字列（カンマ (,)、タブ、パイプ (|) など）をテキストデリミタとして使用できます。
> - Null 値は `\N` を使用して表されます。たとえば、データファイルが 3 列で構成されており、そのデータファイルのレコードが最初と 3 番目の列にデータを持ち、2 番目の列にデータがない場合、この状況では 2 番目の列に `\N` を使用して null 値を表す必要があります。つまり、レコードは `a,\N,b` としてコンパイルされる必要があり、`a,,b` ではありません。`a,,b` はレコードの 2 番目の列が空の文字列を持っていることを示します。

###### csv.enclose

データファイルが CSV 形式の場合、RFC4180 に従ってフィールド値をラップするために使用される文字を指定します。タイプ: 1 バイト文字。デフォルト値: `NONE`。最も一般的な文字はシングルクォーテーションマーク (`'`) とダブルクォーテーションマーク (`"`) です。

`enclose` で指定された文字でラップされたすべての特殊文字（行区切り文字や列区切り文字を含む）は通常の記号と見なされます。StarRocks は、`enclose` で指定された文字として任意の 1 バイト文字を指定できるため、RFC4180 よりも多くのことができます。

フィールド値に `enclose` で指定された文字が含まれている場合、同じ文字を使用してその `enclose` で指定された文字をエスケープできます。たとえば、`enclose` を `"` に設定し、フィールド値が `a "quoted" c` の場合、このフィールド値をデータファイルに `"a ""quoted"" c"` として入力できます。

###### csv.skip_header

CSV 形式のデータでスキップするヘッダ行の数を指定します。タイプ: INTEGER。デフォルト値: `0`。

一部の CSV 形式のデータファイルでは、メタデータ（列名や列データ型など）を定義するために複数のヘッダ行が使用されます。`skip_header` パラメータを設定することで、StarRocks がこれらのヘッダ行をスキップできるようになります。たとえば、このパラメータを `1` に設定すると、StarRocks はデータロード中にデータファイルの最初の行をスキップします。

データファイル内のヘッダ行は、ロードステートメントで指定した行区切り文字を使用して区切られている必要があります。

###### csv.escape

行区切り文字、列区切り文字、エスケープ文字、`enclose` で指定された文字など、さまざまな特殊文字をエスケープするために使用される文字を指定します。これらは StarRocks によって通常の文字と見なされ、フィールド値の一部として解析されます。タイプ: 1 バイト文字。デフォルト値: `NONE`。最も一般的な文字はスラッシュ (`\`) で、SQL ステートメントではダブルスラッシュ (`\\`) として記述する必要があります。

> **NOTE**
>
> `escape` で指定された文字は、各ペアの `enclose` で指定された文字の内側と外側の両方に適用されます。
> 以下の 2 つの例があります:
> - `enclose` を `"` に設定し、`escape` を `\` に設定した場合、StarRocks は `"say \"Hello world\""` を `say "Hello world"` として解析します。
> - 列区切り文字がカンマ (`,`) の場合、`escape` を `\` に設定すると、StarRocks は `a, b\, c` を 2 つの別々のフィールド値 `a` と `b, c` に解析します。

#### schema_detect

バージョン 3.2 以降、FILES() は同じバッチのデータファイルの自動スキーマ検出と統合をサポートしています。StarRocks は最初にバッチ内のランダムなデータファイルの特定のデータ行をサンプリングすることによってデータのスキーマを検出します。次に、StarRocks はバッチ内のすべてのデータファイルから列を統合します。

サンプリングルールは次のパラメータを使用して設定できます:

- `auto_detect_sample_files`: 各バッチでサンプリングするランダムなデータファイルの数。範囲: [0, + ∞]。デフォルト: `1`。
- `auto_detect_sample_rows`: 各サンプリングされたデータファイルでスキャンするデータ行の数。範囲: [0, + ∞]。デフォルト: `500`。

サンプリング後、StarRocks は次のルールに従ってすべてのデータファイルから列を統合します:

- 異なる列名またはインデックスを持つ列は、それぞれ個別の列として識別され、最終的にすべての個別の列の統合が返されます。
- 同じ列名を持つが異なるデータ型を持つ列は、同じ列として識別されますが、相対的に細かい粒度レベルで一般的なデータ型を持ちます。たとえば、ファイル A の列 `col1` が INT で、ファイル B の列 `col1` が DECIMAL の場合、返される列には DOUBLE が使用されます。
  - すべての整数列は、全体的に粗い粒度レベルで整数型として統合されます。
  - 整数列と FLOAT 型列は、DECIMAL 型として統合されます。
  - 他の型を統合するために文字列型が使用されます。
- 一般的に、STRING 型はすべてのデータ型を統合するために使用できます。

例 5 を参照してください。

StarRocks がすべての列を統合できない場合、エラー情報とすべてのファイルスキーマを含むスキーマエラーレポートを生成します。

> **CAUTION**
>
> 単一バッチ内のすべてのデータファイルは、同じファイル形式でなければなりません。

##### ターゲットテーブルスキーマチェックのプッシュダウン

バージョン 3.4.0 以降、システムはターゲットテーブルスキーマチェックを FILES() のスキャンステージにプッシュダウンすることをサポートしています。

FILES() のスキーマ検出は完全に厳密ではありません。たとえば、CSV ファイル内の任意の整数列は、関数がファイルを読み取るときに BIGINT 型として推測およびチェックされます。この場合、ターゲットテーブルの対応する列が TINYINT 型の場合、BIGINT 型を超える CSV データレコードはフィルタリングされず、暗黙的に NULL で埋められます。

この問題に対処するために、システムは動的 FE 構成項目 `files_enable_insert_push_down_schema` を導入し、ターゲットテーブルスキーマチェックを FILES() のスキャンステージにプッシュダウンするかどうかを制御します。`files_enable_insert_push_down_schema` を `true` に設定すると、ターゲットテーブルスキーマチェックに失敗したデータレコードがファイル読み取り時にフィルタリングされます。

##### 異なるスキーマを持つファイルの統合

バージョン 3.4.0 以降、システムは異なるスキーマを持つファイルの統合をサポートし、デフォルトでは存在しない列がある場合にエラーが返されます。プロパティ `fill_mismatch_column_with` を `null` に設定することで、システムがエラーを返す代わりに存在しない列に NULL 値を割り当てることを許可できます。

`fill_mismatch_column_with`: 異なるスキーマを持つファイルを統合する際に存在しない列が検出された場合のシステムの動作。有効な値:
- `none`: 存在しない列が検出された場合にエラーが返されます。
- `null`: 存在しない列に NULL 値が割り当てられます。

たとえば、読み取るファイルが Hive テーブルの異なるパーティションからのものであり、新しいパーティションでスキーマ変更が行われた場合、新旧のパーティションの両方を読み取る際に `fill_mismatch_column_with` を `null` に設定すると、システムは新旧パーティションファイルのスキーマを統合し、存在しない列に NULL 値を割り当てます。

システムは Parquet および ORC ファイルのスキーマを列名に基づいて統合し、CSV ファイルのスキーマを列の位置（順序）に基づいて統合します。

##### Parquet から STRUCT 型を推測する

バージョン 3.4.0 以降、FILES() は Parquet ファイルから STRUCT 型データを推測することをサポートしています。

#### StorageCredentialParams

StarRocks がストレージシステムにアクセスするために使用する認証情報。

StarRocks は現在、HDFS へのシンプル認証、AWS S3 および GCS への IAM ユーザー認証、Azure Blob Storage への共有キー、SAS トークン、マネージドアイデンティティ、サービスプリンシパル認証をサポートしています。

##### HDFS

- HDFS にシンプル認証を使用してアクセスする:

  ```SQL
  "hadoop.security.authentication" = "simple",
  "username" = "xxxxxxxxxx",
  "password" = "yyyyyyyyyy"
  ```

  | **Key**                        | **Required** | **Description**                                              |
  | ------------------------------ | ------------ | ------------------------------------------------------------ |
  | hadoop.security.authentication | No           | 認証方法。有効な値: `simple` (デフォルト)。`simple` はシンプル認証を表し、認証が不要であることを意味します。 |
  | username                       | Yes          | HDFS クラスターの NameNode にアクセスするために使用するアカウントのユーザー名。 |
  | password                       | Yes          | HDFS クラスターの NameNode にアクセスするために使用するアカウントのパスワード。 |

- HDFS に Kerberos 認証を使用してアクセスする:

  現在、FILES() は、**fe/conf**、**be/conf**、および **cn/conf** ディレクトリの下に置かれた設定ファイル **hdfs-site.xml** を介してのみ、HDFS での Kerberos 認証をサポートしています。

  また、各 FE 設定ファイル **fe.conf**、BE 設定ファイル **be.conf**、CN 設定ファイル **cn.conf** の設定項目 `JAVA_OPTS` に以下のオプションを追加する必要がある：

  ```Plain
  # Kerberos 設定ファイルを保存するローカルパスを指定する。
  -Djava.security.krb5.conf=<path_to_kerberos_conf_file>
  ```

  例:

  ```Properties
  JAVA_OPTS="-Xlog:gc*:${LOG_DIR}/be.gc.log.$DATE:time -XX:ErrorFile=${LOG_DIR}/hs_err_pid%p.log -Djava.security.krb5.conf=/etc/krb5.conf"
  ```

  また、各 FE、BE、CN ノードで `kinit` コマンドを実行して、Key Distribution Center（KDC）から Ticket Granting Ticket（TGT）を取得する必要がある。

  ```Bash
  kinit -kt <path_to_keytab_file> <principal>
  ```

  このコマンドを実行するには、使用するプリンシパルが HDFS クラスタへの書き込みアクセス権を持っている必要がある。さらに、このコマンドのために crontab を設定し、特定の間隔でタスクをスケジューリングし、認証が失効するのを防ぐ必要がある。

  例:

  ```Bash
  # TGT を6時間ごとに更新する。
  0 */6 * * * kinit -kt sr.keytab sr/test.starrocks.com@STARROCKS.COM > /tmp/kinit.log
  ```

- HA モードを有効にして HDFS にアクセスする：

  現在、FILES()は、**fe/conf**、**be/conf**、および **cn/conf** ディレクトリの下に置かれた設定ファイル **hdfs-site.xml** を介してのみ、HA モードを有効にした HDFS へのアクセスをサポートしています。

##### AWS S3

ストレージシステムとして AWS S3 を選択する場合、次のいずれかのアクションを実行します：

- インスタンスプロファイルベースの認証方法を選択するには、`StorageCredentialParams` を次のように構成します：

  ```SQL
  "aws.s3.use_instance_profile" = "true",
  "aws.s3.region" = "<aws_s3_region>"
  ```

- アサインドロールベースの認証方法を選択するには、`StorageCredentialParams` を次のように構成します：

  ```SQL
  "aws.s3.use_instance_profile" = "true",
  "aws.s3.iam_role_arn" = "<iam_role_arn>",
  "aws.s3.region" = "<aws_s3_region>"
  ```

- IAM ユーザーベースの認証方法を選択するには、`StorageCredentialParams` を次のように構成します：

  ```SQL
  "aws.s3.use_instance_profile" = "false",
  "aws.s3.access_key" = "<iam_user_access_key>",
  "aws.s3.secret_key" = "<iam_user_secret_key>",
  "aws.s3.region" = "<aws_s3_region>"
  ```

`StorageCredentialParams` に設定する必要があるパラメータは次のように説明されています。

| Parameter                   | Required | Description                                                  |
| --------------------------- | -------- | ------------------------------------------------------------ |
| aws.s3.use_instance_profile | Yes      | 資格情報メソッドのインスタンスプロファイルとアサインドロールを有効にするかどうかを指定します。有効な値: `true` および `false`。デフォルト値: `false`。 |
| aws.s3.iam_role_arn         | No       | AWS S3 バケットに対する権限を持つ IAM ロールの ARN。AWS S3 にアクセスするための資格情報メソッドとしてアサインドロールを選択する場合、このパラメータを指定する必要があります。 |
| aws.s3.region               | Yes      | AWS S3 バケットが存在するリージョン。例: `us-west-1`。        |
| aws.s3.access_key           | No       | IAM ユーザーのアクセスキー。AWS S3 にアクセスするための資格情報メソッドとして IAM ユーザーを選択する場合、このパラメータを指定する必要があります。 |
| aws.s3.secret_key           | No       | IAM ユーザーのシークレットキー。AWS S3 にアクセスするための資格情報メソッドとして IAM ユーザーを選択する場合、このパラメータを指定する必要があります。 |

AWS S3 へのアクセスのための認証方法の選択方法と AWS IAM コンソールでのアクセス制御ポリシーの設定方法については、[Authentication parameters for accessing AWS S3](../../../integrations/authenticate_to_aws_resources.md#authentication-parameters-for-accessing-aws-s3) を参照してください。

##### Google GCS

ストレージシステムとして Google GCS を選択する場合、次のいずれかのアクションを実行します：

- VM ベースの認証方法を選択するには、`StorageCredentialParams` を次のように構成します：

  ```SQL
  "gcp.gcs.use_compute_engine_service_account" = "true"
  ```

  `StorageCredentialParams` に設定する必要があるパラメータは次のように説明されています。

  | **Parameter**                              | **Default value** | **Value** **example** | **Description**                                              |
  | ------------------------------------------ | ----------------- | --------------------- | ------------------------------------------------------------ |
  | gcp.gcs.use_compute_engine_service_account | false             | true                  | Compute Engine にバインドされているサービスアカウントを直接使用するかどうかを指定します。 |

- サービスアカウントベースの認証方法を選択するには、`StorageCredentialParams` を次のように構成します：

  ```SQL
  "gcp.gcs.service_account_email" = "<google_service_account_email>",
  "gcp.gcs.service_account_private_key_id" = "<google_service_private_key_id>",
  "gcp.gcs.service_account_private_key" = "<google_service_private_key>"
  ```

  `StorageCredentialParams` に設定する必要があるパラメータは次のように説明されています。

  | **Parameter**                          | **Default value** | **Value** **example**                                        | **Description**                                              |
  | -------------------------------------- | ----------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
  | gcp.gcs.service_account_email          | ""                | `"user@hello.iam.gserviceaccount.com"` | サービスアカウントの作成時に生成された JSON ファイルのメールアドレス。 |
  | gcp.gcs.service_account_private_key_id | ""                | "61d257bd8479547cb3e04f0b9b6b9ca07af3b7ea"                   | サービスアカウントの作成時に生成された JSON ファイルのプライベートキー ID。 |
  | gcp.gcs.service_account_private_key    | ""                | "-----BEGIN PRIVATE KEY----xxxx-----END PRIVATE KEY-----\n"  | サービスアカウントの作成時に生成された JSON ファイルのプライベートキー。 |

- インパーソネーションベースの認証方法を選択するには、`StorageCredentialParams` を次のように構成します：

  - VM インスタンスにサービスアカウントをインパーソネートさせる：

    ```SQL
    "gcp.gcs.use_compute_engine_service_account" = "true",
    "gcp.gcs.impersonation_service_account" = "<assumed_google_service_account_email>"
    ```

    `StorageCredentialParams` に設定する必要があるパラメータは次のように説明されています。

    | **Parameter**                              | **Default value** | **Value** **example** | **Description**                                              |
    | ------------------------------------------ | ----------------- | --------------------- | ------------------------------------------------------------ |
    | gcp.gcs.use_compute_engine_service_account | false             | true                  | Compute Engine にバインドされているサービスアカウントを直接使用するかどうかを指定します。 |
    | gcp.gcs.impersonation_service_account      | ""                | "hello"               | インパーソネートしたいサービスアカウント。                   |

  - サービスアカウント（メタサービスアカウントと呼ばれる）に別のサービスアカウント（データサービスアカウントと呼ばれる）をインパーソネートさせる：

    ```SQL
    "gcp.gcs.service_account_email" = "<google_service_account_email>",
    "gcp.gcs.service_account_private_key_id" = "<meta_google_service_account_email>",
    "gcp.gcs.service_account_private_key" = "<meta_google_service_account_email>",
    "gcp.gcs.impersonation_service_account" = "<data_google_service_account_email>"
    ```

    `StorageCredentialParams` に設定する必要があるパラメータは次のように説明されています。

    | **Parameter**                          | **Default value** | **Value** **example**                                        | **Description**                                              |
    | -------------------------------------- | ----------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
    | gcp.gcs.service_account_email          | ""                | `"user@hello.iam.gserviceaccount.com"` | メタサービスアカウントの作成時に生成された JSON ファイルのメールアドレス。 |
    | gcp.gcs.service_account_private_key_id | ""                | "61d257bd8479547cb3e04f0b9b6b9ca07af3b7ea"                   | メタサービスアカウントの作成時に生成された JSON ファイルのプライベートキー ID。 |
    | gcp.gcs.service_account_private_key    | ""                | "-----BEGIN PRIVATE KEY----xxxx-----END PRIVATE KEY-----\n"  | メタサービスアカウントの作成時に生成された JSON ファイルのプライベートキー。 |
    | gcp.gcs.impersonation_service_account  | ""                | "hello"                                                      | インパーソネートしたいデータサービスアカウント。             |

##### Azure Blob Storage

- 共有キーを使用して Azure Blob Storage にアクセスする:

  ```SQL
  "azure.blob.shared_key" = "<shared_key>"
  ```

  | **Key**                    | **Required** | **Description**                                              |
  | -------------------------- | ------------ | ------------------------------------------------------------ |
  | azure.blob.shared_key      | Yes          | Azure Blob Storage アカウントにアクセスするために使用できる共有キー。 |

- SAS トークンを使用して Azure Blob Storage にアクセスする:

  ```SQL
  "azure.blob.sas_token" = "<storage_account_SAS_token>"
  ```

  | **Key**                    | **Required** | **Description**                                              |
  | -------------------------- | ------------ | ------------------------------------------------------------ |
  | azure.blob.sas_token       | Yes          | Azure Blob Storage アカウントにアクセスするために使用できる SAS トークン。 |

- マネージドアイデンティティを使用して Azure Blob Storage にアクセスする (v3.4.4 以降でサポート):

  :::note
  - クライアント ID クレデンシャルを持つ User Assigned Managed Identity のみがサポートされる。
  - FE ダイナミック設定 `azure_use_native_sdk` (デフォルト: `true`) は、Managed Identity と Service Principal を使用した認証をシステムに許可するかどうかを制御する。
  :::

  ```SQL
  "azure.blob.oauth2_use_managed_identity" = "true",
  "azure.blob.oauth2_client_id" = "<oauth2_client_id>"
  ```

  | **Key**                                | **Required** | **Description**                                              |
  | -------------------------------------- | ------------ | ------------------------------------------------------------ |
  | azure.blob.oauth2_use_managed_identity | Yes          | Azure Blob Storage アカウントへのアクセスに Managed Identity を使用するかどうか。`true` に設定する。                  |
  | azure.blob.oauth2_client_id            | Yes          | Azure Blob Storage アカウントへのアクセスに使用できるマネージド ID のクライアントID。                |

- サービスプリンシパルを使用して Azure Blob Storage にアクセスする (v3.4.4 以降でサポート):

  :::note
  - Client Secret クレデンシャルのみがサポートされます。
  - FE ダイナミック設定 `azure_use_native_sdk` (デフォルト: `true`) は、Managed Identity と Service Principal を使用した認証をシステムに許可するかどうかを制御する。
  :::

  ```SQL
  "azure.blob.oauth2_client_id" = "<oauth2_client_id>",
  "azure.blob.oauth2_client_secret" = "<oauth2_client_secret>",
  "azure.blob.oauth2_tenant_id" = "<oauth2_tenant_id>"
  ```

  | **Key**                                | **Required** | **Description**                                              |
  | -------------------------------------- | ------------ | ------------------------------------------------------------ |
  | azure.blob.oauth2_client_id            | Yes          | Azure Blob Storage アカウントへのアクセスに使用できるマネージド ID のクライアントID。                    |
  | azure.blob.oauth2_client_secret        | Yes          | Azure Blob Storage アカウントへのアクセスに使用するサービスプリンシパルのクライアントシークレット。          |
  | azure.blob.oauth2_tenant_id            | Yes          | Azure Blob Storage アカウントへのアクセスに使用するサービスプリンシパルのテナント ID。                |

##### Azure Data Lake Storage Gen2

ストレージシステムとして Data Lake Storage Gen2 を選択する場合、次のいずれかのアクションを実行します：

- マネージド ID 認証方法を選択するには、`StorageCredentialParams` を次のように構成します：

  ```SQL
  "azure.adls2.oauth2_use_managed_identity" = "true",
  "azure.adls2.oauth2_tenant_id" = "<service_principal_tenant_id>",
  "azure.adls2.oauth2_client_id" = "<service_client_id>"
  ```

  `StorageCredentialParams` に設定する必要があるパラメータは次のように説明されています。

  | **Parameter**                           | **Required** | **Description**                                              |
  | --------------------------------------- | ------------ | ------------------------------------------------------------ |
  | azure.adls2.oauth2_use_managed_identity | Yes          | マネージド ID 認証方法を有効にするかどうかを指定します。値を `true` に設定します。 |
  | azure.adls2.oauth2_tenant_id            | Yes          | アクセスしたいデータのテナント ID。                          |
  | azure.adls2.oauth2_client_id            | Yes          | マネージド ID のクライアント（アプリケーション）ID。         |

- 共有キー認証方法を選択するには、`StorageCredentialParams` を次のように構成します：

  ```SQL
  "azure.adls2.storage_account" = "<storage_account_name>",
  "azure.adls2.shared_key" = "<storage_account_shared_key>"
  ```

  `StorageCredentialParams` に設定する必要があるパラメータは次のように説明されています。

  | **Parameter**               | **Required** | **Description**                                              |
  | --------------------------- | ------------ | ------------------------------------------------------------ |
  | azure.adls2.storage_account | Yes          | Data Lake Storage Gen2 ストレージアカウントのユーザー名。    |
  | azure.adls2.shared_key      | Yes          | Data Lake Storage Gen2 ストレージアカウントの共有キー。      |

- サービスプリンシパル認証方法を選択するには、`StorageCredentialParams` を次のように構成します：

  ```SQL
  "azure.adls2.oauth2_client_id" = "<service_client_id>",
  "azure.adls2.oauth2_client_secret" = "<service_principal_client_secret>",
  "azure.adls2.oauth2_client_endpoint" = "<service_principal_client_endpoint>"
  ```

  `StorageCredentialParams` に設定する必要があるパラメータは次のように説明されています。

  | **Parameter**                      | **Required** | **Description**                                              |
  | ---------------------------------- | ------------ | ------------------------------------------------------------ |
  | azure.adls2.oauth2_client_id       | Yes          | サービスプリンシパルのクライアント（アプリケーション）ID。   |
  | azure.adls2.oauth2_client_secret   | Yes          | 作成された新しいクライアント（アプリケーション）シークレットの値。 |
  | azure.adls2.oauth2_client_endpoint | Yes          | サービスプリンシパルまたはアプリケーションの OAuth 2.0 トークンエンドポイント（v1）。 |

##### Azure Data Lake Storage Gen1

ストレージシステムとして Data Lake Storage Gen1 を選択する場合、次のいずれかのアクションを実行します：

- マネージドサービス ID 認証方法を選択するには、`StorageCredentialParams` を次のように構成します：

  ```SQL
  "azure.adls1.use_managed_service_identity" = "true"
  ```

  `StorageCredentialParams` に設定する必要があるパラメータは次のように説明されています。

  | **Parameter**                            | **Required** | **Description**                                              |
  | ---------------------------------------- | ------------ | ------------------------------------------------------------ |
  | azure.adls1.use_managed_service_identity | Yes          | マネージドサービス ID 認証方法を有効にするかどうかを指定します。値を `true` に設定します。 |

- サービスプリンシパル認証方法を選択するには、`StorageCredentialParams` を次のように構成します：

  ```SQL
  "azure.adls1.oauth2_client_id" = "<application_client_id>",
  "azure.adls1.oauth2_credential" = "<application_client_credential>",
  "azure.adls1.oauth2_endpoint" = "<OAuth_2.0_authorization_endpoint_v2>"
  ```

  `StorageCredentialParams` に設定する必要があるパラメータは次のように説明されています。

| **Parameter**                 | **Required** | **Description**                                              |
| ----------------------------- | ------------ | ------------------------------------------------------------ |
| azure.adls1.oauth2_client_id  | Yes          | クライアント（アプリケーション）ID。                         |
| azure.adls1.oauth2_credential | Yes          | 作成された新しいクライアント（アプリケーション）シークレットの値。 |
| azure.adls1.oauth2_endpoint   | Yes          | サービスプリンシパルまたはアプリケーションの OAuth 2.0 トークンエンドポイント（v1）。 |

##### その他の S3 互換ストレージシステム

MinIO などの他の S3 互換ストレージシステムを選択する場合、`StorageCredentialParams` を次のように構成します：

```SQL
"aws.s3.enable_ssl" = "false",
"aws.s3.enable_path_style_access" = "true",
"aws.s3.endpoint" = "<s3_endpoint>",
"aws.s3.access_key" = "<iam_user_access_key>",
"aws.s3.secret_key" = "<iam_user_secret_key>"
```

`StorageCredentialParams` に設定する必要があるパラメータは次のように説明されています。

| Parameter                        | Required | Description                                                  |
| -------------------------------- | -------- | ------------------------------------------------------------ |
| aws.s3.enable_ssl                | Yes      | SSL 接続を有効にするかどうかを指定します。有効な値: `true` および `false`。デフォルト値: `true`。 |
| aws.s3.enable_path_style_access  | Yes      | パススタイルの URL アクセスを有効にするかどうかを指定します。有効な値: `true` および `false`。デフォルト値: `false`。MinIO の場合、値を `true` に設定する必要があります。 |
| aws.s3.endpoint                  | Yes      | AWS S3 ではなく、S3 互換ストレージシステムに接続するために使用されるエンドポイント。 |
| aws.s3.access_key                | Yes      | IAM ユーザーのアクセスキー。                                 |
| aws.s3.secret_key                | Yes      | IAM ユーザーのシークレットキー。                             |

#### columns_from_path

バージョン 3.2 以降、StarRocks はファイルパスからキー/値ペアの値を抽出して列の値として使用できます。

```SQL
"columns_from_path" = "<column_name> [, ...]"
```

データファイル **file1** が `/geo/country=US/city=LA/` 形式のパスに保存されているとします。この場合、`columns_from_path` パラメータを `"columns_from_path" = "country, city"` と指定して、ファイルパス内の地理情報を返される列の値として抽出できます。詳細な手順については、例 4 を参照してください。

#### list_files_only

バージョン 3.4.0 以降、FILES() はファイルを読み取る際にファイルのみをリストすることをサポートしています。

```SQL
"list_files_only" = "true"
```

`list_files_only` が `true` に設定されている場合、`data_format` を指定する必要はありません。

詳細については、[Return](#return) を参照してください。

#### list_recursively

StarRocks はファイルとディレクトリを再帰的にリストする `list_recursively` もサポートしています。`list_recursively` は `list_files_only` が `true` に設定されているときのみ有効です。デフォルト値は `false` である。

```SQL
"list_files_only" = "true",
"list_recursively" = "true"
```

`list_files_only` と `list_recursively` の両方が `true` に設定されている場合、StarRocks は以下の処理を行う：

- 指定された `path` がファイルの場合 (具体的に指定されているか、ワイルドカードで表現されているかに関わらず)、StarRocks はそのファイルの情報を表示します。
- 指定された `path` がディレクトリの場合 (具体的に指定されているか、ワイルドカードで表されているか、また `/` でサフィックスされているかどうかに関わらず)、StarRocks はそのディレクトリ以下のすべてのファイルとサブディレクトリを表示します。

詳細については、[Return](#return) を参照してください。

### Return

#### SELECT FROM FILES()

SELECT と共に使用すると、FILES() はファイル内のデータをテーブルとして返します。

- CSV ファイルをクエリする場合、SELECT ステートメントで各列を表すために `$1`, `$2` ... を使用するか、`*` を指定してすべての列からデータを取得できます。

  ```SQL
  SELECT * FROM FILES(
      "path" = "s3://inserttest/csv/file1.csv",
      "format" = "csv",
      "csv.column_separator"=",",
      "csv.row_delimiter"="\n",
      "csv.enclose"='"',
      "csv.skip_header"="1",
      "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
      "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
      "aws.s3.region" = "us-west-2"
  )
  WHERE $1 > 5;
  +------+---------+------------+
  | $1   | $2      | $3         |
  +------+---------+------------+
  |    6 | 0.34413 | 2017-11-25 |
  |    7 | 0.40055 | 2017-11-26 |
  |    8 | 0.42437 | 2017-11-27 |
  |    9 | 0.67935 | 2017-11-27 |
  |   10 | 0.22783 | 2017-11-29 |
  +------+---------+------------+
  5 rows in set (0.30 sec)

  SELECT $1, $2 FROM FILES(
      "path" = "s3://inserttest/csv/file1.csv",
      "format" = "csv",
      "csv.column_separator"=",",
      "csv.row_delimiter"="\n",
      "csv.enclose"='"',
      "csv.skip_header"="1",
      "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
      "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
      "aws.s3.region" = "us-west-2"
  );
  +------+---------+
  | $1   | $2      |
  +------+---------+
  |    1 | 0.71173 |
  |    2 | 0.16145 |
  |    3 | 0.80524 |
  |    4 | 0.91852 |
  |    5 | 0.37766 |
  |    6 | 0.34413 |
  |    7 | 0.40055 |
  |    8 | 0.42437 |
  |    9 | 0.67935 |
  |   10 | 0.22783 |
  +------+---------+
  10 rows in set (0.38 sec)
  ```

- Parquet または ORC ファイルをクエリする場合、SELECT ステートメントで必要な列の名前を直接指定するか、`*` を指定してすべての列からデータを取得できます。

  ```SQL
  SELECT * FROM FILES(
      "path" = "s3://inserttest/parquet/file2.parquet",
      "format" = "parquet",
      "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
      "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
      "aws.s3.region" = "us-west-2"
  )
  WHERE c1 IN (101,105);
  +------+------+---------------------+
  | c1   | c2   | c3                  |
  +------+------+---------------------+
  |  101 |    9 | 2018-05-15T18:30:00 |
  |  105 |    6 | 2018-05-15T18:30:00 |
  +------+------+---------------------+
  2 rows in set (0.29 sec)

  SELECT c1, c3 FROM FILES(
      "path" = "s3://inserttest/parquet/file2.parquet",
      "format" = "parquet",
      "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
      "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
      "aws.s3.region" = "us-west-2"
  );
  +------+---------------------+
  | c1   | c3                  |
  +------+---------------------+
  |  101 | 2018-05-15T18:30:00 |
  |  102 | 2018-05-15T18:30:00 |
  |  103 | 2018-05-15T18:30:00 |
  |  104 | 2018-05-15T18:30:00 |
  |  105 | 2018-05-15T18:30:00 |
  |  106 | 2018-05-15T18:30:00 |
  |  107 | 2018-05-15T18:30:00 |
  |  108 | 2018-05-15T18:30:00 |
  |  109 | 2018-05-15T18:30:00 |
  |  110 | 2018-05-15T18:30:00 |
  +------+---------------------+
  10 rows in set (0.55 sec)
  ```

- `list_files_only` が `true` に設定されたファイルをクエリする場合、システムは `PATH`、`SIZE`、`IS_DIR`（指定されたパスがディレクトリかどうか）、および `MODIFICATION_TIME` を返します。

  ```SQL
  SELECT * FROM FILES(
      "path" = "s3://bucket/*.parquet",
      "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
      "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
      "list_files_only" = "true"
  );
  +-----------------------+------+--------+---------------------+
  | PATH                  | SIZE | IS_DIR | MODIFICATION_TIME   |
  +-----------------------+------+--------+---------------------+
  | s3://bucket/1.parquet | 5221 |      0 | 2024-08-15 20:47:02 |
  | s3://bucket/2.parquet | 5222 |      0 | 2024-08-15 20:54:57 |
  | s3://bucket/3.parquet | 5223 |      0 | 2024-08-20 15:21:00 |
  | s3://bucket/4.parquet | 5224 |      0 | 2024-08-15 11:32:14 |
  +-----------------------+------+--------+---------------------+
  4 rows in set (0.03 sec)
  ```

- `list_files_only` と `list_recursively` を `true` に設定してファイルをクエリすると、システムはファイルとディレクトリを再帰的にリストアップする。

  `s3://bucket/list/` というパスに以下のファイルとサブディレクトリがあるとする：

  ```Plain
  s3://bucket/list/
  ├── basic1.csv
  ├── basic2.csv
  ├── orc0
  │   └── orc1
  │       └── basic_type.orc
  ├── orc1
  │   └── basic_type.orc
  └── parquet
      └── basic_type.parquet
  ```

  ファイルとディレクトリを再帰的にリストアップする：

  ```Plain
  SELECT * FROM FILES(
      "path"="s3://bucket/list/",
      "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
      "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
      "list_files_only" = "true", 
      "list_recursively" = "true"
  );
  +---------------------------------------------+------+--------+---------------------+
  | PATH                                        | SIZE | IS_DIR | MODIFICATION_TIME   |
  +---------------------------------------------+------+--------+---------------------+
  | s3://bucket/list                            |    0 |      1 | 2024-12-24 22:15:59 |
  | s3://bucket/list/basic1.csv                 |   52 |      0 | 2024-12-24 11:35:53 |
  | s3://bucket/list/basic2.csv                 |   34 |      0 | 2024-12-24 11:35:53 |
  | s3://bucket/list/orc0                       |    0 |      1 | 2024-12-24 11:35:53 |
  | s3://bucket/list/orc0/orc1                  |    0 |      1 | 2024-12-24 11:35:53 |
  | s3://bucket/list/orc0/orc1/basic_type.orc   | 1027 |      0 | 2024-12-24 11:35:53 |
  | s3://bucket/list/orc1                       |    0 |      1 | 2024-12-24 22:16:00 |
  | s3://bucket/list/orc1/basic_type.orc        | 1027 |      0 | 2024-12-24 22:16:00 |
  | s3://bucket/list/parquet                    |    0 |      1 | 2024-12-24 11:35:53 |
  | s3://bucket/list/parquet/basic_type.parquet | 2281 |      0 | 2024-12-24 11:35:53 |
  +---------------------------------------------+------+--------+---------------------+
  10 rows in set (0.04 sec)
  ```

  このパスで `orc*` にマッチするファイルとディレクトリを非再帰的にリストアップする：

  ```Plain
  SELECT * FROM FILES(
      "path"="s3://bucket/list/orc*", 
      "list_files_only" = "true", 
      "list_recursively" = "false"
  );
  +--------------------------------------+------+--------+---------------------+
  | PATH                                 | SIZE | IS_DIR | MODIFICATION_TIME   |
  +--------------------------------------+------+--------+---------------------+
  | s3://bucket/list/orc0/orc1           |    0 |      1 | 2024-12-24 11:35:53 |
  | s3://bucket/list/orc1/basic_type.orc | 1027 |      0 | 2024-12-24 22:16:00 |
  +--------------------------------------+------+--------+---------------------+
  2 rows in set (0.03 sec)
  ```

#### DESC FILES()

DESC と共に使用すると、FILES() はファイルのスキーマを返します。

```Plain
DESC FILES(
    "path" = "s3://inserttest/lineorder.parquet",
    "format" = "parquet",
    "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
    "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
    "aws.s3.region" = "us-west-2"
);

+------------------+------------------+------+
| Field            | Type             | Null |
+------------------+------------------+------+
| lo_orderkey      | int              | YES  |
| lo_linenumber    | int              | YES  |
| lo_custkey       | int              | YES  |
| lo_partkey       | int              | YES  |
| lo_suppkey       | int              | YES  |
| lo_orderdate     | int              | YES  |
| lo_orderpriority | varchar(1048576) | YES  |
| lo_shippriority  | int              | YES  |
| lo_quantity      | int              | YES  |
| lo_extendedprice | int              | YES  |
| lo_ordtotalprice | int              | YES  |
| lo_discount      | int              | YES  |
| lo_revenue       | int              | YES  |
| lo_supplycost    | int              | YES  |
| lo_tax           | int              | YES  |
| lo_commitdate    | int              | YES  |
| lo_shipmode      | varchar(1048576) | YES  |
+------------------+------------------+------+
17 rows in set (0.05 sec)
```

`list_files_only` が `true` に設定されたファイルを表示する場合、システムは `PATH`、`SIZE`、`IS_DIR`（指定されたパスがディレクトリかどうか）、および `MODIFICATION_TIME` の `Type` と `Null` プロパティを返します。

```Plain
DESC FILES(
    "path" = "s3://bucket/*.parquet",
    "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
    "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
    "list_files_only" = "true"
);
+-------------------+------------------+------+
| Field             | Type             | Null |
+-------------------+------------------+------+
| PATH              | varchar(1048576) | YES  |
| SIZE              | bigint           | YES  |
| IS_DIR            | boolean          | YES  |
| MODIFICATION_TIME | datetime         | YES  |
+-------------------+------------------+------+
4 rows in set (0.00 sec)
```

## FILES() for unloading

バージョン 3.2.0 以降、FILES() はリモートストレージにファイルとしてデータを書き込むことをサポートしています。INSERT INTO FILES() を使用して、StarRocks からリモートストレージにデータをアンロードできます。

### 構文

```SQL
FILES( data_location , data_format [, StorageCredentialParams ] , unload_data_param )
```

### パラメータ

すべてのパラメータは `"key" = "value"` ペアで指定します。

#### data_location

[FILES() for loading - Parameters - data_location](#data_location) を参照してください。

#### data_format

[FILES() for loading - Parameters - data_format](#data_format) を参照してください。

#### StorageCredentialParams

[FILES() for loading - Parameters - StorageCredentialParams](#storagecredentialparams) を参照してください。

#### unload_data_param

```sql
unload_data_param ::=
    "compression" = { "uncompressed" | "gzip" | "snappy" | "zstd | "lz4" },
    "partition_by" = "<column_name> [, ...]",
    "single" = { "true" | "false" } ,
    "target_max_file_size" = "<int>"
```

| **Key**          | **Required** | **Description**                                              |
| ---------------- | ------------ | ------------------------------------------------------------ |
| compression      | Yes          | データをアンロードする際に使用する圧縮方法。有効な値:<ul><li>`uncompressed`: 圧縮アルゴリズムを使用しません。</li><li>`gzip`: gzip 圧縮アルゴリズムを使用します。</li><li>`snappy`: SNAPPY 圧縮アルゴリズムを使用します。</li><li>`zstd`: Zstd 圧縮アルゴリズムを使用します。</li><li>`lz4`: LZ4 圧縮アルゴリズムを使用します。</li></ul>**NOTE**<br />CSV ファイルへのアンロードはデータ圧縮をサポートしていません。この項目を `uncompressed` に設定する必要があります。                  |
| partition_by     | No           | データファイルを異なるストレージパスにパーティション分割するために使用される列のリスト。複数の列はカンマ (,) で区切られます。FILES() は指定された列のキー/値情報を抽出し、抽出されたキー/値ペアを特徴とするストレージパスの下にデータファイルを保存します。詳細な手順については、例 7 を参照してください。 |
| single           | No           | データを単一のファイルにアンロードするかどうか。有効な値:<ul><li>`true`: データは単一のデータファイルに保存されます。</li><li>`false` (デフォルト): アンロードされたデータ量が 512 MB を超える場合、データは複数のファイルに保存されます。</li></ul>                  |
| target_max_file_size | No           | アンロードされるバッチ内の各ファイルの最大サイズをベストエフォートで指定します。単位: バイト。デフォルト値: 1073741824 (1 GB)。アンロードされるデータのサイズがこの値を超える場合、データは複数のファイルに分割され、各ファイルのサイズはこの値を大幅に超えないようにします。バージョン 3.2.7 で導入されました。 |

## Examples

#### Example 1: ファイルからデータをクエリする

AWS S3 バケット `inserttest` 内の Parquet ファイル **parquet/par-dup.parquet** からデータをクエリします:

```Plain
SELECT * FROM FILES(
     "path" = "s3://inserttest/parquet/par-dup.parquet",
     "format" = "parquet",
     "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
     "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
     "aws.s3.region" = "us-west-2"
);
+------+---------------------------------------------------------+
| c1   | c2                                                      |
+------+---------------------------------------------------------+
|    1 | {"1": "key", "1": "1", "111": "1111", "111": "aaaa"}    |
|    2 | {"2": "key", "2": "NULL", "222": "2222", "222": "bbbb"} |
+------+---------------------------------------------------------+
2 rows in set (22.335 sec)
```

NFS(NAS) 内の Parquet ファイルからデータをクエリします:

```SQL
SELECT * FROM FILES(
  'path' = 'file:///home/ubuntu/parquetfile/*.parquet', 
  'format' = 'parquet'
);
```

#### Example 2: ファイルからデータ行を挿入する

AWS S3 バケット `inserttest` 内の Parquet ファイル **parquet/insert_wiki_edit_append.parquet** からテーブル `insert_wiki_edit` にデータ行を挿入します:

```Plain
INSERT INTO insert_wiki_edit
    SELECT * FROM FILES(
        "path" = "s3://inserttest/parquet/insert_wiki_edit_append.parquet",
        "format" = "parquet",
        "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
        "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
        "aws.s3.region" = "us-west-2"
);
Query OK, 2 rows affected (23.03 sec)
{'label':'insert_d8d4b2ee-ac5c-11ed-a2cf-4e1110a8f63b', 'status':'VISIBLE', 'txnId':'2440'}
```

NFS(NAS) 内の CSV ファイルからテーブル `insert_wiki_edit` にデータ行を挿入します:

```SQL
INSERT INTO insert_wiki_edit
  SELECT * FROM FILES(
    'path' = 'file:///home/ubuntu/csvfile/*.csv', 
    'format' = 'csv', 
    'csv.column_separator' = ',', 
    'csv.row_delimiter' = '\n'
  );
```

#### Example 3: CTAS を使用してファイルからデータ行を挿入する

AWS S3 バケット `inserttest` 内の Parquet ファイル **parquet/insert_wiki_edit_append.parquet** からテーブル `ctas_wiki_edit` を作成し、データ行を挿入します:

```Plain
CREATE TABLE ctas_wiki_edit AS
    SELECT * FROM FILES(
        "path" = "s3://inserttest/parquet/insert_wiki_edit_append.parquet",
        "format" = "parquet",
        "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
        "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
        "aws.s3.region" = "us-west-2"
);
Query OK, 2 rows affected (22.09 sec)
{'label':'insert_1a217d70-2f52-11ee-9e4a-7a563fb695da', 'status':'VISIBLE', 'txnId':'3248'}
```

#### Example 4: ファイルからデータをクエリし、そのパス内のキー/値情報を抽出する

Parquet ファイル **/geo/country=US/city=LA/file1.parquet**（2 列 - `id` と `user` のみを含む）からデータをクエリし、そのパス内のキー/値情報を返される列として抽出します。

```Plain
SELECT * FROM FILES(
    "path" = "hdfs://xxx.xx.xxx.xx:9000/geo/country=US/city=LA/file1.parquet",
    "format" = "parquet",
    "hadoop.security.authentication" = "simple",
    "username" = "xxxxx",
    "password" = "xxxxx",
    "columns_from_path" = "country, city"
);
+------+---------+---------+------+
| id   | user    | country | city |
+------+---------+---------+------+
|    1 | richard | US      | LA   |
|    2 | amber   | US      | LA   |
+------+---------+---------+------+
2 rows in set (3.84 sec)
```

#### Example 5: 自動スキーマ検出と統合

次の例は、S3 バケット内の 2 つの Parquet ファイルに基づいています:

- ファイル 1 には 3 つの列があります - INT 列 `c1`、FLOAT 列 `c2`、DATE 列 `c3`。

```Plain
c1,c2,c3
1,0.71173,2017-11-20
2,0.16145,2017-11-21
3,0.80524,2017-11-22
4,0.91852,2017-11-23
5,0.37766,2017-11-24
6,0.34413,2017-11-25
7,0.40055,2017-11-26
8,0.42437,2017-11-27
9,0.67935,2017-11-27
10,0.22783,2017-11-29
```

- ファイル 2 には 3 つの列があります - INT 列 `c1`、INT 列 `c2`、DATETIME 列 `c3`。

```Plain
c1,c2,c3
101,9,2018-05-15T18:30:00
102,3,2018-05-15T18:30:00
103,2,2018-05-15T18:30:00
104,3,2018-05-15T18:30:00
105,6,2018-05-15T18:30:00
106,1,2018-05-15T18:30:00
107,8,2018-05-15T18:30:00
108,5,2018-05-15T18:30:00
109,6,2018-05-15T18:30:00
110,8,2018-05-15T18:30:00
```

CTAS ステートメントを使用して `test_ctas_parquet` という名前のテーブルを作成し、2 つの Parquet ファイルからデータ行をテーブルに挿入します:

```SQL
CREATE TABLE test_ctas_parquet AS
SELECT * FROM FILES(
    "path" = "s3://inserttest/parquet/*",
    "format" = "parquet",
    "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
    "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
    "aws.s3.region" = "us-west-2"
);
```

`test_ctas_parquet` のテーブルスキーマを表示します:

```SQL
SHOW CREATE TABLE test_ctas_parquet\G
```

```Plain
*************************** 1. row ***************************
       Table: test_ctas_parquet
Create Table: CREATE TABLE `test_ctas_parquet` (
  `c1` bigint(20) NULL COMMENT "",
  `c2` decimal(38, 9) NULL COMMENT "",
  `c3` varchar(1048576) NULL COMMENT ""
) ENGINE=OLAP 
DUPLICATE KEY(`c1`, `c2`)
COMMENT "OLAP"
DISTRIBUTED BY RANDOM
PROPERTIES (
"bucket_size" = "4294967296",
"compression" = "LZ4",
"replication_num" = "3"
);
```

結果は、FLOAT と INT データを含む `c2` 列が DECIMAL 列としてマージされ、DATE と DATETIME データを含む `c3` 列が VARCHAR 列としてマージされていることを示しています。

Parquet ファイルが同じデータを含む CSV ファイルに変更された場合でも、上記の結果は同じです:

```Plain
CREATE TABLE test_ctas_csv AS
  SELECT * FROM FILES(
    "path" = "s3://inserttest/csv/*",
    "format" = "csv",
    "csv.column_separator"=",",
    "csv.row_delimiter"="\n",
    "csv.enclose"='"',
    "csv.skip_header"="1",
    "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
    "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
    "aws.s3.region" = "us-west-2"
);
Query OK, 0 rows affected (30.90 sec)

SHOW CREATE TABLE test_ctas_csv\G
*************************** 1. row ***************************
       Table: test_ctas_csv
Create Table: CREATE TABLE `test_ctas_csv` (
  `c1` bigint(20) NULL COMMENT "",
  `c2` decimal(38, 9) NULL COMMENT "",
  `c3` varchar(1048576) NULL COMMENT ""
) ENGINE=OLAP 
DUPLICATE KEY(`c1`, `c2`)
COMMENT "OLAP"
DISTRIBUTED BY RANDOM
PROPERTIES (
"bucket_size" = "4294967296",
"compression" = "LZ4",
"replication_num" = "3"
);
1 row in set (0.27 sec)
```

- Parquet ファイルのスキーマを統合し、`fill_mismatch_column_with` を `null` に設定してシステムが存在しない列に NULL 値を割り当てることを許可します:

```SQL
SELECT * FROM FILES(
  "path" = "s3://inserttest/basic_type.parquet,s3://inserttest/basic_type_k2k5k7.parquet",
  "format" = "parquet",
  "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
  "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
  "aws.s3.region" = "us-west-2",
  "fill_mismatch_column_with" = "null"
);
+------+------+------+-------+------------+---------------------+------+------+
| k1   | k2   | k3   | k4    | k5         | k6                  | k7   | k8   |
+------+------+------+-------+------------+---------------------+------+------+
| NULL |   21 | NULL |  NULL | 2024-10-03 | NULL                | c    | NULL |
|    0 |    1 |    2 |  3.20 | 2024-10-01 | 2024-10-01 12:12:12 | a    |  4.3 |
|    1 |   11 |   12 | 13.20 | 2024-10-02 | 2024-10-02 13:13:13 | b    | 14.3 |
+------+------+------+-------+------------+---------------------+------+------+
3 rows in set (0.03 sec)
```

#### Example 6: ファイルのスキーマを表示する

DESC を使用して AWS S3 に保存されている Parquet ファイル `lineorder` のスキーマを表示します。

```Plain
DESC FILES(
    "path" = "s3://inserttest/lineorder.parquet",
    "format" = "parquet",
    "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
    "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
    "aws.s3.region" = "us-west-2"
);

+------------------+------------------+------+
| Field            | Type             | Null |
+------------------+------------------+------+
| lo_orderkey      | int              | YES  |
| lo_linenumber    | int              | YES  |
| lo_custkey       | int              | YES  |
| lo_partkey       | int              | YES  |
| lo_suppkey       | int              | YES  |
| lo_orderdate     | int              | YES  |
| lo_orderpriority | varchar(1048576) | YES  |
| lo_shippriority  | int              | YES  |
| lo_quantity      | int              | YES  |
| lo_extendedprice | int              | YES  |
| lo_ordtotalprice | int              | YES  |
| lo_discount      | int              | YES  |
| lo_revenue       | int              | YES  |
| lo_supplycost    | int              | YES  |
| lo_tax           | int              | YES  |
| lo_commitdate    | int              | YES  |
| lo_shipmode      | varchar(1048576) | YES  |
+------------------+------------------+------+
17 rows in set (0.05 sec)
```

#### Example 7: データをアンロードする

HDFS クラスター内のパス **/unload/partitioned/** に `sales_records` のすべてのデータ行を複数の Parquet ファイルとしてアンロードします。これらのファイルは、列 `sales_time` の値によって区別される異なるサブパスに保存されます。

```SQL
INSERT INTO FILES(
    "path" = "hdfs://xxx.xx.xxx.xx:9000/unload/partitioned/",
    "format" = "parquet",
    "hadoop.security.authentication" = "simple",
    "username" = "xxxxx",
    "password" = "xxxxx",
    "compression" = "lz4",
    "partition_by" = "sales_time"
)
SELECT * FROM sales_records;
```

NFS(NAS) 内の CSV および Parquet ファイルにクエリ結果をアンロードします:

```SQL
-- CSV
INSERT INTO FILES(
    'path' = 'file:///home/ubuntu/csvfile/', 
    'format' = 'csv', 
    'csv.column_separator' = ',', 
    'csv.row_delimitor' = '\n'
)
SELECT * FROM sales_records;

-- Parquet
INSERT INTO FILES(
    'path' = 'file:///home/ubuntu/parquetfile/',
    'format' = 'parquet'
)
SELECT * FROM sales_records;
```

#### Example 8: Avro ファイル

Avro ファイルをロードします：

```SQL
INSERT INTO avro_tbl
  SELECT * FROM FILES(
    "path" = "hdfs://xxx.xx.xx.x:yyyy/avro/primitive.avro", 
    "format" = "avro"
);
```

Avro ファイルのデータをクエリーします：

```SQL
SELECT * FROM FILES("path" = "hdfs://xxx.xx.xx.x:yyyy/avro/complex.avro", "format" = "avro")\G
*************************** 1. row ***************************
record_field: {"id":1,"name":"avro"}
  enum_field: HEARTS
 array_field: ["one","two","three"]
   map_field: {"a":1,"b":2}
 union_field: 100
 fixed_field: 0x61626162616261626162616261626162
1 row in set (0.05 sec)
```

Avro ファイルのスキーマを表示します：

```SQL
DESC FILES("path" = "hdfs://xxx.xx.xx.x:yyyy/avro/logical.avro", "format" = "avro");
+------------------------+------------------+------+
| Field                  | Type             | Null |
+------------------------+------------------+------+
| decimal_bytes          | decimal(10,2)    | YES  |
| decimal_fixed          | decimal(10,2)    | YES  |
| uuid_string            | varchar(1048576) | YES  |
| date                   | date             | YES  |
| time_millis            | int              | YES  |
| time_micros            | bigint           | YES  |
| timestamp_millis       | datetime         | YES  |
| timestamp_micros       | datetime         | YES  |
| local_timestamp_millis | bigint           | YES  |
| local_timestamp_micros | bigint           | YES  |
| duration               | varbinary(12)    | YES  |
+------------------------+------------------+------+
```

#### Example 9: Managed Identity と Service Principal を使用して Azure Blob Storage にアクセスする

```SQL
-- Managed Identity
SELECT * FROM FILES(
    "path" = "wasbs://storage-container@storage-account.blob.core.windows.net/ssb_1g/customer/*",
    "format" = "parquet",
    "azure.blob.oauth2_use_managed_identity" = "true",
    "azure.blob.oauth2_client_id" = "1d6bfdec-dd34-4260-b8fd-aaaaaaaaaaaa"
);
-- Service Principal
SELECT * FROM FILES(
    "path" = "wasbs://storage-container@storage-account.blob.core.windows.net/ssb_1g/customer/*",
    "format" = "parquet",
    "azure.blob.oauth2_client_id" = "1d6bfdec-dd34-4260-b8fd-bbbbbbbbbbbb",
    "azure.blob.oauth2_client_secret" = "C2M8Q~ZXXXXXX_5XsbDCeL2dqP7hIR60xxxxxxxx",
    "azure.blob.oauth2_tenant_id" = "540e19cc-386b-4a44-a7b8-cccccccccccc"
);
```

#### Example 10: CSV ファイル

CSV ファイルのデータをクエリーします：

```SQL
SELECT * FROM FILES(                                                                                                                                                     "path" = "s3://test-bucket/file1.csv",
    "format" = "csv",
    "csv.column_separator"=",",
    "csv.row_delimiter"="\r\n",
    "csv.enclose"='"',
    "csv.skip_header"="1",
    "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
    "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
    "aws.s3.region" = "us-west-2"
);
+------+---------+--------------+
| $1   | $2      | $3           |
+------+---------+--------------+
|    1 | 0.71173 | 2017-11-20   |
|    2 | 0.16145 | 2017-11-21   |
|    3 | 0.80524 | 2017-11-22   |
|    4 | 0.91852 | 2017-11-23   |
|    5 | 0.37766 | 2017-11-24   |
|    6 | 0.34413 | 2017-11-25   |
|    7 | 0.40055 | 2017-11-26   |
|    8 | 0.42437 | 2017-11-27   |
|    9 | 0.67935 | 2017-11-27   |
|   10 | 0.22783 | 2017-11-29   |
+------+---------+--------------+
10 rows in set (0.33 sec)
```

CSV ファイルをロードします：

```SQL
INSERT INTO csv_tbl
  SELECT * FROM FILES(
    "path" = "s3://test-bucket/file1.csv",
    "format" = "csv",
    "csv.column_separator"=",",
    "csv.row_delimiter"="\r\n",
    "csv.enclose"='"',
    "csv.skip_header"="1",
    "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
    "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
    "aws.s3.region" = "us-west-2"
);
```
