# Power BI Power Query M Code

Use these queries in **Power Query Advanced Editor** if the generated Folder connector code becomes unstable.

These versions read only real `.parquet` files, ignore hidden files, avoid the generated `Transform File` helper queries, and are easier to maintain.

## `orders_per_minute`

```powerquery
let
    Source = Folder.Files("D:\DEPI Data Engineering\Data-Engineering--DEPI\data_lake\gold\orders_per_minute"),
    ParquetFiles = Table.SelectRows(
        Source,
        each Text.Lower([Extension]) = ".parquet"
            and [Attributes]?[Hidden]? <> true
            and not Text.StartsWith([Name], ".")
    ),
    Tables = Table.AddColumn(ParquetFiles, "Data", each Parquet.Document([Content])),
    Combined = if Table.RowCount(Tables) = 0
        then #table(
            type table [minute_start = datetime, minute_end = datetime, orders_count = Int64.Type],
            {}
        )
        else Table.Combine(Tables[Data]),
    ChangedType = Table.TransformColumnTypes(
        Combined,
        {
            {"minute_start", type datetime},
            {"minute_end", type datetime},
            {"orders_count", Int64.Type}
        }
    )
in
    ChangedType
```

## `order_status_counts`

```powerquery
let
    Source = Folder.Files("D:\DEPI Data Engineering\Data-Engineering--DEPI\data_lake\gold\order_status_counts"),
    ParquetFiles = Table.SelectRows(
        Source,
        each Text.Lower([Extension]) = ".parquet"
            and [Attributes]?[Hidden]? <> true
            and not Text.StartsWith([Name], ".")
    ),
    Tables = Table.AddColumn(ParquetFiles, "Data", each Parquet.Document([Content])),
    Combined = if Table.RowCount(Tables) = 0
        then #table(
            type table [order_status = text, orders_count = Int64.Type],
            {}
        )
        else Table.Combine(Tables[Data]),
    ChangedType = Table.TransformColumnTypes(
        Combined,
        {
            {"order_status", type text},
            {"orders_count", Int64.Type}
        }
    )
in
    ChangedType
```

## `revenue_by_state`

```powerquery
let
    Source = Folder.Files("D:\DEPI Data Engineering\Data-Engineering--DEPI\data_lake\gold\revenue_by_state"),
    ParquetFiles = Table.SelectRows(
        Source,
        each Text.Lower([Extension]) = ".parquet"
            and [Attributes]?[Hidden]? <> true
            and not Text.StartsWith([Name], ".")
    ),
    Tables = Table.AddColumn(ParquetFiles, "Data", each Parquet.Document([Content])),
    Combined = if Table.RowCount(Tables) = 0
        then #table(
            type table [
                customer_state = text,
                product_revenue = number,
                freight_revenue = number,
                total_revenue = number
            ],
            {}
        )
        else Table.Combine(Tables[Data]),
    ChangedType = Table.TransformColumnTypes(
        Combined,
        {
            {"customer_state", type text},
            {"product_revenue", type number},
            {"freight_revenue", type number},
            {"total_revenue", type number}
        }
    )
in
    ChangedType
```

## `top_products`

```powerquery
let
    Source = Folder.Files("D:\DEPI Data Engineering\Data-Engineering--DEPI\data_lake\gold\top_products"),
    ParquetFiles = Table.SelectRows(
        Source,
        each Text.Lower([Extension]) = ".parquet"
            and [Attributes]?[Hidden]? <> true
            and not Text.StartsWith([Name], ".")
    ),
    Tables = Table.AddColumn(ParquetFiles, "Data", each Parquet.Document([Content])),
    Combined = if Table.RowCount(Tables) = 0
        then #table(
            type table [
                product_category = text,
                product_revenue = number,
                total_revenue = number
            ],
            {}
        )
        else Table.Combine(Tables[Data]),
    ChangedType = Table.TransformColumnTypes(
        Combined,
        {
            {"product_category", type text},
            {"product_revenue", type number},
            {"total_revenue", type number}
        }
    )
in
    ChangedType
```

## `daily_orders_forecast`

```powerquery
let
    Source = Csv.Document(
        File.Contents("D:\DEPI Data Engineering\Data-Engineering--DEPI\ml_outputs\daily_orders_forecast.csv"),
        [Delimiter = ",", Columns = 6, Encoding = 65001, QuoteStyle = QuoteStyle.Csv]
    ),
    PromotedHeaders = Table.PromoteHeaders(Source, [PromoteAllScalars = true]),
    BlankToNull = Table.ReplaceValue(
        PromotedHeaders,
        "",
        null,
        Replacer.ReplaceValue,
        {"orders_count", "total_revenue", "absolute_error"}
    ),
    ChangedType = Table.TransformColumnTypes(
        BlankToNull,
        {
            {"record_type", type text},
            {"date", type date},
            {"orders_count", Int64.Type},
            {"predicted_orders", Int64.Type},
            {"total_revenue", type number},
            {"absolute_error", Int64.Type}
        }
    )
in
    ChangedType
```
