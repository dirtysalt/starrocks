---
displayed_sidebar: docs
---

# ADMIN SHOW REPLICA DISTRIBUTION

ADMIN SHOW REPLICA DISTRIBUTION is used to show the distribution status of a table or a partition replica.

:::tip

This operation requires the SYSTEM-level OPERATE privilege. You can follow the instructions in [GRANT](../../account-management/GRANT.md) to grant this privilege.

:::

## Syntax

```sql
ADMIN SHOW REPLICA DISTRIBUTION FROM [db_name.]tbl_name [PARTITION (p1, ...)]
```

Note:

The Graph column in the result shows the distribution ratio of replicas graphically.

## Examples

1. View the replica distribution of tables

    ```sql
    ADMIN SHOW REPLICA DISTRIBUTION FROM tbl1;
    ```

2. View the replica distribution of partitions in the table

    ```sql
    ADMIN SHOW REPLICA DISTRIBUTION FROM db1.tbl1 PARTITION(p1, p2);
    ```
