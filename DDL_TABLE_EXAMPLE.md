# DDLTable API Examples

This document demonstrates the DDLTable API, including both the legacy pattern and the new optimized pattern.

## Backward Compatibility

The DDLTable constructor now supports **both** the old `*args` pattern and the new named parameters pattern:

### Legacy Pattern (Still Supported)
```python
from datasurface.md.schema import DDLTable, DDLColumn, PrimaryKeyList
from datasurface.md.types import String, Integer

# Old style - still works!
customer_table = DDLTable(
    DDLColumn("id", Integer(), PrimaryKeyStatus.PK),
    DDLColumn("name", String(100)),
    DDLColumn("email", String(255))
)
```

### New Optimized Pattern (Recommended)
```python
# New style - faster performance with named parameters
customer_table = DDLTable(
    columns=[
        DDLColumn("id", Integer(), primary_key=PrimaryKeyStatus.PK),
        DDLColumn("name", String(100)),
        DDLColumn("email", String(255))
    ]
)
```

## Performance Benefits

The new named parameter approach provides:
- **17% faster object creation** (1.17x speedup)
- **14.7% time savings** for bulk operations
- Better IDE support with autocomplete
- More readable, self-documenting code
- Compile-time type checking

## Migration Strategy

1. **Immediate**: All existing code continues to work unchanged
2. **Gradual**: Migrate to named parameters for better performance
3. **New code**: Use named parameters for optimal performance

## Complete Examples

### Simple Table Creation
```python
# Legacy (still supported)
simple_table = DDLTable(
    DDLColumn("id", Integer(), PrimaryKeyStatus.PK),
    DDLColumn("name", String(50))
)

# New optimized approach
simple_table = DDLTable(
    columns=[
        DDLColumn("id", Integer(), primary_key=PrimaryKeyStatus.PK),
        DDLColumn("name", String(50))
    ]
)
```

### Table with Primary Key List
```python
# Legacy
order_table = DDLTable(
    PrimaryKeyList(["order_id", "line_item"]),
    DDLColumn("order_id", Integer()),
    DDLColumn("line_item", Integer()),
    DDLColumn("product", String(100)),
    DDLColumn("quantity", Integer())
)

# New optimized
order_table = DDLTable(
    primary_key_list=PrimaryKeyList(["order_id", "line_item"]),
    columns=[
        DDLColumn("order_id", Integer()),
        DDLColumn("line_item", Integer()),
        DDLColumn("product", String(100)),
        DDLColumn("quantity", Integer())
    ]
)
```

### Table with Documentation and Partitioning
```python
from datasurface.md.documentation import PlainTextDocumentation
from datasurface.md.schema import PartitionKeyList

# New optimized approach
sales_table = DDLTable(
    columns=[
        DDLColumn("sale_id", Integer(), primary_key=PrimaryKeyStatus.PK),
        DDLColumn("sale_date", Date()),
        DDLColumn("amount", Decimal(10, 2)),
        DDLColumn("region", String(50))
    ],
    partition_key_list=PartitionKeyList(["sale_date"]),
    documentation=PlainTextDocumentation("Daily sales transactions partitioned by date")
)
```

## Performance Testing Results

Testing with 1,000 DDLTable objects (each with 10 columns):
- **Legacy pattern**: 0.0680s
- **New pattern**: 0.0580s  
- **Improvement**: 17% faster (1.17x speedup)

## Backward Compatible Methods

### Adding Columns After Construction
For backward compatibility, DDLTable still supports adding columns after construction:

```python
# Create table with initial columns
table = DDLTable(
    DDLColumn("id", Integer(), primary_key=PrimaryKeyStatus.PK),
    DDLColumn("name", String(50))
)

# Add more columns later (legacy compatibility)
table.add(DDLColumn("email", String(255)))
table.add(DDLColumn("created_date", Date()))

print(f"Table now has {len(table.columns)} columns")
```

**Note**: While the `add()` method is provided for backward compatibility, it's more efficient to provide all columns at construction time when possible.

## Combined Performance Impact

When combined with DDLColumn optimizations, creating 500 tables with 10 columns each:
- **Total objects**: 5,500 (500 tables + 5,000 columns)
- **Combined speedup**: 1.17x faster
- **Time savings**: 14.7% reduction in object creation time

The performance improvements are particularly beneficial for large data models with hundreds of tables and thousands of columns. 