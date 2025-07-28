# SQLAlchemy Type Checking Configuration

## Overview

SQLAlchemy 2.0 has complex type annotations that often conflict with static type checkers like Pylance/Pyright. This document explains our approach to handling SQLAlchemy type checking issues while maintaining type safety for the rest of the codebase.

## The Problem

SQLAlchemy's dynamic nature and overloaded method signatures create numerous type checking issues:

- `create_engine` has multiple overloads with complex signatures
- `Table.columns` has dynamic typing that confuses type checkers
- `Column` types are often inferred as `Unknown`
- Method signatures like `connection.execute()` have multiple overloads
- `Inspector` class methods have complex return types

## Our Solution

We've implemented a multi-layered approach to handle SQLAlchemy type issues:

### 1. Project-Level Configuration

**File: `pyrightconfig.json`**
- Sets `typeCheckingMode` to "basic" for more lenient checking
- Disables many SQLAlchemy-related type errors globally
- Maintains strict checking for critical issues like undefined variables

**File: `.vscode/settings.json`**
- Configures Pylance to be more lenient with SQLAlchemy
- Sets `python.analysis.typeCheckingMode` to "basic"
- Overrides diagnostic severity for SQLAlchemy-related issues

### 2. File-Level Type Ignores

**Files with SQLAlchemy code:**
```python
# type: ignore[attr-defined, unknown-member, unknown-argument, unknown-variable, unknown-parameter]
```

This suppresses the most common SQLAlchemy type errors while keeping the code clean.

### 3. Targeted Type Ignores

For specific problematic lines, we use targeted type ignores:

```python
inspector = inspect(engine)  # type: ignore[attr-defined]
if inspector.has_table(table.name):  # type: ignore[attr-defined]
    # ...
```

## Migration from SQLAlchemy 1.4 to 2.0

### Key API Changes

1. **`Engine.has_table()` → `Inspector.has_table()`**
   ```python
   # Old (deprecated)
   if engine.has_table(table.name):
   
   # New (SQLAlchemy 2.0)
   inspector = inspect(engine)
   if inspector.has_table(table.name):
   ```

2. **Table.columns access**
   ```python
   # Both work, but type checking is problematic
   for column in table.columns:  # Direct iteration
   for column in table.columns.values():  # Dictionary-like access
   ```

3. **create_engine overloads**
   ```python
   # Type checker sees multiple overloads
   engine = create_engine(url)  # type: ignore[attr-defined]
   ```

### Files Updated

- `src/datasurface/platforms/kubpgstarter/jobs.py`
- `src/tests/test_createOrUpdateTable.py`

## Best Practices

### 1. Use Type Ignores Sparingly

Only use type ignores for SQLAlchemy-specific issues, not for general type problems:

```python
# Good - SQLAlchemy-specific
inspector = inspect(engine)  # type: ignore[attr-defined]

# Bad - General type issue
value = some_function()  # type: ignore
```

### 2. Document Complex Type Issues

When you encounter complex SQLAlchemy type issues, document them:

```python
# SQLAlchemy 2.0: Table.columns has complex typing
# Using type ignore to suppress known issues
for column in table.columns:  # type: ignore[attr-defined]
    # ...
```

### 3. Test Runtime Behavior

Always test that your code works at runtime, even if type checking shows errors:

```bash
python -c "from datasurface.platforms.kubpgstarter.jobs import createOrUpdateTable; print('Working!')"
```

## Configuration Details

### pyrightconfig.json Settings

- `typeCheckingMode: "basic"` - Less strict than "strict"
- `useLibraryCodeForTypes: true` - Use library type stubs when available
- `reportUnknownMemberType: "none"` - Don't complain about unknown member types
- `reportUnknownArgumentType: "none"` - Don't complain about unknown argument types

### VSCode Settings

- `python.analysis.typeCheckingMode: "basic"`
- `python.analysis.diagnosticSeverityOverrides` - Fine-tune specific error types

## Troubleshooting

### Common Issues

1. **"Type of X is partially unknown"**
   - Usually a SQLAlchemy dynamic typing issue
   - Add `# type: ignore[attr-defined]` to the line

2. **"Type of X is unknown"**
   - Often related to SQLAlchemy's dynamic nature
   - Use targeted type ignores or file-level ignores

3. **Overloaded method signatures**
   - Common with `create_engine`, `connection.execute()`
   - Use `# type: ignore[attr-defined]` for these

### When to Update Configuration

- When new SQLAlchemy versions introduce new type issues
- When you add new SQLAlchemy features that cause type errors
- When you want to make type checking stricter for non-SQLAlchemy code

## Future Improvements

1. **Better SQLAlchemy Type Stubs**
   - Monitor SQLAlchemy type stub improvements
   - Consider contributing to SQLAlchemy type stub development

2. **Alternative Type Checkers**
   - Consider mypy with SQLAlchemy plugins
   - Evaluate other type checkers that handle SQLAlchemy better

3. **Code Generation**
   - Consider generating type-safe wrappers for SQLAlchemy operations
   - Use tools like SQLAlchemy's code generation features

## References

- [SQLAlchemy 2.0 Migration Guide](https://docs.sqlalchemy.org/en/20/changelog/migration_20.html)
- [Pyright Configuration](https://github.com/microsoft/pyright/blob/main/docs/configuration.md)
- [Pylance Settings](https://github.com/microsoft/pylance-release/blob/main/DIAGNOSTIC_SEVERITY_RULES.md) 