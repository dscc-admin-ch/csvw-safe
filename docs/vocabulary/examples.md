# CSVW-SAFE Examples

This section provides example metadata structures.

## Minimal Table

```json
{
  "@type": "csvw:Table",
  "name": "penguins"
}
```

## Column Example

```json
{
  "@type": "csvw:Column",
  "name": "species",
  "datatype": "string",
  "publicKeys": [
    "Adelie",
    "Gentoo",
    "Chinstrap"
  ]
}
```

## Partition Example

```json
{
  "@type": "csvw-safe:Partition",
  "csvw-safe:predicate": {
    "month": "January"
  },
  "maxContributions": 31
}
```

## Column Group Example

```json
{
  "@type": "csvw-safe:ColumnGroup",
  "csvw-safe:columns": [
    "year",
    "month"
  ]
}
```

## Dependency Example

```json
{
  "dependsOn": "age",
  "dependencyType": "mapping",
  "valueMap": {
    "0-17": false,
    "18+": true
  }
}
```

## Continuous Partition Example

```json
{
  "partitions": [
    {
      "minimum": 0,
      "maximum": 18
    },
    {
      "minimum": 18,
      "maximum": 65
    }
  ]
}
```

## Complete Example

See:

- Penguin metadata example
- Example notebooks
- CSVW-SAFE library examples

Repository examples demonstrate:

- metadata generation
- validation
- dummy data generation
- DP workflows