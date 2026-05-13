# CSVW-EO Examples

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
  "@type": "csvw-eo:Partition",
  "csvw-eo:predicate": {
    "month": "January"
  },
  "maxContributions": 31
}
```

## Column Group Example

```json
{
  "@type": "csvw-eo:ColumnGroup",
  "csvw-eo:columns": [
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
- CSVW-EO library examples

Repository examples demonstrate:

- metadata generation
- validation
- dummy data generation
- DP workflows