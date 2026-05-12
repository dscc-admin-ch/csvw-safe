# Dummy Dataset Generation

The `make_dummy_from_metadata.py` utility generates synthetic datasets from CSVW-SAFE metadata.

---

## Purpose

The generator creates structurally valid synthetic datasets that follow:

- Column datatypes
- Nullable proportions
- Numeric bounds
- Public partitions
- Column dependencies
- Column-group constraints

The generated data is synthetic and does not preserve real-world statistical distributions unless explicitly encoded in metadata.

---

## Typical Use Cases

- Unit testing
- Integration testing
- DP pipeline debugging
- Schema validation
- Safe data sharing examples

---

## Basic Usage

```bash
python make_dummy_from_metadata.py \
  metadata.json \
  --output dummy.csv
```

## Reproducible Generation

```bash
python make_dummy_from_metadata.py \
  metadata.json \
  --rows 1000 \
  --seed 42 \
  --output dummy.csv
```

## Output Guarantees

Generated datasets:

- Respect metadata schema
- Respect nullable proportions
- Respect public partitions
- Respect numeric bounds
- Follow declared dependencies

## Limitations

The generator does not guarantee:

- Statistical realism
- Correlation preservation

It is intended for structural testing only.
