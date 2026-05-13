# import json

# import numpy as np
# import pandas as pd
# import pytest

# from csvw_safe.assert_same_structure import assert_same_structure
# from csvw_safe.make_dummy_from_metadata import make_dummy_from_metadata
# from csvw_safe.make_metadata_from_data import make_metadata_from_data
# from csvw_safe.validate_metadata import validate_metadata
# from csvw_safe.validate_metadata_shacl import validate_metadata_shacl


# def make_random_unique_id(
#     df: pd.DataFrame,
#     id_column: str,
#     fixed_fields: list[str],
#     max_contributions: int,
# ):
#     df[id_column] = np.arange(len(df))

#     def random_merge(group):
#         idx = group.index.to_numpy().copy()
#         np.random.shuffle(idx)

#         chunks = np.array_split(idx, np.ceil(len(idx) / max_contributions))

#         for chunk in chunks:
#             df.loc[chunk, id_column] = chunk[0]

#     df.groupby(fixed_fields, dropna=False, group_keys=False).apply(
#         random_merge, include_groups=False
#     )

#     assert df.groupby(id_column).size().max() == max_contributions
#     return df


# def get_island_bill_partitions(df: pd.DataFrame, bins: list[float]):
#     """
#     Compute real (species, island, bill_length_bin) partitions from dataframe.
#     """
#     df = df.copy()

#     # Create bins
#     bin_edges = [-np.inf] + bins + [np.inf]

#     df["bill_bin"] = pd.cut(
#         df["bill_length_mm"],
#         bins=bin_edges,
#         right=True,
#         include_lowest=True,
#     )

#     # Drop rows where bill length is missing
#     df = df.dropna(subset=["bill_length_mm"])

#     partitions = set(df.groupby(["species", "island", "bill_bin"]).groups.keys())

#     return partitions


# def build_dataset():
#     df = pd.read_csv("https://raw.githubusercontent.com/mwaskom/seaborn-data/master/penguins.csv")

#     df = make_random_unique_id(
#         df,
#         id_column="penguin_id",
#         fixed_fields=["species", "island", "sex"],
#         max_contributions=3,
#     )

#     df["sex"] = df["sex"].map({"MALE": 1, "FEMALE": 0}).astype(bool)

#     np.random.seed(42)

#     start = pd.Timestamp("2025-01-01")
#     end = pd.Timestamp("2025-12-31")

#     df["timestamp"] = start + pd.to_timedelta(
#         np.random.randint(0, (end - start).days, size=len(df)),
#         unit="D",
#     )

#     df["timestamp_with_time"] = df["timestamp"] + pd.to_timedelta(
#         np.random.randint(0, 24 * 60 * 60, size=len(df)),
#         unit="s",
#     )

#     df["favourite_number"] = np.random.randint(0, 6, size=len(df))

#     df["body_mass_g"] = df["body_mass_g"].astype("Int64")

#     nan_indices = np.random.choice(df.index, size=100, replace=False)
#     df.loc[nan_indices, "bill_length_mm"] = np.nan

#     df.loc[df["bill_length_mm"].isna(), "flipper_length_mm"] = np.nan

#     available = df.index[df["flipper_length_mm"].notna()]
#     extra = np.random.choice(available, size=50, replace=False)

#     df.loc[extra, "flipper_length_mm"] = np.nan

#     # Originally
#     assert list(df.groupby(["species", "island"]).groups.keys()) == [
#         ('Adelie', 'Biscoe'),
#         ('Adelie', 'Dream'),
#         ('Adelie', 'Torgersen'),
#         ('Chinstrap', 'Dream'),
#         ('Gentoo', 'Biscoe'),
#     ]

#     partitions = get_island_bill_partitions(df, [30.0, 40.0, 50.0, 60.0])
#     assert partitions == {
#         ('Adelie', 'Biscoe', pd.Interval(30.0, 40.0, closed='right')),
#         ('Adelie', 'Biscoe', pd.Interval(40.0, 50.0, closed='right')),
#         ('Adelie', 'Dream', pd.Interval(30.0, 40.0, closed='right')),
#         ('Adelie', 'Dream', pd.Interval(40.0, 50.0, closed='right')),
#         ('Adelie', 'Torgersen', pd.Interval(30.0, 40.0, closed='right')),
#         ('Adelie', 'Torgersen', pd.Interval(40.0, 50.0, closed='right')),
#         ('Chinstrap', 'Dream', pd.Interval(40.0, 50.0, closed='right')),
#         ('Chinstrap', 'Dream', pd.Interval(50.0, 60.0, closed='right')),
#         ('Gentoo', 'Biscoe', pd.Interval(40.0, 50.0, closed='right')),
#         ('Gentoo', 'Biscoe', pd.Interval(50.0, 60.0, closed='right')),
#     }

#     return df


# @pytest.fixture(scope="session")
# def df():
#     return build_dataset()


# @pytest.fixture(scope="session")
# def shacl_path():
#     return "../csvw-eo-constraints.ttl"


# @pytest.fixture(scope="session")
# def metadata_dir(tmp_path_factory):
#     return tmp_path_factory.mktemp("metadata")


# continuous_partitions = {
#     "bill_length_mm": [30.0, 40.0, 50.0, 60.0],
#     "timestamp": [
#         "2025-01-02 00:00:00",
#         "2025-06-02 00:00:00",
#         "2025-12-30 00:00:00",
#     ],
# }

# column_groups_categories = [["species", "island"]]
# column_groups_continuous = [
#     ["species", "island"],
#     ["species", "island", "bill_length_mm"],
# ]


# TEST_CASES = [
#     dict(name="table_minimal", with_dependencies=False),
#     dict(name="table_with_dependencies", with_dependencies=True),
#     dict(
#         name="continuous_partitions_table_level",
#         continuous_partitions=continuous_partitions,
#         default_contributions_level="table",
#     ),
#     dict(
#         name="partition_contributions_partition_level",
#         continuous_partitions=continuous_partitions,
#         default_contributions_level="partition",
#     ),
#     dict(
#         name="continuous_partitions_column_level",
#         continuous_partitions=continuous_partitions,
#         default_contributions_level="column",
#     ),
#     dict(
#         name="column_groups_partition_level",
#         column_groups=column_groups_categories,
#         default_contributions_level="partition",
#     ),
#     dict(
#         name="column_groups_column_level",
#         column_groups=column_groups_categories,
#         default_contributions_level="column",
#     ),
#     dict(
#         name="partitions_in_column_groups_partition",
#         continuous_partitions=continuous_partitions,
#         column_groups=column_groups_continuous,
#         default_contributions_level="partition",
#     ),
#     dict(
#         name="partitions_in_column_groups",
#         continuous_partitions=continuous_partitions,
#         column_groups=column_groups_continuous,
#         default_contributions_level="column",
#     ),
# ]


# @pytest.mark.parametrize("config", TEST_CASES)
# def test_metadata_generation(df, shacl_path, metadata_dir, config):
#     name = config["name"]
#     kwargs = {k: v for k, v in config.items() if k != "name"}
#     metadata = make_metadata_from_data(
#         df,
#         privacy_unit="penguin_id",
#         **kwargs,
#     )

#     path = metadata_dir / f"{name}.json-ld"
#     with open(path, "w", encoding="utf-8") as f:
#         json.dump(metadata, f)

#     validate_metadata(metadata)
#     validate_metadata_shacl(path, shacl_path)
#     dummy_df = make_dummy_from_metadata(metadata, nb_rows=100, seed=0)
#     assert_same_structure(df, dummy_df, check_categories=False)


# GROUP_CAT_TEST_CASES = [
#     dict(
#         column_groups=column_groups_categories,
#         default_contributions_level="partition",
#     ),
#     dict(
#         column_groups=column_groups_categories,
#         default_contributions_level="column",
#     ),
# ]


# @pytest.mark.parametrize("config", GROUP_CAT_TEST_CASES)
# def test_species_island_partitions(df, config):
#     metadata = make_metadata_from_data(
#         df,
#         privacy_unit="penguin_id",
#         **config,
#     )
#     dummy_df = make_dummy_from_metadata(metadata, nb_rows=100, seed=0)

#     expected = set(df.groupby(["species", "island"]).groups)
#     observed = set(dummy_df.groupby(["species", "island"]).groups)
#     assert observed == expected


# GROUP_CONT_TEST_CASES = [
#     dict(
#         continuous_partitions=continuous_partitions,
#         column_groups=column_groups_continuous,
#         default_contributions_level="partition",
#     ),
#     dict(
#         continuous_partitions=continuous_partitions,
#         column_groups=column_groups_continuous,
#         default_contributions_level="column",
#     ),
# ]


# @pytest.mark.parametrize("config", GROUP_CONT_TEST_CASES)
# def test_species_island_bill_length_mm_partitions(df, config):
#     metadata = make_metadata_from_data(
#         df,
#         privacy_unit="penguin_id",
#         **config,
#     )
#     dummy_df = make_dummy_from_metadata(metadata, nb_rows=100, seed=0)

#     expected = get_island_bill_partitions(df, continuous_partitions["bill_length_mm"])
#     observed = get_island_bill_partitions(dummy_df, continuous_partitions["bill_length_mm"])
#     assert observed == expected
