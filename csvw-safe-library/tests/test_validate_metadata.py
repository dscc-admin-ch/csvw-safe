from csvw_safe import constants as c
from csvw_safe.constants import DependencyType
from csvw_safe.datatypes import DataTypes
from csvw_safe.metadata_structure import (
    ColumnMetadata,
    ContinuousPredicate,
    MultiColumnPartition,
    SingleColumnKey,
    SingleColumnPartition,
    TableMetadata,
)
from csvw_safe.validate_metadata import validate_metadata


def test_validate_metadata_minimal():
    """Test minimal table metadata."""
    metadata = {
        c.PRIVACY_UNIT: "person",
        c.MAX_CONTRIB: 5,
        c.MAX_LENGTH: 10,
        c.PUBLIC_LENGTH: 10,
        c.TABLE_SCHEMA: {
            c.COL_LIST: [
                {
                    "@type": c.COL_TYPE,
                    c.COL_NAME: "col1",
                    c.DATATYPE: DataTypes.INTEGER,
                    c.REQUIRED: True,
                    c.PRIVACY_ID: False,
                    c.NULL_PROP: 0.0,
                }
            ]
        },
    }

    table = validate_metadata(metadata)
    assert isinstance(table, TableMetadata)
    assert len(table.columns) == 1
    col = table.columns[0]
    assert col.name == "col1"
    assert col.datatype == DataTypes.INTEGER
    assert col.required is True
    assert col.privacy_id is False
    assert col.nullable_proportion == 0.0


def test_validate_metadata_with_dependencies():
    """Test column dependencies FIXED and MAPPING."""
    metadata = {
        c.PRIVACY_UNIT: "person",
        c.MAX_CONTRIB: 10,
        c.MAX_LENGTH: 5,
        c.PUBLIC_LENGTH: 5,
        c.TABLE_SCHEMA: {
            c.COL_LIST: [
                {
                    "@type": c.COL_TYPE,
                    c.COL_NAME: "col1",
                    c.DATATYPE: DataTypes.STRING,
                    c.REQUIRED: True,
                    c.PRIVACY_ID: True,
                    c.NULL_PROP: 0.0,
                    c.ROW_DEP: [
                        {
                            c.DEPENDS_ON: "col2",
                            c.DEPENDENCY_TYPE: DependencyType.FIXED,
                        },
                        {
                            c.DEPENDS_ON: "col3",
                            c.DEPENDENCY_TYPE: DependencyType.MAPPING,
                            c.VALUE_MAP: {"a": 1, "b": 2},
                        },
                    ],
                },
                {
                    "@type": c.COL_TYPE,
                    c.COL_NAME: "col2",
                    c.DATATYPE: DataTypes.STRING,
                    c.REQUIRED: True,
                    c.PRIVACY_ID: False,
                    c.NULL_PROP: 0.0,
                },
                {
                    "@type": c.COL_TYPE,
                    c.COL_NAME: "col3",
                    c.DATATYPE: DataTypes.STRING,
                    c.REQUIRED: True,
                    c.PRIVACY_ID: False,
                    c.NULL_PROP: 0.0,
                },
            ]
        },
    }

    table = validate_metadata(metadata)
    col1 = table.columns[0]
    assert len(col1.dependencies) == 2
    assert col1.dependencies[0].dependency_type == DependencyType.FIXED
    assert col1.dependencies[1].value_map == {"a": 1, "b": 2}


def test_validate_metadata_with_single_column_partitions():
    """Test SingleColumnPartition and SingleColumnKey parsing."""
    metadata = {
        c.PRIVACY_UNIT: "person",
        c.MAX_CONTRIB: 3,
        c.MAX_LENGTH: 10,
        c.PUBLIC_LENGTH: 10,
        c.TABLE_SCHEMA: {
            c.COL_LIST: [
                {
                    "@type": c.COL_TYPE,
                    c.COL_NAME: "col1",
                    c.DATATYPE: DataTypes.STRING,
                    c.REQUIRED: True,
                    c.PRIVACY_ID: False,
                    c.NULL_PROP: 0.0,
                    c.PUBLIC_PARTITIONS: [
                        {
                            "@type": c.PARTITION,
                            c.PREDICATE: {c.PARTITION_VALUE: "a"},
                            c.MAX_LENGTH: 5,
                            c.MAX_GROUPS: 2,
                            c.MAX_CONTRIB: 1,
                        },
                        {
                            "@type": c.PARTITION,
                            c.PREDICATE: {c.PARTITION_VALUE: "b"},
                            c.MAX_LENGTH: 5,
                            c.MAX_GROUPS: 2,
                            c.MAX_CONTRIB: 1,
                        },
                    ],
                    c.KEY_VALUES: ["a", "b"],
                    c.EXHAUSTIVE_PARTITIONS: True,
                }
            ]
        },
    }

    table = validate_metadata(metadata)
    col1 = table.columns[0]
    assert isinstance(col1, ColumnMetadata)
    _ = col1.to_dict()

    assert col1.partitions
    partition = col1.partitions[0]
    assert isinstance(partition, SingleColumnPartition)
    assert partition.predicate.partition_value == "a"
    # Convert partition to key
    key = partition.to_dict()
    assert key[c.PREDICATE][c.PARTITION_VALUE] == "a"

    assert col1.public_keys_values
    public_keys_values = col1.public_keys_values[0]
    assert isinstance(public_keys_values, SingleColumnKey)
    assert public_keys_values.predicate.partition_value == "a"
    # Convert partition to key
    key = public_keys_values.to_dict()
    assert key == "a"


def test_validate_metadata_with_multi_column_partitions():
    """Test MultiColumnPartition and MultiColumnKeys parsing."""
    metadata = {
        c.PRIVACY_UNIT: "person",
        c.MAX_CONTRIB: 3,
        c.MAX_LENGTH: 10,
        c.PUBLIC_LENGTH: 10,
        c.TABLE_SCHEMA: {
            c.COL_LIST: [
                {
                    "@type": c.COL_TYPE,
                    c.COL_NAME: "col1",
                    c.DATATYPE: DataTypes.STRING,
                    c.REQUIRED: True,
                    c.PRIVACY_ID: False,
                    c.NULL_PROP: 0.0,
                    c.EXHAUSTIVE_PARTITIONS: True,
                    c.PUBLIC_PARTITIONS: [
                        {
                            "@type": c.PARTITION,
                            c.PREDICATE: {
                                c.PARTITION_VALUE: "a",
                            },
                            c.MAX_LENGTH: 5,
                            c.MAX_GROUPS: 2,
                            c.MAX_CONTRIB: 1,
                        }
                    ],
                }
            ]
        },
    }

    table = validate_metadata(metadata)
    col1 = table.columns[0]
    partition = col1.partitions[0]
    assert isinstance(partition, MultiColumnPartition) or isinstance(partition, SingleColumnPartition)
    # All predicates exist
    if isinstance(partition, MultiColumnPartition):
        assert set(partition.predicate.keys()) == {"col1", "col2"}


def test_validate_metadata_column_groups():
    """Test ColumnGroupMetadata parsing with public keys."""
    metadata = {
        c.PRIVACY_UNIT: "person",
        c.MAX_CONTRIB: 5,
        c.MAX_LENGTH: 10,
        c.PUBLIC_LENGTH: 10,
        c.TABLE_SCHEMA: {
            c.COL_LIST: [
                {
                    "@type": c.COL_TYPE,
                    c.COL_NAME: "col1",
                    c.DATATYPE: DataTypes.INTEGER,
                },
                {
                    "@type": c.COL_TYPE,
                    c.COL_NAME: "col2",
                    c.DATATYPE: DataTypes.INTEGER,
                },
            ]
        },
        c.ADD_INFO: [
            {
                "@type": c.COLUMN_GROUP,
                c.COLUMNS_IN_GROUP: ["col1", "col2"],
                c.KEY_VALUES: [
                    {
                        "col1": {c.PARTITION_VALUE: "a"},
                        "col2": {c.LOWER_BOUND: 1.0, c.UPPER_BOUND: 10.0},
                    }
                ],
            },
            {
                "@type": c.COLUMN_GROUP,
                c.COLUMNS_IN_GROUP: ["col3", "col4"],
                c.PUBLIC_PARTITIONS: [
                    {
                        "@type": c.PARTITION,
                        c.PREDICATE: {
                            "col3": {c.PARTITION_VALUE: "b"},
                            "col4": {c.LOWER_BOUND: 1.0, c.UPPER_BOUND: 10.0},
                        },
                        c.MAX_LENGTH: 5,
                        c.MAX_GROUPS: 2,
                        c.MAX_CONTRIB: 1,
                    }
                ],
                c.MAX_NUM_PARTITIONS: 2,
                c.EXHAUSTIVE_PARTITIONS: True,
                c.MAX_LENGTH: 100,
            },
        ],
    }

    table = validate_metadata(metadata)
    column_groups = table.column_groups

    keys = column_groups[0]
    keys_dict = keys.to_dict()
    print(keys_dict)
    assert keys_dict[c.COLUMNS_IN_GROUP] == ["col1", "col2"]
    assert keys_dict[c.KEY_VALUES] == [
        {
            "col1": {c.PARTITION_VALUE: "a"},
            "col2": {c.LOWER_BOUND: 1.0, c.UPPER_BOUND: 10.0},
        }
    ]

    partitions = column_groups[1]
    partitions_dict = partitions.to_dict()
    print(partitions_dict)
    assert partitions_dict[c.COLUMNS_IN_GROUP] == ["col3", "col4"]
    assert c.PUBLIC_PARTITIONS in partitions_dict
    assert partitions_dict[c.MAX_NUM_PARTITIONS] == 2
    assert partitions_dict[c.EXHAUSTIVE_PARTITIONS]
    assert partitions_dict[c.MAX_LENGTH] == 100


def test_validate_metadata_round_trip():
    """Test that to_dict and from_dict round-trip preserves data."""
    metadata = {
        c.PRIVACY_UNIT: "person",
        c.MAX_CONTRIB: 3,
        c.MAX_LENGTH: 10,
        c.PUBLIC_LENGTH: 10,
        c.TABLE_SCHEMA: {
            c.COL_LIST: [
                {
                    "@type": c.COL_TYPE,
                    c.COL_NAME: "col1",
                    c.DATATYPE: DataTypes.INTEGER,
                    c.REQUIRED: True,
                    c.PRIVACY_ID: False,
                    c.NULL_PROP: 0.0,
                }
            ]
        },
    }

    table = validate_metadata(metadata)
    dict_out = table.to_dict()
    table2 = validate_metadata(dict_out)
    assert table2.to_dict() == dict_out


def test_validate_metadata_with_continuous_partition():
    """Test parsing a table with a continuous partition (numeric range)."""
    metadata = {
        c.PRIVACY_UNIT: "person",
        c.MAX_CONTRIB: 3,
        c.MAX_LENGTH: 10,
        c.PUBLIC_LENGTH: 10,
        c.TABLE_SCHEMA: {
            c.COL_LIST: [
                {
                    "@type": c.COL_TYPE,
                    c.COL_NAME: "age",
                    c.DATATYPE: DataTypes.DOUBLE,
                    c.REQUIRED: True,
                    c.PRIVACY_ID: False,
                    c.NULL_PROP: 0.0,
                    c.EXHAUSTIVE_PARTITIONS: True,
                    c.PUBLIC_PARTITIONS: [
                        {
                            "@type": c.PARTITION,
                            c.PREDICATE: {
                                c.LOWER_BOUND: 18.0,
                                c.UPPER_BOUND: 65.0,
                            },
                            c.MAX_LENGTH: 5,
                            c.MAX_GROUPS: 2,
                            c.MAX_CONTRIB: 1,
                        }
                    ],
                }
            ]
        },
    }

    table = validate_metadata(metadata)
    col = table.columns[0]

    # Should have one partition
    assert col.partitions is not None
    assert len(col.partitions) == 1

    partition = col.partitions[0]

    # Should be MultiColumnPartition because the predicate is a dict
    assert isinstance(partition, SingleColumnPartition)

    # Should be ContinuousPredicate
    assert isinstance(partition.predicate, ContinuousPredicate)
    assert partition.predicate.lower_bound == 18.0
    assert partition.predicate.upper_bound == 65.0
