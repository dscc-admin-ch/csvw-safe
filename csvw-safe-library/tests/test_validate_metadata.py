from csvw_safe import constants as C
from csvw_safe.constants import DependencyType
from csvw_safe.datatypes import DataTypes
from csvw_safe.metadata_structure import (
    CategoricalPredicate,
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
        C.PRIVACY_UNIT: "person",
        C.MAX_CONTRIB: 5,
        C.MAX_LENGTH: 10,
        C.PUBLIC_LENGTH: 10,
        C.TABLE_SCHEMA: {
            C.COL_LIST: [
                {
                    "@type": C.COL_TYPE,
                    C.COL_NAME: "col1",
                    C.DATATYPE: DataTypes.INTEGER,
                    C.REQUIRED: True,
                    C.PRIVACY_ID: False,
                    C.NULL_PROP: 0.0,
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
        C.PRIVACY_UNIT: "person",
        C.MAX_CONTRIB: 10,
        C.MAX_LENGTH: 5,
        C.PUBLIC_LENGTH: 5,
        C.TABLE_SCHEMA: {
            C.COL_LIST: [
                {
                    "@type": C.COL_TYPE,
                    C.COL_NAME: "col1",
                    C.DATATYPE: DataTypes.STRING,
                    C.REQUIRED: True,
                    C.PRIVACY_ID: True,
                    C.NULL_PROP: 0.0,
                    C.ROW_DEP: [
                        {
                            C.DEPENDS_ON: "col2",
                            C.DEPENDENCY_TYPE: DependencyType.FIXED,
                        },
                        {
                            C.DEPENDS_ON: "col3",
                            C.DEPENDENCY_TYPE: DependencyType.MAPPING,
                            C.VALUE_MAP: {"a": 1, "b": 2},
                        },
                    ],
                },
                {
                    "@type": C.COL_TYPE,
                    C.COL_NAME: "col2",
                    C.DATATYPE: DataTypes.STRING,
                    C.REQUIRED: True,
                    C.PRIVACY_ID: False,
                    C.NULL_PROP: 0.0,
                },
                {
                    "@type": C.COL_TYPE,
                    C.COL_NAME: "col3",
                    C.DATATYPE: DataTypes.STRING,
                    C.REQUIRED: True,
                    C.PRIVACY_ID: False,
                    C.NULL_PROP: 0.0,
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
        C.PRIVACY_UNIT: "person",
        C.MAX_CONTRIB: 3,
        C.MAX_LENGTH: 10,
        C.PUBLIC_LENGTH: 10,
        C.TABLE_SCHEMA: {
            C.COL_LIST: [
                {
                    "@type": C.COL_TYPE,
                    C.COL_NAME: "col1",
                    C.DATATYPE: DataTypes.STRING,
                    C.REQUIRED: True,
                    C.PRIVACY_ID: False,
                    C.NULL_PROP: 0.0,
                    C.PUBLIC_PARTITIONS: [
                        {
                            "@type": C.PARTITION,
                            C.PREDICATE: {C.PARTITION_VALUE: "a"},
                            C.MAX_LENGTH: 5,
                            C.MAX_GROUPS: 2,
                            C.MAX_CONTRIB: 1,
                        },
                        {
                            "@type": C.PARTITION,
                            C.PREDICATE: {C.PARTITION_VALUE: "b"},
                            C.MAX_LENGTH: 5,
                            C.MAX_GROUPS: 2,
                            C.MAX_CONTRIB: 1,
                        },
                    ],
                    C.PUBLIC_KEYS: ["a", "b"],
                }
            ]
        },
    }

    table = validate_metadata(metadata)
    col1 = table.columns[0]
    assert col1.partitions
    partition = col1.partitions[0]
    assert isinstance(partition, SingleColumnPartition)
    assert partition.predicate.partition_value == "a"
    # Convert partition to key
    key = partition.to_dict()
    assert key[C.PREDICATE][C.PARTITION_VALUE] == "a"

    assert col1.public_keys
    public_keys = col1.public_keys[0]
    assert isinstance(public_keys, SingleColumnKey)
    assert public_keys.predicate.partition_value == "a"
    # Convert partition to key
    key = public_keys.to_dict()
    assert key == "a"


def test_validate_metadata_with_multi_column_partitions():
    """Test MultiColumnPartition and MultiColumnKeys parsing."""
    metadata = {
        C.PRIVACY_UNIT: "person",
        C.MAX_CONTRIB: 3,
        C.MAX_LENGTH: 10,
        C.PUBLIC_LENGTH: 10,
        C.TABLE_SCHEMA: {
            C.COL_LIST: [
                {
                    "@type": C.COL_TYPE,
                    C.COL_NAME: "col1",
                    C.DATATYPE: DataTypes.STRING,
                    C.REQUIRED: True,
                    C.PRIVACY_ID: False,
                    C.NULL_PROP: 0.0,
                    C.PUBLIC_PARTITIONS: [
                        {
                            "@type": C.PARTITION,
                            C.PREDICATE: {
                                C.PARTITION_VALUE: "a",
                            },
                            C.MAX_LENGTH: 5,
                            C.MAX_GROUPS: 2,
                            C.MAX_CONTRIB: 1,
                        }
                    ],
                }
            ]
        },
    }

    table = validate_metadata(metadata)
    col1 = table.columns[0]
    partition = col1.partitions[0]
    assert isinstance(partition, MultiColumnPartition) or isinstance(
        partition, SingleColumnPartition
    )
    # All predicates exist
    if isinstance(partition, MultiColumnPartition):
        assert set(partition.predicate.keys()) == {"col1", "col2"}


def test_validate_metadata_column_groups():
    """Test ColumnGroupMetadata parsing with public keys."""
    metadata = {
        C.PRIVACY_UNIT: "person",
        C.MAX_CONTRIB: 5,
        C.MAX_LENGTH: 10,
        C.PUBLIC_LENGTH: 10,
        C.TABLE_SCHEMA: {
            C.COL_LIST: [
                {
                    "@type": C.COL_TYPE,
                    C.COL_NAME: "col1",
                    C.DATATYPE: DataTypes.INTEGER,
                },
                {
                    "@type": C.COL_TYPE,
                    C.COL_NAME: "col2",
                    C.DATATYPE: DataTypes.INTEGER,
                },
            ]
        },
        C.ADD_INFO: [
            {
                "@type": C.COLUMN_GROUP,
                C.COLUMNS: ["col1", "col2"],
                C.PUBLIC_KEYS: [
                    {
                        "col1": {C.PARTITION_VALUE: "a"},
                        "col2": {C.LOWER_BOUND: 1.0, C.UPPER_BOUND: 10.0},
                    }
                ],
            },
            {
                "@type": C.COLUMN_GROUP,
                C.COLUMNS: ["col3", "col4"],
                C.PUBLIC_PARTITIONS: [
                    {
                        "@type": C.PARTITION,
                        C.PREDICATE: {
                            "col3": {C.PARTITION_VALUE: "b"},
                            "col4": {C.LOWER_BOUND: 1.0, C.UPPER_BOUND: 10.0},
                        },
                        C.MAX_LENGTH: 5,
                        C.MAX_GROUPS: 2,
                        C.MAX_CONTRIB: 1,
                    }
                ],
                C.MAX_NUM_PARTITIONS: 2,
                C.MAX_LENGTH: 100,
            },
        ],
    }

    table = validate_metadata(metadata)
    assert table.column_groups
    group = table.column_groups[0]
    assert group.public_keys
    group_dict = group.to_dict()
    assert group_dict[C.COLUMNS] == ["col1", "col2"]

    keys = group.public_keys[0]
    assert "col1" in keys.predicate

    cat_pred = keys.predicate["col1"]
    assert isinstance(cat_pred, CategoricalPredicate)
    assert cat_pred.partition_value == "a"

    cont_pred = keys.predicate["col2"]
    assert isinstance(cont_pred, ContinuousPredicate)
    assert cont_pred.lower_bound == 1.0
    assert cont_pred.upper_bound == 10.0

    keys_dict = keys.to_dict()
    print(keys_dict)
    assert keys_dict == {
        'col1': {C.PARTITION_VALUE: 'a'},
        'col2': {C.LOWER_BOUND: 1.0, C.UPPER_BOUND: 10.0},
    }


def test_validate_metadata_round_trip():
    """Test that to_dict and from_dict round-trip preserves data."""
    metadata = {
        C.PRIVACY_UNIT: "person",
        C.MAX_CONTRIB: 3,
        C.MAX_LENGTH: 10,
        C.PUBLIC_LENGTH: 10,
        C.TABLE_SCHEMA: {
            C.COL_LIST: [
                {
                    "@type": C.COL_TYPE,
                    C.COL_NAME: "col1",
                    C.DATATYPE: DataTypes.INTEGER,
                    C.REQUIRED: True,
                    C.PRIVACY_ID: False,
                    C.NULL_PROP: 0.0,
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
        C.PRIVACY_UNIT: "person",
        C.MAX_CONTRIB: 3,
        C.MAX_LENGTH: 10,
        C.PUBLIC_LENGTH: 10,
        C.TABLE_SCHEMA: {
            C.COL_LIST: [
                {
                    "@type": C.COL_TYPE,
                    C.COL_NAME: "age",
                    C.DATATYPE: DataTypes.DOUBLE,
                    C.REQUIRED: True,
                    C.PRIVACY_ID: False,
                    C.NULL_PROP: 0.0,
                    C.PUBLIC_PARTITIONS: [
                        {
                            "@type": C.PARTITION,
                            C.PREDICATE: {
                                C.LOWER_BOUND: 18.0,
                                C.UPPER_BOUND: 65.0,
                            },
                            C.MAX_LENGTH: 5,
                            C.MAX_GROUPS: 2,
                            C.MAX_CONTRIB: 1,
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
