"""Pydantic models for CSVW-SAFE metadata structure."""

from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, Field

from csvw_safe import constants as C
from csvw_safe.datatypes import DataTypes


class Dependency(BaseModel):
    """
    Row-level dependency between two columns.

    Represents relationships such as mappings or constraints where the value
    of one column depends on the value of another column.
    """

    depends_on: str
    dependency_type: C.DependencyType
    value_map: Optional[Dict[Any, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert the dependency to a CSVW-SAFE compliant dictionary."""
        d: Dict[str, Any] = {
            C.DEPENDS_ON: self.depends_on,
            C.DEPENDENCY_TYPE: self.dependency_type,
        }

        if self.value_map is not None:
            d[C.VALUE_MAP] = self.value_map

        return d

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Dependency":
        """Create a Dependency instance from CSVW-SAFE metadata."""
        return cls(
            depends_on=data[C.DEPENDS_ON],
            dependency_type=data[C.DEPENDENCY_TYPE],
            value_map=data.get(C.VALUE_MAP),
        )


class CategoricalPredicate(BaseModel):
    """Predicate describing how a categorical partition is defined."""

    partition_value: Optional[Any]

    def to_dict(self) -> Dict[str, Any]:
        """Convert the predicate into CSVW-SAFE JSON format."""
        return {C.PARTITION_VALUE: self.partition_value}

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "CategoricalPredicate":
        """Create a Predicate from CSVW-SAFE metadata."""
        return cls(partition_value=data[C.PARTITION_VALUE])


class ContinuousPredicate(BaseModel):
    """Predicate describing how a continuous partition is defined."""

    lower_bound: Optional[Union[float, str]]  # TODO type
    upper_bound: Optional[Union[float, str]]

    def to_dict(self) -> Dict[str, Any]:
        """Convert the predicate into CSVW-SAFE JSON format."""
        return {
            C.LOWER_BOUND: self.lower_bound,
            C.UPPER_BOUND: self.upper_bound,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ContinuousPredicate":
        """Create a Predicate from CSVW-SAFE metadata."""
        return cls(
            lower_bound=data[C.LOWER_BOUND],
            upper_bound=data[C.UPPER_BOUND],
        )


Predicate = Union[CategoricalPredicate, ContinuousPredicate]


def parse_predicate(data: Dict[str, Any]) -> Predicate:
    """Parse predicate depending on its type."""
    if C.PARTITION_VALUE in data:
        return CategoricalPredicate.from_dict(data)
    return ContinuousPredicate.from_dict(data)


class Partition(BaseModel):
    """
    Base class for partition metadata.

    Partitions define how data is grouped when enforcing privacy constraints.
    """

    max_length: int
    max_groups_per_unit: int
    max_contributions: int

    def _predicate_to_dict(self) -> Dict[str, Any]:
        """Serialize the predicate component."""
        raise NotImplementedError

    def to_dict(self) -> Dict[str, Any]:
        """Convert the partition to CSVW-SAFE JSON format."""
        return {
            "@type": C.PARTITION,
            C.PREDICATE: self._predicate_to_dict(),
            C.MAX_LENGTH: self.max_length,
            C.MAX_GROUPS: self.max_groups_per_unit,
            C.MAX_CONTRIB: self.max_contributions,
        }


class SingleColumnPartition(Partition):
    """Partition defined for a single column with details."""

    predicate: Predicate

    def _predicate_to_dict(self) -> Dict[str, Any]:
        return self.predicate.to_dict()

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SingleColumnPartition":
        """Parse a single-column partition from metadata."""
        return cls(
            predicate=parse_predicate(data[C.PREDICATE]),
            max_length=data[C.MAX_LENGTH],
            max_groups_per_unit=data[C.MAX_GROUPS],
            max_contributions=data[C.MAX_CONTRIB],
        )


class MultiColumnPartition(Partition):
    """Partition defined across multiple columns with details."""

    predicate: Dict[str, Predicate]

    def _predicate_to_dict(self) -> Dict[str, Any]:
        return {k: v.to_dict() for k, v in self.predicate.items()}

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MultiColumnPartition":
        """
        Parse a multi-column partition from metadata.

        Parameters
        ----------
        data : dict

        Returns
        -------
        MultiColumnPartition
        """
        predicates = {k: parse_predicate(v) for k, v in data[C.PREDICATE].items()}

        return cls(
            predicate=predicates,
            max_length=data[C.MAX_LENGTH],
            max_groups_per_unit=data[C.MAX_GROUPS],
            max_contributions=data[C.MAX_CONTRIB],
        )


class SingleColumnKey(BaseModel):
    """Partition defined for a single column with only key informations."""

    predicate: Predicate

    def to_dict(self) -> Any:
        """
        Convert the partition to CSVW-SAFE JSON format for key-only usage.

        - Categorical: return only the partition value (e.g., 'blue').
        - Continuous: return the full dict with lower/upper bounds.
        """
        if isinstance(self.predicate, CategoricalPredicate):
            return self.predicate.partition_value  # just the value
        return self.predicate.to_dict()  # should not happen: col level is continuous

    @classmethod
    def from_dict(cls, data: Any) -> "SingleColumnKey":
        """
        Create a SingleColumnKey from JSON metadata.

        Handles either:
        - A dict like {'csvw-safe:part.partitionValue': 'blue'} → CategoricalPredicate
        - A raw value like 'blue' → CategoricalPredicate
        - A continuous dict → ContinuousPredicate
        """
        if isinstance(data, dict):
            pred = parse_predicate(data)
        else:
            # assume raw categorical value
            pred = CategoricalPredicate(partition_value=data)
        return cls(predicate=pred)


class MultiColumnKeys(BaseModel):
    """Partition defined for multiple columns with only key informations."""

    predicate: Dict[str, Predicate]

    def to_dict(self) -> Dict[str, Any]:
        """Convert the partition to CSVW-SAFE JSON format."""
        return {k: v.to_dict() for k, v in self.predicate.items()}

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MultiColumnKeys":
        """Create a MultiColumnKeys from CSVW-SAFE metadata."""
        predicates = {k: parse_predicate(v) for k, v in data.items()}
        return cls(predicate=predicates)


def full_partition_to_key_single(partitions: List[SingleColumnPartition]) -> List[SingleColumnKey]:
    """
    Convert a list of SingleColumnPartition to SingleColumnKey,.

    keeping only predicate information.
    """
    return [SingleColumnKey(predicate=p.predicate) for p in partitions]


def full_partition_to_key_multi(partitions: List[MultiColumnPartition]) -> List[MultiColumnKeys]:
    """
    Convert a list of MultiColumnPartition to MultiColumnKeys,.

    keeping only predicate information.
    """
    return [MultiColumnKeys(predicate=p.predicate) for p in partitions]


class ColumnMetadata(BaseModel):
    """Metadata describing a single table column."""

    name: str
    datatype: DataTypes
    required: bool
    privacy_id: bool
    nullable_proportion: float

    dependencies: List[Dependency] = Field(default_factory=list)

    minimum: Optional[Any] = None
    maximum: Optional[Any] = None

    max_length: Optional[int] = None
    max_groups_per_unit: Optional[int] = None
    max_contributions: Optional[int] = None

    partitions: Optional[List[SingleColumnPartition]] = None
    public_keys: Optional[List[SingleColumnKey]] = None
    max_num_partitions: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert the column metadata to CSVW-SAFE JSON format."""
        d = {
            "@type": C.COL_TYPE,
            C.COL_NAME: self.name,
            C.DATATYPE: self.datatype,
            C.REQUIRED: self.required,
            C.PRIVACY_ID: self.privacy_id,
            C.NULL_PROP: self.nullable_proportion,
        }

        if self.dependencies:
            d[C.ROW_DEP] = [dep.to_dict() for dep in self.dependencies]

        if self.minimum is not None:
            d[C.MINIMUM] = self.minimum

        if self.maximum is not None:
            d[C.MAXIMUM] = self.maximum

        if self.partitions is not None:
            d[C.PUBLIC_PARTITIONS] = [p.to_dict() for p in self.partitions]
            d[C.EXHAUSTIVE_PARTITIONS] = True

        if self.public_keys is not None:
            d[C.PUBLIC_KEYS] = [p.to_dict() for p in self.public_keys]
            d[C.EXHAUSTIVE_PARTITIONS] = True

        if self.max_num_partitions is not None:
            d[C.MAX_NUM_PARTITIONS] = self.max_num_partitions

        if self.max_length is not None:
            d[C.MAX_LENGTH] = self.max_length

        if self.max_groups_per_unit is not None:
            d[C.MAX_GROUPS] = self.max_groups_per_unit

        if self.max_contributions is not None:
            d[C.MAX_CONTRIB] = self.max_contributions

        return d

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ColumnMetadata":
        """
        Parse column metadata from CSVW-SAFE JSON.

        Parameters
        ----------
        data : dict

        Returns
        -------
        ColumnMetadata
        """
        deps = [Dependency.from_dict(d) for d in data.get(C.ROW_DEP, [])]

        col_metadata = ColumnMetadata(
            name=data[C.COL_NAME],
            datatype=data[C.DATATYPE],
            required=data[C.REQUIRED],
            privacy_id=data[C.PRIVACY_ID],
            nullable_proportion=data[C.NULL_PROP],
            dependencies=deps,
            minimum=data.get(C.MINIMUM),
            maximum=data.get(C.MAXIMUM),
            max_num_partitions=data.get(C.MAX_NUM_PARTITIONS),
            max_length=data.get(C.MAX_LENGTH),
            max_groups_per_unit=data.get(C.MAX_GROUPS),
            max_contributions=data.get(C.MAX_CONTRIB),
        )

        raw_partitions = data.get(C.PUBLIC_PARTITIONS)
        raw_public_keys = data.get(C.PUBLIC_KEYS)

        if raw_partitions:
            partitions: List[SingleColumnPartition] = [
                SingleColumnPartition.from_dict(p) for p in raw_partitions
            ]
            col_metadata.partitions = partitions
        if raw_public_keys:
            public_keys: List[SingleColumnKey] = [
                SingleColumnKey.from_dict(p) for p in raw_public_keys
            ]
            col_metadata.public_keys = public_keys

        return col_metadata


class ColumnGroupMetadata(BaseModel):
    """Metadata describing a group of columns that share partition definitions."""

    columns: List[str]

    # one of the two is necessary
    partitions: Optional[List[MultiColumnPartition]] = None
    public_keys: Optional[List[MultiColumnKeys]] = None
    max_num_partitions: Optional[int] = None

    max_length: Optional[int] = None
    max_groups_per_unit: Optional[int] = None
    max_contributions: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        """Serialize the column group metadata."""
        result: Dict[str, Any] = {
            "@type": C.COLUMN_GROUP,
            C.COLUMNS: self.columns,
        }

        if self.partitions is not None:
            result[C.PUBLIC_PARTITIONS] = [p.to_dict() for p in self.partitions]
            result[C.EXHAUSTIVE_PARTITIONS] = True

        if self.public_keys is not None:
            result[C.PUBLIC_KEYS] = [k.to_dict() for k in self.public_keys]
            result[C.EXHAUSTIVE_PARTITIONS] = True

        if self.max_num_partitions is not None:
            result[C.MAX_NUM_PARTITIONS] = self.max_num_partitions

        if self.max_length is not None:
            result[C.MAX_LENGTH] = self.max_length

        if self.max_groups_per_unit is not None:
            result[C.MAX_GROUPS] = self.max_groups_per_unit

        if self.max_contributions is not None:
            result[C.MAX_CONTRIB] = self.max_contributions

        return result

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ColumnGroupMetadata":
        """Parse grouped column metadata from JSON."""
        col_group_metadata = ColumnGroupMetadata(
            columns=data[C.COLUMNS],
            max_num_partitions=data[C.MAX_NUM_PARTITIONS],
            max_length=data.get(C.MAX_LENGTH),
            max_groups_per_unit=data.get(C.MAX_GROUPS),
            max_contributions=data.get(C.MAX_CONTRIB),
        )

        raw_partitions = data.get(C.PUBLIC_PARTITIONS)
        raw_public_keys = data.get(C.PUBLIC_KEYS)

        if raw_partitions:
            partitions: List[MultiColumnPartition] = [
                MultiColumnPartition.from_dict(p) for p in raw_partitions
            ]
            col_group_metadata.partitions = partitions
        if raw_public_keys:
            public_keys: List[MultiColumnKeys] = [
                MultiColumnKeys.from_dict(p) for p in raw_public_keys
            ]
            col_group_metadata.public_keys = public_keys

        return col_group_metadata


class TableMetadata(BaseModel):
    """Top-level metadata object describing a CSVW-SAFE table."""

    privacy_unit: str
    max_contributions: int
    max_length: int
    public_length: int

    columns: List[ColumnMetadata] = Field(default_factory=list)
    column_groups: Optional[List[ColumnGroupMetadata]] = None

    context: List[str] = Field(default_factory=lambda: [C.CSVW_CONTEXT, C.CSVW_SAFE_CONTEXT])

    table_type: str = C.TABLE_TYPE

    def to_dict(self) -> Dict[str, Any]:
        """Serialize the full metadata object to CSVW-SAFE JSON."""
        d: Dict[str, Any] = {
            "@context": self.context,
            "@type": self.table_type,
            C.PRIVACY_UNIT: self.privacy_unit,
            C.MAX_CONTRIB: self.max_contributions,
            C.MAX_LENGTH: self.max_length,
            C.PUBLIC_LENGTH: self.public_length,
            C.TABLE_SCHEMA: {C.COL_LIST: [col.to_dict() for col in self.columns]},
        }

        if self.column_groups is not None:
            d[C.ADD_INFO] = [group.to_dict() for group in self.column_groups]

        return d

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TableMetadata":
        """
        Parse a CSVW-SAFE metadata document.

        Parameters
        ----------
        data : dict
            JSON metadata structure.

        Returns
        -------
        TableMetadata
        """
        schema = data[C.TABLE_SCHEMA]

        columns = [ColumnMetadata.from_dict(c) for c in schema[C.COL_LIST]]

        column_groups = None
        if C.ADD_INFO in data:
            column_groups = [ColumnGroupMetadata.from_dict(g) for g in data[C.ADD_INFO]]

        return cls(
            privacy_unit=data[C.PRIVACY_UNIT],
            max_contributions=data[C.MAX_CONTRIB],
            max_length=data[C.MAX_LENGTH],
            public_length=data[C.PUBLIC_LENGTH],
            columns=columns,
            column_groups=column_groups,
            context=data.get("@context", []),
            table_type=data.get("@type", C.TABLE_TYPE),
        )
