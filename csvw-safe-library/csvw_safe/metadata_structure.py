"""Dataclasses for csvw-safe metadata structure."""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Union

from csvw_safe import constants as C
from csvw_safe.datatypes import DataTypes


@dataclass
class Dependency:
    """Row-level dependency between columns."""

    depends_on: str
    dependency_type: C.DependencyType
    value_map: Optional[Dict[Any, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert dependency to JSON-serializable dictionary."""
        d: Dict[str, Any] = {
            C.DEPENDS_ON: self.depends_on,
            C.DEPENDENCY_TYPE: self.dependency_type,
        }

        if self.value_map is not None:
            d[C.VALUE_MAP] = self.value_map

        return d


@dataclass
class Predicate:
    """Predicate for partition."""

    partition_value: Optional[Any] = None
    lower_bound: Optional[float] = None
    upper_bound: Optional[float] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert dependency to JSON-serializable dictionary."""
        if self.partition_value is not None:
            return {C.PARTITION_VALUE: self.partition_value}
        return {C.LOWER_BOUND: self.lower_bound, C.UPPER_BOUND: self.upper_bound}


@dataclass
class Partition:
    """Partition metadata entry."""

    max_length: int
    max_groups_per_unit: int
    max_contributions: int

    def _predicate_to_dict(self) -> Dict[str, Any]:
        raise NotImplementedError

    def to_dict(self) -> Dict[str, Any]:
        """Convert partition to JSON-serializable dictionary."""
        return {
            "@type": C.PARTITION,
            C.PREDICATE: self._predicate_to_dict(),
            C.MAX_LENGTH: self.max_length,
            C.MAX_GROUPS: self.max_groups_per_unit,
            C.MAX_CONTRIB: self.max_contributions,
        }


@dataclass
class SingleColumnPartition(Partition):
    """Partition metadata entry for a single column."""

    predicate: Predicate

    def _predicate_to_dict(self) -> Dict[str, Any]:
        return self.predicate.to_dict()


@dataclass
class MultiColumnPartition(Partition):
    """Partition metadata entry for multiple columns."""

    predicate: Dict[str, Predicate]

    def _predicate_to_dict(self) -> Dict[str, Any]:
        return {k: v.to_dict() for k, v in self.predicate.items()}


@dataclass
class ColumnMetadata:  # pylint: disable=too-many-instance-attributes
    """Column metadata object."""

    name: str
    datatype: DataTypes
    required: bool
    privacy_id: bool
    nullable_proportion: float

    dependencies: list[Dependency] = field(default_factory=list)
    fixed_per_entity: list[str] = field(default_factory=list)

    minimum: Optional[Any] = None
    maximum: Optional[Any] = None

    max_length: Optional[int] = None
    max_groups_per_unit: Optional[int] = None
    max_contributions: Optional[int] = None

    partitions: Optional[
        Union[
            List[SingleColumnPartition],  # partition-level
            List[str],  # column-level
        ]
    ] = None
    max_num_partitions: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert dependency to JSON-serializable dictionary."""
        d = {
            "@type": "csvw:Column",
            C.COL_NAME: self.name,
            C.DATATYPE: self.datatype,
            C.REQUIRED: self.required,
            C.PRIVACY_ID: self.privacy_id,
            C.NULL_PROP: self.nullable_proportion,
        }

        if self.dependencies:
            d[C.ROW_DEP] = [dep.to_dict() for dep in self.dependencies]

        if self.fixed_per_entity:
            d[C.FIXED_PER_ENTITY] = self.fixed_per_entity

        if self.minimum is not None:
            d[C.MINIMUM] = self.minimum

        if self.maximum is not None:
            d[C.MAXIMUM] = self.maximum

        if self.partitions is not None:
            d[C.PUBLIC_PARTITIONS] = [
                p.to_dict() if hasattr(p, "to_dict") else p for p in self.partitions
            ]
            d[C.MAX_NUM_PARTITIONS] = self.max_num_partitions
            d[C.EXHAUSTIVE_PARTITIONS] = True

        if self.max_length is not None:
            d[C.MAX_LENGTH] = self.max_length

        if self.max_groups_per_unit is not None:
            d[C.MAX_GROUPS] = self.max_groups_per_unit

        if self.max_contributions is not None:
            d[C.MAX_CONTRIB] = self.max_contributions

        return d


@dataclass
class ColumnGroupMetadata:
    """Metadata object describing grouped columns."""

    columns: list[str]
    partitions: Union[
        List[MultiColumnPartition],  # partition-level
        List[Dict[str, Predicate]],  # column-level
    ]
    max_num_partitions: int

    max_length: Optional[int] = None
    max_groups_per_unit: Optional[int] = None
    max_contributions: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert column group metadata to JSON-serializable dictionary."""
        serialized_partitions = []
        for p in self.partitions:
            if hasattr(p, "to_dict"):  # MultiColumnPartition
                serialized_partitions.append(p.to_dict())
            elif isinstance(p, dict):  # Dict[str, Predicate]
                serialized_partitions.append({k: v.to_dict() for k, v in p.items()})
            else:
                raise TypeError(f"Unsupported partition type: {type(p)}")

        result: Dict[str, Any] = {
            "@type": C.COLUMN_GROUP,
            C.COLUMNS: self.columns,
            C.PUBLIC_PARTITIONS: serialized_partitions,
            C.MAX_NUM_PARTITIONS: self.max_num_partitions,
        }
        if serialized_partitions:
            result[C.EXHAUSTIVE_PARTITIONS] = True

        if self.max_length is not None:
            result[C.MAX_LENGTH] = self.max_length

        if self.max_groups_per_unit is not None:
            result[C.MAX_GROUPS] = self.max_groups_per_unit

        if self.max_contributions is not None:
            result[C.MAX_CONTRIB] = self.max_contributions

        return result


@dataclass
class TableMetadata:  # pylint: disable=too-many-instance-attributes
    """CSVW-SAFE table metadata object."""

    privacy_unit: str
    max_contributions: int
    max_length: int
    public_length: int
    columns: List[ColumnMetadata] = field(default_factory=list)
    column_groups: Optional[List[ColumnGroupMetadata]] = None
    context: List[str] = field(
        default_factory=lambda: [
            "http://www.w3.org/ns/csvw",
            "../../../csvw-safe-context.jsonld",
        ]
    )
    table_type: str = "csvw:Table"

    def to_dict(self) -> Dict[str, Any]:
        """Convert the table metadata to a JSON-serializable dictionary."""
        d: Dict[str, Any] = {
            "@context": self.context,
            "@type": self.table_type,
            C.PRIVACY_UNIT: self.privacy_unit,
            C.MAX_CONTRIB: self.max_contributions,
            C.MAX_LENGTH: self.max_length,
            C.PUBLIC_LENGTH: self.public_length,
            C.TABLE_SCHEMA: {C.COL_LIST: [col.to_dict() for col in self.columns]},
        }

        if self.column_groups:
            d[C.ADD_INFO] = [group.to_dict() for group in self.column_groups]

        return d
