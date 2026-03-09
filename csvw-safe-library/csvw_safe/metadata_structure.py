"""Dataclasses for csvw-safe metadata structure."""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from csvw_safe import constants as C


@dataclass
class Dependency:
    """Row-level dependency between columns."""

    depends_on: str
    dependency_type: str
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
class Partition:
    """Partition metadata entry."""

    predicate: Dict[str, Any]
    max_length: int
    max_groups_per_unit: int
    max_contributions: int

    def to_dict(self) -> Dict[str, Any]:
        """Convert dependency to JSON-serializable dictionary."""
        return {
            "@type": C.PARTITION,
            C.PREDICATE: self.predicate,
            C.MAX_LENGTH: self.max_length,
            C.MAX_GROUPS: self.max_groups_per_unit,
            C.MAX_CONTRIB: self.max_contributions,
        }


@dataclass
class ColumnMetadata:  # pylint: disable=too-many-instance-attributes
    """Column metadata object."""

    name: str
    datatype: str
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

    partitions: Optional[list[Dict[str, Any]]] = None  # list of values or complex partitions class
    max_num_partitions: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert dependency to JSON-serializable dictionary."""
        d = {
            "@type": "csvw:Column",
            "name": self.name,
            "datatype": self.datatype,
            "required": self.required,
            C.PRIVACY_ID: self.privacy_id,
            C.NULL_PROP: self.nullable_proportion,
        }

        if self.dependencies:
            d[C.ROW_DEP] = [dep.to_dict() for dep in self.dependencies]

        if self.fixed_per_entity:
            d[C.FIXED_PER_ENTITY] = self.fixed_per_entity

        if self.minimum is not None:
            d["minimum"] = self.minimum

        if self.maximum is not None:
            d["maximum"] = self.maximum

        if self.partitions is not None:
            d[C.PUBLIC_PARTITIONS] = self.partitions
            d[C.MAX_NUM_PARTITIONS] = self.max_num_partitions

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
    partitions: list[Dict[str, Any]]
    max_num_partitions: int

    def to_dict(self) -> Dict[str, Any]:
        """Convert dependency to JSON-serializable dictionary."""
        return {
            "@type": C.COLUMN_GROUP,
            C.COLUMNS: self.columns,
            C.PUBLIC_PARTITIONS: self.partitions,
            C.MAX_NUM_PARTITIONS: self.max_num_partitions,
        }


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
            "csvw:tableSchema": {"columns": [col.to_dict() for col in self.columns]},
        }

        if self.column_groups:
            d[C.ADD_INFO] = [group.to_dict() for group in self.column_groups]

        return d
