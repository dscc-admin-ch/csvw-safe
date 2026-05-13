"""Pydantic models for CSVW-EO metadata structure."""

from typing import Any, Union

from pydantic import BaseModel, Field

from csvw_safe import constants as c
from csvw_safe.datatypes import DataTypes


class Dependency(BaseModel):
    """
    Row-level dependency between two columns.

    Represents relationships such as mappings or constraints where the value
    of one column depends on the value of another column.
    """

    depends_on: str
    dependency_type: c.DependencyType
    value_map: dict[Any, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert the dependency to a CSVW-EO compliant dictionary."""
        d: dict[str, Any] = {
            c.DEPENDS_ON: self.depends_on,
            c.DEPENDENCY_TYPE: self.dependency_type,
        }

        if self.value_map is not None:
            d[c.VALUE_MAP] = self.value_map

        return d

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Dependency":
        """Create a Dependency instance from CSVW-EO metadata."""
        return cls(
            depends_on=data[c.DEPENDS_ON],
            dependency_type=data[c.DEPENDENCY_TYPE],
            value_map=data.get(c.VALUE_MAP),
        )


class CategoricalPredicate(BaseModel):
    """Predicate describing how a categorical partition is defined."""

    partition_value: Any | None

    def to_dict(self) -> dict[str, Any]:
        """Convert the predicate into CSVW-EO JSON format."""
        return {c.PARTITION_VALUE: self.partition_value}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "CategoricalPredicate":
        """Create a Predicate from CSVW-EO metadata."""
        return cls(partition_value=data[c.PARTITION_VALUE])


class ContinuousPredicate(BaseModel):
    """Predicate describing how a continuous partition is defined."""

    lower_bound: float | str | None  # TODO type
    upper_bound: float | str | None

    def to_dict(self) -> dict[str, Any]:
        """Convert the predicate into CSVW-EO JSON format."""
        return {
            c.LOWER_BOUND: self.lower_bound,
            c.UPPER_BOUND: self.upper_bound,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ContinuousPredicate":
        """Create a Predicate from CSVW-EO metadata."""
        return cls(
            lower_bound=data[c.LOWER_BOUND],
            upper_bound=data[c.UPPER_BOUND],
        )


Predicate = Union[CategoricalPredicate, ContinuousPredicate]


def parse_predicate(data: dict[str, Any]) -> Predicate:
    """Parse predicate depending on its type."""
    if c.PARTITION_VALUE in data:
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

    def _predicate_to_dict(self) -> dict[str, Any]:
        """Serialize the predicate component."""
        raise NotImplementedError

    def to_dict(self) -> dict[str, Any]:
        """Convert the partition to CSVW-EO JSON format."""
        return {
            "@type": c.PARTITION,
            c.PREDICATE: self._predicate_to_dict(),
            c.MAX_LENGTH: self.max_length,
            c.MAX_GROUPS: self.max_groups_per_unit,
            c.MAX_CONTRIB: self.max_contributions,
        }


class SingleColumnPartition(Partition):
    """Partition defined for a single column with details."""

    predicate: Predicate

    def _predicate_to_dict(self) -> dict[str, Any]:
        return self.predicate.to_dict()

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "SingleColumnPartition":
        """Parse a single-column partition from metadata."""
        return cls(
            predicate=parse_predicate(data[c.PREDICATE]),
            max_length=data[c.MAX_LENGTH],
            max_groups_per_unit=data[c.MAX_GROUPS],
            max_contributions=data[c.MAX_CONTRIB],
        )


class MultiColumnPartition(Partition):
    """Partition defined across multiple columns with details."""

    predicate: dict[str, Predicate]

    def _predicate_to_dict(self) -> dict[str, Any]:
        return {k: v.to_dict() for k, v in self.predicate.items()}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "MultiColumnPartition":
        """
        Parse a multi-column partition from metadata.

        Parameters
        ----------
        data : dict
            Dictionary containing the serialized multi-column partition metadata.

        Returns
        -------
        MultiColumnPartition

        """
        predicates = {k: parse_predicate(v) for k, v in data[c.PREDICATE].items()}

        return cls(
            predicate=predicates,
            max_length=data[c.MAX_LENGTH],
            max_groups_per_unit=data[c.MAX_GROUPS],
            max_contributions=data[c.MAX_CONTRIB],
        )


class SingleColumnKey(BaseModel):
    """Partition defined for a single column with only key informations."""

    predicate: Predicate

    def to_dict(self) -> Any:  # noqa: ANN401
        """
        Convert a categorical partition to CSVW-EO JSON format.

        Returns:
            The partition value (e.g., 'blue').

        """
        if not isinstance(self.predicate, CategoricalPredicate):
            raise TypeError(f"Expected CategoricalPredicate, got {type(self.predicate).__name__}")

        return self.predicate.partition_value

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "SingleColumnKey":
        """
        Create a SingleColumnKey from a categorical JSON value.

        Args:
            data: A raw categorical value (e.g., 'blue').

        Returns:
            SingleColumnKey with a CategoricalPredicate.

        Raises:
            TypeError: If input is not a categorical value.

        """
        pred = CategoricalPredicate(partition_value=data)
        return cls(predicate=pred)


class MultiColumnKeys(BaseModel):
    """Partition defined for multiple columns with only key informations."""

    predicate: dict[str, Predicate]

    def to_dict(self) -> dict[str, Any]:
        """Convert the partition to CSVW-EO JSON format."""
        return {k: v.to_dict() for k, v in self.predicate.items()}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "MultiColumnKeys":
        """Create a MultiColumnKeys from CSVW-EO metadata."""
        predicates = {k: parse_predicate(v) for k, v in data.items()}
        return cls(predicate=predicates)


def full_partition_to_key_single(
    partitions: list[SingleColumnPartition],
) -> list[SingleColumnKey]:
    """
    Convert a list of SingleColumnPartition to SingleColumnKey,.

    keeping only predicate information.
    """
    return [SingleColumnKey(predicate=p.predicate) for p in partitions]


def full_partition_to_key_multi(
    partitions: list[MultiColumnPartition],
) -> list[MultiColumnKeys]:
    """
    Convert a list of MultiColumnPartition to MultiColumnKeys,.

    keeping only predicate information.
    """
    return [MultiColumnKeys(predicate=p.predicate) for p in partitions]


class ColumnMetadata(BaseModel):
    """Metadata describing a single table column."""

    name: str
    datatype: DataTypes

    required: bool | None = None
    privacy_id: bool | None = None
    nullable_proportion: float | None = None

    dependencies: list[Dependency] = Field(default_factory=list)

    minimum: Any | None = None
    maximum: Any | None = None

    max_length: int | None = None
    max_groups_per_unit: int | None = None
    max_contributions: int | None = None

    partitions: list[SingleColumnPartition] | None = None
    exhaustive_partitions: bool | None = None

    public_keys_values: list[SingleColumnKey] | None = None
    exhaustive_keys: bool | None = None
    invariant_public_keys: bool | None = None

    max_num_partitions: int | None = None

    def to_dict(self) -> dict[str, Any]:  # noqa: PLR0912
        """Convert the column metadata to CSVW-EO JSON format."""
        d: dict[str, Any] = {
            "@type": c.COL_TYPE,
            c.COL_NAME: self.name,
            c.DATATYPE: self.datatype,
            c.REQUIRED: self.required,
            c.PRIVACY_ID: self.privacy_id,
            c.NULL_PROP: self.nullable_proportion,
        }
        if self.required:
            d[c.REQUIRED] = self.required

        if self.privacy_id:
            d[c.PRIVACY_ID] = self.privacy_id

        if self.nullable_proportion:
            d[c.NULL_PROP] = self.nullable_proportion

        if self.dependencies:
            d[c.ROW_DEP] = [dep.to_dict() for dep in self.dependencies]

        if self.minimum is not None:
            d[c.MINIMUM] = self.minimum

        if self.maximum is not None:
            d[c.MAXIMUM] = self.maximum

        if self.partitions is not None:
            d[c.PUBLIC_PARTITIONS] = [p.to_dict() for p in self.partitions]

        if self.exhaustive_partitions is not None:
            d[c.EXHAUSTIVE_PARTITIONS] = self.exhaustive_partitions

        if self.public_keys_values is not None:
            d[c.KEY_VALUES] = [p.to_dict() for p in self.public_keys_values]

        if self.invariant_public_keys is not None:
            d[c.INVARIANT_PUBLIC_KEYS] = self.invariant_public_keys

        if self.exhaustive_keys is not None:
            d[c.EXHAUSTIVE_KEYS] = self.exhaustive_keys

        if self.max_num_partitions is not None:
            d[c.MAX_NUM_PARTITIONS] = self.max_num_partitions

        if self.max_length is not None:
            d[c.MAX_LENGTH] = self.max_length

        if self.max_groups_per_unit is not None:
            d[c.MAX_GROUPS] = self.max_groups_per_unit

        if self.max_contributions is not None:
            d[c.MAX_CONTRIB] = self.max_contributions

        return d

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ColumnMetadata":
        """
        Parse column metadata from CSVW-EO JSON.

        Parameters
        ----------
        data : dict
            Dictionary containing the serialized column metadata.

        Returns
        -------
        ColumnMetadata

        """
        deps = [Dependency.from_dict(d) for d in data.get(c.ROW_DEP, [])]

        col_metadata = ColumnMetadata(
            name=data[c.COL_NAME],
            datatype=data[c.DATATYPE],
            required=data.get(c.REQUIRED),
            privacy_id=data.get(c.PRIVACY_ID),
            nullable_proportion=data.get(c.NULL_PROP),
            dependencies=deps,
            minimum=data.get(c.MINIMUM),
            maximum=data.get(c.MAXIMUM),
            max_num_partitions=data.get(c.MAX_NUM_PARTITIONS),
            max_length=data.get(c.MAX_LENGTH),
            max_groups_per_unit=data.get(c.MAX_GROUPS),
            max_contributions=data.get(c.MAX_CONTRIB),
            exhaustive_keys=data.get(c.EXHAUSTIVE_KEYS),
            exhaustive_partitions=data.get(c.EXHAUSTIVE_PARTITIONS),
            invariant_public_keys=data.get(c.INVARIANT_PUBLIC_KEYS),
        )

        raw_partitions = data.get(c.PUBLIC_PARTITIONS)
        raw_public_keys_values = data.get(c.KEY_VALUES)

        if raw_partitions:
            col_metadata.partitions = [SingleColumnPartition.from_dict(p) for p in raw_partitions]
        if raw_public_keys_values:
            col_metadata.public_keys_values = [SingleColumnKey.from_dict(p) for p in raw_public_keys_values]

        return col_metadata


class ColumnGroupMetadata(BaseModel):
    """Metadata describing a group of columns that share partition definitions."""

    columns: list[str]

    # one of the two is necessary
    partitions: list[MultiColumnPartition] | None = None
    exhaustive_partitions: bool | None = None

    public_keys_values: list[MultiColumnKeys] | None = None
    exhaustive_keys: bool | None = None
    invariant_public_keys: bool | None = None

    max_num_partitions: int | None = None
    public_keys_invariant: bool | None = None

    max_length: int | None = None
    max_groups_per_unit: int | None = None
    max_contributions: int | None = None

    def to_dict(self) -> dict[str, Any]:
        """Serialize the column group metadata."""
        result: dict[str, Any] = {
            "@type": c.COLUMN_GROUP,
            c.COLUMNS_IN_GROUP: self.columns,
        }

        if self.partitions is not None:
            result[c.PUBLIC_PARTITIONS] = [p.to_dict() for p in self.partitions]

        if self.exhaustive_partitions is not None:
            result[c.EXHAUSTIVE_PARTITIONS] = self.exhaustive_partitions

        if self.public_keys_values is not None:
            result[c.KEY_VALUES] = [k.to_dict() for k in self.public_keys_values]

        if self.invariant_public_keys is not None:
            result[c.INVARIANT_PUBLIC_KEYS] = self.invariant_public_keys

        if self.exhaustive_keys is not None:
            result[c.EXHAUSTIVE_KEYS] = self.exhaustive_keys

        if self.max_num_partitions is not None:
            result[c.MAX_NUM_PARTITIONS] = self.max_num_partitions

        if self.max_length is not None:
            result[c.MAX_LENGTH] = self.max_length

        if self.max_groups_per_unit is not None:
            result[c.MAX_GROUPS] = self.max_groups_per_unit

        if self.max_contributions is not None:
            result[c.MAX_CONTRIB] = self.max_contributions

        return result

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ColumnGroupMetadata":
        """Parse grouped column metadata from JSON."""
        col_group_metadata = ColumnGroupMetadata(
            columns=data[c.COLUMNS_IN_GROUP],
            max_num_partitions=data.get(c.MAX_NUM_PARTITIONS),
            max_length=data.get(c.MAX_LENGTH),
            max_groups_per_unit=data.get(c.MAX_GROUPS),
            max_contributions=data.get(c.MAX_CONTRIB),
            exhaustive_keys=data.get(c.EXHAUSTIVE_KEYS),
            exhaustive_partitions=data.get(c.EXHAUSTIVE_PARTITIONS),
            invariant_public_keys=data.get(c.INVARIANT_PUBLIC_KEYS),
        )
        raw_partitions = data.get(c.PUBLIC_PARTITIONS)
        raw_public_keys_values = data.get(c.KEY_VALUES)

        if raw_partitions:
            col_group_metadata.partitions = [MultiColumnPartition.from_dict(p) for p in raw_partitions]
        if raw_public_keys_values:
            col_group_metadata.public_keys_values = [
                MultiColumnKeys.from_dict(p) for p in raw_public_keys_values
            ]

        return col_group_metadata


class TableMetadata(BaseModel):
    """Top-level metadata object describing a CSVW-EO table."""

    privacy_unit: str | None = None
    max_contributions: int | None = None
    max_length: int | None = None
    public_length: int | None = None

    columns: list[ColumnMetadata] = Field(default_factory=list)
    column_groups: list[ColumnGroupMetadata] | None = None

    context: list[str] = Field(default_factory=lambda: [c.CSVW_CONTEXT, c.CSVW_SAFE_CONTEXT])

    table_type: str = c.TABLE_TYPE

    def to_dict(self) -> dict[str, Any]:
        """Serialize the full metadata object to CSVW-EO JSON."""
        d: dict[str, Any] = {
            "@context": self.context,
            "@type": self.table_type,
            c.PRIVACY_UNIT: self.privacy_unit,
            c.MAX_CONTRIB: self.max_contributions,
            c.MAX_LENGTH: self.max_length,
            c.PUBLIC_LENGTH: self.public_length,
            c.TABLE_SCHEMA: {c.COL_LIST: [col.to_dict() for col in self.columns]},
        }

        if self.column_groups is not None:
            d[c.ADD_INFO] = [group.to_dict() for group in self.column_groups]

        return d

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "TableMetadata":
        """
        Parse a CSVW-EO metadata document.

        Parameters
        ----------
        data : dict
            JSON metadata structure.

        Returns
        -------
        TableMetadata

        """
        schema = data[c.TABLE_SCHEMA]

        columns = [ColumnMetadata.from_dict(c) for c in schema[c.COL_LIST]]

        column_groups = None
        if c.ADD_INFO in data:
            column_groups = [ColumnGroupMetadata.from_dict(g) for g in data[c.ADD_INFO]]

        return cls(
            privacy_unit=data.get(c.PRIVACY_UNIT),
            max_contributions=data.get(c.MAX_CONTRIB),
            max_length=data.get(c.MAX_LENGTH),
            public_length=data.get(c.PUBLIC_LENGTH),
            columns=columns,
            column_groups=column_groups,
            context=data.get("@context", []),
            table_type=data.get("@type", c.TABLE_TYPE),
        )
