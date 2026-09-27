"""Backend-owned catalog of WebZFS user-defined vdev properties (issue 113).

ZFS stores user properties as arbitrary strings. This catalog gives the
``webzfs:*`` namespace application-level structure: which keys exist, which
object scope they apply to, how values are typed, and how they are grouped in
the inspector. Templates and views consume this catalog instead of duplicating
the key list.
"""

from dataclasses import dataclass, field
from datetime import date

NAMESPACE = "webzfs:"
MAX_VALUE_LENGTH = 2048
MAX_NOTES_LENGTH = 4096

SCOPE_POOL = "pool"
SCOPE_VDEV = "vdev"
SCOPE_LEAF = "leaf"

GROUP_LABELS = {
    "inventory": "Inventory",
    "vdev": "Top-level vdev metadata",
    "identity": "Disk identity",
    "procurement": "Procurement and lifecycle",
    "notes": "Notes",
}


class PropertyValidationError(ValueError):
    """Raised when a submitted value does not fit the catalog definition."""


@dataclass(frozen=True)
class PropertyDefinition:
    key: str
    label: str
    object_scope: str
    value_type: str = "text"
    group: str = "identity"
    source_mode: str = "manual"
    enum_values: tuple[str, ...] = field(default_factory=tuple)
    description: str = ""
    inline_modes: tuple[str, ...] = field(default_factory=tuple)

    @property
    def short_key(self) -> str:
        return self.key[len(NAMESPACE):]

    @property
    def form_name(self) -> str:
        return self.short_key.replace(".", "_")

    @property
    def discoverable(self) -> bool:
        return self.source_mode == "discoverable"


CATALOG: tuple[PropertyDefinition, ...] = (
    PropertyDefinition("webzfs:inventory.owner", "Inventory owner", SCOPE_POOL, group="inventory",
                       description="Team or person responsible for this pool's hardware inventory."),
    PropertyDefinition("webzfs:inventory.last_audit", "Last audit", SCOPE_POOL, value_type="date", group="inventory",
                       description="Date the physical inventory was last verified."),
    PropertyDefinition("webzfs:disk.location_scheme", "Location scheme", SCOPE_POOL, group="inventory",
                       description="Convention used for physical location values, for example chassis/bay/slot-NN."),
    PropertyDefinition("webzfs:vdev.role", "Role", SCOPE_VDEV, group="vdev",
                       description="Functional role of this vdev group, such as primary-data or special."),
    PropertyDefinition("webzfs:vdev.chassis", "Chassis", SCOPE_VDEV, group="vdev",
                       description="Chassis or server housing this vdev's disks."),
    PropertyDefinition("webzfs:vdev.enclosure", "Enclosure", SCOPE_VDEV, group="vdev",
                       description="Disk shelf or enclosure identifier."),
    PropertyDefinition("webzfs:vdev.notes", "Service notes", SCOPE_VDEV, value_type="textarea", group="vdev"),
    PropertyDefinition("webzfs:disk.model", "Model", SCOPE_LEAF, source_mode="discoverable",
                       inline_modes=("balanced", "identity")),
    PropertyDefinition("webzfs:disk.serial", "Serial", SCOPE_LEAF, source_mode="discoverable",
                       inline_modes=("balanced", "identity")),
    PropertyDefinition("webzfs:disk.wwn", "WWN", SCOPE_LEAF, source_mode="discoverable"),
    PropertyDefinition("webzfs:disk.firmware", "Firmware", SCOPE_LEAF, source_mode="discoverable"),
    PropertyDefinition("webzfs:disk.interface", "Interface", SCOPE_LEAF, value_type="enum",
                       source_mode="discoverable", enum_values=("SAS", "SATA", "NVME", "USB", "THUNDERBOLT")),
    PropertyDefinition("webzfs:disk.formfactor", "Form factor", SCOPE_LEAF, source_mode="discoverable"),
    PropertyDefinition("webzfs:disk.vendor_source", "Vendor source", SCOPE_LEAF, group="procurement"),
    PropertyDefinition("webzfs:disk.condition", "Condition", SCOPE_LEAF, value_type="enum", group="procurement",
                       enum_values=("new", "recert", "refurb", "used")),
    PropertyDefinition("webzfs:disk.purchase_date", "Purchase date", SCOPE_LEAF, value_type="date", group="procurement"),
    PropertyDefinition("webzfs:disk.install_date", "Install date", SCOPE_LEAF, value_type="date", group="procurement"),
    PropertyDefinition("webzfs:disk.warranty_expiry", "Warranty expiry", SCOPE_LEAF, value_type="date",
                       group="procurement", inline_modes=("balanced",)),
    PropertyDefinition("webzfs:disk.location", "Physical location", SCOPE_LEAF, group="procurement",
                       inline_modes=("balanced", "location")),
    PropertyDefinition("webzfs:disk.notes", "Notes", SCOPE_LEAF, value_type="textarea", group="notes"),
)

BY_KEY: dict[str, PropertyDefinition] = {definition.key: definition for definition in CATALOG}
BY_FORM_NAME: dict[str, PropertyDefinition] = {definition.form_name: definition for definition in CATALOG}
INLINE_MODES = ("balanced", "identity", "location", "minimal")


def definitions_for_scope(scope: str) -> list[PropertyDefinition]:
    return [definition for definition in CATALOG if definition.object_scope == scope]


def groups_for_scope(scope: str) -> list[tuple[str, str, list[PropertyDefinition]]]:
    """Return ordered (group id, group label, definitions) tuples for a scope."""
    ordered: list[tuple[str, str, list[PropertyDefinition]]] = []
    for definition in definitions_for_scope(scope):
        if not ordered or ordered[-1][0] != definition.group:
            ordered.append((definition.group, GROUP_LABELS.get(definition.group, definition.group), []))
        ordered[-1][2].append(definition)
    return ordered


def discoverable_keys() -> list[str]:
    return [definition.key for definition in CATALOG if definition.discoverable]


def validate_value(definition: PropertyDefinition, raw_value: str) -> str:
    """Normalize and validate a submitted value. Empty string means clear."""
    value = raw_value.strip()
    if not value:
        return ""
    if "\x00" in value:
        raise PropertyValidationError(f"{definition.label} contains an invalid character.")
    limit = MAX_NOTES_LENGTH if definition.value_type == "textarea" else MAX_VALUE_LENGTH
    if len(value.encode("utf-8")) > limit:
        raise PropertyValidationError(f"{definition.label} exceeds {limit} bytes.")
    if definition.value_type != "textarea" and ("\n" in value or "\r" in value):
        raise PropertyValidationError(f"{definition.label} must be a single line.")
    if definition.value_type == "date":
        try:
            date.fromisoformat(value)
        except ValueError as error:
            raise PropertyValidationError(f"{definition.label} must use YYYY-MM-DD.") from error
    if definition.value_type == "enum":
        matches = [option for option in definition.enum_values if option.lower() == value.lower()]
        if not matches:
            allowed = ", ".join(definition.enum_values)
            raise PropertyValidationError(f"{definition.label} must be one of: {allowed}.")
        value = matches[0]
    return value
