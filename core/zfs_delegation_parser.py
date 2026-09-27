"""Parse C-locale zfs allow output without inferring effective permissions."""

from dataclasses import dataclass
import hashlib
import json
import re


class DelegationError(ValueError):
    """A delegation read or proposed change cannot be handled safely."""


TOKEN = re.compile(r"(?:@[A-Za-z0-9_.:-]+|[A-Za-z0-9_][A-Za-z0-9_.:-]*)\Z")
HEADINGS = {
    "Local permissions:": "local",
    "Descendent permissions:": "descendants",
    "Descendant permissions:": "descendants",
    "Local+Descendent permissions:": "both",
    "Local+Descendant permissions:": "both",
    "Permission sets:": "set",
    "Create time permissions:": "create_time",
}


def fingerprint(value) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def permission_tokens(value: str) -> tuple[str, ...]:
    tokens = value.split(",")
    if not tokens or any(not TOKEN.fullmatch(token) for token in tokens):
        raise DelegationError("Invalid or empty permission list.")
    return tuple(sorted(set(tokens)))


@dataclass
class DelegationEntry:
    source: str
    kind: str
    principal: str
    scope: str
    tokens: tuple[str, ...]
    principal_display: str = ""

    @property
    def key(self) -> str:
        return fingerprint([self.source, self.kind, self.principal, self.scope])

    def applies_to(self, dataset: str) -> bool:
        if self.kind in {"set", "create_time"}:
            return False
        return self.scope != ("descendants" if self.source == dataset else "local")


def parse_delegations(output: str, dataset: str) -> list[DelegationEntry]:
    """Fail closed on unknown syntax, retaining numeric IDs and unknown tokens."""
    entries = {}
    source = None
    section = None
    for line_number, raw in enumerate(output.splitlines(), 1):
        line = raw.strip()
        if not line:
            continue
        header = re.fullmatch(r"-{4,}\s+Permissions on (.*?)\s+-*", line)
        if header:
            source = header[1]
            if source != dataset and not dataset.startswith(source + "/"):
                raise DelegationError("Unexpected delegation source dataset.")
            section = None
            continue
        if source and line in HEADINGS:
            section = HEADINGS[line]
            continue
        try:
            if not source or not section:
                raise ValueError
            if section == "set":
                principal, permissions = line.split(None, 1)
                if not principal.startswith("@") or not TOKEN.fullmatch(principal):
                    raise ValueError
                kind = "set"
            elif section == "create_time":
                kind, principal, permissions = "create_time", "", line
            elif line.startswith("everyone ") or line.startswith("everyone\t"):
                kind, principal, permissions = "everyone", "", line.split(None, 1)[1]
            else:
                match = re.fullmatch(r"(user|group)\s+(\(unknown: ([0-9]+)\)|[^\s()]+)\s+(.+)", line)
                if not match:
                    raise ValueError
                kind, principal, permissions = match[1], match[3] or match[2], match[4]
                if principal.startswith("-") or "," in principal:
                    raise ValueError
            entry = DelegationEntry(source, kind, principal, section, permission_tokens(permissions))
            entry.principal_display = match[2] if kind in {"user", "group"} else principal
            if entry.key in entries:
                entry.tokens = tuple(sorted(set(entry.tokens) | set(entries[entry.key].tokens)))
            entries[entry.key] = entry
        except ValueError as error:
            raise DelegationError(f"Unable to parse delegation output at line {line_number}.") from error
    return list(entries.values())