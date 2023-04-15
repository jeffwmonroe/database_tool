"""Enumerated types used with database_tool."""
import enum


class Status(enum.Enum):
    """Status enumerated type used for workflow."""

    draft = 1
    stage = 2
    production = 3


class Action(enum.Enum):
    """Action performed by a database row."""

    create = 1
    modify = 2
    delete = 3


def status_to_str(status: Status) -> str:
    """Convert a Status enumerated type to a string. This is only used for readability."""
    match status:
        case Status.draft:
            return "draft"
        case Status.stage:
            return "stage"
        case Status.production:
            return "production"
    return None


def str_to_status(string: str | None) -> Status | None:
    """Convert a str to a Status enumerated type.

    None is returned if there is no match.
    """
    if string is None:
        return None
    match string:
        case "draft":
            return Status.draft
        case "stage":
            return Status.stage
        case "production":
            return Status.production
    return None


def action_to_str(status: Action) -> str:
    """Convert an Action enumerated type to a string. This is only used for readability."""
    match status:
        case Action.create:
            return "create"
        case Action.modify:
            return "modify"
        case Action.delete:
            return "delete"
    return None


def str_to_action(string: str | None) -> Action | None:
    """Convert a str to an Action enumerated type.

    None is returned if there is no match.
    """
    if string is None:
        return None
    match string:
        case "create":
            return Action.create
        case "modify":
            return Action.modify
        case "delete":
            return Action.delete
    return None
