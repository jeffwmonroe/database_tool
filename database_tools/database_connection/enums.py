import enum


class Status(enum.Enum):
    draft = 1
    stage = 2
    production = 3


class Action(enum.Enum):
    create = 1
    modify = 2
    delete = 3


def str_to_status(string: str | None) -> Status | None:
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


def str_to_action(string: str | None) -> Action | None:
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