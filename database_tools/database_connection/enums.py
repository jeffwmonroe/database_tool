import enum


class Status(enum.Enum):
    draft = 1
    stage = 2
    production = 3


class Action(enum.Enum):
    create = 1
    modify = 2
    delete = 3
