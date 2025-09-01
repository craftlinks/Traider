from enum import Enum


class Channel(str, Enum):
    EARNINGS = "earnings"
    PRESS_RELEASE = "press_release"