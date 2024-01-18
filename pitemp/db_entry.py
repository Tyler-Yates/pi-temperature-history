from dataclasses import dataclass
from datetime import datetime


@dataclass
class DbEntry:
    timestamp: datetime
    temp: float
