from pydantic import BaseModel
from typing import List, Optional

# --------------- Request Schemas ---------------
class RecordRequest(BaseModel):
    phone_number: str

class SyncDataInRangeRequest(BaseModel):
    year: int
    month: int
    start_day: int
    end_day: int

class SyncDate(BaseModel):
    from_date: str = "2025-12-12T00:00:00.000Z"
    to_date: str = "2025-12-13T00:00:00.000Z"

class CSVDataSyncRequest(BaseModel):
    file_path: str
# --------------- Response Schemas ---------------