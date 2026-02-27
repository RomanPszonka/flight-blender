from typing import Optional

from pydantic import BaseModel


class SignedTelmetryPublicKeySerializer(BaseModel):
    key_id: str
    url: str
    is_active: Optional[bool] = True

    model_config = {"from_attributes": True}
