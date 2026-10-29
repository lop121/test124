from decimal import Decimal
from typing import Annotated

from pydantic import Field, StringConstraints

MoneyAmount = Annotated[Decimal, Field(gt=0, max_digits=15, decimal_places=2, allow_inf_nan=False)]
Description = Annotated[str, StringConstraints(max_length=1000)]
IdempotencyKey = Annotated[
    str, StringConstraints(min_length=1, max_length=128, pattern=r"^[!-~]+$")
]
