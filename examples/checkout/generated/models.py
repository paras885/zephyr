from pydantic import BaseModel

class CheckoutInput(BaseModel):
    order_id: str
    customer_email: str

class TaskOutput(BaseModel):
    status: str

