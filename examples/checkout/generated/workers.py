from typing import Protocol

class AuthorizePaymentWorker(Protocol):
    async def AuthorizePayment(self, input: CheckoutInput) -> TaskOutput: ...

class SendReceiptWorker(Protocol):
    async def SendReceipt(self, input: CheckoutInput) -> TaskOutput: ...

