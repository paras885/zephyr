package generated

import "context"

type AuthorizePaymentWorker interface {
	AuthorizePayment(ctx context.Context, input CheckoutInput) (output TaskOutput, err error)
}

type SendReceiptWorker interface {
	SendReceipt(ctx context.Context, input CheckoutInput) (output TaskOutput, err error)
}
