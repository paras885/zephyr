package generated

type CheckoutInput struct {
	OrderId       string `json:"order_id"`
	CustomerEmail string `json:"customer_email"`
}

type TaskOutput struct {
	Status string `json:"status"`
}
