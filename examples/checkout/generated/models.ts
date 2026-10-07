export interface CheckoutInput {
  order_id: string;
  customer_email: string;
}

export interface TaskOutput {
  status: string;
}

export interface AuthorizePaymentWorker {
  AuthorizePayment(input: CheckoutInput): Promise<TaskOutput>;
}

export interface SendReceiptWorker {
  SendReceipt(input: CheckoutInput): Promise<TaskOutput>;
}

