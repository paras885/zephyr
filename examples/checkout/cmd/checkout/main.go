package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"example.com/checkout/generated"
	zephyrclient "github.com/zephyr-workflow/zephyr/pkg/client"
	"github.com/zephyr-workflow/zephyr/pkg/gateway"
	"github.com/zephyr-workflow/zephyr/pkg/queue"
	"github.com/zephyr-workflow/zephyr/pkg/token"
	"github.com/zephyr-workflow/zephyr/pkg/worker"
)

func main() {
	if err := run(); err != nil {
		slog.Error("checkout application stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	config, err := zephyrclient.ConfigFromEnv()
	if err != nil {
		return err
	}
	var workerToken token.Source
	if tokenURL := os.Getenv("OAUTH_TOKEN_URL"); tokenURL != "" {
		if config.Token != "" {
			return fmt.Errorf("configure OAuth service identities or ZEPHYR_TOKEN, not both")
		}
		config.TokenSource, err = token.NewClientCredentialsSource(token.ClientCredentialsConfig{
			TokenURL: tokenURL, ClientID: os.Getenv("OAUTH_APP_CLIENT_ID"), ClientSecret: os.Getenv("OAUTH_APP_CLIENT_SECRET"),
		})
		if err != nil {
			return err
		}
		workerToken, err = token.NewClientCredentialsSource(token.ClientCredentialsConfig{
			TokenURL: tokenURL, ClientID: os.Getenv("OAUTH_WORKER_CLIENT_ID"), ClientSecret: os.Getenv("OAUTH_WORKER_CLIENT_SECRET"),
		})
		if err != nil {
			return err
		}
	} else {
		if config.Token == "" {
			return fmt.Errorf("configure OAUTH_TOKEN_URL and service identities, or development ZEPHYR_TOKEN")
		}
		workerToken = token.SourceFunc(func(context.Context) (string, error) { return config.Token, nil })
	}
	client, err := generated.NewCheckoutClient(config)
	if err != nil {
		return err
	}
	runs, err := zephyrclient.New(config)
	if err != nil {
		return err
	}
	control, err := worker.NewHTTPTransportWithTokenSource(config.Endpoint, &http.Client{}, workerToken)
	if err != nil {
		return err
	}
	broker, err := queue.NewRabbitMQManager(ctx, os.Getenv("AMQP_URL"))
	if err != nil {
		return err
	}
	defer broker.Close()
	completions, err := queue.NewManagedRabbitMQCompletionQueue(broker, "zephyr-completions")
	if err != nil {
		return err
	}
	transport, err := worker.NewRabbitMQTransport(control, completions)
	if err != nil {
		return err
	}
	consumer, err := worker.NewClient(transport, "checkout-application", 30*time.Second)
	if err != nil {
		return err
	}
	address := os.Getenv("APP_ADDR")
	if address == "" {
		address = "127.0.0.1:8090"
	}
	server := &http.Server{Addr: address, Handler: newHandler(client, runs), ReadHeaderTimeout: 5 * time.Second}
	failures := make(chan error, 2)
	go func() { failures <- consume(ctx, consumer, checkoutTasks{}) }()
	go func() { failures <- server.ListenAndServe() }()
	slog.Info("checkout application started", "address", address, "platform", config.Endpoint, "tasks", "simulated payment and receipt")
	select {
	case <-ctx.Done():
	case err = <-failures:
	}
	stop()
	shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if shutdownErr := server.Shutdown(shutdown); shutdownErr != nil {
		return shutdownErr
	}
	return err
}

func newHandler(client *generated.CheckoutClient, runs *zephyrclient.Client) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		fmt.Fprint(w, indexHTML)
	})
	mux.HandleFunc("POST /orders", func(w http.ResponseWriter, r *http.Request) {
		var input generated.CheckoutInput
		r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
		decoder := json.NewDecoder(r.Body)
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&input); err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		if err := decoder.Decode(new(any)); err != io.EOF {
			writeError(w, http.StatusBadRequest, fmt.Errorf("request body must contain exactly one JSON object"))
			return
		}
		if err := validateInput(input); err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		id, err := client.StartCheckoutWithIdempotencyKey(r.Context(), input, input.OrderId)
		if err != nil {
			writePlatformError(w, err)
			return
		}
		writeJSON(w, http.StatusAccepted, map[string]string{"id": id})
	})
	mux.HandleFunc("GET /orders/{id}", func(w http.ResponseWriter, r *http.Request) {
		run, err := runs.GetWorkflowRun(r.Context(), r.PathValue("id"))
		if err != nil {
			writePlatformError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, run)
	})
	return mux
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		slog.Error("write application response", "error", err)
	}
}

func writeError(w http.ResponseWriter, status int, err error) {
	slog.Warn("application request failed", "status", status, "error", err)
	writeJSON(w, status, map[string]string{"error": err.Error()})
}

func writePlatformError(w http.ResponseWriter, err error) {
	status := http.StatusBadGateway
	var apiError *zephyrclient.APIError
	if errors.As(err, &apiError) && (apiError.StatusCode == http.StatusConflict || apiError.StatusCode == http.StatusNotFound) {
		status = apiError.StatusCode
	}
	writeError(w, status, err)
}

type checkoutTasks struct{}

var _ generated.AuthorizePaymentWorker = checkoutTasks{}
var _ generated.SendReceiptWorker = checkoutTasks{}

func validateInput(input generated.CheckoutInput) error {
	if input.OrderId == "" || input.CustomerEmail == "" {
		return fmt.Errorf("order_id and customer_email are required")
	}
	return nil
}

func (checkoutTasks) AuthorizePayment(ctx context.Context, input generated.CheckoutInput) (generated.TaskOutput, error) {
	return simulate(ctx, input, "authorized")
}

func (checkoutTasks) SendReceipt(ctx context.Context, input generated.CheckoutInput) (generated.TaskOutput, error) {
	return simulate(ctx, input, "receipt_sent")
}

func simulate(ctx context.Context, input generated.CheckoutInput, status string) (generated.TaskOutput, error) {
	if err := validateInput(input); err != nil {
		return generated.TaskOutput{}, err
	}
	select {
	case <-ctx.Done():
		return generated.TaskOutput{}, ctx.Err()
	case <-time.After(time.Second):
		return generated.TaskOutput{Status: status}, nil
	}
}

func consume(ctx context.Context, consumer *worker.Client, tasks checkoutTasks) error {
	for ctx.Err() == nil {
		pollContext, cancelPoll := context.WithTimeout(ctx, 25*time.Second)
		delivery, err := consumer.Receive(pollContext)
		cancelPoll()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if errors.Is(err, context.DeadlineExceeded) {
				slog.Debug("idle worker poll elapsed")
				continue
			}
			slog.Error("task receive failed; retrying", "error", err)
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(time.Second):
				continue
			}
		}
		if err := execute(ctx, consumer, tasks, delivery); err != nil {
			return err
		}
	}
	return nil
}

func execute(ctx context.Context, consumer *worker.Client, tasks checkoutTasks, delivery gateway.WorkDelivery) error {
	task, taskErr := worker.Decode[generated.CheckoutInput](delivery)
	task.Delivery = delivery
	var output generated.TaskOutput
	if taskErr == nil {
		switch delivery.Item.TaskName {
		case "AuthorizePayment":
			output, taskErr = tasks.AuthorizePayment(ctx, task.Payload)
		case "SendReceipt":
			output, taskErr = tasks.SendReceipt(ctx, task.Payload)
		default:
			taskErr = fmt.Errorf("unsupported task %q", delivery.Item.TaskName)
		}
	}
	if taskErr != nil {
		if ctx.Err() != nil {
			return nil
		}
		slog.Error("task failed", "task", delivery.Item.TaskName, "workflow_id", delivery.Item.WorkflowID, "error", taskErr)
		return consumer.Fail(ctx, worker.Failure(task, taskErr))
	}
	if err := consumer.Heartbeat(ctx, delivery, 30*time.Second); err != nil {
		return err
	}
	if err := consumer.Complete(ctx, worker.Completion(task, map[string]any{"status": output.Status})); err != nil {
		return err
	}
	slog.Info("task completion published", "task", delivery.Item.TaskName, "workflow_id", delivery.Item.WorkflowID)
	return nil
}

const indexHTML = `<!doctype html>
<html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width">
<title>Checkout consumer application</title>
<style>body{font:16px system-ui;max-width:700px;margin:40px auto;padding:20px}label,input,button{display:block;margin:12px 0}input{padding:8px;width:90%}pre{white-space:pre-wrap;background:#eee;padding:16px}</style>
<h1>Checkout application</h1>
<p>This is the consumer application, not the Zephyr platform. It starts workflows using generated contracts and consumes tasks using the worker SDK. Payment and email are simulated.</p>
<form id="order"><label>Order ID <input name="order_id" required value="order-001"></label>
<label>Customer email <input name="customer_email" type="email" required value="customer@example.test"></label>
<button>Place order</button></form>
<pre id="result" role="status">No order submitted.</pre>
<p>Open the Zephyr platform UI in another tab to inspect tasks and event history.</p>
<script>
const form=document.getElementById('order'), result=document.getElementById('result');
async function request(path,options){const response=await fetch(path,options);const body=await response.json();if(!response.ok)throw new Error(body.error);return body;}
form.addEventListener('submit',async event=>{
event.preventDefault();form.querySelector('button').disabled=true;
try {
const input=Object.fromEntries(new FormData(form));
const start=await request('/orders',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(input)});
result.textContent='Started '+start.id;
const deadline=Date.now()+60000;
while(Date.now()<deadline){
const run=await request('/orders/'+encodeURIComponent(start.id));result.textContent=JSON.stringify(run,null,2);
if(['COMPLETED','FAILED','COMPENSATED'].includes(run.status))return;
await new Promise(resolve=>setTimeout(resolve,500));}
throw new Error('Workflow still running after 60 seconds. Inspect it in the platform UI.');
}catch(error){result.textContent=error.message;}finally{form.querySelector('button').disabled=false;}
});
</script></html>`
