package cli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/zephyr-workflow/zephyr/pkg/client"
	"github.com/zephyr-workflow/zephyr/pkg/dsl/compiler"
	"github.com/zephyr-workflow/zephyr/pkg/generator"
)

const sample = `type Input { value: string }
type Output { status: string }

workflow Hello(input: Input) -> Output {
    return Output { status: input.value };
}
`

const help = `Usage: zephyr workflow <command> [identifier] [options]
  generate   Create a sample definition (--file workflow.zephyr)
  validate   Compile and validate a definition (--file workflow.zephyr)
  artifacts  Generate contracts/config (--file, --version 1, --output .)
  publish    Register immutable source/version and save contracts (--file, --version 1, --output .)
  start NAME Start a run (--version 0 means latest, --input JSON or @file, --idempotency-key KEY)
  status ID  Fetch run details
  runs NAME  List runs (--limit 25, --offset 0, --status STATUS)
  zephyr auth login|logout|status

Online options: --endpoint URL (issuer/client discovered from platform),
--issuer URL and --client-id ID override discovery,
--ca-file PATH, --no-browser. Environment: ZEPHYR_ENDPOINT, ZEPHYR_OIDC_ISSUER,
ZEPHYR_OIDC_CLIENT_ID, ZEPHYR_CA_FILE, ZEPHYR_CONFIG_DIR.
ZEPHYR_TOKEN explicitly overrides interactive auth for automation/development.
Files are not overwritten unless --force is supplied. Online results are JSON;
authentication instructions go to stderr. Use --help for this usage.
`

type options struct {
	endpoint, issuer, clientID, caFile string
	file, output, input, key, status   string
	version, limit, offset             int
	force, noBrowser                   bool
}

func Run(ctx context.Context, args []string, out, stderr io.Writer) error {
	if len(args) == 0 || args[0] == "--help" || args[0] == "help" {
		_, err := io.WriteString(out, help)
		return err
	}
	if len(args) < 2 || (args[0] != "workflow" && args[0] != "auth") {
		return fmt.Errorf("expected workflow or auth subcommand; use zephyr --help")
	}
	group, command := args[0], args[1]
	opts := options{}
	flags := flag.NewFlagSet(group+" "+command, flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.Usage = func() { fmt.Fprint(stderr, help) }
	flags.StringVar(&opts.endpoint, "endpoint", os.Getenv("ZEPHYR_ENDPOINT"), "platform endpoint")
	flags.StringVar(&opts.issuer, "issuer", os.Getenv("ZEPHYR_OIDC_ISSUER"), "OIDC issuer")
	flags.StringVar(&opts.clientID, "client-id", os.Getenv("ZEPHYR_OIDC_CLIENT_ID"), "public device-flow client")
	flags.StringVar(&opts.caFile, "ca-file", os.Getenv("ZEPHYR_CA_FILE"), "additional trusted CA certificate")
	flags.BoolVar(&opts.noBrowser, "no-browser", false, "print sign-in URL without opening browser")
	if group == "workflow" {
		flags.StringVar(&opts.file, "file", "workflow.zephyr", "definition file")
		flags.StringVar(&opts.output, "output", ".", "artifact directory")
		flags.StringVar(&opts.input, "input", "{}", "input JSON or @filename")
		flags.StringVar(&opts.key, "idempotency-key", "", "start idempotency key")
		flags.StringVar(&opts.status, "status", "", "run status filter")
		version := 1
		if command == "start" {
			version = 0
		}
		flags.IntVar(&opts.version, "version", version, "workflow version")
		flags.IntVar(&opts.limit, "limit", 25, "run count (1-100)")
		flags.IntVar(&opts.offset, "offset", 0, "run offset")
		flags.BoolVar(&opts.force, "force", false, "overwrite generated files")
	}
	ordered, err := orderFlags(args[2:], flags)
	if err != nil {
		return err
	}
	if err := flags.Parse(ordered); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	positionals := flags.Args()
	if group == "auth" {
		if len(positionals) != 0 {
			return fmt.Errorf("auth takes no positional arguments")
		}
		return runAuth(ctx, command, opts, out, stderr)
	}
	needsID := command == "start" || command == "status" || command == "runs"
	if (needsID && len(positionals) != 1) || (!needsID && len(positionals) != 0) {
		return fmt.Errorf("%s requires %s", command, map[bool]string{true: "one identifier", false: "flags only"}[needsID])
	}
	switch command {
	case "generate":
		if err := saveFiles(filepath.Dir(opts.file), map[string][]byte{filepath.Base(opts.file): []byte(sample)}, opts.force); err != nil {
			return err
		}
		return printJSON(out, map[string]string{"file": opts.file})
	case "validate", "artifacts", "publish":
		source, err := os.ReadFile(opts.file)
		if err != nil {
			return fmt.Errorf("read workflow: %w", err)
		}
		definition, err := compiler.Compile(string(source))
		if err != nil {
			return fmt.Errorf("invalid workflow: %w", err)
		}
		if opts.version < 1 {
			return fmt.Errorf("version must be positive")
		}
		files, err := generator.GenerateVersion(string(source), opts.version)
		if err != nil {
			return fmt.Errorf("invalid contracts: %w", err)
		}
		if command == "validate" {
			return printJSON(out, map[string]any{"valid": true, "name": definition.Name, "version": opts.version})
		}
		if err := checkFiles(opts.output, files, opts.force); err != nil {
			return err
		}
		if command == "publish" {
			api, endpoint, err := authenticatedClient(ctx, opts, stderr)
			if err != nil {
				return err
			}
			record, err := api.RegisterWorkflow(ctx, string(source), opts.version)
			if err != nil {
				return describeAPIError(err)
			}
			files = make(map[string][]byte, len(record.Files))
			for name, body := range record.Files {
				files[name] = []byte(body)
			}
			files[".env.example"] = []byte("ZEPHYR_ENDPOINT=" + endpoint + "\nZEPHYR_TOKEN=\nZEPHYR_TIMEOUT=10s\n")
		}
		if err := saveFiles(opts.output, files, opts.force); err != nil {
			if command == "publish" {
				return fmt.Errorf("workflow registered, but artifacts could not be saved (retry with --force): %w", err)
			}
			return err
		}
		return printJSON(out, map[string]any{"name": definition.Name, "version": opts.version, "output": opts.output, "published": command == "publish"})
	case "start", "status", "runs":
		if opts.version < 0 {
			return fmt.Errorf("version cannot be negative")
		}
		if command == "runs" && (opts.limit < 1 || opts.limit > 100 || opts.offset < 0) {
			return fmt.Errorf("limit must be 1-100 and offset non-negative")
		}
		var input map[string]any
		if command == "start" {
			body := []byte(opts.input)
			if strings.HasPrefix(opts.input, "@") {
				body, err = os.ReadFile(strings.TrimPrefix(opts.input, "@"))
				if err != nil {
					return fmt.Errorf("read input: %w", err)
				}
			}
			if err := json.Unmarshal(body, &input); err != nil || input == nil {
				return fmt.Errorf("input must be a JSON object")
			}
		}
		api, _, err := authenticatedClient(ctx, opts, stderr)
		if err != nil {
			return err
		}
		var result any
		switch command {
		case "start":
			if opts.key != "" {
				result, err = api.StartWorkflowWithIdempotencyKey(ctx, positionals[0], opts.version, input, opts.key)
			} else {
				result, err = api.StartWorkflow(ctx, positionals[0], opts.version, input)
			}
		case "status":
			result, err = api.WorkflowRunDetails(ctx, positionals[0])
		case "runs":
			result, err = api.ListWorkflowRuns(ctx, positionals[0], opts.limit, opts.offset, opts.status)
		}
		if err != nil {
			return describeAPIError(err)
		}
		return printJSON(out, result)
	default:
		return fmt.Errorf("unknown workflow command %q; use zephyr --help", command)
	}
}

func describeAPIError(err error) error {
	var failure *client.APIError
	if errors.As(err, &failure) && failure.StatusCode == http.StatusUnauthorized {
		return fmt.Errorf("%w; run zephyr auth login (or replace ZEPHYR_TOKEN if explicitly set)", err)
	}
	return err
}

func printJSON(out io.Writer, value any) error {
	encoder := json.NewEncoder(out)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}

// The standard flag parser stops at the first positional argument.
func orderFlags(args []string, flags *flag.FlagSet) ([]string, error) {
	var options, positional []string
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "--" {
			positional = append(positional, args[i+1:]...)
			break
		}
		if !strings.HasPrefix(arg, "-") {
			positional = append(positional, arg)
			continue
		}
		options = append(options, arg)
		name, _, assigned := strings.Cut(strings.TrimLeft(arg, "-"), "=")
		f := flags.Lookup(name)
		if f == nil {
			if name == "help" || name == "h" {
				continue
			}
			return nil, fmt.Errorf("unknown flag %q", arg)
		}
		boolean, ok := f.Value.(interface{ IsBoolFlag() bool })
		if !assigned && !(ok && boolean.IsBoolFlag()) {
			i++
			if i == len(args) {
				return nil, fmt.Errorf("flag %s requires a value", arg)
			}
			options = append(options, args[i])
		}
	}
	return append(options, positional...), nil
}

func checkFiles(directory string, files map[string][]byte, force bool) error {
	for name := range files {
		if name == "" || filepath.Base(name) != name || name == "." || name == ".." || strings.ContainsAny(name, `/\`) {
			return fmt.Errorf("unsafe artifact filename %q", name)
		}
		info, err := os.Lstat(filepath.Join(directory, name))
		if err == nil && (!force || !info.Mode().IsRegular()) {
			return fmt.Errorf("%s exists; use --force only to overwrite regular files", filepath.Join(directory, name))
		}
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}

func saveFiles(directory string, files map[string][]byte, force bool) error {
	if err := checkFiles(directory, files, force); err != nil {
		return err
	}
	if err := os.MkdirAll(directory, 0755); err != nil {
		return err
	}
	for name, body := range files {
		if err := atomicWriteMode(filepath.Join(directory, name), body, 0644, force); err != nil {
			return fmt.Errorf("write %s: %w", name, err)
		}
	}
	return nil
}

func authenticatedClient(ctx context.Context, opts options, stderr io.Writer) (*client.Client, string, error) {
	transport, err := httpClient(opts.caFile)
	if err != nil {
		return nil, "", err
	}
	source, endpoint, err := newSessionSource(opts, transport, stderr)
	if err != nil {
		return nil, "", err
	}
	config := client.Config{Endpoint: endpoint, Timeout: 15 * time.Second, HTTPClient: source.http}
	if token := os.Getenv("ZEPHYR_TOKEN"); token != "" {
		config.Token = token
	} else {
		config.TokenSource = source
		if _, err := source.Token(ctx); err != nil {
			return nil, "", err
		}
	}
	api, err := client.New(config)
	return api, endpoint, err
}
