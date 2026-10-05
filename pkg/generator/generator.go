package generator

import (
	"fmt"
	"go/format"
	"strings"
	"unicode"

	"github.com/zephyr-workflow/zephyr/pkg/dsl/ast"
	"github.com/zephyr-workflow/zephyr/pkg/dsl/parser"
)

type Files map[string][]byte

func Generate(source string) (Files, error) {
	file, err := parser.Parse(source)
	if err != nil {
		return nil, err
	}
	return GenerateFile(file)
}

func GenerateFile(file *ast.File) (Files, error) {
	if file == nil {
		return nil, fmt.Errorf("DSL file is required")
	}
	if err := validateGeneratedGoNames(file); err != nil {
		return nil, err
	}
	models, err := generateModels(file)
	if err != nil {
		return nil, err
	}
	workers, err := generateWorkers(file)
	if err != nil {
		return nil, err
	}
	clients, err := generateClients(file)
	if err != nil {
		return nil, err
	}
	pythonModels, err := generatePythonModels(file)
	if err != nil {
		return nil, err
	}
	pythonWorkers, err := generatePythonWorkers(file)
	if err != nil {
		return nil, err
	}
	typescript, err := generateTypeScript(file)
	if err != nil {
		return nil, err
	}
	return Files{
		"models.go":    models,
		"workers.go":   workers,
		"client.go":    clients,
		".env.example": []byte("ZEPHYR_ENDPOINT=http://localhost:8080\nZEPHYR_TOKEN=\nZEPHYR_TIMEOUT=10s\n"),
		"models.py":    pythonModels,
		"workers.py":   pythonWorkers,
		"models.ts":    typescript,
	}, nil
}

func validateGeneratedGoNames(file *ast.File) error {
	declarations := make(map[string]string)
	register := func(goName, sourceDeclaration string) error {
		if existing, ok := declarations[goName]; ok {
			return fmt.Errorf("generated Go identifier %q collides between %s and %s", goName, existing, sourceDeclaration)
		}
		declarations[goName] = sourceDeclaration
		return nil
	}

	for _, declaration := range file.Types {
		if err := register(exportName(declaration.Name), fmt.Sprintf("type %q", declaration.Name)); err != nil {
			return err
		}
		fields := make(map[string]string)
		for _, field := range declaration.Fields {
			goName := exportName(field.Name)
			if existing, ok := fields[goName]; ok {
				return fmt.Errorf("generated Go field %q collides within type %q between fields %q and %q", goName, declaration.Name, existing, field.Name)
			}
			fields[goName] = field.Name
		}
	}
	for _, task := range file.Tasks {
		if err := register(exportName(task.Name)+"Worker", fmt.Sprintf("task %q", task.Name)); err != nil {
			return err
		}
	}
	for _, workflow := range file.Workflows {
		clientName := exportName(workflow.Name) + "Client"
		if err := register(clientName, fmt.Sprintf("workflow %q", workflow.Name)); err != nil {
			return err
		}
		if err := register("New"+clientName, fmt.Sprintf("workflow %q", workflow.Name)); err != nil {
			return err
		}
	}
	return nil
}

func generateModels(file *ast.File) ([]byte, error) {
	var source strings.Builder
	source.WriteString("package generated\n\n")
	if hasDuration(file) {
		source.WriteString("import \"time\"\n\n")
	}
	for _, declaration := range file.Types {
		fmt.Fprintf(&source, "type %s struct {\n", exportName(declaration.Name))
		for _, field := range declaration.Fields {
			goType, err := goType(field.Type)
			if err != nil {
				return nil, err
			}
			fmt.Fprintf(&source, "\t%s %s `json:\"%s\"`\n", exportName(field.Name), goType, field.Name)
		}
		source.WriteString("}\n\n")
	}
	return format.Source([]byte(source.String()))
}

func generateWorkers(file *ast.File) ([]byte, error) {
	var source strings.Builder
	source.WriteString("package generated\n\nimport \"context\"\n\n")
	for _, task := range file.Tasks {
		inputName, inputType := splitParameter(task.Input)
		_ = inputName
		fmt.Fprintf(&source, "type %sWorker interface {\n", exportName(task.Name))
		fmt.Fprintf(&source, "\t%s(ctx context.Context, input %s) (output %s, err error)\n", exportName(task.Name), exportName(inputType), exportName(task.Output))
		source.WriteString("}\n\n")
	}
	return format.Source([]byte(source.String()))
}

func generateClients(file *ast.File) ([]byte, error) {
	var source strings.Builder
	source.WriteString("package generated\n")
	if len(file.Workflows) == 0 {
		return []byte(source.String()), nil
	}
	source.WriteString("\nimport (\n\t\"context\"\n\t\"encoding/json\"\n\t\"fmt\"\n\t\"time\"\n\n\tzephyrclient \"github.com/zephyr-workflow/zephyr/pkg/client\"\n)\n")
	for _, workflow := range file.Workflows {
		_, inputType := splitParameter(workflow.Input)
		outputType := exportName(workflow.Output)
		clientName := exportName(workflow.Name) + "Client"
		methodName := "Start" + exportName(workflow.Name)
		fmt.Fprintf(&source, "type %s struct {\n\tclient *zephyrclient.Client\n}\n\n", clientName)
		fmt.Fprintf(&source, "func New%s(config zephyrclient.Config) (*%s, error) {\n", clientName, clientName)
		source.WriteString("\tclient, err := zephyrclient.New(config)\n\tif err != nil {\n\t\treturn nil, err\n\t}\n")
		fmt.Fprintf(&source, "\treturn &%s{client: client}, nil\n}\n\n", clientName)
		fmt.Fprintf(&source, "func New%sFromEnv() (*%s, error) {\n", clientName, clientName)
		source.WriteString("\tconfig, err := zephyrclient.ConfigFromEnv()\n\tif err != nil {\n\t\treturn nil, err\n\t}\n")
		fmt.Fprintf(&source, "\treturn New%s(config)\n}\n\n", clientName)
		fmt.Fprintf(&source, "func (client *%s) %s(ctx context.Context, input %s) (string, error) {\n", clientName, methodName, exportName(inputType))
		fmt.Fprintf(&source, "\trun, err := client.client.StartWorkflow(ctx, %q, 1, input)\n", workflow.Name)
		source.WriteString("\tif err != nil {\n\t\treturn \"\", err\n\t}\n\treturn run.ID, nil\n}\n\n")
		fmt.Fprintf(&source, "func (client *%s) %sWithIdempotencyKey(ctx context.Context, input %s, key string) (string, error) {\n", clientName, methodName, exportName(inputType))
		fmt.Fprintf(&source, "\trun, err := client.client.StartWorkflowWithIdempotencyKey(ctx, %q, 1, input, key)\n", workflow.Name)
		source.WriteString("\tif err != nil {\n\t\treturn \"\", err\n\t}\n\treturn run.ID, nil\n}\n\n")
		fmt.Fprintf(&source, "func (client *%s) Get%sResult(ctx context.Context, workflowID string) (%s, error) {\n", clientName, exportName(workflow.Name), outputType)
		source.WriteString("\trun, err := client.client.GetWorkflowRun(ctx, workflowID)\n\tif err != nil {\n\t\treturn ")
		fmt.Fprintf(&source, "%s{}, err\n\t}\n\treturn client.decode%sResult(run)\n}\n\n", outputType, exportName(workflow.Name))
		fmt.Fprintf(&source, "func (client *%s) WaitFor%s(ctx context.Context, workflowID string, pollInterval time.Duration) (%s, error) {\n", clientName, exportName(workflow.Name), outputType)
		source.WriteString("\trun, err := client.client.WaitWorkflow(ctx, workflowID, pollInterval)\n\tif err != nil {\n\t\treturn ")
		fmt.Fprintf(&source, "%s{}, err\n\t}\n\treturn client.decode%sResult(run)\n}\n\n", outputType, exportName(workflow.Name))
		fmt.Fprintf(&source, "func (client *%s) decode%sResult(run zephyrclient.WorkflowRun) (%s, error) {\n", clientName, exportName(workflow.Name), outputType)
		fmt.Fprintf(&source, "\tif run.Status != \"COMPLETED\" {\n\t\treturn %s{}, fmt.Errorf(\"workflow %%s finished with status %%s\", run.ID, run.Status)\n\t}\n", outputType)
		source.WriteString("\tencoded, err := json.Marshal(run.Result)\n\tif err != nil {\n\t\treturn ")
		fmt.Fprintf(&source, "%s{}, fmt.Errorf(\"encode workflow result: %%w\", err)\n\t}\n\tvar result %s\n\tif err := json.Unmarshal(encoded, &result); err != nil {\n\t\treturn %s{}, fmt.Errorf(\"decode workflow result: %%w\", err)\n\t}\n\treturn result, nil\n}\n\n", outputType, outputType, outputType)
	}
	return format.Source([]byte(source.String()))
}

func goType(typeName string) (string, error) {
	if strings.HasPrefix(typeName, "list[") && strings.HasSuffix(typeName, "]") {
		item, err := goType(typeName[5 : len(typeName)-1])
		if err != nil {
			return "", err
		}
		return "[]" + item, nil
	}
	switch typeName {
	case "string":
		return "string", nil
	case "bool":
		return "bool", nil
	case "int":
		return "int", nil
	case "float":
		return "float64", nil
	case "duration":
		return "time.Duration", nil
	default:
		if typeName == "" {
			return "", fmt.Errorf("empty type name")
		}
		return exportName(typeName), nil
	}
}

func splitParameter(parameter string) (string, string) {
	parts := strings.SplitN(parameter, ":", 2)
	if len(parts) != 2 {
		return "", parameter
	}
	return parts[0], parts[1]
}

func exportName(name string) string {
	var result strings.Builder
	capitalize := true
	for _, character := range name {
		if character == '_' || character == '-' || character == ' ' {
			capitalize = true
			continue
		}
		if capitalize {
			result.WriteRune(unicode.ToUpper(character))
			capitalize = false
		} else {
			result.WriteRune(character)
		}
	}
	return result.String()
}

func hasDuration(file *ast.File) bool {
	for _, declaration := range file.Types {
		for _, field := range declaration.Fields {
			if strings.Contains(field.Type, "duration") {
				return true
			}
		}
	}
	return false
}

func generatePythonModels(file *ast.File) ([]byte, error) {
	var source strings.Builder
	source.WriteString("from pydantic import BaseModel\n\n")
	for _, declaration := range file.Types {
		fmt.Fprintf(&source, "class %s(BaseModel):\n", exportName(declaration.Name))
		if len(declaration.Fields) == 0 {
			source.WriteString("    pass\n\n")
			continue
		}
		for _, field := range declaration.Fields {
			fmt.Fprintf(&source, "    %s: %s\n", field.Name, pythonType(field.Type))
		}
		source.WriteString("\n")
	}
	return []byte(source.String()), nil
}

func generatePythonWorkers(file *ast.File) ([]byte, error) {
	var source strings.Builder
	source.WriteString("from typing import Protocol\n\n")
	for _, task := range file.Tasks {
		_, inputType := splitParameter(task.Input)
		fmt.Fprintf(&source, "class %sWorker(Protocol):\n", exportName(task.Name))
		fmt.Fprintf(&source, "    async def %s(self, input: %s) -> %s: ...\n\n", task.Name, exportName(inputType), exportName(task.Output))
	}
	return []byte(source.String()), nil
}

func generateTypeScript(file *ast.File) ([]byte, error) {
	var source strings.Builder
	for _, declaration := range file.Types {
		fmt.Fprintf(&source, "export interface %s {\n", exportName(declaration.Name))
		for _, field := range declaration.Fields {
			fmt.Fprintf(&source, "  %s: %s;\n", field.Name, typescriptType(field.Type))
		}
		source.WriteString("}\n\n")
	}
	for _, task := range file.Tasks {
		_, inputType := splitParameter(task.Input)
		fmt.Fprintf(&source, "export interface %sWorker {\n  %s(input: %s): Promise<%s>;\n}\n\n", exportName(task.Name), task.Name, exportName(inputType), exportName(task.Output))
	}
	return []byte(source.String()), nil
}

func pythonType(typeName string) string {
	if strings.HasPrefix(typeName, "list[") && strings.HasSuffix(typeName, "]") {
		return "list[" + pythonType(typeName[5:len(typeName)-1]) + "]"
	}
	switch typeName {
	case "string":
		return "str"
	case "bool":
		return "bool"
	case "int":
		return "int"
	case "float":
		return "float"
	default:
		return exportName(typeName)
	}
}

func typescriptType(typeName string) string {
	if strings.HasPrefix(typeName, "list[") && strings.HasSuffix(typeName, "]") {
		return typescriptType(typeName[5:len(typeName)-1]) + "[]"
	}
	switch typeName {
	case "string":
		return "string"
	case "bool":
		return "boolean"
	case "int", "float":
		return "number"
	default:
		return exportName(typeName)
	}
}
