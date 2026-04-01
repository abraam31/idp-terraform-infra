package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"infra-loc/internal/queue"
)

func main() {
	log.Println("worker starting")
	awsRegion := getenv("AWS_REGION", "us-east-1")
	queueURL := strings.TrimSpace(os.Getenv("SQS_QUEUE_URL"))
	if queueURL == "" {
		log.Fatal("SQS_QUEUE_URL is required.")
	}
	sqsQueue, err := queue.NewSQSQueue(context.Background(), awsRegion, queueURL)
	if err != nil {
		log.Fatalf("init sqs queue: %v", err)
	}

	artifactsDir := getenv("ARTIFACTS_DIR", "./artifacts")
	s3MockRoot := getenv("S3_MOCK_DIR", filepath.Join(artifactsDir, "s3", "tf-code"))
	moduleRepo := getenv("TF_MODULE_REPO", "git@github.com:localytics/devops-terraform-modules.git")
	_ = os.MkdirAll(s3MockRoot, 0o755)

	go serveHealth()
	log.Println("worker health endpoint listening on :9000 (/healthz)")

	for {
		msg, err := sqsQueue.Dequeue(context.Background())
		if err != nil {
			log.Printf("read sqs queue: %v", err)
			time.Sleep(5 * time.Second)
			continue
		}
		if msg == nil {
			time.Sleep(2 * time.Second)
			continue
		}

		if err := processMessage(msg.Body, s3MockRoot, moduleRepo); err != nil {
			log.Printf("message processing failed: %v", err)
			continue
		}
		if err := sqsQueue.MarkDone(context.Background(), msg.ReceiptHandle); err != nil {
			log.Printf("warn: failed to ack message: %v", err)
			continue
		}
		log.Println("worker completed one message")
	}
}

func serveHealth() {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	if err := http.ListenAndServe(":9000", mux); err != nil {
		log.Printf("health server stopped: %v", err)
	}
}

func processMessage(body []byte, s3MockRoot string, moduleRepo string) error {
	var job map[string]any
	if err := json.Unmarshal(body, &job); err != nil {
		return fmt.Errorf("parse job: %w", err)
	}
	jobID, _ := job["id"].(string)
	if jobID == "" {
		jobID = time.Now().UTC().Format("20060102T150405Z0700")
	}
	spec, _ := job["spec"].(map[string]any)
	awsMap, _ := spec["aws"].(map[string]any)
	envName, _ := job["env"].(string)
	envCfg, _ := job["env_config"].(map[string]any)

	outDir := filepath.Join(s3MockRoot, jobID)
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return fmt.Errorf("mk out dir: %w", err)
	}
	specBytes, _ := json.MarshalIndent(spec, "", "  ")
	if err := os.WriteFile(filepath.Join(outDir, "spec.json"), specBytes, 0o644); err != nil {
		return fmt.Errorf("write spec.json: %w", err)
	}
	if err := os.WriteFile(filepath.Join(outDir, "backend.tf"), []byte(buildBackendConfig(envName, envCfg, jobID)), 0o644); err != nil {
		return fmt.Errorf("write backend.tf: %w", err)
	}
	if err := os.WriteFile(filepath.Join(outDir, "providers.tf"), []byte(buildProviderConfig(envCfg)), 0o644); err != nil {
		return fmt.Errorf("write providers.tf: %w", err)
	}

	var mainTf string
	mainTf = terraformHeader() + "\n"
	if awsMap != nil {
		for resourceType := range awsMap {
			switch resourceType {
			case "lb":
				mainTf += renderLBModules(awsMap, moduleRepo)
			}
		}
	}
	if strings.TrimSpace(mainTf) == strings.TrimSpace(terraformHeader()) {
		mainTf += "# No supported resources found yet. Add aws.lb entries to generate module blocks.\n"
	}
	if err := os.WriteFile(filepath.Join(outDir, "main.tf"), []byte(mainTf), 0o644); err != nil {
		return fmt.Errorf("write main.tf: %w", err)
	}

	// Log generated files for visibility
	if b, err := os.ReadFile(filepath.Join(outDir, "backend.tf")); err == nil {
		log.Printf("==== backend.tf (%s) ====\n%s\n==== end backend.tf ====", outDir, string(b))
	} else {
		log.Printf("warn: unable to read backend.tf: %v", err)
	}
	if b, err := os.ReadFile(filepath.Join(outDir, "main.tf")); err == nil {
		log.Printf("==== main.tf (%s) ====\n%s\n==== end main.tf ====", outDir, string(b))
	} else {
		log.Printf("warn: unable to read main.tf: %v", err)
	}

	log.Printf("generated terraform in s3-mock dir: %s", outDir)
	return nil
}

func terraformHeader() string {
	return `terraform {
  required_version = ">= 1.5.0"
}
`
}

func buildBackendConfig(envName string, envCfg map[string]any, jobID string) string {
	var out strings.Builder
	bucket := asString(envCfg["tf_state_bucket"])
	lockTable := asString(envCfg["tf_lock_table"])
	prefix := strings.Trim(asString(envCfg["tf_state_prefix"]), "/")
	if prefix == "" {
		prefix = "workspaces/" + strings.ToLower(envName)
	}
	key := prefix + "/" + jobID + "/terraform.tfstate"
	region := asString(envCfg["region"])
	if region == "" {
		region = "us-east-1"
	}
	out.WriteString("terraform {\n")
	out.WriteString("  backend \"s3\" {\n")
	out.WriteString(fmt.Sprintf("    bucket         = %q\n", bucket))
	out.WriteString(fmt.Sprintf("    key            = %q\n", key))
	out.WriteString(fmt.Sprintf("    region         = %q\n", region))
	out.WriteString(fmt.Sprintf("    dynamodb_table = %q\n", lockTable))
	out.WriteString("    encrypt        = true\n")
	out.WriteString("  }\n")
	out.WriteString("}\n")
	return out.String()
}

func buildProviderConfig(envCfg map[string]any) string {
	region := asString(envCfg["region"])
	if region == "" {
		region = "us-east-1"
	}
	roleArn := asString(envCfg["role_arn"])
	var out strings.Builder
	out.WriteString("provider \"aws\" {\n")
	out.WriteString(fmt.Sprintf("  region = %q\n", region))
	if roleArn != "" {
		out.WriteString("  assume_role {\n")
		out.WriteString(fmt.Sprintf("    role_arn = %q\n", roleArn))
		out.WriteString("  }\n")
	}
	out.WriteString("}\n")
	return out.String()
}
func renderLBModules(awsMap map[string]any, moduleRepo string) string {
	if len(awsMap) == 0 {
		return ""
	}
	lbRaw, ok := awsMap["lb"]
	if !ok {
		return ""
	}
	lbs, ok := lbRaw.([]any)
	if !ok {
		return ""
	}

	var out strings.Builder
	for _, item := range lbs {
		lb, ok := item.(map[string]any)
		if !ok {
			continue
		}
		name := asString(lb["name"])
		if name == "" {
			continue
		}
		lbType := asString(lb["type"])
		modulePath := "clb"
		if strings.EqualFold(lbType, "application") || strings.EqualFold(lbType, "alb") {
			modulePath = "alb"
		}
		healthType := asString(lb["health-check-type"])
		healthPort := asString(lb["health-check-port"])
		if healthPort == "" {
			if n, ok := lb["health-check-port"].(float64); ok {
				healthPort = fmt.Sprintf("%.0f", n)
			}
		}
		health := strings.Trim(strings.TrimSpace(healthType)+":"+strings.TrimSpace(healthPort), ":")

		sg := toStringSlice(lb["security-group-id"], lb["security-group-ids"])
		subnets := toStringSlice(lb["subnets"])
		sort.Strings(subnets)

		modName := sanitizeModuleName(name)
		out.WriteString(fmt.Sprintf("module %q {\n", modName))
		out.WriteString(fmt.Sprintf("  source            = %q\n", moduleRepo+"//"+modulePath+"?ref=master"))
		out.WriteString(fmt.Sprintf("  name              = %q\n", name))
		if len(sg) > 0 {
			out.WriteString(fmt.Sprintf("  security_groups   = %s\n", tfList(sg)))
		}
		if len(subnets) > 0 {
			out.WriteString(fmt.Sprintf("  subnets           = %s\n", tfList(subnets)))
		}
		if health != "" {
			out.WriteString(fmt.Sprintf("  health_check_path = %q\n", health))
		}
		out.WriteString("}\n\n")
	}
	return out.String()
}

func toStringSlice(v ...any) []string {
	var out []string
	for _, one := range v {
		switch t := one.(type) {
		case string:
			if t != "" {
				out = append(out, t)
			}
		case []any:
			for _, e := range t {
				if s, ok := e.(string); ok && s != "" {
					out = append(out, s)
				}
			}
		}
	}
	return out
}

func tfList(items []string) string {
	if len(items) == 0 {
		return "[]"
	}
	quoted := make([]string, 0, len(items))
	for _, it := range items {
		quoted = append(quoted, fmt.Sprintf("%q", it))
	}
	return "[" + strings.Join(quoted, ", ") + "]"
}

func asString(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case float64:
		return fmt.Sprintf("%.0f", t)
	default:
		return ""
	}
}

func sanitizeModuleName(name string) string {
	name = strings.ToLower(name)
	name = strings.ReplaceAll(name, " ", "-")
	name = strings.ReplaceAll(name, "_", "-")
	var b strings.Builder
	for _, r := range name {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' {
			b.WriteRune(r)
		}
	}
	out := b.String()
	if out == "" {
		return "generated-lb"
	}
	return out
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

