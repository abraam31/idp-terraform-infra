package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"infra-loc/internal/queue"

	"gopkg.in/yaml.v3"
)

func main() {
	addr := getenv("API_ADDR", ":8080")
	version := getenv("VERSION", "dev")
	awsRegion := getenv("AWS_REGION", "us-east-1")
	queueURL := strings.TrimSpace(os.Getenv("SQS_QUEUE_URL"))
	if queueURL == "" {
		log.Fatal("SQS_QUEUE_URL is required")
	}
	apiToken := strings.TrimSpace(os.Getenv("IDP_API_TOKEN"))
	if apiToken == "" {
		log.Fatal("IDP_API_TOKEN is required")
	}
	sqsQueue, err := queue.NewSQSQueue(context.Background(), awsRegion, queueURL)
	if err != nil {
		log.Fatalf("failed to initialize sqs queue: %v", err)
	}
	samplePath := getenv("SAMPLE_YAML_PATH", "./sample.yaml")
	mockRepoRoot := getenv("MOCK_REPO_ROOT", "./mock-repo")
	fileFetcher := LocalRepoFetcher{Root: mockRepoRoot}

	mux := http.NewServeMux()
	mux.Handle("/healthz", withLogging(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})))
	mux.Handle("/readyz", withLogging(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready"))
	})))
	mux.Handle("/version", withLogging(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(version))
	})))
	// Webhook endpoint: MVP treats body as YAML file content from PR
	mux.Handle("/webhooks/git", withLogging(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqID := strings.TrimSpace(r.Header.Get("X-Request-Id"))
		if reqID == "" {
			reqID = "req-" + time.Now().UTC().Format("20060102T150405.000000000Z0700")
		}

		if r.Method != http.MethodPost {
			log.Printf("webhook rejected request_id=%s reason=method_not_allowed method=%s path=%s", reqID, r.Method, r.URL.Path)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if strings.TrimSpace(r.Header.Get("Authorization")) != "Bearer "+apiToken {
			log.Printf("webhook unauthorized request_id=%s remote_addr=%s user_agent=%q", reqID, r.RemoteAddr, r.UserAgent())
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		defer r.Body.Close()

		body, err := io.ReadAll(r.Body)
		if err != nil {
			log.Printf("webhook bad request request_id=%s reason=read_body_failed err=%v", reqID, err)
			http.Error(w, "unable to read body", http.StatusBadRequest)
			return
		}
		log.Printf("webhook accepted request_id=%s content_type=%q body_bytes=%d", reqID, r.Header.Get("Content-Type"), len(body))

		yamlBytes, err := resolveYAMLInput(r.Context(), r.Header.Get("Content-Type"), body, samplePath, fileFetcher)
		if err != nil {
			log.Printf("webhook bad request request_id=%s reason=resolve_yaml_input_failed err=%v", reqID, err)
			http.Error(w, "unable to resolve yaml input: "+err.Error(), http.StatusBadRequest)
			return
		}
		envName, specMap, err := parseEnvSpec(yamlBytes)
		if err != nil {
			log.Printf("webhook bad request request_id=%s reason=parse_env_spec_failed err=%v", reqID, err)
			http.Error(w, "invalid yaml: "+err.Error(), http.StatusBadRequest)
			return
		}

		awsMap, _ := specMap["aws"].(map[string]any)
		if len(awsMap) == 0 {
			log.Printf("webhook bad request request_id=%s reason=missing_aws_spec", reqID)
			http.Error(w, "aws is required and must contain at least one resource (body bytes="+
				fmt.Sprintf("%d", len(body))+")", http.StatusBadRequest)
			return
		}
		envCfg := resolveEnvConfig(envName)
		ts := time.Now().UTC().Format("20060102T150405Z0700")
		jobID := "job-" + ts
		// Save normalized spec as worker job artifact
		out := map[string]any{
			"id":         jobID,
			"time":       ts,
			"kind":       "plan",
			"filename":   "from-pr.yaml",
			"env":        envName,
			"env_config": envCfg,
			"spec":       specMap,
		}
		b, _ := json.MarshalIndent(out, "", "  ")
		if _, err := sqsQueue.Enqueue(r.Context(), "plan", b); err != nil {
			log.Printf("webhook enqueue failed request_id=%s queue_url=%q aws_region=%q err=%v", reqID, queueURL, awsRegion, err)
			http.Error(w, "failed to enqueue worker job", http.StatusInternalServerError)
			return
		}
		log.Printf("webhook enqueue success request_id=%s job_id=%s", reqID, jobID)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Request-Id", reqID)
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write(b)
	})))

	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		log.Printf("api listening on %s", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server error: %v", err)
		}
	}()

	// Graceful shutdown
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	log.Println("shutting down api...")

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("graceful shutdown failed: %v", err)
	}
	log.Println("api stopped")
}

func withLogging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		ww := &respWriter{ResponseWriter: w, status: 200}
		next.ServeHTTP(ww, r)
		dur := time.Since(start)
		log.Printf("%s %s %d %s", r.Method, r.URL.Path, ww.status, dur)
	})
}

type respWriter struct {
	http.ResponseWriter
	status int
}

func (w *respWriter) WriteHeader(statusCode int) {
	w.status = statusCode
	w.ResponseWriter.WriteHeader(statusCode)
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func parseEnvSpec(body []byte) (string, map[string]any, error) {
	var raw map[string]any
	if err := yaml.Unmarshal(body, &raw); err != nil {
		return "", nil, err
	}
	if len(raw) == 0 {
		return "", nil, fmt.Errorf("empty yaml")
	}
	// Form A: env: sandbox + aws: ...
	if envVal, ok := raw["env"]; ok {
		env := strings.TrimSpace(fmt.Sprintf("%v", envVal))
		spec := map[string]any{}
		for k, v := range raw {
			if k == "env" {
				continue
			}
			spec[k] = v
		}
		return env, spec, nil
	}
	return "", raw, nil
}

func resolveEnvConfig(env string) map[string]any {
	// MVP mapping. Move to external config file later.
	switch strings.ToLower(env) {
	case "prod", "production":
		return map[string]any{
			"region":          "us-east-1",
			"role_arn":        "arn:aws:iam::222222222222:role/idp-terraform-prod",
			"tf_state_bucket": "idp-tf-state-prod",
			"tf_lock_table":   "idp-tf-locks-prod",
			"tf_state_prefix": "workspaces/prod",
		}
	default:
		return map[string]any{
			"region":          "us-east-1",
			"role_arn":        "arn:aws:iam::111111111112:role/idp-terraform-sandbox",
			"tf_state_bucket": "idp-tf-state-sandbox-33",
			"tf_lock_table":   "idp-tf-locks-sandbox-33",
			"tf_state_prefix": "workspaces/sandbox",
		}
	}
}

type WebhookEvent struct {
	Repo         string   `json:"repo"`
	PRNumber     int      `json:"pr_number"`
	ChangedFiles []string `json:"changed_files"`
	FilePath     string   `json:"file_path"` // compatibility shim
	Content      string   `json:"content"`   // compatibility shim
}

type FileFetcher interface {
	FetchFile(ctx context.Context, repo string, prNumber int, filePath string) ([]byte, error)
}

type LocalRepoFetcher struct {
	Root string
}

func (f LocalRepoFetcher) FetchFile(_ context.Context, _ string, _ int, filePath string) ([]byte, error) {
	if strings.TrimSpace(filePath) == "" {
		return nil, errors.New("empty file path")
	}
	fsPath := filepath.Join(f.Root, filepath.Clean(filePath))
	return os.ReadFile(fsPath)
}

func resolveYAMLInput(ctx context.Context, contentType string, body []byte, samplePath string, fetcher FileFetcher) ([]byte, error) {
	// 1) Raw YAML body flow (manual testing / direct API calls)
	if !strings.Contains(contentType, "application/json") {
		trimmed := strings.TrimSpace(string(body))
		if trimmed != "" {
			return []byte(trimmed), nil
		}
		return os.ReadFile(samplePath)
	}

	// 2) Normal webhook flow (JSON event)
	evt, err := parseWebhookEvent(body)
	if err != nil {
		return nil, err
	}
	// Optional inline content shortcut
	if strings.TrimSpace(evt.Content) != "" {
		return []byte(strings.TrimSpace(evt.Content)), nil
	}

	candidates := inferInfraPaths(evt)
	for _, p := range candidates {
		if b, err := fetcher.FetchFile(ctx, evt.Repo, evt.PRNumber, p); err == nil && len(strings.TrimSpace(string(b))) > 0 {
			return b, nil
		}
	}

	// 3) Final fallback for local MVP
	return os.ReadFile(samplePath)
}

func parseWebhookEvent(body []byte) (WebhookEvent, error) {
	var evt WebhookEvent
	if err := json.Unmarshal(body, &evt); err != nil {
		return WebhookEvent{}, fmt.Errorf("invalid json webhook: %w", err)
	}
	return evt, nil
}

func inferInfraPaths(evt WebhookEvent) []string {
	out := make([]string, 0, len(evt.ChangedFiles)+1)
	for _, p := range evt.ChangedFiles {
		pp := filepath.ToSlash(strings.TrimSpace(p))
		if pp == "" {
			continue
		}
		if strings.HasSuffix(pp, "/infra.yaml") || strings.HasSuffix(pp, "/infra.yml") || pp == "infra.yaml" || pp == "infra.yml" {
			out = append(out, p)
		}
	}
	if len(out) == 0 && strings.TrimSpace(evt.FilePath) != "" {
		out = append(out, evt.FilePath)
	}
	return out
}
