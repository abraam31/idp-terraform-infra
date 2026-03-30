package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
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
	log.Printf("Dummy Line to confirm the build is working")
	samplePath := getenv("SAMPLE_YAML_PATH", "./sample.yaml")
	mockRepoRoot := getenv("MOCK_REPO_ROOT", "./mock-repo")
	localFetcher := LocalRepoFetcher{Root: mockRepoRoot}
	var githubFetcher FileFetcher
	githubToken := strings.TrimSpace(os.Getenv("GITHUB_TOKEN"))
	if githubToken != "" {
		githubFetcher = GitHubRepoFetcher{
			Token:   githubToken,
			BaseURL: getenv("GITHUB_API_BASE_URL", "https://api.github.com"),
			Client:  &http.Client{Timeout: 15 * time.Second},
		}
		log.Printf("github file fetcher enabled")
	} else {
		log.Printf("github file fetcher disabled: GITHUB_TOKEN not set")
	}
	eventFetcher := SourceAwareFetcher{
		Local:  localFetcher,
		GitHub: githubFetcher,
	}

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

		yamlBytes, err := resolveYAMLInput(r.Context(), r.Header.Get("Content-Type"), body, samplePath, eventFetcher)
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
	EventName    string   `json:"event_name"`
	Action       string   `json:"action"`
	Source       string   `json:"source"`
	Repo         string   `json:"repo"`
	PRNumber     int      `json:"pr_number"`
	SHA          string   `json:"sha"`
	Ref          string   `json:"ref"`
	ChangedFiles []string `json:"changed_files"`
}

// DownstreamEvent represents the new JSON schema sent by the GitHub Action.
// This replaces the previous POC shape. No backward compatibility is kept.
type DownstreamEvent struct {
	EventName    string            `json:"event_name"`
	Action       string            `json:"action"`
	Source       string            `json:"source"` // github | local | auto (default)
	Repository   string            `json:"repository"`
	Organization string            `json:"organization"`
	SHA          string            `json:"sha"`
	Ref          string            `json:"ref"`
	Actor        string            `json:"actor"`
	Run          map[string]any `json:"run"` // contains id, attempt
	PullRequest  struct {
		Number any    `json:"number"`
		URL    string `json:"url"`
	} `json:"pull_request"`
	ChangedFiles []string `json:"changed_files"`
}

type FileFetcher interface {
	FetchFile(ctx context.Context, repo string, prNumber int, ref string, filePath string) ([]byte, error)
}

type EventFileFetcher interface {
	FetchFirst(ctx context.Context, evt WebhookEvent, candidates []string) ([]byte, string, error)
}

type LocalRepoFetcher struct {
	Root string
}

func (f LocalRepoFetcher) FetchFile(_ context.Context, _ string, _ int, _ string, filePath string) ([]byte, error) {
	if strings.TrimSpace(filePath) == "" {
		return nil, errors.New("empty file path")
	}
	fsPath := filepath.Join(f.Root, filepath.Clean(filePath))
	return os.ReadFile(fsPath)
}

type GitHubRepoFetcher struct {
	Token   string
	BaseURL string
	Client  *http.Client
}

func (f GitHubRepoFetcher) FetchFile(ctx context.Context, repo string, _ int, ref string, filePath string) ([]byte, error) {
	repo = strings.TrimSpace(repo)
	filePath = strings.TrimSpace(filePath)
	ref = strings.TrimSpace(ref)
	if repo == "" || filePath == "" || ref == "" {
		return nil, fmt.Errorf("repo, file path and sha/ref are required")
	}
	base := strings.TrimRight(strings.TrimSpace(f.BaseURL), "/")
	if base == "" {
		base = "https://api.github.com"
	}
	client := f.Client
	if client == nil {
		client = &http.Client{Timeout: 15 * time.Second}
	}
	u := fmt.Sprintf("%s/repos/%s/contents/%s?ref=%s", base, repo, encodeGitHubPath(filePath), url.QueryEscape(ref))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+f.Token)
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("github api returned %d for %s: %s", resp.StatusCode, filePath, strings.TrimSpace(string(respBody)))
	}
	var payload struct {
		Content  string `json:"content"`
		Encoding string `json:"encoding"`
	}
	if err := json.Unmarshal(respBody, &payload); err != nil {
		return nil, fmt.Errorf("invalid github content response: %w", err)
	}
	if payload.Encoding != "base64" {
		return nil, fmt.Errorf("unsupported github content encoding: %q", payload.Encoding)
	}
	decoded, err := base64.StdEncoding.DecodeString(strings.ReplaceAll(payload.Content, "\n", ""))
	if err != nil {
		return nil, fmt.Errorf("failed to decode github file content: %w", err)
	}
	return decoded, nil
}

func encodeGitHubPath(p string) string {
	parts := strings.Split(filepath.ToSlash(strings.TrimSpace(p)), "/")
	encoded := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		encoded = append(encoded, url.PathEscape(part))
	}
	return strings.Join(encoded, "/")
}

type SourceAwareFetcher struct {
	Local  FileFetcher
	GitHub FileFetcher
}

func (f SourceAwareFetcher) FetchFirst(ctx context.Context, evt WebhookEvent, candidates []string) ([]byte, string, error) {
	fetchers := f.fetchersForSource(evt.Source)
	if len(fetchers) == 0 {
		return nil, "", fmt.Errorf("no file fetcher available for source=%q", evt.Source)
	}
	for _, p := range candidates {
		for idx, fetcher := range fetchers {
			if b, err := fetcher.FetchFile(ctx, evt.Repo, evt.PRNumber, evt.SHA, p); err == nil && len(strings.TrimSpace(string(b))) > 0 {
				return b, p, nil
			} else if err != nil {
				log.Printf("webhook fetch failed source=%q path=%q fetcher_index=%d err=%v", evt.Source, p, idx, err)
			}
		}
	}
	return nil, "", errors.New("no yaml content resolved from changed_files")
}

func (f SourceAwareFetcher) fetchersForSource(source string) []FileFetcher {
	mode := strings.ToLower(strings.TrimSpace(source))
	switch mode {
	case "github":
		if f.GitHub != nil {
			return []FileFetcher{f.GitHub}
		}
		return nil
	case "local":
		if f.Local != nil {
			return []FileFetcher{f.Local}
		}
		return nil
	case "", "auto":
		out := make([]FileFetcher, 0, 2)
		if f.GitHub != nil {
			out = append(out, f.GitHub)
		}
		if f.Local != nil {
			out = append(out, f.Local)
		}
		return out
	default:
		return f.fetchersForSource("auto")
	}
}

func resolveYAMLInput(
	ctx context.Context,
	contentType string,
	body []byte,
	samplePath string,
	eventFetcher EventFileFetcher,
) ([]byte, error) {
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
	// Log parsed basic metadata for observability
	log.Printf("webhook parsed event event_name=%q action=%q source=%q repo=%q pr_number=%d changed_files=%d",
		evt.EventName, evt.Action, evt.Source, evt.Repo, evt.PRNumber, len(evt.ChangedFiles))

	candidates := inferInfraPaths(evt)
	if b, path, err := eventFetcher.FetchFirst(ctx, evt, candidates); err == nil {
		log.Printf("webhook yaml source=changed_files path=%q", path)
		return b, nil
	}

	// 3) Final fallback for local MVP
	return os.ReadFile(samplePath)
}

func parseWebhookEvent(body []byte) (WebhookEvent, error) {
	var d DownstreamEvent
	if err := json.Unmarshal(body, &d); err != nil {
		return WebhookEvent{}, fmt.Errorf("invalid json webhook: %w", err)
	}
	prNum, err := parsePRNumber(d.PullRequest.Number)
	if err != nil {
		return WebhookEvent{}, err
	}
	repoFull, err := normalizeRepo(strings.TrimSpace(d.Organization), strings.TrimSpace(d.Repository))
	if err != nil {
		return WebhookEvent{}, err
	}
	changedFiles, err := normalizeChangedFiles(d.ChangedFiles)
	if err != nil {
		return WebhookEvent{}, err
	}
	return WebhookEvent{
		EventName:    strings.TrimSpace(d.EventName),
		Action:       strings.TrimSpace(d.Action),
		Source:       strings.TrimSpace(d.Source),
		Repo:         repoFull,
		PRNumber:     prNum,
		SHA:          strings.TrimSpace(d.SHA),
		Ref:          strings.TrimSpace(d.Ref),
		ChangedFiles: changedFiles,
	}, nil
}

func normalizeRepo(org, repository string) (string, error) {
	repo := strings.TrimSpace(repository)
	if repo == "" {
		return "", fmt.Errorf("repository is required")
	}
	// Accept either "repo-name" or "org/repo-name".
	if strings.Count(repo, "/") == 1 {
		return repo, nil
	}
	if org == "" {
		return "", fmt.Errorf("organization is required when repository is not in org/repo form")
	}
	if strings.Contains(repo, "/") {
		return "", fmt.Errorf("invalid repository format: %q", repo)
	}
	return org + "/" + repo, nil
}

func normalizeChangedFiles(in []string) ([]string, error) {
	// Some senders accidentally pass changed_files as a one-item stringified JSON array.
	// Accept both:
	// 1) ["a.yaml","b.yml"]
	// 2) ["[\"a.yaml\",\"b.yml\"]"]
	if len(in) == 1 {
		raw := strings.TrimSpace(in[0])
		if strings.HasPrefix(raw, "[") && strings.HasSuffix(raw, "]") {
			var parsed []string
			if err := json.Unmarshal([]byte(raw), &parsed); err == nil {
				in = parsed
			}
		}
	}
	out := make([]string, 0, len(in))
	seen := map[string]struct{}{}
	for _, p := range in {
		pp := filepath.ToSlash(strings.TrimSpace(p))
		if pp == "" {
			continue
		}
		if _, ok := seen[pp]; ok {
			continue
		}
		seen[pp] = struct{}{}
		out = append(out, pp)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("changed_files must include at least one path")
	}
	return out, nil
}

func parsePRNumber(v any) (int, error) {
	if v == nil {
		return 0, fmt.Errorf("missing pull_request.number")
	}
	switch n := v.(type) {
	case float64:
		if n <= 0 || n != float64(int(n)) {
			return 0, fmt.Errorf("invalid pull_request.number: %v", n)
		}
		return int(n), nil
	case string:
		s := strings.TrimSpace(n)
		if s == "" {
			return 0, fmt.Errorf("missing pull_request.number")
		}
		var parsed int
		if _, err := fmt.Sscanf(s, "%d", &parsed); err != nil || parsed <= 0 {
			return 0, fmt.Errorf("invalid pull_request.number: %v", s)
		}
		return parsed, nil
	default:
		return 0, fmt.Errorf("invalid pull_request.number type: %T", v)
	}
}

func inferInfraPaths(evt WebhookEvent) []string {
	out := make([]string, 0, len(evt.ChangedFiles))
	seen := map[string]struct{}{}
	for _, p := range evt.ChangedFiles {
		pp := filepath.ToSlash(strings.TrimSpace(p))
		if pp == "" {
			continue
		}
		// Generic for POC: consider any yaml/yml from changed_files.
		if strings.HasSuffix(pp, ".yaml") || strings.HasSuffix(pp, ".yml") {
			if _, ok := seen[pp]; ok {
				continue
			}
			seen[pp] = struct{}{}
			out = append(out, pp)
		}
	}
	return out
}
