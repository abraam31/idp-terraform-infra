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
	"sort"
	"strings"
	"syscall"
	"time"

	"infra-loc/internal/queue"

	"bytes"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
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
	artifactsBucket := strings.TrimSpace(os.Getenv("ARTIFACTS_BUCKET"))
	artifactsPrefix := strings.Trim(strings.TrimSpace(getenv("ARTIFACTS_PREFIX", "jobs")), "/")
	apiToken := strings.TrimSpace(os.Getenv("IDP_API_TOKEN"))
	if apiToken == "" {
		log.Fatal("IDP_API_TOKEN is required")
	}
	sqsQueue, err := queue.NewSQSQueue(context.Background(), awsRegion, queueURL)
	if err != nil {
		log.Fatalf("failed to initialize sqs queue: %v", err)
	}
	// Optional S3 client (only if bucket is configured)
	var s3Client *s3.Client
	if artifactsBucket != "" {
		awsCfg, err := awsconfig.LoadDefaultConfig(context.Background(), awsconfig.WithRegion(awsRegion))
		if err != nil {
			log.Fatalf("failed to init aws config for s3: %v", err)
		}
		s3Client = s3.NewFromConfig(awsCfg)
		log.Printf("s3 uploader enabled bucket=%q prefix=%q", artifactsBucket, artifactsPrefix)
	} else {
		log.Printf("s3 uploader disabled: ARTIFACTS_BUCKET not set")
	}
	samplePath := getenv("SAMPLE_YAML_PATH", "./sample.yaml")
	artifactsDir := getenv("ARTIFACTS_DIR", "/app/artifacts")
	tfRenderRoot := getenv("TF_RENDER_DIR", filepath.Join(artifactsDir, "jobs"))
<<<<<<< CURRENT (yours)
	moduleRepo := getenv("TF_MODULE_REPO", "git@github.com:your-org/terraform-modules.git")
=======
	moduleRepo := getenv("TF_MODULE_REPO", "git::https://github.com/abraam31/terraform-modules.git")
>>>>>>> PROPOSED (Claude)
	if err := os.MkdirAll(tfRenderRoot, 0o755); err != nil {
		log.Fatalf("failed to create TF_RENDER_DIR %q: %v", tfRenderRoot, err)
	}
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
			"spec":       specMap, // kept for traceability/debugging
		}
		jobDir, err := renderTerraformFiles(tfRenderRoot, jobID, envName, envCfg, specMap, moduleRepo)
		if err != nil {
			log.Printf("webhook render failed request_id=%s job_id=%s err=%v", reqID, jobID, err)
			http.Error(w, "failed to render terraform files", http.StatusInternalServerError)
			return
		}
		// Print rendered files for visibility
		if b, err := os.ReadFile(filepath.Join(jobDir, "backend.tf")); err == nil {
			log.Printf("==== backend.tf (%s) ====\n%s\n==== end backend.tf ====", jobDir, string(b))
		} else {
			log.Printf("warn: unable to read backend.tf: %v", err)
		}
		if b, err := os.ReadFile(filepath.Join(jobDir, "main.tf")); err == nil {
			log.Printf("==== main.tf (%s) ====\n%s\n==== end main.tf ====", jobDir, string(b))
		} else {
			log.Printf("warn: unable to read main.tf: %v", err)
		}
		out["terraform_ref"] = jobDir
		// If configured, upload rendered files to S3 and attach a cloud reference as well.
		if s3Client != nil {
			s3Prefix := artifactsPrefix
			if s3Prefix != "" {
				s3Prefix = s3Prefix + "/" + jobID
			} else {
				s3Prefix = jobID
			}
			log.Printf("webhook s3 upload start request_id=%s job_id=%s bucket=%s prefix=%s dir=%s", reqID, jobID, artifactsBucket, s3Prefix, jobDir)
			n, err := uploadDirectoryToS3(r.Context(), s3Client, artifactsBucket, s3Prefix, jobDir)
			if err != nil {
				log.Printf("webhook s3 upload FAILED request_id=%s job_id=%s bucket=%s prefix=%s err=%v", reqID, jobID, artifactsBucket, s3Prefix, err)
				// Do not fail the request; local ref still valid.
			} else {
				log.Printf("webhook s3 upload success request_id=%s job_id=%s uploaded_files=%d s3=%s/%s", reqID, jobID, n, artifactsBucket, s3Prefix)
				out["terraform_s3_ref"] = fmt.Sprintf("s3://%s/%s/", artifactsBucket, s3Prefix)
			}
		} else {
			log.Printf("webhook s3 upload skipped request_id=%s job_id=%s reason=s3_not_configured", reqID, jobID)
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
			"tf_state_bucket": "idp-prod-tfstate",
			"tf_state_prefix": "prod",
		}
	default:
		return map[string]any{
			"region":          "us-east-1",
			"tf_state_bucket": "idp-staging-tfstate",
			"tf_state_prefix": "staging",
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
	EventName    string         `json:"event_name"`
	Action       string         `json:"action"`
	Source       string         `json:"source"` // github | local | auto (default)
	Repository   string         `json:"repository"`
	Organization string         `json:"organization"`
	SHA          string         `json:"sha"`
	Ref          string         `json:"ref"`
	Actor        string         `json:"actor"`
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
	log.Printf("github fetch attempt repo=%q ref=%q path=%q url=%q", repo, abbrev(ref), filePath, u)
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
		return nil, fmt.Errorf("github api %s returned %d for path=%s ref=%s: %s", u, resp.StatusCode, filePath, abbrev(ref), strings.TrimSpace(string(respBody)))
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

func abbrev(s string) string {
	s = strings.TrimSpace(s)
	if len(s) <= 8 {
		return s
	}
	return s[:8]
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
	// Normalize content type and log branch decision for unambiguous tracing
	ctLower := strings.ToLower(strings.TrimSpace(contentType))
	isJSON := strings.Contains(ctLower, "application/json")
	log.Printf("resolveYAMLInput content_type_seen=%q json_branch=%t", contentType, isJSON)

	// 1) Raw YAML body flow (manual testing / direct API calls)
	if !isJSON {
		trimmed := strings.TrimSpace(string(body))
		if trimmed == "" {
			return nil, fmt.Errorf("non-json payload provided with empty body; raw yaml is required for non-json requests (no sample fallback). Body: %s", string(body))
		}
		return []byte(trimmed), nil
	}

	// 2) Normal webhook flow (JSON event)
	evt, err := parseWebhookEvent(body)
	if err != nil {
		return nil, err
	}
	// Log parsed basic metadata for observability
	log.Printf("webhook parsed event event_name=%q action=%q source=%q repo=%q pr_number=%d changed_files=%d",
		evt.EventName, evt.Action, evt.Source, evt.Repo, evt.PRNumber, len(evt.ChangedFiles))

	// Log raw changed_files for debugging
	log.Printf("Body of the webhook: %s", string(body))
	log.Printf("webhook changed_files raw=%v", evt.ChangedFiles)

	candidates := inferInfraPaths(evt)
	// If no YAML-looking candidates but we did receive changed_files, fallback to try all provided paths.
	if len(candidates) == 0 && len(evt.ChangedFiles) > 0 {
		log.Printf("webhook no yaml-looking candidates; falling back to try all changed files")
		candidates = append([]string{}, evt.ChangedFiles...)
	}
	log.Printf("webhook candidates count=%d list=%v repo=%q sha=%q ref=%q source=%q",
		len(candidates), candidates, evt.Repo, abbrev(evt.SHA), evt.Ref, evt.Source)
	// If explicitly github but no token set, fail fast with clear message
	if strings.EqualFold(strings.TrimSpace(evt.Source), "github") && strings.TrimSpace(os.Getenv("GITHUB_TOKEN")) == "" {
		return nil, fmt.Errorf("source=github requested but GITHUB_TOKEN is not configured in API runtime")
	}
	if b, path, err := eventFetcher.FetchFirst(ctx, evt, candidates); err == nil {
		log.Printf("webhook yaml resolved path=%q", path)
		return b, nil
	} else if err != nil {
		log.Printf("webhook fetch_first failed repo=%q sha=%q ref=%q source=%q err=%v",
			evt.Repo, abbrev(evt.SHA), evt.Ref, evt.Source, err)
	}
	// No candidate succeeded for JSON payloads. Do not silently fall back.
	return nil, fmt.Errorf("no yaml resolved from candidates (source=%s, candidates=%v); ensure files exist and are accessible via selected source", evt.Source, candidates)
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

func renderTerraformFiles(tfRenderRoot, jobID, envName string, envCfg, spec map[string]any, moduleRepo string) (string, error) {
	outDir := filepath.Join(tfRenderRoot, jobID)
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return "", fmt.Errorf("mk out dir: %w", err)
	}
	specBytes, _ := json.MarshalIndent(spec, "", "  ")
	if err := os.WriteFile(filepath.Join(outDir, "spec.json"), specBytes, 0o644); err != nil {
		return "", fmt.Errorf("write spec.json: %w", err)
	}
	if err := os.WriteFile(filepath.Join(outDir, "backend.tf"), []byte(buildBackendConfig(envName, envCfg, jobID)), 0o644); err != nil {
		return "", fmt.Errorf("write backend.tf: %w", err)
	}
	if err := os.WriteFile(filepath.Join(outDir, "providers.tf"), []byte(buildProviderConfig(envCfg)), 0o644); err != nil {
		return "", fmt.Errorf("write providers.tf: %w", err)
	}

	awsMap, _ := spec["aws"].(map[string]any)
	mainTf := terraformHeader() + "\n"
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
		return "", fmt.Errorf("write main.tf: %w", err)
	}
	return outDir, nil
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
<<<<<<< CURRENT (yours)
		out.WriteString(fmt.Sprintf("  source            = %q\n", moduleRepo+"//"+modulePath+"?ref=master"))
=======
		out.WriteString(fmt.Sprintf("  source            = %q\n", moduleRepo+"//"+modulePath+"?ref=main"))
>>>>>>> PROPOSED (Claude)
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
	case int:
		return fmt.Sprintf("%d", t)
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

func uploadDirectoryToS3(ctx context.Context, s3Client *s3.Client, bucket, prefix, localDir string) (int, error) {
	prefix = strings.Trim(prefix, "/")
	count := 0
	errWalk := filepath.Walk(localDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(localDir, path)
		if err != nil {
			return err
		}
		key := rel
		if prefix != "" {
			key = prefix + "/" + filepath.ToSlash(rel)
		} else {
			key = filepath.ToSlash(rel)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		var contentType *string
		switch {
		case strings.HasSuffix(strings.ToLower(path), ".tf"):
			ct := "text/plain"
			contentType = &ct
		case strings.HasSuffix(strings.ToLower(path), ".json"):
			ct := "application/json"
			contentType = &ct
		default:
			// let S3 infer or default; leaving nil
		}
		_, err = s3Client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:      aws.String(bucket),
			Key:         aws.String(key),
			Body:        bytesReader(data),
			ContentType: contentType,
			ACL:         types.ObjectCannedACLPrivate,
		})
		if err != nil {
			return fmt.Errorf("upload %s to s3://%s/%s: %w", path, bucket, key, err)
		}
		count++
		return nil
	})
	if errWalk != nil {
		return count, errWalk
	}
	return count, nil
}

// bytesReader wraps a byte slice for io.Reader without extra allocations.
func bytesReader(b []byte) io.Reader {
	return bytes.NewReader(b)
}
