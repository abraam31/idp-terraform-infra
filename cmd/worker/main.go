package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"io"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"infra-loc/internal/queue"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
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

	// Ensure writable artifacts root (default to /app/artifacts inside container)
	artifactsDir := getenv("ARTIFACTS_DIR", "/app/artifacts")
	if err := os.MkdirAll(artifactsDir, 0o755); err != nil {
		log.Printf("warn: unable to create artifacts dir %q: %v; falling back to os.TempDir()", artifactsDir, err)
		artifactsDir = filepath.Join(os.TempDir(), "infra-artifacts")
		_ = os.MkdirAll(artifactsDir, 0o755)
	}
	resultsRoot := getenv("TF_RESULTS_DIR", filepath.Join(artifactsDir, "results"))
	if err := os.MkdirAll(resultsRoot, 0o755); err != nil {
		log.Printf("warn: unable to create TF_RESULTS_DIR %q: %v; attempting fallback under /tmp", resultsRoot, err)
		resultsRoot = filepath.Join(os.TempDir(), "infra-artifacts", "results")
		_ = os.MkdirAll(resultsRoot, 0o755)
	}

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

		if err := processMessage(msg.Body, resultsRoot); err != nil {
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

func processMessage(body []byte, resultsRoot string) error {
	var job map[string]any
	if err := json.Unmarshal(body, &job); err != nil {
		return fmt.Errorf("parse job: %w", err)
	}
	jobID, _ := job["id"].(string)
	if jobID == "" {
		jobID = time.Now().UTC().Format("20060102T150405Z0700")
	}
	tfRef := strings.TrimSpace(asString(job["terraform_ref"]))
	if tfRef == "" {
		return fmt.Errorf("missing terraform_ref in message payload")
	}

	// Ensure terraform_ref exists locally, otherwise try terraform_s3_ref
	if info, err := os.Stat(tfRef); err != nil || !info.IsDir() {
		s3Ref := strings.TrimSpace(asString(job["terraform_s3_ref"]))
		if s3Ref == "" {
			return fmt.Errorf("terraform_ref path unavailable: %w", err)
		}
		log.Printf("local terraform_ref missing; attempting download from %s", s3Ref)
		awsRegion := getenv("AWS_REGION", "us-east-1")
		awsCfg, cfgErr := awsconfig.LoadDefaultConfig(context.Background(), awsconfig.WithRegion(awsRegion))
		if cfgErr != nil {
			return fmt.Errorf("init aws config failed: %w", cfgErr)
		}
		s3Client := s3.NewFromConfig(awsCfg)
		bucket, prefix, parseErr := parseS3URL(s3Ref)
		if parseErr != nil {
			return parseErr
		}
		dlDir := filepath.Join(resultsRoot, jobID, "input")
		if mkErr := os.MkdirAll(dlDir, 0o755); mkErr != nil {
			return fmt.Errorf("create download dir: %w", mkErr)
		}
		if dlErr := downloadS3PrefixToDir(context.Background(), s3Client, bucket, prefix, dlDir); dlErr != nil {
			return fmt.Errorf("download from s3 failed: %w", dlErr)
		}
		tfRef = dlDir
	}

	kind := strings.ToLower(strings.TrimSpace(asString(job["kind"])))
	if kind == "" {
		kind = "plan"
	}
	log.Printf("processing job_id=%s kind=%s terraform_ref=%s", jobID, kind, tfRef)

	// Log generated files for visibility
	if b, err := os.ReadFile(filepath.Join(tfRef, "backend.tf")); err == nil {
		log.Printf("==== backend.tf (%s) ====\n%s\n==== end backend.tf ====", tfRef, string(b))
	} else {
		log.Printf("warn: unable to read backend.tf: %v", err)
	}
	if b, err := os.ReadFile(filepath.Join(tfRef, "main.tf")); err == nil {
		log.Printf("==== main.tf (%s) ====\n%s\n==== end main.tf ====", tfRef, string(b))
	} else {
		log.Printf("warn: unable to read main.tf: %v", err)
	}

	jobResultDir := filepath.Join(resultsRoot, jobID)
	if err := os.MkdirAll(jobResultDir, 0o755); err != nil {
		return fmt.Errorf("create result dir: %w", err)
	}

	if err := runTerraform(tfRef, kind, jobResultDir); err != nil {
		return err
	}

	log.Printf("terraform finished job_id=%s result_dir=%s", jobID, jobResultDir)
	return nil
}

func runTerraform(workDir, kind, resultDir string) error {
	initOut, err := runCmd(workDir, "terraform", "init", "-input=false", "-no-color")
	if writeErr := os.WriteFile(filepath.Join(resultDir, "terraform-init.log"), []byte(initOut), 0o644); writeErr != nil {
		log.Printf("warn: failed to write terraform-init.log: %v", writeErr)
	}
	if err != nil {
		return fmt.Errorf("terraform init failed: %w", err)
	}

	switch kind {
	case "apply":
		applyOut, applyErr := runCmd(workDir, "terraform", "apply", "-auto-approve", "-input=false", "-no-color")
		if writeErr := os.WriteFile(filepath.Join(resultDir, "terraform-apply.log"), []byte(applyOut), 0o644); writeErr != nil {
			log.Printf("warn: failed to write terraform-apply.log: %v", writeErr)
		}
		if applyErr != nil {
			return fmt.Errorf("terraform apply failed: %w", applyErr)
		}
	default:
		planOut, planErr := runCmd(workDir, "terraform", "plan", "-input=false", "-no-color", "-out=tfplan.bin")
		if writeErr := os.WriteFile(filepath.Join(resultDir, "terraform-plan.log"), []byte(planOut), 0o644); writeErr != nil {
			log.Printf("warn: failed to write terraform-plan.log: %v", writeErr)
		}
		if planErr != nil {
			return fmt.Errorf("terraform plan failed: %w", planErr)
		}
		showOut, showErr := runCmd(workDir, "terraform", "show", "-no-color", "tfplan.bin")
		if writeErr := os.WriteFile(filepath.Join(resultDir, "terraform-plan.txt"), []byte(showOut), 0o644); writeErr != nil {
			log.Printf("warn: failed to write terraform-plan.txt: %v", writeErr)
		}
		if showErr != nil {
			return fmt.Errorf("terraform show failed: %w", showErr)
		}
	}
	return nil
}

func runCmd(workDir string, name string, args ...string) (string, error) {
	log.Printf("running command: %s %s (cwd=%s)", name, strings.Join(args, " "), workDir)
	cmd := exec.Command(name, args...)
	cmd.Dir = workDir
	out, err := cmd.CombinedOutput()
	output := string(out)
	if strings.TrimSpace(output) != "" {
		log.Printf("command output (%s):\n%s", name, output)
	}
	return output, err
}

func parseS3URL(u string) (string, string, error) {
	// Expect s3://bucket/prefix/
	s3url := strings.TrimSpace(u)
	if !strings.HasPrefix(s3url, "s3://") {
		return "", "", fmt.Errorf("invalid s3 url: %s", s3url)
	}
	rest := strings.TrimPrefix(s3url, "s3://")
	parts := strings.SplitN(rest, "/", 2)
	if len(parts) == 0 || strings.TrimSpace(parts[0]) == "" {
		return "", "", fmt.Errorf("invalid s3 url (bucket missing): %s", s3url)
	}
	bucket := parts[0]
	prefix := ""
	if len(parts) == 2 {
		prefix = strings.Trim(parts[1], "/")
	}
	return bucket, prefix, nil
}


func downloadS3PrefixToDir(ctx context.Context, s3Client *s3.Client, bucket, prefix string, localDir string) error {
	prefix = strings.Trim(prefix, "/")
	var continuation *string
	for {
		out, err := s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(bucket),
			Prefix:            aws.String(prefix),
			ContinuationToken: continuation,
		})
		if err != nil {
			return fmt.Errorf("could not list objects: %w", err)
		}
		for _, obj := range out.Contents {
			key := aws.ToString(obj.Key)
			if strings.HasSuffix(key, "/") {
				fmt.Printf("Skipping directory: %s\n", key)
				continue
			}
			rel := strings.TrimPrefix(key, prefix)
			rel = strings.TrimLeft(rel, "/")
			target := filepath.Join(localDir, filepath.FromSlash(rel))
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return fmt.Errorf("could not create directories: %w", err)
			}
			downloadOut, downloadErr := s3Client.GetObject(ctx, &s3.GetObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(key),
			})
			if downloadErr != nil {
				return fmt.Errorf("could not download object: %w", downloadErr)
			}

			data, readErr := io.ReadAll(downloadOut.Body)
			_ = downloadOut.Body.Close()
			if readErr != nil {
				return fmt.Errorf("could not read object body: %w", readErr)
			}
			if writeErr := os.WriteFile(target, data, 0o644); writeErr != nil {
				return fmt.Errorf("could not write file: %w", writeErr)
			}
		}
		if !aws.ToBool(out.IsTruncated) || out.NextContinuationToken == nil {
			break
		}
		continuation = out.NextContinuationToken
	}
	return nil
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

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

