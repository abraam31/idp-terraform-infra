package main

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestParseEnvSpec(t *testing.T) {
	env, spec, err := parseEnvSpec([]byte("env: sandbox\naws:\n  lb: []\n"))
	if err != nil {
		t.Fatal(err)
	}
	if env != "sandbox" {
		t.Errorf("env = %q, want sandbox", env)
	}
	if _, ok := spec["env"]; ok {
		t.Error("env key should be stripped from spec")
	}
	if _, ok := spec["aws"]; !ok {
		t.Error("aws key missing from spec")
	}

	if _, _, err := parseEnvSpec([]byte("")); err == nil {
		t.Error("expected error for empty yaml")
	}
}

func TestNormalizeRepo(t *testing.T) {
	tests := []struct {
		org, repo, want string
		wantErr         bool
	}{
		{"acme", "infra", "acme/infra", false},
		{"", "acme/infra", "acme/infra", false},
		{"", "infra", "", true},
		{"acme", "", "", true},
		{"acme", "a/b/c", "", true},
	}
	for _, tt := range tests {
		got, err := normalizeRepo(tt.org, tt.repo)
		if (err != nil) != tt.wantErr || got != tt.want {
			t.Errorf("normalizeRepo(%q, %q) = %q, %v", tt.org, tt.repo, got, err)
		}
	}
}

func TestNormalizeChangedFiles(t *testing.T) {
	got, err := normalizeChangedFiles([]string{`["a.yaml","b.yml","a.yaml"]`})
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"a.yaml", "b.yml"}; !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
	if _, err := normalizeChangedFiles([]string{" "}); err == nil {
		t.Error("expected error for empty changed_files")
	}
}

func TestParsePRNumber(t *testing.T) {
	for _, in := range []any{float64(42), "42"} {
		if n, err := parsePRNumber(in); err != nil || n != 42 {
			t.Errorf("parsePRNumber(%v) = %d, %v", in, n, err)
		}
	}
	for _, in := range []any{nil, float64(0), float64(1.5), "", "abc", true} {
		if _, err := parsePRNumber(in); err == nil {
			t.Errorf("parsePRNumber(%v) expected error", in)
		}
	}
}

func TestInferInfraPaths(t *testing.T) {
	evt := WebhookEvent{ChangedFiles: []string{"README.md", "envs/a.yaml", "envs/b.yml", "envs/a.yaml"}}
	got := inferInfraPaths(evt)
	if want := []string{"envs/a.yaml", "envs/b.yml"}; !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestSanitizeModuleName(t *testing.T) {
	tests := map[string]string{
		"My LB_01":  "my-lb-01",
		"api.lb!":   "apilb",
		"!!!":       "generated-lb",
		"public-lb": "public-lb",
	}
	for in, want := range tests {
		if got := sanitizeModuleName(in); got != want {
			t.Errorf("sanitizeModuleName(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestRenderTerraformFiles(t *testing.T) {
	root := t.TempDir()
	_, spec, err := parseEnvSpec([]byte(`
env: staging
aws:
  lb:
    - name: Public API
      type: application
      security-group-ids: [sg-1]
      subnets: [subnet-b, subnet-a]
      health-check-type: HTTP
      health-check-port: 8080
`))
	if err != nil {
		t.Fatal(err)
	}
	dir, err := renderTerraformFiles(root, "job-1", "staging", resolveEnvConfig("staging"), spec, "git@example.com:org/mods.git")
	if err != nil {
		t.Fatal(err)
	}

	for _, f := range []string{"spec.json", "backend.tf", "providers.tf", "main.tf"} {
		if _, err := os.Stat(filepath.Join(dir, f)); err != nil {
			t.Errorf("missing %s: %v", f, err)
		}
	}

	mainTf, _ := os.ReadFile(filepath.Join(dir, "main.tf"))
	for _, want := range []string{
		`module "public-api"`,
<<<<<<< CURRENT (yours)
		`source            = "git@example.com:org/mods.git//alb?ref=master"`,
=======
		`source            = "git@example.com:org/mods.git//alb?ref=main"`,
>>>>>>> PROPOSED (Claude)
		`subnets           = ["subnet-a", "subnet-b"]`,
		`security_groups   = ["sg-1"]`,
		`health_check_path = "HTTP:8080"`,
	} {
		if !strings.Contains(string(mainTf), want) {
			t.Errorf("main.tf missing %q\n%s", want, mainTf)
		}
	}

	backendTf, _ := os.ReadFile(filepath.Join(dir, "backend.tf"))
	if !strings.Contains(string(backendTf), `key            = "staging/job-1/terraform.tfstate"`) {
		t.Errorf("unexpected backend.tf:\n%s", backendTf)
	}
}
