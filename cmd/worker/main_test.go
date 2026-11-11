package main

import "testing"

func TestParseS3URL(t *testing.T) {
	tests := []struct {
		in, bucket, prefix string
		wantErr            bool
	}{
		{"s3://bucket/jobs/job-1/", "bucket", "jobs/job-1", false},
		{"s3://bucket", "bucket", "", false},
		{"https://bucket/x", "", "", true},
		{"s3:///x", "", "", true},
	}
	for _, tt := range tests {
		b, p, err := parseS3URL(tt.in)
		if (err != nil) != tt.wantErr || b != tt.bucket || p != tt.prefix {
			t.Errorf("parseS3URL(%q) = %q, %q, %v", tt.in, b, p, err)
		}
	}
}
