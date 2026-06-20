package controller

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

// TestParseBackupTimestampMessage is a table-driven test for
// parseBackupTimestampMessage: it extracts the 14-digit gpbackup timestamp that
// follows the "BACKUP_TIMESTAMP=" marker (validated via util.IsGpbackupTimestamp)
// and returns ("", false) when the marker is missing or the value is malformed.
func TestParseBackupTimestampMessage(t *testing.T) {
	tests := []struct {
		name    string
		message string
		wantTS  string
		wantOK  bool
	}{
		{
			name:    "valid marker with 14-digit timestamp",
			message: "BACKUP_TIMESTAMP=20260620092039",
			wantTS:  "20260620092039",
			wantOK:  true,
		},
		{
			name:    "valid marker embedded in a log tail",
			message: "some gpbackup log output\nBACKUP_TIMESTAMP=20260620092039\n",
			wantTS:  "20260620092039",
			wantOK:  true,
		},
		{
			name:    "marker with trailing non-digit content after timestamp",
			message: "BACKUP_TIMESTAMP=20260620092039 done",
			wantTS:  "20260620092039",
			wantOK:  true,
		},
		{
			name:    "last marker wins when repeated",
			message: "BACKUP_TIMESTAMP=20200101010101\nBACKUP_TIMESTAMP=20260620092039",
			wantTS:  "20260620092039",
			wantOK:  true,
		},
		{
			name:    "missing marker returns empty",
			message: "no marker here, just gpbackup output",
			wantTS:  "",
			wantOK:  false,
		},
		{
			name:    "empty message returns empty",
			message: "",
			wantTS:  "",
			wantOK:  false,
		},
		{
			name:    "malformed: too few digits",
			message: "BACKUP_TIMESTAMP=2026062009",
			wantTS:  "",
			wantOK:  false,
		},
		{
			name:    "malformed: 16 contiguous digits is not a 14-digit timestamp",
			message: "BACKUP_TIMESTAMP=2026062009203912",
			wantTS:  "",
			wantOK:  false,
		},
		{
			name:    "malformed: non-numeric value",
			message: "BACKUP_TIMESTAMP=not-a-timestamp",
			wantTS:  "",
			wantOK:  false,
		},
		{
			name:    "marker present but empty value",
			message: "BACKUP_TIMESTAMP=",
			wantTS:  "",
			wantOK:  false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotTS, gotOK := parseBackupTimestampMessage(tc.message)
			assert.Equal(t, tc.wantOK, gotOK)
			assert.Equal(t, tc.wantTS, gotTS)
		})
	}
}

// TestBackupTimestampFromAnnotation verifies backupTimestampFromAnnotation
// returns the REAL gpbackup timestamp carried on the avsoft.io/backup-timestamp
// annotation, and "" when the annotation is absent or malformed (defensive: a
// bad annotation must never poison status.lastBackupTimestamp).
func TestBackupTimestampFromAnnotation(t *testing.T) {
	tests := []struct {
		name        string
		annotations map[string]string
		want        string
	}{
		{
			name:        "valid annotation returned",
			annotations: map[string]string{util.AnnotationBackupTimestamp: "20260620092039"},
			want:        "20260620092039",
		},
		{
			name:        "no annotations returns empty",
			annotations: nil,
			want:        "",
		},
		{
			name:        "annotation absent (other keys present) returns empty",
			annotations: map[string]string{util.AnnotationBackupSizeBytes: "123"},
			want:        "",
		},
		{
			name:        "malformed annotation returns empty",
			annotations: map[string]string{util.AnnotationBackupTimestamp: "not-a-timestamp"},
			want:        "",
		},
		{
			name:        "short numeric annotation returns empty",
			annotations: map[string]string{util.AnnotationBackupTimestamp: "2026"},
			want:        "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Annotations: tc.annotations}}
			assert.Equal(t, tc.want, backupTimestampFromAnnotation(job))
		})
	}
}

// TestBackupTimestampFromJob_PrefersAnnotation verifies backupTimestampFromJob
// PREFERS the REAL captured annotation over the Job-name / CompletionTime
// fallback, and falls back correctly when the annotation is absent or malformed.
func TestBackupTimestampFromJob_PrefersAnnotation(t *testing.T) {
	cluster := backupTestCluster()

	t.Run("annotation preferred over Job-name embedded timestamp", func(t *testing.T) {
		// Job NAME carries one timestamp; the captured annotation carries the
		// REAL (different) gpbackup timestamp, which must win.
		job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
			Name: util.BackupJobName(cluster.Name, "20260101020000"),
			Annotations: map[string]string{
				util.AnnotationBackupTimestamp: "20260620092039",
			},
		}}
		assert.Equal(t, "20260620092039", backupTimestampFromJob(cluster, job))
	})

	t.Run("annotation preferred over CompletionTime fallback", func(t *testing.T) {
		completion := metav1.NewTime(time.Date(2026, 1, 1, 7, 0, 0, 0, time.UTC))
		job := &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				// CronJob-style name: not a parseable timestamp.
				Name: util.BackupCronJobName(cluster.Name) + "-abcde",
				Annotations: map[string]string{
					util.AnnotationBackupTimestamp: "20260620092039",
				},
			},
			Status: batchv1.JobStatus{CompletionTime: &completion},
		}
		assert.Equal(t, "20260620092039", backupTimestampFromJob(cluster, job))
	})

	t.Run("falls back to Job-name timestamp when annotation absent", func(t *testing.T) {
		job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
			Name: util.BackupJobName(cluster.Name, "20260101020000"),
		}}
		assert.Equal(t, "20260101020000", backupTimestampFromJob(cluster, job))
	})

	t.Run("falls back to CompletionTime when annotation malformed", func(t *testing.T) {
		completion := metav1.NewTime(time.Date(2026, 1, 1, 7, 0, 0, 0, time.UTC))
		job := &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				Name: util.BackupCronJobName(cluster.Name) + "-abcde",
				Annotations: map[string]string{
					// Malformed -> backupTimestampFromAnnotation returns "" so
					// the fallback path runs.
					util.AnnotationBackupTimestamp: "bogus",
				},
			},
			Status: batchv1.JobStatus{CompletionTime: &completion},
		}
		got := backupTimestampFromJob(cluster, job)
		assert.Equal(t, "20260101070000", got)
		assert.Regexp(t, `^\d{14}$`, got)
	})
}
