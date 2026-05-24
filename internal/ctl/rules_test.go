package ctl

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v3"
)

// ---------------------------------------------------------------------------
// ValidateRule
// ---------------------------------------------------------------------------

func TestValidateRule(t *testing.T) {
	tests := []struct {
		name    string
		rule    *WorkloadRuleFile
		wantErr string
	}{
		{
			name: "valid rule with all fields",
			rule: &WorkloadRuleFile{
				Name:          "cpu-limit",
				Enabled:       true,
				ResourceGroup: "analytics",
				QueryTag:      "etl",
				Role:          "analyst",
				Action:        "cancel",
				MoveTarget:    "overflow",
				Threshold:     "80",
				ThresholdType: "cpu_time",
				Priority:      10,
			},
			wantErr: "",
		},
		{
			name: "valid rule with minimal fields - cancel",
			rule: &WorkloadRuleFile{
				Name:   "simple-cancel",
				Action: "cancel",
			},
			wantErr: "",
		},
		{
			name: "valid rule with minimal fields - move",
			rule: &WorkloadRuleFile{
				Name:   "simple-move",
				Action: "move",
			},
			wantErr: "",
		},
		{
			name: "valid rule with minimal fields - log",
			rule: &WorkloadRuleFile{
				Name:   "simple-log",
				Action: "log",
			},
			wantErr: "",
		},
		{
			name: "empty name",
			rule: &WorkloadRuleFile{
				Name:   "",
				Action: "cancel",
			},
			wantErr: "rule name is required",
		},
		{
			name: "empty action",
			rule: &WorkloadRuleFile{
				Name:   "my-rule",
				Action: "",
			},
			wantErr: "rule action is required",
		},
		{
			name: "invalid action",
			rule: &WorkloadRuleFile{
				Name:   "my-rule",
				Action: "delete",
			},
			wantErr: `invalid rule action "delete": must be one of cancel, move, log`,
		},
		{
			name: "another invalid action",
			rule: &WorkloadRuleFile{
				Name:   "my-rule",
				Action: "CANCEL",
			},
			wantErr: `invalid rule action "CANCEL": must be one of cancel, move, log`,
		},
		{
			name: "empty name and empty action - name checked first",
			rule: &WorkloadRuleFile{
				Name:   "",
				Action: "",
			},
			wantErr: "rule name is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateRule(tt.rule)
			if tt.wantErr == "" {
				assert.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Equal(t, tt.wantErr, err.Error())
			}
		})
	}
}

// ---------------------------------------------------------------------------
// ReadRuleFromFile
// ---------------------------------------------------------------------------

func TestReadRuleFromFile(t *testing.T) {
	tests := []struct {
		name     string
		content  string // YAML content to write; empty means skip file creation
		noFile   bool   // if true, don't create the file at all
		wantRule *WorkloadRuleFile
		wantErr  string
	}{
		{
			name: "valid single rule with all fields",
			content: `name: cpu-limit
enabled: true
resourceGroup: analytics
queryTag: etl
role: analyst
action: cancel
moveTarget: overflow
threshold: "80"
thresholdType: cpu_time
priority: 10
`,
			wantRule: &WorkloadRuleFile{
				Name:          "cpu-limit",
				Enabled:       true,
				ResourceGroup: "analytics",
				QueryTag:      "etl",
				Role:          "analyst",
				Action:        "cancel",
				MoveTarget:    "overflow",
				Threshold:     "80",
				ThresholdType: "cpu_time",
				Priority:      10,
			},
		},
		{
			name: "valid rule with minimal fields",
			content: `name: simple
action: log
`,
			wantRule: &WorkloadRuleFile{
				Name:   "simple",
				Action: "log",
			},
		},
		{
			name:    "file not found",
			noFile:  true,
			wantErr: "reading rule file",
		},
		{
			name:    "invalid YAML",
			content: "{{invalid yaml: [",
			wantErr: "parsing rule file",
		},
		{
			name: "missing required name field",
			content: `action: cancel
`,
			wantErr: "validating rule from",
		},
		{
			name: "missing required action field",
			content: `name: my-rule
`,
			wantErr: "validating rule from",
		},
		{
			name:    "empty file",
			content: "",
			wantErr: "validating rule from",
		},
		{
			name: "invalid action in file",
			content: `name: bad-action
action: destroy
`,
			wantErr: "validating rule from",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			filePath := filepath.Join(dir, "rule.yaml")

			if !tt.noFile {
				err := os.WriteFile(filePath, []byte(tt.content), 0o600)
				require.NoError(t, err)
			}

			if tt.noFile {
				filePath = filepath.Join(dir, "nonexistent.yaml")
			}

			rule, err := ReadRuleFromFile(filePath)

			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				assert.Nil(t, rule)
			} else {
				require.NoError(t, err)
				require.NotNil(t, rule)
				assert.Equal(t, tt.wantRule.Name, rule.Name)
				assert.Equal(t, tt.wantRule.Enabled, rule.Enabled)
				assert.Equal(t, tt.wantRule.ResourceGroup, rule.ResourceGroup)
				assert.Equal(t, tt.wantRule.QueryTag, rule.QueryTag)
				assert.Equal(t, tt.wantRule.Role, rule.Role)
				assert.Equal(t, tt.wantRule.Action, rule.Action)
				assert.Equal(t, tt.wantRule.MoveTarget, rule.MoveTarget)
				assert.Equal(t, tt.wantRule.Threshold, rule.Threshold)
				assert.Equal(t, tt.wantRule.ThresholdType, rule.ThresholdType)
				assert.Equal(t, tt.wantRule.Priority, rule.Priority)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// ReadRulesFromFile
// ---------------------------------------------------------------------------

func TestReadRulesFromFile(t *testing.T) {
	tests := []struct {
		name      string
		content   string
		noFile    bool
		wantRules []WorkloadRuleFile
		wantErr   string
	}{
		{
			name: "valid array of rules",
			content: `- name: rule1
  action: cancel
  enabled: true
  priority: 1
- name: rule2
  action: move
  moveTarget: overflow
  priority: 2
- name: rule3
  action: log
`,
			wantRules: []WorkloadRuleFile{
				{Name: "rule1", Action: "cancel", Enabled: true, Priority: 1},
				{Name: "rule2", Action: "move", MoveTarget: "overflow", Priority: 2},
				{Name: "rule3", Action: "log"},
			},
		},
		{
			name:      "empty array",
			content:   "[]",
			wantRules: []WorkloadRuleFile{},
		},
		{
			name:    "file not found",
			noFile:  true,
			wantErr: "reading rules file",
		},
		{
			name:    "invalid YAML",
			content: "{{not valid yaml",
			wantErr: "parsing rules file",
		},
		{
			name: "one invalid rule in array - missing name",
			content: `- name: good-rule
  action: cancel
- action: move
`,
			wantErr: "validating rule 1",
		},
		{
			name: "one invalid rule in array - bad action",
			content: `- name: good-rule
  action: cancel
- name: bad-rule
  action: explode
`,
			wantErr: `validating rule 1 ("bad-rule")`,
		},
		{
			name: "single rule not in array format",
			content: `name: single
action: cancel
`,
			wantErr: "parsing rules file",
		},
		{
			name: "valid array with full fields",
			content: `- name: full-rule
  enabled: true
  resourceGroup: analytics
  queryTag: etl
  role: analyst
  action: move
  moveTarget: overflow
  threshold: "100"
  thresholdType: running_time
  priority: 5
`,
			wantRules: []WorkloadRuleFile{
				{
					Name:          "full-rule",
					Enabled:       true,
					ResourceGroup: "analytics",
					QueryTag:      "etl",
					Role:          "analyst",
					Action:        "move",
					MoveTarget:    "overflow",
					Threshold:     "100",
					ThresholdType: "running_time",
					Priority:      5,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			filePath := filepath.Join(dir, "rules.yaml")

			if !tt.noFile {
				err := os.WriteFile(filePath, []byte(tt.content), 0o600)
				require.NoError(t, err)
			}

			if tt.noFile {
				filePath = filepath.Join(dir, "nonexistent.yaml")
			}

			rules, err := ReadRulesFromFile(filePath)

			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				assert.Nil(t, rules)
			} else {
				require.NoError(t, err)
				require.NotNil(t, rules)
				require.Len(t, rules, len(tt.wantRules))
				for i, want := range tt.wantRules {
					assert.Equal(t, want.Name, rules[i].Name, "rule %d name", i)
					assert.Equal(t, want.Action, rules[i].Action, "rule %d action", i)
					assert.Equal(t, want.Enabled, rules[i].Enabled, "rule %d enabled", i)
					assert.Equal(t, want.Priority, rules[i].Priority, "rule %d priority", i)
					assert.Equal(t, want.MoveTarget, rules[i].MoveTarget, "rule %d moveTarget", i)
					assert.Equal(t, want.ResourceGroup, rules[i].ResourceGroup, "rule %d resourceGroup", i)
					assert.Equal(t, want.QueryTag, rules[i].QueryTag, "rule %d queryTag", i)
					assert.Equal(t, want.Role, rules[i].Role, "rule %d role", i)
					assert.Equal(t, want.Threshold, rules[i].Threshold, "rule %d threshold", i)
					assert.Equal(t, want.ThresholdType, rules[i].ThresholdType, "rule %d thresholdType", i)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// WriteRulesToFile
// ---------------------------------------------------------------------------

func TestWriteRulesToFile(t *testing.T) {
	tests := []struct {
		name       string
		rules      []WorkloadRuleFile
		invalidDir bool // use an invalid path
		wantErr    string
	}{
		{
			name: "write multiple rules",
			rules: []WorkloadRuleFile{
				{Name: "rule1", Action: "cancel", Enabled: true, Priority: 1},
				{Name: "rule2", Action: "move", MoveTarget: "overflow"},
			},
		},
		{
			name:  "write single rule",
			rules: []WorkloadRuleFile{{Name: "only", Action: "log"}},
		},
		{
			name:  "write empty rules",
			rules: []WorkloadRuleFile{},
		},
		{
			name:  "write nil rules",
			rules: nil,
		},
		{
			name:       "invalid path",
			rules:      []WorkloadRuleFile{{Name: "r", Action: "log"}},
			invalidDir: true,
			wantErr:    "creating rules file",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			filePath := filepath.Join(dir, "output.yaml")

			if tt.invalidDir {
				filePath = filepath.Join(dir, "nonexistent-dir", "subdir", "output.yaml")
			}

			err := WriteRulesToFile(filePath, tt.rules)

			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
			} else {
				require.NoError(t, err)

				// Verify file exists and has correct permissions.
				info, statErr := os.Stat(filePath)
				require.NoError(t, statErr)
				assert.Equal(t, os.FileMode(0o600), info.Mode().Perm())

				// Verify content is valid YAML.
				data, readErr := os.ReadFile(filePath)
				require.NoError(t, readErr)

				var parsed []WorkloadRuleFile
				require.NoError(t, yaml.Unmarshal(data, &parsed))

				if tt.rules == nil {
					assert.Empty(t, parsed)
				} else {
					assert.Len(t, parsed, len(tt.rules))
				}
			}
		})
	}
}

func TestWriteRulesToFile_Overwrite(t *testing.T) {
	dir := t.TempDir()
	filePath := filepath.Join(dir, "rules.yaml")

	// Write initial content.
	initial := []WorkloadRuleFile{{Name: "old-rule", Action: "log"}}
	require.NoError(t, WriteRulesToFile(filePath, initial))

	// Overwrite with new content.
	updated := []WorkloadRuleFile{
		{Name: "new-rule-1", Action: "cancel"},
		{Name: "new-rule-2", Action: "move"},
	}
	require.NoError(t, WriteRulesToFile(filePath, updated))

	// Verify the file contains only the new content.
	data, err := os.ReadFile(filePath)
	require.NoError(t, err)

	var parsed []WorkloadRuleFile
	require.NoError(t, yaml.Unmarshal(data, &parsed))
	require.Len(t, parsed, 2)
	assert.Equal(t, "new-rule-1", parsed[0].Name)
	assert.Equal(t, "new-rule-2", parsed[1].Name)
}

// ---------------------------------------------------------------------------
// WriteRulesToWriter
// ---------------------------------------------------------------------------

func TestWriteRulesToWriter(t *testing.T) {
	tests := []struct {
		name  string
		rules []WorkloadRuleFile
	}{
		{
			name: "write multiple rules to buffer",
			rules: []WorkloadRuleFile{
				{Name: "rule1", Action: "cancel", Enabled: true, Priority: 1},
				{Name: "rule2", Action: "move", MoveTarget: "overflow"},
			},
		},
		{
			name:  "write single rule to buffer",
			rules: []WorkloadRuleFile{{Name: "only", Action: "log"}},
		},
		{
			name:  "write empty rules to buffer",
			rules: []WorkloadRuleFile{},
		},
		{
			name:  "write nil rules to buffer",
			rules: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			err := WriteRulesToWriter(&buf, tt.rules)
			require.NoError(t, err)

			output := buf.String()
			assert.NotEmpty(t, output, "output should not be empty even for nil/empty slices")

			// Verify the output is valid YAML.
			var parsed []WorkloadRuleFile
			require.NoError(t, yaml.Unmarshal(buf.Bytes(), &parsed))

			if tt.rules == nil {
				assert.Empty(t, parsed)
			} else {
				assert.Len(t, parsed, len(tt.rules))
				for i, want := range tt.rules {
					assert.Equal(t, want.Name, parsed[i].Name)
					assert.Equal(t, want.Action, parsed[i].Action)
				}
			}
		})
	}
}

func TestWriteRulesToWriter_ErrorOnWrite(t *testing.T) {
	// Use a writer that always fails.
	w := &failWriter{}
	rules := []WorkloadRuleFile{{Name: "rule1", Action: "cancel"}}

	err := WriteRulesToWriter(w, rules)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "writing YAML output")
}

// failWriter is an io.Writer that always returns an error.
type failWriter struct{}

func (fw *failWriter) Write(_ []byte) (int, error) {
	return 0, errors.New("simulated write failure")
}

// ---------------------------------------------------------------------------
// Round-trip: WriteRulesToFile → ReadRulesFromFile
// ---------------------------------------------------------------------------

func TestRoundTrip_WriteAndReadRules(t *testing.T) {
	original := []WorkloadRuleFile{
		{
			Name:          "cpu-limit",
			Enabled:       true,
			ResourceGroup: "analytics",
			QueryTag:      "etl",
			Role:          "analyst",
			Action:        "cancel",
			MoveTarget:    "overflow",
			Threshold:     "80",
			ThresholdType: "cpu_time",
			Priority:      10,
		},
		{
			Name:     "simple-log",
			Enabled:  false,
			Action:   "log",
			Priority: 1,
		},
		{
			Name:       "move-rule",
			Enabled:    true,
			Action:     "move",
			MoveTarget: "slow-lane",
			Priority:   5,
		},
	}

	dir := t.TempDir()
	filePath := filepath.Join(dir, "roundtrip.yaml")

	// Write.
	require.NoError(t, WriteRulesToFile(filePath, original))

	// Read back.
	result, err := ReadRulesFromFile(filePath)
	require.NoError(t, err)
	require.Len(t, result, len(original))

	for i, want := range original {
		assert.Equal(t, want.Name, result[i].Name, "rule %d name", i)
		assert.Equal(t, want.Enabled, result[i].Enabled, "rule %d enabled", i)
		assert.Equal(t, want.ResourceGroup, result[i].ResourceGroup, "rule %d resourceGroup", i)
		assert.Equal(t, want.QueryTag, result[i].QueryTag, "rule %d queryTag", i)
		assert.Equal(t, want.Role, result[i].Role, "rule %d role", i)
		assert.Equal(t, want.Action, result[i].Action, "rule %d action", i)
		assert.Equal(t, want.MoveTarget, result[i].MoveTarget, "rule %d moveTarget", i)
		assert.Equal(t, want.Threshold, result[i].Threshold, "rule %d threshold", i)
		assert.Equal(t, want.ThresholdType, result[i].ThresholdType, "rule %d thresholdType", i)
		assert.Equal(t, want.Priority, result[i].Priority, "rule %d priority", i)
	}
}

// ---------------------------------------------------------------------------
// WorkloadRuleFile struct field tags
// ---------------------------------------------------------------------------

func TestWorkloadRuleFile_YAMLTags(t *testing.T) {
	// Verify that YAML marshaling/unmarshaling uses the correct field names.
	rule := WorkloadRuleFile{
		Name:          "test-rule",
		Enabled:       true,
		ResourceGroup: "rg1",
		QueryTag:      "qt1",
		Role:          "admin",
		Action:        "cancel",
		MoveTarget:    "target",
		Threshold:     "50",
		ThresholdType: "cpu_skew",
		Priority:      7,
	}

	data, err := yaml.Marshal(&rule)
	require.NoError(t, err)

	yamlStr := string(data)
	assert.Contains(t, yamlStr, "name: test-rule")
	assert.Contains(t, yamlStr, "enabled: true")
	assert.Contains(t, yamlStr, "resourceGroup: rg1")
	assert.Contains(t, yamlStr, "queryTag: qt1")
	assert.Contains(t, yamlStr, "role: admin")
	assert.Contains(t, yamlStr, "action: cancel")
	assert.Contains(t, yamlStr, "moveTarget: target")
	assert.Contains(t, yamlStr, "threshold: \"50\"")
	assert.Contains(t, yamlStr, "thresholdType: cpu_skew")
	assert.Contains(t, yamlStr, "priority: 7")
}

func TestWorkloadRuleFile_OmitEmpty(t *testing.T) {
	// Verify that omitempty fields are not present when zero-valued.
	rule := WorkloadRuleFile{
		Name:   "minimal",
		Action: "log",
	}

	data, err := yaml.Marshal(&rule)
	require.NoError(t, err)

	yamlStr := string(data)
	assert.Contains(t, yamlStr, "name: minimal")
	assert.Contains(t, yamlStr, "action: log")
	// These should be omitted.
	assert.NotContains(t, yamlStr, "resourceGroup")
	assert.NotContains(t, yamlStr, "queryTag")
	assert.NotContains(t, yamlStr, "role")
	assert.NotContains(t, yamlStr, "moveTarget")
	assert.NotContains(t, yamlStr, "threshold")
	assert.NotContains(t, yamlStr, "thresholdType")
	assert.NotContains(t, yamlStr, "priority")
}

// ---------------------------------------------------------------------------
// validActions map
// ---------------------------------------------------------------------------

func TestValidActions(t *testing.T) {
	assert.True(t, validActions["cancel"])
	assert.True(t, validActions["move"])
	assert.True(t, validActions["log"])
	assert.False(t, validActions["delete"])
	assert.False(t, validActions[""])
	assert.False(t, validActions["CANCEL"])
}
