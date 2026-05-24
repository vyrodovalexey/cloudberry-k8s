package ctl

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"go.yaml.in/yaml/v3"
)

// validActions is the set of valid workload rule actions.
var validActions = map[string]bool{
	"cancel": true,
	"move":   true,
	"log":    true,
}

// WorkloadRuleFile represents a workload rule as read from or written to a YAML file.
// It mirrors the API's WorkloadRule but is independent of the CRD types to avoid
// importing k8s API types in the CLI.
type WorkloadRuleFile struct {
	// Name is the rule name (must be a valid SQL identifier).
	Name string `json:"name" yaml:"name"`
	// Enabled controls whether the rule is active.
	Enabled bool `json:"enabled" yaml:"enabled"`
	// ResourceGroup is the target resource group.
	ResourceGroup string `json:"resourceGroup,omitempty" yaml:"resourceGroup,omitempty"`
	// QueryTag is the query tag to match.
	QueryTag string `json:"queryTag,omitempty" yaml:"queryTag,omitempty"`
	// Role is the database role to match.
	Role string `json:"role,omitempty" yaml:"role,omitempty"`
	// Action is the action to take (cancel, move, log).
	Action string `json:"action" yaml:"action"`
	// MoveTarget is the target resource group for move actions.
	MoveTarget string `json:"moveTarget,omitempty" yaml:"moveTarget,omitempty"`
	// Threshold is the threshold value for the rule.
	Threshold string `json:"threshold,omitempty" yaml:"threshold,omitempty"`
	// ThresholdType is the type of threshold (cpu_skew, cpu_time, running_time, etc.).
	ThresholdType string `json:"thresholdType,omitempty" yaml:"thresholdType,omitempty"`
	// Priority is the rule evaluation priority.
	Priority int32 `json:"priority,omitempty" yaml:"priority,omitempty"`
}

// ValidateRule validates that a WorkloadRuleFile has all required fields.
// It returns an error if the rule name is empty, the action is empty,
// or the action is not one of the valid actions (cancel, move, log).
func ValidateRule(rule *WorkloadRuleFile) error {
	if rule.Name == "" {
		return fmt.Errorf("rule name is required")
	}
	if rule.Action == "" {
		return fmt.Errorf("rule action is required")
	}
	if !validActions[rule.Action] {
		return fmt.Errorf("invalid rule action %q: must be one of cancel, move, log", rule.Action)
	}
	return nil
}

// ReadRuleFromFile reads a single WorkloadRuleFile from a YAML file at the given path.
// The file must contain a single YAML document representing one rule.
// The rule is validated after parsing.
func ReadRuleFromFile(path string) (*WorkloadRuleFile, error) {
	cleanPath := filepath.Clean(path)
	data, err := os.ReadFile(cleanPath)
	if err != nil {
		return nil, fmt.Errorf("reading rule file %q: %w", cleanPath, err)
	}

	var rule WorkloadRuleFile
	if err := yaml.Unmarshal(data, &rule); err != nil {
		return nil, fmt.Errorf("parsing rule file %q: %w", path, err)
	}

	if err := ValidateRule(&rule); err != nil {
		return nil, fmt.Errorf("validating rule from %q: %w", path, err)
	}

	return &rule, nil
}

// ReadRulesFromFile reads multiple WorkloadRuleFile entries from a YAML file at the given path.
// The file must contain a YAML array of rule objects. Each rule is validated after parsing.
// An empty array is valid and returns an empty slice with no error.
func ReadRulesFromFile(path string) ([]WorkloadRuleFile, error) {
	cleanPath := filepath.Clean(path)
	data, err := os.ReadFile(cleanPath)
	if err != nil {
		return nil, fmt.Errorf("reading rules file %q: %w", cleanPath, err)
	}

	var rules []WorkloadRuleFile
	if err := yaml.Unmarshal(data, &rules); err != nil {
		return nil, fmt.Errorf("parsing rules file %q: %w", path, err)
	}

	for i := range rules {
		if err := ValidateRule(&rules[i]); err != nil {
			return nil, fmt.Errorf("validating rule %d (%q) from %q: %w",
				i, rules[i].Name, path, err)
		}
	}

	return rules, nil
}

// WriteRulesToFile writes a slice of WorkloadRuleFile entries to a YAML file at the given path.
// The file is created with 0600 permissions. If the file already exists, it is overwritten.
func WriteRulesToFile(path string, rules []WorkloadRuleFile) error {
	cleanPath := filepath.Clean(path)
	f, err := os.OpenFile(cleanPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("creating rules file %q: %w", cleanPath, err)
	}
	defer f.Close()

	if err := WriteRulesToWriter(f, rules); err != nil {
		return fmt.Errorf("writing rules to %q: %w", cleanPath, err)
	}

	return nil
}

// WriteRulesToWriter writes a slice of WorkloadRuleFile entries to an io.Writer in YAML format.
// The output is a YAML array that can be read back by ReadRulesFromFile.
func WriteRulesToWriter(w io.Writer, rules []WorkloadRuleFile) error {
	out, err := yaml.Marshal(rules)
	if err != nil {
		return fmt.Errorf("marshaling rules to YAML: %w", err)
	}

	if _, err := w.Write(out); err != nil {
		return fmt.Errorf("writing YAML output: %w", err)
	}

	return nil
}
