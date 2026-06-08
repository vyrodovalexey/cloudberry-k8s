package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestSetCondition(t *testing.T) {
	tests := []struct {
		name       string
		conditions []metav1.Condition
		condType   string
		status     metav1.ConditionStatus
		reason     string
		message    string
		validate   func(t *testing.T, result []metav1.Condition)
	}{
		{
			name:       "add new condition to empty slice",
			conditions: nil,
			condType:   "Ready",
			status:     metav1.ConditionTrue,
			reason:     "AllReady",
			message:    "All components ready",
			validate: func(t *testing.T, result []metav1.Condition) {
				require.Len(t, result, 1)
				assert.Equal(t, "Ready", result[0].Type)
				assert.Equal(t, metav1.ConditionTrue, result[0].Status)
				assert.Equal(t, "AllReady", result[0].Reason)
				assert.Equal(t, "All components ready", result[0].Message)
				assert.False(t, result[0].LastTransitionTime.IsZero())
			},
		},
		{
			name: "update existing condition with same status",
			conditions: []metav1.Condition{
				{
					Type:               "Ready",
					Status:             metav1.ConditionTrue,
					Reason:             "OldReason",
					Message:            "Old message",
					LastTransitionTime: metav1.Now(),
				},
			},
			condType: "Ready",
			status:   metav1.ConditionTrue,
			reason:   "NewReason",
			message:  "New message",
			validate: func(t *testing.T, result []metav1.Condition) {
				require.Len(t, result, 1)
				assert.Equal(t, "NewReason", result[0].Reason)
				assert.Equal(t, "New message", result[0].Message)
				// LastTransitionTime should NOT change when status is the same
			},
		},
		{
			name: "update existing condition with different status",
			conditions: []metav1.Condition{
				{
					Type:               "Ready",
					Status:             metav1.ConditionFalse,
					Reason:             "NotReady",
					Message:            "Not ready",
					LastTransitionTime: metav1.Now(),
				},
			},
			condType: "Ready",
			status:   metav1.ConditionTrue,
			reason:   "AllReady",
			message:  "All ready",
			validate: func(t *testing.T, result []metav1.Condition) {
				require.Len(t, result, 1)
				assert.Equal(t, metav1.ConditionTrue, result[0].Status)
				assert.Equal(t, "AllReady", result[0].Reason)
				assert.Equal(t, "All ready", result[0].Message)
			},
		},
		{
			name: "add new condition to existing slice",
			conditions: []metav1.Condition{
				{
					Type:   "Ready",
					Status: metav1.ConditionTrue,
				},
			},
			condType: "Available",
			status:   metav1.ConditionFalse,
			reason:   "NotAvailable",
			message:  "Not available",
			validate: func(t *testing.T, result []metav1.Condition) {
				require.Len(t, result, 2)
				assert.Equal(t, "Ready", result[0].Type)
				assert.Equal(t, "Available", result[1].Type)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := SetCondition(tt.conditions, tt.condType, tt.status, tt.reason, tt.message)
			tt.validate(t, result)
		})
	}
}

func TestFindCondition(t *testing.T) {
	tests := []struct {
		name       string
		conditions []metav1.Condition
		condType   string
		expectNil  bool
	}{
		{
			name:       "find existing condition",
			conditions: []metav1.Condition{{Type: "Ready", Status: metav1.ConditionTrue}},
			condType:   "Ready",
			expectNil:  false,
		},
		{
			name:       "condition not found",
			conditions: []metav1.Condition{{Type: "Ready", Status: metav1.ConditionTrue}},
			condType:   "Available",
			expectNil:  true,
		},
		{
			name:       "empty conditions",
			conditions: nil,
			condType:   "Ready",
			expectNil:  true,
		},
		{
			name:       "empty slice",
			conditions: []metav1.Condition{},
			condType:   "Ready",
			expectNil:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := FindCondition(tt.conditions, tt.condType)
			if tt.expectNil {
				assert.Nil(t, result)
			} else {
				require.NotNil(t, result)
				assert.Equal(t, tt.condType, result.Type)
			}
		})
	}
}

func TestIsConditionTrue(t *testing.T) {
	tests := []struct {
		name       string
		conditions []metav1.Condition
		condType   string
		expected   bool
	}{
		{
			name:       "condition is true",
			conditions: []metav1.Condition{{Type: "Ready", Status: metav1.ConditionTrue}},
			condType:   "Ready",
			expected:   true,
		},
		{
			name:       "condition is false",
			conditions: []metav1.Condition{{Type: "Ready", Status: metav1.ConditionFalse}},
			condType:   "Ready",
			expected:   false,
		},
		{
			name:       "condition not found",
			conditions: nil,
			condType:   "Ready",
			expected:   false,
		},
		{
			name:       "condition is unknown",
			conditions: []metav1.Condition{{Type: "Ready", Status: metav1.ConditionUnknown}},
			condType:   "Ready",
			expected:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsConditionTrue(tt.conditions, tt.condType)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestIsConditionFalse(t *testing.T) {
	tests := []struct {
		name       string
		conditions []metav1.Condition
		condType   string
		expected   bool
	}{
		{
			name:       "condition is false",
			conditions: []metav1.Condition{{Type: "Ready", Status: metav1.ConditionFalse}},
			condType:   "Ready",
			expected:   true,
		},
		{
			name:       "condition is true",
			conditions: []metav1.Condition{{Type: "Ready", Status: metav1.ConditionTrue}},
			condType:   "Ready",
			expected:   false,
		},
		{
			name:       "condition not found",
			conditions: nil,
			condType:   "Ready",
			expected:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsConditionFalse(tt.conditions, tt.condType)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestRemoveCondition(t *testing.T) {
	tests := []struct {
		name       string
		conditions []metav1.Condition
		condType   string
		expected   int
	}{
		{
			name: "remove existing condition",
			conditions: []metav1.Condition{
				{Type: "Ready"},
				{Type: "Available"},
			},
			condType: "Ready",
			expected: 1,
		},
		{
			name: "remove non-existing condition",
			conditions: []metav1.Condition{
				{Type: "Ready"},
			},
			condType: "Available",
			expected: 1,
		},
		{
			name:       "remove from empty slice",
			conditions: nil,
			condType:   "Ready",
			expected:   0,
		},
		{
			name: "remove only condition",
			conditions: []metav1.Condition{
				{Type: "Ready"},
			},
			condType: "Ready",
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := RemoveCondition(tt.conditions, tt.condType)
			assert.Len(t, result, tt.expected)
			// Verify the removed condition is not present
			for _, c := range result {
				assert.NotEqual(t, tt.condType, c.Type)
			}
		})
	}
}
