package util

import (
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// SetCondition sets or updates a condition in the conditions slice.
// If the condition already exists with the same status, it is not updated.
// Returns the updated conditions slice.
func SetCondition(
	conditions []metav1.Condition,
	condType string,
	status metav1.ConditionStatus,
	reason, message string,
) []metav1.Condition {
	now := metav1.NewTime(time.Now())

	for i, c := range conditions {
		if c.Type != condType {
			continue
		}
		if c.Status == status {
			// Status unchanged, update reason and message only.
			conditions[i].Reason = reason
			conditions[i].Message = message
			return conditions
		}
		conditions[i].Status = status
		conditions[i].Reason = reason
		conditions[i].Message = message
		conditions[i].LastTransitionTime = now
		return conditions
	}

	// Condition not found, add it.
	conditions = append(conditions, metav1.Condition{
		Type:               condType,
		Status:             status,
		LastTransitionTime: now,
		Reason:             reason,
		Message:            message,
	})
	return conditions
}

// FindCondition finds a condition by type in the conditions slice.
// Returns nil if not found.
func FindCondition(conditions []metav1.Condition, condType string) *metav1.Condition {
	for i := range conditions {
		if conditions[i].Type == condType {
			return &conditions[i]
		}
	}
	return nil
}

// IsConditionTrue checks if a condition is set to True.
func IsConditionTrue(conditions []metav1.Condition, condType string) bool {
	c := FindCondition(conditions, condType)
	return c != nil && c.Status == metav1.ConditionTrue
}

// IsConditionFalse checks if a condition is set to False.
func IsConditionFalse(conditions []metav1.Condition, condType string) bool {
	c := FindCondition(conditions, condType)
	return c != nil && c.Status == metav1.ConditionFalse
}

// RemoveCondition removes a condition by type from the conditions slice.
func RemoveCondition(conditions []metav1.Condition, condType string) []metav1.Condition {
	result := make([]metav1.Condition, 0, len(conditions))
	for _, c := range conditions {
		if c.Type != condType {
			result = append(result, c)
		}
	}
	return result
}
