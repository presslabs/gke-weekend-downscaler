package downscaler

import (
	corev1 "k8s.io/api/core/v1"
)

func upsertTaint(taint corev1.Taint, taints []corev1.Taint) []corev1.Taint {
	for i := range taints {
		if taints[i].Key == taint.Key && taints[i].Value == taint.Value {
			taints[i].Effect = taint.Effect
		}
	}
	return append(taints, taint)
}

func removeTaint(taint corev1.Taint, taints []corev1.Taint) []corev1.Taint {
	r := taints[:0]
	for _, t := range taints {
		if t.Key != taint.Key || t.Value != taint.Value || t.Effect != taint.Effect {
			r = append(r, t)
		}
	}
	return r
}
