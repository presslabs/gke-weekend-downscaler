# gke-weekend-downscaler
GKE cluster downscaler for weekends

## Configuring

The downscaler reads it's config from the `gke-weekend-downscaler` config map in
the `kube-system` namespace.

| Config Key | Description |
| --- | --- |
| `disabled-until` | Disables the downscaler until the set date. |
| `scale-down-after` | The downscaler scale clusters down, only after this date |
| `scale-up-after` | The downscaler scale clusters up, only after this date |
