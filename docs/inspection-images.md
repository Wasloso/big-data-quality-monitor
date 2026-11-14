# Local inspection images

The API can serve a photo for a retained inspection. It looks up the image reference in `stations.sqlite` using the exact `(run_id, inspection_id)` key. Source image IDs may be reused between inspections/categories, so they are not used alone as file lookup keys.

Images are optional and disabled in the normal Compose configuration. Once the image files exist locally, mount only their root directory read-only:

```bash
INSPECTION_IMAGE_DIR=/absolute/path/to/image-root docker compose -f compose.yaml -f compose.images.yaml up --build -d api
```

The directory must already exist; Docker must not create an empty host directory accidentally. The override sets `INSPECTION_IMAGE_ROOT=/data/images`. The existing serving-subdirectory mount is unchanged, and raw/clean Parquet remains inaccessible to the API.

Each stored `image_uri` must be relative to that root. For example, `bottle/test/good/000.png` requires an image root containing the `bottle` directory. Use the same relative references that later model inference records. Mounting a dataset alone does not make the current fixture paths real or perform inference.

For local Python development, set `INSPECTION_IMAGE_ROOT=/absolute/path/to/image-root` alongside `QUALITY_DATABASE` and `STATIONS_DATABASE` when starting Uvicorn.

`GET /api/inspection-image?run_id=...&inspection_id=...` serves PNG, JPEG or WebP files for retained records. The metadata API supplies an encoded `image_url` for each item when a root is configured; otherwise the field is null. The API resolves the stored path under the root, rejects absolute paths and traversal/symlink escapes, and checks that the result is an allowed image file. There is no endpoint accepting an arbitrary file path or remote URL.

A missing/pruned inspection, unsupported file, unsafe reference or missing image returns 404. A known inspection without image storage configured returns 503. Live image responses use `Cache-Control: no-store`; changing or unavailable files must not be mistaken for a permanently cached successful result. Database schema/read errors remain explicit 503 responses.

The finite fixture scores/references remain synthetic. Bottle preparation, a fitted/evaluated CPU artifact and [actual station publication](continuous-stations.md) now exist; configure the dataset parent as the image root to serve their source photos. Capsule, anomaly maps and numeric-threshold presentation remain follow-ups.
