#!/bin/bash

# Generate dbt docs for metrics and upload to GCS
set -euo pipefail

# Configuration -- patched by test_e2e.py before pushing to GitHub
GCS_BUCKET_NAME="personal-381013"
GCS_BUCKET_PATH="gs://personal-381013/dbt"
TARGET_ENV="prod"

check_and_create_bucket() {
    echo "Checking if bucket exists..."
    if ! gsutil ls "gs://$GCS_BUCKET_NAME" &> /dev/null; then
        echo "Bucket does not exist. Creating bucket..."
        if gsutil mb "gs://$GCS_BUCKET_NAME" 2>/dev/null; then
            echo "Bucket created successfully."
        else
            echo "Bucket creation failed (may already exist globally). Continuing..."
        fi
    else
        echo "Bucket already exists."
    fi
}

run_and_generate_docs() {
    echo "Running dbt project..."
    dbt run --select tag:metrics --target "$TARGET_ENV"
    echo "Generating dbt documentation for metrics..."
    dbt docs generate --select tag:metrics --target "$TARGET_ENV"
    echo "Documentation generated successfully."
}

upload_to_gcs() {
    echo "Uploading dbt documentation to GCS bucket..."
    gsutil cp target/manifest.json "$GCS_BUCKET_PATH/manifest.json"
    gsutil cp target/catalog.json "$GCS_BUCKET_PATH/catalog.json"
    echo "Documentation uploaded successfully."
}

main() {
    echo "Starting dbt documentation upload process..."
    check_and_create_bucket
    run_and_generate_docs
    upload_to_gcs
    echo "Process completed successfully!"
}

main "$@"
