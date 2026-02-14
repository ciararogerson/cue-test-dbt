.PHONY: help update generate-docs clean-docs

help:
	@echo "Available targets:"
	@echo "  update         - Generate dbt docs and upload to GCS"
	@echo "  generate-docs  - Generate dbt documentation only"
	@echo "  clean-docs     - Clean generated documentation files"

update:
	@echo "Generating dbt docs and uploading to GCS..."
	@chmod +x cue_update.sh
	@./cue_update.sh
	@echo "Compiling metrics..."
	@dbt run --select metrics* --target prod
	@echo "Metrics compiled successfully."

generate-docs:
	@echo "Generating dbt documentation..."
	dbt docs generate --select tag:metrics --target prod
	@echo "Documentation generated successfully."

clean-docs:
	@echo "Cleaning generated documentation files..."
	rm -f target/manifest.json target/catalog.json
	@echo "Documentation files cleaned."
