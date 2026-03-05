FROM ghcr.io/dbt-labs/dbt-bigquery:1.7.0

WORKDIR /dbt

# Copy project files
COPY dbt_project.yml .
COPY profiles.yml .
COPY models/ models/

# Install dbt dependencies (packages.yml if it exists)
RUN if [ -f packages.yml ]; then dbt deps --profiles-dir . --project-dir .; fi

# Run dbt when the container starts
CMD ["dbt", "run", "--profiles-dir", ".", "--project-dir", "."]
