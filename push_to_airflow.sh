gsutil cp cset_openalex_augmentation_dag.py gs://us-east1-production-cc2-202-b42a7a54-bucket/dags/

gsutil rm -r gs://us-east1-production-cc2-202-b42a7a54-bucket/dags/schemas/cset_openalex/*
gsutil rm -r gs://us-east1-production-cc2-202-b42a7a54-bucket/dags/sql/cset_openalex/*

gsutil cp schemas/* gs://us-east1-production-cc2-202-b42a7a54-bucket/dags/schemas/cset_openalex/
gsutil cp sql/* gs://us-east1-production-cc2-202-b42a7a54-bucket/dags/sql/cset_openalex/
gsutil cp scripts/upload.py gs://airflow-data-exchange/cset_openalex/
