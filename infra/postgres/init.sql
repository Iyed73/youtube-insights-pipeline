-- init.sql — executed ONCE when the PostgreSQL container is first initialised.
-- Creates the ingestion state database alongside the Airflow metadata database.
-- The Airflow DB (named via POSTGRES_DB env var) is created automatically by
-- the postgres image; we only need to create the second database here.

CREATE DATABASE ingestion;
