# Modern Data Analytics Platform

## End-to-End Data Engineering, Lakehouse and Real-Time Analytics Platform

### Project Documentation
**Version:** 1.0  
**Status:** Completed   
**Team:** Ali Khorasani, Rozhin Ramin, Zahra Arjmand, Amir Golparvar, Reza Mirmaroof, Hossein Kashefi  
**Organisation:** Quera Data Engineering Bootcamp  
**Instructor / Mentor:** Kiarash Azimi  
**Document Date:** 3 August 2026

---

## Document Control

**Document Control Record.**

| Field | Value |
| --- | --- |
| Document Title | Modern Data Analytics Platform — End-to-End Data Engineering, Lakehouse and Real-Time Analytics Platform |
| Version | 1.0 |
| Status | Completed |
| Author | Amir Golparvar |
| Team | Ali Khorasani; Rozhin Ramin; Zahra Arjmand; Amir Golparvar; Reza Mirmaroof; Hossein Kashefi |
| Organisation | Quera Data Engineering Bootcamp |
| Repository | modern_data_analytics_platform — [REPOSITORY URL REDACTED] |
| Date | 3 August 2026 |
| Documentation Language | English (British spelling) |
| Intended Audience | Data engineers, platform engineers, analytics engineers, technical reviewers, bootcamp mentors, operators, and future maintainers |



## Table of Contents

- [1. Problem Statement](#1-problem-statement)
- [2. Project Introduction](#2-project-introduction)
- [3. Requirements and Scope](#3-requirements-and-scope)
- [4. Architecture Overview](#4-architecture-overview)
- [5. Technology Stack](#5-technology-stack)
- [6. Infrastructure and Containerisation](#6-infrastructure-and-containerisation)
- [7. Source Data and Data Contracts](#7-source-data-and-data-contracts)
- [8. Bronze Transactional Layer](#8-bronze-transactional-layer)
- [9. Bronze Behavioural Layer](#9-bronze-behavioural-layer)
- [10. MinIO Storage Architecture](#10-minio-storage-architecture)
- [11. Silver Transactional Layer](#11-silver-transactional-layer)
- [12. Silver Behavioural Layer](#12-silver-behavioural-layer)
- [13. Apache Iceberg and Lakekeeper](#13-apache-iceberg-and-lakekeeper)
- [14. Data Quality and Validation](#14-data-quality-and-validation)
- [15. Gold Layer: Direct Kafka-to-ClickHouse Streaming](#15-gold-layer-direct-kafka-to-clickhouse-streaming)
- [16. Bonus Layer: Near-Real-Time Monitoring](#16-bonus-layer-near-real-time-monitoring)
- [17. Apache Airflow Orchestration](#17-apache-airflow-orchestration)
- [18. ClickHouse Analytical Design](#18-clickhouse-analytical-design)
- [19. Reports and Analytics](#19-reports-and-analytics)
- [20. Reliability, Recovery, and Observability](#20-reliability-recovery-and-observability)
- [21. Configuration and Security](#21-configuration-and-security)
- [22. Project Deployment and Execution](#22-project-deployment-and-execution)
- [23. End-to-End Validation](#23-end-to-end-validation)
- [24. Troubleshooting](#24-troubleshooting)
- [25. Challenges and Design Decisions](#25-challenges-and-design-decisions)
- [26. Final Project Outcomes](#26-final-project-outcomes)
- [27. Conclusion](#27-conclusion)
- [Appendices](#appendices)

---

# 1. Problem Statement

## 1.1 Background

Modern e-commerce platforms generate two materially different classes of information. Transactional data records the commercial state of the business—users, products, categories, orders, order lines, and price history—while behavioural data records how users navigate, search, interact with products, build carts, begin checkout, and complete or abandon journeys. These data domains arrive at different rates, contain different structures, and require different quality controls, yet business reporting depends on a coherent view of both.

The project therefore required a platform that could ingest Kafka streams, preserve source lineage, create a durable object-storage landing zone, refine data into governed lakehouse tables, serve low-latency analytical data, and expose the results through dashboards. The final solution combines streaming and batch processing rather than forcing every workload into a single execution model.

## 1.2 Business Problem

Without a unified platform, commercial questions such as revenue by loyalty tier, order volume by time period, return behaviour, user engagement, and conversion-funnel progression are difficult to answer consistently. Data may be available in separate topics, but raw topic messages are not an appropriate long-term reporting contract: they can contain malformed values, duplicates, optional fields, late events, or schema changes. Business users also require interactive queries without placing analytical load on source systems.

## 1.3 Technical Problem

The engineering problem was to create an end-to-end architecture with the following properties:

- schema-aware consumption of Confluent-compatible Avro messages;
- continuous Bronze ingestion with recoverable offsets and immutable Parquet output;
- independent transactional and behavioural processing;
- deterministic validation, cleaning, deduplication, and lineage retention;
- Kimball-style dimensional modelling in Apache Iceberg;
- direct Kafka-to-ClickHouse analytical streaming using Kafka Engine tables and Materialized Views;
- near-real-time monitoring queries that do not depend on scheduled Silver batch completion;
- reproducible deployment through Docker Compose; and
- orchestration, retries, and operational visibility through Apache Airflow.

## 1.4 Project Objectives

The completed platform was designed to:

1. consume six transactional Kafka topics and one behavioural topic;
2. enforce schemas and standardise records using Spark Structured Streaming;
3. store Bronze data in date-organised Parquet paths in MinIO;
4. isolate transactional and behavioural streaming checkpoints;
5. process Bronze partitions through Airflow-orchestrated Spark batch jobs;
6. build transactional and behavioural Iceberg tables through Lakekeeper;
7. persist rejected, warning, and quarantine information rather than silently discarding it;
8. create direct ClickHouse consumers, transformations, and analytical tables;
9. provide business and operational reporting through Metabase; and
10. preserve replayability and traceability across Kafka, Bronze, Silver, and ClickHouse.

## 1.5 Expected Outcomes

The expected outcome was not a collection of independent scripts, but a coherent data platform. The final delivery provides repeatable infrastructure, executable ingestion and transformation code, dimensional models, analytical DDL, monitoring SQL, validation queries, and dashboard-ready datasets. Temporary runtime states—such as a particular DAG result or current object count—are not part of the permanent architecture.

# 2. Project Introduction

## 2.1 Platform Overview

The Modern Data Analytics Platform implements an e-commerce data architecture organised around Bronze, Silver, Gold, and Bonus responsibilities. Kafka and Schema Registry provide the source stream and schema contracts. Spark performs both continuous and batch computation. MinIO provides S3-compatible object storage. Apache Iceberg and Lakekeeper manage Silver tables. ClickHouse consumes the final analytical streams directly from Kafka, and Metabase provides the reporting interface.

## 2.2 Data Domains

The transactional domain contains categories, users, products, orders, order items, and product-price history. The behavioural domain contains a flexible clickstream event envelope with user, session, device, product, cart, checkout, search, rating, error, and attribution attributes. Separate pipelines allow each domain to use rules appropriate to its semantics.

## 2.3 End-to-End Processing Lifecycle

The processing lifecycle begins with Kafka messages. Bronze jobs fetch the latest registered schema, decode or parse each message, attach Kafka lineage, apply limited standardisation, and append Parquet data to MinIO. Airflow then schedules Silver Spark jobs that read eligible Bronze partitions, validate and clean records, generate deterministic keys, and write governed Iceberg tables. In parallel with the lakehouse path, ClickHouse Kafka Engine tables consume source topics directly; Materialized Views validate and transform messages into durable MergeTree tables. Metabase queries the ClickHouse analytical and monitoring datasets.

## 2.4 Main Platform Components

**Table 1. Main Platform Components and Responsibilities.**

| Component | Primary responsibility | Principal integration |
| --- | --- | --- |
| Apache Kafka | Source streaming backbone for transactional and behavioural messages | Consumed by Spark Bronze jobs and ClickHouse Kafka Engine tables |
| Schema Registry | Authoritative Avro schema lookup and compatibility endpoint | Used by Spark decoders and ClickHouse AvroConfluent consumers |
| Apache Spark | Structured Streaming ingestion and batch Silver transformation | Reads Kafka/MinIO and writes Parquet/Iceberg |
| Apache Airflow | Workflow scheduling, dependency checks, retries, and logs | Submits Silver and supplementary Gold Spark jobs |
| MinIO | S3-compatible persistence for Bronze, Silver, warehouse data, and checkpoints | Accessed through S3A and Iceberg S3FileIO |
| Apache Iceberg | Transactional table format for the Silver lakehouse | Managed from Spark through the Lakekeeper REST catalog |
| Lakekeeper | REST catalogue and warehouse registration for Iceberg | Backed by PostgreSQL and connected to MinIO |
| ClickHouse | Direct stream consumption, transformation, analytical storage, and monitoring queries | Kafka Engine → Materialized View → MergeTree |
| Metabase | Business intelligence and dashboard visualisation | Connects to ClickHouse; metadata stored in PostgreSQL |
| Docker Compose | Reproducible service topology, networks, volumes, and health checks | Coordinates the platform runtime |



## 2.5 Final Platform Capabilities

The platform provides continuous source capture, replayable object storage, schema-governed Silver models, deterministic quality handling, low-latency ClickHouse tables, and BI-ready reporting. It supports independent recovery of the two source domains and allows operational monitoring to continue without waiting for daily Silver jobs.

# 3. Requirements and Scope

## 3.1 Functional Requirements

The solution had to ingest transactional and behavioural streams, preserve all available source metadata, validate data without terminating an entire stream for a bad record, model clean Silver outputs, serve analytical queries, and expose dashboards. Operators also needed repeatable deployment, health verification, retries, and clear recovery paths.

## 3.2 Technical Requirements

The implemented technology set includes Docker and Docker Compose, Python, Kafka, Schema Registry, Spark 3.5.x, Airflow, MinIO, Parquet, Iceberg, Lakekeeper, ClickHouse, PostgreSQL, Redis, Traefik integration, and Metabase. Kubernetes was included in the original project brief as a bonus possibility, but no Kubernetes manifests are present; it is therefore not described as implemented.

## 3.3 Transactional Data Requirements

Transactional messages must retain entity keys, monetary fields, status values, timestamps, and Kafka coordinates. Silver processing must validate business keys and values, detect duplicates, repair selected timestamp problems, enforce parent-child integrity between orders and order items, and build dimensions and facts with stable surrogate keys.

## 3.4 Behavioural Data Requirements

Behavioural messages must retain user/session identity, event type, event timestamp, device and attribution context, and optional event-specific properties. The platform must tolerate sparse events, distinguish critical errors from warnings, protect IP information by hashing it in Silver, generate stable event keys, quarantine rejected events, and recompute touched sessions from the complete fact state.

## 3.5 Analytics Requirements

The analytical layer must support revenue, order, user, loyalty-tier, engagement, return, category, product, behavioural, attribution, and funnel analysis. Low-latency metrics are derived from the direct ClickHouse streams, while governed dimensional data remains available in Silver for reproducible modelling and future cross-domain enrichment.

## 3.6 Monitoring Requirements

The Bonus layer must support orders per minute, average/minimum/maximum order amount, invalid-record counts, ingestion freshness, and behavioural funnel progression. Monitoring must use Kafka-fed ClickHouse tables directly rather than waiting for the scheduled Silver layer.

## 3.7 Project Deliverables

The deliverables consist of the complete repository, Docker Compose topology, Spark applications, Airflow DAGs, Iceberg and Lakekeeper configuration, ClickHouse DDL and queries, MinIO storage design, test and verification artefacts, Metabase-ready reporting datasets, and this final technical documentation.

# 4. Architecture Overview

## 4.1 High-Level Architecture

At the highest level, the platform has two complementary planes. The lakehouse plane preserves and governs data through Kafka → Spark → MinIO Bronze → Airflow/Spark → Iceberg Silver. The serving plane reduces analytical latency through Kafka → ClickHouse Kafka Engine → Materialized View → MergeTree → Metabase. These planes share source contracts but solve different problems: Silver prioritises correctness, replayability, dimensional modelling, and table transactions; ClickHouse prioritises freshness and interactive query performance.

[FIGURE 1 PLACEHOLDER]

Caption: Figure 1. End-to-End Architecture of the Modern Data Analytics Platform.

Recommended placement: Section 4.1, High-Level Architecture.

## 4.2 Medallion Architecture

Bronze is an append-oriented record of what arrived after schema decoding and minimal standardisation. Silver is the governed layer, where data quality, deduplication, referential validation, keys, dimensions, facts, and pipeline state are created. Gold is the direct analytical serving path in ClickHouse. Bonus is a logically separate monitoring use case over direct Kafka-fed ClickHouse data. Gold and Bonus do not use Silver Iceberg as their primary input.

[FIGURE 2 PLACEHOLDER]

Caption: Figure 2. Transactional and Behavioural Data Flows Across the Bronze, Silver, Gold, and Bonus Layers.

Recommended placement: Section 4.2, Medallion Architecture.

## 4.3 Transactional Data Flow

Six Kafka topics are consumed by one Spark Structured Streaming query per logical table. The job writes Snappy-compressed Parquet to `s3a://bronze/transactional/<table>/<yyyyMMdd>`, with a separate checkpoint path per table. The daily Silver transactional DAG selects eligible Bronze partitions, validates and cleans each entity, performs order/order-item cross-validation, builds Kimball dimensions and facts, and writes Iceberg tables under the `transactional` namespace. In the direct serving path, corresponding ClickHouse Kafka Engine tables consume the same topics and Materialized Views populate `lakehouse.realtime_*` tables.

## 4.4 Behavioural Data Flow

The behavioural job consumes `behavioral.events`, decodes the Confluent wire envelope, compares the wire schema ID with the registered schema ID, standardises fields, and writes date-partitioned Parquet under `s3a://bronze/behavioral/events/year=.../month=.../day=...`. The Silver behavioural DAG validates, classifies, cleans, deduplicates, quarantines, builds dimensions and the event fact, and writes Iceberg tables. ClickHouse independently consumes the behavioural topic into `lakehouse.kafka_behavioral_events`, with a Materialized View writing `lakehouse.realtime_behavioral_events`.

## 4.5 Gold Direct-Streaming Flow

The authoritative Gold path is Kafka → ClickHouse Kafka Engine tables → Materialized Views → final ClickHouse analytical tables → Metabase. Each Kafka Engine table uses `AvroConfluent` and a dedicated consumer group. Materialized Views trim and normalise strings, convert timestamps, calculate `is_valid`, record a single `validation_error`, append Kafka topic/partition/offset metadata, and write to MergeTree tables.

## 4.6 Bonus Monitoring Flow

The repository does not create a duplicate second set of Kafka consumers exclusively for monitoring. Instead, monitoring SQL reads the same direct-ingestion ClickHouse tables, which preserves the architectural requirement that Bonus is direct Kafka-to-ClickHouse while avoiding duplicate consumer groups and duplicate durable data. The logical Bonus plane consists of monitoring queries and Metabase cards for stream health, order rates, amount ranges, invalid records, and funnels.

## 4.7 Batch and Streaming Integration

Streaming handles ingestion and low-latency serving. Batch processing handles cross-table rules, SCD Type 2, dimensions, session recomputation, and table-level assertions. The two modes are joined through common source contracts and lineage fields rather than by making one a prerequisite for the other.

## 4.8 Architectural Principles

The final architecture applies separation of concerns, explicit data contracts, immutable Bronze landing, idempotent Silver writes, deterministic keys, preservation of rejected data, independent recovery domains, configuration through environment variables, and durable analytical storage. Claims of exactly-once end-to-end delivery are deliberately avoided: Spark checkpoints and Iceberg MERGE provide strong controls, but Kafka-to-ClickHouse replay semantics still require consumer-group and table-ordering discipline.

# 5. Technology Stack

## 5.1 Technology Summary

The platform is built from open-source components selected for clear workload ownership rather than feature overlap. Spark owns distributed processing; MinIO owns object persistence; Iceberg owns table semantics; Lakekeeper owns cataloguing; ClickHouse owns low-latency serving; Airflow owns scheduled orchestration; and Metabase owns presentation.

## 5.2 Component Responsibilities

Component responsibilities are summarised in Table 1 and expanded throughout the relevant chapters. The repository also includes PostgreSQL for Airflow, Metabase, and catalogue metadata; Redis for Airflow execution support; and Traefik labels for externally routed web interfaces.

## 5.3 Technology Selection Rationale

**Table 2. Technology Selection Rationale.**

| Technology | Reason selected | Workload fit |
| --- | --- | --- |
| Kafka | Partitioned, replayable event log and decoupled producers/consumers | Transactional and behavioural source transport |
| Schema Registry | Central schema identity and Avro contract management | Confluent wire-format decoding and compatibility |
| Spark Structured Streaming | Unified DataFrame APIs, Kafka source, micro-batch recovery | Bronze continuous ingestion |
| Spark batch | Distributed joins, windows, validation, and Iceberg MERGE support | Silver transformations and dimensional modelling |
| MinIO | S3-compatible storage deployable within Docker Compose | Bronze Parquet, Silver warehouse, checkpoints |
| Parquet | Columnar, compressed, interoperable files | Bronze persistence and Iceberg data files |
| Apache Iceberg | ACID table metadata, schema evolution, snapshots, MERGE | Governed Silver datasets |
| Lakekeeper | REST catalogue and warehouse management | Iceberg catalogue service for Spark |
| Airflow | Schedules, retries, task logs, templated process dates | Daily Silver orchestration |
| ClickHouse | Kafka Engine, Materialized Views, MergeTree and fast aggregation | Gold and Bonus serving |
| Metabase | Rapid dashboard creation and parameterised SQL | Business and monitoring visualisation |
| Docker Compose | Reproducible local/VPS service topology | Infrastructure delivery |



## 5.4 Component Integration

Spark connects to Kafka with `spark-sql-kafka`, to MinIO with Hadoop S3A, and to Iceberg through the Lakekeeper REST catalogue and S3FileIO. Airflow workers submit Spark jobs to the standalone Spark master. ClickHouse connects directly to Kafka and Schema Registry through Kafka Engine settings. Metabase connects to ClickHouse through the included ClickHouse-enabled image.

## 5.5 Technology Version Matrix

The runtime configuration identifies Spark 3.5.3, PostgreSQL 16, Redis 7, ClickHouse 25.3, Tabulario Iceberg REST 1.6.0, Iceberg client packages 1.6.1, Hadoop AWS 3.3.4, Airflow 3.2.1 in the Airflow image, and a dated MinIO server release from December 2024. The root `requirements.txt` pins PySpark 3.5.1 for Python package installation, while the Spark containers and submitted packages use Spark 3.5.3; operators should keep these versions aligned when rebuilding images.

# 6. Infrastructure and Containerisation

## 6.1 Docker Compose Architecture

`docker-compose.yml` defines the complete application stack after the externally managed Kafka and Schema Registry boundary. Common environment blocks keep Airflow, Spark, MinIO, Lakekeeper, and ClickHouse configuration consistent. Services are connected through the internal `datalake` bridge network and an external Traefik network for selected web interfaces.

## 6.2 Service Inventory

**Table 3. Docker Service Inventory.**

| Service | Image / build family | Purpose | Persistent volume |
| --- | --- | --- | --- |
| postgres | PostgreSQL 16 Alpine | Airflow metadata database | postgres_data |
| redis | Redis 7 Alpine | Airflow broker/result support | — |
| airflow-init | Custom Airflow 3.2.1 image | Database migration and initial account setup | airflow_logs, airflow_auth |
| airflow-apiserver | Custom Airflow 3.2.1 image | Airflow API and web interface | airflow_logs, airflow_auth |
| airflow-scheduler | Custom Airflow 3.2.1 image | DAG scheduling | airflow_logs |
| airflow-worker | Custom Airflow 3.2.1 image | Task execution | airflow_logs |
| airflow-dag-processor | Custom Airflow 3.2.1 image | DAG parsing | airflow_logs |
| airflow-triggerer | Custom Airflow 3.2.1 image | Deferred task triggers | airflow_logs |
| spark-master | Apache Spark 3.5.3 | Standalone cluster master and Spark UI | spark_data |
| spark-worker | Apache Spark 3.5.3 | Spark executor worker | spark_data |
| minio | MinIO server | Object storage | minio_data |
| iceberg-catalog-db | PostgreSQL 16 Alpine | Metadata DB for auxiliary Iceberg REST service | iceberg_catalog_data |
| iceberg-rest | Tabulario Iceberg REST 1.6.0 | Auxiliary REST catalogue service | — |
| createbuckets | MinIO client | Initial bucket creation | — |
| lakekeeper-db | PostgreSQL 16 Alpine | Lakekeeper metadata database | lakekeeper_db_data |
| lakekeeper-migrate | Lakekeeper image | Catalogue schema migration | — |
| lakekeeper | Lakekeeper | Authoritative Iceberg REST catalogue | — |
| lakekeeper-bootstrap | curl image | Lakekeeper bootstrap | — |
| lakekeeper-warehouse-init | curl image | Warehouse registration | — |
| clickhouse | ClickHouse 25.3 | Gold and monitoring analytical store | clickhouse_data |
| postgres-metabase | PostgreSQL 16 Alpine | Metabase application metadata | postgres_metabase_data |
| metabase | ClickHouse-enabled Metabase image | Dashboards and SQL questions | metabase_data |



## 6.3 Networks

The `datalake` bridge network provides service-to-service name resolution and isolates internal traffic. A pre-existing external Traefik network provides routed access to Airflow, MinIO, Lakekeeper, and Metabase. Public hostnames are intentionally represented in this document as `[AIRFLOW_PUBLIC_HOST]`, `[MINIO_PUBLIC_HOST]`, `[LAKEKEEPER_PUBLIC_HOST]`, and `[METABASE_PUBLIC_HOST]`.

## 6.4 Persistent Volumes

Named volumes preserve PostgreSQL databases, MinIO objects, ClickHouse data, Metabase metadata, Spark working data, Airflow logs/authentication state, and catalogue metadata across container restarts. Checkpoint objects and Iceberg data reside in MinIO and therefore inherit the persistence of `minio_data`.

## 6.5 Service Dependencies

Airflow components depend on the Airflow PostgreSQL database and Redis. Spark workers depend on the Spark master. Lakekeeper migrations and bootstrap precede warehouse initialisation. Metabase depends on its PostgreSQL metadata database and the availability of ClickHouse for analytical queries. ClickHouse direct consumers additionally require network access to the external Kafka broker and Schema Registry.

## 6.6 Health Checks

The Compose configuration contains health checks for the principal databases, Airflow components, Spark master, MinIO, Iceberg REST, Lakekeeper, ClickHouse, and Metabase. Health checks should be treated as service-readiness indicators, not as proof that an end-to-end data path is correct; Chapter 23 adds data-level validation.

## 6.7 External Access

Spark exposes the master RPC interface and master/worker web UIs through mapped ports. Iceberg REST, Lakekeeper, ClickHouse HTTP/native, and selected routed web applications are externally reachable according to the deployment configuration. Credentials and exact private routes must remain outside documentation and source control.

## 6.8 Resource Considerations

Silver DAGs submit Spark drivers with 2 GiB and executors with 4 GiB and two executor cores. Bronze jobs default to small shuffle partitions and bounded executor cores suitable for the project environment. Production sizing must be based on measured message rate, partition count, state size, object-store latency, and ClickHouse ingestion volume; this repository does not provide benchmark evidence for production capacity claims.

# 7. Source Data and Data Contracts

## 7.1 Source Systems

The platform receives externally managed Kafka messages. No source operational database is provisioned inside this repository. Transactional topics expose entity-oriented records, while `behavioral.events` exposes event-oriented clickstream records. The original source contract is represented by topic names, Schema Registry subjects, and the Spark/ClickHouse schemas in the repository.

## 7.2 Kafka Topic Organisation

**Table 4. Kafka Topic Mapping.**

| Kafka topic | Logical dataset | Business content | Domain |
| --- | --- | --- | --- |
| transactional.categories | categories | Category master data | Transactional |
| transactional.order_items | order_items | Order-line records | Transactional |
| transactional.orders | orders | Order header records | Transactional |
| transactional.product_price_history | product_price_history | Product price intervals | Transactional |
| transactional.products | products | Product master data | Transactional |
| transactional.users | users | User master data | Transactional |
| behavioral.events | events | Clickstream and interaction events | Behavioural |



**Table 5. Schema Registry Subject Mapping.**

| Kafka topic | Value subject | Usage |
| --- | --- | --- |
| transactional.categories | transactional.categories-value | Latest value schema fetched from Schema Registry |
| transactional.order_items | transactional.order_items-value | Latest value schema fetched from Schema Registry |
| transactional.orders | transactional.orders-value | Latest value schema fetched from Schema Registry |
| transactional.product_price_history | transactional.product_price_history-value | Latest value schema fetched from Schema Registry |
| transactional.products | transactional.products-value | Latest value schema fetched from Schema Registry |
| transactional.users | transactional.users-value | Latest value schema fetched from Schema Registry |
| behavioral.events | behavioral.events-value | Latest value schema fetched from Schema Registry |



## 7.3 Transactional Data Dictionary

**Table 6. Transactional Data Dictionary.**

| Dataset | Source fields | Grain | Contract note |
| --- | --- | --- | --- |
| categories | category_id, name, parent_category_id | One category | `parent_category_id` nullable |
| order_items | order_item_id, order_id, product_id, quantity, unit_price, item_total_amount | One item within an order | All fields nullable at Bronze parse stage; validated later |
| orders | order_id, user_id, timestamp, total, status, payment_method | One order | `payment_method` nullable union |
| product_price_history | price_history_id, product_id, price, valid_from, is_current | One price-history record | Silver can derive `valid_to` |
| products | product_id, name, price | One product | Silver reader also tolerates optional enriched columns when present |
| users | user_id, username, email, signup_date, loyalty_tier, location | One user | `loyalty_tier` and `location` nullable unions |



## 7.4 Behavioural Data Dictionary

The behavioural schema is dynamic in Schema Registry; the local module intentionally stores only the required and optional column contracts. Event-specific optional fields remain null when they do not apply.

**Table 7. Behavioural Event Dictionary.**

| Field | Type | Required / optional | Analytical purpose |
| --- | --- | --- | --- |
| event_id | String | Required by contract; Silver can fall back to Kafka coordinates | Producer/event identity |
| timestamp | String | Required | Event time before parsing |
| user_id | String | Required at Bronze; anonymous users become Silver warnings | User identity |
| event_type | String | Required | Type of interaction |
| device_type / device | String | Required contract; registry payload is standardised to device_type | Device context |
| session_id | String | Required | Session identity |
| ip_address | String | Optional | Hashed in Silver |
| utm_source | String | Optional | Marketing attribution |
| product_id | String | Optional | Product context |
| quantity | Integer | Optional | Item quantity |
| cart_total_items | Integer | Optional | Cart item count |
| cart_items | Array<Tuple> | Optional | Cart detail |
| cart_value | Double | Optional | Cart monetary value |
| shipping_method | String | Optional | Shipping choice |
| order_id | String | Optional | Order correlation |
| fulfillment_speed | String | Optional | Fulfilment option |
| url_path | String | Optional | Page path |
| duration_sec | Integer | Optional | Duration |
| http_status | Integer | Optional | HTTP response status |
| payment_type | String | Optional | Payment context |
| success | Boolean | Optional | Outcome flag |
| error_code | String | Optional | Failure code |
| query | String | Optional | Search text |
| results_count | Integer | Optional | Search result count |
| clicked_position | Integer | Optional | Rank clicked |
| rating | Integer | Optional | Rating value |
| text_length | Integer | Optional | Review/text length |
| wishlist_name | String | Optional | Wishlist context |



## 7.5 Schema Registry

`src/common/registry_client.py` retrieves the latest subject schema and its numeric ID. Transactional topics use the `<topic>-value` convention. Behavioural metadata explicitly defines `behavioral.events-value`. Registry credentials or URLs are supplied at runtime and are redacted here.

## 7.6 Avro Message Handling

The Confluent wire format begins with a magic byte followed by a four-byte schema ID and the Avro payload. The Bronze decoders expose the wire schema ID, raw value, topic, partition, offset, and Kafka timestamp. The behavioural decoder explicitly reports invalid magic bytes, short messages, Avro decode failures, and schema-ID mismatches. The transactional decoder captures the wire schema ID and parses against the expected schema; it does not prove an explicit wire-ID equality check, so downstream lineage remains important.

## 7.7 Required and Optional Fields

All transactional fields are initially nullable in Spark to prevent a malformed record from terminating stream construction. Required-field enforcement occurs in Silver. Behavioural required fields are `event_id`, `timestamp`, `user_id`, `event_type`, `device_type`, and `session_id`, with the operational nuance that Silver accepts Kafka coordinates as a reliable event identity and treats missing user/device attributes as warnings in selected cases.

## 7.8 Kafka Metadata and Lineage

Lineage includes Kafka topic, partition, offset, broker timestamp, raw value or source file, wire schema ID, Bronze ingestion timestamp, Silver run ID, processing date, and Silver ingestion timestamp. ClickHouse adds `_topic`, `_partition`, `_offset`, and an ingestion timestamp through each Materialized View.

# 8. Bronze Transactional Layer

## 8.1 Purpose

The transactional Bronze layer creates a replayable, schema-shaped representation of six source topics without applying business joins or dimensional modelling. It is implemented in `src/jobs/bronze_transactional_job.py` and supporting modules under `src/common/` and `src/schemas/`.

## 8.2 Kafka Consumption

One streaming query is started for each configured table. `KAFKA_BOOTSTRAP_SERVERS` is required, starting offsets default to `earliest`, and `failOnDataLoss` is disabled so the job can continue when Kafka retention removes an unavailable offset. This setting increases resilience but does not recreate expired source messages.

## 8.3 Schema Enforcement

Static PySpark `StructType` definitions represent the six logical entities. Nullable Avro-union strings can arrive as nested `{string: value}` structures and are flattened during standardisation. Topic and schema mappings are validated to ensure every logical table has both definitions.

## 8.4 Spark Structured Streaming

The Spark session uses UTC for Bronze timestamp handling, Kafka source integration, Hadoop S3A for MinIO, small default shuffle partitioning, and controlled executor resources. Each query uses `foreachBatch`, allowing a deterministic batch ID and explicit target path while preserving streaming checkpoints.

## 8.5 Initial Transformations

The layer standardises timestamp/date columns, flattens nullable unions, adds `source_table` and `bronze_ingestion_timestamp`, and calculates `partition_date`. It preserves Kafka lineage fields and raw message metadata. Business-level rejection is intentionally deferred to Silver.

## 8.6 MinIO Storage

Outputs are appended as Snappy-compressed Parquet. The base pattern is `s3a://bronze/transactional/<table>/<partition_date>`. Separate paths per table make batch selection and recovery independent.

## 8.7 Partitioning

Orders use parsed `event_timestamp`; product-price history uses `valid_from_timestamp`; users use `signup_date`; categories, order items, and products use the Kafka timestamp. The resulting partition string is `yyyyMMdd`.

## 8.8 Checkpoints

Each table writes checkpoint state to `s3a://tr-checkpoints/transactional/<table>`. Checkpoint isolation prevents one entity’s offsets or failures from contaminating another entity’s query.

## 8.9 Recovery

On restart, Spark reads the checkpoint to recover committed source progress and micro-batch state. Operators must not delete checkpoints during a normal restart. A deliberate full replay requires a new consumer/checkpoint path or an explicit reset after confirming the Bronze target can safely accept replayed files.

## 8.10 Output Datasets

The exact Bronze path and checkpoint mappings are consolidated in Tables 9 and 10 in Section 10.

# 9. Bronze Behavioural Layer

## 9.1 Purpose

The behavioural Bronze layer captures the full clickstream envelope, preserves undecodable/invalid evidence, and creates a stable partitioned Parquet dataset for Silver processing. Its source files are `src/jobs/bronze_behavioral_job.py`, `src/common/bronze_behavioral_*`, `src/common/registry_client.py`, and `src/schemas/bronze_behavioral_schemas.py`.

## 9.2 Kafka Consumption

The default topic is `behavioral.events`, configurable through `BEHAVIORAL_KAFKA_TOPIC`. Starting offsets default to `earliest`, `failOnDataLoss` is false, and the micro-batch trigger defaults to 30 seconds.

## 9.3 Behavioural Event Schema

The latest Avro schema is obtained from `behavioral.events-value`. Required and optional metadata are listed in Table 7. The Kafka Engine SQL uses the registry field name `device`, while Bronze standardisation produces `device_type`; this translation is explicit in the Spark transformation.

## 9.4 Event Standardisation

String values are trimmed, event types are lower-cased, devices are normalised, and multiple timestamp formats are attempted. The Bronze `event_id` is deterministically generated as a SHA-256 value from Kafka topic, partition, and offset, providing a non-null source-coordinate identity. The source payload’s original event ID is not retained as a separate field by that transformation.

## 9.5 Initial Validation

Validation records missing required fields, invalid timestamps, Avro decoding failures, and schema-ID mismatch. Invalid rows are retained with flags. This supports forensic inspection and Silver quarantine instead of destructive filtering in the hot path.

## 9.6 MinIO Storage

The writer appends Snappy-compressed Parquet to the behavioural Bronze prefix. It can coalesce each micro-batch to a small number of output files and sets a maximum records-per-file threshold to control object proliferation.

## 9.7 Partitioning

The path uses `year`, `month`, and `day` columns derived from parsed event time. When event time is invalid, the Kafka broker timestamp supplies the partition so the record remains discoverable.

## 9.8 Checkpoints

The checkpoint path is `s3a://be-checkpoints/behavioral/events`, separate from the transactional checkpoint bucket.

## 9.9 Recovery

Spark resumes from the checkpoint and preserves the relationship between Kafka offsets and committed micro-batches. Because the deterministic event identity uses Kafka coordinates, Silver can detect a replayed event even when Bronze receives duplicate files.

## 9.10 Output Dataset

The output is a lineage-rich behavioural event dataset with parsed fields, raw/decode metadata, validation flags, partition columns, and Kafka coordinates. It is not yet the analytical fact table; Silver determines processability and dimensional keys.

# 10. MinIO Storage Architecture

## 10.1 MinIO Role

MinIO is the durable S3-compatible storage boundary for the lakehouse. Spark accesses it with Hadoop S3A for Parquet and checkpoint files, while Iceberg uses S3FileIO through the catalogue configuration.

## 10.2 Bucket Organisation

**Table 8. MinIO Bucket Mapping.**

| Bucket | Responsibility | Access pattern |
| --- | --- | --- |
| bronze | Append-only transactional and behavioural Parquet | Spark Bronze jobs write; Silver jobs read |
| silver | Iceberg warehouse objects and Silver namespaces | Spark/Iceberg write; Lakekeeper registers |
| tr-checkpoints | Transactional Structured Streaming checkpoints | One prefix per transactional table |
| be-checkpoints | Behavioural Structured Streaming checkpoint | One behavioural event prefix |
| gold | Provisioned bucket for future/object-based Gold uses | Not the primary direct ClickHouse Gold input |
| warehouse | Provisioned by bucket initialisation for catalogue compatibility | Auxiliary/initialisation use |



**Table 9. Bronze Output Path Mapping.**

| Logical dataset | Kafka topic | Bronze path | Partition source |
| --- | --- | --- | --- |
| categories | transactional.categories | s3a://bronze/transactional/categories/<yyyyMMdd> | _kafka_timestamp |
| order_items | transactional.order_items | s3a://bronze/transactional/order_items/<yyyyMMdd> | _kafka_timestamp |
| orders | transactional.orders | s3a://bronze/transactional/orders/<yyyyMMdd> | event_timestamp |
| product_price_history | transactional.product_price_history | s3a://bronze/transactional/product_price_history/<yyyyMMdd> | valid_from_timestamp |
| products | transactional.products | s3a://bronze/transactional/products/<yyyyMMdd> | _kafka_timestamp |
| users | transactional.users | s3a://bronze/transactional/users/<yyyyMMdd> | signup_date |
| events | behavioral.events | s3a://bronze/behavioral/events/year=<YYYY>/month=<M>/day=<D> | event_timestamp with Kafka timestamp fallback |



**Table 10. Streaming Checkpoint Mapping.**

| Stream | Checkpoint path | State owner |
| --- | --- | --- |
| categories | s3a://tr-checkpoints/transactional/categories | Spark Structured Streaming |
| order_items | s3a://tr-checkpoints/transactional/order_items | Spark Structured Streaming |
| orders | s3a://tr-checkpoints/transactional/orders | Spark Structured Streaming |
| product_price_history | s3a://tr-checkpoints/transactional/product_price_history | Spark Structured Streaming |
| products | s3a://tr-checkpoints/transactional/products | Spark Structured Streaming |
| users | s3a://tr-checkpoints/transactional/users | Spark Structured Streaming |
| behavioural events | s3a://be-checkpoints/behavioral/events | Spark Structured Streaming |

[FIGURE 3 PLACEHOLDER]

Caption: Figure 3. MinIO Bucket Structure Containing Bronze, Silver, Transactional Checkpoints, and Behavioural Checkpoints.

Recommended placement: Section 10.2, Bucket Organisation.

## 10.3 Bronze Paths

Transactional paths are table-oriented and use compact `yyyyMMdd` partition directories. Behavioural paths use Hive-style `year=`, `month=`, and `day=` directories. This difference reflects the independent implementations and is handled by their respective Silver readers.

## 10.4 Silver Paths

The Silver warehouse is registered through Lakekeeper with MinIO. Physical folders observed under the bucket are catalogue-managed warehouse identifiers for the `transactional`, `transactional_quality`, `behavioral`, and `behavioral_quality` namespaces. These generated identifiers are storage implementation details; clients must address tables through catalogue-qualified names rather than hard-coded physical UUID directories.

## 10.5 Checkpoint Paths

Checkpoint buckets are separated from Bronze data so data-retention or lifecycle policies can be managed independently. Checkpoints are operational state and must not be queried as business data.

## 10.6 Partition Layout

Bronze partitioning is event-date oriented. Silver partitioning is table-specific: event facts use day transforms; order facts use month or bucket transforms; quality and state tables use source or date transforms. Iceberg hides physical partition evolution from clients through metadata.

## 10.7 Persistence

The `minio_data` named volume persists buckets and objects. Production operation should additionally configure backup, object versioning or replication where appropriate, restricted service credentials, lifecycle policies, and external monitoring of free capacity.

# 11. Silver Transactional Layer

## 11.1 Purpose

The transactional Silver pipeline transforms entity-oriented Bronze Parquet into a validated Kimball model in Iceberg. The implementation is led by `src/jobs/silver_transactional_job.py` and the `src/common/silver_transactional_*` modules.

## 11.2 Airflow Workflow

The `silver_transactional_pipeline` DAG runs at 02:00 daily, uses the Asia/Tehran data-interval date formatted as `yyyyMMdd`, submits the job through `SparkSubmitOperator`, disables catch-up, allows one active run, and retries twice with five-minute delays.

## 11.3 Bronze Data Reading

The reader inspects Parquet footers and groups files by schema fingerprint before unioning compatible DataFrames with missing columns allowed. It selects partitions differently by entity: categories use the latest eligible partition; users, products, and price history can read all eligible history; orders include the process partition plus a repair partition when present; order items read the exact process date. Kafka-coordinate duplicates are resolved deterministically.

## 11.4 Validation

Required columns, type expectations, business keys, non-negative monetary values, positive quantities, ID formats, email structure, timestamp validity, and price intervals are checked. Results are separated into rejected, repaired, warning, or valid categories.

## 11.5 Cleaning

Cleaning collapses null-like strings, normalises ID casing, lower-cases email and controlled text fields, trims names, casts numeric types, calculates item amounts, resolves order dates, and deduplicates by business key using latest lineage metadata.

## 11.6 Cross-Table Validation

Order items are validated against orders. A left-semi join keeps order items with an existing parent order. A left-anti join identifies orphans, records `MISSING_PARENT_ORDER` as a rejected quality issue, and excludes the orphan from `fact_order_item`.

## 11.7 Kimball Modelling

The pipeline builds conformed date, user, category, product, product-price, order, and order-item structures. Deterministic SHA-256-based keys (`USR_`, `CAT_`, `PRD_`, and `PPR_`) provide stable surrogate keys across reruns.

## 11.8 Dimensions

`dim_date` spans 2020-01-01 through 2035-12-31. `dim_user`, `dim_category`, and `dim_product` preserve descriptive attributes. `dim_product_price_scd` stores price intervals and includes an unknown price record per product beginning on 1900-01-01 to support facts that cannot match a known interval.

## 11.9 Fact Tables

`fact_order` has one row per order and links user/date keys with status, payment method, amount, and `order_count=1`. `fact_order_item` has one row per order item and links order, product, date, and price-history keys while retaining quantity, unit price, calculated amount, historical price, and price difference.

## 11.10 SCD Type 2

Price-history rows are ordered by `valid_from`; a lead window derives `valid_to` where needed, and current-row ranking identifies the active record. Facts use an interval join to resolve the applicable product-price key at order time.

## 11.11 Iceberg Writes

Tables use Iceberg format version 2, Zstandard compression, hash distribution, and strict schema/merge-key checks. `MERGE` updates mutable attributes while preserving `silver_created_at` and updating `silver_updated_at`.

## 11.12 Quality Tables

The `transactional_quality.transactional_validation_issues` table stores a stable issue key, source table, record identifier, status, arrays of errors and warnings, optional repair description, original record, lineage, and detection timestamps. It is partitioned by source table and day of detection and uses insert-only merge semantics for stable issue identities.

## 11.13 Idempotency

Deterministic business and surrogate keys, source deduplication, non-null/unique merge-key assertions, local checkpointing of merge inputs, and Iceberg MERGE operations permit a process date to be rerun without intentionally multiplying logical records.

## 11.14 Outputs

**Table 11. Silver Transactional Table Mapping.**

| Table | Grain | Merge / logical key | Purpose |
| --- | --- | --- | --- |
| lakekeeper.transactional.dim_date | One calendar date | date_key | Calendar attributes |
| lakekeeper.transactional.dim_user | One user | user_id | Stable user_key and user attributes |
| lakekeeper.transactional.dim_category | One category | category_id | Category and parent key |
| lakekeeper.transactional.dim_product | One product | product_id | Product and category key |
| lakekeeper.transactional.dim_product_price_scd | One price-history interval | price_history_id | SCD Type 2 price attributes |
| lakekeeper.transactional.fact_order | One order | order_id | Order metrics and user/date keys |
| lakekeeper.transactional.fact_order_item | One order item | order_item_id | Product, price, amount, and order keys |
| lakekeeper.transactional_quality.transactional_validation_issues | One stable quality issue | issue_key | Rejected, warning, and repaired evidence |



**Table 13. Transactional Dimension and Fact Mapping.**

| Table | Model role | Key strategy | Analytical grain |
| --- | --- | --- | --- |
| dim_date | Dimension | date_key | Calendar conformance |
| dim_user | Dimension | user_id → user_key | User attributes |
| dim_category | Dimension | category_id → category_key | Category hierarchy |
| dim_product | Dimension | product_id → product_key | Product attributes |
| dim_product_price_scd | SCD dimension | price_history_id → product_price_key | Effective product price interval |
| fact_order | Fact | order_id | Order-level measure grain |
| fact_order_item | Fact | order_item_id | Order-line measure grain |



# 12. Silver Behavioural Layer

## 12.1 Purpose

The behavioural Silver pipeline converts sparse Bronze events into processable records, quality evidence, dimensions, sessions, and a stable event fact. Its implementation includes `src/jobs/silver_behavioral_job.py`, `src/common/silver_behavioral_*`, and `workflow/dags/silver_behavioral_dag.py`.

## 12.2 Airflow Workflow

`silver_behavioral_etl_v2` runs at 02:00 daily. A Python task first checks that the expected MinIO Bronze prefix contains at least one object. A Bash task then submits Spark with the required Iceberg, S3, and catalogue packages. Manual runs may override the execution date.

## 12.3 Event Validation

A processable event requires a session ID, event type, parseable timestamp, and reliable identity. Reliable identity means either an event ID or the complete Kafka topic/partition/offset tuple. User and device omissions can be warnings rather than errors, preserving anonymous or partially attributed behaviour.

## 12.4 Warning and Error Classification

Critical errors include missing event identity, missing session, missing event type, invalid timestamp, and duplicate event key. Warnings include missing or anonymous user, missing UTM source, unknown device/event type, invalid IP format, missing producer event ID when Kafka identity is available, negative counters, out-of-range ratings, and invalid HTTP status ranges. Bronze decode/identity flags are translated into Silver issue codes.

## 12.5 Cleaning

Null-like strings become null; device aliases are grouped into mobile, desktop, tablet, or unknown; event/payment/shipping/fulfilment/UTM values are lower-cased and underscored; numeric and timestamp values use tolerant casts; and a fixed `silver_cleaned_at` timestamp is applied to the run.

## 12.6 Deduplication

`event_key` uses event ID first, Kafka coordinates second, and stable fallback fields only when neither preferred identity is available. Duplicate keys are rejected deterministically after sorting by lineage and ingestion attributes.

## 12.7 Quarantine

Rejected rows are written to `behavioral.behavioral_events_quarantine` with raw attributes, errors, warnings, lineage, and first/last quarantine timestamps. They are excluded from the fact table but remain available for repair and replay.

## 12.8 Data-Quality Issues

`behavioral_quality.behavioral_validation_issues` stores one row per individual issue code, with stable record and issue hashes, severity, source metadata, and first/last detection times. Warning issues can coexist with processable fact rows.

## 12.9 Behavioural Dimensions

The device dimension tracks first and last seen times for each normalised device. The event-type dimension maps event types into browse, search, cart, checkout, engagement, wishlist, error, or other categories. The session dimension stores user association, session boundaries, duration, primary device, and event count.

## 12.10 Behavioural Fact Table

`fact_behavioral_events` has one row per stable `event_key`. It retains business attributes, dimension keys, event timestamp, optional product/order/cart/search fields, data-quality flags, Kafka lineage, processing date, run ID, and Silver ingestion timestamp. IP addresses are represented only through `ip_address_hash`.

## 12.11 Session Modelling

For touched session keys, the pipeline reads the full existing fact state and recomputes start, end, duration, representative user, primary device, and event count. This avoids producing incomplete session aggregates from only the current day’s incremental rows.

## 12.12 Iceberg MERGE

Writer strategies include insert-only, upsert-all, upsert-while-preserving bounds, and fact-deduplicating insert. Inputs are checked for exact schema alignment, non-null keys, and uniqueness before MERGE.

## 12.13 Pipeline-State Tracking

`behavioral_pipeline_state` stores a deterministic run key for `silver_behavioral|execution_date`, status, start/completion times, raw/valid/warning/processable/rejected/fact counts, and error text. States include running, succeeded, failed, and empty outcomes.

## 12.14 Outputs

**Table 12. Silver Behavioural Table Mapping.**

| Table | Grain | Key | Purpose |
| --- | --- | --- | --- |
| lakekeeper.behavioral.dim_behavioral_device | One normalised device | device_key | Device attributes and bounds |
| lakekeeper.behavioral.dim_behavioral_event_type | One event type | event_type_key | Type and analytical category |
| lakekeeper.behavioral.dim_behavioral_session | One session | session_key | Session bounds and engagement |
| lakekeeper.behavioral.fact_behavioral_events | One stable event | event_key | Behavioural fact and lineage |
| lakekeeper.behavioral.behavioral_events_quarantine | One rejected event identity | event_key | Rejected event evidence |
| lakekeeper.behavioral.behavioral_pipeline_state | One pipeline/date run | run_key | Run state and metrics |
| lakekeeper.behavioral_quality.behavioral_validation_issues | One issue per record/code | issue_key | Warnings and errors |



**Table 14. Behavioural Dimension and Fact Mapping.**

| Table | Model role | Key strategy | Grain |
| --- | --- | --- | --- |
| dim_behavioral_device | Dimension | SHA-256 device_key | Normalised device |
| dim_behavioral_event_type | Dimension | SHA-256 event_type_key | Event type and category |
| dim_behavioral_session | Dimension | SHA-256 session_key | Session lifecycle |
| fact_behavioral_events | Fact | event_key | One behavioural event |
| behavioral_events_quarantine | Quality/quarantine | event_key | Rejected event |
| behavioral_validation_issues | Quality | issue_key | One issue occurrence |
| behavioral_pipeline_state | Audit | run_key | One execution date |



# 13. Apache Iceberg and Lakekeeper

## 13.1 Iceberg Role

Parquet alone does not provide atomic table commits, snapshot history, schema evolution, or row-level MERGE semantics. Iceberg adds table metadata, manifests, snapshots, partition specifications, and transactional commits while retaining Parquet as the data-file format.

## 13.2 Lakekeeper REST Catalog

Spark is configured with a catalogue named `lakekeeper`, type `rest`, and a runtime URI represented as `[LAKEKEEPER_REST_ENDPOINT]/catalog`. Lakekeeper is the authoritative catalogue used by the Silver jobs. An auxiliary Tabulario Iceberg REST service also exists in Compose, but pipeline configuration points to Lakekeeper.

## 13.3 Warehouse Configuration

The warehouse name is `silver`, registered against the MinIO Silver bucket through the warehouse initialisation service. Storage credentials are supplied at runtime and are omitted from documentation.

## 13.4 Namespaces

The final namespaces are `transactional`, `transactional_quality`, `behavioral`, and `behavioral_quality`. Namespace separation prevents analytical tables from being confused with quality evidence and allows domain-specific lifecycle management.

## 13.5 Table Registration

Jobs create namespaces and tables if absent, then address them through catalogue-qualified names. Physical object paths are managed by the catalogue and must not be manually constructed from generated folder identifiers.

## 13.6 ACID Transactions

Iceberg commits metadata atomically, so a successful MERGE exposes a complete new snapshot. Failed jobs do not intentionally publish partial table metadata. This is central to idempotent Silver reruns.

## 13.7 Schema Evolution

The writers enforce the expected final schema at write time. Iceberg can support compatible evolution, but any change should update source contracts, transformation code, table definitions, and assertions together rather than relying on implicit coercion.

## 13.8 Snapshot Management

Every successful table commit creates snapshot metadata and manifest references. Snapshot expiration and orphan-file cleanup are operational maintenance tasks; no aggressive automatic retention policy is asserted by the repository.

## 13.9 MinIO Integration

Spark uses Iceberg S3FileIO with path-style access and the MinIO endpoint. Transactional and behavioural tables store data files, manifests, and metadata in the registered warehouse.

[FIGURE 4 PLACEHOLDER]

Caption: Figure 4. Silver-Layer Apache Iceberg Storage Structure in MinIO.

Recommended placement: Section 13.9, MinIO Integration.

# 14. Data Quality and Validation

## 14.1 Data-Quality Strategy

Data quality is layered. Bronze records decoding and structural problems while preserving rows. Silver applies domain rules, duplicate prevention, referential checks, warnings, quarantine, and table assertions. ClickHouse Materialized Views add a lightweight serving-layer validity flag and error reason so low-latency dashboards can exclude invalid records without losing them.

## 14.2 Bronze Validation

Transactional Bronze enforces parsing schemas and lineage but defers business rules. Behavioural Bronze records decode success, schema-ID match, required-field presence, and timestamp parse status.

## 14.3 Silver Transactional Validation

Rules include required keys, identifier patterns, email format, type conversion, non-negative monetary values, positive quantities, valid price intervals, timestamp repair, and order-item parent existence.

## 14.4 Silver Behavioural Validation

Rules include reliable event identity, required session/type/time, duplicate event key, known device/type warnings, optional-field ranges, and sanitisation of sensitive IP information.

## 14.5 ClickHouse Stream Validation

Each Materialized View calculates `is_valid` and a prioritised `validation_error`. Examples include missing keys, negative amounts, invalid timestamps, future signup dates, invalid email, invalid ratings, and negative behavioural counters.

## 14.6 Rejected Records

Rejected transactional issues are recorded in `transactional_validation_issues`; rejected behavioural events are written to quarantine and individual issues are written to the behavioural quality table. Direct ClickHouse ingestion retains invalid rows in final realtime tables with `is_valid=0`.

## 14.7 Quarantine

Quarantine is reserved for behavioural events that cannot safely enter the fact model. It retains enough identity and raw context to diagnose, repair, and selectively replay the record.

## 14.8 Data-Quality Tables

Quality tables separate issue analytics from facts. They allow teams to measure error frequency by source, rule, severity, and date without scanning raw files.

## 14.9 Duplicate Prevention

Kafka coordinates provide source-level uniqueness. Silver uses deterministic keys and unique-merge assertions. ClickHouse realtime tables include partition and offset in sorting keys, but MergeTree does not automatically deduplicate replayed rows. Queries can use latest-offset logic, views such as `latest_products`, or controlled consumer-group replay. The OBT support tables use ReplacingMergeTree for version-based replacement.

## 14.10 Data Lineage

Lineage is propagated rather than reconstructed: Kafka coordinates, source topic/table, schema ID, Bronze ingestion time, source file, process date, run ID, and Silver/Gold load times provide a trace from serving data back to source messages.

## 14.11 Assertions and Contracts

Before Iceberg writes, writers validate expected columns, key non-nullness, and uniqueness. Session/fact counts and pipeline state provide additional operational contracts. ClickHouse DDL fixes input/output types, and verification SQL checks row uniqueness, freshness, and analytical distributions.

**Table 15. Data-Quality Rule Matrix.**

| Layer | Dataset | Validation rule | Severity | Failure handling | Output location |
| --- | --- | --- | --- | --- | --- |
| Bronze behavioural | behavioral.events | Avro decode and wire schema ID | Error | Retain row with decode flags | Bronze Parquet |
| Bronze behavioural | behavioral.events | Required event fields / timestamp | Error | Retain invalid row for Silver | Bronze Parquet |
| Silver transactional | orders | Required ID/user/time/total; total non-negative | Reject or repair | Issue table; exclude rejected fact | transactional_quality.transactional_validation_issues |
| Silver transactional | order_items | Positive quantity; non-negative amounts; parent order | Reject | Issue table; orphan excluded | transactional_quality.transactional_validation_issues |
| Silver transactional | users | ID and email format | Warning / reject by required key | Normalise and record issue | transactional_quality.transactional_validation_issues |
| Silver behavioural | events | Reliable identity, session, event type, timestamp | Reject | Quarantine + issue row | behavioral_events_quarantine / behavioral_validation_issues |
| Silver behavioural | events | Unknown device/type, optional range checks | Warning | Fact retained with dq_flags | behavioral_validation_issues + fact |
| ClickHouse direct | all realtime entities | Entity-specific required/value rules | Invalid flag | Retain with is_valid=0 | lakehouse.realtime_* |
| Iceberg writer | all Silver tables | Schema, non-null merge key, unique merge key | Fail job | Abort write | Airflow/Spark logs and pipeline state |



# 15. Gold Layer: Direct Kafka-to-ClickHouse Streaming

## 15.1 Overview

The final Gold serving path consumes Kafka directly. This is an authoritative architectural correction: Silver Iceberg is essential for the governed lakehouse, but it is not the primary source of Gold ingestion.

## 15.2 Gold Architecture

Kafka Engine tables subscribe to the source topics with `AvroConfluent`. Materialized Views execute on arriving blocks, standardise and validate each message, and insert into durable `lakehouse.realtime_*` MergeTree tables. Metabase queries those tables and derived views.

[FIGURE 6 PLACEHOLDER]

Caption: Figure 6. Direct Kafka-to-ClickHouse Gold Streaming Architecture Using Kafka Engine Tables and Materialized Views.

Recommended placement: Section 15.2, Gold Architecture.

## 15.3 Transactional Sources

The transactional Gold stream consumes categories, order items, orders, product-price history, products, and users. Each source has a matching `lakehouse.kafka_*` Kafka Engine table, `lakehouse.mv_realtime_*` Materialized View, and `lakehouse.realtime_*` final table.

## 15.4 Behavioural Sources

`lakehouse.kafka_behavioral_events` consumes `behavioral.events`. The view parses the event timestamp, normalises text, preserves event-specific optional values, computes validity, and writes to `lakehouse.realtime_behavioral_events`.

## 15.5 Kafka Engine Tables

**Table 17. ClickHouse Kafka Engine Table Mapping.**

| Kafka Engine table | Topic | Consumer group | Format | Consumers |
| --- | --- | --- | --- | --- |
| lakehouse.kafka_categories | transactional.categories | clickhouse-realtime-categories-v1 | AvroConfluent | 1 |
| lakehouse.kafka_order_items | transactional.order_items | clickhouse-realtime-order-items-v1 | AvroConfluent | 1 |
| lakehouse.kafka_orders | transactional.orders | clickhouse-realtime-orders-v1 | AvroConfluent | 1 |
| lakehouse.kafka_product_price_history | transactional.product_price_history | clickhouse-realtime-product-price-history-v1 | AvroConfluent | 1 |
| lakehouse.kafka_products | transactional.products | clickhouse-realtime-products-v2 | AvroConfluent | 1 |
| lakehouse.kafka_users | transactional.users | clickhouse-realtime-users-v1 | AvroConfluent | 1 |
| lakehouse.kafka_behavioral_events | behavioral.events | clickhouse-behavioral-groupk | AvroConfluent | 1 |



## 15.6 Schema Registry Integration

Every Kafka Engine table declares a Schema Registry endpoint through `format_avro_schema_registry_url`. Broker and registry addresses present in SQL are deployment-specific and are represented here as `[KAFKA_BROKER_ENDPOINT]` and `[SCHEMA_REGISTRY_ENDPOINT]`. For production hardening, these values should be parameterised rather than committed into DDL.

## 15.7 Materialized Views

**Table 18. ClickHouse Materialized View Mapping.**

| Materialized View | Source | Target | Principal transformation |
| --- | --- | --- | --- |
| lakehouse.mv_realtime_categories | lakehouse.kafka_categories | lakehouse.realtime_categories | Trim IDs/names; nullable parent; required-key validation |
| lakehouse.mv_realtime_order_items | lakehouse.kafka_order_items | lakehouse.realtime_order_items | Positive quantity; non-negative prices/amounts |
| lakehouse.mv_realtime_orders | lakehouse.kafka_orders | lakehouse.realtime_orders | Timestamp/status normalisation; non-negative total |
| lakehouse.mv_realtime_product_price_history | lakehouse.kafka_product_price_history | lakehouse.realtime_product_price_history | Price interval and current-flag validation |
| lakehouse.mv_realtime_products | lakehouse.kafka_products | lakehouse.realtime_products | Product text, inventory, price, popularity validation |
| lakehouse.mv_realtime_users | lakehouse.kafka_users | lakehouse.realtime_users | Email, signup date, device/loyalty/location normalisation |
| lakehouse.mv_realtime_behavioral_events | lakehouse.kafka_behavioral_events | lakehouse.realtime_behavioral_events | Timestamp parse, event normalisation, optional-range validation |



## 15.8 Stream Transformation

Transformations are deliberately lightweight and row-local: trimming, lower-casing, nullifying blanks, timestamp conversion, decimal validation, and metadata projection. Cross-table joins and historical dimensional logic remain Silver responsibilities.

## 15.9 Stream Validation

Each final realtime table has `is_valid UInt8` and `validation_error Nullable(String)`. The views use `multiIf` to record the first applicable failure reason. Invalid rows are retained, allowing monitoring and later diagnosis.

## 15.10 Final Analytical Tables

**Table 19. Final ClickHouse Table Mapping.**

| Table | Engine | Partitioning | Sorting key |
| --- | --- | --- | --- |
| lakehouse.realtime_categories | MergeTree | toYYYYMM(ingested_at) | category_id, kafka_partition, kafka_offset |
| lakehouse.realtime_order_items | MergeTree | No explicit partition | order_id, order_item_id, kafka_partition, kafka_offset |
| lakehouse.realtime_orders | MergeTree | toYYYYMM(order_timestamp) | order_timestamp, order_id, kafka_partition, kafka_offset |
| lakehouse.realtime_product_price_history | MergeTree | toYYYYMM(valid_from) | valid_from, product_id, price_history_id, kafka_partition, kafka_offset |
| lakehouse.realtime_products | MergeTree | toYYYYMM(ingested_at) | product_id, ingested_at, kafka_partition, kafka_offset |
| lakehouse.realtime_users | MergeTree | toYYYYMM(signup_date) | signup_date, user_id, kafka_partition, kafka_offset |
| lakehouse.realtime_behavioral_events | MergeTree | toYYYYMM(event_timestamp) | event_timestamp, event_type, session_id, kafka_partition, kafka_offset |
| lakehouse.transactional_obt | ReplacingMergeTree(silver_updated_at) | toYYYYMM(full_date) | full_date, category_id, product_id, order_id, order_item_id |
| lakehouse.behavioral_obt | ReplacingMergeTree(gold_loaded_at) | processing_date | processing_date, event_category, event_type, user_key, session_key, event_key |



## 15.11 Table Engines

The direct final tables use MergeTree because they preserve every consumed message and make Kafka coordinates queryable. The repository also contains `transactional_obt` and `behavioral_obt` ReplacingMergeTree tables used by supplementary batch OBT loaders and verification workflows. Those OBT paths are not the primary Gold ingestion architecture.

## 15.12 Partitioning

Orders, users, products, categories, price history, and behavioural events use monthly partitions based on their principal date or ingestion timestamp; order items omit an explicit partition and rely on the sorting key. Partition choices reflect expected time-bounded analysis and manageable partition counts.

## 15.13 Sorting Keys

Sorting keys begin with common time/entity filters and include Kafka partition/offset where available. This supports time scans, entity lookups, and deterministic latest-offset queries. It does not itself guarantee automatic deduplication.

## 15.14 Data Freshness

Freshness is governed by Kafka availability, consumer lag, ClickHouse polling, Materialized View execution, and final insert completion. The SQL queries use `ingested_at` and source event timestamps to calculate freshness. No fixed service-level objective is claimed without measured evidence.

## 15.15 Reliability and Replay

Consumer groups commit Kafka progress through the ClickHouse Kafka Engine. If replay is required, operators must plan the group reset or use a new group, understand whether the destination table will contain duplicates, and apply latest-offset or replacement logic. Replays must be tested on a bounded topic/time range before production use.

## 15.16 Analytical Outputs

`latest_products` uses `argMax` over Kafka offsets to expose the latest product representation. `current_product_prices` selects the current price record by `(valid_from, partition, offset)`. Realtime entity tables support revenue, user, category, price, order, engagement, and monitoring queries.

# 16. Bonus Layer: Near-Real-Time Monitoring

## 16.1 Overview

The Bonus layer turns the direct stream into operational and business-health signals. It is separate from the governed Silver batch lifecycle and therefore remains useful when a Silver DAG is waiting, retrying, or processing historical partitions.

## 16.2 Monitoring Architecture

The physical ingestion objects are shared with Gold: Kafka → Kafka Engine → Materialized View → realtime table. The logical Bonus path begins at the realtime tables and applies monitoring queries/cards. This implementation avoids an unnecessary second copy of each Kafka stream.

[FIGURE 7 PLACEHOLDER]

Caption: Figure 7. Direct Kafka-to-ClickHouse Bonus Monitoring Architecture.

Recommended placement: Section 16.2, Monitoring Architecture.

## 16.3 Business Health Monitoring

Business-health queries use valid order rows and event rows to track throughput, monetary ranges, activity by event type, user/session reach, and source attribution. Invalid rows remain queryable for quality monitoring.

## 16.4 Orders-per-Minute Monitoring

`sql/clickhouse/realtime_reports.sql` groups valid orders by `toStartOfMinute(order_timestamp)` over a recent interval. This produces a time series suitable for detecting pauses or abrupt changes in order ingestion.

## 16.5 Funnel Monitoring

The funnel query builds one row per session and uses ClickHouse `sequenceMatch` to test ordered progression through page view, product search, add to cart, checkout start, payment attempt, and order complete. It calculates conversion from the start, conversion from the previous stage, drop-off sessions, and drop-off rate.

## 16.6 Invalid Record Monitoring

Every realtime table exposes `is_valid` and `validation_error`, enabling counts by rule and source. Examples include invalid order timestamps, negative amounts, malformed email, invalid ratings, and missing identifiers.

## 16.7 Anomaly Detection

The repository provides monitoring primitives rather than a trained anomaly model. Operators can define thresholds for absent orders, lagging `ingested_at`, spikes in invalid rates, or unusual amount ranges. Automated statistical or machine-learning anomaly detection belongs in Future Enhancements.

## 16.8 Monitoring Queries

Implemented query families include orders per minute, order amount statistics, session funnel progression, latest product state, current product price, behavioural volume by category/type, device engagement, and UTM-source progression.

## 16.9 Monitoring Tables

No dedicated duplicate set of `bonus_*` tables is defined. Monitoring reads `lakehouse.realtime_orders`, `lakehouse.realtime_behavioral_events`, other realtime entities, and the derived views. This is an implementation fact and prevents fabricated object names.

## 16.10 Operational Value

The Bonus layer shortens detection time for stopped ingestion, data-contract problems, and funnel degradation. It also separates operational questions from the deeper historical modelling performed in Silver.

# 17. Apache Airflow Orchestration

## 17.1 Airflow Architecture

The Compose stack separates Airflow API server, scheduler, worker, DAG processor, triggerer, initialisation, metadata PostgreSQL, and Redis. DAG source code and Spark applications are mounted into the Airflow runtime.

## 17.2 DAG Inventory

**Table 16. Airflow DAG Reference.**

| DAG ID | Schedule | Timezone basis | Tasks | Retries | Role |
| --- | --- | --- | --- | --- | --- |
| silver_transactional_pipeline | 0 2 * * * | Asia/Tehran interval formatting | run_silver_transactional_job | 2 | Build transactional Iceberg Kimball tables |
| silver_behavioral_etl_v2 | 0 2 * * * | Asia/Tehran | check_behavioral_bronze_partition → run_silver_behavioral_job_v2 | 3 | Build behavioural Iceberg model and quality outputs |
| gold_transactional_daily | 0 3 * * * | Asia/Tehran | check_clickhouse_ready → run_gold_transactional_job | 2 | Supplementary Silver-to-OBT batch loader; not primary Gold path |
| gold_behavioral_clickhouse_etl | 0 3 * * * | Asia/Tehran | check_clickhouse_ready → run_gold_behavioral_job | 2 | Supplementary Silver-to-OBT batch loader; not primary Gold path |



[FIGURE 5 PLACEHOLDER]

Caption: Figure 5. Apache Airflow DAGs Orchestrating Transactional and Behavioural Silver Workflows.

Recommended placement: Section 17.2, DAG Inventory.

## 17.3 Transactional Silver DAG

The DAG uses `SparkSubmitOperator`, the `spark_default` connection, Iceberg 1.6.1 packages, Lakekeeper catalogue configuration, MinIO path-style access, the `transactional` and `transactional_quality` namespaces, and a date dimension range from 2020 through 2035.

## 17.4 Behavioural Silver DAG

The DAG first checks the exact date partition in MinIO through Boto3. It then runs `spark-submit` with catalogue and warehouse environment values. Its process date defaults to the day preceding the data interval start and can be overridden in `dag_run.conf`.

## 17.5 Gold-Related DAGs

Two batch OBT DAGs remain in the repository. They read Silver Iceberg and populate `transactional_obt` or `behavioral_obt`; their source descriptions predate the authoritative direct-stream correction. They are documented as supplementary reconciliation, backfill, or denormalised OBT utilities—not as the main Gold path. The direct Gold Kafka Engine and Materialized Views run inside ClickHouse and do not require a scheduled Airflow task for every message.

## 17.6 Scheduling

Silver runs at 02:00 and supplementary OBT jobs at 03:00. `catchup=False` prevents automatic historical expansion, and `max_active_runs=1` avoids overlapping logical dates.

## 17.7 Timezones

Airflow date templates use Asia/Tehran. Bronze Spark uses UTC for source timestamp standardisation, while Silver and OBT modelling use Asia/Tehran. ClickHouse OBT timestamp types explicitly include Asia/Tehran in the behavioural table. Operators must preserve explicit conversions at boundaries.

## 17.8 Retry Policies

Transactional Silver retries twice; behavioural Silver retries three times; both supplementary Gold DAGs retry twice. Retry delays are five minutes. Behavioural and Gold DAGs set two-hour execution timeouts.

## 17.9 Failure Handling

A missing behavioural Bronze partition fails the pre-check before Spark startup. Spark exceptions propagate to the Airflow task. Behavioural pipeline state records failed status and error information where the job reaches state tracking. Reruns should use the same logical date after the root cause is corrected.

## 17.10 Logs and Observability

Airflow task logs capture command construction and Spark-submit output; Spark UIs expose active applications and executors; application logs report counts and stages; MinIO and ClickHouse expose storage/ingestion evidence. Secrets must never be printed in configuration summaries.

# 18. ClickHouse Analytical Design

## 18.1 Database Organisation

The repository uses the `lakehouse` database for Kafka Engine tables, Materialized Views, realtime final tables, derived views, and supplementary OBT tables.

## 18.2 Kafka Engine Tables

Kafka Engine tables are ephemeral consumption interfaces rather than durable reporting targets. Analysts should never query them as historical stores. Their schemas must remain compatible with the registered Avro values.

## 18.3 Materialized Views

Materialized Views are the insert pipeline. They convert source fields, apply row-level validation, append Kafka virtual columns, and write to final tables. Detaching or dropping a view stops destination inserts even if the Kafka Engine table remains defined.

## 18.4 Transactional Tables

Transactional final tables preserve entity grain and validity fields. Derived views `latest_products` and `current_product_prices` normalise replay/update histories for reporting.

## 18.5 Behavioural Tables

`realtime_behavioral_events` stores one consumed event message with optional event-specific columns and Kafka metadata. It is optimised for time, event type, session, and offset scans.

## 18.6 Monitoring Tables

Monitoring uses the realtime tables; no unsupported dedicated object names are introduced. Saved Metabase questions can materialise query results at the application level without changing ClickHouse storage.

## 18.7 OBT Tables

`transactional_obt` and `behavioral_obt` are wide ReplacingMergeTree tables populated by supplementary Spark jobs. They remain useful for reconciliation and dimensional flattening, but the permanent direct Gold architecture is represented by Kafka Engine → MV → realtime tables.

## 18.8 MergeTree Design

MergeTree supports partition pruning, ordered data parts, and efficient aggregations. Sorting keys include the most common time/entity dimensions and Kafka coordinates.

## 18.9 ReplacingMergeTree Design

The OBT tables use `ReplacingMergeTree(silver_updated_at)` or `ReplacingMergeTree(gold_loaded_at)`. Replacement occurs during merges and is not an immediate uniqueness constraint; queries requiring fully collapsed results may need `FINAL` or version-aware aggregation depending on workload.

## 18.10 Partitioning

Time-based monthly partitions are used for high-volume streams. Behavioural OBT uses processing-date partitions, and transactional OBT uses order month. Excessively granular partitions are avoided.

## 18.11 Sorting

Order tables begin with event/order time and order identifiers. Behavioural data begins with event time/type/session. Product latest-state access uses offset-aware aggregation because the realtime product table preserves every source update.

## 18.12 Query Optimisation

Queries should filter date partitions early, use `is_valid=1` for business metrics, prefer derived latest-state views for mutable entities, avoid scanning nullable event-specific columns unnecessarily, and use pre-aggregated Metabase questions only when refresh semantics are clear.

## 18.13 Duplicate and Replay Handling

The direct realtime tables preserve replay evidence. For mutable master entities, `argMax` by Kafka offset resolves latest state. For append facts, controlled group resets and deduplication by `(topic, partition, offset)` are safer than assuming MergeTree uniqueness.

# 19. Reports and Analytics

## 19.1 Reporting Architecture

Metabase connects to ClickHouse. Business dashboards query valid analytical rows, while monitoring dashboards query recent windows and validity distributions. The Metabase application stores its own metadata in `postgres-metabase`.

## 19.2 Metabase Integration

The Compose stack includes a ClickHouse-enabled Metabase image and a PostgreSQL metadata database. Connection values must use runtime secrets and the internal ClickHouse service endpoint. Saved questions should document filters, grain, and refresh expectations.

## 19.3 Revenue Analysis

Revenue can be aggregated from valid order totals or the transactional OBT, grouped by date, category, product, payment method, location, or loyalty tier. Direct tables provide freshness; the OBT supports richer denormalised attributes where the supplementary load is used.

## 19.4 Loyalty-Tier Analysis

The supplied Metabase dashboard reference includes revenue, user count, total orders, engagement, and return-rate views by loyalty tier. These are permanent report concepts; the particular values visible at screenshot time are not part of this documentation.

[FIGURE 8 PLACEHOLDER]

Caption: Figure 8. Metabase Loyalty-Tier Dashboard Presenting Revenue, User Count, Orders, Engagement, and Return Analysis.

Recommended placement: Section 19.4, Loyalty-Tier Analysis.

## 19.5 User Engagement Analysis

Behavioural dimensions and facts support events per user, sessions per user, items or cart value per user, device mix, session duration, and event-category distribution. The dashboard’s engagement chart compares order and item-oriented user metrics by tier.

## 19.6 Return Analysis

The transactional OBT DDL contains `is_returned`, `return_reason`, `refund_amount`, and `return_timestamp`, enabling return rate and refund analysis. These columns are part of the supplementary OBT contract; direct realtime source SQL in this repository does not define a separate returns Kafka Engine table.

## 19.7 Funnel Analysis

The implemented funnel SQL uses ordered session sequences and reports both cumulative and stage-to-stage conversion. Date parameters can be supplied by Metabase.

## 19.8 Category Analysis

Category and product streams support latest product/category state, while the transactional Silver model exposes conformed category keys. Category revenue requires order-item/product/category association, available from the Silver/OBT model rather than a single direct entity table.

## 19.9 Purchase-to-Wishlist Analysis

Behavioural events contain `wishlist_name`, product, user/session, and purchase/order signals. A purchase-to-wishlist report is analytically supportable by joining or sequencing these events. The repository does not contain a saved SQL artefact with an authoritative report name, so this documentation does not invent one.

## 19.10 Monitoring Dashboards

Monitoring cards can display orders per minute, amount statistics, ingestion freshness, invalid records by error, event throughput, and funnel conversion. Threshold alerts can be added in Metabase or an external alerting system.

## 19.11 Business Value

The reporting layer turns technical events into commercial measures while retaining traceability. It enables product, marketing, operations, and data-quality stakeholders to use the same governed source definitions.

**Table 20. Metabase Report Mapping.**

| Report / query | Evidence | Measures | Grain |
| --- | --- | --- | --- |
| Loyalty-tier business dashboard | Metabase report reference | Revenue, user count, total orders, engagement, return rate | Loyalty tier |
| Orders per minute | sql/clickhouse/realtime_reports.sql | Valid order throughput | Minute |
| Order amount monitoring | sql/clickhouse/realtime_reports.sql | Average, minimum, maximum total | Minute |
| Session funnel | sql/clickhouse/realtime_reports.sql | Stage reach, conversion, drop-off | Session / funnel stage |
| Latest products | lakehouse.latest_products | Latest product attributes and validity | Product |
| Current product prices | lakehouse.current_product_prices | Current price and effective start | Product |
| Behavioural category/type verification | sql/clickhouse/behavioral_gold_verification.sql | Events, users, sessions, cart value | Event category/type |
| Device engagement verification | sql/clickhouse/behavioral_gold_verification.sql | Events, users, sessions, duration | Device |
| Attribution progression | sql/clickhouse/behavioral_gold_verification.sql | Browse/cart/checkout/purchase counts | UTM source |



# 20. Reliability, Recovery, and Observability

## 20.1 Reliability Strategy

Reliability is achieved through layered controls: Kafka retention, Spark checkpoints, durable Bronze Parquet, deterministic Silver keys, Iceberg atomic commits, Airflow retries, ClickHouse consumer groups, and explicit data-level verification. No single control is treated as sufficient.

## 20.2 Spark Checkpoints

Transactional and behavioural checkpoints are isolated by bucket/prefix. They record streaming progress and must be backed up and protected from accidental deletion.

## 20.3 Airflow Retries

Retries address transient failures such as temporary service unavailability. They do not fix deterministic schema or business-rule defects; repeated failures should trigger diagnosis rather than unlimited retry.

## 20.4 Iceberg Transactions

Iceberg commits table state atomically and supports MERGE-based reruns. Snapshot metadata provides a recoverable history subject to retention.

## 20.5 Idempotent Processing

Silver idempotency derives from deterministic keys, partition-scoped processing, unique-key assertions, and MERGE. Behavioural sessions are recomputed from full touched-session state. Direct ClickHouse ingestion is replay-aware but not claimed to be automatically exactly-once.

## 20.6 Kafka Consumer Groups

Each ClickHouse source uses a named consumer group. Group changes or resets affect replay boundaries. Spark Bronze uses its own checkpoint-managed Kafka source progress.

## 20.7 ClickHouse Recovery

After a ClickHouse restart, Kafka Engine consumers resume according to committed group offsets. Materialized Views must be attached and destination tables writable. If messages were consumed while the destination view was absent, recovery requires a planned replay.

## 20.8 Logging

Logs exist at container, Airflow task, Spark driver/executor, MinIO, Lakekeeper, and ClickHouse levels. Run IDs and processing dates should be included in application log messages to correlate failures.

## 20.9 Health Checks

Service health checks verify process readiness. Data health checks verify source lag, latest timestamps, row validity, duplicate keys, expected partitions, and table availability.

## 20.10 Data Freshness Monitoring

Freshness should compare source event timestamps, Kafka timestamps, Bronze ingestion times, Silver ingestion times, and ClickHouse `ingested_at`. Alert thresholds must be workload-specific and measured rather than guessed.

# 21. Configuration and Security

## 21.1 Environment Variables

Configuration is divided between `.env.example`, Compose environment blocks, DAG task environments, and job-specific variables. Values in this documentation are names or placeholders only.

**Table 21. Environment Variable Reference.**

| Variable | Component | Purpose | Classification |
| --- | --- | --- | --- |
| AIRFLOW_FERNET_KEY | Airflow | Encryption key for stored connection data | Secret |
| AIRFLOW_JWT_SECRET | Airflow | JWT signing secret | Secret |
| AIRFLOW_ADMIN_USER | Airflow | Initial administrator username | Sensitive |
| AIRFLOW_ADMIN_PASSWORD | Airflow | Initial administrator password | Secret |
| POSTGRES_USER | Airflow PostgreSQL | Database user | Sensitive |
| POSTGRES_PASSWORD | Airflow PostgreSQL | Database password | Secret |
| MINIO_ROOT_USER | MinIO | Bootstrap access key | Secret |
| MINIO_ROOT_PASSWORD | MinIO | Bootstrap secret key | Secret |
| CLICKHOUSE_USER | ClickHouse | Analytical database user | Sensitive |
| CLICKHOUSE_PASSWORD | ClickHouse | Analytical database password | Secret |
| METABASE_DB_USER | Metabase PostgreSQL | Metadata database user | Sensitive |
| METABASE_DB_PASSWORD | Metabase PostgreSQL | Metadata database password | Secret |
| ICEBERG_CATALOG_DB_USER | Iceberg catalogue DB | Database user | Sensitive |
| ICEBERG_CATALOG_DB_PASSWORD | Iceberg catalogue DB | Database password | Secret |
| KAFKA_BOOTSTRAP_SERVERS | Spark Bronze | Kafka bootstrap endpoint | Sensitive endpoint |
| SCHEMA_REGISTRY_URL | Spark / ClickHouse | Schema Registry endpoint | Sensitive endpoint |
| KAFKA_STARTING_OFFSETS | Spark Bronze | Starting offset policy | Configuration |
| MINIO_ENDPOINT | Spark / Airflow | Internal MinIO endpoint | Configuration |
| BEHAVIORAL_KAFKA_TOPIC | Behavioural Bronze | Topic override | Configuration |
| ICEBERG_REST_URI | Spark Silver | Lakekeeper REST endpoint | Sensitive endpoint |
| ICEBERG_WAREHOUSE | Spark Silver | Warehouse name | Configuration |
| CLICKHOUSE_HOST | Gold utilities | ClickHouse host | Configuration |
| CLICKHOUSE_HTTP_PORT | Gold utilities | ClickHouse HTTP port | Configuration |
| CLICKHOUSE_DB | Gold utilities | Target database | Configuration |



## 21.2 Configuration Sources

`.env.example` documents required secret names. `docker-compose.yml` assembles service configuration. Spark modules supply validated defaults. Airflow DAGs add process-date and job-specific values. ClickHouse SQL contains source settings and should be templated for deployments.

## 21.3 Service Endpoints

Internal service endpoints use Docker DNS names. External endpoints are routed or managed outside the stack. This document replaces exact private addresses with placeholders.

## 21.4 Credential Management

Secrets must be injected through protected environment files, an orchestrator secret store, or a dedicated secrets manager. `.env` must not be committed. Root or default accounts should be used only for bootstrap, then replaced by least-privilege service users.

## 21.5 Network Security

Only required ports should be exposed. Internal databases and Redis need not be published to the host. Traefik routes should use TLS, authentication, and restricted administrative access. Kafka and Schema Registry should use authenticated and encrypted connections where the external cluster supports them.

## 21.6 Sensitive Data Redaction

Passwords, tokens, private IP addresses, private hostnames, access keys, and secret keys are excluded. The behavioural Silver pipeline hashes IP addresses before analytical storage.

## 21.7 Production Hardening

Required hardening includes secret rotation, TLS, non-root service accounts where supported, image digest pinning, network policies/firewall rules, backups, object-store replication, catalogue database backups, ClickHouse user profiles, audit logging, dependency scanning, and parameterisation of broker/registry endpoints in SQL. A Gold DAG currently contains hard-coded example MinIO credentials in Spark command configuration; these must be removed and supplied through secrets before any production deployment.

# 22. Project Deployment and Execution

## 22.1 Prerequisites

A deployment host requires Docker Engine, Docker Compose, sufficient CPU/memory/storage, access to the external Kafka and Schema Registry services, the external Traefik network when routed interfaces are used, and DNS/TLS configuration for public routes.

## 22.2 Repository Setup

Extract or clone the repository into a controlled directory. Confirm that `docker-compose.yml`, `src/`, `workflow/dags/`, `configs/`, and `sql/clickhouse/` are present. The repository URL is intentionally redacted here.

## 22.3 Environment Configuration

Copy `.env.example` to `.env`, generate strong secrets, and set only deployment-appropriate values. Validate that no blank variables override secure defaults. Never use example credentials from source code.

## 22.4 Infrastructure Startup

Create the external proxy network when required, then build and start the stack:

```bash
docker network create traefik_traefik_network 2>/dev/null || true
docker compose up -d --build
```

## 22.5 Service Verification

Use `docker compose ps`, service health endpoints, Airflow component checks, Spark UIs, MinIO health, Lakekeeper health, and `SELECT 1` against ClickHouse. Verify network access to `[KAFKA_BROKER_ENDPOINT]` and `[SCHEMA_REGISTRY_ENDPOINT]` separately because they are external.

## 22.6 MinIO Initialisation

The `createbuckets` service provisions general buckets. Ensure the final required buckets `bronze`, `silver`, `tr-checkpoints`, and `be-checkpoints` exist before starting the relevant jobs. Bucket policies must remain private.

## 22.7 Lakekeeper Initialisation

Allow migration, bootstrap, and warehouse-initialisation services to complete. Verify that warehouse `silver` is registered against the MinIO Silver bucket and that the four namespaces can be created by Spark.

## 22.8 ClickHouse Initialisation

Create the `lakehouse` database, then execute the direct-stream SQL files in dependency order: final table, Kafka Engine table, Materialized View, followed by reporting views and queries. Replace private broker and registry endpoints with runtime-specific values before execution.

## 22.9 Bronze Pipeline Execution

The root Dockerfile defines a standalone transactional Bronze command. In the Compose cluster, jobs can be submitted from the Spark master with the repository paths and required packages. A representative pattern is:

```bash
docker compose exec spark-master spark-submit   --master spark://spark-master:7077   --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262   /opt/spark-apps/jobs/bronze_transactional_job.py
```

Submit `bronze_behavioral_job.py` with its required Kafka/Avro packages and registry environment. Confirm objects and checkpoints rather than relying solely on a running process.

## 22.10 Silver Pipeline Execution

Enable or manually trigger `silver_transactional_pipeline` and `silver_behavioral_etl_v2` for a date that has Bronze partitions. Use DAG-run configuration for supported date overrides. Observe Spark and Airflow logs and validate Iceberg table snapshots after completion.

## 22.11 Gold Stream Verification

After applying the direct SQL, verify Kafka Engine settings through `system.tables`, confirm Materialized Views are attached, and query the destination tables for new `ingested_at` values and Kafka offsets. Do not query Kafka Engine tables directly for durable counts.

## 22.12 Bonus Stream Verification

Run orders-per-minute, amount, invalid-record, and funnel queries against a bounded recent interval. Verify that their latest source timestamps advance while Kafka produces messages.

## 22.13 Metabase Access

Open `[METABASE_PUBLIC_HOST]`, create a ClickHouse connection using a least-privilege analytical user, sync the `lakehouse` schema, and build or import the required questions and dashboard cards.

## 22.14 Platform Shutdown

Stop services without deleting volumes:

```bash
docker compose down
```

Use `docker compose down -v` only for an intentional destructive reset after backups and explicit approval.

# 23. End-to-End Validation

## 23.1 Kafka Validation

Confirm each topic exists, has readable partitions, produces messages, and exposes the expected subject. Validate connectivity from both Spark and ClickHouse containers.

## 23.2 Bronze Validation

For each stream, verify a running query, progressing offsets, valid Parquet files, expected schema/lineage columns, and partition paths. Sample both valid and invalid behavioural rows.

## 23.3 MinIO Validation

Verify required buckets, private access, persistent volume attachment, object timestamps, and expected path structure. Do not use current object counts as permanent acceptance criteria.

## 23.4 Checkpoint Validation

Confirm each query has its own checkpoint prefix and that commit/offset files advance. Restart a job and verify it resumes rather than beginning a blind duplicate replay.

## 23.5 Iceberg Validation

List namespaces/tables through the catalogue, inspect current snapshots, verify data and metadata objects in MinIO, and query tables through Spark SQL.

## 23.6 Silver Model Validation

Check table grain, non-null keys, key uniqueness, dimension/fact relationships, valid SCD intervals, and deterministic rerun results. Compare fact-order-item parents with fact orders.

## 23.7 Data-Quality Validation

Inject or identify controlled invalid records, confirm correct severity, verify issue/quarantine persistence, and ensure rejected records do not enter facts while warnings remain processable.

## 23.8 ClickHouse Validation

Check final table engines, partitions, sorting keys, Materialized View destinations, consumer groups, latest offsets, validity distributions, and query plans for time-filtered reports.

## 23.9 Gold Validation

Confirm that records move directly from Kafka into realtime ClickHouse tables without a Silver dependency. Validate row-level transformations and metadata against source messages.

## 23.10 Bonus Monitoring Validation

Confirm monitoring time series advances, invalid counts react to controlled bad messages, and funnel results use completed session windows to limit premature drop-off classification.

## 23.11 Metabase Validation

Verify ClickHouse connection, field types, filters, dashboard permissions, refresh behaviour, and consistency between card results and equivalent ClickHouse SQL.

**Table 24. End-to-End Validation Matrix.**

| Stage | Validation objective | Method | Acceptance evidence |
| --- | --- | --- | --- |
| Kafka source | Topics and subjects readable | Topic metadata + sample consume | Messages and schemas match contract |
| Transactional Bronze | Six streams write Parquet | MinIO path and Spark progress | Each table has current partition and checkpoint |
| Behavioural Bronze | Decode and retain validity metadata | Sample Parquet rows | Valid/invalid evidence present |
| Checkpoints | Progress survives restart | Restart controlled job | No unplanned replay from earliest |
| Transactional Silver | Dimensions/facts/quality written | Spark SQL and Iceberg metadata | Keys unique; orphan items excluded |
| Behavioural Silver | Facts/dimensions/quarantine/state written | Spark SQL and state table | Rejected excluded; warnings retained |
| Iceberg | Atomic snapshots visible | Catalogue and metadata inspection | Current snapshot readable |
| ClickHouse direct | Kafka Engine and MV populate targets | system tables + target queries | Offsets and ingested_at advance |
| Gold reports | Business aggregates reconcile | Independent SQL comparison | Valid-row totals agree |
| Bonus monitoring | Recent-window metrics advance | Monitoring queries | Fresh timestamps and sensible rates |
| Metabase | Dashboard cards match SQL | Card versus ClickHouse query | Same filters and values |



# 24. Troubleshooting

## 24.1 Kafka Connectivity

Verify DNS/routing from the container, broker listener addresses, firewall rules, authentication, topic permissions, and partition availability. A host-accessible broker address may not be container-reachable if Kafka advertises a different listener.

## 24.2 Schema Registry Errors

Check subject spelling (`<topic>-value`), schema ID availability, registry reachability, Avro compatibility, and Confluent wire framing. A valid registry response does not prove that every producer message uses the latest schema ID.

## 24.3 Spark Streaming Errors

Inspect driver/executor logs, package-version conflicts, Kafka source options, memory, and malformed schema conversions. Keep Spark, Scala, Iceberg, and connector versions compatible. The repository’s Spark runtime is 3.5.3 while the Python requirements pin PySpark 3.5.1; rebuilds should align these.

## 24.4 MinIO Errors

Verify endpoint, path-style access, credentials, bucket existence, DNS, and volume capacity. Distinguish S3A configuration used by Parquet from S3FileIO configuration used by Iceberg.

## 24.5 Checkpoint Errors

Do not reuse one checkpoint for a different query or schema. For corrupt state, preserve a copy, identify the committed source range, and plan a controlled replacement checkpoint plus destination deduplication.

## 24.6 Iceberg Errors

Check catalogue package versions, REST URI, warehouse registration, namespace existence, S3 credentials, table schema, merge-key uniqueness, and snapshot commit conflicts. Multiple Iceberg runtime versions on the classpath can cause serialisation/classloading errors.

## 24.7 Lakekeeper Errors

Confirm its PostgreSQL database is healthy, migrations completed, bootstrap succeeded, warehouse registration exists, and the Spark catalogue URI includes the expected catalogue path.

## 24.8 Airflow DAG Errors

Check DAG parse logs, provider packages, Spark connection `spark_default`, mounted paths, templated dates, environment variables, worker-to-Spark connectivity, and the behavioural partition pre-check.

## 24.9 ClickHouse Kafka Engine Errors

Inspect `system.errors`, server logs, broker/registry reachability, topic/group settings, Avro field/type compatibility, and consumer exceptions. Do not run long analytical selects directly against Kafka Engine tables.

## 24.10 Materialized View Errors

Verify the view is attached, its destination table exists with matching types, and transformations accept nullable source fields. Insert a controlled source message and inspect the destination validity columns.

## 24.11 Metabase Errors

Check ClickHouse driver availability, database credentials, network route, schema sync, timezone configuration, parameter syntax, and user permissions. Compare failing cards with the raw SQL in ClickHouse.

# 25. Challenges and Design Decisions

## 25.1 Multiple Data Domains

Transactional entities and behavioural events have different grain and quality semantics. The solution keeps their Bronze, checkpoints, Silver modules, namespaces, and DAGs independent.

## 25.2 Streaming Ingestion

Continuous ingestion was required without embedding cross-table logic in the hot path. Spark Structured Streaming therefore performs only schema decoding, standardisation, lineage, and partitioned writes.

## 25.3 Lakehouse Storage

Parquet was retained as the physical format, while Iceberg was added for table-level transactions, metadata, MERGE, and evolution.

## 25.4 Schema Management

Schema Registry is authoritative for Avro. Local transactional Spark schemas support deterministic parsing; behavioural local metadata deliberately avoids copying the Avro schema.

## 25.5 Data Quality

The design favours evidence preservation: Bronze keeps invalid records, Silver records issues and quarantine, and ClickHouse stores invalid rows with flags.

## 25.6 SCD Type 2

Product prices require effective intervals. The pipeline derives boundaries with windows, creates stable price keys, and resolves facts by order timestamp.

## 25.7 Direct Kafka-to-ClickHouse Design

Direct consumption reduces serving latency and decouples BI freshness from daily Silver completion. The trade-off is that ClickHouse transformations are row-local and replay must be managed explicitly.

## 25.8 Near-Real-Time Monitoring

Monitoring reuses the direct realtime tables instead of creating a second duplicated consumer topology. Logical separation is achieved through queries and dashboards.

## 25.9 Idempotency

Silver uses deterministic identities and Iceberg MERGE. ClickHouse direct tables preserve Kafka coordinates and use latest-offset/replacement techniques where necessary rather than claiming universal automatic deduplication.

## 25.10 Performance Optimisation

Date partitioning, selective sorting keys, Snappy/Zstandard compression, bounded Spark shuffle settings, batch coalescing, and ClickHouse columnar engines reduce scan and storage cost. No invented benchmark numbers are used.

## 25.11 Lessons Learned

Architecture documentation must distinguish authoritative final paths from legacy or supplementary code. Credentials must never be hard-coded into DAG commands or SQL. Version alignment across Spark and Iceberg is operationally important. Monitoring and governed modelling solve different needs and should coexist.

**Table 23. Challenge and Design-Decision Matrix.**

| Challenge | Final decision | Consequence |
| --- | --- | --- |
| Heterogeneous domains | Separate transactional and behavioural pipelines | Domain-specific schemas, rules, checkpoints, and recovery |
| Schema-controlled streaming | Schema Registry plus explicit Spark/ClickHouse contracts | Decode failures retained and diagnosable |
| Replayable storage | Immutable Bronze Parquet in MinIO | Silver can be rebuilt without re-reading live Kafka |
| Transactional Silver updates | Iceberg format v2 and MERGE | Atomic, idempotent table changes |
| Price history | SCD Type 2 dimension | Correct price context at order time |
| Behavioural identity | Event ID with Kafka-coordinate fallback | Deterministic deduplication and lineage |
| Low-latency analytics | Direct Kafka Engine and Materialized Views | Fresh ClickHouse data independent of Silver schedule |
| Monitoring cost | Reuse direct realtime tables | No second duplicate consumer/storage plane |
| Invalid data | Quality tables and quarantine | No silent loss |
| Secrets and endpoints | Runtime configuration and redaction | Avoid credential exposure; hard-coded examples require remediation |



# 26. Final Project Outcomes

## 26.1 Completed Platform Capabilities

The project delivers continuous transactional and behavioural Bronze ingestion, separate recoverable checkpoints, MinIO object storage, Airflow-orchestrated Silver processing, transactional and behavioural Iceberg models, quality/quarantine outputs, Lakekeeper catalogue integration, direct ClickHouse consumers and transformations, realtime analytical tables, monitoring SQL, and Metabase reporting support.

## 26.2 Technical Outcomes

The solution demonstrates hybrid streaming/batch architecture, schema-aware Avro handling, lineage propagation, Kimball modelling, SCD Type 2, deterministic keys, Iceberg MERGE, direct ClickHouse Kafka consumption, Materialized View transformations, Dockerised infrastructure, and operational validation.

## 26.3 Analytical Outcomes

Available analytical concepts include order throughput and values, user and loyalty-tier measures, product/category state, price history, behavioural event categories, device/session engagement, attribution, return fields in the OBT contract, and ordered funnel conversion.

## 26.4 Business Value

The platform replaces disconnected raw streams with trusted, queryable datasets. It shortens time from event production to insight, supports reproducible modelling, and makes data-quality problems visible rather than hidden.

## 26.5 Known Limitations

Known limitations include the absence of Kubernetes manifests; no performance benchmark or production SLO evidence; direct MergeTree tables that require explicit replay/deduplication procedures; hard-coded deployment examples in some SQL/DAG configuration that must be parameterised; a Spark/PySpark version difference between runtime image and root requirements; no separate returns Kafka Engine DDL; no dedicated duplicated Bonus table set; and saved Metabase artefacts not fully represented as repository files. These constraints do not invalidate the implemented platform but define the boundary of supported claims.

## 26.6 Future Enhancements

Recommended enhancements are secret-manager integration, TLS and authenticated Kafka/Registry connections, removal of hard-coded credentials/endpoints, automated ClickHouse DDL deployment, consumer-lag alerting, formal data-contract compatibility tests, Iceberg snapshot maintenance, object-store replication, lineage catalogue integration, automated Metabase export/import, cross-domain semantic models, statistical anomaly detection, CI/CD, resource autoscaling, and Kubernetes deployment only after manifests and operational ownership exist.

# 27. Conclusion

The Modern Data Analytics Platform implements a complete end-to-end data engineering solution with distinct responsibilities for source streaming, durable landing, governed lakehouse modelling, low-latency serving, and monitoring. Its most important architectural characteristic is the coexistence of a high-integrity Silver lakehouse and a direct Kafka-to-ClickHouse Gold/Bonus path. This design preserves replayability and dimensional correctness without sacrificing analytical freshness. The repository provides concrete implementation evidence for the topics, paths, tables, DAGs, validations, and reporting contracts described in this document, while deployment-specific secrets and temporary runtime observations remain outside the permanent specification.

# Appendices

## Appendix A — Repository Structure

```text
modern_data_analytics_platform/
├── .env.example
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── README.md
├── configs/
│   ├── airflow/Dockerfile
│   ├── spark/
│   └── initial_catalog_database_creator.sql
├── scripts/ops/
├── sql/
│   ├── clickhouse/
│   ├── iceberg/
│   └── metabase/
├── src/
│   ├── common/
│   ├── jobs/
│   ├── schema/
│   ├── schemas/
│   └── transformations/
└── workflow/
    ├── dags/
    ├── tasks/
    └── utils/
```

Core implementation evidence includes `src/jobs/bronze_transactional_job.py`, `src/jobs/bronze_behavioral_job.py`, `src/jobs/silver_transactional_job.py`, `src/jobs/silver_behavioral_job.py`, `workflow/dags/*.py`, and `sql/clickhouse/realtime_*.sql`.

## Appendix B — Docker Service Reference

See Table 3. The principal operational commands are `docker compose up -d --build`, `docker compose ps`, `docker compose logs <service>`, and `docker compose down`.

## Appendix C — Kafka Topic Mapping

See Tables 4, 5, and 17. Topic names are exact; broker and Registry endpoints are deployment placeholders.

## Appendix D — Transactional Data Dictionary

See Table 6. Detailed source types are defined in `src/schemas/bronze_transactional_schemas.py`; Silver-enriched fields are defined in the transactional validation, cleaning, and Kimball modules.

## Appendix E — Behavioural Event Dictionary

See Table 7. The Schema Registry subject is the source of truth for the complete Avro schema; local constants list required and optional analytical columns.

## Appendix F — MinIO Bucket and Path Reference

- `s3a://bronze/transactional/<table>/<yyyyMMdd>`
- `s3a://bronze/behavioral/events/year=<YYYY>/month=<M>/day=<D>`
- `s3a://tr-checkpoints/transactional/<table>`
- `s3a://be-checkpoints/behavioral/events`
- Iceberg warehouse: bucket `silver`, catalogue-managed object layout

## Appendix G — Apache Iceberg Table Reference

Transactional namespace: `dim_date`, `dim_user`, `dim_category`, `dim_product`, `dim_product_price_scd`, `fact_order`, `fact_order_item`. Transactional quality namespace: `transactional_validation_issues`. Behavioural namespace: `dim_behavioral_device`, `dim_behavioral_event_type`, `dim_behavioral_session`, `fact_behavioral_events`, `behavioral_events_quarantine`, `behavioral_pipeline_state`. Behavioural quality namespace: `behavioral_validation_issues`.

## Appendix H — Apache Airflow DAG Reference

See Table 16. Silver DAGs are the authoritative scheduled lakehouse workflows. Gold-related DAGs are supplementary OBT loaders and do not replace the direct ClickHouse stream.

## Appendix I — ClickHouse Kafka Engine Reference

See Table 17. Every Engine uses `AvroConfluent`, one configured consumer, a dedicated consumer group, and runtime broker/Registry endpoints.

## Appendix J — ClickHouse Materialized View Reference

See Table 18. Materialized Views are the executable direct-stream transformations and must remain attached for destination ingestion.

## Appendix K — ClickHouse Final Table Reference

See Table 19. Reporting should query final tables and views, never Kafka Engine tables as durable storage.

## Appendix L — Environment Variable Reference

See Table 21. All secret values must be injected at runtime. The documentation intentionally contains no credential values.

## Appendix M — Service Port Reference

**Table 22. Service Port Reference.**

| Service | Host / route | Container port | Purpose |
| --- | --- | --- | --- |
| Spark master UI | 8081 | 8080 | Host to Spark master web UI |
| Spark master RPC | 7077 | 7077 | Spark-submit and worker registration |
| Spark worker UI | 8082 | 8081 | Host to Spark worker web UI |
| Iceberg REST | 8181 | 8181 | Auxiliary REST catalogue |
| Lakekeeper | 8182 | 8181 | Lakekeeper REST service |
| ClickHouse HTTP | 8123 | 8123 | HTTP API and clients |
| ClickHouse native | 9004 | 9000 | Native protocol |
| Airflow | Routed | Internal service port through Traefik | [AIRFLOW_PUBLIC_HOST] |
| MinIO console/API | Routed | Internal service ports through Traefik | [MINIO_PUBLIC_HOST] |
| Metabase | Routed | Internal service port through Traefik | [METABASE_PUBLIC_HOST] |



## Appendix N — Execution Command Reference

```bash
# Start infrastructure
docker compose up -d --build

# Inspect state
docker compose ps
docker compose logs --tail=200 airflow-scheduler
docker compose logs --tail=200 spark-master
docker compose logs --tail=200 clickhouse

# Open a ClickHouse client
docker compose exec clickhouse clickhouse-client

# Apply one SQL file after replacing endpoint placeholders
docker compose exec -T clickhouse clickhouse-client --multiquery < sql/clickhouse/realtime_orders.sql

# Stop without deleting data
docker compose down
```

For Spark jobs, use the exact mounted application paths and package versions from the Airflow DAGs. For manual Airflow runs, pass the supported `process_date` or `execution_date` field through DAG-run configuration. Commands must be executed only after environment variables and external network access are verified.
