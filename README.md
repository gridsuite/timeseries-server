# Timeseries Server

[![Actions Status](https://github.com/gridsuite/Timeseries-server/actions/workflows/build.yml/badge.svg?branch=main)](https://github.com/gridsuite/Timeseries-server/actions)
[![Coverage Status](https://sonarcloud.io/api/project_badges/measure?project=org.gridsuite%3Atimeseries-server&metric=coverage)](https://sonarcloud.io/component_measures?id=org.gridsuite%3Atimeseries-server&metric=coverage)
[![MPL-2.0 License](https://img.shields.io/badge/license-MPL_2.0-blue.svg)](https://www.mozilla.org/en-US/MPL/2.0/)

## Description

The **timeseries-server** is a microservice of the [GridSuite](https://github.com/gridsuite) platform dedicated to **storing and serving powsybl time series** (`com.powsybl.timeseries.TimeSeries`) produced by other computations.

It acts as a generic, computation-agnostic storage service: producers (e.g. the [dynamic-simulation-server](https://github.com/gridsuite/dynamic-simulation-server)) submit a batch of time series and get back a **group id**; consumers later fetch back the metadata or the data of that group, optionally filtered by name or by time window.

It provides the following capabilities:

- **Create a time series group** from a JSON payload containing a list of `TimeSeries` (all series in a group must share the same index/time reference).
- **List all time series groups** (ids) stored in the database.
- **Get the metadata** of a group (index type, individual series metadata) without fetching the data itself.
- **Get the data** of a group, with optional filters: a subset of series names, a time window, and an optional value compression (`tryToCompress`).
- **Delete a time series group** (metadata and data).

---

## Technical Stack

- Spring Boot (Web, Data JPA, Actuator)
- PostgreSQL
- Liquibase
- API documentation: OpenAPI / Swagger (`springdoc`)
- Micrometer / Prometheus
- [powsybl-time-series-api](https://github.com/powsybl/powsybl-core) (`TimeSeries` model, JSON (de)serialization)

---

## Development Scripts

Build Docker image

```shell
mvn install -DskipTests -Dpowsybl.docker.install
```

Please read [liquibase usage](https://github.com/powsybl/powsybl-parent/#liquibase-usage) for instructions to automatically generate changesets. After you generated a changeset do not forget to add it to git and in `src/main/resources/db/changelog/db.changelog-master.yaml`.


---

## Domain Model

| Concept | Description |
|---|---|
| **Time series group** | A set of time series sharing the same index (time reference), created and fetched together, identified by a UUID. |
| **Index** | The common time reference of a group (e.g. regular time steps), stored as JSON and shared by all series of the group. |
| **Individual metadata** | Per-series metadata (name, data type, etc.), stored as JSON alongside the group. |
| **Data** | The actual time-indexed values of each series in the group, stored separately from the metadata for querying efficiency. |

---

## Useful Links

You can find more information about the `TimeSeries` model in [powsybl-core](https://github.com/powsybl/powsybl-core).
