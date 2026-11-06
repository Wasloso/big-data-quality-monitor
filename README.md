# Big Data Quality Monitor

Project for the **Big Data in Industry 4.0** university course, developed by a three-person team.

We want to monitor product quality across simulated production lines. A vision model will check product images, Kafka will carry inspection results, and Spark will calculate quality statistics. We plan to save inspection history in Parquet and display results in a Streamlit dashboard.

## Current stage

The first step is a working local Spark environment. The script in `scripts/check_spark.py` groups four made-up inspection results by production line and checks the counts. This is a batch smoke check; streaming, Kafka, and image inference are planned next.

## Run the Spark check

Requirements: Docker Engine or Docker Desktop with Linux containers, and Docker Compose v2. On Windows, run the commands from WSL with Docker integration enabled.

From the repository root:

```bash
docker compose run --rm spark-check
```

The first run downloads the image and may take a while. Spark and its Python/Java runtime run inside the container, so a host Java installation or Python virtual environment is not needed for this step. No ports are published and no datasets are downloaded.

After the Spark startup logs, the script should print:

```text
Line A: 3 inspections, 1 rejects
Line B: 1 inspections, 0 rejects
Local Spark check passed.
```

A different result raises an error and returns a nonzero exit code. The script also prints the Python and Spark versions. This single smoke check is enough for the setup stage; there is no separate test suite yet.

## Next steps

- Define the inspection event format and a repeatable two-line scenario.
- Publish inspection events to a local Kafka broker.
- Use Spark Structured Streaming to process events and save history.
- Add visual anomaly detection with MVTec AD images and PatchCore.
- Build the dashboard and measure correctness, throughput, and delays.

Checked on 6 November 2026 using Docker Engine 29.2.1 on WSL2 (Linux x86-64). The container reported Spark 4.0.2, Python 3.10.12, and Java 17.0.17.

The starting image is pinned to `apache/spark:4.0.2`. Kafka connector and model dependencies will be pinned when those components are added.

## References

- [Apache Spark Docker images](https://github.com/apache/spark-docker)
- [Spark 4.0.2 documentation](https://spark.apache.org/docs/4.0.2/)
- [MVTec AD dataset](https://www.mvtec.com/research-teaching/datasets/mvtec-ad)
