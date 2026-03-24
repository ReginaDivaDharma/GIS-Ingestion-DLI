# 🗺️ GIS Data Ingestion on Huawei Cloud DLI

> Ingesting and processing GIS (Geographic Information System) data stored in OBS (Object Storage Service) using Huawei Cloud Data Lake Insight (DLI) with Apache Spark and Apache Sedona.

---

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Technology Stack](#technology-stack)
- [Prerequisites](#prerequisites)
- [Project Structure](#project-structure)
- [Setup & Build](#setup--build)
- [DLI Job Configuration](#dli-job-configuration)
- [Known Limitations](#known-limitations)
- [Troubleshooting](#troubleshooting)
- [Pending](#pending)

---

## Overview

This project demonstrates how to ingest GIS data (stored as Parquet files with binary geometry columns) from Huawei OBS into Huawei DLI for processing. The geometry column is converted from binary to hex string format for compatibility with Spark's data processing pipeline.

### What This Does
- ✅ Reads GIS Parquet files from OBS
- ✅ Processes geometry columns (binary → hex string conversion)
- ✅ Saves processed data back to OBS as Parquet
- ⏳ Save to DLI Hive table (pending Huawei metastore URI — see [Pending](#pending))

---

## Architecture

```
OBS Bucket (Raw GIS Data)
        │
        ▼
DLI Spark Job (JAR)
  ├── Read Parquet from OBS
  ├── Convert binary geometry → hex string
  └── Write processed Parquet back to OBS
        │
        ▼
OBS Bucket (Processed GIS Data)
```

---

## Technology Stack

| Component | Version | Reason |
|---|---|---|
| **DLI Spark** | 3.3.1 | Huawei Cloud DLI's Spark engine version |
| **Java** | 8 (JRE 8) | DLI runtime only supports up to Java 8 (`class file version 52.0`) |
| **Scala** | 2.12.15 | Compatible with Spark 3.3.1 |
| **Apache Sedona** | 1.5.1 | Latest version supporting Spark 3.3 + Java 8 (Sedona 1.8.0+ dropped both) |
| **GeoTools** | 1.5.1-28.2 | Sedona dependency for geometry processing (must match Sedona version) |
| **sbt** | 1.12.5 | Scala build tool to compile code into JAR |

### Why Scala JAR Instead of Python Script?

DLI's Python runtime is **Python 3.7**, but Apache Sedona requires **Python 3.8+**. This causes an `ImportError` when using PySpark. The solution is to use a **Scala JAR** which runs on the JVM and bypasses the Python version limitation entirely.

```
Python approach (failed):
  DLI Python runtime = 3.7
  Sedona requires    = 3.8+
  Result             = ImportError ❌

Scala JAR approach (working):
  Runs on JVM (Java 8)
  No Python dependency
  Result             = Works ✅
```

### Why Sedona 1.5.1 Instead of Latest?

Sedona 1.8.0 dropped support for Spark 3.3 and Java 8. DLI's Spark version is 3.3.1 and its JRE is Java 8. Therefore, Sedona 1.5.1 is the latest compatible version.

| Sedona Version | Spark 3.3 Support | Java 8 Support |
|---|---|---|
| 1.8.x | ❌ | ❌ |
| 1.7.x | ✅ | ✅ |
| **1.5.1** | ✅ | ✅ |

---

## Prerequisites

1. Huawei Cloud account with DLI and OBS services enabled
2. GIS data uploaded to OBS bucket as Parquet files
3. DLI agency with access to OBS bucket
4. Local machine with:
   - Java 17 (for building — compile target is Java 8)
   - sbt 1.12.5+
5. The following JARs downloaded and uploaded to OBS:
   - `sedona-spark-shaded-3.0_2.12-1.5.1.jar` — [Download from Maven](https://mvnrepository.com/artifact/org.apache.sedona/sedona-spark-shaded-3.0_2.12/1.5.1)
   - `geotools-wrapper-1.5.1-28.2.jar` — [Download from Maven](https://mvnrepository.com/artifact/org.datasyslab/geotools-wrapper/1.5.1-28.2)

---

## Project Structure

```
gis-ingestion/
├── build.sbt                          # sbt build configuration
├── project/
│   └── plugins.sbt                    # sbt assembly plugin
└── src/
    └── main/
        └── scala/
            └── GISIngestion.scala     # Main Spark job
```

---

## Setup & Build

### Step 1 — Clone and configure

Update the OBS paths in `GISIngestion.scala` to match your bucket:

```scala
// Input path
val df = config.read.parquet("obs://YOUR-BUCKET/YOUR-PATH/YOUR-FILE")

// Output path
dfConverted.write
  .mode("overwrite")
  .parquet("obs://YOUR-BUCKET/YOUR-PATH/YOUR-FILE-processed")
```

### Step 2 — Set Java 17 for building

```powershell
# Windows PowerShell
$env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-17.x.x.x-hotspot"
$env:Path = "$env:JAVA_HOME\bin;" + $env:Path
java -version  # Should show 17.x.x
```

### Step 3 — Build the JAR

```powershell
cd gis-ingestion
sbt assembly
```

Output JAR location:
```
target/scala-2.12/gis-ingestion-assembly-1.0.jar
```

### Step 4 — Upload to OBS

Upload the JAR to your OBS bucket:
```
obs://YOUR-BUCKET/jobs/jar/gis-ingestion-assembly-1.0.jar
```

---

## Source Code

### `build.sbt`

```scala
name := "gis-ingestion"
version := "1.0"
scalaVersion := "2.12.15"

// Target Java 8 for DLI compatibility
javacOptions ++= Seq("-source", "1.8", "-target", "1.8")
scalacOptions ++= Seq("-target:jvm-1.8")

resolvers ++= Seq(
  "UCAR Unidata" at "https://artifacts.unidata.ucar.edu/repository/unidata-all/",
  "Maven Central" at "https://repo1.maven.org/maven2/"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql"   % "3.3.1" % "provided",
  "org.apache.spark" %% "spark-hive"  % "3.3.1" % "provided",
  "org.apache.sedona" %% "sedona-spark-shaded-3.0" % "1.5.1"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
```

### `project/plugins.sbt`

```scala
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "2.1.1")
```

### `GISIngestion.scala`

```scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object GISIngestion {
  def main(args: Array[String]): Unit = {

    val config = SparkSession.builder()
      .appName("GIS Sedona Ingestion")
      .getOrCreate()

    println("Step 1: Reading GIS parquet from OBS...")
    val df = config.read.parquet("obs://denodo/GIS_data/master_area_adm")
    println("Step 1 complete!")

    println("Step 2: Converting binary geometry to hex string...")
    val dfConverted = df.withColumn(
      "polygon_suburb",
      hex(col("polygon_suburb"))
    )
    println("Step 2 complete!")

    println("Step 3: Saving back to OBS as processed parquet...")
    dfConverted.write
      .mode("overwrite")
      .parquet("obs://denodo/GIS_data/master_area_adm_processed")

    println("Done! GIS data successfully processed by DLI!")
    config.stop()
  }
}
```

---

## DLI Job Configuration

| Field | Value |
|---|---|
| **Job Name** | `gis_ingestion` |
| **Application** | `obs://YOUR-BUCKET/jobs/jar/gis-ingestion-assembly-1.0.jar` |
| **Main Class** | `GISIngestion` |
| **Agency** | Your DLI agency with OBS access |
| **Job Type** | Basic |
| **JAR Dependencies** | `obs://YOUR-BUCKET/jobs/sedona/sedona-spark-shaded-3.0_2.12-1.5.1.jar` |
| | `obs://YOUR-BUCKET/jobs/geotools-wrapper/geotools-wrapper-1.5.1-28.2.jar` |
| **Python Dependencies** | None |

### Spark Arguments (`--conf`)

```
spark.kubernetes.submission.waitAppCompletion=true
spark.driver.memory=1g
spark.executor.memory=1g
spark.executor.instances=1
```

---

## Known Limitations

### 1. Cannot Save Directly to DLI Hive Table
Spark JAR jobs in DLI cannot connect to DLI's Hive metastore directly. The job attempts to use a local Derby database which fails with:
```
Accessing DLI metadata from a Spark JAR job is a restricted feature that requires explicit activation by Huawei, so you need to submit a ticket to whitelist it first
```
**Workaround:** Save processed data back to OBS as Parquet instead.

### 2. Geometry Column Stored as Binary
The `polygon_suburb` geometry column is stored as `binary` type in the Parquet file. DLI's Hive tables do not support binary types directly, so the column is converted to a hex string representation for storage compatibility.

### 3. Python Runtime Version
DLI's Python runtime is 3.7. Apache Sedona 1.5.1+ requires Python 3.8+. Therefore PySpark cannot be used with Sedona in DLI — Scala JAR is required.

---

## Troubleshooting

| Error | Cause | Fix |
|---|---|---|
| `ImportError: cannot import name 'Literal'` | DLI Python 3.7 too old for Sedona | Use Scala JAR instead of PySpark |
| `UnsupportedClassVersionError: class file version 55.0` | JAR compiled for Java 11, DLI needs Java 8 | Add `-target:jvm-1.8` to `build.sbt` |
| `Table/View 'DBS' does not exist` | DLI JAR job cannot access Hive metastore | Save to OBS instead of DLI table |
| `Error downloading edu.ucar:cdm-core` | Missing UCAR repository | Add UCAR resolver to `build.sbt` |
| `sbt: command not found` | sbt not installed or not in PATH | Restart terminal after installing sbt |

---

## Pending

### Save to DLI Hive Table
Once Huawei Cloud Support provides the correct Hive metastore URI for DLI JAR jobs, the code will be updated to save directly to a DLI table:

```scala
// Pending Huawei support ticket response
val config = SparkSession.builder()
  .appName("GIS Sedona Ingestion")
  .config("hive.metastore.uris", "thrift://HUAWEI_PROVIDED_URI")
  .config("spark.sql.warehouse.dir", "luxorfs://hacluster/spark/managedDb")
  .enableHiveSupport()
  .getOrCreate()

// Save to DLI table
dfConverted.write
  .mode("overwrite")
  .insertInto("poc.master_area_adm")
```

The DLI table schema (pre-created via DLI SQL Editor):
```sql
CREATE TABLE IF NOT EXISTS poc.master_area_adm (
  suburb STRING,
  district STRING,
  city STRING,
  province STRING,
  polygon_suburb STRING,
  taxi_operation_city BOOLEAN
)
STORED AS PARQUET;
```

---

## Data Schema

The input GIS Parquet file contains the following columns:

| Column | Type | Description |
|---|---|---|
| `suburb` | STRING | Suburb name |
| `district` | STRING | District name |
| `city` | STRING | City name |
| `province` | STRING | Province name |
| `polygon_suburb` | BINARY | Geometry polygon data (WKB format) |
| `taxi_operation_city` | BOOLEAN | Whether taxi operates in this city |

**Total rows:** 77,474

---

*Last updated: March 2026*
