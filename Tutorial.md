# 🗺️ GIS Data Ingestion on Huawei Cloud DLI

> A step-by-step guide to ingesting and processing GIS data from OBS using Huawei Cloud Data Lake Insight (DLI) with Apache Spark and Apache Sedona.

---

## 📋 Table of Contents
- [Overview](#overview)
- [Why Scala JAR?](#why-scala-jar)
- [Technology Stack](#technology-stack)
- [Prerequisites](#prerequisites)
- [Project Structure](#project-structure)
- [Setup & Build](#setup--build)
- [GIS Format Guides](#gis-format-guides)
  - [GIS Parquet](#1--gis-parquet-simplest)
  - [GeoJSON](#2--geojson)
  - [Shapefile](#3--shapefile)
- [DLI Job Configuration](#dli-job-configuration)
- [Troubleshooting](#troubleshooting)
- [Known Limitations](#known-limitations)
- [Pending](#pending)

---

## Overview

This project shows how to ingest GIS data stored in Huawei OBS into Huawei DLI for processing using Apache Spark and Apache Sedona.

### What This Does
| Status | Feature |
|---|---|
| ✅ | Reads GIS files (Parquet, GeoJSON, Shapefile) from OBS |
| ✅ | Processes geometry columns |
| ✅ | Saves processed data back to OBS |
| ⏳ | Save directly to DLI Hive table *(pending Huawei support — see [Pending](#pending))* |

### Architecture
```
OBS Bucket (Raw GIS Data)
        │
        ▼
DLI Spark Job (Scala JAR)
  ├── Read GIS file from OBS
  ├── Process geometry column
  └── Write processed data back to OBS
        │
        ▼
OBS Bucket (Processed GIS Data)
```

---

## Why Scala JAR?

If you're wondering why this uses a Scala JAR instead of a simpler Python script — it's a DLI platform limitation:

```
❌ Python (PySpark) approach:
   DLI Python runtime = 3.7
   Sedona requires    = 3.8+
   Result             = ImportError

✅ Scala JAR approach:
   Runs on JVM (Java 8)
   No Python dependency
   Result             = Works!
```

---

## Technology Stack

| Component | Version | Why This Version |
|---|---|---|
| **DLI Spark** | 3.3.1 | Huawei Cloud DLI's Spark engine |
| **Java** | 8 (JRE 8) | DLI runtime only supports up to Java 8 |
| **Scala** | 2.12.15 | Compatible with Spark 3.3.1 |
| **Apache Sedona** | 1.5.1 | Latest version supporting Spark 3.3 + Java 8 |
| **GeoTools** | 1.5.1-28.2 | Required Sedona dependency — must match Sedona version |
| **sbt** | 1.12.5 | Scala build tool |

> ⚠️ **Why not the latest Sedona?** Sedona 1.6.0+ dropped support for Spark 3.3 and Java 8. Sedona 1.5.1 is the newest version that works on DLI.

---

## Prerequisites

Before you start, make sure you have:

1. A Huawei Cloud account with **DLI** and **OBS** services enabled
2. GIS data uploaded to your OBS bucket
3. A DLI agency with access to your OBS bucket
4. On your local machine:
   - Java 17 *(for building — the compile target is Java 8)*
   - sbt 1.12.5+
5. These JARs downloaded and uploaded to OBS:
   - [`sedona-spark-shaded-3.0_2.12-1.5.1.jar`](https://mvnrepository.com/artifact/org.apache.sedona/sedona-spark-shaded-3.0_2.12/1.5.1)
   - [`geotools-wrapper-1.5.1-28.2.jar`](https://mvnrepository.com/artifact/org.datasyslab/geotools-wrapper/1.5.1-28.2)

---

## Project Structure

```
gis-ingestion/
├── build.sbt                     # Build configuration
├── project/
│   └── plugins.sbt               # sbt assembly plugin
└── src/
    └── main/
        └── scala/
            ├── GISParquetIngestion.scala
            ├── GeoJSONIngestion.scala
            └── ShapefileIngestion.scala
```

---

## Setup & Build

### Step 1 — Update OBS paths in your `.scala` file

```scala
val inputPath  = "obs://YOUR-BUCKET/YOUR-INPUT-PATH"
val outputPath = "obs://YOUR-BUCKET/YOUR-OUTPUT-PATH"
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

Output JAR:
```
target/scala-2.12/gis-ingestion-assembly-1.0.jar
```

### Step 4 — Upload to OBS

Upload the JAR to your OBS bucket:
```
obs://YOUR-BUCKET/GIS_driver/GIS_JAR/gis-ingestion-assembly-1.0.jar
```

### `build.sbt`

```scala
name := "gis-ingestion"
version := "1.0"
scalaVersion := "2.12.15"

javacOptions ++= Seq("-source", "1.8", "-target", "1.8")
scalacOptions ++= Seq("-target:jvm-1.8")

resolvers ++= Seq(
  "UCAR Unidata" at "https://artifacts.unidata.ucar.edu/repository/unidata-all/",
  "Maven Central" at "https://repo1.maven.org/maven2/"
)

libraryDependencies ++= Seq(
  "org.apache.spark"  %% "spark-sql"                % "3.3.1" % "provided",
  "org.apache.spark"  %% "spark-hive"               % "3.3.1" % "provided",
  "org.apache.sedona" %% "sedona-spark-shaded-3.0"  % "1.5.1"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", "services", _*) => MergeStrategy.concat
  case PathList("META-INF", _*)             => MergeStrategy.discard
  case _                                    => MergeStrategy.first
}
```

### `project/plugins.sbt`

```scala
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "2.1.1")
```

---

## GIS Format Guides

There are three supported GIS formats, ordered from simplest to most complex.

---

### 1. 📦 GIS Parquet (Simplest)

Parquet is natively supported by Spark.

In this example, a geometry column (`polygon_suburb`) is stored as raw binary. We convert it to a hex string so it becomes readable when saved back to Parquet. This is a binary conversion job only — no coordinate parsing or CRS transformation.

#### Code Template

```scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object GISParquetIngestion {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("GIS Parquet Ingestion")
      .getOrCreate()

    // ── Config ────────────────────────────────────────────────────────────────
    val inputPath   = "obs://your-bucket/data_source/master_area_adm"
    val outputPath  = "obs://your-bucket/cleaned_tables/master_area_adm_processed"
    val geometryCol = "polygon_suburb"  // ← change to your geometry column name

    // ── Step 1: Read GIS Parquet from OBS ────────────────────────────────────
    println("Step 1: Reading GIS Parquet from OBS...")
    val df = spark.read.parquet(inputPath)
    println("Step 1 complete!")

    // ── Step 2: Convert binary geometry column to hex string ──────────────────
    println("Step 2: Converting binary geometry to hex string...")
    val dfConverted = df.withColumn(geometryCol, hex(col(geometryCol)))
    println("Step 2 complete!")

    // ── Step 3: Save back to OBS as Parquet ───────────────────────────────────
    println("Step 3: Saving to OBS...")
    dfConverted.write
      .mode("overwrite")
      .parquet(outputPath)
    println("Done! GIS Parquet data successfully processed!")

    spark.stop()
  }
}
```

---

### 2. 🌍 GeoJSON

**Requires Sedona.** GeoJSON files can be large single files, so we use `wholeTextFiles()` to read the entire file as one string. We then use **json4s** (bundled with Spark) to manually parse the `features` array — because GeoJSON is not a flat table, each feature contains nested `geometry` and `properties` objects.

Sedona's `ST_GeomFromGeoJSON` parses the geometry into a proper geometry object, and we output both WKT (human-readable) and hex/WKB (binary, compact) formats.

#### Code Template

```scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.sedona.spark.SedonaContext

object GeoJSONIngestion {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("GeoJSON Sedona Ingestion")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryo.registrator", "org.apache.sedona.core.serde.SedonaKryoRegistrator")
      .getOrCreate()

    SedonaContext.create(spark)

    // ── Config ────────────────────────────────────────────────────────────────
    val inputPath  = "obs://your-bucket/data_source/GeoJSON/your-file.json"
    val outputPath = "obs://your-bucket/cleaned_tables/your-output"

    // ── Step 1: Read entire GeoJSON file as raw text ──────────────────────────
    println("Step 1: Reading GeoJSON from OBS...")
    val rawJson = spark.sparkContext
      .wholeTextFiles(inputPath)
      .values
      .first()
    println("Step 1 complete!")

    // ── Step 2: Parse features (geometry + properties) ────────────────────────
    println("Step 2: Parsing GeoJSON features...")
    import org.json4s._
    import org.json4s.jackson.JsonMethods._
    implicit val formats: DefaultFormats.type = DefaultFormats

    val parsed   = parse(rawJson)
    val features = (parsed \ "features").children

    val rows: Seq[(String, String)] = features.map { f =>
      val geomStr = compact(render(f \ "geometry"))
      val propStr = compact(render(f \ "properties"))
      (geomStr, propStr)
    }

    import spark.implicits._
    val featuresDf = spark.createDataset(rows).toDF("geometry_json", "properties")
    featuresDf.count()
    println("Step 2 complete!")

    // ── Step 3: Parse geometry with Sedona ────────────────────────────────────
    println("Step 3: Parsing geometry with Sedona...")
    val dfWithGeom = featuresDf
      .withColumn("geometry",     expr("ST_GeomFromGeoJSON(geometry_json)"))
      .withColumn("geometry_wkt", expr("ST_AsText(geometry)"))    // human-readable
      .withColumn("geometry_hex", expr("ST_AsBinary(geometry)"))  // binary/compact
      .drop("geometry", "geometry_json")
    dfWithGeom.count()
    println("Step 3 complete!")

    // ── Step 4: Save to OBS as Parquet ────────────────────────────────────────
    println("Step 4: Saving to OBS...")
    dfWithGeom.write
      .mode("overwrite")
      .parquet(outputPath)
    println("Done! GeoJSON data successfully processed!")

    spark.stop()
  }
}
```

---

### 3. 🗺️ Shapefile

**Requires Sedona.** A shapefile is always made up of three files that must share the same base filename:

| File | Contents |
|---|---|
| `.shp` | Geometry features |
| `.shx` | Geometry index |
| `.dbf` | Attribute data (columns) |

> ⚠️ Unlike GeoJSON, Shapefile **cannot** use `spark.read.format("shapefile")` in DLI — the DataSource registration doesn't work correctly. Instead, we use Sedona's **RDD API** (`ShapefileReader`) which reads the folder directly and auto-joins all three files.

Point the `inputPath` to the **folder**, not an individual file. Sedona will auto-discover and join `.shp`, `.dbf`, and `.shx` automatically.

#### Code Template

```scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.sedona.spark.SedonaContext
import org.apache.sedona.core.formatMapper.shapefileParser.ShapefileReader
import org.apache.sedona.sql.utils.Adapter

object ShapefileIngestion {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Shapefile Sedona Ingestion")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryo.registrator", "org.apache.sedona.core.serde.SedonaKryoRegistrator")
      .getOrCreate()

    SedonaContext.create(spark)

    // ── Config ────────────────────────────────────────────────────────────────
    val inputPath  = "obs://your-bucket/data_source/Shapefile"  // ← folder, not a file
    val outputPath = "obs://your-bucket/cleaned_tables/Shapefile"

    // ── Step 1: Read Shapefile folder (auto-joins .shp, .dbf, .shx) ──────────
    println("Step 1: Reading Shapefile from OBS...")
    val spatialRDD  = ShapefileReader.readToGeometryRDD(spark.sparkContext, inputPath)
    val shapefileDf = Adapter.toDf(spatialRDD, spark)
    shapefileDf.count()
    println("Step 1 complete!")

    // ── Step 2: Convert geometry to WKT ──────────────────────────────────────
    println("Step 2: Converting geometry to WKT...")
    val dfWithWkt = shapefileDf
      .withColumn("geometry_wkt", expr("ST_AsText(geometry)"))
      .drop("geometry")
    dfWithWkt.count()
    println("Step 2 complete!")

    // ── Step 3: Save as CSV ───────────────────────────────────────────────────
    println("Step 3: Saving to OBS as CSV...")
    dfWithWkt.write
      .mode("overwrite")
      .option("header", "true")
      .csv(outputPath)
    println("Done! Shapefile data successfully processed!")

    spark.stop()
  }
}
```

---

## DLI Job Configuration

| Field | Value |
|---|---|
| **Job Name** | `gis-ingestion` |
| **Spark Version** | `3.3.1` |
| **Application** | `obs://YOUR-BUCKET/GIS_driver/GIS_JAR/gis-ingestion-assembly-1.0.jar` |
| **Main Class** | `GISParquetIngestion` / `GeoJSONIngestion` / `ShapefileIngestion` |
| **Agency** | Your DLI agency with OBS access |
| **Job Type** | Basic |
| **JAR Dependencies** | `obs://YOUR-BUCKET/GIS_driver/Apache_Sedona/sedona-spark-shaded-3.0_2.12-1.5.1.jar` |
| | `obs://YOUR-BUCKET/GIS_driver/Apache_Sedona/geotools-wrapper-1.5.1-28.2.jar` |

### Spark Arguments (`--conf`)

```
spark.driver.memory=2g
spark.executor.memory=2g
spark.executor.memoryOverhead=512m
spark.executor.instances=1
spark.executor.cores=2
```

---

## Troubleshooting

| Error | Cause | Fix |
|---|---|---|
| `Failed to find data source: shapefile` | Spark DataSource registration fails for shapefile format | Use `ShapefileReader` RDD API instead of `spark.read.format("shapefile")` |
| `ImportError: cannot import name 'Literal'` | DLI Python 3.7 is too old for Sedona | Use Scala JAR instead of PySpark |
| `UnsupportedClassVersionError: class file version 55.0` | JAR compiled for Java 11, DLI needs Java 8 | Add `-target:jvm-1.8` to `build.sbt` |
| `Table/View 'DBS' does not exist` | DLI JAR jobs cannot access Hive metastore by default | Save to OBS instead; see [Pending](#pending) |
| `Error downloading edu.ucar:cdm-core` | Missing UCAR repository in build config | Add UCAR resolver to `build.sbt` |
| `ClassNotFoundException: shapefile.DefaultSource` | Wrong Sedona JAR version for your Spark version | Use `sedona-spark-shaded-3.0_2.12-1.5.1.jar` for Spark 3.3 |

---

## Known Limitations

### 1. Cannot Save Directly to DLI Hive Table
Spark JAR jobs in DLI cannot connect to DLI's Hive metastore directly without explicit activation by Huawei:
```
Accessing DLI metadata from a Spark JAR job is a restricted feature
that requires explicit activation by Huawei — submit a support ticket to whitelist it.
```
**Workaround:** Save processed data back to OBS as Parquet instead.

### 2. Geometry Column Stored as Binary
The geometry column is stored as `binary` type in Parquet files. DLI Hive tables do not support binary types directly, so it must be converted to a hex string for storage compatibility.

### 3. Python Runtime Version
DLI's Python runtime is 3.7. Apache Sedona 1.5.1+ requires Python 3.8+. PySpark cannot be used with Sedona in DLI — a Scala JAR is required.

---

## Pending

### Save to DLI Hive Table

Once Huawei Cloud Support provides the correct Hive metastore URI, the code will be updated to save directly to a DLI table:

```scala
// Pending Huawei support ticket response
val spark = SparkSession.builder()
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

Pre-create the DLI table via the SQL Editor:

```sql
CREATE TABLE IF NOT EXISTS poc.master_area_adm (
  suburb               STRING,
  district             STRING,
  city                 STRING,
  province             STRING,
  polygon_suburb       STRING,
  taxi_operation_city  BOOLEAN
)
STORED AS PARQUET;
```

---

## Data Schema (GIS Parquet Example)

| Column | Type | Description |
|---|---|---|
| `suburb` | STRING | Suburb name |
| `district` | STRING | District name |
| `city` | STRING | City name |
| `province` | STRING | Province name |
| `polygon_suburb` | BINARY | Geometry polygon in WKB format |
| `taxi_operation_city` | BOOLEAN | Whether taxi operates in this city |

**Total rows:** 77,474

---

*Last updated: April 2026*
