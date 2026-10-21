# Databricks notebook source
CATALOG = "workspace"

notes = """
2026-10-21 Anterior A
Machine Chest Press 40 kg 8 set 1
Machine Chest Press 40 kg 7 set 2
Incline Machine Chest Press 15 kg 8 set 1
Incline Machine Chest Press 15 kg 7 set 2
Machine Lateral Raises 10 kg 8 set 1
Machine Lateral Raises 10 kg 7 set 2
Shoulder Press 40 kg 8 set 1
Shoulder Press 40 kg 7 set 2
Hack Squat 15 kg 12 set 1
Hack Squat 15 kg 10 set 2
Leg Extension 35 kg 10 set 1
Leg Extension 35 kg 8 set 2
Overhead Triceps Cable 15 kg 10 set 1
Overhead Triceps Cable 15 kg 8 set 2
Triceps Pushdown 20 kg 8 set 1
"""

dbutils.fs.put(f"/Volumes/{CATALOG}/gym/raw/notes.txt", notes.strip() + "\n", overwrite=True)

# COMMAND ----------

import csv, sys, os
from pathlib import Path

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..")))
from src.parser import parse_notes

CATALOG = "workspace"
notes_path = f"/Volumes/{CATALOG}/gym/raw/notes.txt"
out_path = f"/Volumes/{CATALOG}/gym/raw/workouts.csv"

rows, bad = parse_notes(Path(notes_path).read_text(encoding="utf-8"))

with open(out_path, "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(["date", "exercise", "set_no", "weight_kg", "reps"])
    w.writerows(rows)

print(f"parsed rows: {len(rows)}")
for b in bad:
    print("SKIPPED:", b)

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "workspace"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.gym")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.gym.raw")
path = f"/Volumes/{CATALOG}/gym/raw/workouts.csv"

# ---------- Bronze ----------
(spark.read.option("header", True).csv(path)
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.col("_metadata.file_path"))
    .write.format("delta").mode("overwrite")
    .saveAsTable(f"{CATALOG}.gym.bronze_workouts"))

# ---------- Silver ----------
silver = (
    spark.table(f"{CATALOG}.gym.bronze_workouts")
    .select(
        F.to_date("date").alias("workout_date"),
        F.trim(F.col("exercise")).alias("exercise"),
        F.col("set_no").cast("int").alias("set_no"),
        F.col("weight_kg").cast("double").alias("weight_kg"),
        F.col("reps").cast("int").alias("reps"),
    )
    .dropDuplicates()
    .filter(F.col("weight_kg").isNotNull() & (F.col("reps") > 0))
    .withColumn("volume_kg", F.col("weight_kg") * F.col("reps"))
    .withColumn("est_1rm", F.round(F.col("weight_kg") * (1 + F.col("reps") / 30), 1))
)
silver.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.gym.silver_workouts")

# ---------- Gold ----------
gold = (
    silver
    .groupBy(F.date_trunc("week", "workout_date").cast("date").alias("week_start"), "exercise")
    .agg(
        F.sum("volume_kg").alias("total_volume_kg"),
        F.max("est_1rm").alias("best_est_1rm"),
        F.count("*").alias("sets"),
    )
)
gold.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.gym.gold_weekly_progress")

display(spark.table(f"{CATALOG}.gym.gold_weekly_progress").orderBy("exercise", "week_start"))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS workspace.gym.silver_workouts (
# MAGIC   workout_date DATE, exercise STRING, set_no INT,
# MAGIC   weight_kg DOUBLE, reps INT, volume_kg DOUBLE, est_1rm DOUBLE
# MAGIC );
# MAGIC
# MAGIC MERGE INTO workspace.gym.silver_workouts AS t
# MAGIC USING (
# MAGIC   SELECT
# MAGIC     to_date(`date`) AS workout_date,
# MAGIC     trim(exercise) AS exercise,
# MAGIC     CAST(set_no AS INT) AS set_no,
# MAGIC     CAST(weight_kg AS DOUBLE) AS weight_kg,
# MAGIC     CAST(reps AS INT) AS reps,
# MAGIC     CAST(weight_kg AS DOUBLE) * CAST(reps AS INT) AS volume_kg,
# MAGIC     ROUND(CAST(weight_kg AS DOUBLE) * (1 + CAST(reps AS INT) / 30), 1) AS est_1rm
# MAGIC   FROM workspace.gym.bronze_workouts
# MAGIC   WHERE weight_kg IS NOT NULL AND CAST(reps AS INT) > 0
# MAGIC   QUALIFY ROW_NUMBER() OVER (
# MAGIC     PARTITION BY to_date(`date`), trim(exercise), CAST(set_no AS INT)
# MAGIC     ORDER BY _ingested_at DESC
# MAGIC   ) = 1
# MAGIC ) AS s
# MAGIC ON  t.workout_date = s.workout_date
# MAGIC AND t.exercise     = s.exercise
# MAGIC AND t.set_no       = s.set_no
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE workspace.gym.gold_weekly_progress AS
# MAGIC SELECT
# MAGIC   CAST(date_trunc('week', workout_date) AS DATE) AS week_start,
# MAGIC   exercise,
# MAGIC   SUM(volume_kg) AS total_volume_kg,
# MAGIC   MAX(est_1rm)   AS best_est_1rm,
# MAGIC   COUNT(*)       AS sets
# MAGIC FROM workspace.gym.silver_workouts
# MAGIC GROUP BY CAST(date_trunc('week', workout_date) AS DATE), exercise

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "workspace"
path = f"/Volumes/{CATALOG}/gym/raw/program.csv"

dbutils.fs.put(path, """session,ex_order,exercise,planned_sets,muscle_group
Anterior A,1,Machine Chest Press,2,chest
Anterior A,2,Incline Machine Chest Press,2,chest
Anterior A,3,Machine Lateral Raises,2,shoulders
Anterior A,4,Shoulder Press,2,shoulders
Anterior A,5,Hack Squat,2,quads
Anterior A,6,Leg Extension,2,quads
Anterior A,7,Overhead Triceps Cable,2,triceps
Anterior A,8,Triceps Pushdown,1,triceps
Posterior A,1,Lat Pulldown,2,back
Posterior A,2,Seated Cable Row (Close Grip),2,back
Posterior A,3,Reverse Fly Machine,2,rear_delts
Posterior A,4,Face Away Cable Curl,2,biceps
Posterior A,5,Biceps Cable,2,biceps
Posterior A,6,Leg Curl,2,hamstrings
Posterior A,7,Calves,2,calves
Posterior A,8,Abductor Machine,2,glutes
Anterior B,1,Incline Chest Press,2,chest
Anterior B,2,Chest Fly Machine,2,chest
Anterior B,3,Seated Machine Lateral Raises,2,shoulders
Anterior B,4,Shoulder Press,1,shoulders
Anterior B,5,Leg Press,2,quads
Anterior B,6,Hack Squat,2,quads
Anterior B,7,Triceps Dips Machine,2,triceps
Anterior B,8,Triceps Pushdown,1,triceps
Posterior B,1,Wide Grip Cable Row,2,back
Posterior B,2,Lat Pulldown (Close Grip),2,back
Posterior B,3,Reverse Fly Machine,2,rear_delts
Posterior B,4,Cable Shrugs,2,traps
Posterior B,5,Biceps Preacher Curl,2,biceps
Posterior B,6,Leg Curl,2,hamstrings
Posterior B,7,Calves,2,calves
Posterior B,8,Abductor Machine,2,glutes
""", overwrite=True)

# Bronze
(spark.read.option("header", True).csv(path)
    .withColumn("_ingested_at", F.current_timestamp())
    .write.format("delta").mode("overwrite")
    .saveAsTable(f"{CATALOG}.gym.bronze_program"))

# Silver
silver_prog = (
    spark.table(f"{CATALOG}.gym.bronze_program")
    .select(
        F.trim(F.col("session")).alias("session"),
        F.col("ex_order").cast("int").alias("ex_order"),
        F.trim(F.col("exercise")).alias("exercise"),
        F.col("planned_sets").cast("int").alias("planned_sets"),
        F.trim(F.col("muscle_group")).alias("muscle_group"),
    )
)
silver_prog.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.gym.silver_program")

# Gold: المجموعات المخططة لكل عضلة (دورة كاملة من الجلسات الأربع)
gold_prog = (
    silver_prog.groupBy("muscle_group")
    .agg(F.sum("planned_sets").alias("planned_sets_per_cycle"))
)
gold_prog.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.gym.gold_planned_sets")

display(spark.table(f"{CATALOG}.gym.gold_planned_sets").orderBy(F.desc("planned_sets_per_cycle")))

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "workspace"

workouts = spark.table(f"{CATALOG}.gym.silver_workouts")
ex_map = (spark.table(f"{CATALOG}.gym.silver_program")
          .select("exercise", "muscle_group").distinct())
planned = spark.table(f"{CATALOG}.gym.gold_planned_sets")

actual = (
    workouts.join(ex_map, "exercise", "left")
    .withColumn("muscle_group", F.coalesce("muscle_group", F.lit("unmapped")))
    .groupBy(F.date_trunc("week", "workout_date").cast("date").alias("week_start"), "muscle_group")
    .agg(F.count("*").alias("actual_sets"))
)

gold = (
    actual.join(planned, "muscle_group", "left")
    .withColumn("completion_pct", F.round(100 * F.col("actual_sets") / F.col("planned_sets_per_cycle"), 1))
)
gold.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.gym.gold_planned_vs_actual")

display(gold.orderBy("week_start", "muscle_group"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS silver_rows FROM workspace.gym.silver_workouts