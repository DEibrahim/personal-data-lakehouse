# Personal Data Lakehouse

Personal gym data pipeline on Databricks (PySpark + Delta Lake), using a Bronze → Silver → Gold layout.

## Flow
1. Free-text workout notes are parsed into `workouts.csv` (Python regex parser).
2. Workout log: `bronze_workouts` → `silver_workouts` → `gold_weekly_progress` (weekly volume and estimated 1RM per exercise).
3. Training program: `bronze_program` → `silver_program` → `gold_planned_sets` (planned sets per muscle group).
4. `gold_planned_vs_actual` compares planned and actual sets per muscle group per week.

## Run
Open `notebooks/gym_medallion.py` as a Databricks notebook and run the cells in order.
