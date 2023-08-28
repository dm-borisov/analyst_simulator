# Analyst Simulator

Here I store code from analyst simulator.

## A/B Tests
- aa_test.ipynb - verifies that the split system works correctly by conducting an A/A test
- ab_test.ipynb - compares different tests on ctr sample data
- linearized_likes.ipynb - compares t-test and linearized t-test

## Airflow
- alert.py - checks different metrics for anomalies by using the interquartile method and sends a report if founded
- report.py - extracts views/likes for yesterday and last week and makes a telegram report
- report_app.py - extracts different user metrics and makes a telegram repor
