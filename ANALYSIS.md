# Analysis

## Problem 1 Analysis

The log-level analysis revealed a total of **33,236,604 lines** scanned from the Spark cluster logs. Among these, **27,410,336 lines (≈82.5%)** contained recognizable log levels (INFO, WARN, ERROR, or FATAL).  
The breakdown is as follows:

| Log Level | Count | Percentage |
|------------|--------|-------------|
| INFO | 27,389,482 | 99.92% |
| WARN | 9,595 | 0.04% |
| ERROR | 11,259 | 0.04% |
| FATAL | 0 | 0.00% |

The overwhelming majority of log entries are **INFO-level messages**, indicating that most of the recorded activity reflects normal Spark job execution and system events (e.g., executor registration, security initialization, and remoting).  
Only a small fraction (≈0.08%) represent **warnings or errors**, which suggests the system was stable during execution and that failures or resource issues were rare.  
The sample entries confirm that Spark’s security manager and remoting components generated the most frequent informational events, typical during cluster startup.

---

## Problem 2 Analysis

The second part of the analysis examined **194 Spark applications** distributed across **6 unique clusters**, as shown below:

| Cluster ID | Number of Applications | First App Time | Last App Time |
|-------------|------------------------|----------------|----------------|
| 1485248649253 | 181 | 2017-01-24 17:00:28 | 2017-07-27 21:45:00 |
| 1472621869829 | 8 | 2016-09-09 07:43:47 | 2016-09-09 10:07:06 |
| 1448006111297 | 2 | 2016-04-07 10:45:21 | 2016-04-07 12:22:08 |
| 1440487435730 | 1 | 2015-09-01 18:14:40 | 2015-09-01 18:19:50 |
| 1460011102909 | 1 | 2016-07-26 11:54:20 | 2016-07-26 12:19:25 |
| 1474351042505 | 1 | 2016-11-18 22:30:06 | 2016-11-19 00:59:04 |

The **bar chart** (`problem2_bar_chart.png`) clearly illustrates the extreme workload imbalance:  
Cluster **1485248649253** handled **181 of 194 total applications (≈93%)**, while the remaining five clusters executed only 13 applications combined.  
This concentration implies that one cluster was the primary execution node for almost all Spark jobs, likely due to configuration defaults, job scheduling, or resource locality preferences.

The **job duration distribution** (`problem2_density_plot.png`) for this main cluster shows a **right-skewed pattern** with most jobs completing in under 1,000 seconds and a few long-running outliers exceeding 10,000 seconds.  
This suggests that while most applications were lightweight, there were a small number of computationally intensive tasks that significantly extended total cluster runtime.

---

## Performance & Observations

- **Total applications:** 194  
- **Unique clusters:** 6  
- **Average applications per cluster:** 32.33  
- **Dominant cluster:** 1485248649253 (181 apps, 93% of total)  
- **Log size analyzed:** ~3 GB across 194 directories  

Key performance observations:
1. Spark successfully processed a large, distributed log dataset without major failures or corruption.
2. The imbalance in application distribution suggests either a single-master job submission pattern or uneven scheduling configuration.
3. Minimal WARN/ERROR messages corroborate that the Spark environment maintained strong reliability under workload stress.

---

## Conclusion

The Spark log analysis demonstrates a **highly stable yet unevenly utilized** cluster environment.  
Most events were **informational system logs**, and nearly all jobs executed on a single cluster (1485248649253), showing limited distribution across nodes.  
While the environment shows reliability and consistent job completion, the cluster utilization pattern indicates potential room for optimization in **job scheduling and load balancing** across available clusters.
