# Anomaly-ranking experiment `demo-14ab7a19d5`

> **How to read this report.** Scores are *rankings* from unsupervised models (higher = more anomalous), not probabilities. Critical/High/Medium/Low are review bands calibrated on a reference period, not severities; `Benign` means *below the review threshold*, not *safe*. Supplied pentest date ranges are broad, weak temporal context: a window inside one is not a confirmed attack, and activity outside them is not cleared. Nothing here is accuracy, precision, recall or a false-positive rate - there are no reliable labels.

> **Mock/synthetic input** (`allow_inside_repo: true`): results describe fixture or demo data, not any real network.

- Generated: 2026-09-29T01:20:48.583994+00:00 (UTC). Code: 78f2d313efedd74e041d5be00403ed9f57de6f66 (src has uncommitted changes).
- Input: 6 Parquet files, 388,749 flows in 118,584 host x 15-min windows, 300 hosts, 2026-09-01 00:00:00+00 .. 2026-09-07 00:00:00+00 (UTC).
- Model inputs (11): `flows`, `bytes_total`, `packets_total`, `max_flow_bytes`, `bytes_per_packet`, `uniq_dst_ip`, `uniq_dst_port`, `internal_share`, `tcp_share`, `udp_share`, `mean_duration_s`.
- Models: iforest, ocsvm. Bands calibrated on: validation (quantile mode).

| period | UTC start | UTC end (excl.) | windows | days with data | hosts |
|---|---|---|---|---|---|
| train | 2026-09-01 | 2026-09-03 | 39433 | 2 | 300 |
| validation | 2026-09-03 | 2026-09-04 | 19778 | 1 | 300 |
| test | 2026-09-04 | 2026-09-07 | 59373 | 3 | 300 |

Scores on the training period are in-sample (exploratory only); validation calibrates the bands; test simulates later deployment.

**Warnings**

- feature icmp_share dropped: constant on training rows

## 1. Overview

![overview](charts/overview.svg)

Daily flow volume (top) and windows in any review band per model (bottom). Shaded ranges are the supplied pentest dates (grey) and their buffers (hatched).

## 2. Highest-ranked periods (finer resolution)

Up to 3 periods of 48 h centred on the highest reference percentile across models, at least 48 h apart. Bucket = one 15-minute window; each point is the highest-ranked host in that bucket.

![zoom 1](charts/zoom_1.svg)

Top window: `10.10.5.36` at 2026-09-05 01:00 UTC (max reference percentile 1.00000). Annotation context within the plotted range: demo-injected-attack-days (inside known pentest window).

![zoom 2](charts/zoom_2.svg)

Top window: `10.10.1.46` at 2026-09-02 09:15 UTC (max reference percentile 1.00000). Annotation context within the plotted range: no supplied interval.

## 3. Review bands over time and alert volume

![bands](charts/bands_over_time.svg)

![cutoffs](charts/cutoff_curve.svg)

Band cutoffs (raw-score thresholds from reference-period quantiles; change them with `netanomaly report --bands FILE` without retraining):

| model | reference | rows | mode | Critical: q / raw >= | High: q / raw >= | Medium: q / raw >= | Low: q / raw >= |
|---|---|---|---|---|---|---|---|
| iforest | validation | 19,778 | quantile | 0.99900 / 0.64562 | 0.99500 / 0.62615 | 0.99000 / 0.61154 | 0.97500 / 0.58733 |
| ocsvm | validation | 19,778 | quantile | 0.99900 / -71.208 | 0.99500 / -101.91 | 0.99000 / -111.93 | 0.97500 / -121.92 |

| model | period | band | windows | per day |
|---|---|---|---|---|
| iforest | test | Critical | 65 | 21.67 |
| iforest | test | High | 226 | 75.33 |
| iforest | test | Medium | 365 | 121.7 |
| iforest | test | Low | 967 | 322.3 |
| iforest | train | Critical | 38 | 19 |
| iforest | train | High | 143 | 71.5 |
| iforest | train | Medium | 246 | 123 |
| iforest | train | Low | 665 | 332.5 |
| iforest | validation | Critical | 20 | 20 |
| iforest | validation | High | 79 | 79 |
| iforest | validation | Medium | 99 | 99 |
| iforest | validation | Low | 297 | 297 |
| ocsvm | test | Critical | 135 | 45 |
| ocsvm | test | High | 325 | 108.3 |
| ocsvm | test | Medium | 324 | 108 |
| ocsvm | test | Low | 928 | 309.3 |
| ocsvm | train | Critical | 43 | 21.5 |
| ocsvm | train | High | 197 | 98.5 |
| ocsvm | train | Medium | 211 | 105.5 |
| ocsvm | train | Low | 572 | 286 |
| ocsvm | validation | Critical | 20 | 20 |
| ocsvm | validation | High | 79 | 79 |
| ocsvm | validation | Medium | 99 | 99 |
| ocsvm | validation | Low | 297 | 297 |

## 4. Score distributions

![scores](charts/score_distribution.svg)

Score direction for both models: `raw = -score_samples(x)`, higher = more anomalous. A test distribution shifted right of validation means more windows look unusual than during calibration (drift or new activity), so the bands fire more often.

## 5. Hosts x time

![iforest heatmap](charts/heatmap_iforest.svg)

![ocsvm heatmap](charts/heatmap_ocsvm.svg)

Hosts with the most review-band windows (all periods):

| host | windows | train windows | iforest review | ocsvm review | iforest review inside ranges | ocsvm review inside ranges | iforest max pct | ocsvm max pct |
|---|---|---|---|---|---|---|---|---|
| 10.10.1.49 | 345 | 115 | 86 | 117 | 50 | 68 | 0.9998 | 0.9999 |
| 10.10.1.47 | 344 | 121 | 84 | 109 | 36 | 46 | 1 | 1 |
| 10.10.1.44 | 328 | 113 | 77 | 105 | 37 | 54 | 1 | 0.9997 |
| 10.10.1.45 | 347 | 113 | 78 | 104 | 40 | 48 | 0.9999 | 1 |
| 10.10.1.41 | 349 | 121 | 70 | 101 | 33 | 55 | 0.9995 | 1 |
| 10.10.1.46 | 342 | 118 | 72 | 97 | 34 | 41 | 0.9999 | 1 |
| 10.10.1.42 | 338 | 113 | 70 | 92 | 36 | 48 | 1 | 0.9999 |
| 10.10.1.40 | 344 | 114 | 67 | 94 | 38 | 54 | 0.9999 | 1 |
| 10.10.1.43 | 350 | 116 | 62 | 96 | 33 | 45 | 1 | 0.9998 |
| 10.10.1.48 | 355 | 116 | 53 | 82 | 29 | 44 | 0.9996 | 1 |
| 10.10.5.28 | 390 | 125 | 9 | 34 | 3 | 29 | 0.9999 | 0.9999 |
| 10.10.4.244 | 385 | 120 | 8 | 34 | 4 | 31 | 0.9974 | 0.9999 |
| 10.10.20.32 | 572 | 191 | 23 | 15 | 11 | 6 | 0.9993 | 0.9968 |
| 10.10.4.199 | 355 | 118 | 6 | 32 | 3 | 27 | 0.9994 | 0.9999 |
| 10.10.20.16 | 570 | 189 | 18 | 15 | 8 | 11 | 0.999 | 0.9997 |

## 6. Ranked candidate windows

Top 25 review-band windows by highest reference percentile across models (all of them: `alerts.parquet`). *Largest deviations* are robust z-scores against training medians on the model's input scale - context for the analyst, not model attributions. Traces give source files and 0-based row indexes (`trace_rows` in alerts.parquet).

| # | host | window (UTC) | period | annotation context | best band | models | iforest pct | ocsvm pct | flows | largest deviations | beyond training range | host baseline | trace |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 10.10.1.47 | 2026-09-03 15:00 | validation | buffer / uncertain | Critical | iforest,ocsvm | 1.00000 | 0.99995 | 9 | mean_duration_s=1016.58 (z +6.5); packets_total=4028.0 (z +2.7); max_flow_bytes=1076411.0 (z +2.5) | - | 121 train windows; flows median 2, uniq_dst_ip median 2 | 9 flows; 1 file(s); first synth_netflow_20260903.parquet#38970 |
| 2 | 10.10.1.43 | 2026-09-04 14:00 | test | inside known pentest window | Critical | iforest,ocsvm | 1.00000 | 0.99985 | 10 | mean_duration_s=309.57 (z +5.1); packets_total=4941.0 (z +2.8); bytes_per_packet=239.96 (z -2.7) | - | 116 train windows; flows median 2, uniq_dst_ip median 2 | 10 flows; 1 file(s); first synth_netflow_20260904.parquet#31495 |
| 3 | 10.10.2.126 | 2026-09-06 04:00 | test | inside known pentest window | Critical | iforest,ocsvm | 0.99980 | 1.00000 | 14 | max_flow_bytes=79275306.0 (z +5.5); bytes_total=352424659.0 (z +5.4); packets_total=251724.0 (z +5.3) | bytes_total, packets_total, max_flow_bytes | 124 train windows; flows median 2, uniq_dst_ip median 2 | 14 flows; 1 file(s); first synth_netflow_20260906.parquet#4787 |
| 4 | 10.10.5.36 | 2026-09-05 01:00 | test | inside known pentest window | Critical | iforest,ocsvm | 0.99980 | 1.00000 | 300 | uniq_dst_port=300.0 (z +9.0); bytes_per_packet=60.0 (z -6.5); flows=300.0 (z +5.3) | flows, uniq_dst_port | 122 train windows; flows median 2, uniq_dst_ip median 2 | 300 flows; 1 file(s); first synth_netflow_20260905.parquet#1032 |
| 5 | 10.10.1.44 | 2026-09-05 15:00 | test | inside known pentest window | Critical | iforest,ocsvm | 1.00000 | 0.99970 | 13 | mean_duration_s=107.43 (z +3.9); packets_total=4499.0 (z +2.8); bytes_per_packet=281.15 (z -2.3) | - | 113 train windows; flows median 2, uniq_dst_ip median 2 | 13 flows; 1 file(s); first synth_netflow_20260905.parquet#39480 |
| 6 | 10.10.1.46 | 2026-09-02 09:15 | train | outside supplied windows | Critical | iforest,ocsvm | 0.99960 | 1.00000 | 1 | mean_duration_s=2438.05 (z +7.5); bytes_per_packet=60.0 (z -6.5); max_flow_bytes=120.0 (z -3.6) | - | 118 train windows; flows median 2, uniq_dst_ip median 2 | 1 flows; 1 file(s); first synth_netflow_20260902.parquet#10747 |
| 7 | 10.10.2.74 | 2026-09-05 18:15 | test | inside known pentest window | Critical | iforest,ocsvm | 0.99960 | 1.00000 | 15 | max_flow_bytes=88475991.0 (z +5.5); bytes_total=306870282.0 (z +5.3); packets_total=219225.0 (z +5.2) | bytes_total, packets_total, max_flow_bytes | 122 train windows; flows median 2, uniq_dst_ip median 2 | 15 flows; 1 file(s); first synth_netflow_20260905.parquet#56468 |
| 8 | 10.10.2.74 | 2026-09-05 18:00 | test | inside known pentest window | Critical | iforest,ocsvm | 0.99934 | 1.00000 | 11 | bytes_total=193225192.0 (z +5.1); max_flow_bytes=44593305.0 (z +5.1); packets_total=138026.0 (z +4.9) | bytes_total, packets_total, max_flow_bytes | 122 train windows; flows median 2, uniq_dst_ip median 2 | 11 flows; 1 file(s); first synth_netflow_20260905.parquet#55733 |
| 9 | 10.10.4.222 | 2026-09-04 07:00 | test | inside known pentest window | Critical | iforest,ocsvm | 0.99934 | 1.00000 | 120 | bytes_per_packet=60.0 (z -6.5); uniq_dst_ip=120.0 (z +5.4); flows=120.0 (z +4.2) | flows, uniq_dst_ip | 132 train windows; flows median 2, uniq_dst_ip median 2 | 120 flows; 1 file(s); first synth_netflow_20260904.parquet#7875 |
| 10 | 10.10.1.46 | 2026-09-04 21:00 | test | inside known pentest window | Critical | iforest,ocsvm | 0.99929 | 1.00000 | 1 | bytes_per_packet=60.0 (z -6.5); mean_duration_s=460.09 (z +5.6); max_flow_bytes=120.0 (z -3.6) | - | 118 train windows; flows median 2, uniq_dst_ip median 2 | 1 flows; 1 file(s); first synth_netflow_20260904.parquet#61994 |
| 11 | 10.10.1.48 | 2026-09-05 14:15 | test | inside known pentest window | Critical | iforest,ocsvm | 0.99929 | 1.00000 | 2 | mean_duration_s=571.31 (z +5.8); bytes_per_packet=80.67 (z -5.7); max_flow_bytes=122.0 (z -3.6) | - | 116 train windows; flows median 2, uniq_dst_ip median 2 | 2 flows; 1 file(s); first synth_netflow_20260905.parquet#34414 |
| 12 | 10.10.1.40 | 2026-09-05 10:30 | test | inside known pentest window | Critical | iforest,ocsvm | 0.99914 | 1.00000 | 2 | bytes_per_packet=70.67 (z -6.1); mean_duration_s=396.98 (z +5.4); max_flow_bytes=120.0 (z -3.6) | - | 114 train windows; flows median 2, uniq_dst_ip median 2 | 2 flows; 1 file(s); first synth_netflow_20260905.parquet#14871 |
| 13 | 10.10.1.42 | 2026-09-01 14:00 | train | outside supplied windows | Critical | iforest,ocsvm | 1.00000 | 0.99899 | 10 | mean_duration_s=359.37 (z +5.3); max_flow_bytes=559426.0 (z +2.1); packets_total=1582.0 (z +2.1) | - | 113 train windows; flows median 2, uniq_dst_ip median 2 | 10 flows; 1 file(s); first synth_netflow_20260901.parquet#31386 |
| 14 | 10.10.1.45 | 2026-09-06 07:45 | test | inside known pentest window | Critical | iforest,ocsvm | 0.99874 | 1.00000 | 1 | bytes_per_packet=60.0 (z -6.5); max_flow_bytes=120.0 (z -3.6); mean_duration_s=73.78 (z +3.5) | - | 113 train windows; flows median 2, uniq_dst_ip median 2 | 1 flows; 1 file(s); first synth_netflow_20260906.parquet#9249 |
| 15 | 10.10.5.36 | 2026-09-05 07:00 | test | inside known pentest window | Critical | iforest,ocsvm | 0.99869 | 1.00000 | 300 | flows=300.0 (z +5.3); bytes_per_packet=168.43 (z -3.7); packets_total=3620.0 (z +2.6) | flows | 122 train windows; flows median 2, uniq_dst_ip median 2 | 300 flows; 1 file(s); first synth_netflow_20260905.parquet#8155 |
| 16 | 10.10.2.79 | 2026-09-04 12:15 | test | inside known pentest window | Critical | iforest,ocsvm | 0.99863 | 1.00000 | 16 | max_flow_bytes=95018333.0 (z +5.6); bytes_total=393187488.0 (z +5.5); packets_total=280851.0 (z +5.4) | bytes_total, packets_total, max_flow_bytes | 118 train windows; flows median 2, uniq_dst_ip median 2 | 16 flows; 1 file(s); first synth_netflow_20260904.parquet#20688 |
| 17 | 10.10.2.126 | 2026-09-06 04:15 | test | inside known pentest window | Critical | iforest,ocsvm | 0.99793 | 1.00000 | 7 | max_flow_bytes=61855527.0 (z +5.3); bytes_total=147575427.0 (z +4.9); packets_total=105409.0 (z +4.7) | bytes_total, packets_total, max_flow_bytes | 124 train windows; flows median 2, uniq_dst_ip median 2 | 7 flows; 1 file(s); first synth_netflow_20260906.parquet#5130 |
| 18 | 10.10.2.79 | 2026-09-04 12:00 | test | inside known pentest window | Critical | iforest,ocsvm | 0.99752 | 1.00000 | 10 | max_flow_bytes=70118055.0 (z +5.4); bytes_total=106896691.0 (z +4.7); packets_total=76420.0 (z +4.5) | bytes_total, packets_total, max_flow_bytes | 118 train windows; flows median 2, uniq_dst_ip median 2 | 10 flows; 1 file(s); first synth_netflow_20260904.parquet#19218 |
| 19 | 10.10.5.41 | 2026-09-04 04:00 | test | inside known pentest window | Critical | iforest,ocsvm | 0.99752 | 1.00000 | 301 | flows=301.0 (z +5.3); bytes_per_packet=170.29 (z -3.7); packets_total=3575.0 (z +2.6) | flows | 128 train windows; flows median 2, uniq_dst_ip median 2 | 301 flows; 1 file(s); first synth_netflow_20260904.parquet#4280 |
| 20 | 10.10.3.166 | 2026-09-03 21:00 | validation | buffer / uncertain | Critical | iforest,ocsvm | 0.99732 | 1.00000 | 1 | bytes_per_packet=60.0 (z -6.5); max_flow_bytes=60.0 (z -4.1); bytes_total=60.0 (z -3.8) | - | 129 train windows; flows median 2, uniq_dst_ip median 2 | 1 flows; 1 file(s); first synth_netflow_20260903.parquet#60935 |
| 21 | 10.10.1.61 | 2026-09-06 15:00 | test | inside known pentest window | Critical | iforest,ocsvm | 0.99712 | 1.00000 | 307 | uniq_dst_port=302.0 (z +9.0); flows=307.0 (z +5.3); packets_total=797.0 (z +1.7) | flows, uniq_dst_port | 116 train windows; flows median 3, uniq_dst_ip median 2 | 307 flows; 1 file(s); first synth_netflow_20260906.parquet#39375 |
| 22 | 10.10.2.75 | 2026-09-04 15:00 | test | inside known pentest window | Critical | iforest,ocsvm | 0.99621 | 1.00000 | 306 | uniq_dst_port=302.0 (z +9.0); flows=306.0 (z +5.3); bytes_per_packet=172.57 (z -3.6) | flows, uniq_dst_port | 121 train windows; flows median 2, uniq_dst_ip median 2 | 306 flows; 1 file(s); first synth_netflow_20260904.parquet#38730 |
| 23 | 10.10.1.58 | 2026-09-06 03:00 | test | inside known pentest window | Critical | iforest,ocsvm | 0.99585 | 1.00000 | 301 | flows=301.0 (z +5.3); bytes_per_packet=217.79 (z -3.0); packets_total=3783.0 (z +2.6) | flows | 127 train windows; flows median 3, uniq_dst_ip median 2 | 301 flows; 1 file(s); first synth_netflow_20260906.parquet#3283 |
| 24 | 10.10.1.68 | 2026-09-02 19:30 | train | outside supplied windows | Critical | iforest,ocsvm | 0.99565 | 1.00000 | 1 | bytes_per_packet=60.0 (z -6.5); mean_duration_s=135.85 (z +4.2); max_flow_bytes=120.0 (z -3.6) | - | 122 train windows; flows median 2, uniq_dst_ip median 2 | 1 flows; 1 file(s); first synth_netflow_20260902.parquet#58538 |
| 25 | 10.10.1.41 | 2026-09-01 06:30 | train | outside supplied windows | Critical | iforest,ocsvm | 0.99277 | 1.00000 | 1 | mean_duration_s=11708.91 (z +9.3); bytes_per_packet=330.15 (z -1.9); packets_total=533.0 (z +1.4) | - | 121 train windows; flows median 2, uniq_dst_ip median 2 | 1 flows; 1 file(s); first synth_netflow_20260901.parquet#6916 |

*Beyond training range*: model inputs outside the training min/max. Isolation Forest cannot extrapolate (a value far beyond the training range scores like the most extreme training value), so such windows can rank lower than their novelty suggests. Outside training: 42 windows have at least one input beyond the training range; 2 of them are in no review band - filter `scores.parquet` on `len(beyond_train_range) > 0` to review them.

## 7. iforest vs ocsvm

![comparison](charts/model_comparison.svg)

Spearman = rank correlation over all windows of the period; top-N Jaccard = overlap of each model's 200 highest windows; daily top-20 overlap = mean share of each day's top windows the models share. Both see the same inputs but define 'unusual' differently (isolation depth vs distance from a learned boundary), so moderate agreement is expected; windows both rank high are the strongest candidates for review, not confirmed findings.

| period | windows | spearman | top-200 jaccard | daily top-20 overlap | both in review | only iforest | only ocsvm |
|---|---|---|---|---|---|---|---|
| test | 59373 | 0.604 | 0.1331 | 0.2 | 745 | 878 | 967 |
| train | 39433 | 0.6012 | 0.2085 | 0.175 | 451 | 641 | 572 |
| validation | 19778 | 0.596 | 0.1834 | 0.25 | 205 | 290 | 290 |

Largest disagreements in the test period (in one model's top 15, not the other's):

| kind | host | window (UTC) | iforest pct | ocsvm pct | annotation |
|---|---|---|---|---|---|
| top in iforest only | 10.10.1.43 | 2026-09-04 14:00:00+00 | 1 | 0.9998 | inside |
| top in ocsvm only | 10.10.2.75 | 2026-09-04 15:00:00+00 | 0.9962 | 1 | inside |
| top in iforest only | 10.10.1.44 | 2026-09-05 15:00:00+00 | 1 | 0.9997 | inside |
| top in iforest only | 10.10.1.45 | 2026-09-04 15:00:00+00 | 0.9999 | 0.9983 | inside |
| top in ocsvm only | 10.10.1.61 | 2026-09-06 15:00:00+00 | 0.9971 | 1 | inside |
| top in iforest only | 10.10.1.45 | 2026-09-06 16:00:00+00 | 0.9999 | 0.9994 | inside |
| top in ocsvm only | 10.10.4.222 | 2026-09-04 07:00:00+00 | 0.9993 | 1 | inside |
| top in iforest only | 10.10.1.40 | 2026-09-04 15:15:00+00 | 0.9999 | 0.9992 | inside |
| top in ocsvm only | 10.10.5.36 | 2026-09-05 07:00:00+00 | 0.9987 | 1 | inside |
| top in iforest only | 10.10.1.47 | 2026-09-05 11:30:00+00 | 0.9999 | 0.9999 | inside |
| top in ocsvm only | 10.10.4.223 | 2026-09-05 03:00:00+00 | 0.9916 | 1 | inside |
| top in iforest only | 10.10.5.28 | 2026-09-05 13:45:00+00 | 0.9999 | 0.9999 | inside |
| top in ocsvm only | 10.10.5.41 | 2026-09-04 04:00:00+00 | 0.9975 | 1 | inside |
| top in iforest only | 10.10.1.43 | 2026-09-04 19:00:00+00 | 0.9999 | 0.9987 | inside |
| top in ocsvm only | 10.10.5.35 | 2026-09-06 07:00:00+00 | 0.981 | 1 | inside |
| top in ocsvm only | 10.10.1.58 | 2026-09-06 03:00:00+00 | 0.9959 | 1 | inside |
| top in iforest only | 10.10.2.109 | 2026-09-06 01:30:00+00 | 0.9998 | 0.986 | inside |
| top in iforest only | 10.10.1.44 | 2026-09-04 12:30:00+00 | 0.9998 | 0.9997 | inside |
| top in iforest only | 10.10.1.44 | 2026-09-05 03:00:00+00 | 0.9998 | 0.9997 | inside |
| top in ocsvm only | 10.10.2.74 | 2026-09-05 18:15:00+00 | 0.9996 | 1 | inside |
| top in ocsvm only | 10.10.2.79 | 2026-09-04 12:15:00+00 | 0.9986 | 1 | inside |
| top in ocsvm only | 10.10.2.74 | 2026-09-05 18:00:00+00 | 0.9993 | 1 | inside |
| top in ocsvm only | 10.10.2.126 | 2026-09-06 04:15:00+00 | 0.9979 | 1 | inside |
| top in iforest only | 10.10.1.68 | 2026-09-06 10:15:00+00 | 0.9998 | 0.986 | inside |
| top in ocsvm only | 10.10.2.79 | 2026-09-04 12:00:00+00 | 0.9975 | 1 | inside |
| top in iforest only | 10.10.5.17 | 2026-09-05 17:30:00+00 | 0.9998 | 0.984 | inside |

## 8. Supplied pentest ranges (weak annotations)

| name | start (UTC) | end (UTC, excl.) | source | confidence | notes | periods touched |
|---|---|---|---|---|---|---|
| demo-injected-attack-days | 2026-09-04T00:00:00+00:00 | 2026-09-07T00:00:00+00:00 | data/synth/truth/injections.csv (synthetic demo) | high | scans, brute force, beaconing and exfiltration were injected on these days | test |

Where review-band and daily top-20 windows fall, versus all windows (buffer 12 h). *Ratio* = share among flagged windows / share among all windows; > 1 = over-represented. This describes co-occurrence with broad date ranges; it is not a detection rate, and any unusual activity that happens to coincide with the ranges would look the same.

| model | period | category | windows | base share | review share | review ratio | top-20 share | top-20 ratio |
|---|---|---|---|---|---|---|---|---|
| iforest | test | inside known pentest window | 59373 | 1 | 1 | 1 | 1 | 1 |
| iforest | train | outside supplied windows | 39433 | 1 | 1 | 1 | 1 | 1 |
| iforest | validation | buffer / uncertain | 11500 | 0.5815 | 0.5657 | 0.9728 | 0.7 | 1.204 |
| iforest | validation | outside supplied windows | 8278 | 0.4185 | 0.4343 | 1.038 | 0.3 | 0.7168 |
| ocsvm | test | inside known pentest window | 59373 | 1 | 1 | 1 | 1 | 1 |
| ocsvm | train | outside supplied windows | 39433 | 1 | 1 | 1 | 1 | 1 |
| ocsvm | validation | outside supplied windows | 8278 | 0.4185 | 0.4182 | 0.9991 | 0.35 | 0.8362 |
| ocsvm | validation | buffer / uncertain | 11500 | 0.5815 | 0.5818 | 1.001 | 0.65 | 1.118 |

Sensitivity to the buffer width, for windows *inside a range or its buffer* (a pattern that appears for one buffer only is fragile; a ratio that grows with a wider buffer suggests activity near, not in, the supplied dates):

| model | period | buffer h | inside+buffer base share | review share | review ratio | top-20 share | top-20 ratio |
|---|---|---|---|---|---|---|---|
| iforest | test | 0 | 1 | 1 | 1 | 1 | 1 |
| iforest | test | 12 | 1 | 1 | 1 | 1 | 1 |
| iforest | test | 48 | 1 | 1 | 1 | 1 | 1 |
| iforest | validation | 0 | 0 | 0 | - | 0 | - |
| iforest | validation | 12 | 0.5815 | 0.5657 | 0.9728 | 0.7 | 1.204 |
| iforest | validation | 48 | 1 | 1 | 1 | 1 | 1 |
| ocsvm | test | 0 | 1 | 1 | 1 | 1 | 1 |
| ocsvm | test | 12 | 1 | 1 | 1 | 1 | 1 |
| ocsvm | test | 48 | 1 | 1 | 1 | 1 | 1 |
| ocsvm | validation | 0 | 0 | 0 | - | 0 | - |
| ocsvm | validation | 12 | 0.5815 | 0.5818 | 1.001 | 0.65 | 1.118 |
| ocsvm | validation | 48 | 1 | 1 | 1 | 1 | 1 |

## 9. Robustness, contamination and drift

**Seed stability** (validation period): each model refitted with other seeds (the seed also changes the training sample).

| model | variant | spearman | top-200 jaccard |
|---|---|---|---|
| iforest | iforest_seed43 | 0.983 | 0.7467 |
| iforest | iforest_seed44 | 0.9697 | 0.6949 |
| ocsvm | ocsvm_seed43 | 1 | 0.99 |
| ocsvm | ocsvm_seed44 | 1 | 0.9802 |

**Contamination sensitivity** (test period): the same model trained on alternative baselines. `excl_annotated` / `incl_annotated` flip whether windows in the supplied ranges (and buffers) are training data; `trimNNN` refits without the primary model's own highest-scoring training windows. Large changes mean the baseline choice matters: a baseline containing attack-like activity can absorb it as normal.

| model | variant | train rows | spearman vs primary | top-200 jaccard | top-N inside/buffer/outside (primary) | (variant) |
|---|---|---|---|---|---|---|
| iforest | iforest_trim990 | 39038 | 0.986 | 0.5094 | 1.00 / 0.00 / 0.00 | 1.00 / 0.00 / 0.00 |
| ocsvm | ocsvm_trim990 | 20000 | 0.9955 | 0.6393 | 1.00 / 0.00 / 0.00 | 1.00 / 0.00 / 0.00 |

Hosts over-represented in iforest's top 1 % of *training* windows (possible baseline contamination, or unusual-but-normal hosts; worth a look):

| host | train windows | top windows | share of top | share of windows |
|---|---|---|---|---|
| 10.10.1.46 | 118 | 19 | 0.0481 | 0.002992 |
| 10.10.1.45 | 113 | 15 | 0.03797 | 0.002866 |
| 10.10.1.40 | 114 | 12 | 0.03038 | 0.002891 |
| 10.10.1.47 | 121 | 12 | 0.03038 | 0.003068 |
| 10.10.1.49 | 115 | 12 | 0.03038 | 0.002916 |

Hosts over-represented in ocsvm's top 1 % of *training* windows (possible baseline contamination, or unusual-but-normal hosts; worth a look):

| host | train windows | top windows | share of top | share of windows |
|---|---|---|---|---|
| 10.10.1.46 | 118 | 32 | 0.08101 | 0.002992 |
| 10.10.1.47 | 121 | 31 | 0.07848 | 0.003068 |
| 10.10.1.45 | 113 | 29 | 0.07342 | 0.002866 |
| 10.10.1.43 | 116 | 22 | 0.0557 | 0.002942 |
| 10.10.1.49 | 115 | 22 | 0.0557 | 0.002916 |

Daily top-20 concentration for iforest (few hosts dominating = one noisy host may crowd out others):

| period | top windows | distinct hosts | top-5 host share | most frequent host | count |
|---|---|---|---|---|---|
| test | 60 | 40 | 0.3333 | 10.10.1.46 | 5 |
| train | 40 | 31 | 0.325 | 10.10.1.46 | 4 |
| validation | 20 | 17 | 0.4 | 10.10.1.44 | 3 |

Daily top-20 concentration for ocsvm (few hosts dominating = one noisy host may crowd out others):

| period | top windows | distinct hosts | top-5 host share | most frequent host | count |
|---|---|---|---|---|---|
| test | 60 | 29 | 0.5333 | 10.10.4.244 | 14 |
| train | 40 | 30 | 0.3 | 10.10.1.41 | 3 |
| validation | 20 | 17 | 0.4 | 10.10.1.44 | 3 |

![drift](charts/drift.svg)

## 10. Coverage, field mapping and data quality

| canonical | source column | type | status | conversion | note |
|---|---|---|---|---|---|
| flow_start | flow_start_time | TIMESTAMP WITH TIME ZONE | ok | UTC instant |  |
| src_ip | src_id_addr | VARCHAR | ok | text; IPv4 dotted quads recognised for the internal/external share |  |
| flow_end | flow_end_time | TIMESTAMP WITH TIME ZONE | ok | UTC instant |  |
| dst_ip | dist_id_addr | VARCHAR | ok | text; IPv4 dotted quads recognised for the internal/external share |  |
| dst_port | dist_port | BIGINT | ok | integer code (never used as a magnitude) |  |
| protocol | ip_protocol_id | BIGINT | ok | integer code (never used as a magnitude) |  |
| bytes | num_bytes | BIGINT | ok | numeric; negative values treated as NULL |  |
| packets | num_packets | BIGINT | ok | numeric; negative values treated as NULL |  |

Unmapped source columns (never modelled): 34. Full profile: `uv run netanomaly profile` -> `profile/profile.md`.

Assumptions:

- No field meaning is validated against collector documentation (contract: 0/42 validated); the mapping is a configuration choice, not a verified semantic.
- bytes/packets are assumed per flow, one direction (octetDeltaCount / packetDeltaCount).
- flow_end - flow_start is assumed to be the flow duration; exporter active/idle timeouts split long flows, so durations are capped by the exporter configuration.
- internal share uses RFC 1918 IPv4 ranges only; IPv6 or non-dotted values count as unknown.

| feature | null share | train null share | train non-finite | status | handling |
|---|---|---|---|---|---|
| flows | 0 | 0 | 0 | ok | log1p (stateless), then median imputation fitted on training rows |
| bytes_total | 0 | 0 | 0 | ok | log1p (stateless), then median imputation fitted on training rows |
| packets_total | 0 | 0 | 0 | ok | log1p (stateless), then median imputation fitted on training rows |
| max_flow_bytes | 0 | 0 | 0 | ok | log1p (stateless), then median imputation fitted on training rows |
| bytes_per_packet | 0 | 0 | 0 | ok | log1p (stateless), then median imputation fitted on training rows |
| uniq_dst_ip | 0 | 0 | 0 | ok | log1p (stateless), then median imputation fitted on training rows |
| uniq_dst_port | 0 | 0 | 0 | ok | log1p (stateless), then median imputation fitted on training rows |
| internal_share | 0 | 0 | 0 | ok | median imputation fitted on training rows |
| tcp_share | 0 | 0 | 0 | ok | median imputation fitted on training rows |
| udp_share | 0 | 0 | 0 | ok | median imputation fitted on training rows |
| icmp_share | 0 | 0 | 0 | constant | dropped from the model (constant on training rows) |
| mean_duration_s | 0 | 0 | 0 | ok | log1p (stateless), then median imputation fitted on training rows |

Training filter: including windows in supplied ranges. Training samples (bounded, spread over days):

| model | train rows | available | sample rule |
|---|---|---|---|
| iforest | 39433 | 39433 | training rows ranked per UTC day by hash(src_ip, window_start, seed=42); at most 100000 rows per day (= ceil(200000 / 2 training days)), then at most 200000 overall by the same hash. A row's inclusion depends only on its own key and day. |
| ocsvm | 20000 | 39433 | training rows ranked per UTC day by hash(src_ip, window_start, seed=42); at most 10000 rows per day (= ceil(20000 / 2 training days)), then at most 20000 overall by the same hash. A row's inclusion depends only on its own key and day. |

## 11. Limitations of this run

- No reliable labels: nothing here measures detection quality. Pentest ranges are broad; overlap with them is descriptive.
- A high rank means *unusual relative to the chosen training baseline*, which may be benign change (new services, backups, your own scanners) - and a baseline that contains attack-like activity can hide it.
- Features describe one host and one window only (no history, no peer baselines, no periodicity), so slow, low-volume or beacon-like activity may rank low.
- Field meanings are unvalidated (see mapping). Timestamps without offset were assumed UTC where noted.
- Changing settings after looking at test results makes later test results optimistic.
- Raw scores are only comparable within one fitted model; use percentiles/bands to compare models.
