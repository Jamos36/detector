# Research influence on the PoC

Five papers and one code repository were read in full (PDF text of the arXiv versions, read 2026-09-28) before
the PoC was designed. Each table separates **what the authors report** (their data, protocol and numbers) from
**what this project takes from it**. None of the reported results is evidence of how the PoC will perform on the
target network: the studies use other datasets (mostly labelled benchmarks with injected attacks), other units
(single flows, packets, images, a utilisation signal) and, in several cases, labels or oracle choices that the PoC
does not have. Nothing in this repository re-measures their claims.

Transferable principles that shaped the PoC: check the training baseline for contamination, keep time order,
calibrate thresholds on a separate normal-ish period and freeze them, show uncertainty/stability instead of single
numbers, explain a flag with the evidence behind it, and watch for drift.

## 1. Miguel-Diez et al., *Anomaly detection in network flows using unsupervised online machine learning* (2025)

Paper: <https://arxiv.org/abs/2509.01375> · Code: <https://github.com/amigueldiez/anomaly-detection-online-learning-paper>

| aspect | content |
|---|---|
| Core idea | Online One-Class SVM (River `anomaly.OneClassSVM` inside a `QuantileFilter`) on individual NetFlow v9 flows; online scaler (MaxAbsScaler chosen) warmed up on 1,000 flows, detector warmed up on 100,000 benign flows, then test-then-train: a flow is scored, flagged if its score is above the running `q`-quantile, and learned only if not flagged. |
| Authors' data and results | NF-UNSW-NB15 (v1, v2): 8 fields incl. source/destination IPs converted to integers, both ports, protocol, in/out bytes, duration. Training flows selected by the benign label; test set = all attacks + an equal number of benign flows (balanced), randomly shuffled. Hyper-parameters (`q`, `nu`, learning rate, scaler) tuned against labelled accuracy/recall/FPR. Reported mean of 12 runs: v1 accuracy 0.985, recall 0.997, FPR 0.026; v2 accuracy 0.985, recall 1.0, FPR 0.030; < 0.033 ms per flow. |
| Code (inspected) | `hyperparameter_optimization.py`, `multiexecution.py`, `parameters.py`, `config.yaml`, `CITATION.cff`; pandas + River. Training rows are chosen with `Label == 0`; the online scaler also `learn_one`s every test flow before transforming it; metrics are computed per flow against labels. **No LICENSE file** (only CITATION.cff), so no code is copied or adapted; only ideas are cited. |
| Applicability here | Partial. Same data family (NetFlow) and the same detector family, but per-flow scoring, label-selected clean training, balanced shuffled test sets and label-tuned thresholds do not exist in our setting (no reliable labels, strong time order, realistic prevalence). |
| Adopted now | One-Class SVM as one of the two baselines (scikit-learn batch version); a scaler fitted before the detector on training data only; thresholds as quantiles of the score distribution (our review bands = reference-period quantiles); repeated runs to show stability (our seed repeats). |
| Deferred | Online/incremental updating (would let slow attacks become "normal"; revisit only with drift evidence and a guard against self-training on flagged data); per-flow scoring (host-window rows are traceable and tractable for a year). |
| Rejected | IP addresses and ports as numeric features (identifiers, not magnitudes); random shuffling and balanced test sets (break time order and prevalence); selecting training data by labels and tuning thresholds on labelled metrics (no labels here); a River dependency. |

## 2. Kamiguchi & Nishio, *Robust Unsupervised Network Intrusion Detection via Federated Learning with Selective Aggregation under Anomalous Sample Contamination* (2026)

Paper: <https://arxiv.org/abs/2607.25439> · Code (linked by the authors, not inspected): <https://github.com/nishio-laboratory/FLANDRE>

| aspect | content |
|---|---|
| Core idea | Unsupervised NIDS training data collected from deployed networks is contaminated. FLANDRE trains Deep SVDD with FedAvg across gateways (clients) and, after a warm-up, clusters clients by the distance of their local update to the global model (2-component GMM via EM) and excludes the more distant cluster, on the premise that only a minority of clients (devices) are compromised. |
| Authors' data and results | ToN IoT, CSE-CIC-IDS2018 (flows of 2018-03-23) and NF-UQ-NIDS-v2; 100 simulated clients; contamination ratio r = 0.1, 50 % anomalies in compromised clients. Best F1 over 20 runs, where "best" is the maximum test F1 along the learning curve (an oracle stopping point that uses test labels): FLANDRE 0.969 / 0.838 / 0.823 vs centralised Deep SVDD 0.925 / 0.797 / 0.590 and a contamination-free reference 0.973 / 0.831 / 0.876. |
| Applicability here | The problem statement applies directly: our baseline year may contain pentests or other attacks, and a model trained on them can absorb them as normal. The mechanism (federated clients, Deep SVDD) does not: we have one collection point, no clients, and no deep models. |
| Adopted now | Contamination sensitivity as a first-class output: every model is refitted on alternative baselines (with/without windows in the supplied ranges, and a trimmed refit without its own top-1 % training windows) and the ranking change is reported. A per-host check of the training period's highest scores (the host-level analogue of "anomalies concentrate in few clients"). Training periods overlapping a supplied range produce a warning. |
| Deferred | Iterative trimming until convergence; per-subnet or per-exporter baselines as "clients". |
| Rejected | Federated learning and Deep SVDD for a single-site offline PoC (new dependencies, no benefit without distributed clients). |

## 3. Luo et al., *URA-Net: Uncertainty-Integrated Anomaly Perception and Restoration Attention Network for Unsupervised Anomaly Detection* (2026)

Paper: <https://arxiv.org/abs/2603.22840>

| aspect | content |
|---|---|
| Core idea | Image anomaly detection by feature reconstruction: a pre-trained CNN gives multi-level features; synthetic feature-level anomalies are created during training (Perlin masks with ImageNet textures); a Bayesian module estimates anomalous regions with mean and variance (uncertainty); a restoration-attention transformer restores anomalous regions from global normal features; the anomaly map is the input-restoration discrepancy. |
| Authors' data and results | MVTec AD, BTAD (industrial) and OCT-2017 (medical); image- and pixel-level AUROC close to 100 % on most MVTec categories; the authors name logical anomalies (misplaced components) as a failure case. |
| Applicability here | Low. Different modality, supervision via synthetic anomalies, pre-trained vision backbones. |
| Adopted now | Only the principle of exposing uncertainty instead of a bare score: the report shows seed and baseline-variant rank agreement, caps percentile "rarity" at the reference sample's resolution, and states that a percentile of 1.0 means "above every reference window", not certainty. Their over-generalisation argument (a model can learn to reproduce anomalies) is the same risk as baseline contamination (section 2). |
| Rejected | Reconstruction networks, Bayesian layers and synthetic anomaly generation (the PoC must not depend on synthetic attacks). |

## 4. Lozano-Paredes et al., *Explainable Autoencoder-Based Anomaly Detection in IEC 61850 GOOSE Networks* (2026)

Paper: <https://arxiv.org/abs/2601.09287>

| aspect | content |
|---|---|
| Core idea | Two asymmetric autoencoders on sliding-window features of GOOSE traffic: one for protocol-sequence semantics (stNum/sqNum behaviour), one for timing/volume; thresholds from Extreme Value Theory (generalised Pareto tail of the training reconstruction error); explanations as per-feature reconstruction error. |
| Authors' data and results | Trained only on real operational substation traffic (steady state), tested on a public laboratory dataset (IEC61850SecurityDataset, ~10-minute traces with message suppression, data manipulation and DoS). Reported detection above 99 % with false positives below 5 % of traffic; window length matters (0.5-1 s best of 0.1-3 s). |
| Applicability here | Medium for principles, low for mechanics: GOOSE is deterministic and periodic, NetFlow host traffic is bursty; the protocol-sequence features have no NetFlow counterpart. |
| Adopted now | Evidence next to every flag: each candidate window lists its largest deviations from training medians (robust z on the model's input scale; described as context, not model attribution) and the inputs outside the training range. Thresholds come from the tail of a normal-ish reference period's scores. Window length is a first-class, configurable experiment parameter. |
| Deferred | EVT/GPD tail fits for band cutoffs (empirical quantiles on a large validation period are adequate for a PoC; EVT becomes useful if the reference period is short); a separate timing view (would need interarrival features, see TODO). |
| Rejected | Autoencoders and IEC 61850 semantics. |

## 5. Sheela & Dey, *Drift-Aware RL-based Wavelet Denoising for Network-Traffic Anomaly Detection* (2026)

Paper: <https://arxiv.org/abs/2607.20011>

| aspect | content |
|---|---|
| Core idea | A traffic-utilisation signal is noisy and drifts; a four-detector drift gate (Page-Hinkley, variance ratio, Jensen-Shannon, Anderson-Darling) decides per window whether a PPO agent should choose a wavelet-denoising configuration, rewarded by downstream anomaly-detection AUC and capacity-estimation error. Calibration, training, validation and test splits are kept apart; detector thresholds are frozen after calibration. |
| Authors' data and results | Reference utilisation trajectories (T = 701 samples) augmented by neural style transfer, with synthetic drift and noise. The authors state that the task numbers in the preprint come from a synthetic stand-in signal and an oracle configuration search (an upper bound), not the trained policy; drift-detection rates come from their original pipeline. |
| Applicability here | Low for the method (one univariate signal, RL), medium for the protocol and the drift concern: a year of traffic will drift, and a baseline from early months can make later normal changes look anomalous. |
| Adopted now | Drift monitoring of every model input against the training distribution (PSI per day or week, charted); band cutoffs calibrated on validation and then frozen, with the test period kept separate and the report warning that repeated tuning against test results makes them optimistic. |
| Rejected | Reinforcement learning and wavelet denoising (no single utilisation signal to denoise; adds complexity without a label-based reward). |

## Finding from building the PoC (not from the papers)

scikit-learn's `IsolationForest` cannot extrapolate: split thresholds are drawn inside the training range, so a
window whose values lie far beyond everything in the training period is scored like the most extreme *training*
windows, and can rank below sparse-but-in-range windows. A test pins this down
(`tests/test_poc_units.py::test_ocsvm_extrapolates_but_isolation_forest_cannot`): a held-out 200-port sweep ranks
first for the RBF One-Class SVM but not for the Isolation Forest. The PoC therefore reports both models side by side
and adds a `beyond_train_range` column (inputs outside the training min/max) to every scored window.
