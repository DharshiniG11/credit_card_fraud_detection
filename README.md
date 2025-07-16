**# credit_card_fraud_detection**
A MySQL-powered fraud detection system analyzing transaction patterns, device anomalies, and Benford's Law violations to identify suspicious credit card activity.
This project implements rule-based fraud detection using SQL queries that identify suspicious transaction patterns based on established financial fraud theories:

**1. Behavioral Analysis Theory**
Detects device/location anomalies using the "Impossible Travel" concept

Applies velocity checking (multiple transactions in short time windows)

Implements "Known Fraudulent Device" pattern recognition

**2. Benford's Law (Digital Analysis)**
Mathematical law stating that in natural datasets, leading digits follow a specific distribution

Fraudulent transactions often violate this distribution

Our implementation shows actual vs expected digit frequencies

**3. Time-Series Anomaly Detection**
Based on "Temporal Pattern Disruption" theory

Flags unusual activity:

After long dormancy periods (account takeover)

During non-typical hours (1AM-5AM)

Abnormal weekend/weekday ratios

**4. Micro-Transaction Pattern Theory**
Detects "Transaction Smurfing" (structuring)

Identifies multiple sub-threshold transactions

Implements the "Below Radar" detection principle

**5. Shared Device Theory**
Applies "Device Reputation" models

Devices used by multiple users have higher fraud probability

Correlates with "Compromised Terminal" research
