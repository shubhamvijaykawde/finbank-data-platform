# FinBank marts

The analytics layer contains:

- `dim_customer`
- `dim_account`
- `dim_merchant`
- `dim_date`
- `fact_transactions`
- `fact_fraud_alerts` (Phase 7)

`fact_transactions` remains the transaction-grain fact. `fact_fraud_alerts` has one row per alerted transaction and aggregates multiple triggered fraud rules into explainable rule codes and reasons.

The fraud model is an analytical demonstration layer. It is not a production banking fraud-control system.
