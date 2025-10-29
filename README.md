# Policy Agreement ETL DBT Project

### Business & Technical Summary

Implements a full migration of the Informatica policy/contract agreement ETL pipeline into DBT for Snowflake. Joins and validates policy records from two input sources, standardizes and canonicalizes all data fields, applies code and date validation, generates all required surrogate and audit keys, splits/loads main agreement and role/party marts, and logs validation errors to distinct error logs. Implements all routing, sequence, union, filtering, and record type logic. 

**Source tables:**
- stg_pol_data_a
- stg_pol_data_b
- code/org/currency reference tables

**Mart outputs:**
- agmt
- cml_agmt (insert/update paths)
- finc_servs_rol (PCR, POR, INS streams)
- rol_plyr (PCR, POR streams)
- ctrl_on_off_keys_ciid (insert)
- ctrl_ctrl_on_off_keys_ciid_upd (update)
- ctrl_atm_update
- error_log_* (one for each error stream)

**Main Features:**
- Table and code validations via staging layer
- Date field normalization/validation
- Surrogate and audit key handling using DBT
- Insert/update routing logic per mart
- Comprehensive error logging, one mart per error type

