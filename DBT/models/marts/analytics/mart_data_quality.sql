{{ config(materialized = 'table') }}

/*
    BUSINESS QUESTION  Can we trust today's numbers?  Which rules fire, how often,
                       and does every batch reconcile end-to-end?
    GRAIN              snapshot_date x batch_id x rule_name
    VALUE              Observability mart consumed by monitoring/alerting; makes
                       the pipeline's health queryable next to the business data.
*/
with dq as (

    select * from {{ ref('stg_silver__dq_results') }}

),

recon as (

    select * from {{ ref('stg_silver__reconciliation') }}

),

fact_counts as (

    select snapshot_date, batch_id, count(*) as fact_rows
    from {{ ref('fct_fare_observations') }}
    group by 1, 2

)

select
    dq.snapshot_date,
    dq.batch_id,
    dq.rule_name,
    dq.severity,
    dq.description,
    dq.failed_rows,
    dq.evaluated_rows,
    dq.failure_rate,

    r.bronze_rows,
    r.rejected_rows,
    r.duplicates_removed,
    r.silver_rows,
    fc.fact_rows,
    r.is_balanced                                   as bronze_to_silver_balanced,
    coalesce(fc.fact_rows, 0) = r.silver_rows       as silver_to_gold_balanced,
    r.rejection_rate,
    dq.evaluated_at

from dq
left join recon       r  on r.snapshot_date = dq.snapshot_date and r.batch_id = dq.batch_id
left join fact_counts fc on fc.snapshot_date = dq.snapshot_date and fc.batch_id = dq.batch_id