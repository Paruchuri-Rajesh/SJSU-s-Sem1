{% snapshot snapshot_session_summary %}
{% do config(
    target_schema='SNAPSHOT',
    unique_key='sessionId',
    strategy='timestamp',
    updated_at='ts',
    invalidate_hard_deletes=True
) %}
select * from {{ ref('session_summary') }}
{% endsnapshot %}