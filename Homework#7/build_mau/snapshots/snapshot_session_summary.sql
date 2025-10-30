{% snapshot snapshot_session_summary %}
{{
  config(
    target_schema='SNAPSHOT',
    unique_key='sessionId',
    strategy='timestamp',
    updated_at='ts',
    invalidate_hard_deletes=true
  )
}}
select * from {{ ref('session_summary') }}
{% endsnapshot %}
