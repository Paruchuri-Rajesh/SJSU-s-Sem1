-- models/output/session_summary.sql
with user_channel as (
  select * from {{ ref('user_session_channel') }}
),
session_ts as (
  select * from {{ ref('session_timestamp') }}
)

select
  uc.userId,
  uc.sessionId,
  uc.channel,
  min(st.ts) as session_start,
  max(st.ts) as session_end,
  datediff('minute', min(st.ts), max(st.ts)) as session_duration_min,
  count(st.ts) as event_count
from user_channel uc
left join session_ts st
  on uc.sessionId = st.sessionId
group by
  uc.userId,
  uc.sessionId,
  uc.channel
