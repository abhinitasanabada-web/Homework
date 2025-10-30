SELECT
  u.userId,
  u.sessionId,
  u.channel,
  st.ts
FROM {{ ref('user_session_channel') }} AS u
JOIN {{ ref('session_timestamp') }} AS st
  ON u.sessionId = st.sessionId
