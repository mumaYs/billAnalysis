SELECT 
  NVL (T1.call_type, '') call_type,
  NVL (T1.other_city, '') other_city,
  NVL (T1.other_location, '') other_location,
  NVL (T1.other_phone, '') other_phone,
  CASE WHEN find_in_set(substr(trim(T1.other_phone),0,2), '134,135,136,137,138,139,147,150,151,152,157,158,159,178,182,183,184,187,188') > 0 THEN '移动'
  WHEN find_in_set(substr(trim(T1.other_phone),0,2), '130,131,132,155,156,185,186,145,176') > 0 THEN '联通'
  WHEN find_in_set(substr(trim(T1.other_phone),0,2), '133,153,177,180,181,189,173,177') > 0 THEN '电信'
  ELSE '未知' END other_operator,
  NVL (T1.own_city, '') own_city,
  NVL (T1.own_location, '') own_location,
  NVL (T1.own_phone, '') own_phone,
  CASE WHEN find_in_set(substr(trim(T1.own_phone),0,2), '134,135,136,137,138,139,147,150,151,152,157,158,159,178,182,183,184,187,188') > 0 THEN '移动'
  WHEN find_in_set(substr(trim(T1.own_phone),0,2), '130,131,132,155,156,185,186,145,176') > 0 THEN '联通'
  WHEN find_in_set(substr(trim(T1.own_phone),0,2), '133,153,177,180,181,189,173,177') > 0 THEN '电信'
  ELSE '未知' END own_operator,
  NVL (T1.own_station_id, '') own_station_id,
  NVL (T1.talk_time, '') talk_time,
  NVL (T1.own_xq, '') own_xq,
  NVL (T1.other_xq, '') other_xq,
  NVL (T1.three_phone, '') three_phone,
  NVL (T1.jhj_id, '') jhj_id,
  NVL (T1.other_phone_qz, '') other_phone_qz,
  NVL (T1.jswzq, '') jswzq,
  NVL (T1.jsxq, '') jsxq,
  NVL (T1.own_thd, '') own_thd,
  NVL (T1.other_station_id, '') other_station_id,
  CONCAT(T1.begin_date, ' ', T1.begin_time) date_time,
  CAST(
    FROM_UNIXTIME(
      UNIX_TIMESTAMP(
        CONCAT(T1.begin_date, ' ', T1.begin_time),
        'yyyy-MM-dd HH:mm:ss'
      ),
      'yyyyMMddHHmmss'
    ) AS BIGINT
  ) timestamp,
  CAST(
    FROM_UNIXTIME(
      UNIX_TIMESTAMP(
        CONCAT(T1.begin_date, ' ', T1.begin_time),
        'yyyy-MM-dd HH:mm:ss'
      ),
      'yyyyMMddHHmmss'
    ) AS STRING
  ) str_timestamp,
  NVL (T2.mcc, '') mcc,
  NVL (T2.mnc, '') mnc,
  NVL (T2.lac, '') lac,
  NVL (T2.ci, '') ci,
  NVL (T2.lat, '') lat,
  NVL (T2.lon, '') lon,
  NVL (T2.acc, '') acc,
  NVL (T2.date, '') date,
  NVL (T2.x, '') x,
  NVL (T2.addr, '') addr,
  NVL (T2.province, '') province,
  NVL (T2.city, '') city,
  NVL (T2.district, '') district,
  NVL (T2.township, '') township 
FROM
  BILL T1 
  LEFT OUTER JOIN BASESTATION T2 
    ON (
      T1.own_station_id = T2.lac 
      AND T1.own_xq = T2.ci
    )