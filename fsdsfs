DROP TABLE tmp_cafemom;
CREATE TABLE tmp_cafemom AS
SELECT
  id,
  ts,
  COALESCE(xsup_id,xexc_id) "supplier",
  meas_state,
  domain_est,
  xurl_domain,
  xurl_host,
  xhost_match,
  has_page_tree,
  frm_ad_n,
  ad_recyc
FROM ads_raw
WHERE
  DATE_TRUNC('week', ts + INTERVAL '1 day') in (DATE_TRUNC('week', GETDATE() - INTERVAL '6 day'), DATE_TRUNC('week', GETDATE() - INTERVAL '13 day'))
  AND cli_id = '86599460ea5a4d508ecaa22e927db2133ee9e720' -- cafemom
;
ALTER TABLE tmp_cafemom ADD COLUMN reason varchar(100);

-- SUPPLIER SUMMARY
SELECT
  supplier "SUPPLIER",
	SUM(CASE WHEN DATE_TRUNC('week', ts + INTERVAL '1 day') = DATE_TRUNC('week', GETDATE() - INTERVAL '6 day') THEN 1 ELSE 0 END) "TOTAL",
(SUM(CASE WHEN DATE_TRUNC('week', ts + INTERVAL '1 day') = DATE_TRUNC('week', GETDATE() - INTERVAL '6 day') THEN 1 ELSE 0 END) - SUM(CASE WHEN DATE_TRUNC('week', ts + INTERVAL '1 day') = DATE_TRUNC('week', GETDATE() - INTERVAL '13 day') THEN 1 ELSE 0 END))::float / NULLIF(SUM(CASE WHEN DATE_TRUNC('week', ts + INTERVAL '1 day') = DATE_TRUNC('week', GETDATE() - INTERVAL '13 day') THEN 1 ELSE 0 END),0)::float "Percent Change",
  SUM(CASE WHEN DATE_TRUNC('week', ts + INTERVAL '1 day') = DATE_TRUNC('week', GETDATE() - INTERVAL '6 day') THEN 1 ELSE 0 END)::float / MAX(total_imps) "PCT OF TOTAL",
((SUM(CASE WHEN DATE_TRUNC('week', ts + INTERVAL '1 day') = DATE_TRUNC('week', GETDATE() - INTERVAL '6 day') THEN 1 ELSE 0 END)::float) / MAX(total_imps) - (SUM(CASE WHEN DATE_TRUNC('week', ts + INTERVAL '1 day') = DATE_TRUNC('week', GETDATE() - INTERVAL '13 day') THEN 1 ELSE 0 END)::float) / MAX(total_imps_old)) "Change in Percent",
  SUM(CASE WHEN meas_state = 'measured' AND DATE_TRUNC('week', ts + INTERVAL '1 day') = DATE_TRUNC('week', GETDATE() - INTERVAL '6 day') THEN 1 ELSE 0 END) "MEASURED",
((((SUM(CASE WHEN meas_state = 'measured' AND DATE_TRUNC('week', ts + INTERVAL '1 day') = DATE_TRUNC('week', GETDATE() - INTERVAL '6 day') THEN 1 ELSE 0 END) - SUM(CASE WHEN meas_state = 'measured' AND DATE_TRUNC('week', ts + INTERVAL '1 day') = DATE_TRUNC('week', GETDATE() - INTERVAL '13 day') THEN 1 ELSE 0 END))::float) / (NULLIF(SUM(CASE WHEN meas_state = 'measured' AND DATE_TRUNC('week', ts + INTERVAL '1 day') = DATE_TRUNC('week', GETDATE() - INTERVAL '13 day') THEN 1 ELSE 0 END),0))::float)) "Percent Change",
  SUM(CASE WHEN meas_state = 'measured' AND DATE_TRUNC('week', ts + INTERVAL '1 day') = DATE_TRUNC('week', GETDATE() - INTERVAL '6 day') THEN 1 ELSE 0 END)::float / NULLIF(SUM(CASE WHEN DATE_TRUNC('week', ts + INTERVAL '1 day') = DATE_TRUNC('week', GETDATE() - INTERVAL '6 day') THEN 1 ELSE 0 END),0) "PCT",
--(SUM(CASE WHEN meas_state = 'measured' AND DATE_TRUNC('week', ts + INTERVAL '1 day') = DATE_TRUNC('week', GETDATE() - INTERVAL '6 day') THEN 1 ELSE 0 END)::float / SUM(CASE WHEN DATE_TRUNC('week', ts + INTERVAL '1 day') = DATE_TRUNC('week', GETDATE() - INTERVAL '6 day') THEN 1 ELSE 0 END)) - (SUM(CASE WHEN meas_state = 'measured' AND DATE_TRUNC('week', ts + INTERVAL '1 day') = DATE_TRUNC('week', GETDATE() - INTERVAL '13 day') THEN 1 ELSE 0 END)::float / NULLIF(SUM(CASE WHEN DATE_TRUNC('week', ts + INTERVAL '1 day') = DATE_TRUNC('week', GETDATE() - INTERVAL '13 day') THEN 1 ELSE 0 END),0)) "Change in Percent",
(SUM(CASE WHEN meas_state = 'measured' AND DATE_TRUNC('week', ts + INTERVAL '1 day') = DATE_TRUNC('week', GETDATE() - INTERVAL '6 day') THEN 1 ELSE 0 END)::float / NULLIF((SUM(CASE WHEN DATE_TRUNC('week', ts + INTERVAL '1 day') = DATE_TRUNC('week', GETDATE() - INTERVAL '6 day') THEN 1 ELSE 0 END)),0)) - (SUM(CASE WHEN meas_state = 'measured' AND DATE_TRUNC('week', ts + INTERVAL '1 day') = DATE_TRUNC('week', GETDATE() - INTERVAL '13 day') THEN 1 ELSE 0 END)::float / NULLIF(SUM(CASE WHEN DATE_TRUNC('week', ts + INTERVAL '1 day') = DATE_TRUNC('week', GETDATE() - INTERVAL '13 day') THEN 1 ELSE 0 END),0)) "Change in Percent",
	SUM(CASE WHEN xhost_match IS TRUE AND meas_state = 'measured' AND DATE_TRUNC('week', ts + INTERVAL '1 day') = DATE_TRUNC('week', GETDATE() - INTERVAL '6 day') THEN 1 ELSE 0 END) "MATCHED",
(SUM(CASE WHEN xhost_match IS TRUE AND meas_state = 'measured' AND DATE_TRUNC('week', ts + INTERVAL '1 day') = DATE_TRUNC('week', GETDATE() - INTERVAL '6 day') THEN 1 ELSE 0 END) - SUM(CASE WHEN xhost_match IS TRUE AND meas_state = 'measured' AND DATE_TRUNC('week', ts + INTERVAL '1 day') = DATE_TRUNC('week', GETDATE() - INTERVAL '13 day') THEN 1 ELSE 0 END)::float) / NULLIF(SUM(CASE WHEN xhost_match IS TRUE AND meas_state = 'measured' AND DATE_TRUNC('week', ts + INTERVAL '1 day') = DATE_TRUNC('week', GETDATE() - INTERVAL '13 day') THEN 1 ELSE 0 END),0) "PERCENT CHANGE",
	SUM(CASE WHEN xhost_match IS NOT TRUE AND meas_state = 'measured' AND DATE_TRUNC('week', ts + INTERVAL '1 day') = DATE_TRUNC('week', GETDATE() - INTERVAL '6 day') THEN 1 ELSE 0 END) "MISMATCHED",
(SUM(CASE WHEN xhost_match IS NOT TRUE AND meas_state = 'measured' AND DATE_TRUNC('week', ts + INTERVAL '1 day') = DATE_TRUNC('week', GETDATE() - INTERVAL '6 day') THEN 1 ELSE 0 END) - SUM(CASE WHEN xhost_match IS NOT TRUE AND meas_state = 'measured' AND DATE_TRUNC('week', ts + INTERVAL '1 day') = DATE_TRUNC('week', GETDATE() - INTERVAL '13 day') THEN 1 ELSE 0 END))::float / NULLIF(SUM(CASE WHEN xhost_match IS NOT TRUE AND meas_state = 'measured' AND DATE_TRUNC('week', ts + INTERVAL '1 day') = DATE_TRUNC('week', GETDATE() - INTERVAL '13 day') THEN 1 ELSE 0 END),0)::float "PERCENT CHANGE",
  SUM(CASE WHEN xhost_match IS NOT TRUE AND meas_state = 'measured' AND DATE_TRUNC('week', ts + INTERVAL '1 day') = DATE_TRUNC('week', GETDATE() - INTERVAL '6 day') THEN 1 ELSE 0 END)::float / NULLIF(SUM(CASE WHEN meas_state = 'measured' AND DATE_TRUNC('week', ts + INTERVAL '1 day') = DATE_TRUNC('week', GETDATE() - INTERVAL '6 day') THEN 1 ELSE 0 END),0) "PCT",
(SUM(CASE WHEN xhost_match IS NOT TRUE AND meas_state = 'measured' AND DATE_TRUNC('week', ts + INTERVAL '1 day') = DATE_TRUNC('week', GETDATE() - INTERVAL '6 day') THEN 1 ELSE 0 END)::float / NULLIF(SUM(CASE WHEN meas_state = 'measured' AND DATE_TRUNC('week', ts + INTERVAL '1 day') = DATE_TRUNC('week', GETDATE() - INTERVAL '6 day') THEN 1 ELSE 0 END),0) - SUM(CASE WHEN xhost_match IS NOT TRUE AND meas_state = 'measured' AND DATE_TRUNC('week', ts + INTERVAL '1 day') = DATE_TRUNC('week', GETDATE() - INTERVAL '13 day') THEN 1 ELSE 0 END)::float / NULLIF(SUM(CASE WHEN meas_state = 'measured' AND DATE_TRUNC('week', ts + INTERVAL '1 day') = DATE_TRUNC('week', GETDATE() - INTERVAL '13 day') THEN 1 ELSE 0 END),0)) "Change in Percent",
  SUM(CASE WHEN frm_ad_n > 1 AND has_page_tree IS TRUE AND DATE_TRUNC('week', ts + INTERVAL '1 day') = DATE_TRUNC('week', GETDATE() - INTERVAL '6 day') THEN 1 ELSE 0 END) "TOTAL HIDDEN",
(SUM(CASE WHEN frm_ad_n > 1 AND has_page_tree IS TRUE AND DATE_TRUNC('week', ts + INTERVAL '1 day') = DATE_TRUNC('week', GETDATE() - INTERVAL '6 day') THEN 1 ELSE 0 END) - SUM(CASE WHEN frm_ad_n > 1 AND has_page_tree IS TRUE AND DATE_TRUNC('week', ts + INTERVAL '1 day') = DATE_TRUNC('week', GETDATE() - INTERVAL '13 day') THEN 1 ELSE 0 END))::float / NULLIF(SUM(CASE WHEN frm_ad_n > 1 AND has_page_tree IS TRUE AND DATE_TRUNC('week', ts + INTERVAL '1 day') = DATE_TRUNC('week', GETDATE() - INTERVAL '13 day') THEN 1 ELSE 0 END),0)::float "Percent Change",
  SUM(CASE WHEN frm_ad_n > 1 AND has_page_tree IS TRUE AND DATE_TRUNC('week', ts + INTERVAL '1 day') = DATE_TRUNC('week', GETDATE() - INTERVAL '6 day') THEN 1 ELSE 0 END)::float / NULLIF(SUM(CASE WHEN has_page_tree IS TRUE AND DATE_TRUNC('week', ts + INTERVAL '1 day') = DATE_TRUNC('week', GETDATE() - INTERVAL '6 day') THEN 1 ELSE 0 END),0) "PCT",
(SUM(CASE WHEN frm_ad_n > 1 AND has_page_tree IS TRUE AND DATE_TRUNC('week', ts + INTERVAL '1 day') = DATE_TRUNC('week', GETDATE() - INTERVAL '6 day') THEN 1 ELSE 0 END)::float / NULLIF(SUM(CASE WHEN has_page_tree IS TRUE AND DATE_TRUNC('week', ts + INTERVAL '1 day') = DATE_TRUNC('week', GETDATE() - INTERVAL '6 day') THEN 1 ELSE 0 END),0) - SUM(CASE WHEN frm_ad_n > 1 AND has_page_tree IS TRUE AND DATE_TRUNC('week', ts + INTERVAL '1 day') = DATE_TRUNC('week', GETDATE() - INTERVAL '13 day') THEN 1 ELSE 0 END)::float / NULLIF(SUM(CASE WHEN has_page_tree IS TRUE AND DATE_TRUNC('week', ts + INTERVAL '1 day') = DATE_TRUNC('week', GETDATE() - INTERVAL '13 day') THEN 1 ELSE 0 END),0)) "Change in Percent",
  SUM(CASE WHEN ad_recyc = 0 AND has_page_tree IS TRUE AND DATE_TRUNC('week', ts + INTERVAL '1 day') = DATE_TRUNC('week', GETDATE() - INTERVAL '6 day') THEN 1 ELSE 0 END) "FIRST LOOK",
(SUM(CASE WHEN ad_recyc = 0 AND has_page_tree IS TRUE AND DATE_TRUNC('week', ts + INTERVAL '1 day') = DATE_TRUNC('week', GETDATE() - INTERVAL '6 day') THEN 1 ELSE 0 END) - SUM(CASE WHEN ad_recyc = 0 AND has_page_tree IS TRUE AND DATE_TRUNC('week', ts + INTERVAL '1 day') = DATE_TRUNC('week', GETDATE() - INTERVAL '13 day') THEN 1 ELSE 0 END))::float / NULLIF(SUM(CASE WHEN ad_recyc = 0 AND has_page_tree IS TRUE AND DATE_TRUNC('week', ts + INTERVAL '1 day') = DATE_TRUNC('week', GETDATE() - INTERVAL '13 day') THEN 1 ELSE 0 END),0)::float "Percent Change",
  SUM(CASE WHEN ad_recyc > 0 AND has_page_tree IS TRUE AND DATE_TRUNC('week', ts + INTERVAL '1 day') = DATE_TRUNC('week', GETDATE() - INTERVAL '6 day') THEN 1 ELSE 0 END) "RECYCLED",
(SUM(CASE WHEN ad_recyc > 0 AND has_page_tree IS TRUE AND DATE_TRUNC('week', ts + INTERVAL '1 day') = DATE_TRUNC('week', GETDATE() - INTERVAL '6 day') THEN 1 ELSE 0 END) - SUM(CASE WHEN ad_recyc > 0 AND has_page_tree IS TRUE AND DATE_TRUNC('week', ts + INTERVAL '1 day') = DATE_TRUNC('week', GETDATE() - INTERVAL '13 day') THEN 1 ELSE 0 END))::float / NULLIF(SUM(CASE WHEN ad_recyc > 0 AND has_page_tree IS TRUE AND DATE_TRUNC('week', ts + INTERVAL '1 day') = DATE_TRUNC('week', GETDATE() - INTERVAL '13 day') THEN 1 ELSE 0 END),0)::float "Percent Change",
	SUM(CASE WHEN ad_recyc > 0 AND has_page_tree IS TRUE AND DATE_TRUNC('week', ts + INTERVAL '1 day') = DATE_TRUNC('week', GETDATE() - INTERVAL '6 day') THEN 1 ELSE 0 END)::float / NULLIF(SUM(CASE WHEN has_page_tree IS TRUE AND DATE_TRUNC('week', ts + INTERVAL '1 day') = DATE_TRUNC('week', GETDATE() - INTERVAL '6 day') THEN 1 ELSE 0 END),0) "PCT",
(SUM(CASE WHEN ad_recyc > 0 AND has_page_tree IS TRUE AND DATE_TRUNC('week', ts + INTERVAL '1 day') = DATE_TRUNC('week', GETDATE() - INTERVAL '6 day') THEN 1 ELSE 0 END)::float / NULLIF(SUM(CASE WHEN has_page_tree IS TRUE AND DATE_TRUNC('week', ts + INTERVAL '1 day') = DATE_TRUNC('week', GETDATE() - INTERVAL '6 day') THEN 1 ELSE 0 END),0) - SUM(CASE WHEN ad_recyc > 0 AND has_page_tree IS TRUE AND DATE_TRUNC('week', ts + INTERVAL '1 day') = DATE_TRUNC('week', GETDATE() - INTERVAL '13 day') THEN 1 ELSE 0 END)::float / NULLIF(SUM(CASE WHEN has_page_tree IS TRUE AND DATE_TRUNC('week', ts + INTERVAL '1 day') = DATE_TRUNC('week', GETDATE() - INTERVAL '13 day') THEN 1 ELSE 0 END),0)) "Change in Percent"

FROM (
  SELECT
    supplier,
    meas_state,
    xhost_match,
    frm_ad_n,
    has_page_tree,
    ad_recyc,
		ts,
    SUM(CASE WHEN DATE_TRUNC('week', ts + INTERVAL '1 day') = DATE_TRUNC('week', GETDATE() - INTERVAL '6 day') THEN 1 ELSE 0 END) OVER () total_imps,
		SUM(CASE WHEN DATE_TRUNC('week', ts + INTERVAL '1 day') = DATE_TRUNC('week', GETDATE() - INTERVAL '13 day') THEN 1 ELSE 0 END) OVER () total_imps_old
  FROM tmp_cafemom
)
GROUP BY 1
ORDER BY "TOTAL" DESC
;

-- REPORTED DOMAIN SUMMARY
SELECT
  xurl_domain "REPORTED DOMAIN",
  supplier "SUPPLIER",
  COUNT(*) "TOTAL",
  COUNT(*)::float / MAX(total_imps) "PCT OF TOTAL",
  COUNT(*)::float / MAX(domain_imps) "PCT OF DOMAIN TOTAL",
  SUM(CASE WHEN meas_state = 'measured' THEN 1 ELSE 0 END) "MEASURED",
  SUM(CASE WHEN meas_state = 'measured' THEN 1 ELSE 0 END)::float / COUNT(*) "PCT",
  SUM(CASE WHEN xhost_match IS TRUE AND meas_state = 'measured' THEN 1 ELSE 0 END) "MATCHED",
  SUM(CASE WHEN xhost_match IS NOT TRUE AND meas_state = 'measured' THEN 1 ELSE 0 END) "MISMATCHED",
  SUM(CASE WHEN xhost_match IS NOT TRUE AND meas_state = 'measured' THEN 1 ELSE 0 END)::float / NULLIF(SUM(CASE WHEN meas_state = 'measured' THEN 1 ELSE 0 END),0) "PCT",
  SUM(CASE WHEN frm_ad_n > 1 AND has_page_tree IS TRUE THEN 1 ELSE 0 END) "TOTAL HIDDEN",
  SUM(CASE WHEN frm_ad_n > 1 AND has_page_tree IS TRUE THEN 1 ELSE 0 END)::float / NULLIF(SUM(CASE WHEN has_page_tree IS TRUE THEN 1 ELSE 0 END),0) "PCT",
  SUM(CASE WHEN ad_recyc = 0 AND has_page_tree IS TRUE THEN 1 ELSE 0 END) "FIRST LOOK",
  SUM(CASE WHEN ad_recyc > 0 AND has_page_tree IS TRUE THEN 1 ELSE 0 END) "RECYCLED",
  SUM(CASE WHEN ad_recyc > 0 AND has_page_tree IS TRUE THEN 1 ELSE 0 END)::float / NULLIF(SUM(CASE WHEN has_page_tree IS TRUE THEN 1 ELSE 0 END),0) "PCT"
FROM (
  SELECT
    xurl_domain,
    supplier,
    meas_state,
    xhost_match,
    frm_ad_n,
    has_page_tree,
    ad_recyc,
		ts,
    COUNT(*) OVER (PARTITION BY xurl_domain) domain_imps,
    COUNT(*) OVER () total_imps
  FROM tmp_cafemom
	--WHERE DATE_TRUNC('week', ts + INTERVAL '1 day') = DATE_TRUNC('week', GETDATE() - INTERVAL '6 day')
)
WHERE DATE_TRUNC('week', ts + INTERVAL '1 day') = DATE_TRUNC('week', GETDATE() - INTERVAL '6 day')
GROUP BY 1,2
ORDER BY "TOTAL" DESC
;

-- ACTUAL DOMAIN SUMMARY
SELECT
  domain_est "ACTUAL DOMAIN",
  supplier "SUPPLIER",
  COUNT(*) "TOTAL",
  COUNT(*)::float / MAX(total_imps) "PCT OF TOTAL",
  COUNT(*)::float / MAX(domain_imps) "PCT OF DOMAIN TOTAL",
  SUM(CASE WHEN xhost_match IS TRUE THEN 1 ELSE 0 END) "MATCHED",
  SUM(CASE WHEN xhost_match IS NOT TRUE THEN 1 ELSE 0 END) "MISMATCHED",
  SUM(CASE WHEN xhost_match IS NOT TRUE THEN 1 ELSE 0 END)::float / COUNT(*) "PCT",
  SUM(CASE WHEN frm_ad_n > 1 AND has_page_tree IS TRUE THEN 1 ELSE 0 END) "TOTAL HIDDEN",
  SUM(CASE WHEN frm_ad_n > 1 AND has_page_tree IS TRUE THEN 1 ELSE 0 END)::float / NULLIF(SUM(CASE WHEN has_page_tree IS TRUE THEN 1 ELSE 0 END),0) "PCT",
  SUM(CASE WHEN ad_recyc = 0 AND has_page_tree IS TRUE THEN 1 ELSE 0 END) "FIRST LOOK",
  SUM(CASE WHEN ad_recyc > 0 AND has_page_tree IS TRUE THEN 1 ELSE 0 END) "RECYCLED",
  SUM(CASE WHEN ad_recyc > 0 AND has_page_tree IS TRUE THEN 1 ELSE 0 END)::float / NULLIF(SUM(CASE WHEN has_page_tree IS TRUE THEN 1 ELSE 0 END),0) "PCT"
FROM (
  SELECT
    domain_est,
    supplier,
    meas_state,
    xhost_match,
    frm_ad_n,
    has_page_tree,
    ad_recyc,
		ts,
    COUNT(*) OVER (PARTITION BY domain_est) domain_imps,
    COUNT(*) OVER () total_imps
  FROM tmp_cafemom
  WHERE meas_state = 'measured'
	--AND DATE_TRUNC('week', ts + INTERVAL '1 day') = DATE_TRUNC('week', GETDATE() - INTERVAL '6 day')
)
WHERE DATE_TRUNC('week', ts + INTERVAL '1 day') = DATE_TRUNC('week', GETDATE() - INTERVAL '6 day')
GROUP BY 1,2
ORDER BY "TOTAL" DESC
;

-- SUPPLIER DOMAIN MISMATCH
UPDATE tmp_cafemom
SET reason = (
  SELECT r.reason
  FROM reasons r
  WHERE xurl_host LIKE CONCAT(CONCAT('%', r.host_rpt), '%')
        AND (r.domain_est IS NULL OR r.domain_est = domain_est)
  LIMIT 1)
WHERE
  reason IS NULL
  AND meas_state = 'measured'
  AND xhost_match IS FALSE
  AND xurl_domain IS NOT NULL
	--AND DATE_TRUNC('week', ts + INTERVAL '1 day') = DATE_TRUNC('week', GETDATE() - INTERVAL '6 day')
;

SELECT
  supplier "SUPPLIER",
  xurl_domain "REPORTED DOMAIN",
  domain_est "ACTUAL DOMAIN",
  reason "REASON",
  COUNT(*) "MISMATCH TOTAL"
FROM
  tmp_cafemom
WHERE
  meas_state = 'measured'
  AND xhost_match IS FALSE
  AND xurl_domain IS NOT NULL
	AND DATE_TRUNC('week', ts + INTERVAL '1 day') = DATE_TRUNC('week', GETDATE() - INTERVAL '6 day')
GROUP BY 1,2,3,4
ORDER BY "MISMATCH TOTAL" DESC
;

-- DAILY SUMMARY
SELECT
  DATE(ts) "DATE",
  COUNT(*) "TOTAL",
  SUM(CASE WHEN meas_state = 'measured' THEN 1 ELSE 0 END) "MEASURED",
	SUM(CASE WHEN meas_state = 'measured' AND EXTRACT(ISODOW FROM ts) = EXTRACT(ISODOW FROM (ts)) THEN 1 ELSE 0 END) "LAST WEEK MEASURED",
  SUM(CASE WHEN meas_state = 'measured' THEN 1 ELSE 0 END)::float / COUNT(*) "PCT",
  SUM(CASE WHEN xhost_match IS TRUE AND meas_state = 'measured' THEN 1 ELSE 0 END) "MATCHED",
  SUM(CASE WHEN xhost_match IS NOT TRUE AND meas_state = 'measured' THEN 1 ELSE 0 END) "MISMATCHED",
  SUM(CASE WHEN xhost_match IS NOT TRUE AND meas_state = 'measured' THEN 1 ELSE 0 END)::float / NULLIF(SUM(CASE WHEN meas_state = 'measured' THEN 1 ELSE 0 END),0) "PCT",
  SUM(CASE WHEN frm_ad_n > 1 AND has_page_tree IS TRUE THEN 1 ELSE 0 END) "TOTAL HIDDEN",
  SUM(CASE WHEN frm_ad_n > 1 AND has_page_tree IS TRUE THEN 1 ELSE 0 END)::float / NULLIF(SUM(CASE WHEN has_page_tree IS TRUE THEN 1 ELSE 0 END),0) "PCT",
  SUM(CASE WHEN ad_recyc = 0 AND has_page_tree IS TRUE THEN 1 ELSE 0 END) "FIRST LOOK",
  SUM(CASE WHEN ad_recyc > 0 AND has_page_tree IS TRUE THEN 1 ELSE 0 END) "RECYCLED",
  SUM(CASE WHEN ad_recyc > 0 AND has_page_tree IS TRUE THEN 1 ELSE 0 END)::float / NULLIF(SUM(CASE WHEN has_page_tree IS TRUE THEN 1 ELSE 0 END),0) "PCT"
FROM tmp_cafemom
WHERE DATE_TRUNC('week', ts + INTERVAL '1 day') = DATE_TRUNC('week', GETDATE() - INTERVAL '6 day')
GROUP BY 1
ORDER BY "DATE" DESC
;
