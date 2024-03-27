/*
-------------------------------------------------------------------------------
SQL Queries and Pipeline Sample

Please Note: Please note elements of this SQL code might be removed for 
privacy reasons and replaced with ... or different names.

Created By: Rashmi M

Description: 
This is a small sample of SQL work of mine. 

The first 2 entries are queries:
-Sample 1.
-Sample 2.

This last entry is one (of many) views in the KPI Pipeline in the presentation
in this repository. For this we pull and aggregate encounter level data 
to monthly and fiscal figures across several measures and from different sources.
-Sample 3.
   

-------------------------------------------------------------------------------
*/


-------------------------------------------------------------------------------
--Sample Query 1
WITH cmrg AS
(
  SELECT DISTINCT(urd.mr_number), rep.no_upt, 
         EXTRACT(MONTH FROM urd.transfer_date) AS xfer_month,
		 EXTRACT(YEAR FROM urd.transfer_date) AS xfer_year
  FROM cqi.upt_r_daily urd
    LEFT JOIN 
     (
      SELECT e.mr_number, COUNT(e.mr_number) AS no_upt
      FROM schemac.view_upt_encounters e
        LEFT JOIN schemas.d_date_f_y dfy ON e.transfer_date =  dfy.date_actual
      WHERE planned_transfer = false AND dfy.months_ago > 0
      GROUP BY mr_number
     ) AS rep ON  urd.mr_number = rep.mr_number
WHERE planned_transfer = false
)
SELECT cmrg.no_upt AS no_past_upt, COUNT(cmrg.mr_number) AS no_patients, 
       SUM(no_patients) OVER (PARTITION BY first_day_of_month) AS tot_patients,
       ((no_patients * 1.0) / (tot_patients * 1.0) *100) AS perc_patients,
       cmrg.xfer_year, xfer_month, rdman.fac_fiscal_year, rdman.fac_fiscal_period, 
	   rdman.fac_current_fy_period, rdman.months_ago, rdman.fiscal_year_ago, 
	   rdman.first_day_of_month       
FROM cmrg
  LEFT JOIN schemar.ref_date_monthly_ago_new rdman ON cmrg.xfer_year = rdman.year_actual 
	AND cmrg.xfer_month = rdman.month_actual
GROUP BY cmrg.no_upt, cmrg.xfer_year, cmrg.xfer_month, rdman.fac_fiscal_year, 
         rdman.fac_fiscal_period, rdman.months_ago, rdman.fiscal_year_ago, 
		 rdman.first_day_of_month, rdman.fac_current_fy_period 
ORDER BY cmrg.xfer_year DESC, cmrg.xfer_month DESC, cmrg.no_upt;






-------------------------------------------------------------------------------
----Sample Query 2
SELECT transfer_month, transfer_year, 
       COUNT(uv.encntr_id) as vad_upt_tot,
       SUM(CASE WHEN uv.loc_floor = '11' THEN 1 ELSE 0 END) AS vad_upt_f11,
       SUM(CASE WHEN uv.loc_floor = '22' THEN 1 ELSE 0 END) AS vad_upt_f22,
       SUM(CASE WHEN uv.loc_floor = '33' THEN 1 ELSE 0 END) AS vad_upt_f33,
       SUM(CASE WHEN uv.loc_floor = '44' THEN 1 ELSE 0 END) AS vad_upt_f44,
       SUM(CASE WHEN uv.loc_floor = '55' THEN 1 ELSE 0 END) AS vad_upt_f55,
       SUM(CASE WHEN uv.loc_floor = '66' THEN 1 ELSE 0 END) AS vad_upt_f66,
       SUM(CASE WHEN uv.loc_floor = '77' THEN 1 ELSE 0 END) AS vad_upt_f77,
       SUM(CASE WHEN uv.loc_floor = '88' THEN 1 ELSE 0 END) AS vad_upt_f88
FROM
    (
    SELECT loav.encntr_id, 
      CASE
        WHEN loav.leave_dt_tm >= ds1.mar_utc_dschange AND loav.leave_dt_tm < ds1.nov_utc_dschange THEN loav.leave_dt_tm - '05:00:00'
        ELSE loav.leave_dt_tm - '06:00:00'
      END AS leave_dt_tm_ct, 
      EXTRACT(MONTH FROM leave_dt_tm_ct) AS transfer_month, 
      EXTRACT(YEAR FROM leave_dt_tm_ct) AS transfer_year, 
      SUBSTRING(cv.display, '^[0-9]*') AS loc_floor
    FROM
        (
        SELECT encntr_id, leave_dt_tm, MIN(return_dt_tm) AS return_dt_tm
        FROM schemacc.encntr_leave_history elh
        WHERE elh.encntr_id IN 
              (
              SELECT DISTINCT(ie2.encntr_id)
              FROM schemacc.diagnosis d
                LEFT JOIN schemar.inpatient_encntr_2 ie2 ON d.person_id = ie2.person_id
              WHERE d.nomenclature_id = 2184.... AND d.diag_type_cd IN (89,93) AND d.active_ind = 1 AND ie2.inpatient_admit_dt_tm_ct > '2016-01-01' AND loc_floor >=11
              ORDER BY encntr_id
              )
          AND elh.leave_type_cd = 669... AND elh.active_ind = 1
        GROUP BY encntr_id, leave_dt_tm
        ) AS loav
    LEFT JOIN schemacc.encounter e ON loav.encntr_id = e.encntr_id
    LEFT JOIN schemacc.code_value cv ON cv.code_value = e.loc_nurse_unit_cd
    LEFT JOIN schemar.daylight_savings ds1 ON ds1.year_ds = EXTRACT(YEAR FROM loav.leave_dt_tm AT TIME ZONE 'UTC')
    ) uv
GROUP BY transfer_year, transfer_month
ORDER BY transfer_year DESC, transfer_month DESC;




-------------------------------------------------------------------------------
----Sample View 3
/*This last entry is one (of many) views in the KPI Pipeline in the presentation.
For this we pull and aggregate encounter level data to monthly and fiscal figures 
across several measures and from different sources. 

Please Note: Please note elements of this SQL might be removed for privacy reasons
and replaced with ... or different names.
*/



CREATE OR REPLACE VIEW vw_monthly_counts_adm_admit_fac
(
  adm_year,
  adm_month,
  total_adm,
  medserv_1,
  medserv_2,
  medserv_3,
  medserv_4,
  medserv_5,
  medserv_6,
  medserv_unknown,
  floor_11,
  floor_22,
  floor_33,
  floor_44,
  floor_55,
  floor_66,
  floor_77,
  floor_88,
  floor_unknown,
  refer_fac_yellow,
  refer_fac_blue,
  refer_fac_red,
  refer_fac_orange,
  refer_fac_green,
  refer_fac_purple,
  refer_fac_grey,
  refer_fac_pink,
  refer_fac_indigo,
  refer_fac_cyan,
  refer_fac_golden,
  refer_fac_other  
)
AS 
 WITH admit_dashb AS 
		(
         SELECT ie14.encntr_id, ie14.inpatient_admit_dt_tm,
				EXTRACT(MONTH FROM ie14.inpatient_admit_dt_tm) AS adm_month,
				EXTRACT(YEAR FROM ie14.inpatient_admit_dt_tm) AS adm_year, 				
				CASE
                    WHEN mg.fac_inno_grp = 'Condition_1' THEN 1
                    ELSE 0
                END AS medserv_1, 
                CASE
                    WHEN mg.fac_inno_grp = 'Condition_2' THEN 1
                    ELSE 0
                END AS medserv_2, 
                CASE
                    WHEN mg.fac_inno_grp = 'Condition_3' THEN 1
                    ELSE 0
                END AS medserv_3, 
                CASE
                    WHEN mg.fac_inno_grp = 'Condition_4' THEN 1
                    ELSE 0
                END AS medserv_4, 
                CASE
                    WHEN mg.fac_inno_grp = 'Condition_5' THEN 1
                    ELSE 0
                END AS medserv_5, 
                CASE
                    WHEN mg.fac_inno_grp = 'Condition_6' THEN 1
                    ELSE 0
                END AS medserv_6,
				CASE
                    WHEN mg.fac_inno_grp = 'Condition_c' THEN 1
                    ELSE 0
                END AS medserv_c,
                CASE
                    WHEN mg.fac_inno_grp = '' OR mg.fac_inno_grp IS NULL THEN 1
                    ELSE 0
                END AS medserv_unknown,
                CASE
                    WHEN ie14.loc_floor = 11 THEN 1
                    ELSE 0
                END AS flr_11, 
                CASE
                    WHEN ie14.loc_floor = 22 THEN 1
                    ELSE 0
                END AS flr_22, 
                CASE
                    WHEN ie14.loc_floor = 33 THEN 1
                    ELSE 0
                END AS flr_33, 
                CASE
                    WHEN ie14.loc_floor = 44 THEN 1
                    ELSE 0
                END AS flr_44, 
                CASE
                    WHEN ie14.loc_floor = 55 THEN 1
                    ELSE 0
                END AS flr_55, 
                CASE
                    WHEN ie14.loc_floor = 66 THEN 1
                    ELSE 0
                END AS flr_66, 
                CASE
                    WHEN ie14.loc_floor = 77 THEN 1
                    ELSE 0
                END AS flr_77, 
                CASE
                    WHEN ie14.loc_floor = 88 THEN 1
                    ELSE 0
                END AS flr_88, 
                CASE
					WHEN ie14.loc_floor NOT IN ('11', '22', '33', '44', '55', '66', '77', '88')
						 OR ie14.loc_floor IS NULL
						 OR ie14.loc_floor = '' THEN 1
					ELSE 0
				END AS floor_unknown,
                CASE  
					WHEN ie14.org_id = 690... THEN 1
                    ELSE 0
                END AS refer_fac_yellow, 
                CASE 
					WHEN ie14.org_id = 690... THEN 1
                    ELSE 0
                END AS refer_fac_blue, 
                CASE 
                    WHEN ie14.org_id = 1270... THEN 1
                    ELSE 0
                END AS refer_fac_red, 
                CASE 
                    WHEN ie14.org_id = 715... THEN 1
                    ELSE 0
                END AS refer_fac_orange, 
                CASE 
                    WHEN ie14.org_id = 665... THEN 1
                    ELSE 0
                END AS refer_fac_green, 
                CASE 
                    WHEN ie14.org_id = 693... THEN 1
                    ELSE 0
                END AS refer_fac_purple, 
                CASE 
                    WHEN ie14.org_id = 5749... THEN 1
                    ELSE 0
                END AS refer_fac_grey, 
                CASE 
                    WHEN ie14.org_id = 691... THEN 1
                    ELSE 0
                END AS refer_fac_pink, 
                CASE 
                    WHEN ie14.org_id = 691... THEN 1
                    ELSE 0
                END AS refer_fac_indigo, 
                CASE 
                    WHEN ie14.org_id = 691... THEN 1
                    ELSE 0
                END AS refer_fac_cyan, 
                CASE 
                    WHEN ie14.org_id = 691... THEN 1
                    ELSE 0
                END AS refer_fac_golden, 
                CASE 
                    WHEN (ie14.org_id NOT IN (690..., 690..., 1270..., 715..., 665..., 693...,
											 5749..., 691..., 691..., 691..., 691...)) OR ie14.org_id IS NULL THEN 1
                    ELSE 0
                END AS refer_fac_other
		   FROM db.schemau.vw_dart_inpatient_encntr_2014 ie14
		   LEFT JOIN db.schemarf.medserv_grp mg ON ie14.med_service_cd = mg.med_service_cd
        )
 SELECT admit_dashb.adm_year, admit_dashb.adm_month,
		COUNT(DISTINCT admit_dashb.encntr_id) AS total_adm,
		SUM(admit_dashb.medserv_1) + ((SUM(admit_dashb.medserv_c)) *.5)  AS medserv_1, 
		SUM(admit_dashb.medserv_2) AS medserv_2, 
		SUM(admit_dashb.medserv_3) + ((SUM(admit_dashb.medserv_c)) *.5) AS medserv_3, 
		SUM(admit_dashb.medserv_4) AS medserv_4, 
		SUM(admit_dashb.medserv_5) AS medserv_5, 
		SUM(admit_dashb.medserv_6) AS medserv_6,
        SUM(admit_dashb.medserv_unknown) AS medserv_unknown,
		SUM(admit_dashb.flr_11) AS floor_11, SUM(admit_dashb.flr_22) AS floor_22, 
		SUM(admit_dashb.flr_33) AS floor_33, SUM(admit_dashb.flr_44) AS floor_44, 
		SUM(admit_dashb.flr_55) AS floor_55, SUM(admit_dashb.flr_66) AS floor_66, 
		SUM(admit_dashb.flr_77) AS floor_77, SUM(admit_dashb.flr_88) AS floor_88,
        SUM(admit_dashb.floor_unknown) AS floor_unknown,
		SUM(admit_dashb.refer_fac_yellow) AS refer_fac_yellow, sum(admit_dashb.refer_fac_blue) AS refer_fac_blue, 
		SUM(admit_dashb.refer_fac_red) AS refer_fac_red, sum(admit_dashb.refer_fac_orange) AS refer_fac_orange, 
		SUM(admit_dashb.refer_fac_green) AS refer_fac_green, sum(admit_dashb.refer_fac_purple) AS refer_fac_purple, 
		SUM(admit_dashb.refer_fac_grey) AS refer_fac_grey, sum(admit_dashb.refer_fac_pink) AS refer_fac_pink, 
		SUM(admit_dashb.refer_fac_indigo) AS refer_fac_indigo, sum(admit_dashb.refer_fac_cyan) AS refer_fac_cyan, 
		SUM(admit_dashb.refer_fac_golden) AS refer_fac_golden, sum(admit_dashb.refer_fac_other) AS refer_fac_other 		
 FROM admit_dashb
 GROUP BY admit_dashb.adm_year, admit_dashb.adm_month; 

