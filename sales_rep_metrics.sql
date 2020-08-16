CREATE OR REPLACE VIEW reporting.vw_sales_metrics
AS WITH offers AS (
         SELECT o_1.app_id,
            max(o_1.loan_amount) AS offer_amt
           FROM data.offer o_1
          GROUP BY o_1.app_id
        ),
        /* getting max offer */
         tier AS (
         SELECT vw_offers.app_id,
            vw_offers.product_tier
           FROM offers
          WHERE offers.rnk_lastoffer = 1
        ),
        /* getting product tier for most recent offer */
         data_ AS (
         SELECT a_1.id AS app_id,
                CASE
                    WHEN a_1.status::text = ANY (ARRAY['Sold'::character varying::text, 'Accept'::character varying::text, 'Offered'::character varying::text, 'Did not sell'::character varying::text, 'Customer Reject'::character varying::text, 'Close Reject'::character varying::text]) THEN 1
                    ELSE 0
                END AS approval_count,
            f.id AS loan_id,
            f.loan_amount AS loan_amt,
            f.amt_owed,
            s.rep_name,
            a_1.outsourced_group,
            a_1.app_date,
            o.offer_amt,
                CASE
                    WHEN f.loan_amount IS NULL THEN NULL::text
                    WHEN f.loan_amount < 100000 THEN '0-99K'::text
                    WHEN f.loan_amount >= 100000 AND f.loan_amount < 200000 THEN '100-199K'::text
                    WHEN f.loan_amount >= 200000 AND f.loan_amount < 350000 THEN '200-349K'::text
                    WHEN f.loan_amount >= 350000 THEN '350K+'::text
                    ELSE NULL::text
                END AS loanbins,
                CASE
                    WHEN f.loan_amount IS NULL THEN NULL::integer
                    WHEN f.loan_amount < 100000 THEN 1
                    WHEN f.loan_amount >= 100000 AND f.loan_amount < 200000 THEN 2
                    WHEN f.loan_amount >= 200000 AND f.loan_amount < 350000 THEN 3
                    WHEN f.loan_amount >= 350000 THEN 4
                    ELSE NULL::integer
                END AS loanrnk,
                CASE
                    WHEN o.offer_amt IS NULL THEN NULL::text
                    WHEN o.offer_amt < 100000::numeric THEN '0-99K'::text
                    WHEN o.offer_amt >= 100000::numeric AND o.offer_amt < 200000::numeric THEN '100-199K'::text
                    WHEN o.offer_amt >= 200000::numeric AND o.offer_amt < 350000::numeric THEN '200-349K'::text
                    WHEN o.offer_amt >= 350000::numeric THEN '350K+'::text
                    ELSE NULL::text
                END AS offerbins,
                CASE
                    WHEN o.offer_amt IS NULL THEN NULL::integer
                    WHEN o.offer_amt < 100000::numeric THEN 1
                    WHEN o.offer_amt >= 100000::numeric AND o.offer_amt < 200000::numeric THEN 2
                    WHEN o.offer_amt >= 200000::numeric AND o.offer_amt < 350000::numeric THEN 3
                    WHEN o.offer_amt >= 350000::numeric THEN 4
                    ELSE NULL::integer
                END AS offerrnk,
            initcap(amt_owedim(replace(replace(replace(replace(btrim("substring"(lower(t.product_tier::text), '[^\d]+'::text)), ' - '::text, '-'::text), ' -'::text, ''::text), 'tier'::text, ''::text), '-'::text, ' '::text))) AS standardized_product_tier,
            /* getting rid of junk in string for product tier */
                CASE
                    WHEN a_1.process_stage::text = ANY (ARRAY['Underwritten'::character varying::text, 'Deal Made'::character varying::text, 'Loan'::character varying::text, 'Offered'::character varying::text]) THEN 1
                    ELSE 0
                END AS underwritten
           FROM data.apps a_1
             LEFT JOIN data.loans f ON a_1.id = f.app_id
             LEFT JOIN data.sales_representatives s ON a_1.sales_representatives_id = s.id
             LEFT JOIN offers o ON a_1.id = o.app_id
             LEFT JOIN tier t ON a_1.id = t.app_id
          WHERE s.rep_type::text = 'Outsouced'::text
        ), last90 AS (
         SELECT data_.app_id,
            data_.approval_count,
            data_.loan_id,
            data_.loan_amt,
            data_.amt_owed,
            data_.rep_name,
            data_.outsourced_group,
            data_.app_date,
            data_.offer_amt,
            data_.loanbins,
            data_.loanrnk,
            data_.offerbins,
            data_.offerrnk,
            data_.standardized_product_tier,
            data_.underwritten
           FROM data_
          WHERE data_.app_date >= (CURRENT_DATE - '90 days'::interval day)
        ), top10loanapp AS (
         SELECT last90.rep_name,
            1 AS top10fbya,
            sum(last90.loan_amt) / count(last90.app_id)
           FROM last90
          GROUP BY last90.rep_name
         HAVING (sum(last90.loan_amt) / count(last90.app_id)) IS NOT NULL
          ORDER BY (sum(last90.loan_amt) / count(last90.app_id)) DESC
         LIMIT 10
        ),
        /* top ten reps by loan amount in last 90 days */
        top10avgloan AS (
         SELECT last90.rep_name,
            1 AS top10famt,
            avg(last90.loan_amt) AS avg
           FROM last90
          GROUP BY last90.rep_name
         HAVING avg(last90.loan_amt) IS NOT NULL
          ORDER BY (avg(last90.loan_amt)) DESC
         LIMIT 10
       ),
       /* top 10 reps by avg loan amt */
         top10apct AS (
         SELECT last90.rep_name,
            1 AS top10aprvpct,
            round(sum(last90.approval_count)::numeric / count(last90.app_id)::numeric, 2) AS round
           FROM last90
          GROUP BY last90.rep_name
         HAVING round(sum(last90.approval_count)::numeric / count(last90.app_id)::numeric, 2) IS NOT NULL
          ORDER BY (round(sum(last90.approval_count)::numeric / count(last90.app_id)::numeric, 2)) DESC
         LIMIT 10
        ),
        /* top 10 reps by # approvals */
        top10apps AS (
         SELECT last90.rep_name,
            1 AS top10app,
            count(last90.app_id) AS count
           FROM last90
          GROUP BY last90.rep_name
         HAVING count(last90.app_id) IS NOT NULL
          ORDER BY (count(last90.app_id)) DESC
         LIMIT 10
        )
        /* top 10 reps by # apps */
 SELECT d.app_id,
    d.approval_count,
    d.loan_id,
    d.loan_amt,
    d.amt_owed,
    d.rep_name,
    d.outsourced_group,
    d.app_date,
    d.offer_amt,
    d.loanbins,
    d.loanrnk,
    d.offerbins,
    d.offerrnk,
    d.standardized_product_tier,
    d.underwritten,
    fa.top10fbya,
    af.top10famt,
    ap.top10aprvpct,
    a.top10app
   FROM data_ d
     LEFT JOIN top10loanapp fa ON d.rep_name::text = fa.rep_name::text
     LEFT JOIN top10avgloan af ON d.rep_name::text = af.rep_name::text
     LEFT JOIN top10apct ap ON d.rep_name::text = ap.rep_name::text
     LEFT JOIN top10apps a ON d.rep_name::text = a.rep_name::text;
