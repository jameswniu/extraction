with cte1 as ( --*add new user notes, then select relevant records
	select	
		cust_id
		, pat_acct
		, insurance_name
		, claim_num
		, pm_sk 
		, charges
		, code
		, note
		, created_at
	from
		tpl_claim_responses
	where 
		(response_from is null or response_from != 'MX_E_OBCAL') and (
			(code = '277-F4' and note ~* 'A7:35')
			or (code = '277-F4' and note ~* 'A7:97:PR - patient eligibility not found with the payer')
			or (code = '277-F4' and note = 'missing Claim Number')
			or (code = '277-F4' and note = 'patient cannot be identified as our insured')
			or (code = '835-N587' and note = 'lifetime benefit maximum has been reached for this service')
			or (code = '835-N650' and note = 'incomplete/invalid documentation')
			or (code = '277-P1' and note = 'pending')
			or (code = '835-B13' and note ~* 'payment of')
			or (code = '277-A7' and note ~* 'please contact payer for more info')
			or (code = '277-A6' and note ~* 'A6:233 - missing Hospital discharge hour')
			or (code = '277-A7' and note ~* 'A7:123:85 - need more info from billing provider')
			or (code = '277-A7' and note ~* 'A7:0 - please contact payer for more info')
			or (code = '277-A7' and note ~* 'A7:578 - bad insurance type code')
			or (code = '277-A6' and note ~* 'A6:297 - missing or invalid supporting notes/medical records') 
		)
		and cust_id = 483
)
, cte2 as ( --find inst or prof info
	select 
		pm_sk
		, claim_type fp_tp
		, case when content->>'claim_type' ~ 'I' then 'Inst' else 'Prof' end inst_prof 
	from 
		tpl_pre_billing_records 
	where 
		cust_id = 483
		and exists (select 1 from cte1 where pm_sk = tpl_pre_billing_records.pm_sk)
)
, cte3 as ( --merge inst or prof info
	select
		cte1.cust_id
		, cte1.pat_acct
		, cte1.insurance_name
		, cte1.claim_num
		, cte1.pm_sk
		, cte2.inst_prof
		, cte2.fp_tp
		, cte1.charges claim_charge
		, cte1.code x12_response
		, cte1.note user_note
		, cte1.created_at
	from 
		cte1
	left join
		cte2 on cte1.pm_sk = cte2.pm_sk
)
, cte4 as ( --group by claim
	select
		cust_id
		, pat_acct
		, insurance_name
		, string_agg(distinct claim_num::text, ';  ') claim_num
		, string_agg(distinct pm_sk::text, '; ') pm_sk
		, string_agg(distinct inst_prof, '; ') inst_prof
		, string_agg(distinct fp_tp, '; ') fp_tp
		, round(max(claim_charge)) claim_charge
		, string_agg(distinct x12_response, ';  ') x12_response
		, string_agg(distinct user_note, ';  ') user_note
		, min(created_at)::date created_at
	from
		cte3
	where 
		created_at in (select max(created_at) from cte1 group by cust_id, pat_acct, insurance_name)
	group by
		cust_id
		, pat_acct
		, insurance_name
)
, cte5 as ( --merge cust info
select 
	cte4.created_at created_at_tab
	, tpl_cust_infos.master_id
	, cte4.cust_id
	, tpl_cust_infos.cust_name
	, cte4.pat_acct
	, cte4.insurance_name
	, cte4.claim_num
	, cte4.pm_sk
	, cte4.inst_prof
	, cte4.fp_tp
	, cte4.claim_charge
	, cte4.x12_response
	, cte4.user_note
from 
	cte4
left join tpl_cust_infos on
	cte4.cust_id = tpl_cust_infos.cust_id
)
, cte6 as (
select 
	cust_id 
	, pat_acct
	, charges
	, trans_amt
	, trans_date
	, trans_id
	, insurance_name
	, created_at
from 
	tpl_mva_trans
where 
	cust_id = 483 
	and duplicate_payment = false 
)
, cte7 as (
select
	cust_id
	, pat_acct
	, round(max(charges)) charges
	, round(sum(trans_amt)) tot_trans_amt
	, min(trans_date) earliest_cust_trans
	, max(trans_date) latest_cust_trans
	, string_agg(concat(insurance_name, ' -- ', '$'||to_char(round(trans_amt), '999G999')), ';  ') insurance_name
from 
	cte6
group by
	cust_id
	, pat_acct
)
-->
select
	foo1.cust_id 
	, foo1.pat_acct
	, foo1.charges
	, foo1.x12_response
	, substring(foo1.user_note,'(?<=\().*[^)]') claim_adj_seg
	, round(substring(foo1.user_note,'(?<=# )(\d{1,30})')::numeric) jopari_check_num
	, round(substring(foo1.user_note,'(?<=\$)[^,]*')::numeric) jopari_paymt
	, foo2.tot_trans_amt
	, case when round(substring(foo1.user_note,'(?<=\$)[^,]*')::numeric) <= foo2.tot_trans_amt then 'Y' else 'N' end trans_meet_payment
	, foo1.created_at_tab latest_jopari_response
	, foo2.latest_cust_trans
	, case when foo2.latest_cust_trans > foo1.created_at_tab then 'Y' else 'N' end trans_after_response
	, foo1.insurance_name jopari_carrier 
	, foo2.insurance_name trans_carrier
from (
	select 
		created_at_tab
		, cust_id
		, pat_acct
		, insurance_name
		, claim_charge charges
		, x12_response 
		, user_note
	from cte5 where x12_response ~* '835-B13'
	) foo1
left join (
	select 
		cust_id
		, pat_acct
		, charges
		, tot_trans_amt
		, earliest_cust_trans
		, latest_cust_trans
		, insurance_name
	from 
		cte7
	) foo2 on
		foo1.cust_id = foo2.cust_id and foo1.pat_acct = foo2.pat_acct
order by
	latest_jopari_response desc

