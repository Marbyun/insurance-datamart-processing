DELETE_DATA = f"""
DELETE FROM"""

INSERT_DATA = f"""
with raw_contract as (
    select contract_id, product_id, start_date, end_date, premium, coverage, discount_rate, row_number() over (order by contract_id, start_date) id_temp
    from landing_insurance_contracts
)
, raw_claim as (
    select claim_id, contract_id, claim_amount, claim_date, row_number() over (order by contract_id,claim_date) id_temp
    from landing_claims
)
, get_combine as (select rct.id_temp,
                         rct.product_id,
                         rct.contract_id,
                         rcm.contract_id contract_in_claim,
                         rcm.claim_id,
                         rcm.claim_amount,
                         rct.coverage,
                         rct.discount_rate
                  from raw_contract rct
                           left join raw_claim rcm on rct.id_temp = rcm.id_temp
)
, get_claims as (select id_temp,
                        product_id,
                        claim_id,
                        coverage,
                        discount_rate,
                        sum(claim_amount) claims
                 from get_combine
                 group by id_temp, product_id,claim_id, coverage, discount_rate
)
, get_fcs as (select id_temp,
                     product_id,
                     claim_id,
                     coverage,
                     discount_rate,
                     claims,
                     (coverage - claims) feture_cash_flow
              from get_claims)
insert into datamart_assurance
select id_temp, product_id, claim_id, coverage, discount_rate, claims, feture_cash_flow, (feture_cash_flow * discount_rate) cms
from get_fcs
"""