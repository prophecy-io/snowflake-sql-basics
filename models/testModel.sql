WITH CUSTOMER AS (

  SELECT * 
  
  FROM {{ source('PROPHECY_EXP.PROPHECY_EXP_SCHEMA', 'CUSTOMER') }}

),

FuzzyMatch_1 AS (

  {#Enhances data quality by identifying similar customer records for potential merging.#}
  {{
    SnowflakeSqlBasics.FuzzyMatch(
      'CUSTOMER', 
      'MERGE', 
      'C_CUSTOMER_ID', 
      'C_CURRENT_CDEMO_SK', 
      { 'equals': ['C_CUSTOMER_ID'] }, 
      80, 
      true
    )
  }}

)

SELECT *

FROM FuzzyMatch_1
