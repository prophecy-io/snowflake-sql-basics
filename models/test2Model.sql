WITH CUSTOMER AS (

  SELECT * 
  
  FROM {{ source('PROPHECY_EXP.PROPHECY_EXP_SCHEMA', 'CUSTOMER') }}

),

TextToColumns_1 AS (

  {#Transforms customer data into a structured format for better analysis.#}
  {{ SnowflakeSqlBasics.TextToColumns('CUSTOMER') }}

)

SELECT *

FROM TextToColumns_1
