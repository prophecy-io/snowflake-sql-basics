WITH CUSTOMERSSTT AS (

  SELECT * 
  
  FROM {{ source('PROPHECY_EXP.PROPHECY_EXP_SCHEMA', 'CUSTOMER') }}

),

TextToColumns_1 AS (

  {{ SnowflakeSqlBasics.TextToColumns('orders', in0) }}

)

SELECT *

FROM TextToColumns_1
