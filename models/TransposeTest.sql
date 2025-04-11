WITH DUMMY_JSON_DATA_NEW AS (

  SELECT * 
  
  FROM {{ source('PROPHECY_EXP.PUBLIC', 'DUMMY_JSON_DATA_NEW') }}

),

SALES_DATA1 AS (

  SELECT * 
  
  FROM {{ source('PROPHECY_EXP.PUBLIC', 'SALES_DATA') }}

),

sales_cost_transpose AS (

  {{
    SnowflakeSqlBasics.Transpose(
      '', 
      ['ID', 'COUNTRY', 'SALES', 'COST'], 
      ['ID', 'COUNTRY'], 
      ['SALES', 'COST']
    )
  }}

),

Transpose_1 AS (

  {{ SnowflakeSqlBasics.Transpose('', ['ID', 'COUNTRY', 'SALES', 'COST'], [], []) }}

),

XML_TEST AS (

  SELECT * 
  
  FROM {{ source('PROPHECY_EXP.PROPHECY_EXP_SCHEMA', 'XML_TEST') }}

),

json_data_parsing AS (

  {{ SnowflakeSqlBasics.JSONParse('DUMMY_JSON_DATA_NEW', ['JSON_DATA']) }}

),

xml_parsing AS (

  {{ SnowflakeSqlBasics.XMLParse('XML_TEST', ['XML_VALUE']) }}

)

SELECT *

FROM sales_cost_transpose
