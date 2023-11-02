select l_discount
from {{ref('stg_lineitem')}}
where l_discount not between 0.00 and 1.00