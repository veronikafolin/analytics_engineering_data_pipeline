select discount
from {{ref('stg_lineitem')}}
where discount not between 0.00 and 1.00