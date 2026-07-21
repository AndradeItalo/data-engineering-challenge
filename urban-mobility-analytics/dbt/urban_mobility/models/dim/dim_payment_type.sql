select
    payment_type,
    payment_description,
    is_valid_payment
from {{ source('bronze', 'payment_type_reference') }}

union all

-- payment_type fora do dicionário (ex: 0) cai aqui, sem quebrar o fk da fato
select
    -1 as payment_type,
    'Unknown' as payment_description,
    false as is_valid_payment