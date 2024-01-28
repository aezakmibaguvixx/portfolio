With Spis_INN as(список инн)

select inn,
       col_30
from(
SELECT LK_DDC_CLAIM.inn,
       case when PROMISE_SIGN = 0 then 'Нет' 
            when PROMISE_SIGN = 1 then 'Да' end col_30,
       ROW_NUMBER() OVER(PARTITION BY LK_DDC_CLAIM.Inn ORDER BY ID DESC) RN       
FROM DBS$ENS_DDC.LK_DDC_CLAIM join Spis_INN ON Spis_INN.inn = LK_DDC_CLAIM.inn
)
where RN = 1
