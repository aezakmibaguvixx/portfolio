With Spis_INN as(список инн)


select inn,
       col_32
from(
       SELECT DDC_DCN.TP_INN inn,
              case when PROCESSING_STATE = 5 then 'Положительное решение' 
                   when PROCESSING_STATE = 4 then 'Отрицательное решение'
                     else null
                        end col_32,
              ROW_NUMBER() OVER(PARTITION BY DDC_DCN.TP_INN ORDER BY ID DESC) RN        
FROM DBS$ENS_DDC.DDC_DCN join Spis_INN ON Spis_INN.inn = DDC_DCN.TP_INN
where DDC_TYPE_ID in (1, 2)
      )
where RN = 1
