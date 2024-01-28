With Spis_INN as(список инн),


NP as (Select distinct tu.ID_ENTITY,tu.inn,
tu.NAME_FULL,dh.PR_MSP from spis_inn join
(select WT_ENTITY_EGRN.ID_ENTITY,
       WT_UL.INN,
       WT_UL.NAME_FULL NAME_FULL
      
from TPACC$LST.WT_UL
   INNER JOIN TPACC$LST.WT_ENTITY_EGRN  ON ((WT_UL.ENTITY_EGRN_ID = WT_ENTITY_EGRN.ID))
  
union all
select ID_ENTITY, 
       INN,
       NAME_FULL

from TAX3TPACCOUNTING$I$DM.WT_IO
union all
select ID_ENTITY, 
       INN,
       SUBSTR (
          TRIM (LAST_NAME || ' ' || FIRST_NAME || ' ' || SECOND_NAME),
          1,
          255) NAME_FULL
from TAX3TPACCOUNTING$I$DM.WT_FL) tu on spis_inn.inn = tu.inn
              join CAM$PFR.KOL_ZL_DASHBOARD dh on dh.inn = tu.inn
              
where YEAR_ = 2023 and PR_MSP = 1 
)

    
                   
select /*+ PARALLEL(32)*/  
       np.ID_ENTITY,
     np.inn "inn",
     np.NAME_FULL,
       np.PR_MSP
from np

         
     
