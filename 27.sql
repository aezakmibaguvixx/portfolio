
With Spis_INN as(список инн),

NP as (Select /*+ PARALLEL(16) */ 
                   ff.fid,              
                   ff.INN
                             
            from 
                   (select WT_ENTITY_EGRN.ID_ENTITY fid,       
                           WT_UL.INN INN
                           
               
                    from TPACC$LST.WT_UL JOIN TPACC$LST.WT_ENTITY_EGRN  
                                         ON WT_UL.ENTITY_EGRN_ID = WT_ENTITY_EGRN.ID                                                    
              union all
                    select WT_IO.ID_ENTITY,        
                           WT_IO.INN
                          
             
                    from TAX3TPACCOUNTING$I$DM.WT_IO                      
              union all
                    select WT_FL.ID_ENTITY,        
                           WT_FL.INN
                           
             
              from TAX3TPACCOUNTING$I$DM.WT_FL ) ff 
                   join Spis_INN 
                        ON Spis_INN.inn = ff.inn        
            )
select np.fid,
np.inn "inn",
claim_reason,
lawsuit_date 

    
from NP join ABDULLIN.BAUM_USI_ISK_NP abd on np.inn = abd.inn
--ABDULLIN.BAUM_USI_ISK_NO
--ABDULLIN.BAUM_USI_ISK_NP
where claim_reason = 'Оспаривание ненормативного правового акта (ННПА)'
