
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
select distinct np.fid,
np.inn "inn",dcn.action_date 

 

    
from NP join I$DBS.V$ENS_DDC_LINE dbs on np.fid = dbs.tp_fid
     join DBS$ENS_DDC.DDC_DCN_line dcn on dbs.dcn_line_id = dcn.id
     --where np.inn = '6937003516' 
--ABDULLIN.BAUM_USI_ISK_NO
--ABDULLIN.BAUM_USI_ISK_NP
--where claim_reason = 'Оспаривание ненормативного правового акта (ННПА)'
