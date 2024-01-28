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
            ),

crim as (select  EGRN_TP_ID FID, CDD.DOC_DATE, owner_unit_code 
        FROM SPB.CRIM_CLAIM CC
               join (select /*+ parallel(16)*/ distinct 
                        claim_ID,
                        DOC_STATUS,
                        DOC_DATE
                   from SPB.CRIM_DECISION CD 
                   where DOC_DATE in (select max(DOC_DATE) from SPB.CRIM_DECISION where CLAIM_ID=cd.CLAIM_ID)
                   ) CDD
                                 ON CC.id=CDD.claim_ID
                   and DOC_BASE in ( 6, 7, 8)
                     and CC.DROP_DATE IS NULL
                     JOIN SPB.DEBTOR_FILE f on f.id = cc.file_id
            JOIN spb.taxpayer tp
                  ON tp.id = f.taxpayer_id  
                     and f.DROP_DATE IS NULL   )   
                  
            
select np.INN "inn", doc_date "26." ,owner_unit_code
              
from np left join crim  
          ON crim.fid = np.fid

