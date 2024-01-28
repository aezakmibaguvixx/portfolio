
With Spis_INN as(
select column_value as INN from
table(sys.ODCIVARCHAR2LIST(список инн),

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
sum(nvl(p011600, 0))                                "14.float" --сальдо енс


    
from NP join ens.ens 
  on np.fid = ens.ens.tp_fid
join IR_BC.IRRSB_TP IRRSB_TP
                    ON IRRSB_TP.np_fid  = NP.fid 
              join IR_BC.IRRSB_DOCS IRRSB_DOCS
                    ON IRRSB_TP.id = IRRSB_DOCS.IRRSB_TP_ID
                    and IRRSB_TP.PERIOD_DATE = to_date('01/12/2023', 'dd/mm/yyyy')  
                    
               group by np.fid,np.inn
