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
select 
np.inn "inn",
s_ens                                "saldo" --сальдо енс

--DBS$ENS_MON.OPER_SUSP_DCN

    
from NP join ens.ens 
  on np.fid = ens.ens.tp_fid

                    
               where s_ens < 0
