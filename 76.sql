With Spis_INN as(
select column_value as INN from
table(sys.ODCIVARCHAR2LIST(список инн),

NP as (Select /*+ PARALLEL(16) */ 
                   ff.fid,              
                   ff.INN,
                   NAME_FULL       
            from 
                   (select WT_ENTITY_EGRN.ID_ENTITY fid,       
                           WT_UL.INN INN,
                           nvl(NAME_SHORT, NAME_FULL) NAME_FULL
                    from TPACC$LST.WT_UL JOIN TPACC$LST.WT_ENTITY_EGRN  
                                         ON WT_UL.ENTITY_EGRN_ID = WT_ENTITY_EGRN.ID                                                    
              union all
                    select WT_IO.ID_ENTITY,        
                           WT_IO.INN,
                           nvl(NAME_SHORT, NAME_FULL)
                    from TAX3TPACCOUNTING$I$DM.WT_IO                      
              union all
                    select WT_FL.ID_ENTITY,        
                           WT_FL.INN,
                           LAST_NAME || ' ' || FIRST_NAME || ' ' || SECOND_NAME
              from TAX3TPACCOUNTING$I$DM.WT_FL ) ff 
                   join Spis_INN 
                        ON Spis_INN.inn = ff.inn        
            
)
select  distinct np.inn,
       mon.doc_date, 
       mon.version_date
        
       
from np join  DBS$ENS_MON.INCASSO_HST hst on hst.PAYER_INN = np.inn
join DBS$ENS_MON.OPER_SUSP_DCN mon on mon.wdrw_dcn_id  = hst.wdrw_dcn_id 


--OPER_SUSP_DCN_R
--"DBS$ENS_MON"."ISEQ$$_25140329".nextval
--"DBS$ENS_MON"."ISEQ$$_26706685".nextval
