
With Spis_INN as(список инн),

NP as (Select /*+ PARALLEL(16) */ 
                   ff.fid,              
                   ff.INN ,
                   NAME_FULL,
           type_np,
           no_mri_code
                 
            from 
                   (select WT_ENTITY_EGRN.ID_ENTITY fid,       
                           WT_UL.INN INN,
                           nvl(NAME_SHORT, NAME_FULL) NAME_FULL,
               '1' type_np,
               no_mri_code
                    from TPACC$LST.WT_UL JOIN TPACC$LST.WT_ENTITY_EGRN  
                                         ON WT_UL.ENTITY_EGRN_ID = WT_ENTITY_EGRN.ID 
                                          LEFT JOIN TPACC$EGRN.WT_UCHET_OBJ_UL  ON ((WT_ENTITY_EGRN.ID = WT_UCHET_OBJ_UL.ENTITY_EGRN_ID) AND (WT_UCHET_OBJ_UL.ACTUAL_RECORD_FOR_UCHET_OBJ_UL = 1) AND (WT_UCHET_OBJ_UL.R$SYS_OBJ_TYPE_ID = 1) AND (WT_UCHET_OBJ_UL.ID > 0) AND (WT_UCHET_OBJ_UL.DATE_INVALIDATION is null))                                                   
              union all
                    select WT_IO.ID_ENTITY,        
                           WT_IO.INN,
                           nvl(NAME_SHORT, NAME_FULL),
               '3' type_np,
               null
                    from TAX3TPACCOUNTING$I$DM.WT_IO                      
              union all
                    select WT_FL.ID_ENTITY,        
                           WT_FL.INN,
                           LAST_NAME || ' ' || FIRST_NAME || ' ' || SECOND_NAME,
               '2' type_np,
               no_mri_code
              from TAX3TPACCOUNTING$I$DM.WT_FL ) ff 
                   join Spis_INN 
                        ON Spis_INN.inn = ff.inn        
            )
            
            
 select sum1, sum2, sum3, inn 
 from np join I$CAM.V$NBO_1152017_FMS fm on np.fid = fm.fid 
 where inn = '0104013318'
