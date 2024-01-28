With Spis_INN as(
select column_value as INN from
table(sys.ODCIVARCHAR2LIST(список инн),

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
            ),
            
                
dec_0710099 as ( select /*+ PARALLEL(4) */
                   Spis_INN.inn,
                   max(gg.doc) doc,
                   '0710099' knd,
                   YEAR_ 
            from  Spis_INN join 
                     (select VON.INN inn, 
                             YEAR_,          
                             VON.ID doc,
                             row_number() over (partition by VON.inn, YEAR_ ORDER BY CODE_NSI_SONO DESC) rn
                      from I$CAM.V$NBO_0710099_NDS2 VON                                   
                      where YEAR_ in (2022) --and INN = '1835058718'              
                                    ) gg
                       on gg.inn = Spis_INN.inn 
            where gg.rn = 1 
            group by Spis_INN.inn, YEAR_)
			
			
select  /*+ PARALLEL(4) */ 
        declar.inn,
        sum(coalesce(o2.S_2100_4, o3.S_2100_4, o7.S_2100_4))   col_34 --declar.year_, declar.knd,   
                
        
from dec_0710099 declar
--
-- 2100-2500 
   left join 
     (select S_2100_4, id_document                                   
            from CAM$DATA.T549_F2_OTCHET where id_nsi_doc_value_type=1 --group by id_document
            ) o2 
                               on declar.doc=o2.id_document      
    left join 
     (select S_2100_4, 
     id_document                                   
            from CAM$DATA.T585_F2_OTCHET where id_nsi_doc_value_type=1 --group by id_document
            ) o3 
                               on declar.doc=o3.id_document                  
    left join 
     (select S_2100_4,
      id_document                                   
            from CAM$DATA.T069_F2_OTCHET where id_nsi_doc_value_type=1 --group by id_document
            ) o7 
                               on declar.doc=o7.id_document                                                                           

group by declar.inn
   
