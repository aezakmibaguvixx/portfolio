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
             
declar as (
       select /*+ parallel(32)*/           
           b.ID_TAXPAYER fid,
           max(ID_INC_DOCUMENT_ACTUAL) doc                                  
       from CAM$NBO.INC$DOCUMENT b JOIN CAM$NBO.INC$COMPLECT c
                              ON b.ID = c.ID_INC_DOCUMENT_ACTUAL 
       where b.id_nsi_document = 10 
             and b.ID_NSI_TAXPAYER_TYPE=1
            -- and c.year_ = 2022
           --  and b.CODE_PERIOD = '34'
           group by b.ID_TAXPAYER
             
          ),
          
declar_summa as (select  /*+ parallel(16)*/  
                         fid fid,
                         f1.S_2_100 dvasto, f2.S_4_010 cheture, f3.S_5_060 shest
                                                
                 from declar left join 
                      (select S_2_100, 
                              id_document                                   
                       from CAM$DATA.T1570_R2 where id_nsi_doc_value_type=1 
                                                    ) f1
                               on declar.doc = f1.id_document                                     
                             left join 
                      (select S_4_010, 
                              id_document                                   
                       from CAM$DATA.T1570_R4 where id_nsi_doc_value_type=1 
                                                    ) f2
                               on declar.doc = f2.id_document     
                             left join 
                      (select S_5_060, 
                              id_document                                   
                       from CAM$DATA.T1570_R5 where id_nsi_doc_value_type=1 
                                                    ) f3
                               on declar.doc = f3.id_document
                       
                               ) 
                               
select /*+ parallel(16)*/
       inn "inn",
       sum(nvl(dvasto, 0)) +
       sum(nvl(cheture, 0)) +
       sum(nvl(shest, 0)) 
from np left join declar_summa 
              ON declar_summa.fid = np.fid
               
group by inn              
      
