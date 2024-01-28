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
            ),

dec_1151020 as (
          select inn inn,
                 max(ID_DOCUMENT) ID_DOCUMENT 
          from CAM$NDFL.V$NBO join NP ON NP.fid = V$NBO.ID_TAXPAYER
          group by inn ),
          
row_page as (select  inn, 
                    max(nvl(T460_PART_2.id, T470_PART_2.id)) doc
             from dec_1151020 left join CAM$DATA.T460_PART_2
                                        ON T460_PART_2.ID_DOCUMENT = dec_1151020.ID_DOCUMENT
                              left join CAM$DATA.T470_PART_2
                                        ON T470_PART_2.ID_DOCUMENT = dec_1151020.ID_DOCUMENT 
             group by inn                           
                                        )        

select  /*+ PARALLEL(4) */ 
        declar.inn,  
        sum(nvl(b1.P376, b2.P376)) +  
        sum(nvl(b1.P370, b2.P370))   col_5
        
 from row_page declar
      left join (select P376, P370, ID_PAGE
            from CAM$DATA.T460_PART_2_T363 where id_nsi_doc_value_type=1 --group by id_document
            ) b1 
                                        on declar.doc=b1.ID_PAGE 
      left join (select P376, P370, ID_PAGE                      
            from CAM$DATA.T470_PART_2_T363 where id_nsi_doc_value_type=1 --group by id_document
            ) b2                                      
                 on declar.doc=b2.ID_PAGE 
group by declar.inn
