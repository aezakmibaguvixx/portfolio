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
                   max(gg.doc) doc
                  
            from  Spis_INN join 
                     (select VON.INN inn, 
                                       
                             VON.ID doc,
                             row_number() over (partition by VON.inn ORDER BY CODE_NSI_SONO DESC) rn
                      from I$CAM.V$NBO_0710099_NDS2 VON                                   
                      --where YEAR_ in (2022) --and INN = '1835058718'              
                                    ) gg
                       on gg.inn = Spis_INN.inn 
            where gg.rn = 1 
            group by Spis_INN.inn)
			
			
select  /*+ PARALLEL(4) */ 
        declar.inn, --declar.year_, declar.knd,   
      --  sum(coalesce(f2.S_1150_4, f3.S_1150_4, f4.S_1150_4))   col_4, 
      --  sum(coalesce(f2.S_1100_4, f3.S_1100_4, f4.S_1100_4))   col_5,
        
       -- sum(coalesce(g2.S_1210_4, g3.S_1210_4, g4.S_1210_4))   col_6, 
      --  sum(coalesce(g2.S_1230_4, g3.S_1230_4, g4.S_1230_4))   col_7, 
       -- sum(coalesce(g2.S_1230_5, g3.S_1230_5, g4.S_1230_5))   col_8,   
       -- sum(coalesce(g2.S_1240_4, g3.S_1240_4, g4.S_1240_4))   col_9,  
       -- sum(coalesce(g2.S_1250_4, g3.S_1250_4, g4.S_1250_4))   col_10, 
       -- sum(coalesce(g2.S_1200_4, g3.S_1200_4, g4.S_1200_4))   col_11,      
        
       -- SUM(coalesce(p10.S_1600_4, p11.S_1600_4, p2.S_1600_4, p3.S_1600_4, p4.S_1600_4, p5.S_1600_4))      col_12,
       -- SUM(coalesce(p10.S_1600_5, p11.S_1600_5, p2.S_1600_5, p3.S_1600_5, p4.S_1600_5, p5.S_1600_5))      col_13,    
     
       -- sum(coalesce(j2.S_1310_4, j3.S_1310_4, j4.S_1310_4))   col_14,
       -- sum(coalesce(j2.S_1300_4, j3.S_1300_4, j4.S_1300_4))   col_15,
       -- sum(coalesce(j2.S_1300_5, j3.S_1300_5, j4.S_1300_5))   col_16,
        
       -- sum(coalesce(y2.S_1410_4, y3.S_1410_4, y4.S_1410_4))   col_17,
       -- sum(coalesce(y2.S_1450_4, y3.S_1450_4, y4.S_1450_4))   col_18,
       -- sum(coalesce(y2.S_1400_4, y3.S_1400_4, y4.S_1400_4))   col_19,
        
       -- sum(coalesce(u2.S_1510_4, u3.S_1510_4, u4.S_1510_4))   col_20,
       -- sum(coalesce(u2.S_1520_4, u3.S_1520_4, u4.S_1520_4))   col_21,
       -- sum(coalesce(u2.S_1520_5, u3.S_1520_5, u4.S_1520_5))   col_22,
        --sum(coalesce(u2.S_1530_4, u3.S_1530_4, u4.S_1530_4))   col_23,   
       --- sum(coalesce(u2.S_1550_4, u3.S_1550_4, u4.S_1550_4))   col_24,
       --- sum(coalesce(u2.S_1500_4, u3.S_1500_4, u4.S_1500_4))   col_25,    
        
       -- sum(coalesce(o2.S_2110_4, o3.S_2110_4, o7.S_2110_4))   col_26, 
       -- sum(coalesce(o2.S_2110_5, o3.S_2110_5, o7.S_2110_5))   col_27, 
       -- sum(coalesce(o2.S_2120_4, o3.S_2120_4, o7.S_2120_4))   col_28,  
       -- sum(coalesce(o2.S_2200_4, o3.S_2200_4, o7.S_2200_4))   col_29,
       -- sum(coalesce(o2.S_2210_4, o3.S_2210_4, o7.S_2210_4))   col_30,
       -- sum(coalesce(o2.S_2220_4, o3.S_2220_4, o7.S_2220_4))   col_31,    
       -- sum(coalesce(o2.S_2310_4, o3.S_2310_4, o7.S_2310_4))   col_32,  
       -- sum(coalesce(o2.S_2320_4, o3.S_2320_4, o7.S_2320_4))   col_33,   
       -- sum(coalesce(o2.S_2330_4, o3.S_2330_4, o7.S_2330_4))   col_34,  
       --- sum(coalesce(o2.S_2340_4, o3.S_2340_4, o7.S_2340_4))   col_35,  
       -- sum(coalesce(o2.S_2350_4, o3.S_2350_4, o7.S_2350_4))   col_36,  
       -- sum(coalesce(o2.S_2410_4, o3.S_2410_4, o7.S_2410_4))   col_37, 
       -- sum(coalesce(o2.S_2400_4, o3.S_2400_4, o7.S_2400_4))   col_38, 
        --sum(coalesce(o2.S_2400_5, o3.S_2400_5, o7.S_2400_5))   col_39,  
        
        --sum(coalesce(b1.s_3600_3, b2.s_3600_3, b3.s_3600_3, b4.s_3600_3, b5.s_3600_3, b6.s_3600_3, b7.s_3600_3))   col_40,  
        --sum(coalesce(b1.s_3600_4, b2.s_3600_4, b3.s_3600_4, b4.s_3600_4, b5.s_3600_4, b6.s_3600_4, b7.s_3600_4))   col_41 
        ----
        sum(coalesce(o2.S_2100_4, o3.S_2100_4, o7.S_2100_4))   col_34
                
        
from dec_0710099 declar

    
                                       -- on declar.doc=b7.id_document 
-- 2100-2500 
   left join 
     (select S_2100_4, id_document
     --(S_2110_4) S_2110_4, (S_2110_5) S_2110_5, (S_2120_4) S_2120_4, (S_2200_4) S_2200_4,
             --(S_2210_4) S_2210_4, (S_2220_4) S_2220_4, (S_2310_4) S_2310_4, (S_2320_4) S_2320_4, 
             --(S_2330_4) S_2330_4, (S_2340_4) S_2340_4, (S_2350_4) S_2350_4, (S_2410_4) S_2410_4, 
             --(S_2400_4) S_2400_4, (S_2400_5) S_2400_5, id_document                                   
            from CAM$DATA.T549_F2_OTCHET where id_nsi_doc_value_type=1 --group by id_document
            ) o2 
                               on declar.doc=o2.id_document      
    left join 
     (select S_2100_4, id_document
     --(S_2110_4) S_2110_4, (S_2110_5) S_2110_5, (S_2120_4) S_2120_4, (S_2200_4) S_2200_4,
             --(S_2210_4) S_2210_4, (S_2220_4) S_2220_4, (S_2310_4) S_2310_4, (S_2320_4) S_2320_4, 
             --(S_2330_4) S_2330_4, (S_2340_4) S_2340_4, (S_2350_4) S_2350_4, (S_2410_4) S_2410_4, 
             --(S_2400_4) S_2400_4, (S_2400_5) S_2400_5, id_document                                   
            from CAM$DATA.T585_F2_OTCHET where id_nsi_doc_value_type=1 --group by id_document
            ) o3 
                               on declar.doc=o3.id_document                  
    left join 
     (select S_2100_4, id_document
     --(S_2110_4) S_2110_4, (S_2110_5) S_2110_5, (S_2120_4) S_2120_4, (S_2200_4) S_2200_4,
             --(S_2210_4) S_2210_4, (S_2220_4) S_2220_4, (S_2310_4) S_2310_4, (S_2320_4) S_2320_4, 
             --(S_2330_4) S_2330_4, (S_2340_4) S_2340_4, (S_2350_4) S_2350_4, (S_2410_4) S_2410_4, 
             --(S_2400_4) S_2400_4, (S_2400_5) S_2400_5, id_document                                   
            from CAM$DATA.T069_F2_OTCHET where id_nsi_doc_value_type=1 --group by id_document
            ) o7 
                               on declar.doc=o7.id_document                                                                           

   group by declar.inn--, declar.year_, declar.knd  

   
