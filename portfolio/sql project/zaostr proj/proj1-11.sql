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
            )
select 
			np.fid  				"0.",  
			np.INN 					"inn",
			--NAME_FULL				"3.",
			--type_np					"4.",
			--no_mri_code  		"5.",
      --total.id_complex                   "7.float",  --УН проверки
      --total.date_assign                  "8.",--data resheniya
      --total.dvs                          "9.date",--data vstypleniya
      --total.number_root                  "10.float", -- номер решения
      --qt.SUMM_RES_14_R3                "11.float",
      months_between(total.date_end, total.date_begin) "kol vtczwtd",
      total.date_end,
      total.date_begin
        
      
from np left join CAM$IR.VNP_MONITORING_QUALITY total
ON total.id_taxpayer = np.fid
join CAM$IR.VNP_MONITORING_QUALITY_TOTAL qt on qt.id_complex = total.id_complex 

where np.inn = '2130163063'




			
