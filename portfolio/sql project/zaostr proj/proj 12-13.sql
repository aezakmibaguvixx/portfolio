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


select  np.inn "inn",
		listagg(to_char(sd.DOC_DATE, 'dd.mm.yyyy'), ', ' ON OVERFLOW TRUNCATE)         
                               within group (order by sd.DOC_DATE)   "12.",             --"12_ƒата док-та 77",
		listagg(to_char(sd.stop_arrest_date, 'dd.mm.yyyy'), ', ' ON OVERFLOW TRUNCATE) 
                               within group (order by sd.DOC_DATE) 	 "13."					--"13_прекращение ареста",	
from DBS$ENS_OWN.SEIZURE_DCN sd
        join dbs$ens.tp_info                        tp    on tp.id = sd.tp_info_id 
        join np 
	on np.fid = tp.tp_id  
group by np.inn
  






     
  
        
