With Spis_INN as(������ ���),

spisok_all as (Select /*+ PARALLEL(16) */ 
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
            ),

obj2 as(select  /*+ parallel(4)*/ 
                spisok_all.inn inn,
                --count(onul.FID_ENTITY) count_CAD_COST,                
                sum(nvl(onul.CAD_COST, 0) * nvl(SIZE_SHARE, 0)) as cadcost,
                --null count_LAND,
                null LAND_cost
        from spisok_all left join TAX_OBJ.V$PROPERTYRIGHT onul 
                        ON spisok_all.fid = onul.FID_ENTITY 
        where onul.DATE_STOP is null
        --and nvl(ul.inn, fl.inn) in (select inn from Spis_INN)
        group by spisok_all.inn
    union 
        select  /*+ parallel(4)*/ 
                spisok_all.inn inn,
               -- null,
                null,
               -- count(LAND.FID_ENTITY) count_LAND,                
                sum(nvl(LAND.CAD_COST, 0) * nvl(LAND.SIZE_SHARE, 0)) as LAND_cost
        from spisok_all left join TAX_OBJ.V$LANDRIGHT LAND 
                  ON spisok_all.fid = LAND.FID_ENTITY
        where LAND.DATE_STOP is null
        --and nvl(ul.inn, fl.inn) in (select inn from Spis_INN)
        group by spisok_all.inn
        ),
        
obj_car as(select  /*+ parallel(4)*/ 
                spisok_all.inn inn,
                count(ts.FID_OBJ) FID_OBJ_car
       from spisok_all left join TAX_OBJ.V$TCRIGHT ts 
                       ON spisok_all.fid = ts.FID_ENTITY
       where ts.DATE_STOP is null
       group by spisok_all.inn)
 
 --ﰨ񮥤譿嬠ꠤ౲𮢳þ 񲮨쮱򼠮򪰻򻵠媲
select /*+ parallel(4)*/
       spisok_all.inn "inn",
       --sum(nvl(count_CAD_COST, 0) + nvl(count_LAND, 0))+ max(nvl(FID_OBJ_car,0))  "Общее количество",  
      -- sum(nvl(count_LAND, 0)) "Количество земли", 
	  sum(nvl(cadcost,0)) 						   "19.float", 				--"Сумма недвижимости",
       sum(nvl(LAND_cost,0)) 			           "20.float",				--"Сумма земли",
      -- sum(nvl(count_CAD_COST, 0)) 										"Количество недвижимости", 
       max(nvl(FID_OBJ_car, 0)) 				   "21.float",                 --"Количество машин",
	   sum(nvl(LAND_cost,0)) + sum(nvl(cadcost,0)) "22.float"
from spisok_all left join obj2 
                   on spisok_all.inn = obj2.inn
              left join obj_car 
                   on spisok_all.inn = obj_car.inn   
--where Spis_INN.inn in ('2470200065', '2411004492', '2411005915', '2465296788')                
group by spisok_all.inn
