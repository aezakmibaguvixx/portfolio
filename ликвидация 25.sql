With Spis_INN as(
select column_value as INN from
table(sys.ODCIVARCHAR2LIST(список инн)
Select fid,elimination_date,              
              Spis_INN.INN       
       from Spis_INN left join 
                   (select WT_ENTITY_EGRN.ID_ENTITY fid,elimination_date,       
                           WT_UL.INN INN
                    from TPACC$LST.WT_UL JOIN TPACC$LST.WT_ENTITY_EGRN  
                                         ON WT_UL.ENTITY_EGRN_ID = WT_ENTITY_EGRN.ID                                                   
               ) ff 
          ON Spis_INN.inn = ff.inn    
            
  


                              

