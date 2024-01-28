
With Spis_INN as(список инн),

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

 
 
 SELECT DISTINCT np.inn,
                SUM(bank_account_balance) OVER (PARTITION BY np.inn) AS total_balance,
                ROW_NUMBER() OVER (PARTITION BY np.inn ORDER BY bank_account_balance_date DESC) AS row_num,
                -- bank_account_balance_date,
                fid
FROM np join  DBS$MON.TAXPAYER_BA_BALANCE_BA_ACT act on act.taxpayer_id  = np.fid
-- where np.inn = '0274170967' 
ORDER BY np.inn;
--DBS$MON.TAXPAYER_BA_BALANCE_BA
--TAXPAYER_BA_BALANCE_BA_ACT
