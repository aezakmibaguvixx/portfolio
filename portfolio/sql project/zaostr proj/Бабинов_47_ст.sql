---Для Бабинова по срочному запросу ст.47 остаток неоплаченной суммы по ст.47 больше 10 млн.

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



select /*+ parallel(32)*/
        distinct        
        np.inn "ИНН",
           
        wdc.DOC_NUM "Номер ст.47",
        wdc.DOC_DATE "Дата документа ст.47",
        --wdc.INIT_TAX_ORGAN_CODE "Код НО, по которому сформирован документ",
       -- wdc.ADMIN_TAX_ORGAN_CODE "Код НО, сформировавшего документ",
        --wdc.TOTAL_SUM "Сумма по документу ст.47",
        wdc.UNPAID_AMOUNT  "Остаток неоплаченной суммы ст.47",
        case when wdc.DOC_NUM is not null then 'Не исполнено' else null end "Статус по ст.47"
        
from 
        ens.ens  ee
        join np on ee.TP_FID = np.fid 
        left join (
                select --sysdate,
                        Case 
                                when s.vid in('18','35','55')
                                        then s.kodv --код ИФНС у МИ УДОЛ, двухуровневых,МИ по КН
                                when s.kod like ('99%') and s.vid not in('18','35','55')
                                        then s.kod
                                else SUBSTR(s.kod, 1, 2)||'00'--when s.vid in(30,31,39,41,42,43,50,52,55,60)
                        end ufns,
                        s.kod tno,
                        s.KODV "Вышестоящий орган",
                        s.vid
                from 
                        DBS$ERD.MV$THES_SOUN s 
                where  
                        S.ROW_PRIORITY = 1
        ) reg_code
                ON ee.IFNS = reg_code.tno 
left join ens.debt d   on d.tp_fid = ee.tp_fid
                          and d.CURRENT_AMOUNT_TOTAL > 0
                          AND d.DATE_TO            = DATE '3000-01-01'
LEFT JOIN ENS.KNO                           kno      ON     kno.ENS_ID                  = d.ENS_ID
                                                            AND kno.CLOSE_DATE              IS NULL
                       
LEFT join DBS$ENS_OWN.ASSET_WDRW_DOC_CASE wdc  on wdc.TP_ID = kno.tp_fid --47 ст.
                                              and wdc.UNPAID_AMOUNT > 0
                                              and wdc.DOC_NUM <> '0'                       
                        

       
         
