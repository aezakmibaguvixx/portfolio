With Spis_INN as(
select column_value as INN from
table(sys.ODCIVARCHAR2LISTсписок инн),

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
        SYSDATE,
        ee.TP_FID "ФИД налогоплательщика",
        np.inn "ИНН",
        np.NAME_FULL "Наименование",
        reg_code.ufns "УФНС",
        reg_code.TNO  "ТНО",
        case when nvl(ee.s_ens,0) < 0  THEN ee.s_ens END "Отрицательный ЕНС",
        CASE when nvl(ee.s_ens,0) < 0 then WDRW_DCN.DOC_DATE  end "Дата документа ст.46"
          
        
       -- req.doc_num  "Номер ст.69 ТУ",
       -- req.doc_date   "Дата ст.69 ТУ",
       -- req.INIT_TAX_ORGAN_CODE "Код НО, по которому сформирован документ",
       -- req.ADMIN_TAX_ORGAN_CODE "Код НО, сформировавшего документ",
       -- req.INIT_DOC_SUM "Сумма отр.сальдо  на момент формирования ст.69 ТУ",
      --  max(case when req.doc_date is not null then d.CURRENT_AMOUNT_TOTAL else null end) "Остаток неоплаченной суммы ст.69",
       -- case when req.doc_date is not null then 'Не исполнено' else null end "Статус ТУ",
       -- WDRW_DCN.DOC_NUM "№ документа ст.46",
      --  WDRW_DCN.DOC_DATE "Дата документа ст.46",
       -- WDRW_DCN.INIT_TAX_ORGAN_CODE "Код НО, по которому сформирован документ",
       -- WDRW_DCN.ADMIN_TAX_ORGAN_CODE "Код НО, сформировавшего документ",
       -- WDRW_DCN.INIT_DOC_SUM_TOTAL "Сумма по документу ст.46",
       -- max(case when WDRW_DCN.DOC_DATE is not null then d.CURRENT_AMOUNT_TOTAL else null end) "Остаток неоплаченной суммы ст.46"
        
from 
        ens.ens ee  
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
left join dbs$ens_mon.req req on d.id = req.ens_debt_id
                                 and req.DOC_NUM <> '0'
left join DBS$ENS_MON.WDRW_DCN WDRW_DCN ON d.id = WDRW_DCN.ens_debt_id
                              and WDRW_DCN.DOC_PROCESSING_STATE = 0
                              and WDRW_DCN.DOC_NUM <> '0'                              
 

                     
--where ee.s_ens < 0
--group by SYSDATE,
       --  ee.TP_FID,
       --  np.inn,
        -- np.NAME_FULL,
        -- reg_code.ufns,
      --   reg_code.TNO,
       --  ee.s_ens,
        -- req.doc_num,
        -- req.doc_date,
         --req.INIT_TAX_ORGAN_CODE,
        -- req.ADMIN_TAX_ORGAN_CODE,
        -- req.INIT_DOC_SUM,
        -- WDRW_DCN.DOC_NUM,
        -- WDRW_DCN.DOC_DATE,
        -- WDRW_DCN.INIT_TAX_ORGAN_CODE,
        -- WDRW_DCN.ADMIN_TAX_ORGAN_CODE,
        -- WDRW_DCN.INIT_DOC_SUM_TOTAL
         
         
