--�������� � ��������� �������������� �����������, ����������� ��� � ����������� ��������������� ��������� 
--�����������, �������� � ��������-�������������� ������� ������� ������ (����������� � ���������� �����������, 
--� ��������� ������� ������������ �� ���� � ����������� ���������� � ����� � ����������� �������� ����������)
--����� ��������                                                                  

With Spis_INN as(������ ���),

debt_strateg as (
   select /*+ parallel(32)*/
        f.id id,
        ul.INN inn,
        f.owner_unit_code  UNIT_CODE,                      --"��� ��, �������������� ���� � ����������� "
        nvl(ul.SHORT_NAME, ul.FULL_NAME)  SHORT_NAME,                        --����������� ������������ �� 
        ul.KPP KPP,                                         --KPP 
        okved2.NAMEOKVED2 NAIM,                                            --�������� ��� ������������                           
        app_law.RECEIPT_DATE RECEIPT_DATE,        -- ���� ������� ������������ �� ���� � �����������
        d.DOC_NUM DOC_NUM,                       --����� ���� �� ������ �������� ���������       
        to_char(f.STATE)||'. '||fs.NAME STATE,      -- "������� ��������� ����������"
        max(f.CATEG_date) CATEG_date, 
        max(f.CREATION_DATE) CREATION_DATE,       -- "���� �������� ����� ������ �������"
        f.LEVEL_ID LEVEL_ID,                      --  "������� ��������� ���� � �����������"
        f.CATEG_ID CATEG_ID,                      --  "��������� ����� � ����� � �������� ��� ��-21-18/24"
        d.doc_date doc_date,                               -- "���� �������� ����� ������ �������"
        OPK,
        p_1226, 
        p_1009,
        foiv_name,        
        listagg(distinct (ALV.APPLICANT_NAME ||' '||  ALV.APPLICANT_INN), ', ' ON OVERFLOW TRUNCATE) 
                                             within group (order by ALV.APPLICANT_NAME) APPLICANT_NAME,
        case
          when app_law.APPLICANT_TYPE = '1' then '�������'
          when app_law.APPLICANT_TYPE = '2' then '���������� ��������'
          when app_law.APPLICANT_TYPE = '3' then '����'  
          when app_law.APPLICANT_TYPE = '4' then '�������������� �����'
          when app_law.APPLICANT_TYPE = '5' then '��������'
          when app_law.APPLICANT_TYPE = '6' then '���������� �����'
          when app_law.APPLICANT_TYPE = '7' then '������ ����' 
          when app_law.APPLICANT_TYPE = '8' then '�� ��' 
          when app_law.APPLICANT_TYPE = '9' then '��� ��'    
            end zayavitel,
         listagg(distinct VTF.FOUNDER_NAME, ', ' ON OVERFLOW TRUNCATE) within group (order by VTF.FOUNDER_NAME) uchrediteli,
         d.VOTE_127 dolya_gol_po_127_fz,
         d.VOTE_40 dolya_gol_po_40_fz                       
   FROM SPB.DEBTOR_FILE f 
            JOIN spb.taxpayer tp
                  ON tp.id = f.taxpayer_id  
                     and f.DROP_DATE IS NULL               
                     and f.STATE IN (6, 7, 8, 9, 12) 
             JOIN (select distinct ull.inn, kpp, short_name, FULL_NAME, EGRN_TP_ID, OKVED_CODE
                   from SPB.V$UL_S ull join spis_inn
                                   ON spis_inn.inn = ull.inn
                   where OFF_DATE is NULL and OBJ_TYPE_ID = 1              
                    ) ul
                  ON tp.EGRN_TP_ID = ul.EGRN_TP_ID
                     --and ul.OFF_DATE is NULL
                     --and ul.OBJ_TYPE_ID = 1
             join Spis_INN 
                  ON Spis_INN.inn = ul.inn                     
             left join CSR.R$SPOKVED2CONV okved2 
                  ON ul.OKVED_CODE = okved2.OKVED1 
                     and PRZ_OVD = 1     
             --join SPB.NV$STRATEG NS 
                  --ON ul.OGRN = NS.OGRN 
             left join SPB.APP_LAW app_law
                  ON app_law.id = f.id
             left join SPB.APP_LAW_VAR ALV 
                  on ALV.id = app_law.id     
             LEFT OUTER JOIN SPB.PROC p 
                  ON f.ID = p.file_ID 
                     and p.state = 1 
             LEFT OUTER JOIN SPB.LAW_DEF d
                  ON  p.DEF_ID = d.ID
                     AND d.DROP_DATE IS NULL 
                     and d.doc_date is not NULL 
             left JOIN SPB$IRD.V$FILE_STATE fs 
                  ON fs.ID = f.STATE
             left join cam$taxmon.v$taxpayer_founders VTF
                  ON VTF.id_taxpayer = tp.EGRN_TP_ID
             left join (select TAXPAYER_ID TAXPAYER_ID,
                          max(OPK) OPK,
                          max(p_1226) p_1226, 
                          max(p_1009) p_1009,
                          max(foiv_name) foiv_name
                   from (select TAXPAYER_ID TAXPAYER_ID,        
                                max(case
                                       when strat_list.TYPE_ID = 1 
                                          then '1'
                                             else '-'
                                                end) OPK,
                                max(case
                                       when strat_list.TYPE_ID = 2
                                          then '1'
                                             else '-'
                                                end) p_1226, 
                                max(case
                                       when strat_list.TYPE_ID = 3 
                                          then '1'
                                             else '-'
                                                end) p_1009,
                                max(RSF.name) foiv_name
                        from SPB.STRATEG Strat
                                         join SPB.STRATEG_LIST strat_list
                                              ON Strat.List_Id = strat_list.id
                                                 and START_DATE is not null 
                                                 and FINISH_DATE is null                                
                                         left join SPB$IRD.R$S_FOIV RSF                                    
                                              on RSF.id = Strat.FOIV_ID  
                       --where taxpayer_id in (367723, 409939, 358018)   
                       group by TAXPAYER_ID  
                                     ) gg                             
                        group by TAXPAYER_ID   
                                     ) Strat_l
                         ON tp.id = Strat_l.TAXPAYER_ID
          
   where d.doc_date is not NULL   
         and f.id not in (323436, 323445)
         --and ul.inn in (select inn from Spis_INN)      --������ 2 �� ��������� �� "172 ����"                
   group by f.id,  ul.INN, f.owner_unit_code, ul.SHORT_NAME, ul.KPP,  okved2.NAMEOKVED2,                       
        app_law.RECEIPT_DATE, d.DOC_NUM, f.STATE, fs.NAME, f.LEVEL_ID, ul.FULL_NAME, 
        f.CATEG_ID, d.doc_date, --NS.LIST_1, NS.LIST_2, NS.LIST_3,
        app_law.APPLICANT_TYPE, d.VOTE_127, d.VOTE_40, OPK, p_1226, p_1009, foiv_name
            )


                  
select inn "inn",
STATE,      -- "������� ��������� ����������"24
          debt_strateg.RECEIPT_DATE "23.���� ������� ���� � �����"
                      
from debt_strateg  
               
                  
