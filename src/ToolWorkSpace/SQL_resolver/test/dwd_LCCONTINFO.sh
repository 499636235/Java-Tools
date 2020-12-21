#! /bin/bash

export HADOOP_USER_NAME=hdfs

hive -e "insert overwrite table soochow_data.dwd_LCCONTINFO  
   select laco.PRTNO as LAPRTNO,
       laco.CONTTYPE as LACONTTYPE,
       laco.AGENTCODE as LAAGENTCODE,
       laco.NAME,
       laco.BRANCHATTR,
       laco.AGENTGROUP as LAAGENTGROUP,
       laco.PAYMONEY,
       laco.ACCEPTDATE,
       laco.ACCEPTTIME,
       laco.UPLOADFLAG,
       laco.FLAG1,
       laco.FLAG2,
       laco.FLAG3,
       laco.FLAG4,
       laco.FLAG5,
       laco.FLAG6,
       laco.OPERATOR as LAOPERATOR,
       laco.MANAGECOM as LAMANAGECOM,
       laco.MAKEDATE as LAMAKEDATE,
       laco.MAKETIME as LAMAKETIME,
       laco.MODIFYDATE as LAMODIFYDATE,
       laco.MODIFYTIME as LAMODIFYTIME,
       laco.STANDBYFLAG1,
       laco.STANDBYFLAG2,
       laco.STANDBYFLAG3,
       laco.STANDBYFLAG4,
       laco.STANDBYFLAG5,
       laco.ACCEPTNAME,
       laco.AGENTCOM as LAAGENTCOM,
       laco.AGENTCOMNAME,
       laco.APPNTNAME as LAAPPNTNAME,
       laco.PEOPLES as LAPEOPLES,
       laco.RISKKIND,
       laco.PAYMODE as LAPAYMODE,
       laco.BANKCODE as LABANKCODE,
       laco.BANKACCNO as LABANKACCNO,
       laco.SALESIGNFLAG,
       laco.CONTPLANBOOKFLAG,
       laco.REPORTFEEFLAG,
       laco.PEOPLESLISTFLAG,
       laco.COMCODEFLAG,
       laco.TAXNAME,
       laco.TAXNO,
       laco.TAXADDRESS,
       laco.TAXTYPE,
       laco.TAXPHONE,
       laco.TAXBANKCODE,
       laco.TAXBANKACCNO,
       laco.PUBLISHTIPTYPE,
       laco.GENERALCERTSCANFLAG,
       laco.TAXSUBBANK,
       laco.IDTYPE,
       laco.IDNO,
       lcct.GRPCONTNO,
       lcct.CONTNO as LCCONTNO,
       lcct.PROPOSALCONTNO,
       lcct.PRTNO,
       lcct.CONTTYPE as LCCONTTYPE,
       lcct.FAMILYTYPE,
       lcct.FAMILYID,
       lcct.POLTYPE,
       lcct.CARDFLAG,
       lcct.MANAGECOM as LCMANAGECOM,
       ldcm.NAME as MANAGECOMNAME,
       ldco.NAME as ERCOMNAME,
       ldc.NAME as SANCOMNAME,
       ld.NAME as SICOMNAME,
       lcct.EXECUTECOM,
       lcct.AGENTCOM,
       lcct.AGENTCODE,
       case
         when temp_laob.CONTNO is not null then
          laat.name
         else
          laa.name
       end as AGENTNAME,
       case
         when temp_laob.CONTNO is not null then
          laae.GRADENAME
         else
          la.GRADENAME
       end as GRADENAME,
       lcct.AGENTGROUP,
       ladp.NAME as DISTICTNAME,
       case
         when temp_laob.CONTNO is not null then
          temp_ll.re
         else
          temp_la.re
       end as DEPARTNAME,
       case
         when temp_laob.CONTNO is not null then
          temp_ll.re1
         else
          temp_la.re1
       end as GROUPNAME,
       lcct.AGENTCODE1,
       lcct.AGENTTYPE,
       lcct.SALECHNL,
       ldce.CODENAME as SALECHNLNAME,
       lcct.HANDLER,
       lcct.PASSWORD,
       lcct.APPNTNO,
       lcct.APPNTNAME,
       lcct.APPNTSEX,
       lcct.APPNTBIRTHDAY,
       lcct.APPNTIDTYPE,
       ldd.CODENAME as APPNTIDTYPENAME,
       lcct.APPNTIDNO,
       lcat.IDEXPDATE as APPNTIDEXPDATE,
       temp_lcp.HOMEADDRESS as APPNTADDRESS,
       temp_lcp.PHONE as APPNTPHONE,
       temp_lpa.OCCUPATIONNAME as APPNTOCCUPATIONNAME,
       temp_ldm.codename as APPNTNATIVEPLACE,
       temp_rel.codename as RELATIONTOINSUREDNAME,
       case when temp_lcbf.re is NULL then 0 else temp_lcbf.re end as BNFNUM,
       lcct.INSUREDNO,
       lcct.INSUREDNAME,
       lcct.INSUREDSEX,
       lcct.INSUREDBIRTHDAY,
       lcct.INSUREDIDTYPE,
       lcct.INSUREDIDNO,
       lcct.PAYINTV,
       lcct.PAYMODE,
       lcct.PAYLOCATION,
       lcct.DISPUTEDFLAG,
       lcct.OUTPAYFLAG,
       lcct.GETPOLMODE,
       lcct.SIGNCOM,
       lcct.SIGNDATE,
       lcct.SIGNTIME,
       lcct.CONSIGNNO,
       lcct.BANKCODE,
       lcct.BANKACCNO,
       lcct.ACCNAME,
       lcct.PRINTCOUNT,
       lcct.LOSTTIMES,
       lcct.LANG,
       lcct.CURRENCY,
       lcct.REMARK,
       lcct.PEOPLES,
       lcct.MULT,
       lcct.PREM,
       lcct.AMNT,
       lcct.SUMPREM,
       lcct.DIF,
       lcct.PAYTODATE,
       lcct.FIRSTPAYDATE,
       lcct.CVALIDATE,
       lcct.INPUTOPERATOR,
       lcct.INPUTDATE,
       lcct.INPUTTIME,
       lcct.APPROVEFLAG,
       lcct.APPROVECODE,
       lcct.APPROVEDATE,
       lcct.APPROVETIME,
       lcct.UWFLAG,
       lcct.UWOPERATOR,
       lcct.UWDATE,
       lcct.UWTIME,
       lcct.APPFLAG,
       '' CONTSTATE,
       lcct.POLAPPLYDATE,
       lcct.GETPOLDATE,
       lcct.GETPOLTIME,
       lcct.CUSTOMGETPOLDATE,
       lcct.STATE,
       lcct.OPERATOR as LCOPERATOR,
       lcct.MAKEDATE as LCMAKEDATE,
       lcct.MAKETIME as LCMAKETIME,
       lcct.MODIFYDATE as LCMODIFYDATE,
       lcct.MODIFYTIME as LCMODIFYTIME,
       lcct.FIRSTTRIALOPERATOR,
       lcct.FIRSTTRIALDATE,
       lcct.FIRSTTRIALTIME,
       lcct.RECEIVEOPERATOR,
       lcct.RECEIVEDATE,
       lcct.RECEIVETIME,
       lcct.TEMPFEENO,
       lcct.SELLTYPE,
       lcct.FORCEUWFLAG,
       lcct.FORCEUWREASON,
       lcct.NEWBANKCODE,
       lcct.NEWBANKACCNO,
       lcct.NEWACCNAME,
       lcct.NEWPAYMODE,
       lcct.AGENTBANKCODE,
       lcct.BANKAGENT,
       lcct.AUTOPAYFLAG,
       lcct.RNEWFLAG,
       lcct.FAMILYCONTNO,
       lcct.BUSSFLAG,
       lcct.SIGNNAME,
       lcct.ORGANIZEDATE,
       lcct.ORGANIZETIME,
       lcct.NEWAUTOSENDBANKFLAG,
       lcct.AGENTCODEOPER,
       lcct.AGENTCODEASSI,
       lcct.DELAYREASONCODE,
       lcct.DELAYREASONDESC,
       lcct.XQREMINDFLAG,
       lcct.ORGANCOMCODE,
       lcct.BANKNETID,
       lcct.NEWBANKNETID,
       lcct.ARBITRATIONORG,
       lcct.BATTYPE,
       lcct.REMITTEDPARTNO,
       lcct.CERTIFICATENO,
       lcct.DEDUPREMFROMACC,
       lcct.SELFPOLFLAG,
       lcct.SALEWEBNETNAME,
       lcct.CONFIRMATIONNO,
       lcct.RECIPROCALDATE,
       lcct.RECIPROCALFLAG,
       lcct.CVALITIME,
       lcct.REVISITTIME,
       lcct.SURROUNDSYSFLAG,
       lcct.NETAMNT,
       lcct.TAX,
       lcct.REVISITDATE,
       '' UWPASSFLAG,
       '' UWRESULT,
       '' FIRSTSENDDATE,
       '' LASTREPLYDATE,
       '' PHYEXAMFLAG,
       '' TRANSSTATUS,
       '' FAILREASON,
       '' CHANNEL,
       '' as ADDPREM,
       '' REDUCLEARDATE,
       '' WTFLAG,
       NVL(temp_lbmn.MAKEDATE,lcct.POLAPPLYDATE) as UWPASSDATE,
       temp_lacon.fyc as FYC,
       temp_esdn.MAKEDATE as RETURNSCANDATE,
       lcco.CONTNO as LOCONTNO,
       lcco.OPERATOR as LOOPERATOR,
       lcco.MAKEDATE as LOMAKEDATE,
       lcco.MAKETIME as LOMAKETIME,
       lcco.MODIFYDATE as LOMODIFYDATE,
       lcco.MODIFYTIME as LOMODIFYTIME,
       lcco.VIN,
       lcco.PAYFORM,
       lcco.CONTFORM1,
       lcco.CONTFORM2,
       lcco.YBTASYNFLAG,
       lcco.VISAFLAG,
       lcco.CROSSSALETYPE,
       lcco.DISCOUNTFLAG,
       lcco.AGENTFLAG,
       lcco.AUDIOVIDEOFLAG,
       lcco.NEEDAUDIOVIDEOFLAG,
       lcco.QUALITYRESLUT,
       lcco.FAILQUALITYREASON,
       lcco.SENDRESULT,
       lcco.SOCIALSALEFLAG,
       lcco.SELLERNO,
       lcco.YDZYDISCOUNTFLAG,
       lcco.ALIAS,
       '', --lcco.BUSYTYPE,
       lccot.CONTNO,
       lccot.PRTFLAG,
       lccot.PRTTIMES,
       lccot.MANAGECOM,
       lccot.OPERATOR,
       lccot.MAKEDATE,
       lccot.MAKETIME,
       lccot.MODIFYDATE,
       lccot.MODIFYTIME,
       lccot.CONTINFO,
       lccot.CONTTYPE,
	   case
         when ABtemp.agtflag = '1' then
          ABtemp.AGENTCODE
         else
          lcct.AGENTCODE
       end as SALESCODE,
	   lcm.UWIDEA as UWIDEA,
	   case
         when temp_laob.CONTNO is not null then
          temp_ll.AGENTGROUP
         else
          temp_la.AGENTGROUP
       end as DEPARTAGENTGROUP,
       case
         when temp_laob.CONTNO is not null then
          temp_ll.AGENTGROUP1
         else
          temp_la.AGENTGROUP1
       end as GROUPAGENTGROUP,
        'IN'  OGGACTION ,
        from_unixtime(unix_timestamp(),'yyyy-MM-dd')  OGGDATE ,
        from_unixtime(unix_timestamp(),'yyyy-MM-dd')  PushDate ,		
        from_unixtime(unix_timestamp(),'HH:mm:ss')  PushTime
  FROM soochow_data.ods_lis_LCCONT lcct
  LEFT JOIN soochow_data.ods_lis_LCACCEPTINFO laco
    on lcct.PRTNO = laco.PRTNO
  LEFT JOIN soochow_data.ods_lis_LCCONTOTHERINFO lcco
    ON lcco.CONTNO = lcct.PRTNO
  LEFT JOIN soochow_data.ods_lis_LCCONTPRINT lccot
    ON lccot.CONTNO = lcct.CONTNO
  left join soochow_data.ods_lis_LDCOM ldcm
    on ldcm.COMCODE = lcct.MANAGECOM
  left join soochow_data.ods_lis_LDCOM ldco
    on ldco.COMCODE = SUBSTR(lcct.MANAGECOM, 0, 4)
  left join soochow_data.ods_lis_LDCOM ldc
    on ldc.COMCODE = SUBSTR(lcct.MANAGECOM, 0, 6)
  left join soochow_data.ods_lis_LDCOM ld
    on ld.COMCODE = SUBSTR(lcct.MANAGECOM, 0, 8)
  left join (SELECT MIN(EDORNO) as edorno, CONTNO
               FROM soochow_data.ods_lis_LAORPHANPOLICYB
              group by CONTNO) temp_laob
    on temp_laob.CONTNO = lcct.CONTNO
  left join soochow_data.ods_lis_LAORPHANPOLICYB laob
    on laob.CONTNO = lcct.CONTNO
   and laob.EDORNO = temp_laob.edorno
  left join soochow_data.ods_lis_LAAGENT laat
    on laat.AGENTCODE = laob.AGENTCODE
  left join soochow_data.ods_lis_LAAGENT laa
    on laa.AGENTCODE = lcct.AGENTCODE
  left join soochow_data.ods_lis_LATREE late
    on late.AGENTCODE = laob.AGENTCODE
  left join soochow_data.ods_lis_LAAGENTGRADE laae
    on laae.GRADECODE = late.AGENTGRADE
  left join soochow_data.ods_lis_LATREE lat
    on lat.AGENTCODE = lcct.AGENTCODE
  left join soochow_data.ods_lis_LAAGENTGRADE la
    on la.GRADECODE = lat.AGENTGRADE
  left join soochow_data.ods_lis_LABRANCHGROUP labp
    on labp.AGENTGROUP = lcct.AGENTGROUP
  left join soochow_data.ods_lis_LADISTRICTGROUP ladp
    on ladp.DISTICT = labp.DISTICT
  left join (select if(LL.BRANCHLEVEL = '02', LL.NAME, labu.NAME) re,
					if(LL.BRANCHLEVEL = '02', LL.AGENTGROUP, labu.AGENTGROUP) AGENTGROUP,
                    LA.AGENTCODE as AGENTCODE,
                    LD.NAME as re1,
					LD.AGENTGROUP as AGENTGROUP1
               from soochow_data.ods_lis_LAAGENT LA
              inner join soochow_data.ods_lis_LATREE LT
                 on LA.AGENTCODE = LT.AGENTCODE
              inner join soochow_data.ods_lis_LABRANCHGROUP LD
                 on LT.BRANCHCODE = LD.AGENTGROUP
              inner join soochow_data.ods_lis_LABRANCHGROUP LL
                 on LT.AGENTGROUP = LL.AGENTGROUP
               left join soochow_data.ods_lis_LABRANCHGROUP labu
                 on labu.AGENTGROUP = LL.UPBRANCH) temp_ll
    on temp_ll.AGENTCODE = laob.AGENTCODE
  left join (select if(LL.BRANCHLEVEL = '02', LL.NAME, labu.NAME) re,
					if(LL.BRANCHLEVEL = '02', LL.AGENTGROUP, labu.AGENTGROUP) AGENTGROUP,
                    LA.AGENTCODE as AGENTCODE,
                    LD.NAME as re1,
					LD.AGENTGROUP as AGENTGROUP1
               from soochow_data.ods_lis_LAAGENT LA
              inner join soochow_data.ods_lis_LATREE LT
                 on LA.AGENTCODE = LT.AGENTCODE
              inner join soochow_data.ods_lis_LABRANCHGROUP LD
                 on LT.BRANCHCODE = LD.AGENTGROUP
              inner join soochow_data.ods_lis_LABRANCHGROUP LL
                 on LT.AGENTGROUP = LL.AGENTGROUP
               left join soochow_data.ods_lis_LABRANCHGROUP labu
                 on labu.AGENTGROUP = LL.UPBRANCH) temp_la
    on temp_la.AGENTCODE = lcct.AGENTCODE
  left join soochow_data.ods_lis_LDCODE ldce
    on ldce.CODETYPE = 'salechnl'
   AND ldce.CODE = lcct.SALECHNL
  left join soochow_data.ods_lis_LDCODE ldd
    on ldd.CODETYPE = 'idtype'
   AND ldd.CODE = lcct.APPNTIDTYPE
  left join soochow_data.ods_lis_LCAPPNT lcat
    on lcat.CONTNO = lcct.CONTNO
  left join (SELECT NVL(LS.POSTALADDRESS, LS.HOMEADDRESS) as HOMEADDRESS,
                    NVL(LS.MOBILE, LS.PHONE) as PHONE,
                    LCP.CONTNO as CONTNO
               FROM soochow_data.ods_lis_LCAPPNT   LCP,
                    soochow_data.ods_lis_LCADDRESS LS
              WHERE 1 = 1
                AND LS.CUSTOMERNO = LCP.APPNTNO
                AND LCP.ADDRESSNO = LS.ADDRESSNO) temp_lcp
    on temp_lcp.CONTNO = lcct.CONTNO
  left join (select LPA.OCCUPATIONNAME as OCCUPATIONNAME,
                    LCP.CONTNO         as CONTNO
               from soochow_data.ods_lis_LCAPPNT LCP
               left join soochow_data.ods_lis_LDOCCUPATION LPA
                 on LPA.OCCUPATIONCODE = LCP.OCCUPATIONCODE) temp_lpa
    on temp_lpa.CONTNO = lcct.CONTNO
  left join (select ld.codename as codename,
                    m.contno    as contno,
                    m.appntno   as appntno
               from soochow_data.ods_lis_lcappnt m
               left join soochow_data.ods_lis_ldcode ld
                 on ld.code = m.nativeplace
                and ld.codetype = 'nativeplace') temp_ldm
    on temp_ldm.contno = lcct.contno
   and temp_ldm.appntno = lcct.appntno
  left join (select LD.CODENAME as CODENAME, LCP.CONTNO as CONTNO
               from soochow_data.ods_lis_LCAPPNT LCP
               left join soochow_data.ods_lis_LDCODE LD
                 on LD.CODETYPE = 'relation'
                AND LD.CODE = LCP.RELATIONTOINSURED) temp_rel
    on temp_rel.CONTNO = lcct.CONTNO
  left join (SELECT COUNT(1) re, CONTNO
               FROM soochow_data.ods_lis_LCBNF
              WHERE NAME <> '法定'
              group by CONTNO) temp_lcbf
    on temp_lcbf.CONTNO = lcct.CONTNO
  left join (SELECT MAX(LB.MAKEDATE) as MAKEDATE,
                    LB.MISSIONPROP1 as MISSIONPROP1
               FROM soochow_data.ods_lis_LBMISSION LB
              WHERE 1 = 1
                AND LB.ACTIVITYID = '0000001150'
              group by LB.MISSIONPROP1) temp_lbmn
    on temp_lbmn.MISSIONPROP1 = lcct.PRTNO
  left join (SELECT SUM(LB.FYC) as fyc, LB.CONTNO as CONTNO
               FROM soochow_data.ods_lis_LACOMMISION LB
              group by LB.CONTNO) temp_lacon
    on temp_lacon.CONTNO = lcct.CONTNO
  left join (SELECT ES.MAKEDATE as MAKEDATE,
                    ES.DOCCODE as DOCCODE,
                    row_number() over(partition by ES.DOCCODE order by ES.DOCCODE) rank
               FROM soochow_data.ods_lis_ES_DOC_MAIN ES
              WHERE 1 = 1
                AND ES.SUBTYPE = 'UR301') temp_esdn
    on temp_esdn.rank = 1
   and temp_esdn.DOCCODE = lcct.CONTNO
   LEFT JOIN (SELECT row_number() over(partition by AB.CONTNO order by AB.EDORNO asc) rowsnum,
                    '1' AS agtflag,
                    AB.AGENTCODE,
                    AB.CONTNO
               FROM soochow_data.ods_lis_LAORPHANPOLICYB AB) ABtemp
    on ABtemp.CONTNO = lcct.CONTNO
   and ABtemp.rowsnum = 1 
   left join soochow_data.ods_lis_LCCUWMASTER lcm
    on lcm.CONTNO = lcct.CONTNO" 

		
if [ $? -ne 0 ]; then
  #异常退出
  echo "从ods 提数据到dwd表dwd_LCCONTINFO失败"
   exit 1
  else
  echo "从ods 提数据到dwd表dwd_LCCONTINFO成功"
fi

hive -e "insert overwrite table soochow_data.dwd_LCCONTINFO  
    select
        lcct.LAPRTNO,
        lcct.LACONTTYPE,
        lcct.LAAGENTCODE,
        lcct.NAME,
        lcct.BRANCHATTR,
        lcct.LAAGENTGROUP,
        lcct.PAYMONEY,
        lcct.ACCEPTDATE,
        lcct.ACCEPTTIME,
        lcct.UPLOADFLAG,
        lcct.FLAG1,
        lcct.FLAG2,
        lcct.FLAG3,
        lcct.FLAG4,
        lcct.FLAG5,
        lcct.FLAG6,
        lcct.LAOPERATOR,
        lcct.LAMANAGECOM,
        lcct.LAMAKEDATE,
        lcct.LAMAKETIME,
        lcct.LAMODIFYDATE,
        lcct.LAMODIFYTIME,
        lcct.STANDBYFLAG1,
        lcct.STANDBYFLAG2,
        lcct.STANDBYFLAG3,
        lcct.STANDBYFLAG4,
        lcct.STANDBYFLAG5,
        lcct.ACCEPTNAME,
        lcct.LAAGENTCOM,
        lcct.AGENTCOMNAME,
        lcct.LAAPPNTNAME,
        lcct.LAPEOPLES,
        lcct.RISKKIND,
        lcct.LAPAYMODE,
        lcct.LABANKCODE,
        lcct.LABANKACCNO,
        lcct.SALESIGNFLAG,
        lcct.CONTPLANBOOKFLAG,
        lcct.REPORTFEEFLAG,
        lcct.PEOPLESLISTFLAG,
        lcct.COMCODEFLAG,
        lcct.TAXNAME,
        lcct.TAXNO,
        lcct.TAXADDRESS,
        lcct.TAXTYPE,
        lcct.TAXPHONE,
        lcct.TAXBANKCODE,
        lcct.TAXBANKACCNO,
        lcct.PUBLISHTIPTYPE,
        lcct.GENERALCERTSCANFLAG,
        lcct.TAXSUBBANK,
        lcct.IDTYPE,
        lcct.IDNO,
        lcct.GRPCONTNO,
        lcct.LCCONTNO,
        lcct.PROPOSALCONTNO,
        lcct.PRTNO,
        lcct.LCCONTTYPE,
        lcct.FAMILYTYPE,
        lcct.FAMILYID,
        lcct.POLTYPE,
        lcct.CARDFLAG,
        lcct.LCMANAGECOM,
        lcct.MANAGECOMNAME,
        lcct.ERCOMNAME,
        lcct.SANCOMNAME,
        lcct.SICOMNAME,
        lcct.EXECUTECOM,
        lcct.AGENTCOM,
        lcct.AGENTCODE,
		lcct.AGENTNAME,
        lcct.GRADENAME,			  
        lcct.AGENTGROUP,
        lcct.DISTICTNAME,		
		lcct.DEPARTNAME,
        lcct.GROUPNAME,
        lcct.AGENTCODE1,
        lcct.AGENTTYPE,
        lcct.SALECHNL,
        lcct.SALECHNLNAME,
        lcct.HANDLER,
        lcct.PASSWORD,
        lcct.APPNTNO,
        lcct.APPNTNAME,
        lcct.APPNTSEX,
        lcct.APPNTBIRTHDAY,
        lcct.APPNTIDTYPE,
        lcct.APPNTIDTYPENAME,
        lcct.APPNTIDNO,
        lcct.APPNTIDEXPDATE,
        lcct.APPNTADDRESS,
        lcct.APPNTPHONE,
        lcct.APPNTOCCUPATIONNAME,
        lcct.APPNTNATIVEPLACE,
        lcct.RELATIONTOINSUREDNAME,
        lcct.BNFNUM,
        lcct.INSUREDNO,
        lcct.INSUREDNAME,
        lcct.INSUREDSEX,
        lcct.INSUREDBIRTHDAY,
        lcct.INSUREDIDTYPE,
        lcct.INSUREDIDNO,
        lcct.PAYINTV,
        lcct.PAYMODE,
        lcct.PAYLOCATION,
        lcct.DISPUTEDFLAG,
        lcct.OUTPAYFLAG,
        lcct.GETPOLMODE,
        lcct.SIGNCOM,
        lcct.SIGNDATE,
        lcct.SIGNTIME,
        lcct.CONSIGNNO,
        lcct.BANKCODE,
        lcct.BANKACCNO,
        lcct.ACCNAME,
        lcct.PRINTCOUNT,
        lcct.LOSTTIMES,
        lcct.LANG,
        lcct.CURRENCY,
        lcct.REMARK,
        lcct.PEOPLES,
        lcct.MULT,
        lcct.PREM,
        lcct.AMNT,
        lcct.SUMPREM,
        lcct.DIF,
        lcct.PAYTODATE,
        lcct.FIRSTPAYDATE,
        lcct.CVALIDATE,
        lcct.INPUTOPERATOR,
        lcct.INPUTDATE,
        lcct.INPUTTIME,
        lcct.APPROVEFLAG,
        lcct.APPROVECODE,
        lcct.APPROVEDATE,
        lcct.APPROVETIME,
        lcct.UWFLAG,
        lcct.UWOPERATOR,
        lcct.UWDATE,
        lcct.UWTIME,
        lcct.APPFLAG,
		CASE
            WHEN temp_lwmn.MISSIONPROP1 is not null THEN
             '投保'
            WHEN lcct.APPFLAG = '0' AND lcct.UWFLAG <> 'a' AND temp_lcil.CONTNO is not null THEN
             '问题件'
            WHEN lcct.APPFLAG = '0' AND lcct.UWFLAG <> 'a' AND temp_lcpe.CONTNO is not null THEN
             '问题件'
            WHEN lcct.APPFLAG = '0' AND lcct.UWFLAG <> 'a' AND temp_lcrt.CONTNO is not null THEN
             '问题件'
            WHEN temp_lwn.MISSIONPROP1 is not null THEN
             '核保中'
            WHEN lcct.APPFLAG = '0' AND lcct.UWFLAG <> 'a' AND temp_lw.MISSIONPROP1 is not null AND
                 temp_ljte.OTHERNO is null  THEN
             '核保完成'
            WHEN temp_ln.MISSIONPROP1 is null AND
                 lcct.APPFLAG = '0' AND lcct.UWFLAG = '9' THEN
             '核保完成'
            WHEN lcct.APPFLAG = '1' AND lcct.CUSTOMGETPOLDATE IS NULL THEN
             '已承保'
            WHEN lcct.APPFLAG = '1' AND lcct.CUSTOMGETPOLDATE IS NOT NULL THEN
             '已回执回销'
            WHEN temp_feee.CONTNO is not null THEN
             '犹豫期退保'
            WHEN lcct.APPFLAG = '4' AND temp_llce.CONTNO is not null THEN
             '理赔终止'
            WHEN lcct.APPFLAG = '4' AND temp_feee.CONTNO is null AND temp_llce.CONTNO is null THEN
             '退保终止'
            WHEN temp_lcct.re = '1' THEN
             '撤单'
        END CONTSTATE,
        lcct.POLAPPLYDATE,
        lcct.GETPOLDATE,
        lcct.GETPOLTIME,
        lcct.CUSTOMGETPOLDATE,
        lcct.STATE,
        lcct.LCOPERATOR,
        lcct.LCMAKEDATE,
        lcct.LCMAKETIME,
        lcct.LCMODIFYDATE,
        lcct.LCMODIFYTIME,
        lcct.FIRSTTRIALOPERATOR,
        lcct.FIRSTTRIALDATE,
        lcct.FIRSTTRIALTIME,
        lcct.RECEIVEOPERATOR,
        lcct.RECEIVEDATE,
        lcct.RECEIVETIME,
        lcct.TEMPFEENO,
        lcct.SELLTYPE,
        lcct.FORCEUWFLAG,
        lcct.FORCEUWREASON,
        lcct.NEWBANKCODE,
        lcct.NEWBANKACCNO,
        lcct.NEWACCNAME,
        lcct.NEWPAYMODE,
        lcct.AGENTBANKCODE,
        lcct.BANKAGENT,
        lcct.AUTOPAYFLAG,
        lcct.RNEWFLAG,
        lcct.FAMILYCONTNO,
        lcct.BUSSFLAG,
        lcct.SIGNNAME,
        lcct.ORGANIZEDATE,
        lcct.ORGANIZETIME,
        lcct.NEWAUTOSENDBANKFLAG,
        lcct.AGENTCODEOPER,
        lcct.AGENTCODEASSI,
        lcct.DELAYREASONCODE,
        lcct.DELAYREASONDESC,
        lcct.XQREMINDFLAG,
        lcct.ORGANCOMCODE,
        lcct.BANKNETID,
        lcct.NEWBANKNETID,
        lcct.ARBITRATIONORG,
        lcct.BATTYPE,
        lcct.REMITTEDPARTNO,
        lcct.CERTIFICATENO,
        lcct.DEDUPREMFROMACC,
        lcct.SELFPOLFLAG,
        lcct.SALEWEBNETNAME,
        lcct.CONFIRMATIONNO,
        lcct.RECIPROCALDATE,
        lcct.RECIPROCALFLAG,
        lcct.CVALITIME,
        lcct.REVISITTIME,
        lcct.SURROUNDSYSFLAG,
        lcct.NETAMNT,
        lcct.TAX,
        lcct.REVISITDATE,
        CASE
         WHEN temp_lccr.CONTNO is not null THEN
          '否'
         WHEN temp_lcur.CONTNO is not null THEN
          '否'
         ELSE
          '是'
        END UWPASSFLAG,
        nvl(temp_lccu.CODENAME,temp_lccu1.CODENAME) UWRESULT,
        temp_lcil1.re as FIRSTSENDDATE,
        temp_lcil1.re1 as LASTREPLYDATE,
        if(temp_lcpe1.contno is not null,'有','无') PHYEXAMFLAG, 
        CASE
         WHEN temp_lyrb.POLNO is not null THEN
          '转账成功'
         WHEN temp_lyrb.POLNO is null AND lcct.APPFLAG IN ('1', '4') THEN
          '实时收费成功'
         WHEN temp_lyrb1.POLNO is not null AND temp_lyrb.POLNO is null AND lcct.APPFLAG = '0' THEN
          '转账失败'
         WHEN temp_ljsy.OTHERNO is not null THEN
          '已抽盘'
         WHEN temp_lyrb2.POLNO  is null AND temp_lcf.CONTNO is not null THEN
          '待实时扣费'
         WHEN temp_lyrb2.POLNO is null AND temp_lcf0.CONTNO is not null THEN
          '待抽盘'
        END TRANSSTATUS,
        CASE
         WHEN temp_lyrb.POLNO is null THEN
          temp_bang.codename
        END FAILREASON,
        CASE
           WHEN temp_lacm.AGENTCOM is not null THEN
             '中介'
           WHEN temp_lacm.AGENTCOM is null THEN
            CASE
              WHEN temp_laob.CONTNO is not null THEN
                temp_btype.CODENAME
            ELSE
                temp_btype1.CODENAME
            END
        END CHANNEL,
        temp_lpem.smoney as ADDPREM,
        temp_lpem1.EDORVALIDATE as REDUCLEARDATE,
        CASE
           WHEN temp_lpem2.CONTNO is not null THEN
              '是'
           ELSE
              '否'
        END WTFLAG,
        lcct.UWPASSDATE,
        lcct.FYC,
        lcct.RETURNSCANDATE,
        lcct.LOCONTNO,
        lcct.LOOPERATOR,
        lcct.LOMAKEDATE,
        lcct.LOMAKETIME,
        lcct.LOMODIFYDATE,
        lcct.LOMODIFYTIME,
        lcct.VIN,
        lcct.PAYFORM,
        lcct.CONTFORM1,
        lcct.CONTFORM2,
        lcct.YBTASYNFLAG,
        lcct.VISAFLAG,
        lcct.CROSSSALETYPE,
        lcct.DISCOUNTFLAG,
        lcct.AGENTFLAG,
        lcct.AUDIOVIDEOFLAG,
        lcct.NEEDAUDIOVIDEOFLAG,
        lcct.QUALITYRESLUT,
        lcct.FAILQUALITYREASON,
        lcct.SENDRESULT,
        lcct.SOCIALSALEFLAG,
        lcct.SELLERNO,
        lcct.YDZYDISCOUNTFLAG,
        lcct.ALIAS,
        lcct.BUSYTYPE,
        lcct.CONTNO,
        lcct.PRTFLAG,
        lcct.PRTTIMES,
        lcct.MANAGECOM,
        lcct.OPERATOR,
        lcct.MAKEDATE,
        lcct.MAKETIME,
        lcct.MODIFYDATE,
        lcct.MODIFYTIME,
        lcct.CONTINFO,
        lcct.CONTTYPE,
		lcct.SALESCODE,
		lcct.UWIDEA,
		lcct.DEPARTAGENTGROUP,
		lcct.GROUPAGENTGROUP,
        lcct.OGGACTION,
		lcct.OGGDATE,
		lcct.PushDate,
		lcct.PushTime		
	FROM soochow_data.dwd_LCCONTinfo lcct 
	left join (SELECT MIN(EDORNO) as edorno ,CONTNO FROM soochow_data.ods_lis_LAORPHANPOLICYB  group by CONTNO) temp_laob
      on temp_laob.CONTNO = lcct.LCCONTNO
    left join  soochow_data.ods_lis_LAORPHANPOLICYB laob 
      on laob.CONTNO = lcct.LCCONTNO and laob.EDORNO=temp_laob.edorno
	left join (select MISSIONPROP1 from soochow_data.ods_lis_LWMISSION 
                  where ACTIVITYID 
				  IN ('0000001098', '0000001099', '0000001001') group by MISSIONPROP1) temp_lwmn
      on temp_lwmn.MISSIONPROP1 = lcct.PRTNO				  
    left join (SELECT CONTNO
                     FROM soochow_data.ods_lis_LCISSUEPOL 
                    WHERE 1=1
                      AND NEEDPRINT = 'Y'
                      AND REPLYDATE IS NULL group by CONTNO) temp_lcil
      on temp_lcil.CONTNO = lcct.LCCONTNO				  
    left join(SELECT CONTNO
                     FROM soochow_data.ods_lis_LCPENOTICE 
                    WHERE 1=1
                      AND REPEDATE IS NULL group by CONTNO)	temp_lcpe   
      on temp_lcpe.CONTNO = lcct.LCCONTNO
    left join (SELECT CONTNO
                     FROM soochow_data.ods_lis_LCRREPORT 
                    WHERE 1=1
                      AND REPLYDATE IS NULL group by CONTNO) temp_lcrt
      on temp_lcrt.CONTNO = lcct.LCCONTNO			  
    left join (SELECT MISSIONPROP1
                     FROM soochow_data.ods_lis_LWMISSION LW
                    WHERE 1=1
                      AND ACTIVITYID IN ('0000001003', '0000001100') group by MISSIONPROP1 ) temp_lwn
      on temp_lwn.MISSIONPROP1 = lcct.PRTNO	   
    left join (SELECT MISSIONPROP1
                     FROM soochow_data.ods_lis_LWMISSION LW
                    WHERE 1=1
                      AND ACTIVITYID IN ('0000001150', '0000001149') group by MISSIONPROP1 ) temp_lw
      on temp_lw.MISSIONPROP1 = lcct.PRTNO	   
    left join(SELECT LJ.OTHERNO as OTHERNO FROM soochow_data.ods_lis_LJTEMPFEE LJ group by OTHERNO)	temp_ljte
      on temp_ljte.OTHERNO = lcct.PRTNO   
    left join (SELECT MISSIONPROP1 FROM soochow_data.ods_lis_LWMISSION group by MISSIONPROP1 ) temp_ln	
     on  temp_ln.MISSIONPROP1 = lcct.PRTNO	 
    left join(SELECT LP.CONTNO as CONTNO
                     FROM soochow_data.ods_lis_LJAGETENDORSE LJ, soochow_data.ods_lis_LCPOL LP
                    WHERE LJ.FEEOPERATIONTYPE = 'WT'
                      AND LJ.FEEFINATYPE = 'TF'
                      AND LP.POLNO = LP.MAINPOLNO
                      AND LJ.POLNO = LP.POLNO group by LP.CONTNO) temp_feee
      on temp_feee.CONTNO = lcct.LCCONTNO
    left join (SELECT CONTNO FROM soochow_data.ods_lis_LLCONTSTATE  group by CONTNO) temp_llce
      on temp_llce.CONTNO = lcct.LCCONTNO
    left join (SELECT '1' re,CONTNO
                     FROM soochow_data.ods_lis_LCCONT
                    WHERE 1=1
                      AND UWFLAG IN ('a', '1') group by CONTNO) temp_lcct
      on temp_lcct.CONTNO = lcct.LCCONTNO
	left join (SELECT CONTNO FROM soochow_data.ods_lis_LCCUWERROR 
					WHERE UWRULECODE <> 'IU5071'
					group by CONTNO) temp_lccr 
      on temp_lccr.CONTNO = lcct.LCCONTNO
    left join (SELECT CONTNO FROM soochow_data.ods_lis_LCUWERROR 
					WHERE UWRULECODE <> 'IU5071'
					group by CONTNO) temp_lcur 
      on temp_lcur.CONTNO = lcct.LCCONTNO
	left join(SELECT MAX(H.UWNO) as UWNO ,H.CONTNO as CONTNO FROM soochow_data.ods_lis_LCCUWSUB H
                             WHERE 1=1
                               AND H.AUTOUWFLAG = '2' group by H.CONTNO) temp_lccb
      on temp_lccb.CONTNO=lcct.LCCONTNO
    left join(select lccb.CONTNO as CONTNO,lccb.UWNO as UWNO,ld.CODENAME as CODENAME from soochow_data.ods_lis_LCCUWSUB lccb left join soochow_data.ods_lis_LDCODE ld 
           on  ld.CODETYPE = 'uwstate1'
                AND ld.CODE = lccb.PASSFLAG where lccb.AUTOUWFLAG = '2') temp_lccu
      on temp_lccu.CONTNO = lcct.LCCONTNO
        and temp_lccu.UWNO = temp_lccb.UWNO
    left join(SELECT MAX(H.UWNO) as UWNO ,H.CONTNO as CONTNO FROM soochow_data.ods_lis_LCCUWSUB H
                             WHERE 1=1
                               AND H.AUTOUWFLAG = '1' group by H.CONTNO) temp_lccb1
      on temp_lccb1.CONTNO=lcct.LCCONTNO
    left join(select lccb.CONTNO as CONTNO,lccb.UWNO as UWNO,ld.CODENAME as CODENAME from soochow_data.ods_lis_LCCUWSUB lccb left join soochow_data.ods_lis_LDCODE ld 
           on  ld.CODETYPE = 'uwstate1'
                AND ld.CODE = lccb.PASSFLAG where lccb.AUTOUWFLAG = '1') temp_lccu1
      on temp_lccu1.CONTNO = lcct.LCCONTNO
       and temp_lccu1.UWNO = temp_lccb1.UWNO
	left join(SELECT MIN(MAKEDATE) re ,MAX(REPLYDATE) re1, CONTNO FROM soochow_data.ods_lis_LCISSUEPOL group by CONTNO) temp_lcil1
	  on temp_lcil1.CONTNO=lcct.LCCONTNO
	left join (SELECT CONTNO FROM soochow_data.ods_lis_LCPENOTICE  group by CONTNO) temp_lcpe1 
      on temp_lcpe1.CONTNO = lcct.LCCONTNO
	left join (SELECT LYB.POLNO as POLNO
                FROM soochow_data.ods_lis_LYRETURNFROMBANKB LYB
                WHERE 1=1
                  AND LYB.BANKSUCCFLAG = '0000'
				group by POLNO ) temp_lyrb 
      on temp_lyrb.POLNO = lcct.PRTNO
    left join (SELECT LYB.POLNO as POLNO
                    FROM soochow_data.ods_lis_LYRETURNFROMBANKB LYB
                    WHERE 1=1
                      AND LYB.BANKSUCCFLAG <> '0000'
    				group by POLNO ) temp_lyrb1
      on temp_lyrb1.POLNO = lcct.PRTNO
    left join (SELECT LS.OTHERNO as OTHERNO
                     FROM soochow_data.ods_lis_LJSPAY LS
                    WHERE 1=1
                      AND LS.BANKSUCCFLAG = '1'
    				group by OTHERNO) temp_ljsy
      on temp_ljsy.OTHERNO = lcct.PRTNO
    left join (SELECT LYB.POLNO as POLNO
                    FROM soochow_data.ods_lis_LYRETURNFROMBANKB LYB
    				group by POLNO ) temp_lyrb2
     on  temp_lyrb2.POLNO = lcct.PRTNO
    left join (SELECT LCF.CONTNO as CONTNO
                     FROM soochow_data.ods_lis_LCCONTOTHERINFO LCF
                    WHERE 1=1
                      AND LCF.PAYFORM = '1' group by LCF.CONTNO) temp_lcf 
      on  temp_lcf.CONTNO = lcct.PRTNO
    left join (SELECT LCF.CONTNO as CONTNO
                     FROM soochow_data.ods_lis_LCCONTOTHERINFO LCF
                    WHERE 1=1
                      AND LCF.PAYFORM = '0' group by LCF.CONTNO) temp_lcf0 
      on  temp_lcf0.CONTNO = lcct.PRTNO
	left join (SELECT ld.codename as codename ,LYB.POLNO as POLNO, row_number() over(partition by LYB.POLNO order by LYB.POLNO) rank FROM soochow_data.ods_lis_LYRETURNFROMBANKB LYB left join soochow_data.ods_lis_LDCODE ld on LD.CODETYPE = 'bankerror'
                      AND LD.CODE = LYB.BANKSUCCFLAG WHERE 1=1
              AND LYB.BANKSUCCFLAG <> '0000' ) temp_bang
      on temp_bang.rank=1 and temp_bang.POLNO=lcct.PRTNO
	left join(SELECT AGENTCOM
                 FROM soochow_data.ods_lis_LACOM 
                WHERE 1=1
                  AND ACTYPE <> '01' group by AGENTCOM ) temp_lacm
      on temp_lacm.AGENTCOM = lcct.AGENTCOM
    left join(select LD.CODENAME as CODENAME ,LA.AGENTCODE from soochow_data.ods_lis_LAAGENT LA left join soochow_data.ods_lis_LDCODE LD
	                   on LD.CODE = LA.BRANCHTYPE
                       WHERE LD.CODETYPE = 'branchtype'
                         ) temp_btype
      on temp_btype.AGENTCODE =laob.AGENTCODE
    left join(select LD.CODENAME as CODENAME ,LA.AGENTCODE from soochow_data.ods_lis_LAAGENT LA left join soochow_data.ods_lis_LDCODE LD
	                   on LD.CODE = LA.BRANCHTYPE
                       WHERE LD.CODETYPE = 'branchtype'
                         ) temp_btype1
      on temp_btype1.AGENTCODE =lcct.AGENTCODE 
	left join (SELECT SUM(LP.GETMONEY) smoney,CONTNO
          FROM soochow_data.ods_lis_LPEDORITEM LP
              WHERE 1=1
               AND LP.EDORTYPE = 'AP'
               AND LP.EDORSTATE = '0' group by CONTNO) temp_lpem
      on temp_lpem.CONTNO = lcct.LCCONTNO
    left join (SELECT LP.EDORVALIDATE as EDORVALIDATE ,LP.CONTNO as CONTNO ,row_number() over(partition by CONTNO order by CONTNO) rank
          FROM soochow_data.ods_lis_LPEDORITEM LP
              WHERE 1=1
               AND LP.EDORTYPE = 'PU'
               AND LP.EDORSTATE = '0') temp_lpem1
      on temp_lpem1.rank=1 and temp_lpem1.CONTNO = lcct.LCCONTNO
	left join (SELECT LP.CONTNO as CONTNO 
          FROM soochow_data.ods_lis_LPEDORITEM LP
              WHERE 1=1
               AND LP.EDORTYPE = 'WT'
               AND LP.EDORSTATE = '0' group by LP.CONTNO) temp_lpem2
      on temp_lpem2.CONTNO = lcct.LCCONTNO

"
	  
	  
if [ $? -ne 0 ]; then
  #异常退出
  echo "从step_2:ods 提数据到dwd表dwd_LCCONTINFO失败"
   exit 1
  else
  echo "从step_2:从ods 提数据到dwd表dwd_LCCONTINFO成功"
fi
	  
