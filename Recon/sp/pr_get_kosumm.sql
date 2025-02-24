CREATE DEFINER=`root`@`%` PROCEDURE `pr_get_kosumm`(
  in in_recon_code text,
  in in_period_from date,
  in in_period_to date,
  in in_ip_addr varchar(255),
  in in_user_code varchar(32),
  in in_conversion_type varchar(2),
  in in_dataset_formt varchar(25),
  out out_msg text,
  out out_result int
)
me:BEGIN
  /*
    Created By : Vijayavel
    Created Date : 24-11-2023

    Updated By : Muthu
    updated Date : 21-02-2025

    Version : 2
  */

  declare v_rptsession_gid int default 0;
  declare v_rec_count int default 0;
  declare v_condition text default '';
  declare v_recontype text default '';

  /*
  DECLARE EXIT HANDLER FOR SQLEXCEPTION
  BEGIN
    GET DIAGNOSTICS CONDITION 1 @sqlstate = RETURNED_SQLSTATE,
    @errno = MYSQL_ERRNO, @text = MESSAGE_TEXT;

    set @text = concat(@text,' ',err_msg);

    SET @full_error = CONCAT("ERROR ", @errno, " (", @sqlstate, "): ", @text);

    ROLLBACK;

    set out_msg = @full_error;
    set out_result = 0;
  END;
  */
  select recontype_code into v_recontype from recon_mst_trecon where recon_code = in_recon_code;
  if (v_recontype = 'W') then	
	call pr_get_kosummary_proof (in_recon_code,in_period_from,in_period_to,in_ip_addr,in_user_code,in_conversion_type,in_dataset_formt,@out_msg, @out_result);
    -- select @out_msg, @out_result;
  else if(v_recontype = 'N') then
	call pr_get_kosummary_nonvaluebase (in_recon_code,in_period_from,in_period_to,in_ip_addr,in_user_code,in_conversion_type,in_dataset_formt,@out_msg, @out_result);
  else if(v_recontype = 'V') then
	call pr_get_kosummary_valuebase (in_recon_code,in_period_from,in_period_to,in_ip_addr,in_user_code,in_conversion_type,in_dataset_formt,@out_msg, @out_result);
  else if(in_dataset_formt = "format2") then
      call pr_get_kosummPivot (in_recon_code,in_period_from,in_period_to,in_ip_addr,in_user_code,in_conversion_type,in_dataset_formt,@out_msg, @out_result);
  else
  begin
  drop temporary table if exists recon_tmp_tkodtl;
  drop temporary table if exists recon_tmp_treconcode;
  drop temporary table if exists recon_tmp_tkosumm;
  drop temporary table if exists recon_tmp_tkosumm1;

  create temporary table recon_tmp_tkodtl
  (
    kodtl_gid int not null,
    recon_code varchar(32) default null,
    recon_name text default null,
    rule_code varchar(32) default null,
    rule_name text default null,
    rule_order decimal(9,2),
    tran_gid int not null default 0,
    dataset_code varchar(32) default null,
    tran_acc_mode char(1) default null,
    tran_mult tinyint not null default 0,
    manual_matchoff char(1) default null,
    ko_value double(15,2) default null,
    key idx_recon_code (recon_code),
    key idx_tran_gid (tran_gid),
    key idx_rule_order (rule_order),
    key idx_manual_matchoff (manual_matchoff),
    PRIMARY KEY (kodtl_gid)
  ) ENGINE = MyISAM;

  create temporary table recon_tmp_treconcode
  (
    recon_code varchar(32) not null,
    recon_name text default null,
    PRIMARY KEY (recon_code)
  ) ENGINE = MyISAM;

  create temporary table recon_tmp_tkosumm
  (
    kosumm_gid int not null AUTO_INCREMENT,
    recon_code varchar(32) default null,
    dataset_code varchar(32) default null,
    rec_slno int(10) NOT NULL default 0,
    row_desc text default null,
    dr_count int default null,
    dr_value double(15,2) default null,
    cr_count int default null,
    cr_value double(15,2) default null,
    tot_count int default null,
    tot_value double(15,2) default null,
    fontbold_flag char(1) not null default 'N',
    backcolor_flag char(1) default 'N',
    forecolor varchar(32) default null,
    backcolor varchar(32) default null,
    PRIMARY KEY (kosumm_gid)
  ) ENGINE = MyISAM;

  create temporary table recon_tmp_tkosumm1
  (
    kosumm_gid int not null,
    recon_code varchar(32) default null,
    dataset_code varchar(32) default null,
    rec_slno int(10) NOT NULL default 0,
    row_desc text default null,
    dr_count int default null,
    dr_value double(15,2) default null,
    cr_count int default null,
    cr_value double(15,2) default null,
    tot_count int default null,
    tot_value double(15,2) default null,
    fontbold_flag char(1) not null default 'N',
    backcolor_flag char(1) default 'N',
    forecolor varchar(32) default null,
    backcolor varchar(32) default null,
    PRIMARY KEY (kosumm_gid)
  ) ENGINE = MyISAM;

  insert into recon_tmp_treconcode
    select a.* from (
    select recon_code,recon_name from recon_mst_trecon
    where recon_code = in_recon_code
    and period_from <= curdate()
    and (period_to >= curdate()
    or until_active_flag = 'Y')
    and active_status = 'Y'
    and delete_flag = 'N'
    LOCK IN SHARE MODE) as a;

  -- generate condition
  set v_condition = concat(" and a.recon_code '",in_recon_code,"'
    and a.ko_date >= '",date_format(in_period_from,'%Y-%m-%d'),"'
    and a.ko_date <= '",date_format(in_period_to,'%Y-%m-%d'),"' ");

  insert into recon_tmp_tkodtl
  (
    kodtl_gid,recon_code,tran_gid,ko_value,manual_matchoff,recon_name,dataset_code,tran_acc_mode,tran_mult,rule_name,rule_order
  )
  select a.* from (
  select
    d.kodtl_gid,r.recon_code,d.tran_gid,d.ko_value,k.manual_matchoff,
    r.recon_name,t.dataset_code,if(d.ko_mult=-1,'D','C'),d.ko_mult,e.rule_name,e.rule_order
  from recon_tmp_treconcode as r
  inner join recon_trn_tko as k on r.recon_code = k.recon_code
  inner join recon_trn_tkodtl as d on k.ko_gid = d.ko_gid and d.delete_flag = 'N'
  inner join recon_trn_ttran as t on d.tran_gid = t.tran_gid and t.delete_flag = 'N'
  left join recon_mst_trule as e on k.rule_code = e.rule_code and e.delete_flag = 'N'
  where k.ko_date >= in_period_from
  and k.ko_date <= in_period_to
  and k.delete_flag = 'N'
  order by e.rule_order
  LOCK IN SHARE MODE) as a;

  insert into recon_tmp_tkodtl
  (
    kodtl_gid,recon_code,tran_gid,ko_value,manual_matchoff,recon_name,dataset_code,tran_acc_mode,tran_mult,rule_name,rule_order
  )
  select a.* from (
  select
    d.kodtl_gid,r.recon_code,d.tran_gid,d.ko_value,k.manual_matchoff,
    r.recon_name,t.dataset_code,if(d.ko_mult=-1,'D','C'),d.ko_mult,e.rule_name,e.rule_order
  from recon_tmp_treconcode as r
  inner join recon_trn_tko as k on r.recon_code = k.recon_code
  inner join recon_trn_tkodtl as d on k.ko_gid = d.ko_gid and d.delete_flag = 'N'
  inner join recon_trn_ttranko as t on d.tran_gid = t.tran_gid and t.delete_flag = 'N'
  left join recon_mst_trule as e on k.rule_code = e.rule_code and e.delete_flag = 'N'
  where k.ko_date >= in_period_from
  and k.ko_date <= in_period_to
  and k.delete_flag = 'N'
  order by e.rule_order
  LOCK IN SHARE MODE) as a;

  /*
  insert into recon_tmp_tkodtl
  (
    kodtl_gid,recon_code,tran_gid,ko_value,manual_matchoff,recon_name,dataset_code,tran_acc_mode,tran_mult,rule_name,rule_order
  )
  select
    d.kodtl_gid,r.recon_code,d.tran_gid,d.ko_value,k.manual_matchoff,
    r.recon_name,t.dataset_code,t.tran_acc_mode,t.tran_mult,e.rule_name,e.rule_order
  from recon_tmp_treconcode as r
  inner join recon_trn_tko as k on r.recon_code = k.recon_code
  inner join recon_trn_tkodtl as d on k.ko_gid = d.ko_gid and d.tranbrkp_gid = 0 and d.delete_flag = 'N'
  inner join recon_trn_ttran as t on d.tran_gid = t.tran_gid and t.delete_flag = 'N'
  left join recon_mst_trule as e on k.rule_code = e.rule_code and e.delete_flag = 'N'
  where k.ko_date >= in_period_from
  and k.ko_date <= in_period_to
  and k.delete_flag = 'N'
  order by e.rule_order;

  insert into recon_tmp_tkodtl
  (
    kodtl_gid,recon_code,tran_gid,ko_value,manual_matchoff,recon_name,dataset_code,tran_acc_mode,tran_mult,rule_name,rule_order
  )
  select
    d.kodtl_gid,r.recon_code,d.tran_gid,d.ko_value,k.manual_matchoff,
    r.recon_name,t.dataset_code,t.tran_acc_mode,t.tran_mult,e.rule_name,e.rule_order
  from recon_tmp_treconcode as r
  inner join recon_trn_tko as k on r.recon_code = k.recon_code
  inner join recon_trn_tkodtl as d on k.ko_gid = d.ko_gid and d.tranbrkp_gid = 0 and d.delete_flag = 'N'
  inner join recon_trn_ttranko as t on d.tran_gid = t.tran_gid and t.delete_flag = 'N'
  left join recon_mst_trule as e on k.rule_code = e.rule_code and e.delete_flag = 'N'
  where k.ko_date >= in_period_from
  and k.ko_date <= in_period_to
  and k.delete_flag = 'N'
  order by e.rule_order;

  insert into recon_tmp_tkodtl
  (
    kodtl_gid,recon_code,tran_gid,ko_value,manual_matchoff,recon_name,dataset_code,tran_acc_mode,tran_mult,rule_name,rule_order
  )
  select
    d.kodtl_gid,r.recon_code,d.tran_gid,d.ko_value,k.manual_matchoff,
    r.recon_name,t.dataset_code,t.tran_acc_mode,t.tran_mult,e.rule_name,e.rule_order
  from recon_tmp_treconcode as r
  inner join recon_trn_tko as k on r.recon_code = k.recon_code
  inner join recon_trn_tkodtl as d on k.ko_gid = d.ko_gid and d.tranbrkp_gid > 0 and d.delete_flag = 'N'
  inner join recon_trn_ttranbrkpko as t on d.tran_gid = t.tran_gid and d.tranbrkp_gid = t.tranbrkp_gid and t.delete_flag = 'N'
  left join recon_mst_trule as e on k.rule_code = e.rule_code and e.delete_flag = 'N'
  where k.ko_date >= in_period_from
  and k.ko_date <= in_period_to
  and k.delete_flag = 'N'
  order by e.rule_order;
  */

  -- run KO Report
  -- call pr_run_pagereport('RPT_KO',-1,v_condition,false,in_ip_addr,in_user_code,v_rec_count,@msg,v_rptsession_gid);

  -- insert in ko summary
  -- insert recon_name
  set @row_slno = 0;

  insert into recon_tmp_tkosumm
  (
    rec_slno,
    recon_code,
    row_desc,
    fontbold_flag,
    backcolor_flag,
    forecolor,
    backcolor
  )
  select
    distinct @row_slno = @row_slno + 1,
    recon_code,
    recon_name,
    'Y',
    'Y',
    'Red',
    'Yellow'
  from recon_tmp_tkodtl;

  -- insert summary by acc_no wise
  insert into recon_tmp_tkosumm
  (
    rec_slno,
    recon_code,
    dataset_code,
    row_desc,
    forecolor
  )
  select
    @row_slno = @row_slno + 1,
    a.recon_code,
    a.dataset_code,
    fn_get_datasetname(a.dataset_code),
    'Blue'
  from (
    select recon_code,dataset_code from recon_tmp_tkodtl
    group by recon_code,dataset_code) as a;

  -- insert rule based
  insert into recon_tmp_tkosumm
  (
    rec_slno,
    recon_code,
    dataset_code,
    row_desc,
    dr_count,
    dr_value,
    cr_count,
    cr_value,
    tot_count,
    tot_value
  )
  select
    @row_slno = @row_slno + 1,
    recon_code,
    dataset_code,
    concat('  ',rule_name),
    count(distinct if(tran_acc_mode = 'D',tran_gid,null)) as dr_count,
    sum(if(tran_acc_mode = 'D',ko_value,0)) as dr_value,
    count(distinct if(tran_acc_mode = 'C',tran_gid,null)) as cr_count,
    sum(if(tran_acc_mode = 'C',ko_value,0)) as cr_value,
    count(distinct tran_gid),
    abs(sum(ko_value*tran_mult))
    -- sum(ko_value)
  from recon_tmp_tkodtl
  where manual_matchoff = 'N'
  group by recon_code,dataset_code,rule_order;

  -- insert manual
  insert into recon_tmp_tkosumm
  (
    rec_slno,
    recon_code,
    dataset_code,
    row_desc,
    dr_count,
    dr_value,
    cr_count,
    cr_value,
    tot_count,
    tot_value
  )
  select
    @row_slno = @row_slno + 1,
    recon_code,
    dataset_code,
    '  Manual KO' as matchoff_type,
    count(distinct if(tran_acc_mode = 'D',tran_gid,null)) as dr_count,
    sum(if(tran_acc_mode = 'D',ko_value,0)) as dr_value,
    count(distinct if(tran_acc_mode = 'C',tran_gid,null)) as cr_count,
    sum(if(tran_acc_mode = 'C',ko_value,0)) as cr_value,
    count(distinct tran_gid),
    abs(sum(ko_value*tran_mult))
    -- sum(ko_value)
  from recon_tmp_tkodtl
  where manual_matchoff = 'Y'
  group by recon_code,dataset_code,matchoff_type;

  insert into recon_tmp_tkosumm1 select * from recon_tmp_tkosumm where dr_count is not null;

  -- reconaccwise total
  insert into recon_tmp_tkosumm
  (
    rec_slno,
    recon_code,
    dataset_code,
    row_desc,
    dr_count,
    dr_value,
    cr_count,
    cr_value,
    tot_count,
    tot_value,
    fontbold_flag,
    backcolor_flag,
    forecolor,
    backcolor
  )
  select
    @row_slno = @row_slno + 1,
    recon_code,
    dataset_code,
    'Sub Total',
    sum(dr_count) as dr_count,
    sum(dr_value) as dr_value,
    sum(cr_count) as cr_count,
    sum(cr_value) as cr_value,
    sum(dr_count)+sum(cr_count),
    abs(sum(dr_value)-sum(cr_value)),
    'Y',
    'Y',
    'Red',
    'Yellow'
  from recon_tmp_tkosumm1
  group by recon_code,dataset_code;

  /*
  insert into recon_tmp_tkosumm
  (
    rec_slno,
    recon_code,
    dataset_code,
    row_desc,
    dr_count,
    dr_value,
    cr_count,
    cr_value,
    tot_count,
    tot_value,
    fontbold_flag,
    backcolor_flag,
    forecolor,
    backcolor
  )
  select
    @row_slno = @row_slno + 1,
    recon_code,
    dataset_code,
    'Sub Total',
    count(distinct if(tran_acc_mode = 'D',tran_gid,null)) as dr_count,
    sum(if(tran_acc_mode = 'D',ko_value,0)) as dr_value,
    count(distinct if(tran_acc_mode = 'C',tran_gid,null)) as cr_count,
    sum(if(tran_acc_mode = 'C',ko_value,0)) as cr_value,
    count(distinct tran_gid),
    abs(sum(ko_value*tran_mult)),
    -- sum(ko_value),
    'Y',
    'Y',
    'Red',
    'Yellow'
  from recon_tmp_tkodtl
  group by recon_code,dataset_code;
  */

  -- insert blank line
  insert into recon_tmp_tkosumm
  (
    rec_slno,
    recon_code,
    dataset_code,
    row_desc,
    backcolor,
    forecolor
  )
  select
    @row_slno = @row_slno + 1,
    recon_code,
    'XXX9999999999999999',
    '',
    'White',
    'White'
  from recon_tmp_tkosumm1
  group by recon_code;

  -- grant total
  insert into recon_tmp_tkosumm
  (
    rec_slno,
    recon_code,
    row_desc,
    dr_count,
    dr_value,
    cr_count,
    cr_value,
    tot_count,
    tot_value,
    fontbold_flag,
    backcolor_flag,
    forecolor,
    backcolor
  )
  select
    @row_slno = @row_slno + 1,
    'ZZZ9999999',
    'Grant Total',
    sum(dr_count) as dr_count,
    sum(dr_value) as dr_value,
    sum(cr_count) as cr_count,
    sum(cr_value) as cr_value,
    sum(dr_count)+sum(cr_count),
    abs(sum(dr_value)-sum(cr_value)),
    -- sum(ko_value),
    'Y',
    'Y',
    'White',
    'Black'
  from recon_tmp_tkosumm1
  group by recon_code;

  /*
  insert into recon_tmp_tkosumm
  (
    rec_slno,
    recon_code,
    row_desc,
    dr_count,
    dr_value,
    cr_count,
    cr_value,
    tot_count,
    tot_value,
    fontbold_flag,
    backcolor_flag,
    forecolor,
    backcolor
  )
  select
    @row_slno = @row_slno + 1,
    '',
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null;

  select
    @row_slno = @row_slno + 1,
    'ZZZ9999999',
    'Grant Total',
    count(distinct if(tran_acc_mode = 'D',tran_gid,null)) as dr_count,
    sum(if(tran_acc_mode = 'D',ko_value,0)) as dr_value,
    count(distinct if(tran_acc_mode = 'C',tran_gid,null)) as cr_count,
    sum(if(tran_acc_mode = 'C',ko_value,0)) as cr_value,
    count(distinct tran_gid),
    abs(sum(ko_value*tran_mult)),
    -- sum(ko_value),
    'Y',
    'Y',
    'White',
    'Black'
  from recon_tmp_tkodtl;
  */

  -- return result
  select
    row_desc as 'Row Labels',
    dr_count as 'Dr Count',
    -- format(cast(ifnull(dr_value,0) as decimal(20,2)),2,'en_IN') as 'Dr Value',
    ifnull(dr_value,0) as 'Dr Value',
    (select fn_get_currency_format(ifnull(dr_value,0),'INR',in_conversion_type,'MIS')) as 'Formal Dr Value',
    cr_count as 'Cr Count',
    -- format(ifnull(cr_value,0),2,'en_IN') as 'Cr Value',
    ifnull(cr_value,0) as 'Cr Value',
    (select fn_get_currency_format(ifnull(cr_value,0),'INR',in_conversion_type,'MIS')) as 'Formal Cr Value',
    /*tot_count as 'Total Count',
    format(tot_value,2,'en_IN') as 'Total Value',*/
    case when dr_value > cr_value then
    ifnull(dr_value,0) - ifnull(cr_value,0) 
    when cr_value > dr_value then
    ifnull(cr_value,0) - ifnull(dr_value,0) end as 'Net Value',
    case when dr_value > cr_value then
    fn_get_currency_format(ifnull(dr_value,0) - ifnull(cr_value,0),'INR',in_conversion_type,'MIS')
    when cr_value > dr_value then
    fn_get_currency_format(ifnull(cr_value,0) - ifnull(dr_value,0),'INR',in_conversion_type,'MIS') end as 'Formal Net Value',
    ifnull(backcolor,'White') as backcolor,
    ifnull(forecolor,'Black') as forecolor,
    v_recontype as recontype
  from recon_tmp_tkosumm
  order by recon_code,dataset_code,rec_slno;

  /*drop temporary table if exists recon_tmp_treconcode;
  drop temporary table if exists recon_tmp_tkosumm;
  drop temporary table if exists recon_tmp_tkosumm1;
  drop temporary table if exists recon_tmp_tko;*/
  end;
  end if;
  end if;
  end if;
  end if;
end