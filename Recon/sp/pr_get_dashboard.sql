CREATE DEFINER=`root`@`%` PROCEDURE `pr_get_dashboard`(
  in in_recon_code text,
  in in_period_from date,
  in in_period_to date,
  in in_user_code varchar(32),
  in in_conversion_type varchar(5),
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

  declare v_recontype_code text default '';
  declare v_recon_count int default 0;
  declare v_dataset_count int default 0;
  declare v_tran_count int default 0;
  declare v_count int default 0;
  declare v_ko_count int default 0;
  declare v_ko_manual_count int default 0;
  declare v_ko_system_count int default 0;
  declare v_ko_partialexcp_count int default 0;
  declare v_ko_zeroexcp_count int default 0;
  declare v_openingexcp_count int default 0;
  declare v_excp_count int default 0;
  declare v_ko_value decimal(18,2) default 0.00;
  declare v_excp_value decimal(18,2) default 0.00;
  declare v_trnko_value decimal(18,2) default 0.00;
  declare v_openingexcp_value decimal(18,2) default 0.00;
  declare v_ko_partialexcp_value decimal(18,2) default 0.00;

  

  drop temporary table if exists recon_tmp_trecon;
  drop temporary table if exists recon_tmp_ttrangid;
  drop temporary table if exists recon_tmp_tkodtlgid;
  drop temporary table if exists recon_tmp_tgid;

  create temporary table recon_tmp_trecon
  (
    recon_code varchar(32) NOT NULL,
    PRIMARY KEY (recon_code)
  );

  create temporary table recon_tmp_ttrangid
  (
    tran_gid int(10) unsigned NOT NULL,
    tran_date date default null,
    tran_value double(15,2) not null default 0,
    excp_value double(15,2) not null default 0,
    roundoff_value double(15,2) not null default 0,
    tran_mult tinyint not null default 0,
    PRIMARY KEY (tran_gid),
    key idx_tran_date(tran_date),
    key idx_excp_value(excp_value)
  ) Engine = MyISAM;

  create temporary table recon_tmp_tkodtlgid
  (
    kodtl_gid int(10) unsigned NOT NULL,
    ko_gid int not null,
    ko_date date default null,
    manual_matchoff char(1) default null,
    tran_gid int not null,
    tran_date date default null,
    PRIMARY KEY (kodtl_gid),
    key idx_ko_date(ko_date),
    key idx_tran_date(tran_date)
  ) Engine = MyISAM;

  create temporary table recon_tmp_tgid
  (
    gid int(10) unsigned NOT NULL,
    PRIMARY KEY (gid)
  ) Engine = MyISAM;

  if in_recon_code = '' then
    insert into recon_tmp_trecon
    select b.* from (
      select a.recon_code from admin_mst_tusercontext as u
      inner join admin_mst_treconcontext as r on u.master_syscode = r.master_syscode
        and u.parent_master_syscode = r.parent_master_syscode
        and r.delete_flag = 'N'
      inner join recon_mst_trecon as a on r.recon_code = a.recon_code
        and a.active_status = 'Y'
        and a.period_from <= curdate()
        and (a.period_to >= curdate()
        or a.until_active_flag = 'Y')
        and a.delete_flag = 'N'
      where u.user_code = in_user_code
      and u.active_status = 'Y'
      and u.delete_flag = 'N' 
      LOCK IN SHARE MODE) as b;
  else
    insert into recon_tmp_trecon
    select a.* from (
      select recon_code from recon_mst_trecon
      where recon_code = in_recon_code
      and active_status = 'Y'
      and period_from <= curdate()
      and (period_to >= curdate()
      or until_active_flag = 'Y')
      and delete_flag = 'N'
      LOCK IN SHARE MODE) as a;
  end if;

  
  insert into recon_tmp_ttrangid
  (
    tran_gid,tran_date,tran_mult,tran_value,excp_value,roundoff_value
  )
  select a.* from(
  select t.tran_gid,t.tran_date,t.tran_mult,t.tran_value,t.excp_value,t.roundoff_value from recon_tmp_trecon as r
  inner join recon_mst_trecon as c on r.recon_code = c.recon_code
    and c.recontype_code in ('W','B','I')
    and c.delete_flag = 'N'
  inner join recon_trn_ttran as t on r.recon_code = t.recon_code
    and t.tran_date >= in_period_from
    and t.tran_date <= in_period_to
    and t.delete_flag = 'N'
    LOCK IN SHARE MODE) as a;

  insert into recon_tmp_ttrangid
  (
    tran_gid,tran_date,tran_mult,tran_value,excp_value,roundoff_value
  )
  select a.* from(
  select t.tran_gid,cast(s.insert_date as date),1 as tran_mult,1 as tran_value,1 as excp_value,t.roundoff_value from recon_tmp_trecon as r
  inner join recon_mst_trecon as c on r.recon_code = c.recon_code
    and c.recontype_code in ('V','N')
    and c.delete_flag = 'N'
  inner join recon_trn_ttran as t on r.recon_code = t.recon_code
    and t.delete_flag = 'N'
  inner join recon_trn_tscheduler as s on t.scheduler_gid = s.scheduler_gid
    and s.insert_date >= in_period_from
    and s.insert_date < date_add(in_period_to,interval 1 day)
    LOCK IN SHARE MODE) as a;

  
  insert into recon_tmp_ttrangid
  (
    tran_gid,tran_date,tran_mult,tran_value,excp_value,roundoff_value
  )
  select a.* from (
  select t.tran_gid,t.tran_date,t.tran_mult,t.tran_value,t.excp_value,t.roundoff_value from recon_tmp_trecon as r
  inner join recon_mst_trecon as c on r.recon_code = c.recon_code
    and c.recontype_code in ('W','B','I')
    and c.delete_flag = 'N'
  inner join recon_trn_ttranko as t on r.recon_code = t.recon_code
    and t.tran_date >= in_period_from
    and t.tran_date <= in_period_to
    and t.delete_flag = 'N'
    LOCK IN SHARE MODE) as a;

  insert into recon_tmp_ttrangid
  (
    tran_gid,tran_date,tran_mult,tran_value,excp_value,roundoff_value
  )
  select a.* from (
  select t.tran_gid,cast(s.insert_date as date),1 as tran_mult,0 as tran_value,0 as excp_value,0 as roundoff_value from recon_tmp_trecon as r
  inner join recon_mst_trecon as c on r.recon_code = c.recon_code
    and c.recontype_code in ('V','N')
    and c.delete_flag = 'N'
  inner join recon_trn_ttranko as t on r.recon_code = t.recon_code
    and t.delete_flag = 'N'
  inner join recon_trn_tscheduler as s on t.scheduler_gid = s.scheduler_gid
    and s.insert_date >= in_period_from
    and s.insert_date < date_add(in_period_to,interval 1 day)
    LOCK IN SHARE MODE) as a;

  
  insert into recon_tmp_ttrangid
  (
    tran_gid,tran_date,tran_mult,tran_value,excp_value,roundoff_value
  )
  select a.* from(
  select t.tran_gid,t.tran_date,t.tran_mult,t.tran_value,t.excp_value,t.roundoff_value from recon_tmp_trecon as r
  inner join recon_trn_ttran as t on r.recon_code = t.recon_code
    and t.tran_date < in_period_from
    and t.excp_value <> 0
    and (t.excp_value - t.roundoff_value * t.tran_mult) <> 0
    and t.delete_flag = 'N'
    LOCK IN SHARE MODE) as a;

  insert into recon_tmp_ttrangid
  (
    tran_gid,tran_date,tran_mult,tran_value,excp_value,roundoff_value
  )
  select a.* from (
  select t.tran_gid,t.tran_date,t.tran_mult,t.tran_value,t.excp_value,t.roundoff_value from recon_tmp_trecon as r
  inner join recon_trn_ttranko as t on r.recon_code = t.recon_code
    and t.tran_date < in_period_from
    and t.excp_value <> 0
    and (t.excp_value - t.roundoff_value * t.tran_mult) <> 0
    and t.delete_flag = 'N'
    LOCK IN SHARE MODE) as a;

  
  insert into recon_tmp_tkodtlgid
  (
    kodtl_gid,ko_gid,ko_date,manual_matchoff,tran_gid,tran_date
  )
  select a.* from (
  select
    d.kodtl_gid,k.ko_gid,k.ko_date,k.manual_matchoff,d.tran_gid,t.tran_date
  from recon_tmp_ttrangid as t
  inner join recon_trn_tkodtl as d on t.tran_gid = d.tran_gid and d.delete_flag = 'N'
  inner join recon_trn_tko as k on d.ko_gid = k.ko_gid and k.delete_flag = 'N'
  where t.tran_date >= in_period_from
  group by d.kodtl_gid,k.ko_gid,k.ko_date,k.manual_matchoff,d.tran_gid,t.tran_date
  LOCK IN SHARE MODE) as a;

  
  select count(*) into v_recon_count from recon_tmp_trecon;

  
  select
    count(distinct b.dataset_code) into v_dataset_count
  from recon_tmp_trecon as r
  inner join recon_mst_trecondataset as b on r.recon_code = b.recon_code
    and b.active_status = 'Y'
    and b.delete_flag = 'N'
  inner join recon_mst_tdataset as d on b.dataset_code = d.dataset_code
    and d.delete_flag = 'N'
    LOCK IN SHARE MODE;

  
  select count(*) into v_tran_count from recon_tmp_ttrangid
  where tran_date >= in_period_from;

  
  select count(*) into v_excp_count from recon_tmp_ttrangid
  where excp_value <> 0
  and (excp_value - roundoff_value * tran_mult) <> 0;

  set v_excp_count = ifnull(v_excp_count,0);

  
  select count(*) into v_openingexcp_count from recon_tmp_ttrangid
  where excp_value <> 0
  and (excp_value - roundoff_value * tran_mult) <> 0
  and tran_date < in_period_from;

  set v_openingexcp_count = ifnull(v_openingexcp_count,0);

  
  select count(distinct tran_gid) into v_ko_count from recon_tmp_tkodtlgid;

  
  insert into recon_tmp_tgid (gid)
  select distinct tran_gid from recon_tmp_tkodtlgid
  where tran_date >= in_period_from
    and tran_date <= in_period_to
    and manual_matchoff = 'Y';

  select count(*) into v_ko_manual_count from recon_tmp_tgid;

  
  select count(distinct tran_gid) into v_ko_system_count from recon_tmp_tkodtlgid
  where tran_date >= in_period_from
  and tran_date <= in_period_to
  and manual_matchoff = 'N'
  and tran_gid not in
  (
    select gid from recon_tmp_tgid
  );

  
  select count(distinct tran_gid) into v_ko_zeroexcp_count from recon_tmp_ttrangid
  where excp_value = 0;

  
  select count(distinct tran_gid) into v_ko_partialexcp_count from recon_tmp_ttrangid
  where excp_value <> 0
  and tran_value <> excp_value
  and (excp_value - roundoff_value * tran_mult) <> 0
  and tran_date >= in_period_from;

  
  select  v_recon_count          as recon_count,
          v_dataset_count        as dataset_count,
          v_tran_count           as tran_count,
          v_ko_count - v_ko_partialexcp_count as ko_count,
          v_ko_system_count      as ko_system_count,
          v_ko_manual_count      as ko_manual_count,
          v_excp_count - v_ko_partialexcp_count as excp_count,
          v_openingexcp_count    as opening_excp_count,
          v_ko_zeroexcp_count    as ko_zeroexcp_count,
          v_ko_partialexcp_count as ko_partialexcp_count;

  
  select
    '' as ko_month,
    v_ko_manual_count as manual_ko_count,
    v_ko_system_count as system_ko_count,
    v_ko_count as ko_count,
    '' as ko_month1;

  
  set v_count = v_excp_count;

  if v_count = 0 then
    set v_count = 1;
  end if;

  select
    ag.aging_desc,ifnull(ex.excp_count,0) as excp_count,ifnull(ex.excp_percent,0) as excp_percent
  from recon_mst_taging as ag
  left join
  (
    select
      c.aging_gid,
      c.aging_desc,
      count(*) as excp_count,
      cast((count(*)/v_count)*100 as decimal(6,2)) as excp_percent
    from recon_tmp_ttrangid as t
    right join recon_mst_taging as c on datediff(curdate(),t.tran_date) between c.aging_from and c.aging_to
      and c.delete_flag = 'N'
    where t.excp_value <> 0
    and (t.excp_value - t.roundoff_value * tran_mult) <> 0
    group by c.aging_gid,c.aging_desc
  ) as ex on ag.aging_gid = ex.aging_gid;
  
  
  
  
    
    call pr_get_dashboardvalue(in_recon_code, in_period_from, in_period_to,in_user_code,in_conversion_type, @out_msg, @out_result);

  drop temporary table if exists recon_tmp_tgid;
  drop temporary table if exists recon_tmp_ttrangid;
  drop temporary table if exists recon_tmp_trecon;
  drop temporary table if exists recon_tmp_tkodtlgid;
end