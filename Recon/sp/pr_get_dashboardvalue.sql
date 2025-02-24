CREATE DEFINER=`root`@`%` PROCEDURE `pr_get_dashboardvalue`(
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
    Created By : Muthu
    Created Date : 19-02-2025

    Updated By : Muthu
    updated Date : 21-02-2025

    Version : 2
  */
    
    declare v_ko_count bigint default 0;
    declare v_ko_value decimal(18,2) default 0;
    declare v_ko_expcount bigint default 0;
    declare v_ko_expvalue decimal(18,2) default 0;
    declare v_tranbal_count bigint default 0;
    declare v_tranbal_value decimal(18,2) default 0;
    declare v_tranbal_expcount bigint default 0;
    declare v_tranbal_expvalue decimal(18,2) default 0;
    declare v_totaltran_count bigint default 0;
    declare v_totaltran_value decimal(18,2) default 0;
    declare v_totaltran_expcount bigint default 0;
    declare v_totaltran_expvalue decimal(18,2) default 0;
    declare v_partial_count bigint default 0;
    declare v_partialmatch_value decimal(18,2) default 0;
    declare v_partialexp_value decimal(18,2) default 0;
    declare v_exp_count bigint default 0;
    declare v_exp_value decimal(18,2) default 0;
    declare v_openingexcp_count int default 0;
    declare v_openingexcp_value decimal(18,2) default 0.00;
    declare v_recontype_code text default '';
    
    drop temporary table if exists recon_tmp_ttran;
    create temporary table recon_tmp_ttran
	(
		tran_gid int(10) unsigned NOT NULL,
		tran_date date default null,
		tran_value double(15,2) not null default 0,
		excp_value double(15,2) not null default 0,
		roundoff_value double(15,2) not null default 0,
		tran_mult tinyint not null default 0,
        recon_code varchar(25) not null,
        delete_flag char not null,
		PRIMARY KEY (tran_gid),
		key idx_tran_date(tran_date),
		key idx_excp_value(excp_value)
	) Engine = MyISAM;
    
    drop temporary table if exists recon_tmp_ttranexp;
    create temporary table recon_tmp_ttranexp
	(
		tran_gid int(10) unsigned NOT NULL,
		tran_date date default null,
		tran_value double(15,2) not null default 0,
		excp_value double(15,2) not null default 0,
		roundoff_value double(15,2) not null default 0,
		tran_mult tinyint not null default 0,
        recon_code varchar(25) not null,
        delete_flag char not null,
		PRIMARY KEY (tran_gid),
		key idx_tran_date(tran_date),
		key idx_excp_value(excp_value)
	) Engine = MyISAM;
    
    
    insert into recon_tmp_ttran
  (
    tran_gid,tran_date,tran_mult,tran_value,excp_value,roundoff_value,recon_code,delete_flag
  )
  select a.* from (
  select t.tran_gid,t.tran_date,t.tran_mult,t.tran_value,t.excp_value,t.roundoff_value,r.recon_code,t.delete_flag from recon_tmp_trecon as r
  inner join recon_mst_trecon as c on r.recon_code = c.recon_code
    and c.recontype_code in ('W','B','I')
    and c.delete_flag = 'N'
  inner join recon_trn_ttran as t on r.recon_code = t.recon_code
	and t.delete_flag = 'N'
    and t.recon_code = in_recon_code
	LOCK IN SHARE MODE) as a;

  insert into recon_tmp_ttran
  (
    tran_gid,tran_date,tran_mult,tran_value,excp_value,roundoff_value,recon_code,delete_flag
  )
  select a.* from (
  select t.tran_gid,cast(s.insert_date as date),1 as tran_mult,1 as tran_value,1 as excp_value,t.roundoff_value,r.recon_code,t.delete_flag from recon_tmp_trecon as r
  inner join recon_mst_trecon as c on r.recon_code = c.recon_code
    and c.recontype_code in ('V','N')
    and c.delete_flag = 'N'
  inner join recon_trn_ttran as t on r.recon_code = t.recon_code
    and t.delete_flag = 'N'
  inner join recon_trn_tscheduler as s on t.scheduler_gid = s.scheduler_gid
    and t.recon_code = in_recon_code
    LOCK IN SHARE MODE) as a;
    
    select ifnull(count(*),0),ifnull(sum(excp_value) - sum(roundoff_value) * sum(tran_mult),0) into v_openingexcp_count,v_openingexcp_value 
    from recon_tmp_ttrangid
	where excp_value <> 0
	and (excp_value - roundoff_value * tran_mult) <> 0
	and tran_date < in_period_from;

	set v_openingexcp_value = ifnull(v_openingexcp_value,0);
    
    select ifnull(count(*),0),ifnull(sum(tran_value),0) as tranvalue  into v_tranbal_count,v_tranbal_value
    from recon_tmp_ttran 
    where recon_code = in_recon_code 
    and tran_date >= in_period_from
    and tran_date <= in_period_to
    and delete_flag='N';
    
    select ifnull(count(*),0),ifnull(sum(tran_value),0) as tranvalue  into v_tranbal_expcount,v_tranbal_expvalue
    from recon_tmp_ttran 
    where recon_code = in_recon_code 
    and delete_flag='N';
    
    select ifnull(count(*),0),ifnull(sum(tran_value),0) as tranvalue into v_ko_count,v_ko_value 
    from recon_trn_ttranko 
    where recon_code = in_recon_code 
	and tran_date >= in_period_from
    and tran_date <= in_period_to
    and delete_flag='N'
    LOCK IN SHARE MODE;
    
    select ifnull(count(*),0),ifnull(sum(tran_value),0) as tranvalue into v_ko_expcount,v_ko_expvalue 
    from recon_trn_ttranko 
    where recon_code = in_recon_code and delete_flag='N' LOCK IN SHARE MODE;
    
    select ifnull(count(*),0)
    ,ifnull(sum(ifnull(excp_value,0)),0) as excpovalue
    ,ifnull(sum(ifnull(tran_value,0) - ifnull(excp_value,0)),0)
    into v_partial_count,v_partialexp_value,v_partialmatch_value
	from recon_tmp_ttran 
    where recon_code = in_recon_code
    and tran_date >= in_period_from
    and tran_date <= in_period_to
	and tran_value != excp_value and excp_value != 0 and tran_value != 0
    and delete_flag='N';
    
    select ifnull(count(*),0)
	,ifnull(sum(ifnull(excp_value,0)),0) as excpovalue
    into v_partial_count,v_partialexp_value
	from recon_tmp_ttran 
    where recon_code = in_recon_code
    and tran_value != excp_value and excp_value != 0 and tran_value != 0
    and delete_flag='N';
    
    select ifnull(count(*),0),ifnull(sum(ifnull(excp_value,0)),0) as excpvalue 
    into v_exp_count,v_exp_value
    from recon_tmp_ttran 
    where recon_code = in_recon_code
    and tran_value = excp_value and excp_value != 0
    and delete_flag='N';
    
    set v_totaltran_count = ifnull(v_tranbal_count + v_ko_count,0);
    set v_totaltran_value = ifnull(v_tranbal_value + v_ko_value,0);
    set v_totaltran_expcount = ifnull(v_tranbal_expcount + v_ko_expcount,0);
    set v_totaltran_expvalue = ifnull(v_tranbal_expvalue + v_ko_expvalue,0);
    
     select recontype_code  into v_recontype_code from recon_mst_trecon where recon_code = in_recon_code and delete_flag='N';
    
    select	concat(v_totaltran_count,' - Lines') 		as tran_count,
			round(v_totaltran_value,3) 		as tran_value,
            v_ko_count 				as ko_count,
            v_ko_value 				as ko_value,
            v_partial_count 		as ko_partialexcp_count,
            v_exp_count 			as excp_count,
            v_exp_value 			as excp_value,
            v_partialmatch_value 	as partialmatch_value,
            v_partialexp_value		as partialexcp_value,
            (v_partialexp_value + v_partialmatch_value) as partialexcpmatch_value,
            v_openingexcp_value 	as openingexcp_value,
            v_openingexcp_count		as openingexcp_count,
            v_recontype_code 		as recontype_code,
            (select fn_get_currency_format(v_totaltran_value,'INR',in_conversion_type,'Dashboard')) 	as c_tran_value,
            (select fn_get_currency_format(v_ko_value+v_partialmatch_value,'INR',in_conversion_type,'Dashboard')) 			as c_ko_value,
            (select fn_get_currency_format(v_partialexp_value,'INR',in_conversion_type,'Dashboard'))	as c_partialexpense_value,
            (select fn_get_currency_format(v_partialmatch_value,'INR',in_conversion_type,'Dashboard'))	as c_partialmatch_value,
            (select fn_get_currency_format((v_partialexp_value),'INR',in_conversion_type,'Dashboard'))	as c_partialexcp_value,
            (select fn_get_currency_format(v_exp_value,'INR',in_conversion_type,'Dashboard')) 			as c_excp_value,
            (select fn_get_currency_format(v_openingexcp_value,'INR',in_conversion_type,'Dashboard')) 	as c_openingexcp_value,
            concat(' (',round(ifnull(ifnull(((v_ko_value+v_partialmatch_value)/(v_totaltran_value+v_openingexcp_value)),0)*100,0),2),' %)')	as p_ko_value,
            concat(' (',round(ifnull(ifnull((v_exp_value/(v_totaltran_value+v_openingexcp_value)),0)*100,0),2),' %)')	as p_exp_value,
            concat(' (',round(ifnull(ifnull((v_partialexp_value/v_totaltran_expvalue),0)*100,0),2),' %)')		as p_partialexpense_value,
            concat(' (',round(ifnull(ifnull((v_partialmatch_value/v_totaltran_value),0)*100,0),2),' %)')	as p_partialmatch_value,
            concat(' (',round(ifnull(ifnull(((v_partialexp_value)/(v_totaltran_value+v_openingexcp_value)),0)*100,0),2),' %)')	as p_partialexp_value,
            concat(' - Lines (',round(ifnull(ifnull((v_ko_count/v_totaltran_count),0)*100,0),2),' %)')	as p_ko_count,
            concat(' - Lines (',round(ifnull(ifnull((v_exp_count/v_totaltran_expcount),0)*100,0),2),' %)')	as p_excp_count,
            concat(' - Lines (',round(ifnull(ifnull(((v_partial_count)/v_totaltran_expcount),0)*100,0),2),' %)')	as p_partial_count,
            v_totaltran_value,v_totaltran_expvalue;
    
    drop temporary table if exists recon_tmp_ttran;

END