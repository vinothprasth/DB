CREATE DEFINER=`root`@`%` PROCEDURE `pr_run_reconrule`(
  in in_recon_code text,
  in in_period_from date,
  in in_period_to date,
  in in_automatch_flag char(1),
  in in_ip_addr varchar(255),
  in in_user_code varchar(32),
  out out_msg text,
  out out_result int
)
me:BEGIN
  /*
	Version - 001
	Created By - Muthu
    Created Date - 2025-02-19
	Updated Date - 2025-02-19
*/
  declare v_query text default "";
  declare err_msg text default '';
  declare err_flag varchar(10) default false;
  declare v_count integer default 0;
  declare v_opening_count integer default 0;
  declare v_opening_value decimal(18,2) default 0;
  declare v_current_count integer default 0;
  declare v_current_value decimal(18,2) default 0;
  declare v_while_count integer default 0;
  declare v_koqueue_gid integer default 0;
  
  DECLARE EXIT HANDLER FOR SQLEXCEPTION
  BEGIN
    GET DIAGNOSTICS CONDITION 1 @sqlstate = RETURNED_SQLSTATE,
    @errno = MYSQL_ERRNO, @text = MESSAGE_TEXT;

    SET @full_error = CONCAT("ERROR ", @errno, " (", @sqlstate, "): ", @text,' ',err_msg);

    ROLLBACK;

    -- call pr_upd_koqueue(v_job_gid,'F',@full_error,@msg,@result);

    set out_msg = @full_error;
    set out_result = 0;
	
    SIGNAL SQLSTATE '99999' SET
    MYSQL_ERRNO = @errno,
    MESSAGE_TEXT = @text;
  END;
  select count(*) into v_count from recon_trn_tkoqueue where koqueue_status in ('I','P') and recon_code = in_recon_code and delete_flag = 'N';
  if (v_count <= 2)then	 
		-- set v_query = Concat("call pr_run_reconruleredirect('",in_recon_code,"','",in_period_from,"','",in_period_to,"','",in_automatch_flag,"','",in_ip_addr,"','",in_user_code,"', @out_msg, @out_result);");
        set v_query = Concat("call pr_run_reconrulerecursion('",in_recon_code,"','",in_period_from,"','",in_period_to,"','",in_automatch_flag,"','",in_ip_addr,"','",in_user_code,"', @out_msg, @out_result);");
		insert into recon_trn_tkoqueue(recon_code,ko_query,koqueue_status,scheduled_date,scheduled_by,delete_flag)
		values (in_recon_code,v_query,'I',now(),in_user_code,'N');
	  
        
  end if;
  set out_result = 1;
  set out_msg = 'Success';
  COMMIT;
end