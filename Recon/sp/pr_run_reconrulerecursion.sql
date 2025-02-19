CREATE DEFINER=`root`@`%` PROCEDURE `pr_run_reconrulerecursion`(
  in in_recon_code text,
  in in_period_from date,
  in in_period_to date,
  in in_automatch_flag char(1),
  in in_ip_addr varchar(255),
  in in_user_code varchar(32),
  out out_msg text,
  out out_result int
)
BEGIN
	/*
	Version - 001
	Created By - Muthu
    Created Date - 2025-02-19
	Updated Date - 2025-02-19
*/
	declare v_opening_count integer default 0;
	declare v_opening_value decimal(18,2) default 0;
	declare v_current_count integer default 0;
	declare v_current_value decimal(18,2) default 0;
	declare v_while_count integer default 0;
	declare v_koqueue_gid integer default 0;
    declare v_out_msg text;
    declare v_out_result int;
    declare v_job_gid integer default 0;
    declare v_job_input_param text;
    declare v_txt text;
    declare v_value_diff decimal(18,2) default 0;
    declare v_count_diff decimal(18,2) default 0;
    declare v_kocount decimal(18,2) default 0;
    
	select count(tran_gid) as count,sum(excp_value) as value into v_opening_count,v_opening_value 
    from recon_trn_ttran where recon_code = in_recon_code and delete_flag = 'N';
    
    call pr_run_reconruleredirect(in_recon_code,in_period_from,in_period_to,in_automatch_flag,in_ip_addr,in_user_code,@out_job_gid,@out_msg, @out_result);
    select @out_job_gid,@out_msg,@out_result into v_job_gid,v_out_msg,v_out_result;
    
    if(v_out_result != 0)then
    begin
		select count(tran_gid) as count,sum(excp_value) as value into v_current_count,v_current_value from recon_trn_ttran where recon_code = in_recon_code and delete_flag = 'N';
		select MAX(koqueue_gid) into v_koqueue_gid from recon_trn_tkoqueue 
		where recon_code = in_recon_code
		and (koqueue_status = 'P' || koqueue_status = 'C') 
		and delete_flag = 'N';
		
		 if(v_opening_count!=v_current_count || v_opening_value!=v_current_value)then
						UPDATE recon_trn_tkoqueue 
						SET recon_trn_tkoqueue.koqueue_status = 'I',
						recon_trn_tkoqueue.koqueue_remark = concat('Recursive running'),
						recon_trn_tkoqueue.start_date = sysdate()
						WHERE recon_trn_tkoqueue.koqueue_gid = v_koqueue_gid
						and recon_trn_tkoqueue.delete_flag = 'N';
                        
                        set v_value_diff = (v_opening_value-v_current_value);
                        set v_count_diff = (v_opening_count-v_current_count);
                        
                        if( v_count_diff > 0 || v_value_diff > 0)then
							select count(*) into v_kocount from recon_trn_tko 
                            where job_gid = v_job_gid and recon_code = in_recon_code and delete_flag = 'N';
                            
							select job_input_param,concat(job_remark,CHAR(10),'Iteration Completed - ',v_kocount) 
                            into v_job_input_param,v_txt from recon_trn_tjob 
                            where job_gid = v_job_gid and delete_flag = 'N';
                            
							call pr_upd_jobwithparam(v_job_gid,v_job_input_param,'R',v_txt,@msg,@result);
                        end if;
		  else
				call pr_upd_koqueue(v_koqueue_gid,'C',"",@msg,@result);
		  end if;
    end;
    end if;
END