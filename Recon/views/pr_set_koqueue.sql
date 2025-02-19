CREATE DEFINER=`root`@`%` PROCEDURE `pr_set_koqueue`()
me:BEGIN
	/*
	Version - 001
	Created By - Muthu
    Created Date - 2025-02-19
	Updated Date - 2025-02-19
*/
    DECLARE v_query TEXT;
    DECLARE v_koqueue_gid INT;
    DECLARE err_msg text default '';
	DECLARE err_flag varchar(10) default false;
    
	DECLARE EXIT HANDLER FOR SQLEXCEPTION
	  BEGIN
		GET DIAGNOSTICS CONDITION 1 @sqlstate = RETURNED_SQLSTATE,
		@errno = MYSQL_ERRNO, @text = MESSAGE_TEXT;

		SET @full_error = CONCAT("ERROR ", @errno, " (", @sqlstate, "): ", @text,' ',err_msg);

		ROLLBACK;

		call pr_upd_koqueue(v_koqueue_gid,'F',@full_error,@msg,@result);

		set @out_msg = @full_error;
		set @out_result = 0;

		SIGNAL SQLSTATE '99999' SET
		MYSQL_ERRNO = @errno,
		MESSAGE_TEXT = @text;
	  END;
  
    
  IF NOT EXISTS (
        SELECT 1 
        FROM recon_trn_tkoqueue 
        WHERE koqueue_status = 'P' AND delete_flag = 'N'
    ) THEN
      
     
        
        SELECT kq.ko_query, kq.koqueue_gid  
        INTO @vquery, v_koqueue_gid 
        FROM recon_trn_tkoqueue as kq
        WHERE kq.delete_flag = 'N' 
        and kq.koqueue_status = 'I'
        ORDER BY kq.koqueue_gid 
        LIMIT 1;
        
        
        call pr_upd_koqueue(v_koqueue_gid,'P',"",@msg,@result);
        
        
        PREPARE stmt FROM @vquery;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
        
        
        -- call pr_upd_koqueue(v_koqueue_gid,'C',"",@msg,@result);
        
        COMMIT;
    END IF;
END