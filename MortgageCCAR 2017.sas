%macro m_append_dsn(dsnin=, dsnout=, delete_dsnin=0);
	proc  append base=&dsnout. data=&dsnin. ;
	run;
	%if %eval(&delete_dsnin=1) %then %do;
		proc delete data=&dsnin. ;
		run;
	%end;
%mend m_append_dsn;
;
/*-----------------------------------------------------------------------*/
 * Macro for creating Vintage and Appending forecast data;
/*-----------------------------------------------------------------------*/
%macro m_create_masterdata(datain=%str(), dataout=,is_mds_run=0,is_rfo_run=0,date_st=0);
       %if %eval (&is_rfo_run) %then %do;
            proc sql;
				create table table_main_temp as
					select distinct
                    t0.servicing_acct_number
                    ,datepart(t0.effective_date_pk) format=mmddyy10. as effective_date
                    ,datepart(t0.effective_date_pk) format=mmddyy10. as period_date
                    ,calculated period_date format=yyq6. as period
                    ,intck("qtr","01Jan1960"d, calculated period) as qtime
                    ,1 as existing_flag
                    ,. format=mmddyy10. as vintage
                    ,intck("months", datepart(t0.loan_orig_note_date), calculated period_date) as mob
                    ,t0.loan_fk_pk
                    ,t0.status_fk
                    ,t0.loan_curr_chg_off_amt
                    ,t0.loan_curr_int_rate
                    ,t0.loan_curr_exposure_amt
                    ,t0.loan_curr_upb_amt
                    ,t0.loan_institution_fk
    				,coalesce(t0.loan_int_only_ind,1) as loan_int_only_ind
                    ,t0.bkr_chapter_type_fk
                    ,t0.bkr_ind
                    ,t0.delq_curr
                    ,t0.delq_foreclosure_ind
                    ,t0.original_loan_fk
                    ,t0.loan_business_line_fk
                    ,t0.loan_portfolio_fk
                    ,t0.loan_lien_position
                    ,t0.loan_servicing_system_fk  
                    ,t0.loan_arm_ind
                    ,t0.loan_orig_exposure_amt
                    ,datepart(t0.loan_orig_note_date) format=mmddyy10. as loan_orig_note_date
                    ,t0.loan_orig_upb_amt
                    ,t0.credit_orig_cltv
                    ,t0.credit_score
                    ,t0.credit_rescore 
                    ,t0.loan_pk
                    ,t0.loan_revolver_draw_period 
                    ,datepart(t0.loan_booked_date) as loan_booked_date
                    ,t0.credit_first_rescore
                    ,t0.CREDIT_FIRST_RESCR_MODL_TYP_FK as credit_first_rescore_model_type_
                    ,t0.property_usage_type_fk
                    ,t0.collateral_ordinal
                    ,t0.app_cltv_ratio
                    ,datepart(t0.app_date_initialized) as app_date_initialized
                    ,t0.co
                    ,t0.gl
                    ,t0.collateral_state as ostate
                    ,t0.pd_group
					,t0.default_flag
                    ,case when  t0.pd_group="HELOC" 
                            and (t0.loan_revolver_draw_period=. or t0.loan_revolver_draw_period=999 or t0.loan_revolver_draw_period<0)
                    									and calculated loan_orig_note_date< &mvintage. then 180 
                    	when  t0.pd_group="HELOC" and (t0.loan_revolver_draw_period=. or t0.loan_revolver_draw_period=999 or t0.loan_revolver_draw_period<0)
                    									and calculated loan_orig_note_date>=&mvintage. then 120
                    	else t0.loan_revolver_draw_period end as 	revolver_imp 
                    ,case when t0.loan_institution_fk = 999 and   pd_group = "MORTGAGE" then "SERVICED" 
                     when t0.loan_institution_fk = 1 and   pd_group = "MORTGAGE" then "OWNED"  else "" end as product_type
                    ,datepart(t0.loan_revolver_draw_period) format=mmddyy10. as eod_date1
                    ,intnx('month', calculated loan_orig_note_date, calculated revolver_imp ) format=mmddyy10.  as eod_date2 
                    ,case when year(calculated eod_date1) in (.,1900) then calculated eod_date2 else calculated eod_date1 end format=mmddyy10. as eod_date

                    from &datain.(where=(default_flag="&default_ind_rfo.")) as t0
/*                    left join rdmlib.t_loan_history_monthly as t1*/
/*                        on t0.original_loan_fk = t1.original_loan_fk*/
					where upcase(pd_group) in (&product_list.)
					and t0.status_fk=1 
					and loan_servicing_system_fk < 9000
                    and calculated effective_date eq &date_st.
	       ;quit;
        %end;
        %else %do;
    		%if %eval(&is_mds_run) %then %do;
                proc sql;
    				create table table_main_temp as
    					select distinct
    						t0.servicing_acct_number
    						,datepart(t0.effective_date_pk) format=mmddyy10. as effective_date
    						,datepart(t0.effective_date_pk) format=mmddyy10. as period_date
    						,calculated period_date format=yyq6. as period
    						,intck("qtr","01Jan1960"d, calculated period) as qtime
    						,1 as existing_flag
    						,. format=mmddyy10. as vintage
    						,intck("months", datepart(t0.loan_orig_note_date), calculated period_date) as mob
    						,t0.loan_fk_pk
    						,t0.status_fk
    						,t0.loan_curr_chg_off_amt
    						,t0.loan_curr_int_rate
    						,t0.loan_curr_exposure_amt
    						,t0.loan_curr_upb_amt
    						,t0.loan_institution_fk
    						,coalesce(t0.loan_int_only_ind,1) as loan_int_only_ind
    						,t1.bkr_chapter_type_fk
    						,t1.bkr_ind
    						,t0.delq_curr
    						,t1.delq_foreclosure_ind
    						,t0.original_loan_fk
    						,t0.loan_business_line_fk
    						,t0.loan_portfolio_fk
    						,t0.loan_lien_position
    						,t2.loan_servicing_system_fk  
    						,t0.loan_arm_ind
    						,t0.loan_orig_exposure_amt
    						,datepart(t0.loan_orig_note_date) format=mmddyy10. as loan_orig_note_date
    						,t0.loan_orig_upb_amt
    						,t0.credit_orig_cltv
    						,t0.credit_score
    						,t1.credit_rescore 
    						,t0.loan_revolver_draw_period 
    						,datepart(t2.loan_booked_date) as loan_booked_date
    						,t0.credit_first_rescore
    						,t2.credit_first_rescore_model_type_
    						,t3.property_usage_type_fk
    						,t0.collateral_ordinal
    						,t4.app_cltv_ratio
    						,datepart(t4.app_date_initialized) as app_date_initialized
    						,t6.co
    						,t6.gl
    						,t3.collateral_state as ostate	
    						,t0.pd_group	
    						,case when  pd_group="HELOC" 
    									and (t0.loan_revolver_draw_period=. or t0.loan_revolver_draw_period=999 or t0.loan_revolver_draw_period<0)
    									and calculated loan_orig_note_date< &mvintage. then 180 
    							when  pd_group="HELOC" 
    									and (t0.loan_revolver_draw_period=. or t0.loan_revolver_draw_period=999 or t0.loan_revolver_draw_period<0)
    									and calculated loan_orig_note_date>=&mvintage. then 120
    							else t0.loan_revolver_draw_period end as 	revolver_imp 
                         	,case when t0.loan_institution_fk = 999 and   pd_group = "MORTGAGE" then "SERVICED" 
                                 when t0.loan_institution_fk = 1 and   pd_group = "MORTGAGE" then "OWNED" 
                                 else "" end as product_type
    						,datepart(t2.loan_revolver_draw_period_end_da) format=mmddyy10. as eod_date1
    						,intnx('month', calculated loan_orig_note_date, calculated revolver_imp ) format=mmddyy10.  as eod_date2 
    						,case when year(calculated eod_date1) in (.,1900) then calculated eod_date2 else calculated eod_date1 end format=mmddyy10. as eod_date
    					from &datain. as t0
    					left join rdmlib.t_loan_history_monthly as t1
    						on t0.original_loan_fk = t1.original_loan_fk
    					left join rdmlib.t_loan as t2
    						on t0.original_loan_fk = t2.original_loan_fk
    					left join rdmlib.t_loan_collateral as t3
    						on t0.loan_fk_pk = t3.loan_fk
    						and t0.collateral_ordinal = 1
    					left join rdmlib.t_app as t4
    						on t0.original_loan_fk = t4.loan_fk
    					left join rdmlib.t_mtg_co_data as t6
    						on t0.servicing_acct_number = t6.an
    					where upcase(pd_group) in (&product_list.)
    					and t0.status_fk=1 and loan_servicing_system_fk < 9000
    	       ;quit;	
    				
    		%end;
    		%else %do;
    			proc sql;
    				create table table_main_temp as
    					select distinct
    						t1.servicing_acct_number
    						,datepart(t1.effective_date_pk) format=mmddyy10. as effective_date
    						,datepart(t1.effective_date_pk) format=mmddyy10. as period_date
    						,calculated period_date format=yyq6. as period
    						,intck("qtr","01Jan1960"d, calculated period) as qtime
    						,1 as existing_flag
    						,. format=mmddyy10. as vintage
    						,intck("months", datepart(t2.loan_orig_note_date), calculated period_date) as mob
    						,t1.loan_fk_pk
    						,t1.status_fk
    						,t1.loan_curr_chg_off_amt
    						,t1.loan_curr_int_rate
    						,t1.loan_curr_upb_amt
    						,t1.loan_curr_exposure_amt
    						,t1.loan_institution_fk
    						,t1.bkr_chapter_type_fk
    						,t1.bkr_ind
    						,t1.delq_curr
    						,t1.delq_foreclosure_ind
    						,t2.original_loan_fk
    						,t2.loan_business_line_fk
    						,t1.loan_portfolio_fk
    						,t2.loan_lien_position
    						,t2.loan_servicing_system_fk  
    						,t2.loan_arm_ind
    						,t2.loan_orig_exposure_amt
    						,datepart(t2.loan_orig_note_date) format=mmddyy10. as loan_orig_note_date
    						,t2.loan_orig_upb_amt
    						,t2.credit_orig_cltv
    						,t2.credit_score
    						,t1.credit_rescore 
    						,t2.loan_pk
    						,t2.loan_revolver_draw_period 
    						,datepart(t2.loan_booked_date) as loan_booked_date
    						,t2.credit_first_rescore
    						,t2.credit_first_rescore_model_type_
    						,t3.property_usage_type_fk
    						,t3.collateral_ordinal
    						,t4.app_cltv_ratio
    						,datepart(t4.app_date_initialized) as app_date_initialized
    						,t6.co
    						,t6.gl
    						,t3.collateral_state as ostate
    						,case when t2.loan_institution_fk in (1,999)   and t2.loan_business_line_fk in (4 104 ) 
    						and t2.loan_servicing_system_fk < 9000  then "MORTGAGE"
    						when t2.loan_institution_fk = 1 and  t2.loan_business_line_fk in ( 1  3  101 ) 
    						and t2.loan_servicing_system_fk < 9000 and t1.loan_portfolio_fk = 102 then "HELOC"
    						 else ""  end as pd_group
    						,case when calculated pd_group="HELOC" 
    									and (t2.loan_revolver_draw_period=. or t2.loan_revolver_draw_period=999 or t2.loan_revolver_draw_period<0)
    									and calculated loan_orig_note_date< &mvintage. then 180 
    							when calculated pd_group="HELOC" 
    									and (t2.loan_revolver_draw_period=. or t2.loan_revolver_draw_period=999 or t2.loan_revolver_draw_period<0)
    									and calculated loan_orig_note_date>=&mvintage. then 120
    							else t2.loan_revolver_draw_period end  as 	revolver_imp 
     						,case when t2.loan_int_only_ind= . then 1 else t2.loan_int_only_ind end as loan_int_only_ind
                         	,case when t2.loan_institution_fk = 999 and  calculated pd_group = "MORTGAGE" then "SERVICED" 
                                 when t2.loan_institution_fk = 1 and  calculated pd_group = "MORTGAGE" then "OWNED" 
                                 else "" end as product_type
    						,datepart(t2.loan_revolver_draw_period_end_da) format=mmddyy10. as eod_date1
    						,intnx('month', calculated loan_orig_note_date, calculated revolver_imp ) format=mmddyy10. as eod_date2 
    						,case when year(calculated eod_date1) in (.,1900) then calculated eod_date2 else calculated eod_date1 end format=mmddyy10. as eod_date

    					from rdmlib.t_loan_history_monthly as t1
    					left join rdmlib.t_loan as t2
    						on t1.original_loan_fk = t2.original_loan_fk
    					left join rdmlib.t_loan_collateral as t3
    						on t1.loan_fk_pk = t3.loan_fk
    						and collateral_ordinal = 1
    					left join rdmlib.t_app as t4
    						on t1.original_loan_fk = t4.loan_fk
    					left join rdmlib.t_mtg_co_data as t6
    						on t1.servicing_acct_number = t6.an

    					where upcase(calculated pd_group) in (&product_list.)
    					and status_fk=1
    	       ;quit;	
    		%end;
        %end;

        data &dataout.;
            set table_main_temp;
        run;

        proc delete data=table_main_temp;
        run;
%mend m_create_masterdata;
;
%macro m_create_vintage_source_loop(yearin=,monthin=,no_of_quarter=,source_ind=, dsnout=);
/*possible value of source_ind is any one of the following: R - RDM, M - MDS, RFO - F*/
	%let date_end = %sysfunc(intnx(month,%sysfunc(inputn(&monthin.01&yearin.,mmddyy10.)),0,e));
	%let date_start = %sysfunc(intnx(qtr,%sysfunc(inputn(&monthin.01&yearin.,mmddyy10.)),%eval(-1*&no_of_quarter.),e));
	%let date_start1 = &date_start.;
    %let counter = 0;	
	%if %sysfunc(exist(&dsnout.)) %then %do;
		proc delete data=&dsnout.; run;
	%end;
	%if %sysfunc(exist(%str(retail_vintage_source))) %then %do;
		proc delete data=retail_vintage_source; run;
	%end;

    %if %eval(&source_ind. eq M) %then %do;
/*        &mds_ref_date. represents the earliest date for which MDS data available;*/
        %if %eval(date_start1 < "&mds_ref_date."d) %then %do;
            %let date_start1 =  %sysfunc(intnx(qtr,"&mds_ref_date."d,0,e);
        %end;
    %end;

    %put &date_start1. &date_end.;


	%do %until(%eval(&date_start1. gt &date_end.));
        %let counter = %eval(&counter. + 1);	
		%let mm = %sysfunc(month(&date_start1.));
		%let yyyy = %sysfunc(year(&date_start1.));
		%if %eval(&mm. lt 10) %then %let mm = 0&mm.;

		libname rdmlib "/sbg/warehouse/risk/clrghouse/dev/data01/Data_&yyyy._&mm./RDM" 	access=readonly;
		libname mds "/sbg/warehouse/risk/clrghouse/dev/data04/cart_ey/&yyyy._&mm./RDM/DELIVERY" access=readonly;

		%if (&source_ind. eq R) %then %do;
			%m_create_masterdata(dataout=work.retail_vintage_src,is_mds_run=0,is_rfo_run=0);
/*			%m_append_dsn(dsnin=work.retail_vintage_src,dsnout=&dsnout.,delete_dsnin=1);				*/
			%m_append_dsn(dsnin=work.retail_vintage_src,dsnout=work.retail_vintage_source,delete_dsnin=1);				
		%end;
		%else %if (&source_ind. eq M) %then %do;
/*                pulling out the MDS dataset name from the given list as assigned in config file*/
            %let mds_dsn = %scan(&mds_anchor_list.,&counter.,%str("|"));
			%m_create_masterdata(datain=mds.&mds_dsn.,dataout=work.retail_vintage_src,is_mds_run=1,is_rfo_run=0);
			%m_append_dsn(dsnin=work.retail_vintage_src,dsnout=work.retail_vintage_source,delete_dsnin=1);				
		%end;
        %else  %if %eval(&source_ind. eq F) %then %do;
            %let uname         = SASG_SA_USR ;              
            %let pwd           = '$Gh+YJbb7TN' ;            
            %let path          = MOODYSUAT.vsp.sas.com ;         
            libname rfo  oracle  user=&uname password=&pwd path=&path schema=MACCAR; 
        	%m_create_masterdata(datain=rfo.&rfo_dsn.,dataout=work.retail_vintage_src,is_mds_run=0,is_rfo_run=1, date_st = &date_start1.);
        	%m_append_dsn(dsnin=work.retail_vintage_src,dsnout=work.retail_vintage_source,delete_dsnin=1);				
        %end;

		%let date_start1 = %sysfunc(intnx( qtr,&date_start1., 1, e ));
		%put &yyyy. &mm.;
	%end;

	/*	dedup MORTGAGE loans original_loan_fk on the basis of pd_group and earliest loan_orig_note_date*/
		proc sort data=retail_vintage_source (where=(upcase(pd_group) eq "MORTGAGE" and product_type not in ("SERVICED"))) 
			dupout=&OutDataLib..table_main_mrtg_dup nodupkey out=table_main_mrtg_temp;
			by original_loan_fk  qtime pd_group loan_orig_note_date;
		run;

	/*	dedup HELOC loans original_loan_fk on the basis of pd_group and earliest loan_orig_note_date*/
		proc sort data=retail_vintage_source  (where=(upcase(pd_group) eq "HELOC"))
			out=table_main_heloc_temp_;
			by original_loan_fk qtime pd_group loan_orig_note_date loan_pk
        ;run;

		data table_main_heloc_temp table_main_heloc_dup;
			set table_main_heloc_temp_;
			by original_loan_fk qtime;
			if first.qtime then output table_main_heloc_temp;
			else output table_main_heloc_dup;
		run;

		%m_append_dsn(dsnin=table_main_heloc_temp, dsnout=&dsnout., delete_dsnin=0);
		%m_append_dsn(dsnin=table_main_mrtg_temp, dsnout=&dsnout., delete_dsnin=0);
		data &dsnout.;
			set &dsnout.;
				default_flag =coalesce(( delq_curr ge 180 or (bkr_ind =1 and bkr_chapter_type_fk =7) or delq_foreclosure_ind =1 
								or (loan_curr_chg_off_amt >0 and loan_curr_chg_off_amt NE .)),0);
/*				default_flag_rfo = coalesce(default_flag in ("&default_ind_rfo."),0)*/
				if default_flag in (&default_loan_ind.);

		run;

%MEND m_create_vintage_source_loop;
;
%macro m_create_originationdata(datain=, dataout=,no_of_iteration=1);
	data &dataout.;
		set &datain.;
		format period yyq6.;
		existing_flag=0;
/*			last date of the first month of the next quarter*/
		loan_orig_note_date = intnx("month",intnx("qtr",period_date,&no_of_iteration.,"b"),0,"e");
/*			last date of the last month of the next quarter*/
		period_date = intnx("month",intnx("qtr",period_date,&no_of_iteration.,"e"),0,"e");
		effective_date = period_date;
		vintage = period_date;
		period = period_date;
		qtime = intck("qtr","01Jan1960"d, period);
		mob=intck("months", loan_orig_note_date, period_date);
/*		mob=intck("month", datepart(loan_orig_note_date),(period_date));*/

	run;
%mend m_create_originationdata;
;


%macro m_create_forecasting_data(datain=,dataout=,no_of_qtr_to_forecast=,is_origination_data=0);

	%let datasetin = &datain.;

	%if %eval(&is_origination_data.) %then %do;
		%m_append_dsn(dsnin=&datasetin., dsnout=&dataout., delete_dsnin=0);
	%end;

	%do i=1 %to &no_of_qtr_to_forecast. %by 1;
		%if %eval(&i.gt 1) %then %do;
			%let j=%eval(&i.-1);
			%let datasetin = &datain.&j.;
		%end;
		data &datain.&i.;
			set &datasetin.;
/*			last date of the last month of the next quarter*/
			format period yyq6.;
			period_date = intnx("month",intnx("qtr",period_date,1,"e"),0,"e");
			effective_date = period_date;
			period = period_date;
			qtime = intck("qtr","01Jan1960"d, period);
			mob=intck("months", loan_orig_note_date, period_date);
/*			mob=intck("months", datepart(loan_orig_note_date),(period_date));*/
		run;
		%m_append_dsn(dsnin=&datain.&i., dsnout=&dataout., delete_dsnin=0);
	%end;

%mend m_create_forecasting_data;
;

%macro m_append_forecasting_data_loop(year_in=,month_number_in=,no_of_qtr_to_forecast=,data_in=,data_origination_in=,data_out=, source_ind=);
	%if (&source_ind. eq R) %then %do;
        %let mds_run_ind=0;
        %let rfo_run_ind = 0;
    %end;
	%else %if (&source_ind. eq M) %then %do; 
        %let mds_run_ind=1;
        %let rfo_run_ind = 0;
    %end;
    %else %if (&source_ind. eq F) %then %do; 
        %let mds_run_ind=0;
        %let rfo_run_ind = 1;
    %end;
	%let dt = %sysfunc(intnx(month,%sysfunc(inputn(&month_number_in.01&year_in.,mmddyy10.)),0,e));

	data work.temp_data_in;
		set &data_in.(where=(effective_date = &dt. and product_type not in ("SERVICED")));
	run;

	%if %sysfunc(exist(&data_out.)) %then %do;
		proc delete data=&data_out.;run;
	%end;

    %if %eval(&rfo_run_ind.) %then %do;
        proc sql;
        	create table &data_origination_in. as
    		select distinct
                t0.servicing_acct_number
                ,datepart(t0.effective_date_pk) format=mmddyy10. as effective_date
                ,datepart(t0.effective_date_pk) format=mmddyy10. as period_date
                ,calculated period_date format=yyq6. as period
                ,intck("qtr","01Jan1960"d, calculated period) as qtime
                ,1 as existing_flag
                ,. format=mmddyy10. as vintage
                ,intck("months", datepart(t0.loan_orig_note_date), calculated period_date) as mob
                ,t0.loan_fk_pk
                ,t0.status_fk
                ,t0.loan_curr_chg_off_amt
                ,t0.loan_curr_int_rate
                ,t0.loan_curr_exposure_amt
                ,t0.loan_curr_upb_amt
                ,t0.loan_institution_fk
                ,coalesce(case t0.loan_int_only_ind when "F" then 0 else 1 end,1) as loan_int_only_ind
                ,t0.bkr_chapter_type_fk
                ,t0.bkr_ind
                ,t0.delq_curr
                ,t0.delq_foreclosure_ind
                ,t0.original_loan_fk
                ,t0.loan_business_line_fk
                ,t0.loan_portfolio_fk
                ,t0.loan_lien_position
                ,t0.loan_servicing_system_fk  
                ,t0.loan_arm_ind
                ,t0.loan_orig_exposure_amt
                ,datepart(t0.loan_orig_note_date) format=mmddyy10. as loan_orig_note_date
                ,t0.loan_orig_upb_amt
                ,t0.credit_orig_cltv
                ,t0.credit_score
                ,t1.credit_rescore 
                ,t0.loan_pk
                ,t0.loan_revolver_draw_period 
                ,datepart(t0.loan_booked_date) as loan_booked_date
                ,t0.credit_first_rescore
                ,t0.CREDIT_FIRST_RESCR_MODL_TYP_FK as credit_first_rescore_model_type_
                ,t0.property_usage_type_fk
                ,t0.collateral_ordinal
                ,t0.app_cltv_ratio
                ,datepart(t0.app_date_initialized) as app_date_initialized
                ,t0.co
                ,t0.gl
                ,t0.collateral_state as ostate
                ,t0.pd_group
                ,case when  t0.pd_group="HELOC" 
                        and (t0.loan_revolver_draw_period=. or t0.loan_revolver_draw_period=999 or t0.loan_revolver_draw_period<0)
                    								and calculated loan_orig_note_date< &mvintage. then 180 
                    when  t0.pd_group="HELOC" and (t0.loan_revolver_draw_period=. or t0.loan_revolver_draw_period=999 or t0.loan_revolver_draw_period<0)
                    								and calculated loan_orig_note_date>=&mvintage. then 120
                    else t0.loan_revolver_draw_period end as 	revolver_imp 
                ,case when t0.loan_institution_fk = 999 and   pd_group = "MORTGAGE" then "SERVICED" 
                 when t0.loan_institution_fk = 1 and   pd_group = "MORTGAGE" then "OWNED"  else "" end as product_type
                ,datepart(t0.loan_revolver_draw_period) format=mmddyy10. as eod_date1
                ,intnx('month', calculated loan_orig_note_date, calculated revolver_imp ) format=mmddyy10.  as eod_date2 
                ,case when year(calculated eod_date1) in (.,1900) then calculated eod_date2 else calculated eod_date1 end format=mmddyy10. as eod_date

                from rfo.&data_origination_in. as t0
                left join rdmlib.t_loan_history_monthly as t1
                    on t0.original_loan_fk = t1.original_loan_fk
				where upcase(pd_group) in (&product_list.)
				and t0.status_fk=1 
				and loan_servicing_system_fk < 9000
/*                and calculated effective_date eq &date_start1.*/
        ;quit;
        %let data_orig_input =  &data_origination_in.;
    %end;
    %else %do;
    	%if %eval(&mds_run_ind.) %then %do;
            proc sql;
        		create table &data_origination_in. as
    					select distinct
    						t0.servicing_acct_number
    						,t0.PD_GROUP
    						,datepart(t0.effective_date_pk) format=mmddyy10. as effective_date
    						,datepart(t0.effective_date_pk) format=mmddyy10. as period_date
    						,calculated period_date format=yyq6. as period
    						,intck("qtr","01Jan1960"d, calculated period) as qtime
    						,1 as existing_flag
    						,. format=mmddyy10. as vintage
    						,intck("months", datepart(t0.loan_orig_note_date), calculated period_date) as mob
    						,t0.loan_fk_pk
    						,t0.loan_pk
    						,t0.status_fk
    						,t0.loan_curr_chg_off_amt
    						,t0.loan_curr_int_rate
    						,t0.loan_curr_exposure_amt
    						,t0.loan_curr_upb_amt
    						,t0.loan_institution_fk
    						,coalesce(t0.loan_int_only_ind,1) as loan_int_only_ind
    						,t1.bkr_chapter_type_fk
    						,t1.bkr_ind
    						,t0.delq_curr
    						,t1.delq_foreclosure_ind
    						,t0.original_loan_fk
    						,t0.loan_business_line_fk
    						,t0.loan_portfolio_fk
    						,t0.loan_lien_position
    						,t2.loan_servicing_system_fk  
    						,t0.loan_arm_ind
    						,t0.loan_orig_exposure_amt
    						,datepart(t0.loan_orig_note_date) format=mmddyy10. as loan_orig_note_date
    						,t0.loan_orig_upb_amt
    						,t0.credit_orig_cltv
    						,t0.credit_score
    						,t1.credit_rescore 
    						,t0.loan_revolver_draw_period 
    						,datepart(t2.loan_booked_date) as loan_booked_date
    						,t0.credit_first_rescore
    						,t2.credit_first_rescore_model_type_
    						,t3.property_usage_type_fk
    						,t0.collateral_ordinal
    						,t4.app_cltv_ratio
    						,datepart(t4.app_date_initialized) as app_date_initialized
    /*						,t5.seg_break*/
    /*						,datepart(t5.min_eff_date) as min_eff_date*/
    						,t6.co
    						,t6.gl
    						,t3.collateral_state as ostate
    						
    						,case when  pd_group="HELOC" 
    									and (t0.loan_revolver_draw_period=. or t0.loan_revolver_draw_period=999 or t0.loan_revolver_draw_period<0)
    									and calculated loan_orig_note_date< &mvintage. then 180 
    							when  pd_group="HELOC" 
    									and (t0.loan_revolver_draw_period=. or t0.loan_revolver_draw_period=999 or t0.loan_revolver_draw_period<0)
    									and calculated loan_orig_note_date>=&mvintage. then 120
    							else t0.loan_revolver_draw_period end as 	revolver_imp 
                         	,case when t0.loan_institution_fk = 999 and   pd_group = "MORTGAGE" then "SERVICED" 
                                 when t0.loan_institution_fk = 1 and   pd_group = "MORTGAGE" then "OWNED" 
                                 else "" end as product_type
    						,datepart(t2.loan_revolver_draw_period_end_da) format=mmddyy10. as eod_date1
    						,intnx('month', calculated loan_orig_note_date, calculated revolver_imp ) format=mmddyy10.  as eod_date2 
    						,case when year(calculated eod_date1) in (.,1900) then calculated eod_date2 else calculated eod_date1 end format=mmddyy10. as eod_date

    					from mds.&data_origination_in. as t0
    					left join rdmlib.t_loan_history_monthly as t1
    						on t0.original_loan_fk = t1.original_loan_fk
    					left join rdmlib.t_loan as t2
    						on t0.original_loan_fk = t2.original_loan_fk
    					left join rdmlib.t_loan_collateral as t3
    						on t0.loan_fk_pk = t3.loan_fk
    						and t0.collateral_ordinal = 1
    					left join rdmlib.t_app as t4
    						on t0.loan_fk_pk = t4.loan_fk
    /*					left join ref.alt_a_jumbo as t5*/
    /*						on t0.original_loan_fk = t5.original_loan_fk*/
    					left join rdmlib.t_mtg_co_data as t6
    						on t0.servicing_acct_number = t6.an


    					where upcase(pd_group) in (&product_list.)
    					and t0.status_fk=1 and loan_servicing_system_fk < 9000
    	       ;quit;
    	 	%let data_orig_input =  &data_origination_in.;
    	%end;
    	%else %do;
    		data work.temp_data_orig_in;
    			set work.temp_data_in(where=(intnx("month", period_date,-12,"b") 
    															le loan_orig_note_date 
    																le intnx("month",period_date,0,"e")));
    		run;
    		%let data_orig_input = work.temp_data_orig_in;
    	%end;
    %end;
	%m_create_forecasting_data(datain=work.temp_data_in
								,dataout=&data_out.
								,no_of_qtr_to_forecast=&no_of_qtr_to_forecast.
								,is_origination_data=0
								);
	%do k=1 %to &no_of_qtr_to_forecast. %by 1;
		%m_create_originationdata(datain=&data_orig_input.
							,dataout=work.test_origination_&k.							
							,no_of_iteration=&k.);
		%m_create_forecasting_data(datain=work.test_origination_&k.
									,dataout=&data_out.
									,no_of_qtr_to_forecast=%eval(&no_of_qtr_to_forecast.-&k.)
									,is_origination_data=1
								);
	%end;

/*	stacking vintage and forecast dataset into one where product type is not "SERVICED" */
	data &data_out.;
		set &data_in. (where=(product_type not in ("SERVICED")))
		&data_out.;
	run; 
/*creating a surrogate key in the dataset*/
	data &data_out.;
		retain surrogate_pk;
		set &data_out.;
		surrogate_pk+1;
	run;
%mend m_append_forecasting_data_loop;
;
/*-----------------------------------------------------------------------*/
;
;
/*-----------------------------------------------------------------------*/
 * Macros for Adjusting Utilization Rate for HELOC;
/*-----------------------------------------------------------------------*/
;
/*** Macro procedure: Adjust utilization rate ********/
%macro m_adjust_util(data_in=,data_in_util=, data_out=, scen= , scen2= , qstartactual =);

	data util_1;
		set &data_in. ;
			where qtime <= &qstartactual. and pd_group ="HELOC";
			keep original_loan_fk loan_curr_upb_amt mtime qtime  effective_date_pk loan_curr_exposure_amt pd_group existing_flag;
	run;

	proc sql;
		create table utilization_0 as 
			select a.*, b.loan_orig_exposure_amt
				from util_1 as a
					left join out.RETAIL_VINTG_FRCAST_DATA(where=(qtime=&qstartactual. and pd_group = "HELOC")) as b
						on a.original_loan_fk = b.original_loan_fk
		;
	quit;


	/******* Calculate utilization rate of past quarters as per &number_of_qtr_for_vintage. for each loan ***/
    data utilization_1;
        set utilization_0 ;
           if PD_GROUP = "HELOC" and existing_flag = 1 then do;
	           if (&qstartactual.- &number_of_qtr_for_vintage.) le qtime le &qstartactual.;
	           if  loan_curr_upb_amt ne . and coalesce(loan_orig_exposure_amt,0) ne 0 then
	              utilization_rate = loan_curr_upb_amt / loan_orig_exposure_amt;

	           if utilization_rate = . and coalesce(loan_curr_exposure_amt,0) ne 0 then
	              utilization_rate = loan_curr_upb_amt / loan_curr_exposure_amt;

            end;
    run;

     proc sql;
           create table utilization_2 as 
           select original_loan_fk
                , avg(utilization_rate) as avg_last_utilization_rate
           from utilization_1
           group by original_loan_fk 
     ;quit;

     /******** Merge avg_last_utilization_rate to stacked dataset(historical + forecasting) ****/
     proc sql;
           create table loan_level_all_1_&scen. as
                select a.*, b.avg_last_utilization_rate as utilization_rate 
                    from &data_in.  a
                           left join utilization_2 b
                                on a.original_loan_fk = b.original_loan_fk
           ;
     quit;

     data loan_level_all_1_&scen.;
          set loan_level_all_1_&scen.;
               if utilization_rate = . or utilization_rate > 1 then
               utilization_rate =1;
     run;

     /********** Calculate growth rate for each quarter *******************/
     /*calculating the current rate using the PPNR forecast for given number of scenarios*/
     data curr_target_2_&scen.;
            set &data_in_util.; 
                Growth_rate = &scen2. / lag(&scen2.);
     run;

     /***********Calculate portfolio level utilization rate  ***************************/

     proc sql;
           select sum(utilization_rate*loan_orig_exposure_amt)/sum(loan_curr_exposure_amt) into: util_overall_actual  
                from  loan_level_all_1_&scen.                                     
                     where  qtime=&qstartactual.+1 and PD_GROUP in ("HELOC") and existing_flag = 1 
           ;
     quit;

     /***********Get ppnr utilization rate  ***************************************/
     proc sql;
           select  &scen2   into: util_target 
                from curr_target_2_&scen.
/*                     where qtime=&qstartactual.;*/
                     where qtime=&qstartactual.+1;
     quit;


     /**** Adjust utilization rate to match the PPNR number ***/
     data loan_level_all_2_&scen.;
           set loan_level_all_1_&scen.;
           utilization_rate_bf_change = utilization_rate;

           /******** Calculate adjust factor *************/
           adjust_factor = &util_target. / &util_overall_actual.;

           /******** Apply adjust factor to loan level dataset ***********/
           if qtime>=&qstartactual.+1 and existing_flag = 1 then
                utilization_rate = adjust_factor * utilization_rate_bf_change;
     run;

     /**** Merge growth rate to loan level dataset ****************/
     proc sql;
        create table loan_level_all_3_&scen. as 
           select a.*
                ,"&scen." as scenario                
                ,b.Growth_rate
           from loan_level_all_2_&scen.  a
                left join curr_target_2_&scen. b 
                on a.qtime=b.qtime
                order by original_loan_fk, qtime;
           ;
     quit;


     /**** Adjust utilization rate to match the PPNR number (2) ***/
     data &data_out.;
           set loan_level_all_3_&scen. ;
     if PD_GROUP = "HELOC" and existing_flag = 1 then
           do;
               retain lag_util;
               by original_loan_fk; 
                   
               if first.original_loan_fk then lag_util=utilization_rate;
               if (qtime gt &qstartactual.+1) then utilization_rate = lag_util*Growth_rate;
               lag_util = utilization_rate; 

			   balance = utilization_rate*loan_orig_exposure_amt ;
           end; 
     run;
/*   Checking the utilization rate after adjustment*/
     proc sql;
           create table check_&scen. as 
           select qtime
                , sum(loan_curr_upb_amt) as sum_balance
                , sum(loan_curr_exposure_amt) as sum_loan_curr_exposure_amt 
                , sum(utilization_rate*loan_orig_exposure_amt)/sum(loan_curr_exposure_amt) as util_af_adj
           from &data_out.
           where qtime >= &qstartactual.+1 and PD_GROUP in ("HELOC") and  existing_flag = 1
           group by qtime
     ;quit;

%mend m_adjust_util;
;
%macro m_process_util_src(dsn_util_in=, dsn_util_out=);
	proc sql; 
		create table &dsn_util_out. as 
		select (intck("qtr","01Jan1960"d,date)) as qtime
		,avg(base) as base 
		,avg(adverse) as adverse 
		,avg(SA) as SA 
		from &dsn_util_in. 
		group by qtime 
	;quit;
%mend m_process_util_src;
;

%macro m_adjust_util_loop(dsnin=,dsnin_util=,dsnout=);
     %if %sysfunc(exist(&dsnout.)) %then %do; 
         proc delete data=&dsnout.;run; 
     %end;
	 %m_process_util_src(dsn_util_in=&dsnin_util., dsn_util_out=work.utilization_rate_target);
     *&num_scen. is defined in 00_config and represents number of scenarios;
     %do cntr = 1 %to &num_scen.;
           %let scenario = &&scn&cntr.;
           %let scenario_desc = %scan(&&scno&cntr.,2,"_");
           %let qtime_start = %sysfunc(intck(qtr,"01Jan1960"d,%sysfunc(inputn(&month.01&year.,mmddyy10.))));   
           %m_adjust_util(data_in=&dsnin.
                           ,data_in_util=work.utilization_rate_target
                           ,data_out=&dsnout._&scenario.
                           ,scen= &scenario.
                           ,scen2= &scenario_desc.
                           ,qstartactual=&qtime_start.
                           );
           %m_append_dsn(dsnin=&dsnout._&scenario., dsnout=&dsnout., delete_dsnin=1);

     %end;

%mend m_adjust_util_loop;
;
%macro m_appnd_util_rate_to_retl_frcast(dsnin1=,dsnin2=,dsnout=);
     proc sql;
           create table &dsnout. as
           select b.scenario
               , a.*
                ,b.utilization_rate 
                ,b.utilization_rate_bf_change 
                ,b.adjust_factor 
                ,b.Growth_rate
                ,b.lag_util
      from &dsnin1.(where = (qtime gt %sysfunc(intck(qtr,"01Jan1960"d,%sysfunc(inputn(&month.01&year.,mmddyy10.)))))) as a
          left join &dsnin2. as b
                on a.surrogate_pk = b.surrogate_pk
                order by a.surrogate_pk
     ;quit;
%mend m_appnd_util_rate_to_retl_frcast;
;
/*-----------------------------------------------------------------------*/
 * Macro for creating Panel data;
/*-----------------------------------------------------------------------*/
;

%macro m_create_panel(Datain= ,DataOut=);

     PROC SQL;
           create table &dataout. AS 
				select distinct surrogate_pk, servicing_acct_number, effective_date, period_date, period, 
				qtime, existing_flag, vintage, mob, loan_fk_pk, status_fk,loan_curr_chg_off_amt, loan_curr_int_rate, 
				loan_curr_upb_amt, loan_curr_exposure_amt,loan_institution_fk, loan_int_only_ind, bkr_chapter_type_fk, bkr_ind, delq_curr, 
				delq_foreclosure_ind, original_loan_fk, loan_business_line_fk, loan_portfolio_fk, loan_lien_position, 
				loan_servicing_system_fk, loan_arm_ind, loan_orig_exposure_amt, loan_orig_note_date, loan_orig_upb_amt, 
				credit_orig_cltv, credit_score as credit_score_without_transform, credit_rescore, loan_revolver_draw_period, loan_booked_date, 
				credit_first_rescore, credit_first_rescore_model_type_, property_usage_type_fk, collateral_ordinal, 
				app_date_initialized, CO, GL, ostate,
				pd_group, revolver_imp, product_type, eod_date1, eod_date2, eod_date, 
				utilization_rate, utilization_rate_bf_change, adjust_factor, scenario, Growth_rate, 
				lag_util, balance
	            ,CASE WHEN app_cltv_ratio = 9.9999 THEN . ELSE app_cltv_ratio END AS app_cltv_ratio
	            ,CASE WHEN MAX( calculated app_cltv_ratio, credit_orig_cltv)>1 THEN 1 ELSE MAX( calculated app_cltv_ratio, credit_orig_cltv) END AS ocltv

	             ,CASE WHEN loan_booked_date < "01APR2013"d AND loan_booked_date NE . THEN 1
	             WHEN app_date_initialized < "01APR2013"d AND app_date_initialized NE . THEN 1
	             ELSE 0
	             END AS trans_ind

                 /**********************************************************************************************************/     
                 /*Need to review this logic again*/  
                 ,CASE WHEN  PD_GROUP = "MORTGAGE" THEN 
                                 CASE WHEN (credit_score >=9000 OR credit_score = . OR credit_score <300 ) THEN 
                                        CASE WHEN 300<=credit_first_rescore <=850 THEN 
                                                 credit_first_rescore
                                            ELSE 
                                                 credit_score 
                                        END
                                       WHEN credit_score > 850 THEN 
                                                 850 
                                       ELSE 
                                            credit_score 
                                 END
                        ELSE 
                                  CASE WHEN (credit_score >=9000 OR credit_score = . ) THEN 
                                            CASE WHEN . < credit_first_rescore < 9000 AND credit_first_rescore_model_type_ = 22 THEN
                                                            CASE WHEN round((credit_first_rescore + 151.125 ) / 1.235 ) > 850 THEN 850 
                                                            ELSE  round((credit_first_rescore + 151.125 ) / 1.235 )
                                                            END
                                                  ELSE
                                                            CASE WHEN credit_first_rescore>=9000 THEN .
                                                                 ELSE credit_first_rescore
                                                                 END
                                                 END
                                        ELSE
                                            CASE WHEN CALCULATED trans_ind = 1 THEN 
                                                       CASE WHEN round((credit_score + 151.125 ) / 1.235 ) >850 THEN 850 
                                                            ELSE  round((credit_score + 151.125 ) / 1.235 ) 
                                                            END 
                                            ELSE 
											    CASE WHEN credit_score > 850 THEN 850 ELSE credit_score END
                                            END
                                 END
                  END AS credit_score
                 /**********************************************************************************************************/      

				/*PANEL CREATION*/
                ,PUT(ostate,$prop_location_panel.) AS PROPERTY_LOCATION

				/*Mortgage PANEL Creation Data*/
		        ,CASE WHEN loan_arm_ind = 1 THEN "ARM" ELSE "FIX" END AS Fix_Arm 
				,PUT( calculated ocltv,CLTV_mrtg.) AS CLTV_MRTG
				,PUT(calculated credit_score,FICO_mrtg.) AS FICO_mrtg
				,PUT(MOB,LOAN_AGE_MRTG.) AS LOAN_AGE_MRTG

				/*HELOC PANEL Creation Data*/
                ,CASE WHEN loan_lien_position = 1 THEN "FIRST" ELSE "SECOND" END AS Loan_LIEN
				,PUT( calculated ocltv,CLTV_Heloc.) AS CLTV_Heloc
				,PUT(calculated credit_score,FICO_Heloc.) AS FICO_Heloc
				,PUT(MOB,Loan_age_Heloc.) AS Loan_age_Heloc

                ,CASE WHEN UPCASE( PD_GROUP) = "MORTGAGE" THEN catx(" | ",CALCULATED FIX_ARM, CALCULATED CLTV_mrtg, CALCULATED FICO_mrtg, CALCULATED LOAN_AGE_MRTG, CALCULATED PROPERTY_LOCATION) 
                      ELSE catx(" | " ,CALCULATED LOAN_LIEN, CALCULATED CLTV_HELOC, CALCULATED FICO_HELOC, CALCULATED LOAN_AGE_HELOC, CALCULATED PROPERTY_LOCATION) 
                 END AS PANEL

                 /*gl*/
                ,CASE WHEN ((substr(GL,5,1)) = "5") THEN "Recoveries" ELSE "Gross CO" END AS GLtype

/*				,case when credit_score >=9000 then . else credit_score end as credit_score*/
/*				,case when calculated trans_ind = 1 then */
/*				case when round((credit_score + 151.125 ) / 1.235 ) >850 then 850 else  round((credit_score + 151.125 ) / 1.235 ) end */
/*				else credit_score end as credit_score_unified		*/

				,case when credit_first_rescore > 9000 then . else credit_first_rescore end as credit_first_rescore2
				/*JUMBO, AFFORDABLE and ALT-A Flag*/
/*				,case when upcase (seg_break) in ("JUMBO") then 1 else 0 end as JUMBO*/
/*				,case when upcase (seg_break) in ("AFFORDABLE") then 1 else 0 end as Affordable_perf*/
/*				,case when upcase (seg_break) in ("ALT-A") then 1  else 0 end as ALT_A_perf*/

            FROM &datain.
	;quit;

%mend m_create_panel;
;
%macro m_create_jumbo(datain=,refdatain=,dataout=);
	data alta_jumbo(keep=original_loan_fk Jumbo_ever);
		set &refdatain.;
			min_eff_date=datepart(min_eff_date);
			Jumbo_ever=1;
			if upcase(seg_break) in ("JUMBO") and min_eff_date<&min_eff_date. then output;
	 run;

   proc sort data = alta_jumbo;
           by original_loan_fk;
     run;

     proc sort data = &datain.;
           by original_loan_fk;
     run;

	 data &dataout.;
		merge &datain.(in=yes) alta_jumbo;
			by original_loan_fk;
			if yes;
			if jumbo_ever=. then jumbo_ever=0;
	 run;
	
	 data &dataout.;
		 set &dataout.;
			 where upcase(pd_group) = "MORTGAGE" or (upcase(pd_group) = "HELOC" and existing_flag=1);
	 run;

%mend m_create_jumbo;
;
/*-----------------------------------------------------------------------*/
 * Macros for Preprocessing source and creating macroeconomic data;
/*-----------------------------------------------------------------------*/
;
%macro m_preprocess_macroecon_src_loop(dsnin=_null_,dsnout=,is_macroeco_src_directory=,is_macroeco_rfo_directory=);
	%if %sysfunc(exist(&dsnout.)) %then %do; 
	    proc delete data=&dsnout.;run; 
	%end;

	%if %eval(&is_macroeco_src_directory.) %then %do;
		*&num_scen. is defined in 00_config and represents number of scenarios;
		%do cntr = 1 %to &num_scen.;
			%let scenario = &&scn&cntr.;
			%let lib_st = mac&scenario._st;
			%let lib_us = mac&scenario._us;
	

			proc transpose data=&lib_st..&macroecon_data_prod.;
				out=&macroecon_data_prod._&scenario._st(drop=_label_ rename=(col1=val)) name=var_state; 
				by period notsorted;
			run;

			proc transpose data=&lib_us..&macroecon_data_prod.
				out=&macroecon_data_prod._&scenario._us(drop=_label_ rename=(col1=val)) name=var_state; 
				by period notsorted;
			run;

			data temp_macroecon_&scenario.;
				retain period_dt macro_name scenario state val;
                length var_state $40.;                
				set &macroecon_data_prod._&scenario._st
					&macroecon_data_prod._&scenario._us;
                format period_dt yyq6.;
                period_dt = input(period,yyq6.);
                drop period; rename period_dt = period;

				macro_name = left(trim(scan(var_state,1,"_")));
                *Removing trailing Q for few macro variables;
                macro_name_reverse = left(trim(reverse(macro_name)));
                if substr(macro_name_reverse,1,1)="Q" then macro_name = left(trim(reverse(substr(macro_name_reverse,2, length(macro_name_reverse)))));
				state = left(trim(scan(var_state,2,"_")));
				scenario = "&scenario.";
				drop var_state;
			    if macro_name in (&macro_econ_var.) and state in (&state_in.%str(,)&lgd_judicial_states.%str(,)&lgd_states_with_macro. );
			run;

			%m_append_dsn(dsnin=temp_macroecon_&scenario., dsnout=&dsnout., delete_dsnin=1); 

		 
		%end;
	%end;
	%else %do;

		%if %eval (&is_macroeco_rfo_directory.) %then %do;
			%do cntr = 1 %to &num_scen.;
				%let scenario = &&scn&cntr.;
	            %let uname         = SASG_SA_USR ;              
	            %let pwd           = '$Gh+YJbb7TN' ;            
	            %let path          = MOODYSUAT.vsp.sas.com ;         
	            libname rfomacro  oracle  user=&uname password=&pwd path=&path schema=MACCAR; 

				data rfo_macro_st;
					set rfomacro.&macroecon_data_rfo_st. ;
					where upcase(SCENARIO) = upcase("&sceno_lst.") and upcase(FREQUENCY) = upcase("&macro_frequency.");
				run;

				proc transpose data=rfo_macro_st
					out=&macroecon_data_rfo_st._&scenario._st(drop=_label_ rename=(col1=val)) name=var_state; 
					by period notsorted;
				run;


				data rfo_macro_us;
					set rfomacro.&macroecon_data_rfo_us. ;
					where upcase(SCENARIO) = upcase("&sceno_lst.") and upcase(FREQUENCY) = upcase("&macro_frequency.");
				run;

				proc transpose data=rfo_macro_us
					out=&macroecon_data_rfo_us._&scenario._us(drop=_label_ rename=(col1=val)) name=var_state; 
					by period notsorted;
				run;

				data temp_macroecon_&scenario.;
					retain period_dt macro_name scenario  state val;
	                length var_state $40.;                
					set &macroecon_data_rfo_st._&scenario._st
						&macroecon_data_rfo_us._&scenario._us;
	                format period_dt yyq6.;
	                period_dt = input(period,yyq6.);
	                drop period; rename period_dt = period;

					macro_name = left(trim(scan(var_state,1,"_")));
	                *Removing trailing Q for few macro variables;
	                macro_name_reverse = left(trim(reverse(macro_name)));
	                if substr(macro_name_reverse,1,1)="Q" then macro_name = left(trim(reverse(substr(macro_name_reverse,2, length(macro_name_reverse)))));
					state = left(trim(scan(var_state,2,"_")));
					scenario = "&scenario.";
					drop var_state macro_name_reverse ;
				    if macro_name in (&macro_econ_var.) and state in (&state_in.%str(,)&lgd_judicial_states.%str(,)&lgd_states_with_macro. );
				run;

				%m_append_dsn(dsnin=temp_macroecon_&scenario., dsnout=&dsnout., delete_dsnin=1); 		
			%end;
		%end;
		%else %do;
		     proc sort data=&dsnin.;
		           by period;
		     run;

		     proc transpose data=&dsnin. out=macro_dev_quarterly_tr1(drop=_label_);
		           by period;
		     run;

		     proc sql;
		           create table &dsnout. as 
		                select period
		                     ,scan(_name_,1,"_") as macro_name
		                     ,case substr(scan(_name_,2,"_"),1) when "FEDBMC" then "BL"
								when "FEDMMC" then "AD"
								when "FEDSMC" then "SA"
								else "ST" end as scenario
		                     ,
		                case 
		                     when length(trim(left(scan(_name_,3,"_"))))>2 then substr(trim(left(scan(_name_,3,"_"))),2,2) 
		                     else  trim(left(scan(_name_,3,"_"))) end length=2 as state
		                ,col1 as val
		           from work.macro_dev_quarterly_tr1
		                where calculated macro_name in (&macro_econ_var.)
		                     order by period, state, scenario,macro_name, val
		           ;
		     quit;
		%end;
	%end;
*Rounding off input Macro Economic Variables upto 2 decimal digit;
	    data &dsnout.; 
	        set &dsnout.; 
	        val = round(val,0.01); 
	    run;

%mend m_preprocess_macroecon_src_loop;
;
%macro m_preprocess_macroecon_source(using_macroecon_rfo_directory=,using_macroecon_src_directory=,dsnout=);
	%if %eval(&using_macroecon_src_directory.) %then %do;
		%m_preprocess_macroecon_src_loop(dsnout=&dsnout.
										,is_macroeco_src_directory=&using_macroecon_src_directory.
										,is_macroeco_rfo_directory=&using_macroecon_rfo_directory.);
	%end;
	%else %if %eval(&using_macroecon_rfo_directory.) %then %do;
		%m_preprocess_macroecon_src_loop(dsnout=&dsnout.
										,is_macroeco_src_directory=&using_macroecon_src_directory.
										,is_macroeco_rfo_directory=&using_macroecon_rfo_directory.);
	%end;
	%else %do;	
		%m_preprocess_macroecon_src_loop(dsnin=&reflib..&macroecon_data_dev.
										,dsnout=&dsnout.
										,is_macroeco_src_directory=&using_macroecon_src_directory.
										,is_macroeco_rfo_directory=&using_macroecon_rfo_directory.);
	%end;
%mend m_preprocess_macroecon_source;
;


%macro loop(values=,cnt=);
     %unquote(TLOG&vvar. = log(&vvar.);)
     %if %eval(&cnt. gt 4) %then %do;
           %unquote(TYY&vvar. = dif4(&vvar.) / lag4(&vvar.) * 100;)
     %end;
     %if %eval(&cnt. gt 2) %then %do;
           %unquote(T2Q&vvar. = dif2(&vvar.) / lag2(&vvar.) * 100;)
     %end;

     %if %eval(&cnt. gt 1) %then %do;
                %unquote(TQQ&vvar. = dif(&vvar.)/lag(&vvar.) * 100;)
				
     %end;

           %let count=%sysfunc(countw(&values));

     %do i = 1 %to &count;
        %let value=%qscan(%bquote(&values),&i,%str(,));

           %if %eval(&cnt. gt 1) %then %do;
            %unquote(T&vvar.&value.=lag&value.(&vvar.);)
           %unquote(TQQ&vvar.&value. = dif(T&vvar.&value.)  /lag(T&vvar.&value.) * 100;)

           %end;
           %if %eval(&cnt. gt 4) %then %do;
           %unquote(TYY&vvar.&value. = dif4(T&vvar.&value.) / lag4(T&vvar.&value.) * 100;)
           %end;
           %if %eval(&cnt. gt 2) %then %do;
           %unquote(T2Q&vvar.&value. = dif2(T&vvar.&value.) / lag2(T&vvar.&value.) * 100;)
           %end;
          %unquote(TLOG&vvar.&value. = log(T&vvar.&value.);)

     %end;
%mend;
;

%macro loop2(charvars=,counter=);
     %let count2=%sysfunc(countw(&charvars));

     %do j = 1 %to &count2;
           %let vvar=%scan(%bquote(&charvars),&j,%str( ));

           %loop(values=%str(1,2,3,4),cnt=&counter.);
     %end;
%mend;
;
%macro weighted(var=, state=, state_bin=, dsnin=, dsnout=, weight_var= );
     proc means data=&dsnin.
           (where = (state in (&state.)));
         by period ;
           class prop_location;
         var &var.;
         weight &weight_var.;
     
         output out = &dsnout. (where=(prop_location^="") drop=_type_ _freq_) 
           mean=&var.;
     run;
     proc sort data= weighted_&state_bin.; by period;run;
%mend weighted;
;

%macro m_create_weight(dsnin=,dsnout=);
     proc sort data=&dsnin.;
           by period ostate pd_group;
     run;

     proc summary data=&dsnin.;
           by period ostate pd_group;
           output out=summary
           sum(loan_curr_upb_amt) = balance_t;
     run;


     proc sql;
           create table temp_stacked_data as
           select a.*
                ,b.balance_t
           from &dsnin. as a
           left join summary as b
           on a.period=b.period
           and a.ostate = b.ostate
           and a.pd_group = b.pd_group
     ;quit;

     proc sql;
           create table &dsnout. as
           select distinct pd_group, ostate
                , avg(balance_t/sum_balance_t) as weight
           from(
                select distinct ostate, qtime, pd_group, balance_t, sum(balance_t) as sum_balance_t
                from 
                     (select distinct ostate, qtime, pd_group, balance_t
                           ,put(ostate,$property_location.) as ostate_bin
                     from temp_stacked_data
                     (where=(upcase(ostate) in (&state_in.)))
                     ) as v
                group by ostate_bin, qtime, pd_group
           ) as vv
           group by ostate, pd_group
     ;quit;


/*   Adding records for Other states where for each pd_group the wight will be 1.0*/
     proc sql;
           create table &dsnout. as
           select pd_group, ostate, weight 
           from &dsnout.
           union all
           select distinct pd_group,"US" as ostate, 1 as weight
           from &dsnout.
           order by pd_group, ostate, weight
     ;quit;

%mend m_create_weight;
;


%macro m_create_macroecon_data(scen=,dsnin=,dsnin_weight =,dsnout_pd=, dsnout_lgd=);

* Processing for PD: 
State values of macros to be used only for those states which are assigned to SAS macro variable state_in
within 00_config. For all other states US macro value to be used.;
    proc sort data=&dsnin.;
        by period state scenario macro_name val;
    run;

     proc transpose data=&dsnin.(where=(state in (&state_in.,"US"))) out=work.macro_qtr_tr_pd_&scen. (drop=_name_);
           by period state scenario;
           var  val;
           id macro_name;
     run;  

     proc sql;
           create table &OutDataLib..macro_qtr_state_pd_&scen. as 
            select a.*
                     ,b.pd_group
                     ,coalesce(b.weight,1) as weight
                ,put(a.state,$PROPERTY_LOCATION.)  as prop_location                                
           from work.macro_qtr_tr_pd_&scen. as a
                left join &dsnin_weight. as b
                     on upcase(a.state)  = upcase(b.ostate)
                where upcase(scenario)=upcase("&scen.")
            order by pd_group, prop_location, period, scenario

     ;quit;


     proc summary data=&OutDataLib..macro_qtr_state_pd_&scen.;
           by pd_group prop_location period scenario;
           var &vart.;
              weight weight;
           output out=macro_qtr_summary_pd_&scen.(drop=_freq_ _type_)
           sum()= ;
     run;
     

     data work.vintage_in_tecon_pd_&scen.;
           set macro_qtr_summary_pd_&scen.;
           %do i = 1 %to %sysfunc(countw(&var));
                %scan(&var,&i) = %scan(&vart,&i);
           %end;
              qtime=intck("qtr","01Jan1960"d, period);

     run;

     proc sort data=work.vintage_in_tecon_pd_&scen.;
/*	 sorting by pd_group prop_location qtime before applying lag and diff function in macro loop2*/
              by pd_group prop_location qtime;
     run;

     data work.vintage_in_tecon_pd_&scen.;
     set work.vintage_in_tecon_pd_&scen.;
           by pd_group prop_location qtime;
           cnt+1;
           if first.state then cnt=1;
           call symput("cnt",cnt);
     run;

     data work.vintage_out_teconx_pd_&scen.;
           set work.vintage_in_tecon_pd_&scen.;
/*           %loop2(%str(&var));*/
              %loop2(charvars=%str(&var),counter=cnt);
     run;
     

     proc sort data = work.vintage_out_teconx_pd_&scen.(drop=&var.) 
           out = &dsnout_pd.;
           by qtime;
     run;
     
* Processing for LGD:
State values of macros to be used for all states including which are assigned to SAS macro variable state_in
within 00_config;

     proc transpose data=&dsnin. out=work.macro_qtr_tr_lgd_&scen. (drop=_name_);
           by period state scenario;
           var  val;
           id macro_name;
     run;  

     data &OutDataLib..macro_qtr_state_lgd_&scen.;
         set work.macro_qtr_tr_lgd_&scen.(where=(upcase(scenario)=upcase("&scen.")));
     run;

     data macro_qtr_summary_lgd_&scen.;
           set &OutDataLib..macro_qtr_state_lgd_&scen.;
     run;

     data work.vintage_in_tecon_lgd_&scen.;
            set macro_qtr_summary_lgd_&scen.;
            %do i = 1 %to %sysfunc(countw(&var));
                 %scan(&var,&i) = %scan(&vart,&i);
            %end;
              qtime=intck("qtr","01Jan1960"d, period);

     run;

     proc sort data=work.vintage_in_tecon_lgd_&scen.;
	 /*	 sorting by state qtime before applying lag and diff function in macro loop2*/
         by state qtime;
     run;

     data work.vintage_in_tecon_lgd_&scen.;
           set work.vintage_in_tecon_lgd_&scen.;
              by state qtime;
              cnt+1;
              if first.state then cnt=1;
              call symput("cnt",cnt);
     run;

     data work.vintage_out_teconx_lgd_&scen.;
            set work.vintage_in_tecon_lgd_&scen.;
                %loop2(charvars=%str(&var),counter=cnt);

     run;


     proc sort data = work.vintage_out_teconx_lgd_&scen.(drop=&var. ) 
            out = &dsnout_lgd.;
            by state;
     run;
%mend m_create_macroecon_data;
;
%macro m_create_macroecon_data_loop(dsnin=,macrodsnin=,dsnout_pd=, dsnout_lgd=);
    %if %sysfunc(exist(&dsnout_pd.)) %then %do; 
        proc delete data=&dsnout_pd.;run; 
    %end; 
    %if %sysfunc(exist(&dsnout_lgd.)) %then %do; 
        proc delete data=&dsnout_lgd.;run; 
    %end;

     %m_create_weight(dsnin=&dsnin.,dsnout=work.avg_weight_summary_state);
     *&num_scen. is defined in 00_config and represents number of scenarios;
     %do cntr = 1 %to &num_scen.;
          %let scenario = &&scn&cntr.;
           %m_create_macroecon_data(scen=&scenario.
                                           ,dsnin=&macrodsnin.
                                           ,dsnin_weight = work.avg_weight_summary_state
                                           ,dsnout_pd=&dsnout_pd._&scenario.
                                           ,dsnout_lgd=&dsnout_lgd._&scenario.
                                           );
           %m_append_dsn(dsnin=&dsnout_pd._&scenario., dsnout=&dsnout_pd., delete_dsnin=1);
           %m_append_dsn(dsnin=&dsnout_lgd._&scenario., dsnout=&dsnout_lgd., delete_dsnin=1);
     %end;
%mend m_create_macroecon_data_loop;
;
/*-----------------------------------------------------------------------*/

/*-----------------------------------------------------------------------*/
 * Macros for scoring PD for HELOC;
/*-----------------------------------------------------------------------*/
;

%macro m_score_heloc_PD(DataIn=,DataOut=,MacroData_In=,qtimestart=);

     data HELOC_MACRO_US;
           set &MacroData_In.(where=(upcase(pd_group)="HELOC" and upcase(prop_location)="US"));
     run;

     data HELOC_MACRO_ST;
           set &MacroData_In.(where=(upcase(pd_group)="HELOC"));
     run;

     data HELOC_LOAN;
           set &DataIn.(where=(upcase(pd_group)="HELOC" and existing_flag = 1));

           delinquent = 0;

           if 90<=delq_curr<=179 /*  and default = 0 and prepay=0 */then
                do;
                     delinquent = 1;
                end;
     run;

     data HELOC_loan_copy;
           set HELOC_LOAN(keep=qtime loan_curr_upb_amt original_loan_fk scenario);
           qtime_1 = qtime + 1;
     run;

     proc sql;
           create table HELOC_LOAN_2 as
                select a.*, b.loan_curr_upb_amt as lag1_loan_curr_upb_amt
                     from HELOC_LOAN a
                           left join HELOC_loan_copy b
                                on a.qtime = b.qtime_1 and a.original_loan_fk = b.original_loan_fk
                                and a.scenario = b.scenario
           ;
     quit;

     proc sql;
           create table HELOC_panel_1 as select
                      existing_flag,
                scenario,
                qtime, period, loan_lien, cltv_heloc, fico_heloc, loan_age_heloc, property_location, panel,
                sum(loan_int_only_ind)/count(loan_int_only_ind)*100 as pct_IO_100, 
                avg(loan_curr_int_rate)*100 as avg_loan_curr_int_rate100, 
                count(original_loan_fk) as FREQ, 
                avg(utilization_rate)*100 as avg_utilization_rate_100, 
           sum(loan_orig_exposure_amt) as loan_orig_exposure_amt,
           sum(lag1_loan_curr_upb_amt) as lag1_loan_curr_upb_amt, 
           sum(loan_curr_upb_amt) as loan_curr_upb_amt, 
/*           sum(lag1_loan_curr_upb_amt*delinquent)/sum(lag1_loan_curr_upb_amt)*100 as rdelinquent_100*/
           sum(loan_curr_upb_amt*delinquent)/sum(loan_curr_upb_amt)*100 as rdelinquent_100,
              sum(loan_curr_upb_amt*delinquent) as sum_delq_bal,
              sum(delinquent) as sum_delq
           from HELOC_LOAN_2
                group by 
                     existing_flag,scenario,qtime, period, loan_lien, cltv_heloc, fico_heloc, loan_age_heloc, property_location, panel;
     quit;

     /*-----------------------------------------------*/
     proc sql;
           create table HELOC_panel_2 as 
                select a.*, 
                     b.TLOGRPRIME2, b.T2QCBC, b.T2QRPRIME, b.FRPRIME,
                     c.T2QHOFHOPI, c.TQQHOFHOPI, c.TLBR2, c.T2QLBR1,
                     (a.avg_loan_curr_int_rate100 - b.FRPRIME) as avg_spread
                from HELOC_panel_1 a
           left join HELOC_MACRO_US b
                on a.qtime = b.qtime
                and a.scenario = b.scenario
           left join HELOC_MACRO_ST c
           on a.qtime = c.qtime and a.property_location = c.prop_location
           and a.scenario = c.scenario
           ;
     QUIT;

     data HELOC_panel_copy;
           set HELOC_panel_2 (keep=panel qtime avg_spread avg_utilization_rate_100 rdelinquent_100 scenario);
           qtime_1 = qtime+1;
     run;

     proc sql;
           create table HELOC_panel_3 as
                select a.*, 
                     b.avg_spread as lag1_avg_spread, 
                     b.avg_utilization_rate_100 as lag1_avg_utilization_rate_100,
                     b.rdelinquent_100 as lag1_rdelinquent_100
                from HELOC_panel_2 a
                     left join HELOC_panel_copy b
                           on a.qtime = b.qtime_1 and a.panel = b.panel
                           and a.scenario = b.scenario
           ;
     quit;

     proc sql;
           create table HELOC_panel_4 as 
                select a.*,
                     case when loan_lien = "FIRST" then 1 else 0 end as lien_1st,
                     case when qtr(period) = 4 then 1 else 0 end as Q4,
                          input(put(cltv_heloc,$ocltv_lt50_heloc.),best12.) as ocltv_lt50,
                           input(put(cltv_heloc,$ocltv_50_80_heloc.),best12.) as ocltv_50_80,
                           input(put(FICO_heloc,$fico_lt699_heloc.),best12.) as fico_lt699,
                           input(put(FICO_heloc,$fico_700_739_heloc.),best12.) as fico_700_739,
                           input(put(FICO_heloc,$fico_740_799_heloc.),best12.) as fico_740_799,
                           input(put(FICO_heloc,$fico_ge800_heloc.),best12.) as fico_ge800,
                           input(put(FICO_heloc,$fico_700_799_heloc.),best12.) as fico_700_799,
                           input(put(loan_age_heloc,$mob_0_18_heloc.),best12.) as mob_0_18,
                           input(put(loan_age_heloc,$mob_19_48_heloc.),best12.) as mob_19_48,
                           input(put(loan_age_heloc,$mob_0_48_heloc.),best12.) as mob_0_48,
                           input(put(loan_age_heloc,$mob_gt48_heloc.),best12.) as mob_gt48
           from HELOC_panel_3 a
           ;
     quit;

     data HELOC_panel_5;
     set HELOC_panel_4;
                Pred_D90 = 1/(1+exp(-(-7.02015401+0.0067274*pct_IO_100-0.622188471*ocltv_lt50-0.441336818*ocltv_50_80
                                -0.572614683*fico_700_739-0.876489475*fico_740_799-1.316559852*fico_ge800
                         -1.217349095*mob_0_18-0.281477255*mob_19_48+0.10634871*Q4+0.219632962*lag1_avg_spread+0.021528265*lag1_avg_utilization_rate_100
                         -0.119559097*TQQHOFHOPI-0.01445506*T2QRPRIME+0.049034679*TLBR2)));

                Pred_PP = 1/(1+exp(-(-6.028266603+0.005996433*pct_IO_100+0.677597423*mob_0_48-0.005273508*lag1_avg_utilization_rate_100
                        +0.202470593*T2QHOFHOPI+1.422201532*TLOGRPRIME2+0.006085423*T2QCBC)));

     run;

     data HELOC_panel_copy;
     set HELOC_panel_5(keep=Pred_D90 panel qtime scenario);
           qtime_1=qtime+1;
     run;


/*proc sql;*/
/*           create table HELOC_panel_6 as*/
/*                select a.*, b.Pred_D90 * 100 as lag1_Pred_D90_100,*/
/*                case when a.qtime = (&qtimestart. + 2) then lag1_rdelinquent_100 else calculated lag1_Pred_D90_100 end as lag1_D90_100*/
/*           from HELOC_panel_5 a*/
/*                left join HELOC_panel_copy b*/
/*                     on a.qtime=b.qtime_1 and a.panel = b.panel*/
/*                     and a.scenario = b.scenario*/
/*           ;*/
/*     quit;*/
     proc sql;
           create table HELOC_panel_6 as
                select a.*
/*                   ,case when a.qtime=&qtimestart.+01 then a.rdelinquent_100 else b.Pred_D90 * 100 end as lag1_D90_100*/
                     ,case when a.qtime=&qtimestart.+01 then a.lag1_rdelinquent_100 else b.Pred_D90 * 100 end as lag1_D90_100

           from HELOC_panel_5 a
                left join HELOC_panel_copy b
                     on a.qtime=b.qtime_1 and a.panel = b.panel
                     and a.scenario = b.scenario
           ;
     quit;


     data &DataOut.;
     retain scenario period qtime property_location panel;
     set HELOC_panel_6(where=(qtime >= &qtimestart.));

                Pred_D180 = 1/(1+exp(-(-7.922681595-0.192953108*lien_1st-0.810325829*ocltv_lt50-0.361372345*ocltv_50_80
                     -0.13375272*fico_700_799-0.410419335*fico_ge800-1.399365001*mob_0_18-0.302841082*mob_19_48
                     +0.09862952*Q4+0.169183928*lag1_D90_100+0.04120632*lag1_avg_utilization_rate_100
                     +0.005199584*T2QLBR1-0.035253931*T2QHOFHOPI)));

     run;
%mend m_score_heloc_PD;

;
%macro m_score_heloc_PD_model(dsnin=,dsnout=);
     %if %sysfunc(exist(&dsnout.)) %then %do; 
         proc delete data=&dsnout.;run; 
     %end;
           %let qtime_start = %sysfunc(intck(qtr,"01Jan1960"d,%sysfunc(inputn(&month.01&year.,mmddyy10.)))); 
           %put &qtime_start.;  

           %m_score_heloc_PD(DataIn=&dsnin.
                                     ,DataOut=&dsnout.
                                     ,MacroData_In=&outdatalib..macro_econ_pd
                                     ,qtimestart = &qtime_start.);

%mend m_score_heloc_PD_model;
;

/*-----------------------------------------------------------------------*/
;
/*-----------------------------------------------------------------------*/
 * Macros for scoring PD for Mortgage;
/*-----------------------------------------------------------------------*/
;
%macro m_score_mortgage_pd(DataIn=, DataOut=, MacroDataIn=,qtimestart=,include_orig=);

	 %if %eval(&include_orig.) %then %do;
	 	%let where_txt = where  1;
	 %end;
	 %else %let where_txt = where existing_flag eq 1;

     data MORTGAGE_MACRO_US;
           set &MacroDataIn.(where=(upcase(pd_group)="MORTGAGE" and upcase(prop_location)="US"));

     run;

     data MORTGAGE_MACRO_ST;
           set &MacroDataIn.(where=(upcase(pd_group)="MORTGAGE"));		 	
     run;

   
     data test_stacked_data_w_panel_t1;
           set &DataIn.(where=(upcase(pd_group)="MORTGAGE"));
           delinquent = 0;
           if delq_curr>=90 and delq_curr<=179 /*  and default = 0 and prepay=0 */ then
            do;
                 delinquent = 1;
            end;	           
     run;

     proc sort data=test_stacked_data_w_panel_t1;
        by scenario original_loan_fk vintage qtime;
     run;

     data test_stacked_data_w_panel_t1;
        retain scenario original_loan_fk qtime loan_curr_int_rate lag_loan_curr_int_rate;
        set test_stacked_data_w_panel_t1;
        by scenario original_loan_fk vintage qtime;
            lag_loan_curr_int_rate = lag(loan_curr_int_rate);
            if first.vintage then lag_loan_curr_int_rate=.;
    run;
    data test_stacked_data_w_panel_t;
        set test_stacked_data_w_panel_t1;
        *excluding origination for UAT only;
        &where_txt.;       
    run;

	 proc sql;
	 create table test_stacked_data_w_panel_t as 
	 	select a.*
            ,(a.lag_loan_curr_int_rate*100 - b.TRFHFBE1) as loan_spread
	 from test_stacked_data_w_panel_t as a
		left join MORTGAGE_MACRO_ST b
           on a.qtime = b.qtime 
           and a.property_location = b.prop_location
           and a.scenario = b.scenario	
		;
	 quit;

     proc sort data = test_stacked_data_w_panel_t;
           by original_loan_fk;
     run;


     data MORTGAGE_loan_copy;
           set test_stacked_data_w_panel_t(keep=qtime loan_curr_upb_amt original_loan_fk scenario existing_flag Jumbo_ever);
           qtime_1 = qtime + 1;
     run;



     proc sql;
           create table MORTGAGE_LOAN_2 as
                select a.*, b.loan_curr_upb_amt as lag1_loan_curr_upb_amt
                    	 from test_stacked_data_w_panel_t a
                           left join MORTGAGE_loan_copy b
                                on a.qtime = b.qtime_1 
								and a.original_loan_fk = b.original_loan_fk
                                and a.scenario = b.scenario
								and a.existing_flag = b.existing_flag
           ;
     quit;

     proc sql;
           create table MORTGAGE_panel_1 as select
				scenario,
                qtime, period, Product_Type, Fix_Arm, cltv_MRTG, fico_MRTG, loan_age_MRTG, property_location, panel,
                sum(loan_int_only_ind)/count(loan_int_only_ind)*100 as pct_IO_100, 
                avg(loan_curr_int_rate)*100 as avg_loan_curr_int_rate100, 
                avg(loan_spread) as avg_spread,
                count(original_loan_fk) as FREQ, 
                avg(Jumbo_ever)*100 as rjumbo_ever_100,
                sum(lag1_loan_curr_upb_amt) as lag1_loan_curr_upb_amt, 
                sum(loan_curr_upb_amt) as loan_curr_upb_amt, 
                sum(loan_curr_upb_amt*delinquent)/sum(loan_curr_upb_amt)*100 as rdelinquent_100,
				case when Fix_Arm = "FIX" then 1 else 0 end as Fix,
           		case when Product_Type = "OWNED" then 1  else 0 end as Owned
           from MORTGAGE_LOAN_2
                group by 
                     scenario,
                     qtime, period, Product_Type, Fix_Arm, cltv_MRTG, fico_MRTG, loan_age_MRTG, property_location, panel
           ;
     quit;

     proc sql;
           create table MORTGAGE_panel_2 as 
                select a.*, 
                     b.TRILIBOR12M1, b.TQQCBC1, b.TQQCBC,
                     C.T2QRFHFBE1, c.TYYLBR1, c.TYYHOFHOPI1, c.T2QLBR1, c.TLOGLBR, c.TQQHOFHOPI, c.TYYLBR, c.TLBR1, c.FRFHFBE,
                     avg_spread * Fix as avg_spread_fix
                from MORTGAGE_panel_1 a
                     left join MORTGAGE_MACRO_US b
                           on a.qtime = b.qtime
                           and a.scenario = b.scenario
                     left join MORTGAGE_MACRO_ST c
                           on a.qtime = c.qtime 
                           and a.property_location = c.prop_location
                           and a.scenario = c.scenario
    ;quit;

     data MORTGAGE_panel_copy;
           set MORTGAGE_panel_2 (keep=panel qtime avg_spread_fix rdelinquent_100 scenario);
           qtime_1 = qtime+1;
     run;

     proc sql; 
           create table MORTGAGE_panel_3 as 
                select a.*, 
                        b.avg_spread_fix as lag1_avg_spread_fix, 
                        b.rdelinquent_100 as lag1_rdelinquent_100 
                from MORTGAGE_panel_2 a 
                        left join MORTGAGE_panel_copy b 
                                on a.qtime = b.qtime_1 
								and a.panel = b.panel 
                                and a.scenario = b.scenario 
             ; 
     quit;

     proc sql;
           create table MORTGAGE_panel_4 as 
                select a.*
					 ,input(put(cltv_mrtg,$ocltv_lt50_mrtg.),best12.) as ocltv_lt50
					 ,input(put(cltv_mrtg,$ocltv_50_70_mrtg.),best12.) as ocltv_50_70
					 ,input(put(cltv_mrtg,$ocltv_70_80_mrtg.),best12.) as ocltv_70_80
					 ,input(put(cltv_mrtg,$ocltv_ge80_mrtg.),best12.) as ocltv_ge80
					 ,input(put(FICO_mrtg,$fico_le659_mrtg.),best12.) as fico_le659
					 ,input(put(FICO_mrtg,$fico_660_699_mrtg.),best12.) as fico_660_699
					 ,input(put(FICO_mrtg,$fico_700_739_mrtg.),best12.) as fico_700_739
					 ,input(put(FICO_mrtg,$fico_ge740_mrtg.),best12.) as fico_ge740
					 ,input(put(loan_age_mrtg,$mob_0_12_mrtg.),best12.) as mob_0_12
					 ,input(put(loan_age_mrtg,$mob_le48_mrtg.),best12.) as mob_le48
					 ,input(put(loan_age_mrtg,$mob_49_96_mrtg.),best12.) as mob_49_96
                     ,case when qtr(period) = 4 then 1 else 0 end as Q4
                     ,TYYLBR1*calculated ocltv_ge80 as TYYLBR1_ocltv_ge80
                     ,TYYLBR1*calculated fico_le659 as TYYLBR1_fico_le659
           from MORTGAGE_panel_3 a
           ;
     quit;

     data MORTGAGE_panel_5;
           set MORTGAGE_panel_4;
           Pred_D90 = 1/(1+exp(-(-3.8056587417+0.1467593942*Owned-0.3655157862*Fix-1.4743065578*ocltv_lt50-0.5940417743*ocltv_50_70-0.3265325571*ocltv_70_80
                -0.6272330417*fico_660_699-1.0890595890*fico_700_739-2.1811435604*fico_ge740-1.1653214617*mob_0_12+0.1139690611*Q4
				-0.0569312114*TYYHOFHOPI1
				+0.0174722313*T2QLBR1+0.2161287560*TLOGLBR-0.1310385031*TRILIBOR12M1
				-0.0186672025*rjumbo_ever_100
				)));
           Pred_PP = 1/(1+exp(-(-3.330689331-0.315039229*Fix
				-0.109320146*ocltv_70_80
				-0.359344921*ocltv_ge80
				-0.52122805*fico_le659
                -0.420769833*fico_660_699
				-0.304524529*fico_700_739
                +0.625103771*mob_le48
				+0.239891818*mob_49_96
/*				+0.141804739*Q4*/
				+0.376384416*avg_spread_fix
				-0.019225518*T2QRFHFBE1
				-0.003664128*TYYLBR1
				+0.00288531*TQQCBC
                -0.00388783*TYYLBR1_ocltv_ge80
				-0.006733397*TYYLBR1_fico_le659
				)));
     run;

     data MORTGAGE_panel_copy;
           set MORTGAGE_panel_5(keep=Pred_D90 panel qtime scenario);
           qtime_1=qtime+1;
     run;

     proc sql;
           create table MORTGAGE_panel_6 as
                select a.*
					,case when a.qtime=&qtimestart.+01 then a.lag1_rdelinquent_100 else b.Pred_D90 * 100 end as lag1_D90_100

                     from MORTGAGE_panel_5 a
                           left join MORTGAGE_panel_copy b
                                on a.qtime=b.qtime_1 and a.panel = b.panel
                                and a.scenario = b.scenario
           ;
     quit;

	 data &DataOut.;
           retain scenario period qtime property_location panel;
           set MORTGAGE_panel_6(where=(qtime >= &qtimestart.));
           Pred_D180 = 1/(1+exp(-(-5.9063037456+0.1390178238*Owned-0.3090367074*Fix-1.6416022293*ocltv_lt50-0.6808012766*ocltv_50_70-0.3052885060*ocltv_70_80
                -0.2650903959*fico_660_699-0.5534402029*fico_700_739-1.4983558808*fico_ge740+0.1154190848*Q4
                -1.4235574064*mob_0_12
				+0.1502513029*lag1_D90_100
				-0.0176256779*rjumbo_ever_100-0.1141728042*TQQHOFHOPI
				-0.0035900516*TQQCBC1
                +0.0081296885*TYYLBR-0.1147284029*TRILIBOR12M1+0.2391963316*TLBR1
				)));
     run;

		proc sql; 
			create table Mortgage_chk as 
				select scenario 
				,qtime
				,sum(Pred_PP*lag1_loan_curr_upb_amt)/sum(lag1_loan_curr_upb_amt) as pp
				,sum(Pred_D90*lag1_loan_curr_upb_amt)/sum(lag1_loan_curr_upb_amt) as D90 
				,sum(Pred_D180*lag1_loan_curr_upb_amt)/sum(lag1_loan_curr_upb_amt) as D180
				,sum(lag1_loan_curr_upb_amt) as balance 
			from &DataOut.
				where qtime > &qtimestart. 
				group by scenario, qtime
			; 
		quit;

%mend m_score_mortgage_pd;
;



%macro m_score_mortgage_pd_model(dsnin=,dsnout=,include_origination=1);
     %if %sysfunc(exist(&dsnout.)) %then %do; 
         proc delete data=&dsnout.;run; 
     %end;
           %let qtime_start = %sysfunc(intck(qtr,"01Jan1960"d,%sysfunc(inputn(&month.01&year.,mmddyy10.)))); 
           %put &qtime_start.;  

           %m_score_mortgage_pd(DataIn=&dsnin.
                                     ,DataOut=&dsnout.
                                     ,MacroDataIn=&outdatalib..macro_econ_pd
/*                                     ,MacroDataIn=macro_dev_fin*/
                                     ,qtimestart = &qtime_start.
									 ,include_orig=&include_origination.);

%mend m_score_mortgage_pd_model;
;
/*-----------------------------------------------------------------------*/
;
/*-----------------------------------------------------------------------*/
 * Macros for scoring LGD;
/*-----------------------------------------------------------------------*/
;

%macro m_score_lgd(data_in=,data_out=,scen=, macro_econ=, LGD_bin_1=, LGD_bin_2=, LGD_bin_3=,qtimestart=,include_orig= );

	data temp1;
		/*only keep forecasting period */
		set &Data_in.(where=(qtime >= &qtimestart.)); 
		/*occupany indicator - 1 (primary residence), 0 otherwise (investment, second home etc.)*/
		occupancy = coalesce((property_usage_type_fk=4),0);                                                            
		/*states where the foreclosure process uses courts*/
		if ostate in (&lgd_judicial_states.) then judicial=1; else judicial=0;           
		/*states with NULL values are provided US figures*/
		if ostate not in (&lgd_states_with_macro.) then ostate="US";                                           
		/*create a numeric value for origination date */
		Oqtime = intck("qtr","01Jan1960"d, loan_orig_note_date);    
		/*if the origination date is before Q1-2000 make the date Q1-2000 as macro vars available from 2000  */

		if Oqtime<intck("qtr","01Jan1960"d, &min_loan_orig_note_dt_macroecon.) then 
			Oqtime=intck("qtr","01Jan1960"d, &min_loan_orig_note_dt_macroecon.);                                             
	run;

	proc sort data=temp1;
	     by qtime;
	run;


	data temp2;
	set &macro_econ.(where=(upcase(state)="US"));
	/*** retrive US-level macro econ vars ***/
	keep TLOGCBC qtime;
	run; 

	proc sort data=temp2;
	     by qtime;
	run;

	data temp3;
	/*** Merge Macro econ Vars to loan level dataset ***/
	/*** US-level: Merge by qtime ***/
	     merge temp1 (in=a) temp2;
	     by qtime;
	     if a;
	run;

	proc sort data=temp3;
	     by ostate qtime;
	run;


	data temp4;
		set &macro_econ.;
		/*** retrive STATE-level macro econ vars ***/
		keep T2QHOFHOPI TLBR2 FHOFHOPI state qtime;
		/*HPI at current time required for calculating CLTV*/
		rename FHOFHOPI=FHOFHOPI_T state=ostate;	  
	 
	run;

	proc sort data=TEMP4;
	     by ostate qtime;
	run;

	data TEMP4A;
		set TEMP4;
		keep FHOFHOPI_T ostate qtime;
		rename qtime=Oqtime;
		/*HPI at origination time required for calculating CLTV*/
		rename FHOFHOPI_T=FHOFHOPI_O;                                              
	run;

	proc sort data=TEMP4A;
	     by ostate Oqtime;
	run;



	data lgd_&scen._1;
	/*** Merge Macro econ Vars to loan level dataset ***/
	/*** STATE-level: Merge by qtime and state ***/
	     merge TEMP3(in=a) TEMP4;
	    by ostate qtime;                                                       
	    if a;
	run;

	proc sort data=lgd_&scen._1;
	     by ostate Oqtime;
	run;

	data lgd_&scen._1A;
	/*** Merge origination HPI ***/
	     merge lgd_&scen._1 (in=a) TEMP4A;
	    by ostate Oqtime;                                                      
	    if a;
	run;
	     


	/*** Calculate Current CLTV ***/
	/*** Current CLTV = Origination exposure / (Origination valuation *(Current HPI / Origination HPI)) ***/
	/*** Origination valuation = Origination exposure / OCLTV ***/
	data lgd_&scen._2;
	set lgd_&scen._1A;
	     /*if OCLTV is missing then assume it to be 100%*/
 
		if upcase(pd_group) = "MORTGAGE" then do;	     
		 valuation_at_origination=loan_orig_exposure_amt/OCLTV;          

		     current_valuation=valuation_at_origination*FHOFHOPI_T/FHOFHOPI_O;          
		     
		     if current_valuation=0 OR current_valuation =. then Calculated_CLTV=1;                       
		     else Calculated_CLTV=loan_curr_exposure_amt/current_valuation;      
		end;
		else;
 		if upcase(pd_group) = "HELOC" then do;	
		     if ocltv=. OR ocltv=0 then NoMiss_OCLTV=1;
		     else NoMiss_OCLTV=ocltv;                    
			 valuation_at_origination=loan_orig_exposure_amt/NoMiss_OCLTV;          
		     current_valuation=valuation_at_origination*FHOFHOPI_T/FHOFHOPI_O;          
		     
		     if current_valuation=0 OR current_valuation =. then Calculated_CLTV=1;                       
		     else Calculated_CLTV=loan_orig_exposure_amt/current_valuation;        
		end;
	run;
	 %if %eval(&include_orig.) %then %do;
	 	%let existing_flag =   1;
	 %end;
	 %else %let existing_flag = existing_flag eq 1;


	/*** Calculate LGD ***/
	data &data_out.;
		retain pd_group period qtime scenario state;
		set lgd_&scen._2; 
		scenario = "&scen.";

		/*"1st Lien HELOC" and "Mortgage" use the same model*/
		if upcase(pd_group) = "MORTGAGE" then do;
            if (&existing_flag.)  then                                            
    		   LGD=0.2317272451+(0.1765021917*Calculated_CLTV)             
                   +(0.0398364819*judicial)-(0.0251178167*occupancy)
                   -(0.0157807007*T2QHOFHOPI)+(0.0145807112*TLBR2)
                   -(0.0402447050*TLOGCBC);      
        end; 
/*“The primary-residence indicator field (occupancy) is muted while calculating LGD rate for 1st lien HELOCs.”*/
		else if upcase(pd_group) = "HELOC" and existing_flag = 1 then do;
			if (loan_lien_position = 1 and existing_flag = 1)  then                                            
			   LGD=0.231728+(0.176502*Calculated_CLTV)             
			        +(0.039836*judicial)-(0.0*occupancy)
			        -(0.01578*T2QHOFHOPI)+(0.014581*TLBR2)
			        -(0.04025*TLOGCBC);   
		/*"2nd Lien HELOC" use LGD provided by ALLL*/
			else if (loan_lien_position=2 and existing_flag = 1) and Calculated_CLTV<0.7        
				then LGD=&LGD_bin_1*1;               
			else if (loan_lien_position=2 and existing_flag = 1) and 0.7<=Calculated_CLTV<0.9
				then LGD=&LGD_bin_2*1;    
			else if (loan_lien_position=2 and existing_flag = 1) and Calculated_CLTV>=0.9
				then LGD=&LGD_bin_3*1;  
		end;
	run;

%mend m_score_lgd;
;

%macro m_score_lgd_loop(dsnin=,dsnout=, macro_dsnin=,include_origination=);
     %if %sysfunc(exist(&dsnout.)) %then %do; 
         proc delete data=&dsnout.;run; 
     %end;

     *&num_scen. is defined in 00_config and represents number of scenarios;
     %do cntr = 1 %to &num_scen.;
           %let scenario = &&scn&cntr.;
           %let qtime_start = %sysfunc(intck(qtr,"01Jan1960"d,%sysfunc(inputn(&month.01&year.,mmddyy10.)))); 
			%let lgd_bin_1 = &&sc_bin_1&cntr.;
			%let lgd_bin_2 = &&sc_bin_2&cntr.;
			%let lgd_bin_3 = &&sc_bin_3&cntr.;

			data temp_macro_econ_lgd_&scenario.;
				set &macro_dsnin.(where=(upcase(scenario) = "&scenario."));
			run;
			data temp_lgd_&scenario.;
				set &dsnin.(where=(upcase(scenario) = "&scenario." and qtime >= &qtime_start.));
			run;				
           %m_score_lgd(Data_in=temp_lgd_&scenario.
                     , data_out=&dsnout._&scenario.
                     , scen=&scenario.
                     , macro_econ=temp_macro_econ_lgd_&scenario.
                     , lgd_bin_1=&lgd_bin_1;
                     , lgd_bin_2=&lgd_bin_2;
                     , lgd_bin_3=&lgd_bin_3; 
                     , qtimestart = &qtime_start.
                     , include_orig=&include_origination.);

           %m_append_dsn(dsnin=&dsnout._&scenario., dsnout=&dsnout., delete_dsnin=1);

     %end;

 	data &dsnout.;
		set &dsnout.;
			where upcase(pd_group) = "MORTGAGE" or (upcase(pd_group) = "HELOC" and existing_flag=1);
	run;

    proc sql; 
    	create table LGD_chk as 
    		select 
            pd_group
            ,scenario 
    		,qtime
     		,sum(LGD*loan_curr_upb_amt)/sum(loan_curr_upb_amt) as LGD
    		,sum(loan_curr_upb_amt) as balance 
    	from &dsnout.
    	where qtime > &qtime_start. and existing_flag=1
    	group by pd_group,scenario, qtime
    	; 
    quit;
%mend m_score_lgd_loop;
;

/*-----------------------------------------------------------------------*/
;
/*-----------------------------------------------------------------------*/
 * Macros for merging score results of PD LGD;
/*-----------------------------------------------------------------------*/
;

%macro m_merge_score_results(dsnin=,dsn_heloc_pd=, dsn_mortgage_pd=,dsn_lgd=,dsnout=);

           %let qtime_start = %sysfunc(intck(qtr,"01Jan1960"d,%sysfunc(inputn(&month.01&year.,mmddyy10.)))); 

	proc sql;
		create table &dsnout. as 
			select a.*
				,case when a.pd_group="MORTGAGE" then c.Pred_D90 
					when a.pd_group="HELOC" then b.Pred_D90 else . end as Pred_D90
				,case when a.pd_group="MORTGAGE" then c.Pred_PP 
					when a.pd_group="HELOC" then b.Pred_PP else . end as Pred_PP
/*				,case when a.pd_group="MORTGAGE" then c.lag1_Pred_D90_100 */
/*					when a.pd_group="HELOC" then b.lag1_Pred_D90_100 else . end as lag1_Pred_D90_100*/
				,case when a.pd_group="MORTGAGE" then c.lag1_D90_100 
					when a.pd_group="HELOC" then b.lag1_D90_100 else . end as lag1_D90_100
				,case when a.pd_group="MORTGAGE" then c.Pred_D180 
					when a.pd_group="HELOC" then b.Pred_D180 else . end as Pred_D180
				,case when a.pd_group="MORTGAGE" then c.lag1_loan_curr_upb_amt 
					when a.pd_group="HELOC" then b.lag1_loan_curr_upb_amt else . end as lag1_loan_curr_upb_amt				
				,calculated Pred_D180*(a.lgd) as GCO
				,case when a.pd_group="HELOC" then b.T2QLBR1 else c.T2QLBR1 end as T2QLBR1
				,(1-(calculated PRED_D180 + Calculated Pred_PP + Calculated Pred_D90)) as CURRENT
                ,b.TLOGRPRIME2 
                ,b.T2QCBC 
                ,b.T2QRPRIME
                ,b.T2QHOFHOPI
                ,b.TQQHOFHOPI
                ,b.TLBR2
                ,b.FRPRIME 
				,b.lag1_avg_utilization_rate_100
				,c.lag1_avg_spread_fix
				,c.rjumbo_ever_100
                ,c.TRILIBOR12M1 
                ,c.TQQCBC1 
                ,c.TQQCBC 
                ,c.T2QRFHFBE1 
                ,c.TYYLBR1 
                ,c.TYYHOFHOPI1 
                ,c.TLOGLBR 
                ,c.TQQHOFHOPI 
                ,c.TYYLBR 
                ,c.TLBR1 
                ,c.FRFHFBE 
                ,c.TYYLBR1_ocltv_ge80 
                ,c.TYYLBR1_fico_le659

			from &dsn_lgd. as a
				left join &dsn_heloc_pd. as b
				on a.qtime = b.qtime
				and a.panel = b.panel
				and a.scenario = b.scenario
                and a.existing_flag = b.existing_flag                
				left join &dsn_mortgage_pd. as c 
				on a.qtime = c.qtime
				and a.panel = c.panel
				and a.scenario = c.scenario
			order by pd_group,scenario,qtime,panel
		;quit;

/*Checking balance for PD_group (portfolio) level*/
	proc sql;
		create table balance_chk as 
		select scenario,
			pd_group
			,qtime
			,sum(loan_curr_upb_amt) as sum_bal
			,sum(Pred_D180*loan_curr_upb_amt)/sum(loan_curr_upb_amt) as PD
/*			,avg(Pred_D180) as PD*/
			,avg(lgd) as LGD
			,calculated PD* calculated LGD as EL
/*			,avg(GCO) as avg_GCO*/
		from &dsnout.
		(where=(existing_flag=1 and qtime>&qtime_start.))
		group by scenario,pd_group,qtime
		;
	quit;

%mend m_merge_score_results;



;
/*-----------------------------------------------------------------------*/
;
/*-----------------------------------------------------------------------*/
 * Macros for eod overlay claculation;
/*-----------------------------------------------------------------------*/
;
%macro m_eod_overlay_calculation(dsnin=, dsnout1=, dsnout2=);
	%let qtime_start = %sysfunc(intck(qtr,"01Jan1960"d,%sysfunc(inputn(&month.01&year.,mmddyy10.)))); 
/*	Note: The EOD overlay calculation is applicable to HELOC only*/
	proc sql;
		create table EOD_1 as 
		select *
		,sum(intck("qtr","01JAN1960"d,eod_date), -1* qtime) as  Qtr_To_EoD
		,case when credit_rescore =. or credit_rescore lt 725 then 1 else 2 end as FICO_bin
		,case when calculated_cltv = . or calculated_cltv lt 0.8 then 1 else 2 end as CLTV_bin
		,case when calculated FICO_bin = 1 and calculated CLTV_bin = 2 then 1
			when calculated FICO_bin = 1 and calculated CLTV_bin = 1 then 2
			when calculated FICO_bin = 2 and calculated CLTV_bin = 2 then 2
			else 3 
		end as Risk_Seg
        ,utilization_rate*loan_orig_exposure_amt as balance
		from &dsnin.
		where qtime=&qtime_start. and  1 le calculated Qtr_To_EoD le &number_of_qtr_to_forecast. and existing_flag = 1
	;quit;

	proc sql;
		create table EOD_2 as 
			select a.*
				, b.Risk_Seg
				, b.FICO_bin
				, b.CLTV_bin
                ,b.balance
				,a.qtime - intck("qtr","01JAN1960"d,a.eod_date) as Qtr_To_EoD_2
				,case when calculated Qtr_To_EoD_2 >= -1 and b.Risk_Seg = 1 then 0.022
					when calculated Qtr_To_EoD_2 >= -1 and b.Risk_Seg = 2 then 0.0075
					when calculated Qtr_To_EoD_2 >= -1 and b.Risk_Seg = 3 then 0.0015
					else 0 
				 end as Adj_Factor
				,sum(a.Pred_D180,calculated Adj_Factor) as Adj_Pred_D180
				,case when calculated Qtr_To_EoD_2 ge -1 and Risk_Seg in (1,2) then 1 else a.utilization_rate end as Adj_Utilization
				,case when calculated Qtr_To_EoD_2 ge -1 and Risk_Seg in (1,2) then a.loan_curr_exposure_amt else a.loan_curr_upb_amt end as Adj_Balance
				,a.qtime + 1 as qtime_1
			from &dsnin.(where=(existing_flag = 1)) as a 
				left join EOD_1 as b
					on a.original_loan_fk = b.original_loan_fk
					and a.scenario = b.scenario
	;quit;



	proc sql;
		create table EOD_3 as 
			select a.*
	 		,b.qtime_1 as qtime_1a
			,b.Adj_Balance as Adj_lag_qtrbal
		from EOD_2 a
			left join EOD_2 b
			on a.original_loan_fk = b.original_loan_fk
			and a.scenario = b.scenario
			and a.qtime=b.qtime_1
	;quit;

	proc sql;
		create table &dsnout1.  as 
		select scenario
			,Period 
			,qtime
            ,sum(loan_curr_upb_amt*Pred_D90)/sum(loan_curr_upb_amt)  as D90_rate
            ,sum(loan_curr_upb_amt*Adj_Pred_D180)/sum(loan_curr_upb_amt)  as PD_rate
			,sum(loan_curr_upb_amt*Adj_Pred_D180*LGD)/sum(loan_curr_upb_amt*Adj_Pred_D180 )  as LGD_rate
			,sum(loan_curr_upb_amt*Adj_Pred_D180*LGD)/sum(loan_curr_upb_amt)  as GCO_rate
			,sum(loan_curr_upb_amt*pred_pp)/sum(loan_curr_upb_amt )  as prepay_rate 
			,sum(loan_curr_upb_amt) as sum_balance
			,sum(loan_curr_upb_amt) as sum_Adj_lag_qtrbal
			,sum(balance) as Util_lag_qtrbal
		from eod_3
		where &qtime_start. lt qtime le %eval(&qtime_start.+&number_of_qtr_to_forecast.)
		and upcase(pd_group)="HELOC" and existing_flag=1
		group by scenario,Period,qtime 
	;quit;



	proc sql;
		create table &dsnout2. as 
			select loan_lien_position
			,scenario
			,Period 
			,qtime
			,sum(Adj_lag_qtrbal*Adj_Pred_D180)/sum(Adj_lag_qtrbal)  as PD_rate
			,sum(Adj_lag_qtrbal*Adj_Pred_D180*LGD)/sum(Adj_lag_qtrbal*Adj_Pred_D180 )  as LGD_rate
			,sum(Adj_lag_qtrbal*Adj_Pred_D180*LGD)/sum(Adj_lag_qtrbal)  as GCO_rate
			, sum(Adj_lag_qtrbal) as Adj_lag_qtrbal
		from eod_3
		where &qtime_start. lt qtime le %eval(&qtime_start.+&number_of_qtr_to_forecast.)
		and upcase(pd_group)="HELOC" and existing_flag=1
		group by scenario,Loan_lien_position,Period,qtime
	;quit;



%mend m_eod_overlay_calculation;
