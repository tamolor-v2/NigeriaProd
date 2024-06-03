#!/bin/sh
exec scala -cp /mnt/beegfs/Deployment/DEV/scripts/BslScript/presto-jdbc-0.191.jar -savecompiled "$0" "$@"
!#

import java.io._
import java.util.{ Calendar, TimeZone }
import scala.collection.mutable.ArrayBuffer
import java.util.concurrent.{ExecutorService, Executors, TimeUnit}

object RewriteCustomerSubject {
  val rewriteQuery: String =
    """
      #set hive.exec.dynamic.partition=true;
      #set hive.exec.dynamic.partition.mode=nonstrict;
      #set hive.execution.engine=mr;
      #set hive.auto.convert.join=false;
      #
      #truncate table flare_8.CustomerSubject_ValidMsisdnsTmp ;
      #
      #insert into flare_8.CustomerSubject_ValidMsisdnsTmp 
      #select coalesce(a.msisdn_key, b.msisdn_key) as msisdn_key, coalesce(b.is_in_today_sdp, false) as is_in_today_sdp,
      #coalesce(b.is_in_prevdays_sdp, false) as is_in_prevdays_sdp,
      #coalesce(a.is_valid_vas_subscriber, false) as is_valid_vas_subscriber,
      #coalesce(a.has_rev, false) as has_rev
      #from
      #(select msisdn_key, 
      #(vas_subscription_1 is not null and length(vas_subscription_1) > 0) as is_valid_vas_subscriber,
      #((coalesce(rev_vas1, 0.0) + coalesce(rev_voice_roam_outgoing, 0.0) + coalesce(customer_sys_01, 0.0) + coalesce(customer_sys_03, 0.0) + coalesce(tot_rev, 0.0) + COALESCE(rev_rentals, 0.0) + COALESCE(rev_data_rental, 0.0) ) > 0) has_rev
      #from flare_8.CustomerSubject where tbl_dt = ${DATA_DATE} and aggr = 'daily' and ((vas_subscription_1 is not null and length(vas_subscription_1) > 0) or
      #((coalesce(rev_vas1, 0.0) + coalesce(rev_voice_roam_outgoing, 0.0) + coalesce(customer_sys_01, 0.0) + coalesce(customer_sys_03, 0.0) + coalesce(tot_rev, 0.0) + COALESCE(rev_rentals, 0.0) + COALESCE(rev_data_rental, 0.0) ) > 0))) a
      #full outer join (select msisdn_key, (case when max(tbl_dt) = ${DATA_DATE} then true else false end) as is_in_today_sdp, (case when min(tbl_dt) < ${DATA_DATE} then true else false end) as is_in_prevdays_sdp
      #from flare_8.SDP_DMP_MA 
      #where tbl_dt between ${SDP_START_DATE} and ${DATA_DATE} 
      #group by msisdn_key) b
      #on (a.msisdn_key = b.msisdn_key)
      #;
      #
      #
      #insert overwrite table flare_8.CustomerSubject PARTITION (aggr,tbl_dt,snapshot,subsnapshot)
      #select 
      #cust.msisdn_key, cust.year, cust.month, cust.day, cust.date_key, cust.rec_create_date, cust.business_day, cust.country,
      #cust.opco_name, cust.opco_business_type, cust.uid, cust.subscriber_type, cust.customer_type, cust.b2b_type, cust.cons_type,
      #cust.tariff_type, cust.last_activity_date, cust.last_rge_date, cust.first_usage_date, cust.last_usage_date, cust.first_recharge_date,
      #cust.last_recharge_date, cust.activation_date, cust.churn_date, cust.channel_id, cust.status, cust.dola, cust.hs_make, cust.hs_model,
      #cust.tac, cust.imei, cust.imsi, cust.nw_3g_ind, cust.lte_4g_ind, cust.nw_2g_ind, cust.smartphone_ind, cust.edge_ind, cust.gprs_ind,
      #cust.bts_first_call_lat, cust.bts_first_call_lon, cust.bts_first_call_site_id, cust.bts_mu_lat, cust.bts_mu_lon, cust.bts_mu_site_id,
      #cust.bts_day_lat, cust.bts_day_lon, cust.bts_day_site_id, cust.bts_night_lat, cust.bts_night_lon, cust.bts_night_site_id, cust.loyalty_id,
      #cust.loyalty_points_earned, cust.loyalty_points_redeemed, cust.loyalty_points_balance, cust.rev_voice_onnet, cust.rev_voice_offnet,
      #cust.rev_voice_int, cust.rev_voice_roam_outgoing, cust.rev_voice_roam_incoming, cust.rev_sms_onnet, cust.rev_sms_offnet, cust.rev_sms_int,
      #cust.rev_vas1, cust.rev_vas2, cust.rev_vas3, cust.rev_vas4, cust.rev_other_vas, cust.rev_data_rental, cust.rev_data_payg, cust.rev_data_total,
      #cust.rev_rentals, cust.rev_other, cust.tot_rev_in, cust.tot_rev_out, cust.tot_rev, cust.voi_onnet_in_secs, cust.voi_onnet_in_counter,
      #cust.voi_onnet_out_b_secs, cust.voi_onnet_out_b_counter, cust.voi_onnet_out_nb_secs, cust.voi_onnet_out_nb_counter, cust.voi_onnet_out_secs_free,
      #cust.voi_onnet_out_counter_free, cust.voi_cc_out_secs, cust.voi_cc_out_counter, cust.voi_offnet_in_secs, cust.voi_offnet_in_counter,
      #cust.voi_offnet_out_b_secs, cust.voi_offnet_out_b_counter, cust.voi_offnet_out_nb_secs, cust.voi_offnet_out_nb_counter, cust.voi_offnet_out_secs_free,
      #cust.voi_offnet_out_counter_free, cust.voi_int_in_secs, cust.voi_int_in_counter, cust.voi_int_out_b_secs, cust.voi_int_out_b_counter,
      #cust.voi_int_out_nb_secs, cust.voi_int_out_nb_counter, cust.int_top_incoming_destination, cust.int_top_outgoing_destination, cust.int_moc_countries,
      #cust.int_top_moc_country_calls, cust.int_top_moc_country_secs, cust.int_mtc_countries, cust.int_top_mtc_country_calls, cust.int_top_mtc_country_secs,
      #cust.voi_int_out_secs_free, cust.voi_int_out_counter_free, cust.voi_roam_moc_secs, cust.voi_roam_moc_counter, cust.voi_roam_moc_free_secs,
      #cust.voi_roam_moc_free_counter, cust.voi_roam_mtc_secs, cust.voi_roam_mtc_counter, cust.voi_roam_mtc_free_secs, cust.voi_roam_mtc_free_counter,
      #cust.sms_in_count, cust.sms_onnet_out_b_count, cust.sms_onnet_out_nb_count, cust.sms_onnet_out_count_free, cust.sms_offnet_out_b_count,
      #cust.sms_offnet_out_nb_count, cust.sms_offnet_out_count_free, cust.sms_int_out_b_count, cust.sms_int_out_nb_count, cust.sms_int_out_count_free,
      #cust.vas_subscription_1, cust.vas_subscription_2, cust.vas_subscription_3, cust.vas_subscription_4, cust.vas_subscription_other, cust.momo_bal,
      #cust.momo_dep_cnt, cust.momo_dep_amt, cust.momo_wit_cnt, cust.momo_wit_amt, cust.momo_p2p_trx_cnt, cust.momo_p2p_trx_amt, cust.data_kb, cust.data_dl_kb,
      #cust.data_up_kb, cust.data_kb_2g, cust.data_kb_3g, cust.data_kb_4g, cust.data_in_bundle_kb, cust.data_out_bundle_kb, cust.data_free_kb, cust.data_session_cnt,
      #cust.data_session_cnt_2g, cust.data_session_cnt_3g, cust.data_session_cnt_4g, cust.data_session_drtn_secs, cust.data_session_drtn_secs_2g, cust.data_session_drtn_secs_3g,
      #cust.data_session_drtn_secs_4g, cust.data_roam_drtn_secs, cust.data_roam_session_cnt, cust.data_roam_kb, cust.active_data, cust.rch_count_digital, cust.rch_digital_rev,
      #cust.rch_count_voucher, cust.rch_voucher_rev, cust.rch_last_date, cust.rch_count_d1, cust.rch_value_d1, cust.rch_count_d2, cust.rch_value_d2, cust.rch_count_d3, cust.rch_value_d3,
      #cust.rch_count_d4, cust.rch_value_d4, cust.rch_count_d5, cust.rch_value_d5, cust.rch_count_d6, cust.rch_value_d6, cust.rch_count_d7, cust.rch_value_d7, cust.rch_count_d8,
      #cust.rch_value_d8, cust.rch_count_d9, cust.rch_value_d9, cust.rch_count_d10, cust.rch_value_d10, cust.rch_denom_others_cnt, cust.rch_denom_others_rev, cust.balance_transfer_in_cnt,
      #cust.balance_transfer_out_cnt, cust.balance_transfer_in_rev, cust.balance_transfer_out_rev, cust.act_days_onnet_moc, cust.act_days_offnet_moc, cust.act_days_intl_moc,
      #cust.act_days_all_moc, cust.act_days_intl_mtc, cust.act_days_onnet_mtc, cust.act_days_offnet_mtc, cust.act_days_all_mtc, cust.act_days_sms_intl_moc, cust.act_days_sms_onnet_moc,
      #cust.act_days_sms_offnet_moc, cust.act_days_data, cust.act_days_moc, cust.act_days_mtc, cust.act_days_rge, cust.hr1_secs_onnet_lcl, cust.hr2_secs_onnet_lcl, cust.hr3_secs_onnet_lcl,
      #cust.hr4_secs_onnet_lcl, cust.hr5_secs_onnet_lcl, cust.hr6_secs_onnet_lcl, cust.hr7_secs_onnet_lcl, cust.hr8_secs_onnet_lcl, cust.hr9_secs_onnet_lcl, cust.hr10_secs_onnet_lcl,
      #cust.hr11_secs_onnet_lcl, cust.hr12_secs_onnet_lcl, cust.hr13_secs_onnet_lcl, cust.hr14_secs_onnet_lcl, cust.hr15_secs_onnet_lcl, cust.hr16_secs_onnet_lcl, cust.hr17_secs_onnet_lcl,
      #cust.hr18_secs_onnet_lcl, cust.hr19_secs_onnet_lcl, cust.hr20_secs_onnet_lcl, cust.hr21_secs_onnet_lcl, cust.hr22_secs_onnet_lcl, cust.hr23_secs_onnet_lcl, cust.hr0_secs_onnet_lcl,
      #cust.hr1_secs_offnet_lcl, cust.hr2_secs_offnet_lcl, cust.hr3_secs_offnet_lcl, cust.hr4_secs_offnet_lcl, cust.hr5_secs_offnet_lcl, cust.hr6_secs_offnet_lcl, cust.hr7_secs_offnet_lcl,
      #cust.hr8_secs_offnet_lcl, cust.hr9_secs_offnet_lcl, cust.hr10_secs_offnet_lcl, cust.hr11_secs_offnet_lcl, cust.hr12_secs_offnet_lcl, cust.hr13_secs_offnet_lcl, cust.hr14_secs_offnet_lcl,
      #cust.hr15_secs_offnet_lcl, cust.hr16_secs_offnet_lcl, cust.hr17_secs_offnet_lcl, cust.hr18_secs_offnet_lcl, cust.hr19_secs_offnet_lcl, cust.hr20_secs_offnet_lcl, cust.hr21_secs_offnet_lcl,
      #cust.hr22_secs_offnet_lcl, cust.hr23_secs_offnet_lcl, cust.hr0_secs_offnet_lcl, cust.hr1_kb, cust.hr2_kb, cust.hr3_kb, cust.hr4_kb, cust.hr5_kb, cust.hr6_kb, cust.hr7_kb, cust.hr8_kb,
      #cust.hr9_kb, cust.hr10_kb, cust.hr11_kb, cust.hr12_kb, cust.hr13_kb, cust.hr14_kb, cust.hr15_kb, cust.hr16_kb, cust.hr17_kb, cust.hr18_kb, cust.hr19_kb, cust.hr20_kb, cust.hr21_kb,
      #cust.hr22_kb, cust.hr23_kb, cust.hr0_kb, cust.hr1_secs_intl, cust.hr2_secs_intl, cust.hr3_secs_intl, cust.hr4_secs_intl, cust.hr5_secs_intl, cust.hr6_secs_intl, cust.hr7_secs_intl,
      #cust.hr8_secs_intl, cust.hr9_secs_intl, cust.hr10_secs_intl, cust.hr11_secs_intl, cust.hr12_secs_intl, cust.hr13_secs_intl, cust.hr14_secs_intl, cust.hr15_secs_intl, 
      #cust.hr16_secs_intl, cust.hr17_secs_intl, cust.hr18_secs_intl, cust.hr19_secs_intl, cust.hr20_secs_intl, cust.hr21_secs_intl, cust.hr22_secs_intl, cust.hr23_secs_intl, 
      #cust.hr0_secs_intl, cust.mon_secs_onnet, cust.tue_secs_onnet, cust.wed_secs_onnet, cust.thu_secs_onnet, cust.fri_secs_onnet, cust.sat_secs_onnet, cust.sun_secs_onnet,
      #cust.mon_secs_offnet, cust.tue_secs_offnet, cust.wed_secs_offnet, cust.thu_secs_offnet, cust.fri_secs_offnet, cust.sat_secs_offnet, cust.sun_secs_offnet, cust.mon_secs_intl,
      #cust.tue_secs_intl, cust.wed_secs_intl, cust.thu_secs_intl, cust.fri_secs_intl, cust.sat_secs_intl, cust.sun_secs_intl, cust.mon_kb, cust.tue_kb, cust.wed_kb, cust.thu_kb,
      #cust.fri_kb, cust.sat_kb, cust.sun_kb, cust.mon_bal, cust.tue_bal, cust.wed_bal, cust.thu_bal, cust.fri_bal, cust.sat_bal, cust.sun_bal, cust.mon_rev, cust.tue_rev, cust.wed_rev,
      #cust.thu_rev, cust.fri_rev, cust.sat_rev, cust.sun_rev, cust.mon_rchg, cust.tue_rchg, cust.wed_rchg, cust.thu_rchg, cust.fri_rchg, cust.sat_rchg, cust.sun_rchg, cust.sn_total_moc,
      #cust.sn_total_mtc, cust.sn_on_net_outgoing, cust.sn_on_net_incoming, cust.sn_top_onnet_num1, cust.sn_top_onnet_num1_og_calls, cust.sn_top_onnet_num1_og_secs, cust.sn_top_onnet_num2,
      #cust.sn_top_onnet_num2_og_calls, cust.sn_top_onnet_num2_og_secs, cust.sn_xnet_outgoing, cust.sn_xnet_incoming, cust.sn_top_xnet_moc_num1, cust.sn_top_xnet_moc_num1_ic_calls,
      #cust.sn_top_xnet_moc_num1_ic_secs, cust.sn_top_xnet_moc_num1_og_calls, cust.sn_top_xnet_moc_num1_og_secs, cust.sn_top_xnet_moc_num2, cust.sn_top_xnet_moc_num2_ic_calls,
      #cust.sn_top_xnet_moc_num2_ic_secs, cust.sn_top_xnet_moc_num2_og_calls, cust.sn_top_xnet_moc_num2_og_secs, cust.sn_top_xnet_mtc_num1, cust.sn_top_xnet_mtc_num1_ic_calls,
      #cust.sn_top_xnet_mtc_num1_ic_secs, cust.sn_top_xnet_mtc_num1_og_calls, cust.sn_top_xnet_mtc_num1_og_secs, cust.sn_top_xnet_mtc_num2, cust.sn_top_xnet_mtc_num2_ic_calls,
      #cust.sn_top_xnet_mtc_num2_ic_secs, cust.sn_top_xnet_mtc_num2_og_calls, cust.sn_top_xnet_mtc_num2_og_secs, cust.sn_intl_moc, cust.sn_intl_mtc, cust.sn_top_intl_moc_num1,
      #cust.sn_top_intl_num1_og_calls, cust.sn_top_intl_num1_og_secs, cust.sn_top_intl_moc_num2, cust.sn_top_intl_num2_og_calls, cust.sn_top_intl_num2_og_secs,
      #cust.voi_offnet_moc_op1_drtn_sec, cust.voi_offnet_moc_op1_counter, cust.voi_offnet_mtc_op1_drtn_sec, cust.voi_offnet_mtc_op1_counter, cust.voi_offnet_moc_op2_drtn_sec,
      #cust.voi_offnet_moc_op2_counter, cust.voi_offnet_mtc_op2_drtn_sec, cust.voi_offnet_mtc_op2_counter, cust.voi_offnet_moc_op3_drtn_sec, cust.voi_offnet_moc_op3_counter,
      #cust.voi_offnet_mtc_op3_drtn_sec, cust.voi_offnet_mtc_op3_counter, cust.voi_offnet_moc_op4_drtn_sec, cust.voi_offnet_moc_op4_counter, cust.voi_offnet_mtc_op4_drtn_sec,
      #cust.voi_offnet_mtc_op4_counter, cust.bal_avg_daily, cust.bal_days_less_5, cust.bal_days_negative, cust.bal_times_less_5, cust.bal_times_negative, cust.bal_last_day_period,
      #cust.val_seg, cust.intc_voi_mtc_int_count, cust.intc_voi_mtc_int_secs, cust.intc_voi_mtc_offnet_count, cust.intc_voi_mtc_offnet_secs, cust.intc_voi_moc_count, cust.intc_voi_moc_secs,
      #cust.intc_sms_mtc_int_count, cust.intc_sms_mtc_offnet_count, cust.intc_sms_moc_count, cust.sys_lsu_num, cust.sys_node_id, cust.sys_sequence_num, cust.sys_timestamp, 
      #cust.datausage30days, cust.datausage30dayskb, cust.momo30days, cust.account_code_n, cust.sn_top_xnet_moc_num1_op_name, cust.sn_top_xnet_moc_num2_op_name,
      #cust.sn_top_xnet_mtc_num1_op_name, cust.sn_top_xnet_mtc_num2_op_name, cust.sn_top_intl_moc_num1_dest, cust.sn_top_intl_moc_num2_dest, cust.voi_offnet_moc_op1_name,cust.voi_offnet_mtc_op1_name,
      #cust.voi_offnet_moc_op2_name, cust.voi_offnet_mtc_op2_name, cust.voi_offnet_moc_op3_name, cust.voi_offnet_mtc_op3_name, cust.voi_offnet_moc_op4_name, cust.voi_offnet_mtc_op4_name,
      #cust.customer_sys_01, cust.customer_sys_02, cust.customer_sys_03, cust.customer_sys_04, coalesce(sdp.is_in_today_sdp, false) as is_in_today_sdp, 
      #coalesce(sdp.is_in_prevdays_sdp, false) as is_in_prevdays_sdp, coalesce(sdp.is_valid_vas_subscriber, false) as is_valid_vas_subscriber, coalesce(sdp.has_rev, false) as has_rev, 
      #cust.aggr, cust.tbl_dt, cust.snapshot, cust.subsnapshot
      #from flare_8.CustomerSubject cust
      #inner join flare_8.CustomerSubject_ValidMsisdnsTmp sdp
      #on (cust.tbl_dt = ${DATA_DATE} and cust.aggr = 'daily' and cust.msisdn_key = sdp.msisdn_key)
      #;
      #
      """.stripMargin('#')


  private def getGMTCalendarFromYYYYMMDD(ts: Int): Calendar = {
    var cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"))
    cal.setTimeInMillis(0)
    cal.set(ts/10000, ((ts%10000)/100) - 1, ts%100)
    cal
  }

  private def dateAdd_YYYYMMDD(dt: Int, dateOffset: Int): Int = {
    try {
      val cal = getGMTCalendarFromYYYYMMDD(dt)
      cal.add(Calendar.DATE, dateOffset);
      return (cal.get(Calendar.YEAR) * 10000 + (cal.get(Calendar.MONTH) + 1) * 100 + cal.get(Calendar.DAY_OF_MONTH))
    } catch {
      case e1: Throwable => {
        throw e1
      }
    }
  }

  private def replaceKeyValues(template: String, keyValue: Map[String, String]): String = {
    var _template = template

    keyValue.foreach(kv => {
      _template = _template.replace(kv._1, kv._2)
    })
    _template
  }
  
  private def addInputStreamThreadPool(pool: java.util.concurrent.ExecutorService, proc: Process, bufferReads: ArrayBuffer[BufferedReader], is: InputStream, sb: StringBuilder = null): Unit = {
    try {
      pool.execute(new Runnable() {
        val inputStream = is

        override def run(): Unit = {
          var bufferedReader: BufferedReader = null
          try {
            bufferedReader = new BufferedReader(new InputStreamReader(inputStream))
            bufferReads += bufferedReader
            var line: String = null
            line = bufferedReader.readLine
            while (!pool.isShutdown && !pool.isTerminated && proc.isAlive && line != null) {
              println(line)
              if (sb != null)
                sb.append(line).append("\n")
              line = bufferedReader.readLine
            }
          }
          catch {
            case e: Throwable =>
              e.printStackTrace()
          } finally {
            if (bufferedReader != null)
              bufferedReader.close()
          }
        }
      })
    } catch {
      case e: Throwable =>
        e.printStackTrace()
    }
  }

  private def runSysCmd(argsList: Seq[String], outputSb: StringBuilder = null, errorSb: StringBuilder = null, timeOut: Long = 120, maxRetry: Int = 1): Int = {
    var procRetCode = -1000
    val mRetry = if (maxRetry > 0) maxRetry else 1
    var retry = 0
    var isFinished = false
    var process: Process = null
    try {
      val pool: ExecutorService = Executors.newFixedThreadPool(2)
      val bufferReaders = ArrayBuffer[BufferedReader]()
      while (retry <= mRetry && !isFinished) {
        println("executing command: %s".format(argsList.mkString(" ")))
        try {
          val procBuilder = new java.lang.ProcessBuilder(argsList: _*)
          process = procBuilder.start()
          addInputStreamThreadPool(pool, process, bufferReaders, process.getErrorStream, errorSb)
          addInputStreamThreadPool(pool, process, bufferReaders, process.getInputStream, outputSb)
          isFinished = process.waitFor(timeOut, TimeUnit.MINUTES)
          procRetCode = process.exitValue()
        }
        catch {
          case e: Throwable =>
            try {
              retry = retry + 1
              val msg = "Timeout when executed command: %s .Timeout is %2d minutes. retry:%2d/%2d".format(argsList.mkString(" "), timeOut, retry, maxRetry)
              println(msg)
              e.printStackTrace()
              errorSb.append(e.getMessage)
              process.destroyForcibly()
            } catch {
              case ex: Throwable => ex.printStackTrace()
            }
        }
      }
      pool.shutdownNow()
      pool.awaitTermination(1, TimeUnit.HOURS)
      bufferReaders.foreach(br => {
        try {
          if (br != null)
            br.close()
        } catch {
          case e: Throwable => e.printStackTrace()
        }
      })
    } catch {
      case e: Throwable => e.printStackTrace()
    }
    procRetCode
  }

  private def runRewriteTbl(sdpStartDate: Int, qryDataDate: Int): Unit = {
    val errorStringBuilder = new StringBuilder
    val hiveCommandTemplate = "hive||-e||{HQL}"
    val hql = replaceKeyValues(rewriteQuery, Map("${SDP_START_DATE}" -> sdpStartDate.toString, "${DATA_DATE}" -> qryDataDate.toString))
    println("running query:\n %s \n\n".format(hql))
    val hiveCommand = hiveCommandTemplate.replace("{HQL}", hql)
    val seq = hiveCommand.split("\\|\\|")
    val returnCode = runSysCmd(seq, errorSb = errorStringBuilder)
    if (returnCode != 0) {
      println("Failed to execute query:" + hql + "\n" + errorStringBuilder.toString)
      throw new Exception(errorStringBuilder.toString)
    }
  }

def parseArgs = (args: Array[String]) => {
    if (args.length >= 2)
      args.sliding(2, 2).toList.collect {
        case Array("--stdate", argStartDate: String) => st_Date = argStartDate.trim
        case Array("--enddate", argEndDate: String) => end_Date = argEndDate.trim
      }
    else
      println("Please pass stdate and enddate ")
  }

var st_Date=""
var end_Date=""

  def main(args: Array[String]): Unit = {
println(args.toList)
parseArgs(args)
    val stDate = st_Date.toInt
    val endDate = end_Date.toInt
println("stDate: "+stDate)
println("endDate: "+endDate)
    val stepVal = 1
    val extraSdpPrevDays = 3
    var curDt = stDate
    while ((stepVal < 0 && curDt >= endDate) || (stepVal > 0 && curDt <= endDate)) {
      try {
        val qryStartDate = dateAdd_YYYYMMDD(curDt, extraSdpPrevDays * -1)
        runRewriteTbl(qryStartDate, curDt)
      } catch {
        case e: Throwable => {}
      }

      curDt = dateAdd_YYYYMMDD(curDt, stepVal)
    }
  }
}
