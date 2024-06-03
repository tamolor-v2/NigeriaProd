startdate=$1
enddate=$2
spool=$3
rev_dir=/nas/share_05/scripts/revenue
flytxt_dir=/nas/share05/scripts/flytxt
script_dir=/nas/share05/scripts

#Validate if startdate is less or equal than end date
if [ $startdate -gt $enddate ]
then
echo "Startdate cannot be greater than enddate. First date parameter cannot be greater than second date parameter."
exit
fi

#cd $rev_dir
#bash run_rev.sh $startdate $enddate

cd $script_dir 
#bash run_this.sh $startdate $enddate borrow_amount flytxt
#bash run_this.sh $startdate $enddate dnd_msisdn_report flytxt
bash run_this.sh $startdate $enddate datapackbalance flytxt
#bash run_this.sh $startdate $enddate last_recharge_type flytxt
#bash run_this.sh $startdate $enddate last_recharge_types flytxt
#bash run_this.sh $startdate $enddate last_event flytxt
#bash run_this.sh $startdate $enddate vlr flytxt
#bash run_this.sh $startdate $enddate sms_roam flytxt
#bash run_this.sh $startdate $enddate not_charged flytxt
#bash run_this.sh $startdate $enddate not_charged_data flytxt
#bash run_this.sh $startdate $enddate not_charged_sms flytxt
#bash run_this.sh $startdate $enddate sit_payg_bundled_usage flytxt
bash run_this.sh $startdate $enddate cm_profile_bdail_momo_fee flytxt
bash run_this.sh $startdate $enddate cm_profile_bdail_events flytxt
bash run_this.sh $startdate $enddate cm_profile_bdail_recharge flytxt
bash run_this.sh $startdate $enddate cm_profile_bdail flytxt

cd $flytxt_dir 
#Run the flytxt SPOOL here
case $spool in
1)
#while [ $startdate -le $enddate ]
#do
#echo $startdate >> run_flytxt_spool.log
echo $enddate >> run_flytxt_spool.log
#bash run_flytxt_spool.sh $startdate >> run_flytxt_spool.log
bash run_flytxt_spool.sh $enddate > /dev/null 
#startdate=$( date -d"$startdate +1 day" +%Y%m%d )
#done
;;
*)
echo "No spool selected" >> run_flytxt_spool.log
;;
esac

exit
