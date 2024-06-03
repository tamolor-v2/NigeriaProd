# bash run_reprocess.sh --config ./reprocess.conf --feed CS5_AIR_REFILL_MA --date 20180205 2>&1 | tee CS5_AIR_REFILL_MA_20180205_1.log 

bash run_reprocess.sh --config ./reprocess.conf --feed SDP_DMP_MA --date 20180208
bash run_reprocess.sh --config ./reprocess.conf --feed MSC_CDR --date 20180206
bash run_reprocess.sh --config ./reprocess.conf --feed MSC_CDR --date 20180207
bash run_reprocess.sh --config ./reprocess.conf --feed MSC_CDR --date 20180208
bash run_reprocess.sh --config ./reprocess.conf --feed MSC_CDR --date 20180209
bash run_reprocess.sh --config ./reprocess.conf --feed MSC_CDR --date 20180210

bash run_reprocess.sh --config ./reprocess.conf --feed GGSN_CDR --date 20180205
bash run_reprocess.sh --config ./reprocess.conf --feed GGSN_CDR --date 20180206
bash run_reprocess.sh --config ./reprocess.conf --feed GGSN_CDR --date 20180208
bash run_reprocess.sh --config ./reprocess.conf --feed GGSN_CDR --date 20180209

bash run_reprocess.sh --config ./reprocess.conf --feed CS5_CCN_VOICE_MA --date 20180206
bash run_reprocess.sh --config ./reprocess.conf --feed CS5_CCN_VOICE_MA --date 20180207
bash run_reprocess.sh --config ./reprocess.conf --feed CS5_CCN_VOICE_MA --date 20180208
bash run_reprocess.sh --config ./reprocess.conf --feed CS5_CCN_VOICE_MA --date 20180209

bash run_reprocess.sh --config ./reprocess.conf --feed CS5_CCN_GPRS_MA --date 20180207
bash run_reprocess.sh --config ./reprocess.conf --feed CS5_CCN_GPRS_MA --date 20180208
bash run_reprocess.sh --config ./reprocess.conf --feed CS5_CCN_GPRS_MA --date 20180209
bash run_reprocess.sh --config ./reprocess.conf --feed CS5_CCN_GPRS_MA --date 20180210

bash run_reprocess.sh --config ./reprocess.conf --feed CS5_AIR_ADJ_MA --date 20180205



