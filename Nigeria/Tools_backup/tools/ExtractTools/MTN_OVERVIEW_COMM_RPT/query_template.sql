SELECT TO_CHAR(TO_DATE(ORDER_DATE,'DD-MON-YY'),'yyyymmdd') REPORT_GENDATE_D,
     ROW_NO,
     TRANSLATE(TRANSLATE(TRANSLATE(ORDER_NO, CHR(10), ' '), CHR(13), ' '), CHR(09), ' ') ORDER_NO,
     TRANSLATE(TRANSLATE(TRANSLATE(SITE, CHR(10), ' '), CHR(13), ' '), CHR(09), ' ') SITE,
     TRANSLATE(TRANSLATE(TRANSLATE(LINE_NO, CHR(10), ' '), CHR(13), ' '), CHR(09), ' ') LINE_NO,
     TRANSLATE(TRANSLATE(TRANSLATE(RELEASE_NO, CHR(10), ' '), CHR(13), ' '), CHR(09), ' ') RELEASE_NO,
     TRANSLATE(TRANSLATE(TRANSLATE(SUPPLIER, CHR(10), ' '), CHR(13), ' '), CHR(09), ' ') SUPPLIER,
     TRANSLATE(TRANSLATE(TRANSLATE(SUPPLIER_NAME, CHR(10), ' '), CHR(13), ' '), CHR(09), ' ') SUPPLIER_NAME,
     TRANSLATE(TRANSLATE(TRANSLATE(STATUS, CHR(10), ' '), CHR(13), ' '), CHR(09), ' ') STATUS,
     TRANSLATE(TRANSLATE(TRANSLATE(ORDER_DATE, CHR(10), ' '), CHR(13), ' '), CHR(09), ' ') ORDER_DATE,
     TRANSLATE(TRANSLATE(TRANSLATE(PART_NO, CHR(10), ' '), CHR(13), ' '), CHR(09), ' ') PART_NO,
     TRANSLATE(TRANSLATE(TRANSLATE(DESCRIPTION, CHR(10), ' '), CHR(13), ' '), CHR(09), ' ') DESCRIPTION,
     TRANSLATE(TRANSLATE(TRANSLATE(TRANSLATE(PURCHASE_GRP, CHR(10), ' '), CHR(13), ' '), CHR(09), ' '),CHR(124), ' ') PURCHASE_GRP,
     QUANTITY,
     TRANSLATE(TRANSLATE(TRANSLATE(PURCH_UM, CHR(10), ' '), CHR(13), ' '), CHR(09), ' ') PURCH_UM,
     TRANSLATE(TRANSLATE(TRANSLATE(QTY_IN_INV_UM, CHR(10), ' '), CHR(13), ' '), CHR(09), ' ') QTY_IN_INV_UM,
     TRANSLATE(TRANSLATE(TRANSLATE(INV_UM, CHR(10), ' '), CHR(13), ' '), CHR(09), ' ') INV_UM,
     TRANSLATE(TRANSLATE(TRANSLATE(PRICE_CURR, CHR(10), ' '), CHR(13), ' '), CHR(09), ' ') PRICE_CURR,
     PRICE_BASE,
     TRANSLATE(TRANSLATE(TRANSLATE(CURRENCY, CHR(10), ' '), CHR(13), ' '), CHR(09), ' ') CURRENCY,
     DISCOUNT_PERCENT,
     ADDITIONAL_COST,
     TOTAL_COST_PRICE,
     TOTAL_BASE,
     QTY_REC,
     VALUE_REC,
     EXTCOMMITMENT,
     INVOICED_QUANTITY,
     INVOICED_PRICE_UNIT,
     INVOICED_DISCOUNT,
     TRANSLATE(TRANSLATE(TRANSLATE(RECEIPT_DATE, CHR(10), ' '), CHR(13), ' '), CHR(09), ' ') RECEIPT_DATE,
     TRANSLATE(TRANSLATE(TRANSLATE(DELIVERY_DATE, CHR(10), ' '), CHR(13), ' '), CHR(09), ' ') DELIVERY_DATE,
     TRANSLATE(TRANSLATE(TRANSLATE(WANTED_DELIVERY_DATE, CHR(10), ' '), CHR(13), ' '), CHR(09), ' ') WANTED_DELIVERY_DATE,
     TRANSLATE(TRANSLATE(TRANSLATE(PROMISED_DELIVERY_DATE, CHR(10), ' '), CHR(13), ' '), CHR(09), ' ') PROMISED_DELIVERY_DATE,
     TRANSLATE(TRANSLATE(TRANSLATE(REQUISITION_NO, CHR(10), ' '), CHR(13), ' '), CHR(09), ' ') REQUISITION_NO,
     TRANSLATE(TRANSLATE(TRANSLATE(REQUISITION_LINE, CHR(10), ' '), CHR(13), ' '), CHR(09), ' ') REQUISITION_LINE,
     TRANSLATE(TRANSLATE(TRANSLATE(REQUISITION_REL_LINE, CHR(10), ' '), CHR(13), ' '), CHR(09), ' ') REQUISITION_REL_LINE,
     TRANSLATE(TRANSLATE(TRANSLATE(REQUISITIONER, CHR(10), ' '), CHR(13), ' '), CHR(09), ' ') REQUISITIONER,
     TRANSLATE(TRANSLATE(TRANSLATE(DEPARTMENT, CHR(10), ' '), CHR(13), ' '), CHR(09), ' ') DEPARTMENT,
     TRANSLATE(TRANSLATE(TRANSLATE(ACCOUNT, CHR(10), ' '), CHR(13), ' '), CHR(09), ' ') ACCOUNT,
     TRANSLATE(TRANSLATE(TRANSLATE(CC_R, CHR(10), ' '), CHR(13), ' '), CHR(09), ' ') CC_R,
     TRANSLATE(TRANSLATE(TRANSLATE(PRICE_PLAN, CHR(10), ' '), CHR(13), ' '), CHR(09), ' ') PRICE_PLAN,
     TRANSLATE(TRANSLATE(TRANSLATE(SEGMENT, CHR(10), ' '), CHR(13), ' '), CHR(09), ' ') SEGMENT,
     TRANSLATE(TRANSLATE(TRANSLATE(FA, CHR(10), ' '), CHR(13), ' '), CHR(09), ' ') FA,
     TRANSLATE(TRANSLATE(TRANSLATE(PROJECT, CHR(10), ' '), CHR(13), ' '), CHR(09), ' ') PROJECT,
     TRANSLATE(TRANSLATE(TRANSLATE(SUBLEDGER, CHR(10), ' '), CHR(13), ' '), CHR(09), ' ') SUBLEDGER,
     TRANSLATE(TRANSLATE(TRANSLATE(ACC_SITE, CHR(10), ' '), CHR(13), ' '), CHR(09), ' ') ACC_SITE,
     TRANSLATE(TRANSLATE(TRANSLATE(CODE_J, CHR(10), ' '), CHR(13), ' '), CHR(09), ' ') CODE_J,
     TRANSLATE(TRANSLATE(TRANSLATE(REPORT_GENDATE, CHR(10), ' '), CHR(13), ' '), CHR(09), ' ') REPORT_GENDATE,
     TRANSLATE(TRANSLATE(TRANSLATE(ACTUAL_RECEIPT_DATE, CHR(10), ' '), CHR(13), ' '), CHR(09), ' ') ACTUAL_RECEIPT_DATE,
     TRANSLATE(TRANSLATE(TRANSLATE(RECEIPT_NO, CHR(10), ' '), CHR(13), ' '), CHR(09), ' ') RECEIPT_NO
 FROM IFSAPP.MTN_OVERVIEW_COMM_RPT T
