SELECT
    TO_CHAR(TO_DATE('${DATE}','dd_mm_yyyy')),
    TRANSLATE(TRANSLATE(TRANSLATE(SERIAL_NUMBER, CHR(10), ' '), CHR(13), ' '), CHR(09), ' ') SERIAL_NUMBER,
    TRANSLATE(TRANSLATE(TRANSLATE(LOG_PHY_IND, CHR(10), ' '), CHR(13), ' '), CHR(09), ' ') LOG_PHY_IND,
    TRANSLATE(TRANSLATE(TRANSLATE(OUTLET_CODE, CHR(10), ' '), CHR(13), ' '), CHR(09), ' ') OUTLET_CODE,
    TRANSLATE(TRANSLATE(TRANSLATE(ORDER_NUMBER, CHR(10), ' '), CHR(13), ' '), CHR(09), ' ') ORDER_NUMBER,
    TRANSLATE(TRANSLATE(TRANSLATE(DATE_ORDERED, CHR(10), ' '), CHR(13), ' '), CHR(09), ' ') DATE_ORDERED,
    CARD_VALUE
 FROM PREPAID.EDW_REPORT_${DATE}
