from datetime import datetime, timedelta

def get_date_range_by_offset(offset):
    today = datetime.today().replace(day=1)  # Start from the 1st of the current month
    start_date = (today - timedelta(days=30 * offset)).replace(day=1)
    next_month = (start_date + timedelta(days=31)).replace(day=1)
    end_date = (next_month - timedelta(days=1))
    return start_date.strftime('%d-%m-%Y'), end_date.strftime('%d-%m-%Y')

# Example usage for offsets 2, 3, and 4
for offset in range(2, 5):  # 1 = 2 months ago, 2 = 3 months ago, 3 = 4 months ago
    start_date, end_date = get_date_range_by_offset(offset)
    print(f"Offset {offset}: Start Date = {start_date}, End Date = {end_date}")



get_date_range_for_month




  SELECT 
    t.GLOB_ID,t.DATE_VYD_D,S1.DATE_DELQ,o.ACCOUNT,o.KOD_ACC,t.K_VID_CRED,
    MAX(S1.DATN) OVER (PARTITION BY T.GLOB_ID ORDER BY T.GLOB_ID) AS DATN,
    s1.PAYM_CUMULAT,S1.DELQ_CUMULAT
    FROM asbt.sp_dog_loans t 
    INNER JOIN asbt.sp_dog_loans_acc o
    on t.glob_id=o.glob_id
    and o.kod_acc IN(2,12) 
    LEFT JOIN  ( SELECT ACCOUNT,dati AS DATE_DELQ,(SUM(OBOR_C)over (partition by account order by dati))/100 as PAYM_CUMULAT,                     
                        dense_rank() over (partition by account,ID_MFO order by obor_d_y) as RANK,DATN,ID_MFO,
                        OBOR_D_Y/100 AS DELQ_CUMULAT
                FROM ASBT.SALDO WHERE OBOR_D_Y<>0)s1
        on o.account=s1.ACCOUNT  
        WHERE 1=1  
        AND t.DATE_VYD_D between '{start_date}' and '{end_date}'
        AND t.REC_ACTIV=0
        AND t.K_VID_CRED in ({product})      
        AND t.ID_MFO=S1.ID_MFO 