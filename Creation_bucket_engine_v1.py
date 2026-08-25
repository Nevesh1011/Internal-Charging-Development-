#!/usr/bin/env python3
import os, sys, time, uuid, calendar, logging, traceback
from datetime import datetime, timedelta
import psycopg
from psycopg.rows import dict_rowx
LOG_FILE=os.getenv('LOG_FILE','/opt/bucket_engine_v1/logs/bucket_engine_v1.log')

os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
logger=logging.getLogger('bucket_engine_v1'); logger.setLevel(logging.INFO); logger.handlers.clear()
fmt=logging.Formatter('%(asctime)s | %(levelname)s | %(message)s','%Y-%m-%d %H:%M:%S')
fh=logging.FileHandler(LOG_FILE); fh.setFormatter(fmt); logger.addHandler(fh)
sh=logging.StreamHandler(sys.stdout); sh.setFormatter(fmt); logger.addHandler(sh)

class BucketProcessingError(Exception):
    def __init__(self, code, message):
        super().__init__(message); self.code=code; self.message=message

def get_connection():
    if not DB_NAME or not DB_USER:
        raise RuntimeError('DB_NAME and DB_USER must be set')
    return psycopg.connect(host=DB_HOST,port=DB_PORT,dbname=DB_NAME,user=DB_USER,password=DB_PASSWORD,row_factory=dict_row)

REQUIRED_TABLES=['bucket_request','customer','pop_usage_spec_group','usg_bucket_grp','po_bucket_grp','bucket_validity_period','validity_unit','bucket_triggers','customer_bucket']
REQUIRED_COLUMNS={
'bucket_request':['request_id','customer_ban','subscr_id','product_offering_id','pop_id','order_id','bucket_trigger_type','process_status','process_attempt_count','customer_bucket_id','error_code','error_message','processed_dt','updated_dt'],
'customer':['customer_ban','subscr_id'],
'pop_usage_spec_group':['pop_id','usage_specification_id'],
'usg_bucket_grp':['usage_specification_id','bucket_type_group_id'],
'po_bucket_grp':['bucket_type_group_id','bucket_usage_type','bucket_unit','bucket_initial_value','validity_period_id'],
'bucket_validity_period':['validity_period_id','validity_period_name','validity_duration','validity_unit','timeband_start','timeband_end','status','valid_from','valid_to'],
'validity_unit':['validity_unit_id','validity_unit_name'],
'bucket_triggers':['bucket_trigger_type','bucket_type_group_id'],
'customer_bucket':['bucket_id','customer_ban','status','bucket_usage_type','bucket_validity_period','bucket_usage_group','bucket_trigger_type','order_id','product_offering_id','pop_id','timeband_start','timeband_end','valid_start_time','valid_end_time','created_at','last_updated_at','bucket_unit','bucket_initial_value','bucket_remaining_value']}

def validate_database_structure():
    logger.info('STARTUP | validating DB connection/schema')
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute('SELECT current_database() db, current_user usr')
            x=cur.fetchone(); logger.info('STARTUP | DB SUCCESS | database=%s user=%s',x['db'],x['usr'])
            mt=[]; mc=[]
            for t in REQUIRED_TABLES:
                cur.execute("SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema=%s AND table_name=%s) e",(DB_SCHEMA,t))
                if not cur.fetchone()['e']:
                    logger.error('STARTUP | MISSING TABLE | %s.%s',DB_SCHEMA,t); mt.append(t); continue
                logger.info('STARTUP | TABLE OK | %s.%s',DB_SCHEMA,t)
                for c in REQUIRED_COLUMNS[t]:
                    cur.execute("SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema=%s AND table_name=%s AND column_name=%s) e",(DB_SCHEMA,t,c))
                    if not cur.fetchone()['e']:
                        logger.error('STARTUP | MISSING COLUMN | %s.%s.%s',DB_SCHEMA,t,c); mc.append(f'{t}.{c}')
            if mt or mc: raise RuntimeError(f'DB structure invalid; missing_tables={mt}; missing_columns={mc}')
    logger.info('STARTUP | schema validation SUCCESS')

def pick_pending_requests(conn):
    with conn.cursor() as cur:
        logger.info('POLL | searching PENDING requests | batch=%s',BATCH_SIZE)
        cur.execute(f'''SELECT request_id,customer_ban,subscr_id,product_offering_id,pop_id,order_id,bucket_trigger_type FROM {DB_SCHEMA}.bucket_request WHERE process_status='PENDING' ORDER BY request_id FOR UPDATE SKIP LOCKED LIMIT %s''',(BATCH_SIZE,))
        rows=cur.fetchall()
        for r in rows:
            cur.execute(f'''UPDATE {DB_SCHEMA}.bucket_request SET process_status='PROCESSING',process_attempt_count=process_attempt_count+1,updated_dt=CURRENT_TIMESTAMP WHERE request_id=%s''',(r['request_id'],))
            logger.info('REQUEST | request_id=%s | PENDING -> PROCESSING',r['request_id'])
        return rows

def validate_customer(conn,r):
    if not rows: raise BucketProcessingError('BUCKET_GROUP_NOT_FOUND',f'No bucket group for usage spec {u}')
    groups=[x['bucket_type_group_id'] for x in rows]; logger.info('REQUEST | %s | STEP3 SUCCESS | groups=%s',r['request_id'],groups); return groups

def get_bucket_def(conn,r,g):
    logger.info('REQUEST | %s | STEP4 PO_BUCKET_GRP lookup group=%s',r['request_id'],g)
    with conn.cursor() as cur:
        cur.execute(f'''SELECT product_characteristic_value_id,product_characteristic_value,bucket_usage_type,bucket_type_group_id,bucket_unit,bucket_initial_value,validity_period_id FROM {DB_SCHEMA}.po_bucket_grp WHERE bucket_type_group_id=%s''',(g,)); rows=cur.fetchall()
    if not rows: raise BucketProcessingError('BUCKET_DEFINITION_NOT_FOUND',f'No PO_BUCKET_GRP for group {g}')
    if len(rows)>1: raise BucketProcessingError('MULTIPLE_BUCKET_DEFINITION_FOUND',f'Multiple PO_BUCKET_GRP rows for group {g}')
    b=rows[0]; missing=[k for k in ['bucket_usage_type','bucket_unit','bucket_initial_value','validity_period_id'] if b.get(k) is None]
    if missing: raise BucketProcessingError('BUCKET_CONFIG_INCOMPLETE',f'Group {g} missing {missing}')
    logger.info('REQUEST | %s | STEP4 SUCCESS | type=%s unit=%s initial=%s validity_id=%s',r['request_id'],b['bucket_usage_type'],b['bucket_unit'],b['bucket_initial_value'],b['validity_period_id']); return b

def get_validity(conn,r,vid):
    logger.info('REQUEST | %s | STEP5 BUCKET_VALIDITY_PERIOD lookup id=%s',r['request_id'],vid)
    with conn.cursor() as cur:
        cur.execute(f'''SELECT bvp.validity_period_id,bvp.validity_period_name,bvp.validity_duration,vu.validity_unit_name,bvp.timeband_start,bvp.timeband_end FROM {DB_SCHEMA}.bucket_validity_period bvp JOIN {DB_SCHEMA}.validity_unit vu ON vu.validity_unit_id=bvp.validity_unit WHERE bvp.validity_period_id=%s AND bvp.status=1 AND CURRENT_TIMESTAMP>=bvp.valid_from AND (bvp.valid_to IS NULL OR CURRENT_TIMESTAMP<bvp.valid_to)''',(vid,)); v=cur.fetchone()
    if not v: raise BucketProcessingError('VALIDITY_PERIOD_NOT_FOUND',f'Validity period {vid} missing/inactive/outside validity dates')
    logger.info('REQUEST | %s | STEP5 SUCCESS | %s %s %s',r['request_id'],v['validity_period_name'],v['validity_duration'],v['validity_unit_name']); return v

def validate_trigger(conn,r,g):
    logger.info('REQUEST | %s | STEP6 validate trigger=%s group=%s',r['request_id'],r['bucket_trigger_type'],g)
    with conn.cursor() as cur:
        cur.execute(f'''SELECT 1 FROM {DB_SCHEMA}.bucket_triggers WHERE LOWER(TRIM(bucket_trigger_type))=LOWER(TRIM(%s)) AND bucket_type_group_id=%s LIMIT 1''',(r['bucket_trigger_type'],g))
        if not cur.fetchone(): raise BucketProcessingError('INVALID_TRIGGER',f"Trigger {r['bucket_trigger_type']} not mapped to group {g}")
    logger.info('REQUEST | %s | STEP6 SUCCESS',r['request_id'])

def find_existing(conn,r,g):
    logger.info('REQUEST | %s | STEP7 duplicate check',r['request_id'])
    with conn.cursor() as cur:
        cur.execute(f'''SELECT bucket_id FROM {DB_SCHEMA}.customer_bucket WHERE customer_ban=%s AND product_offering_id=%s AND order_id=%s AND bucket_usage_group=%s LIMIT 1''',(r['customer_ban'],r['product_offering_id'],r['order_id'],str(g))); x=cur.fetchone()
    if x: logger.warning('REQUEST | %s | STEP7 DUPLICATE prevented | bucket_id=%s',r['request_id'],x['bucket_id'])
    else: logger.info('REQUEST | %s | STEP7 SUCCESS | no duplicate',r['request_id'])
    return x

def add_months(dt,m):
    idx=dt.month-1+m; y=dt.year+idx//12; mo=idx%12+1; d=min(dt.day,calendar.monthrange(y,mo)[1]); return dt.replace(year=y,month=mo,day=d)

def add_years(dt,y):
    ny=dt.year+y; d=28 if dt.month==2 and dt.day==29 and not calendar.isleap(ny) else dt.day; return dt.replace(year=ny,day=d)

def calc_end(start,duration,unit):
    duration=int(duration); u=str(unit).strip().upper()
    if u in ('MINUTE','MINUTES'): return start+timedelta(minutes=duration)
    if u in ('HOUR','HOURS'): return start+timedelta(hours=duration)
    if u in ('DAY','DAYS'): return start+timedelta(days=duration)
    if u in ('MONTH','MONTHS'): return add_months(start,duration)
    if u in ('YEAR','YEARS'): return add_years(start,duration)
    raise BucketProcessingError('UNSUPPORTED_VALIDITY_UNIT',f'Unsupported unit {u}')

def insert_bucket(conn,r,g,b,v):
    logger.info('REQUEST | %s | STEP8 prepare CUSTOMER_BUCKET',r['request_id'])
    bid=str(uuid.uuid4()); start=datetime.now(); end=calc_end(start,v['validity_duration'],v['validity_unit_name']); initial=b['bucket_initial_value']
    logger.info('REQUEST | %s | STEP8 generated bucket_id=%s start=%s end=%s initial=%s remaining=%s',r['request_id'],bid,start,end,initial,initial)
    with conn.cursor() as cur:
        cur.execute(f'''INSERT INTO {DB_SCHEMA}.customer_bucket(bucket_id,customer_ban,status,bucket_usage_type,bucket_validity_period,bucket_usage_group,bucket_trigger_type,order_id,product_offering_id,pop_id,timeband_start,timeband_end,valid_start_time,valid_end_time,created_at,last_updated_at,bucket_unit,bucket_initial_value,bucket_remaining_value) VALUES(%s,%s,'ACTIVE',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,%s,%s,%s)''',(bid,r['customer_ban'],b['bucket_usage_type'],v['validity_period_id'],str(g),r['bucket_trigger_type'],r['order_id'],r['product_offering_id'],str(r['pop_id']),v['timeband_start'],v['timeband_end'],start,end,b['bucket_unit'],initial,initial))
    logger.info('REQUEST | %s | STEP8 SUCCESS | inserted CUSTOMER_BUCKET bucket_id=%s',r['request_id'],bid); return bid

def mark_success(conn,rid,bids):
    first=bids[0] if bids else None; details='Created bucket IDs: '+','.join(bids) if len(bids)>1 else None
    with conn.cursor() as cur:
        cur.execute(f'''UPDATE {DB_SCHEMA}.bucket_request SET process_status='SUCCESS',customer_bucket_id=%s,processed_dt=CURRENT_TIMESTAMP,updated_dt=CURRENT_TIMESTAMP,error_code=NULL,error_message=%s WHERE request_id=%s''',(first,details,rid))
    logger.info('REQUEST | %s | FINAL SUCCESS | bucket_ids=%s',rid,bids)

def mark_failed(conn,rid,code,msg):
    with conn.cursor() as cur:
        cur.execute(f'''UPDATE {DB_SCHEMA}.bucket_request SET process_status='FAILED',processed_dt=CURRENT_TIMESTAMP,updated_dt=CURRENT_TIMESTAMP,error_code=%s,error_message=%s WHERE request_id=%s''',(code,msg[:1000],rid))
    logger.error('REQUEST | %s | FINAL FAILED | code=%s | message=%s',rid,code,msg)

def process_request(conn,r):
    logger.info('='*100); logger.info('REQUEST | %s | START | BAN=%s SUBSCR=%s PRODUCT=%s POP=%s ORDER=%s TRIGGER=%s',r['request_id'],r['customer_ban'],r['subscr_id'],r['product_offering_id'],r['pop_id'],r['order_id'],r['bucket_trigger_type'])
    validate_customer(conn,r); u=get_usage_spec(conn,r); groups=get_groups(conn,r,u); bids=[]
    for g in groups:
        b=get_bucket_def(conn,r,g); v=get_validity(conn,r,b['validity_period_id']); validate_trigger(conn,r,g); x=find_existing(conn,r,g)
        bids.append(x['bucket_id'] if x else insert_bucket(conn,r,g,b,v))
    if not bids: raise BucketProcessingError('NO_BUCKET_CREATED','No customer bucket created/found')
    return bids

def process_batch():
    with get_connection() as conn:
        reqs=pick_pending_requests(conn); conn.commit()
    if not reqs: logger.info('POLL | no PENDING requests'); return 0
    logger.info('POLL | picked %s request(s)',len(reqs))
    for r in reqs:
        rid=r['request_id']
        try:
            with get_connection() as conn:
                try:
                    bids=process_request(conn,r); mark_success(conn,rid,bids); conn.commit()
                except BucketProcessingError as e:
                    conn.rollback(); mark_failed(conn,rid,e.code,e.message); conn.commit()
                except Exception as e:
                    conn.rollback(); mark_failed(conn,rid,'UNEXPECTED_ERROR',f'{type(e).__name__}: {e}'); conn.commit(); logger.error('REQUEST | %s | TRACEBACK\n%s',rid,traceback.format_exc())
        except Exception:
            logger.exception('REQUEST | %s | fatal connection/processing failure',rid)
    return len(reqs)




