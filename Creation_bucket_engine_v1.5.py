#!/usr/bin/env python3
import os, sys, uuid, calendar, logging, traceback
from datetime import datetime, timedelta
import psycopg
from psycopg.rows import dict_row

DB_HOST=os.getenv('db_host','127.0.0.1')
DB_PORT=int(os.getenv('db_port','5432'))
DB_NAME=os.getenv('db_name','')
DB_USER=os.getenv('db_user','')
DB_PASSWORD=os.getenv('db_password','')
DB_SCHEMA=os.getenv('DB_SCHEMA','public')
# CUSTOMER_BUCKET.STATUS is INTEGER in the current DB design.
# Default ACTIVE status ID = 1. Override with environment variable if required.
CUSTOMER_BUCKET_ACTIVE_STATUS=int(os.getenv('CUSTOMER_BUCKET_ACTIVE_STATUS','1'))
SCRIPT_DIR='/home/edgerating/bucket_scripts'
LOG_DIR='/home/edgerating/bucket_logs'
os.makedirs(LOG_DIR,exist_ok=True)

# New log file for every script execution.
# Example: bucket_engine_v1_20260817_181530.log
RUN_TIMESTAMP=datetime.now().strftime('%Y%m%d_%H%M%S')
LOG_FILE=os.path.join(LOG_DIR,f'bucket_engine_v1_{RUN_TIMESTAMP}.log')

logger=logging.getLogger('bucket_engine_v1'); logger.setLevel(logging.INFO); logger.handlers.clear()
fmt=logging.Formatter('%(asctime)s | %(levelname)s | %(message)s','%Y-%m-%d %H:%M:%S')

# Detailed logs are written ONLY to the log file.
fh=logging.FileHandler(LOG_FILE)
fh.setLevel(logging.INFO)
fh.setFormatter(fmt)
logger.addHandler(fh)

class BucketProcessingError(Exception):
    def __init__(self,code,message,existing_bucket_id=None):
        super().__init__(message)
        self.code=code
        self.message=message
        self.existing_bucket_id=existing_bucket_id

def get_connection():
    if not DB_NAME or not DB_USER:
        raise RuntimeError('db_name/db_user not found. Run: source /home/edgerating/.profile')
    return psycopg.connect(host=DB_HOST,port=DB_PORT,dbname=DB_NAME,user=DB_USER,password=DB_PASSWORD,row_factory=dict_row)

REQUIRED_TABLES=['bucket_request','external_id_ref','pop_usage_spec_group','usg_bucket_grp','po_bucket_grp','bucket_definition','bucket_validity_period','validity_unit','bucket_triggers','customer_bucket']
REQUIRED_COLUMNS={
'bucket_request':['request_id','customer_ban','subscr_id','product_offering_id','pop_id','order_id','bucket_trigger_type','process_status','process_attempt_count','customer_bucket_id','error_code','error_message','processed_dt','updated_dt'],
'external_id_ref':['external_id','external_id_type','ban','subscr_id','active_dt','inactive_dt','tenant_id','status'],
'pop_usage_spec_group':['pop_id','usage_specification_id'],
'usg_bucket_grp':['usage_specification_id','bucket_group_id','tenant_id'],
'po_bucket_grp':['product_characteristic_value_id','product_characteristic_value','product_offering_id','bucket_type_group_id','tenant_id'],
'bucket_definition':['bucket_type_group_id','bucket_usage_type','bucket_unit','bucket_initial_value','validity_period_id','status','tenant_id'],
'bucket_validity_period':['validity_period_id','validity_period_name','validity_duration','validity_unit','timeband_start','timeband_end','status','valid_from','valid_to'],
'validity_unit':['validity_unit_id','validity_unit_name'],
'bucket_triggers':['bucket_trigger_type','bucket_trigger_id','bucket_type_group_id','tenant_id'],
'customer_bucket':['bucket_id','customer_ban','status','bucket_usage_type','bucket_validity_period','bucket_usage_group','bucket_trigger_type','order_id','product_offering_id','pop_id','timeband_start','timeband_end','valid_start_time','valid_end_time','created_at','last_updated_at','bucket_unit','bucket_initial_value','bucket_remaining_value']}

def validate_database_structure():
    logger.info('STARTUP | validating DB connection/schema')
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute('SELECT current_database() db,current_user usr'); x=cur.fetchone()
            logger.info('STARTUP | DB SUCCESS | database=%s user=%s host=%s port=%s',x['db'],x['usr'],DB_HOST,DB_PORT)
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
        cur.execute(f"""SELECT request_id,customer_ban,subscr_id,product_offering_id,pop_id,order_id,bucket_trigger_type FROM {DB_SCHEMA}.bucket_request WHERE process_status='PENDING' ORDER BY request_id FOR UPDATE SKIP LOCKED""")
        rows=cur.fetchall()
        for r in rows:
            cur.execute(f"""UPDATE {DB_SCHEMA}.bucket_request SET process_status='PROCESSING',process_attempt_count=process_attempt_count+1,updated_dt=CURRENT_TIMESTAMP WHERE request_id=%s""",(r['request_id'],))
            logger.info('REQUEST | %s | PENDING -> PROCESSING',r['request_id'])
        return rows

def validate_customer(conn,r):
    logger.info('REQUEST | %s | STEP1 validate EXTERNAL_ID_REF BAN=%s SUBSCR=%s',r['request_id'],r['customer_ban'],r['subscr_id'])
    with conn.cursor() as cur:
        cur.execute(f"""SELECT COUNT(*) cnt FROM {DB_SCHEMA}.external_id_ref WHERE ban::text=%s AND subscr_id=%s AND UPPER(TRIM(status))='ACTIVE' AND active_dt<=CURRENT_TIMESTAMP AND (inactive_dt IS NULL OR inactive_dt>CURRENT_TIMESTAMP)""",(r['customer_ban'],r['subscr_id']))
        cnt=cur.fetchone()['cnt']
    if cnt==0: raise BucketProcessingError('CUSTOMER_NOT_FOUND',f"No active EXTERNAL_ID_REF for BAN={r['customer_ban']} SUBSCR_ID={r['subscr_id']}")
    logger.info('REQUEST | %s | STEP1 SUCCESS | active_external_ids=%s',r['request_id'],cnt)

def get_usage_spec(conn,r):
    logger.info('REQUEST | %s | STEP2 POP_USAGE_SPEC_GROUP POP=%s',r['request_id'],r['pop_id'])
    with conn.cursor() as cur:
        cur.execute(f"SELECT usage_specification_id FROM {DB_SCHEMA}.pop_usage_spec_group WHERE pop_id=%s",(r['pop_id'],)); rows=cur.fetchall()
    if not rows: raise BucketProcessingError('USAGE_SPEC_NOT_FOUND',f"No usage spec for POP_ID={r['pop_id']}")
    if len(rows)>1: raise BucketProcessingError('MULTIPLE_USAGE_SPEC_FOUND',f"Multiple usage specs for POP_ID={r['pop_id']}")
    u=rows[0]['usage_specification_id']; logger.info('REQUEST | %s | STEP2 SUCCESS | POP=%s -> USAGE_SPEC=%s',r['request_id'],r['pop_id'],u); return u

def get_groups(conn,r,u):
    logger.info('REQUEST | %s | STEP3 USG_BUCKET_GRP usage_spec=%s',r['request_id'],u)
    with conn.cursor() as cur:
        cur.execute(f"SELECT bucket_group_id,tenant_id FROM {DB_SCHEMA}.usg_bucket_grp WHERE usage_specification_id::text=%s ORDER BY bucket_group_id",(str(u),)); rows=cur.fetchall()
    if not rows: raise BucketProcessingError('BUCKET_GROUP_NOT_FOUND',f'No bucket group for usage spec {u}')
    logger.info('REQUEST | %s | STEP3 SUCCESS | groups=%s',r['request_id'],[(x['bucket_group_id'],x['tenant_id']) for x in rows]); return rows

def validate_po_mapping(conn,r,g,tenant_id):
    logger.info('REQUEST | %s | STEP4 PO_BUCKET_GRP product=%s group=%s tenant=%s',r['request_id'],r['product_offering_id'],g,tenant_id)
    with conn.cursor() as cur:
        cur.execute(f"""SELECT product_characteristic_value_id,product_characteristic_value,product_offering_id,bucket_type_group_id,tenant_id FROM {DB_SCHEMA}.po_bucket_grp WHERE product_offering_id::text=%s AND bucket_type_group_id=%s AND tenant_id=%s""",(r['product_offering_id'],g,tenant_id)); rows=cur.fetchall()
    if not rows: raise BucketProcessingError('PO_BUCKET_MAPPING_NOT_FOUND',f"No PO_BUCKET_GRP for product={r['product_offering_id']} group={g} tenant={tenant_id}")
    if len(rows)>1: raise BucketProcessingError('MULTIPLE_PO_BUCKET_MAPPING_FOUND',f"Multiple PO_BUCKET_GRP rows for product={r['product_offering_id']} group={g} tenant={tenant_id}")
    logger.info('REQUEST | %s | STEP4 SUCCESS | char_id=%s value=%s',r['request_id'],rows[0]['product_characteristic_value_id'],rows[0]['product_characteristic_value'])

def get_bucket_definition(conn,r,g,tenant_id):
    logger.info('REQUEST | %s | STEP5 BUCKET_DEFINITION group=%s tenant=%s',r['request_id'],g,tenant_id)
    with conn.cursor() as cur:
        cur.execute(f"""SELECT bucket_type_group_id,bucket_usage_type,bucket_unit,bucket_initial_value,validity_period_id,status,tenant_id FROM {DB_SCHEMA}.bucket_definition WHERE bucket_type_group_id=%s AND tenant_id=%s AND UPPER(TRIM(status))='ACTIVE'""",(g,tenant_id)); rows=cur.fetchall()
    if not rows: raise BucketProcessingError('BUCKET_DEFINITION_NOT_FOUND',f'No active BUCKET_DEFINITION for group={g} tenant={tenant_id}')
    if len(rows)>1: raise BucketProcessingError('MULTIPLE_BUCKET_DEFINITION_FOUND',f'Multiple BUCKET_DEFINITION rows for group={g} tenant={tenant_id}')
    b=rows[0]; logger.info('REQUEST | %s | STEP5 SUCCESS | type=%s unit=%s initial=%s validity_id=%s',r['request_id'],b['bucket_usage_type'],b['bucket_unit'],b['bucket_initial_value'],b['validity_period_id']); return b

def get_validity(conn,r,vid):
    logger.info('REQUEST | %s | STEP6 BUCKET_VALIDITY_PERIOD id=%s',r['request_id'],vid)
    with conn.cursor() as cur:
        cur.execute(f"""SELECT bvp.validity_period_id,bvp.validity_period_name,bvp.validity_duration,vu.validity_unit_name,bvp.timeband_start,bvp.timeband_end FROM {DB_SCHEMA}.bucket_validity_period bvp JOIN {DB_SCHEMA}.validity_unit vu ON vu.validity_unit_id=bvp.validity_unit WHERE bvp.validity_period_id=%s AND bvp.status=1 AND CURRENT_TIMESTAMP>=bvp.valid_from AND (bvp.valid_to IS NULL OR CURRENT_TIMESTAMP<bvp.valid_to)""",(vid,)); v=cur.fetchone()
    if not v: raise BucketProcessingError('VALIDITY_PERIOD_NOT_FOUND',f'Validity period {vid} missing/inactive/outside dates')
    logger.info('REQUEST | %s | STEP6 SUCCESS | %s %s %s',r['request_id'],v['validity_period_name'],v['validity_duration'],v['validity_unit_name']); return v

def validate_trigger(conn,r,g,tenant_id):
    logger.info('REQUEST | %s | STEP7 trigger=%s group=%s tenant=%s',r['request_id'],r['bucket_trigger_type'],g,tenant_id)
    with conn.cursor() as cur:
        cur.execute(f"""SELECT 1 FROM {DB_SCHEMA}.bucket_triggers WHERE LOWER(TRIM(bucket_trigger_type))=LOWER(TRIM(%s)) AND bucket_type_group_id=%s AND tenant_id=%s LIMIT 1""",(r['bucket_trigger_type'],g,tenant_id))
        if not cur.fetchone(): raise BucketProcessingError('INVALID_TRIGGER',f"Trigger {r['bucket_trigger_type']} not mapped to group={g} tenant={tenant_id}")
    logger.info('REQUEST | %s | STEP7 SUCCESS',r['request_id'])

def find_existing(conn,r,g):
    logger.info(
        'REQUEST | %s | STEP8 checking existing active bucket',
        r['request_id']
    )

    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT
                bucket_id,
                order_id,
                status,
                valid_start_time,
                valid_end_time
            FROM {DB_SCHEMA}.customer_bucket
            WHERE customer_ban::text=%s
              AND product_offering_id::text=%s
              AND bucket_usage_group::text=%s
              AND UPPER(TRIM(status::text))='ACTIVE'
              AND (
                    valid_end_time IS NULL
                    OR valid_end_time>CURRENT_TIMESTAMP
                  )
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (
                r['customer_ban'],
                r['product_offering_id'],
                str(g)
            )
        )
        x=cur.fetchone()

    if x:
        logger.warning(
            'REQUEST | %s | STEP8 DUPLICATE | existing_bucket_id=%s existing_order_id=%s',
            r['request_id'],
            x['bucket_id'],
            x['order_id']
        )
    else:
        logger.info(
            'REQUEST | %s | STEP8 SUCCESS | no active bucket exists',
            r['request_id']
        )

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
    if u in ('WEEK','WEEKS'): return start+timedelta(weeks=duration)
    if u in ('MONTH','MONTHS'): return add_months(start,duration)
    if u in ('YEAR','YEARS'): return add_years(start,duration)
    raise BucketProcessingError('UNSUPPORTED_VALIDITY_UNIT',f'Unsupported validity unit {u}')

def insert_bucket(conn,r,g,b,v):
    logger.info('REQUEST | %s | STEP9 prepare CUSTOMER_BUCKET',r['request_id'])
    bid=str(uuid.uuid4()); start=datetime.now(); end=calc_end(start,v['validity_duration'],v['validity_unit_name']); initial=b['bucket_initial_value']
    with conn.cursor() as cur:
        cur.execute(f"""INSERT INTO {DB_SCHEMA}.customer_bucket(bucket_id,customer_ban,status,bucket_usage_type,bucket_validity_period,bucket_usage_group,bucket_trigger_type,order_id,product_offering_id,pop_id,timeband_start,timeband_end,valid_start_time,valid_end_time,created_at,last_updated_at,bucket_unit,bucket_initial_value,bucket_remaining_value) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,%s,%s,%s)""",(bid,r['customer_ban'],CUSTOMER_BUCKET_ACTIVE_STATUS,b['bucket_usage_type'],v['validity_period_id'],str(g),r['bucket_trigger_type'],r['order_id'],r['product_offering_id'],str(r['pop_id']),v['timeband_start'],v['timeband_end'],start,end,b['bucket_unit'],initial,initial))
    logger.info('REQUEST | %s | STEP9 SUCCESS | bucket_id=%s status_id=%s start=%s end=%s initial=%s remaining=%s',r['request_id'],bid,CUSTOMER_BUCKET_ACTIVE_STATUS,start,end,initial,initial); return bid

def mark_success(conn,rid,bids):
    first=bids[0] if bids else None; details='Created bucket IDs: '+','.join(bids) if len(bids)>1 else None
    with conn.cursor() as cur:
        cur.execute(f"""UPDATE {DB_SCHEMA}.bucket_request SET process_status='SUCCESS',customer_bucket_id=%s,processed_dt=CURRENT_TIMESTAMP,updated_dt=CURRENT_TIMESTAMP,error_code=NULL,error_message=%s WHERE request_id=%s""",(first,details,rid))
    logger.info('REQUEST | %s | FINAL SUCCESS | bucket_ids=%s',rid,bids)

def mark_failed(conn,rid,code,msg,existing_bucket_id=None):
    with conn.cursor() as cur:
        cur.execute(
            f"""
            UPDATE {DB_SCHEMA}.bucket_request
            SET process_status='FAILED',
                customer_bucket_id=%s,
                processed_dt=CURRENT_TIMESTAMP,
                updated_dt=CURRENT_TIMESTAMP,
                error_code=%s,
                error_message=%s
            WHERE request_id=%s
            """,
            (
                existing_bucket_id,
                code,
                msg[:1000],
                rid
            )
        )

    logger.error(
        'REQUEST | %s | FINAL FAILED | code=%s | existing_bucket_id=%s | message=%s',
        rid,
        code,
        existing_bucket_id,
        msg
    )

def process_request(conn,r):
    logger.info('='*100)
    logger.info(
        'REQUEST | %s | START | BAN=%s SUBSCR=%s PRODUCT=%s POP=%s ORDER=%s TRIGGER=%s',
        r['request_id'],
        r['customer_ban'],
        r['subscr_id'],
        r['product_offering_id'],
        r['pop_id'],
        r['order_id'],
        r['bucket_trigger_type']
    )

    validate_customer(conn,r)
    u=get_usage_spec(conn,r)
    groups=get_groups(conn,r,u)
    bids=[]

    for row in groups:
        g=row['bucket_group_id']
        tenant_id=row['tenant_id']

        validate_po_mapping(conn,r,g,tenant_id)
        b=get_bucket_definition(conn,r,g,tenant_id)
        v=get_validity(conn,r,b['validity_period_id'])
        validate_trigger(conn,r,g,tenant_id)

        x=find_existing(conn,r,g)

        if x:
            raise BucketProcessingError(
                'ACTIVE_BUCKET_ALREADY_EXISTS',
                (
                    f"Active bucket already exists for "
                    f"CUSTOMER_BAN={r['customer_ban']}, "
                    f"PRODUCT_OFFERING_ID={r['product_offering_id']}, "
                    f"BUCKET_GROUP={g}, "
                    f"EXISTING_ORDER_ID={x['order_id']}, "
                    f"EXISTING_BUCKET_ID={x['bucket_id']}"
                ),
                existing_bucket_id=x['bucket_id']
            )

        bids.append(insert_bucket(conn,r,g,b,v))

    if not bids:
        raise BucketProcessingError(
            'NO_BUCKET_CREATED',
            'No customer bucket created'
        )

    return bids

def process_batch():
    total=success=failed=0
    with get_connection() as conn:
        reqs=pick_pending_requests(conn); conn.commit()
    if not reqs: logger.info('POLL | no PENDING requests'); return total,success,failed
    total=len(reqs); logger.info('POLL | picked %s request(s)',total)
    for r in reqs:
        rid=r['request_id']
        with get_connection() as conn:
            try:
                bids=process_request(conn,r); mark_success(conn,rid,bids); conn.commit(); success+=1
            except BucketProcessingError as e:
                conn.rollback()
                mark_failed(
                    conn,
                    rid,
                    e.code,
                    e.message,
                    e.existing_bucket_id
                )
                conn.commit()
                failed+=1
            except Exception as e:
                conn.rollback()
                mark_failed(
                    conn,
                    rid,
                    'UNEXPECTED_ERROR',
                    f'{type(e).__name__}: {e}',
                    None
                )
                conn.commit()
                logger.error(
                    'REQUEST | %s | TRACEBACK\n%s',
                    rid,
                    traceback.format_exc()
                )
                failed+=1
    return total,success,failed

def main():
    print("Bucket Engine V1 started")

    logger.info('='*100)
    logger.info('Bucket Engine V1 starting')
    logger.info('Python=%s',sys.version.replace('\n',' '))
    logger.info('Script path=%s | Log path=%s',SCRIPT_DIR,LOG_FILE)
    logger.info(
        'DB host=%s port=%s db=%s user=%s schema=%s',
        DB_HOST,DB_PORT,DB_NAME,DB_USER,DB_SCHEMA
    )

    try:
        validate_database_structure()
    except Exception:
        logger.exception('STARTUP FAILED')

        print("")
        print("Total Requests : 0")
        print("Success Count  : 0")
        print("Error Count    : 1")
        print("")
        print("Bucket Engine V1 completed with errors")
        print(f"Log File       : {LOG_FILE}")
        sys.exit(1)

    try:
        total,success,failed=process_batch()

        logger.info(
            'RUN SUMMARY | total=%s | success=%s | failed=%s',
            total,success,failed
        )
        logger.info('Bucket Engine V1 completed')

        print("")
        print(f"Total Requests : {total}")
        print(f"Success Count  : {success}")
        print(f"Error Count    : {failed}")
        print("")

        if total == 0:
            print("Bucket Engine V1 completed - No pending requests")
        elif success == total:
            print("Bucket Engine V1 completed successfully")
        elif failed == total:
            print("Bucket Engine V1 completed with errors")
        else:
            print("Bucket Engine V1 partially completed")

        print(f"Log File       : {LOG_FILE}")

    except Exception:
        logger.exception('MAIN PROCESSING ERROR')

        print("")
        print("Total Requests : 0")
        print("Success Count  : 0")
        print("Error Count    : 1")
        print("")
        print("Bucket Engine V1 completed with errors")
        print(f"Log File       : {LOG_FILE}")
        sys.exit(1)

if __name__=='__main__': main()
