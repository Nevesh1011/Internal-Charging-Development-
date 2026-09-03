#!/usr/bin/env python3
import os, sys, calendar, logging, traceback, atexit
from datetime import datetime, timedelta
import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

DB_HOST=os.getenv('db_host','127.0.0.1')
DB_PORT=int(os.getenv('db_port','5432'))
DB_NAME=os.getenv('db_name','')
DB_USER=os.getenv('db_user','')
DB_PASSWORD=os.getenv('db_password','')
DB_SCHEMA=os.getenv('DB_SCHEMA','public')
BUCKET_ID_SEQUENCE=f'{DB_SCHEMA}.customer_bucket_id_seq'

# PostgreSQL connection-pool configuration.
# Connections are created once and reused across engine stages/requests.
DB_POOL_MIN_SIZE=int(os.getenv('DB_POOL_MIN_SIZE','1'))
DB_POOL_MAX_SIZE=int(os.getenv('DB_POOL_MAX_SIZE','10'))
DB_POOL_TIMEOUT=float(os.getenv('DB_POOL_TIMEOUT','30'))
DB_POOL=None
# CUSTOMER_BUCKET.STATUS is VARCHAR in the current DB design.
# Default active value = ACTIVE.
CUSTOMER_BUCKET_ACTIVE_STATUS=os.getenv('CUSTOMER_BUCKET_ACTIVE_STATUS','ACTIVE').strip().upper()
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

def init_connection_pool():
    """Create the PostgreSQL connection pool once per engine execution."""
    global DB_POOL

    if DB_POOL is not None:
        return DB_POOL

    if not DB_NAME or not DB_USER:
        raise RuntimeError('db_name/db_user not found. Run: source /home/edgerating/.profile')

    if DB_POOL_MIN_SIZE < 1:
        raise RuntimeError('DB_POOL_MIN_SIZE must be >= 1')

    if DB_POOL_MAX_SIZE < DB_POOL_MIN_SIZE:
        raise RuntimeError('DB_POOL_MAX_SIZE must be >= DB_POOL_MIN_SIZE')

    DB_POOL=ConnectionPool(
        conninfo='',
        min_size=DB_POOL_MIN_SIZE,
        max_size=DB_POOL_MAX_SIZE,
        timeout=DB_POOL_TIMEOUT,
        kwargs={
            'host':DB_HOST,
            'port':DB_PORT,
            'dbname':DB_NAME,
            'user':DB_USER,
            'password':DB_PASSWORD,
            'row_factory':dict_row
        },
        open=True
    )

    # Fail fast during startup if PostgreSQL cannot provide the minimum pool size.
    DB_POOL.wait(timeout=DB_POOL_TIMEOUT)

    logger.info(
        'STARTUP | DB connection pool ready | min_size=%s max_size=%s timeout=%ss',
        DB_POOL_MIN_SIZE,DB_POOL_MAX_SIZE,DB_POOL_TIMEOUT
    )

    return DB_POOL


def get_connection():
    """Borrow one reusable connection from the pool.

    Existing code can continue using:
        with get_connection() as conn:
            ...

    Leaving the with-block returns the connection to the pool instead of
    closing the physical PostgreSQL connection.
    """
    pool=init_connection_pool()
    return pool.connection(timeout=DB_POOL_TIMEOUT)


def close_connection_pool():
    """Close all pooled PostgreSQL connections when the engine exits."""
    global DB_POOL

    if DB_POOL is not None:
        try:
            DB_POOL.close()
            logger.info('SHUTDOWN | DB connection pool closed')
        finally:
            DB_POOL=None


atexit.register(close_connection_pool)

REQUIRED_TABLES=['bucket_request_inbound','bucket_request','external_id_ref','pop_usage_spec_group','usg_bucket_grp','pop_bucket_grp','po_bucket_grp','bucket_definition','bucket_validity_period','validity_unit','bucket_triggers','bucket_rollover_config','uom_ref','customer_bucket']
REQUIRED_COLUMNS={
'bucket_request_inbound':['inbound_request_id','order_id','customer_ban','subscr_id','status','bucket_usage_type','bucket_trigger_type','bucket_size','unit','product_offering_id','pop_id','product_characteristic_value_id','is_rollover_enabled','rollover_cap_value','rollover_cap_unit','timeband_start','timeband_end','activation_time','termination_time','request_status','engine_request_id','customer_bucket_id','error_code','error_message','received_dt','processed_dt','updated_dt'],
'bucket_request':['request_id','customer_ban','subscr_id','product_offering_id','pop_id','order_id','bucket_trigger_type','bucket_trigger_id','process_status','process_attempt_count','customer_bucket_id','error_code','error_message','processed_dt','updated_dt','usage_spec_id','bucket_type_group_id','product_characteristic_value_id','bucket_usage_type','bucket_size','bucket_unit','is_rollover_enabled','rollover_cap_value','rollover_cap_unit'],
'external_id_ref':['external_id','external_id_type','ban','subscr_id','active_dt','inactive_dt','tenant_id','status'],
'pop_usage_spec_group':['pop_id','usage_spec_id'],
'usg_bucket_grp':['usage_spec_id','bucket_type_group_id','tenant_id'],
'pop_bucket_grp':['pop_id','usage_spec_id','bucket_type_group_id','tenant_id','status'],
'po_bucket_grp':['product_characteristic_value_id','product_characteristic_value','product_offering_id','bucket_type_group_id','tenant_id'],
'bucket_definition':['bucket_type_group_id','bucket_usage_type','bucket_unit','bucket_initial_value','validity_period_id','status','tenant_id'],
'uom_ref':['uom_id','description','tenant_id','base_uom_id','conversion_factor','dimension'],
'bucket_validity_period':['validity_period_id','validity_period_name','validity_duration','validity_unit','timeband_start','timeband_end','status','valid_from','valid_to'],
'validity_unit':['validity_unit_id','validity_unit_name'],
'bucket_triggers':['bucket_trigger_type','bucket_trigger_id','bucket_type_group_id','tenant_id'],
'bucket_rollover_config':['product_offering_id','bucket_type_group_id','tenant_id','is_rollover_enabled','rollover_cap_value','rollover_cap_unit','status','valid_from','valid_to'],
'customer_bucket':['bucket_id','customer_ban','tenant_id','status','bucket_usage_type','bucket_validity_period','bucket_usage_group','bucket_trigger_type','bucket_trigger_id','order_id','product_offering_id','pop_id','product_spec_char_value_use_id','is_rollover_enabled','rollover_cap_value','rollover_cap_unit','timeband_start','timeband_end','valid_start_time','valid_end_time','created_at','last_updated_at','bucket_unit','bucket_initial_value','bucket_remaining_value']}

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


def mark_inbound_failed(conn,inbound_id,code,message):
    with conn.cursor() as cur:
        cur.execute(
            f"""
            UPDATE {DB_SCHEMA}.bucket_request_inbound
            SET request_status='FAILED',
                error_code=%s,
                error_message=%s,
                processed_dt=CURRENT_TIMESTAMP,
                updated_dt=CURRENT_TIMESTAMP
            WHERE inbound_request_id=%s
            """,
            (code,message[:1000],inbound_id)
        )

    logger.error(
        'INBOUND | %s | FAILED | code=%s | message=%s',
        inbound_id,
        code,
        message
    )


def process_inbound_requests():
    """
    Stage 1:
      BUCKET_REQUEST_INBOUND(RECEIVED)
         -> validate mandatory values
         -> duplicate/conflict check
         -> BUCKET_REQUEST(PENDING)
         -> reverse update inbound as SUBMITTED
    """
    received=0
    submitted=0
    rejected=0

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT
                    inbound_request_id,
                    order_id,
                    customer_ban,
                    subscr_id,
                    bucket_trigger_type,
                    product_offering_id,
                    pop_id
                FROM {DB_SCHEMA}.bucket_request_inbound
                WHERE request_status='RECEIVED'
                ORDER BY inbound_request_id
                FOR UPDATE SKIP LOCKED
                """
            )
            rows=cur.fetchall()

        received=len(rows)

        for r in rows:
            inbound_id=r['inbound_request_id']

            try:
                # Mandatory inbound validation
                mandatory={
                    'order_id':r['order_id'],
                    'customer_ban':r['customer_ban'],
                    'subscr_id':r['subscr_id'],
                    'bucket_trigger_type':r['bucket_trigger_type'],
                    'product_offering_id':r['product_offering_id'],
                    'pop_id':r['pop_id']
                }

                missing=[
                    key
                    for key,value in mandatory.items()
                    if value is None or str(value).strip()==''
                ]

                if missing:
                    mark_inbound_failed(
                        conn,
                        inbound_id,
                        'MANDATORY_INPUT_MISSING',
                        f"Missing mandatory inbound fields: {','.join(missing)}"
                    )
                    conn.commit()
                    rejected+=1
                    continue

                # POP_ID must be compatible with BUCKET_REQUEST.pop_id BIGINT
                try:
                    pop_id=int(str(r['pop_id']).strip())
                except (ValueError,TypeError):
                    mark_inbound_failed(
                        conn,
                        inbound_id,
                        'INVALID_POP_ID',
                        f"POP_ID={r['pop_id']} is not numeric and cannot be moved to BUCKET_REQUEST"
                    )
                    conn.commit()
                    rejected+=1
                    continue

                # ORDER_ID is globally unique in engine processing.
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        SELECT
                            request_id,
                            customer_ban,
                            subscr_id,
                            product_offering_id,
                            pop_id,
                            bucket_trigger_type,
                            process_status,
                            customer_bucket_id
                        FROM {DB_SCHEMA}.bucket_request
                        WHERE order_id::text=%s
                        ORDER BY request_id
                        LIMIT 1
                        """,
                        (str(r['order_id']),)
                    )
                    existing=cur.fetchone()

                if existing:
                    same_request=(
                        str(existing['customer_ban'])==str(r['customer_ban'])
                        and str(existing['subscr_id'])==str(r['subscr_id'])
                        and str(existing['product_offering_id'])==str(r['product_offering_id'])
                        and str(existing['pop_id'])==str(pop_id)
                        and str(existing['bucket_trigger_type']).strip().lower()
                            ==str(r['bucket_trigger_type']).strip().lower()
                    )

                    if same_request:
                        # Idempotent recovery: do not create another BUCKET_REQUEST.
                        with conn.cursor() as cur:
                            cur.execute(
                                f"""
                                UPDATE {DB_SCHEMA}.bucket_request_inbound
                                SET request_status='SUBMITTED',
                                    engine_request_id=%s,
                                    customer_bucket_id=%s,
                                    error_code=NULL,
                                    error_message=NULL,
                                    updated_dt=CURRENT_TIMESTAMP
                                WHERE inbound_request_id=%s
                                """,
                                (
                                    existing['request_id'],
                                    existing['customer_bucket_id'],
                                    inbound_id
                                )
                            )

                        logger.warning(
                            'INBOUND | %s | ORDER already forwarded | order=%s engine_request_id=%s status=%s',
                            inbound_id,
                            r['order_id'],
                            existing['request_id'],
                            existing['process_status']
                        )
                        conn.commit()
                        submitted+=1
                        continue

                    mark_inbound_failed(
                        conn,
                        inbound_id,
                        'ORDER_ID_CONFLICT',
                        (
                            f"ORDER_ID={r['order_id']} already exists in BUCKET_REQUEST "
                            f"with different request details; EXISTING_REQUEST_ID={existing['request_id']}"
                        )
                    )
                    conn.commit()
                    rejected+=1
                    continue

                # Create internal engine request.
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        INSERT INTO {DB_SCHEMA}.bucket_request
                        (
                            customer_ban,
                            subscr_id,
                            product_offering_id,
                            pop_id,
                            order_id,
                            bucket_trigger_type
                        )
                        VALUES (%s,%s,%s,%s,%s,%s)
                        RETURNING request_id
                        """,
                        (
                            str(r['customer_ban']),
                            r['subscr_id'],
                            str(r['product_offering_id']),
                            pop_id,
                            str(r['order_id']),
                            str(r['bucket_trigger_type'])
                        )
                    )

                    engine_request_id=cur.fetchone()['request_id']

                    cur.execute(
                        f"""
                        UPDATE {DB_SCHEMA}.bucket_request_inbound
                        SET request_status='SUBMITTED',
                            engine_request_id=%s,
                            error_code=NULL,
                            error_message=NULL,
                            updated_dt=CURRENT_TIMESTAMP
                        WHERE inbound_request_id=%s
                        """,
                        (engine_request_id,inbound_id)
                    )

                logger.info(
                    'INBOUND | %s | SUBMITTED | order=%s -> engine_request_id=%s',
                    inbound_id,
                    r['order_id'],
                    engine_request_id
                )

                conn.commit()
                submitted+=1

            except Exception as e:
                conn.rollback()

                try:
                    mark_inbound_failed(
                        conn,
                        inbound_id,
                        'INBOUND_UNEXPECTED_ERROR',
                        f'{type(e).__name__}: {e}'
                    )
                    conn.commit()
                except Exception:
                    logger.exception(
                        'INBOUND | %s | unable to reverse update inbound failure',
                        inbound_id
                    )

                logger.error(
                    'INBOUND | %s | TRACEBACK\n%s',
                    inbound_id,
                    traceback.format_exc()
                )
                rejected+=1

    return received,submitted,rejected


def sync_inbound_results():
    """
    Stage 3:
      Reverse-update BUCKET_REQUEST_INBOUND after BUCKET_REQUEST processing.

      Control/result fields come from BUCKET_REQUEST.
      Final bucket snapshot fields come from CUSTOMER_BUCKET using
      BUCKET_REQUEST.customer_bucket_id = CUSTOMER_BUCKET.bucket_id.

      This keeps BUCKET_REQUEST lightweight while making the inbound row
      easy to use for end-to-end request/result backtracking.
    """
    synced=0

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT
                    bri.inbound_request_id,
                    br.request_id,
                    br.process_status,
                    br.customer_bucket_id,
                    br.error_code,
                    br.error_message,
                    br.processed_dt,
                    cb.status AS bucket_status,
                    cb.tenant_id AS bucket_tenant_id,
                    cb.bucket_usage_type,
                    cb.bucket_trigger_type AS final_bucket_trigger_type,
                    cb.bucket_initial_value AS bucket_size,
                    cb.bucket_unit AS bucket_uom_id,
                    uom.description AS unit,
                    cb.product_spec_char_value_use_id AS product_characteristic_value_id,
                    cb.is_rollover_enabled,
                    cb.rollover_cap_value,
                    cb.rollover_cap_unit,
                    cb.timeband_start,
                    cb.timeband_end,
                    cb.valid_start_time AS activation_time,
                    cb.valid_end_time AS termination_time
                FROM {DB_SCHEMA}.bucket_request_inbound bri
                JOIN {DB_SCHEMA}.bucket_request br
                  ON br.request_id=bri.engine_request_id
                LEFT JOIN {DB_SCHEMA}.customer_bucket cb
                  ON cb.bucket_id::text=br.customer_bucket_id::text
                LEFT JOIN {DB_SCHEMA}.uom_ref uom
                  ON uom.uom_id=cb.bucket_unit
                 AND uom.tenant_id=cb.tenant_id
                WHERE bri.engine_request_id IS NOT NULL
                  AND br.process_status IN ('SUCCESS','FAILED')
                  AND (
                        bri.request_status IS DISTINCT FROM br.process_status
                        OR bri.customer_bucket_id IS DISTINCT FROM br.customer_bucket_id
                        OR bri.error_code IS DISTINCT FROM br.error_code
                        OR bri.error_message IS DISTINCT FROM br.error_message
                        OR (br.process_status='SUCCESS' AND (
                              bri.status IS DISTINCT FROM cb.status
                              OR bri.bucket_usage_type IS DISTINCT FROM cb.bucket_usage_type
                              OR bri.bucket_size IS DISTINCT FROM cb.bucket_initial_value
                              OR bri.unit IS DISTINCT FROM uom.description
                              OR bri.product_characteristic_value_id::text IS DISTINCT FROM cb.product_spec_char_value_use_id::text
                              OR bri.is_rollover_enabled IS DISTINCT FROM cb.is_rollover_enabled
                              OR bri.rollover_cap_value IS DISTINCT FROM cb.rollover_cap_value
                              OR bri.rollover_cap_unit IS DISTINCT FROM cb.rollover_cap_unit
                              OR bri.timeband_start IS DISTINCT FROM cb.timeband_start
                              OR bri.timeband_end IS DISTINCT FROM cb.timeband_end
                              OR bri.activation_time IS DISTINCT FROM cb.valid_start_time
                              OR bri.termination_time IS DISTINCT FROM cb.valid_end_time
                            ))
                      )
                ORDER BY bri.inbound_request_id
                """
            )
            rows=cur.fetchall()

            for row in rows:
                # For FAILED requests there may be no newly-created CUSTOMER_BUCKET.
                # In that case only processing/error fields are updated and the
                # original inbound business input remains untouched.
                if row['process_status']=='SUCCESS' and row['customer_bucket_id'] is not None:
                    cur.execute(
                        f"""
                        UPDATE {DB_SCHEMA}.bucket_request_inbound
                        SET request_status=%s,
                            customer_bucket_id=%s,
                            error_code=%s,
                            error_message=%s,
                            processed_dt=%s,
                            status=%s,
                            bucket_usage_type=%s,
                            bucket_trigger_type=%s,
                            bucket_size=%s,
                            unit=%s,
                            product_characteristic_value_id=%s,
                            is_rollover_enabled=%s,
                            rollover_cap_value=%s,
                            rollover_cap_unit=%s,
                            timeband_start=%s,
                            timeband_end=%s,
                            activation_time=%s,
                            termination_time=%s,
                            updated_dt=CURRENT_TIMESTAMP
                        WHERE inbound_request_id=%s
                        """,
                        (
                            row['process_status'],
                            row['customer_bucket_id'],
                            row['error_code'],
                            row['error_message'],
                            row['processed_dt'],
                            row['bucket_status'],
                            row['bucket_usage_type'],
                            row['final_bucket_trigger_type'],
                            row['bucket_size'],
                            row['unit'],
                            None if row['product_characteristic_value_id'] is None else str(row['product_characteristic_value_id']),
                            row['is_rollover_enabled'],
                            row['rollover_cap_value'],
                            row['rollover_cap_unit'],
                            row['timeband_start'],
                            row['timeband_end'],
                            row['activation_time'],
                            row['termination_time'],
                            row['inbound_request_id']
                        )
                    )
                else:
                    cur.execute(
                        f"""
                        UPDATE {DB_SCHEMA}.bucket_request_inbound
                        SET request_status=%s,
                            customer_bucket_id=%s,
                            error_code=%s,
                            error_message=%s,
                            processed_dt=%s,
                            updated_dt=CURRENT_TIMESTAMP
                        WHERE inbound_request_id=%s
                        """,
                        (
                            row['process_status'],
                            row['customer_bucket_id'],
                            row['error_code'],
                            row['error_message'],
                            row['processed_dt'],
                            row['inbound_request_id']
                        )
                    )

                logger.info(
                    'INBOUND | %s | REVERSE UPDATE | engine_request_id=%s status=%s bucket_id=%s bucket_type=%s rollover_enabled=%s',
                    row['inbound_request_id'],
                    row['request_id'],
                    row['process_status'],
                    row['customer_bucket_id'],
                    row['bucket_usage_type'],
                    row['is_rollover_enabled']
                )

                synced+=1

        conn.commit()

    return synced


def pick_pending_requests(conn):
    with conn.cursor() as cur:
        cur.execute(f"""SELECT request_id,customer_ban,subscr_id,product_offering_id,pop_id,order_id,bucket_trigger_type FROM {DB_SCHEMA}.bucket_request WHERE process_status='PENDING' ORDER BY request_id FOR UPDATE SKIP LOCKED""")
        rows=cur.fetchall()
        for r in rows:
            cur.execute(f"""UPDATE {DB_SCHEMA}.bucket_request SET process_status='PROCESSING',process_attempt_count=process_attempt_count+1,updated_dt=CURRENT_TIMESTAMP WHERE request_id=%s""",(r['request_id'],))
            logger.info('REQUEST | %s | PENDING -> PROCESSING',r['request_id'])
        return rows

def find_processed_request_duplicate(conn,r):
    logger.info(
        'REQUEST | %s | PRECHECK ORDER duplicate ORDER_ID=%s',
        r['request_id'],
        r['order_id']
    )

    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT
                request_id,
                customer_ban,
                subscr_id,
                customer_bucket_id,
                process_status,
                processed_dt
            FROM {DB_SCHEMA}.bucket_request
            WHERE request_id <> %s
              AND order_id::text = %s
              AND process_status = 'SUCCESS'
            ORDER BY processed_dt DESC NULLS LAST, request_id DESC
            LIMIT 1
            """,
            (
                r['request_id'],
                r['order_id']
            )
        )
        existing = cur.fetchone()

    if existing:
        logger.warning(
            'REQUEST | %s | DUPLICATE ORDER | order_id=%s existing_request_id=%s existing_ban=%s existing_subscr=%s existing_bucket_id=%s',
            r['request_id'],
            r['order_id'],
            existing['request_id'],
            existing['customer_ban'],
            existing['subscr_id'],
            existing['customer_bucket_id']
        )
    else:
        logger.info(
            'REQUEST | %s | PRECHECK ORDER SUCCESS | no previous SUCCESS request with ORDER_ID=%s',
            r['request_id'],
            r['order_id']
        )

    return existing

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
        cur.execute(f"SELECT usage_spec_id FROM {DB_SCHEMA}.pop_usage_spec_group WHERE pop_id=%s",(r['pop_id'],)); rows=cur.fetchall()
    if not rows: raise BucketProcessingError('USAGE_SPEC_NOT_FOUND',f"No usage spec for POP_ID={r['pop_id']}")
    if len(rows)>1: raise BucketProcessingError('MULTIPLE_USAGE_SPEC_FOUND',f"Multiple usage specs for POP_ID={r['pop_id']}")
    u=rows[0]['usage_spec_id']; logger.info('REQUEST | %s | STEP2 SUCCESS | POP=%s -> USAGE_SPEC=%s',r['request_id'],r['pop_id'],u); return u

def get_groups(conn,r,u):
    """
    Resolve the exact bucket group for the incoming POP.

    Why POP_BUCKET_GRP is required:
      - Multiple POPs may use the same USAGE_SPEC_ID.
      - One USAGE_SPEC_ID may map to multiple bucket groups.
      - The same Product Offering may contain those multiple groups.

    Example:
      POP 9103 -> Usage Spec 23103 -> Group 1003 -> rollover enabled
      POP 9104 -> Usage Spec 23103 -> Group 1004 -> rollover disabled

    Resolution path:
      1. USG_BUCKET_GRP gives candidate groups for the Usage Spec.
      2. POP_BUCKET_GRP selects the exact group for POP + Usage Spec.
      3. PO_BUCKET_GRP confirms that selected group belongs to the Product Offering.
    """
    logger.info(
        'REQUEST | %s | STEP3 resolve group | POP=%s USAGE_SPEC=%s PRODUCT=%s',
        r['request_id'],r['pop_id'],u,r['product_offering_id']
    )

    # 1. Get all candidate groups configured for the Usage Spec.
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT bucket_type_group_id, tenant_id
            FROM {DB_SCHEMA}.usg_bucket_grp
            WHERE usage_spec_id::text=%s
            ORDER BY bucket_type_group_id
            """,
            (str(u),)
        )
        candidate_rows=cur.fetchall()

    if not candidate_rows:
        raise BucketProcessingError(
            'BUCKET_GROUP_NOT_FOUND',
            f'No bucket group for usage spec {u}'
        )

    logger.info(
        'REQUEST | %s | STEP3 CANDIDATE GROUPS | usage_spec=%s groups=%s',
        r['request_id'],u,
        [(x['bucket_type_group_id'],x['tenant_id']) for x in candidate_rows]
    )

    # 2. POP_BUCKET_GRP keeps the POP distinction even when Usage Spec is shared.
    #    Join to USG_BUCKET_GRP so the selected POP group must also be a valid
    #    candidate group for the Usage Spec.
    #    Join to PO_BUCKET_GRP so the same group must belong to the incoming PO.
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT DISTINCT
                pbg.bucket_type_group_id,
                pbg.tenant_id
            FROM {DB_SCHEMA}.pop_bucket_grp pbg
            JOIN {DB_SCHEMA}.usg_bucket_grp ubg
              ON ubg.usage_spec_id = pbg.usage_spec_id
             AND ubg.bucket_type_group_id = pbg.bucket_type_group_id
             AND ubg.tenant_id = pbg.tenant_id
            JOIN {DB_SCHEMA}.po_bucket_grp pobg
              ON pobg.bucket_type_group_id = pbg.bucket_type_group_id
             AND pobg.tenant_id = pbg.tenant_id
            WHERE pbg.pop_id = %s
              AND pbg.usage_spec_id::text = %s
              AND UPPER(TRIM(pbg.status)) = 'ACTIVE'
              AND pobg.product_offering_id::text = %s
            ORDER BY pbg.bucket_type_group_id
            """,
            (r['pop_id'],str(u),r['product_offering_id'])
        )
        rows=cur.fetchall()

    if not rows:
        raise BucketProcessingError(
            'POP_BUCKET_GROUP_MAPPING_NOT_FOUND',
            (
                f"No active exact bucket-group mapping for "
                f"POP_ID={r['pop_id']} USAGE_SPEC_ID={u} "
                f"PRODUCT_OFFERING_ID={r['product_offering_id']}"
            )
        )

    if len(rows)>1:
        raise BucketProcessingError(
            'MULTIPLE_POP_BUCKET_GROUP_MAPPING_FOUND',
            (
                f"Multiple exact bucket groups found for POP_ID={r['pop_id']} "
                f"USAGE_SPEC_ID={u} PRODUCT_OFFERING_ID={r['product_offering_id']}: "
                f"{[(x['bucket_type_group_id'],x['tenant_id']) for x in rows]}"
            )
        )

    logger.info(
        'REQUEST | %s | STEP3 SUCCESS | POP=%s usage_spec=%s product=%s exact_group=%s tenant=%s',
        r['request_id'],r['pop_id'],u,r['product_offering_id'],
        rows[0]['bucket_type_group_id'],rows[0]['tenant_id']
    )

    return rows

def validate_po_mapping(conn,r,g,tenant_id):
    logger.info('REQUEST | %s | STEP4 PO_BUCKET_GRP product=%s group=%s tenant=%s',r['request_id'],r['product_offering_id'],g,tenant_id)
    with conn.cursor() as cur:
        cur.execute(f"""SELECT product_characteristic_value_id,product_characteristic_value,product_offering_id,bucket_type_group_id,tenant_id FROM {DB_SCHEMA}.po_bucket_grp WHERE product_offering_id::text=%s AND bucket_type_group_id=%s AND tenant_id=%s""",(r['product_offering_id'],g,tenant_id)); rows=cur.fetchall()
    if not rows: raise BucketProcessingError('PO_BUCKET_MAPPING_NOT_FOUND',f"No PO_BUCKET_GRP for product={r['product_offering_id']} group={g} tenant={tenant_id}")
    if len(rows)>1: raise BucketProcessingError('MULTIPLE_PO_BUCKET_MAPPING_FOUND',f"Multiple PO_BUCKET_GRP rows for product={r['product_offering_id']} group={g} tenant={tenant_id}")
    po_map=rows[0]
    logger.info('REQUEST | %s | STEP4 SUCCESS | char_id=%s value=%s',r['request_id'],po_map['product_characteristic_value_id'],po_map['product_characteristic_value'])
    return po_map

def get_bucket_definition(conn,r,g,tenant_id):
    logger.info('REQUEST | %s | STEP5 BUCKET_DEFINITION group=%s tenant=%s',r['request_id'],g,tenant_id)
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT
                bd.bucket_type_group_id,
                bd.bucket_usage_type,
                bd.bucket_unit AS bucket_uom_id,
                u.description AS bucket_uom_description,
                u.base_uom_id,
                u.conversion_factor,
                u.dimension,
                bd.bucket_initial_value,
                bd.validity_period_id,
                bd.status,
                bd.tenant_id
            FROM {DB_SCHEMA}.bucket_definition bd
            JOIN {DB_SCHEMA}.uom_ref u
              ON u.uom_id = bd.bucket_unit
             AND u.tenant_id = bd.tenant_id
            WHERE bd.bucket_type_group_id=%s
              AND bd.tenant_id=%s
              AND UPPER(TRIM(bd.status))='ACTIVE'
            """,
            (g,tenant_id)
        )
        rows=cur.fetchall()

    if not rows:
        raise BucketProcessingError(
            'BUCKET_DEFINITION_OR_UOM_NOT_FOUND',
            f'No active BUCKET_DEFINITION with valid UOM_REF for group={g} tenant={tenant_id}'
        )
    if len(rows)>1:
        raise BucketProcessingError(
            'MULTIPLE_BUCKET_DEFINITION_FOUND',
            f'Multiple BUCKET_DEFINITION rows for group={g} tenant={tenant_id}'
        )

    b=rows[0]
    logger.info(
        'REQUEST | %s | STEP5 SUCCESS | type=%s uom_id=%s uom=%s dimension=%s initial=%s validity_id=%s',
        r['request_id'],b['bucket_usage_type'],b['bucket_uom_id'],b['bucket_uom_description'],
        b['dimension'],b['bucket_initial_value'],b['validity_period_id']
    )
    return b

def get_validity(conn,r,vid):
    logger.info('REQUEST | %s | STEP6 BUCKET_VALIDITY_PERIOD id=%s',r['request_id'],vid)
    with conn.cursor() as cur:
        cur.execute(f"""SELECT bvp.validity_period_id,bvp.validity_period_name,bvp.validity_duration,vu.validity_unit_name,bvp.timeband_start,bvp.timeband_end FROM {DB_SCHEMA}.bucket_validity_period bvp JOIN {DB_SCHEMA}.validity_unit vu ON vu.validity_unit_id=bvp.validity_unit WHERE bvp.validity_period_id=%s AND bvp.status=1 AND CURRENT_TIMESTAMP>=bvp.valid_from AND (bvp.valid_to IS NULL OR CURRENT_TIMESTAMP<bvp.valid_to)""",(vid,)); v=cur.fetchone()
    if not v: raise BucketProcessingError('VALIDITY_PERIOD_NOT_FOUND',f'Validity period {vid} missing/inactive/outside dates')
    logger.info('REQUEST | %s | STEP6 SUCCESS | %s %s %s',r['request_id'],v['validity_period_name'],v['validity_duration'],v['validity_unit_name']); return v

def validate_trigger(conn,r,g,tenant_id):
    logger.info('REQUEST | %s | STEP7 trigger=%s group=%s tenant=%s',r['request_id'],r['bucket_trigger_type'],g,tenant_id)
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT bucket_trigger_id,bucket_trigger_type,bucket_type_group_id,tenant_id
            FROM {DB_SCHEMA}.bucket_triggers
            WHERE LOWER(TRIM(bucket_trigger_type))=LOWER(TRIM(%s))
              AND bucket_type_group_id=%s
              AND tenant_id=%s
            ORDER BY bucket_trigger_id
            """,
            (r['bucket_trigger_type'],g,tenant_id)
        )
        rows=cur.fetchall()

    if not rows:
        raise BucketProcessingError(
            'INVALID_TRIGGER',
            f"Trigger {r['bucket_trigger_type']} not mapped to group={g} tenant={tenant_id}"
        )
    if len(rows)>1:
        raise BucketProcessingError(
            'MULTIPLE_TRIGGER_MAPPING_FOUND',
            f"Multiple trigger rows for trigger={r['bucket_trigger_type']} group={g} tenant={tenant_id}"
        )

    trigger=rows[0]
    logger.info(
        'REQUEST | %s | STEP7 SUCCESS | trigger_id=%s trigger_type=%s',
        r['request_id'],trigger['bucket_trigger_id'],trigger['bucket_trigger_type']
    )
    return trigger

def get_rollover_config(conn,r,g,tenant_id,bucket_uom_id):
    logger.info(
        'REQUEST | %s | STEP8 BUCKET_ROLLOVER_CONFIG product=%s group=%s tenant=%s',
        r['request_id'],r['product_offering_id'],g,tenant_id
    )

    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT
                is_rollover_enabled,
                rollover_cap_value,
                rollover_cap_unit
            FROM {DB_SCHEMA}.bucket_rollover_config
            WHERE product_offering_id::text=%s
              AND bucket_type_group_id=%s
              AND tenant_id=%s
              AND UPPER(TRIM(status))='ACTIVE'
              AND valid_from<=CURRENT_TIMESTAMP
              AND (valid_to IS NULL OR valid_to>CURRENT_TIMESTAMP)
            ORDER BY valid_from DESC
            """,
            (r['product_offering_id'],g,tenant_id)
        )
        rows=cur.fetchall()

    if not rows:
        raise BucketProcessingError(
            'ROLLOVER_CONFIGURATION_NOT_FOUND',
            f"No active rollover configuration for product={r['product_offering_id']} group={g} tenant={tenant_id}"
        )

    if len(rows)>1:
        raise BucketProcessingError(
            'MULTIPLE_ROLLOVER_CONFIGURATION_FOUND',
            f"Multiple active rollover configurations for product={r['product_offering_id']} group={g} tenant={tenant_id}"
        )

    rc=rows[0]

    if rc['is_rollover_enabled']:
        if rc['rollover_cap_value'] is None or rc['rollover_cap_unit'] is None:
            raise BucketProcessingError(
                'INVALID_ROLLOVER_CONFIGURATION',
                f"Rollover enabled but cap value/unit missing for product={r['product_offering_id']} group={g}"
            )

        # BUCKET_DEFINITION/CUSTOMER_BUCKET now use UOM_REF.uom_id.
        # For backward compatibility the rollover table may still hold either
        # the numeric UOM id (recommended) or the description such as 'GB'.
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT uom_id,description
                FROM {DB_SCHEMA}.uom_ref
                WHERE tenant_id=%s
                  AND (uom_id::text=%s OR UPPER(TRIM(description))=UPPER(TRIM(%s)))
                ORDER BY CASE WHEN uom_id::text=%s THEN 0 ELSE 1 END
                LIMIT 1
                """,
                (tenant_id,str(rc['rollover_cap_unit']),str(rc['rollover_cap_unit']),str(rc['rollover_cap_unit']))
            )
            rollover_uom=cur.fetchone()

        if not rollover_uom:
            raise BucketProcessingError(
                'ROLLOVER_UOM_NOT_FOUND',
                f"Rollover cap unit={rc['rollover_cap_unit']} not found in UOM_REF for tenant={tenant_id}"
            )

        if int(rollover_uom['uom_id']) != int(bucket_uom_id):
            raise BucketProcessingError(
                'ROLLOVER_UNIT_MISMATCH',
                f"Bucket UOM_ID={bucket_uom_id}, rollover UOM_ID={rollover_uom['uom_id']} ({rollover_uom['description']})"
            )

    logger.info(
        'REQUEST | %s | STEP8 SUCCESS | enabled=%s cap=%s unit=%s',
        r['request_id'],rc['is_rollover_enabled'],rc['rollover_cap_value'],rc['rollover_cap_unit']
    )

    return rc


def find_existing(conn,r,g):
    logger.info(
        'REQUEST | %s | PRECHECK BUCKET checking BAN + PRODUCT + BUCKET_GROUP',
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
            WHERE customer_ban::text = %s
              AND product_offering_id::text = %s
              AND bucket_usage_group::text = %s
              AND (
                    valid_end_time IS NULL
                    OR valid_end_time > CURRENT_TIMESTAMP
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
        x = cur.fetchone()

    if x:
        logger.warning(
            'REQUEST | %s | PRECHECK BUCKET DUPLICATE | existing_bucket_id=%s existing_order_id=%s status=%s',
            r['request_id'],
            x['bucket_id'],
            x['order_id'],
            x['status']
        )
    else:
        logger.info(
            'REQUEST | %s | PRECHECK BUCKET SUCCESS | no existing valid bucket',
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

def generate_bucket_id(conn):
    """
    Generate CUSTOMER_BUCKET.BUCKET_ID from PostgreSQL sequence.
    Example: 1000, 1001, 1002...
    """
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT nextval('{BUCKET_ID_SEQUENCE}') AS bucket_id"
        )
        row=cur.fetchone()

    if not row or row['bucket_id'] is None:
        raise BucketProcessingError(
            'BUCKET_ID_GENERATION_FAILED',
            f'Unable to generate bucket_id from sequence {BUCKET_ID_SEQUENCE}'
        )

    return str(row['bucket_id'])


def update_bucket_request_resolution(conn,r,usage_spec_id,g,po_map,b,trigger,rollover):
    """
    Persist the resolved configuration snapshot in BUCKET_REQUEST.

    BUCKET_REQUEST remains the engine audit/processing table, while these
    fields make it possible to trace how POP/Usage/Group/Product mapping
    resolved before the final CUSTOMER_BUCKET was created.
    """
    with conn.cursor() as cur:
        cur.execute(
            f"""
            UPDATE {DB_SCHEMA}.bucket_request
            SET usage_spec_id=%s,
                bucket_type_group_id=%s,
                product_characteristic_value_id=%s,
                bucket_usage_type=%s,
                bucket_size=%s,
                bucket_unit=%s,
                bucket_trigger_id=%s,
                is_rollover_enabled=%s,
                rollover_cap_value=%s,
                rollover_cap_unit=%s,
                updated_dt=CURRENT_TIMESTAMP
            WHERE request_id=%s
            """,
            (
                usage_spec_id,
                g,
                po_map['product_characteristic_value_id'],
                b['bucket_usage_type'],
                b['bucket_initial_value'],
                b['bucket_uom_id'],
                trigger['bucket_trigger_id'],
                rollover['is_rollover_enabled'],
                rollover['rollover_cap_value'],
                rollover['rollover_cap_unit'],
                r['request_id']
            )
        )

    logger.info(
        'REQUEST | %s | AUDIT SNAPSHOT | usage_spec=%s group=%s char_id=%s type=%s size=%s uom_id=%s trigger_id=%s rollover=%s cap=%s cap_unit=%s',
        r['request_id'],usage_spec_id,g,po_map['product_characteristic_value_id'],
        b['bucket_usage_type'],b['bucket_initial_value'],b['bucket_uom_id'],trigger['bucket_trigger_id'],
        rollover['is_rollover_enabled'],rollover['rollover_cap_value'],rollover['rollover_cap_unit']
    )


def insert_bucket(conn,r,g,tenant_id,b,v,po_map,trigger,rollover):
    logger.info('REQUEST | %s | STEP9 prepare CUSTOMER_BUCKET',r['request_id'])
    bid=generate_bucket_id(conn)
    start=datetime.now()
    end=calc_end(start,v['validity_duration'],v['validity_unit_name'])
    initial=b['bucket_initial_value']

    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO {DB_SCHEMA}.customer_bucket
            (
                bucket_id,
                customer_ban,
                tenant_id,
                status,
                bucket_usage_type,
                bucket_validity_period,
                bucket_usage_group,
                bucket_trigger_type,
                bucket_trigger_id,
                order_id,
                product_offering_id,
                pop_id,
                product_spec_char_value_use_id,
                is_rollover_enabled,
                rollover_cap_value,
                rollover_cap_unit,
                timeband_start,
                timeband_end,
                valid_start_time,
                valid_end_time,
                created_at,
                last_updated_at,
                bucket_unit,
                bucket_initial_value,
                bucket_remaining_value
            )
            VALUES
            (
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                %s,%s,%s,
                %s,%s,%s,%s,
                CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,
                %s,%s,%s
            )
            """,
            (
                bid,
                r['customer_ban'],
                tenant_id,
                CUSTOMER_BUCKET_ACTIVE_STATUS,
                b['bucket_usage_type'],
                v['validity_period_id'],
                str(g),
                trigger['bucket_trigger_type'],
                trigger['bucket_trigger_id'],
                r['order_id'],
                r['product_offering_id'],
                str(r['pop_id']),
                po_map['product_characteristic_value_id'],
                rollover['is_rollover_enabled'],
                rollover['rollover_cap_value'],
                rollover['rollover_cap_unit'],
                v['timeband_start'],
                v['timeband_end'],
                start,
                end,
                b['bucket_uom_id'],
                initial,
                initial
            )
        )

    logger.info(
        'REQUEST | %s | STEP9 SUCCESS | bucket_id=%s status=%s type=%s trigger_id=%s uom_id=%s uom=%s initial=%s remaining=%s rollover_enabled=%s rollover_cap=%s rollover_unit=%s',
        r['request_id'],bid,CUSTOMER_BUCKET_ACTIVE_STATUS,b['bucket_usage_type'],trigger['bucket_trigger_id'],b['bucket_uom_id'],b['bucket_uom_description'],initial,initial,
        rollover['is_rollover_enabled'],rollover['rollover_cap_value'],rollover['rollover_cap_unit']
    )
    return bid

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

    # PRECHECK 1:
    # Same ORDER_ID already processed successfully?
    duplicate_request = find_processed_request_duplicate(conn,r)

    if duplicate_request:
        raise BucketProcessingError(
            'DUPLICATE_ORDER_ALREADY_PROCESSED',
            (
                f"ORDER_ID={r['order_id']} has already been processed successfully. "
                f"EXISTING_REQUEST_ID={duplicate_request['request_id']}, "
                f"EXISTING_CUSTOMER_BAN={duplicate_request['customer_ban']}, "
                f"EXISTING_SUBSCR_ID={duplicate_request['subscr_id']}, "
                f"EXISTING_BUCKET_ID={duplicate_request['customer_bucket_id']}"
            ),
            existing_bucket_id=duplicate_request['customer_bucket_id']
        )

    # STEP 1: Validate subscriber
    validate_customer(conn,r)

    # STEP 2: POP -> Usage Specification
    usage_spec_id = get_usage_spec(conn,r)

    # STEP 3: Usage Specification -> Bucket Group(s)
    groups = get_groups(conn,r,usage_spec_id)

    bids=[]

    for row in groups:
        g=row['bucket_type_group_id']
        tenant_id=row['tenant_id']

        # PRECHECK 2:
        # Once bucket group is known, immediately reject an already-created
        # valid bucket for the same BAN + Product Offering + Bucket Group.
        existing = find_existing(conn,r,g)

        if existing:
            raise BucketProcessingError(
                'BUCKET_ALREADY_EXISTS',
                (
                    f"Bucket already exists for "
                    f"CUSTOMER_BAN={r['customer_ban']}, "
                    f"PRODUCT_OFFERING_ID={r['product_offering_id']}, "
                    f"BUCKET_GROUP={g}, "
                    f"EXISTING_ORDER_ID={existing['order_id']}, "
                    f"EXISTING_BUCKET_ID={existing['bucket_id']}"
                ),
                existing_bucket_id=existing['bucket_id']
            )

        # Only continue configuration lookup if no existing bucket is found.
        po_map=validate_po_mapping(conn,r,g,tenant_id)
        b=get_bucket_definition(conn,r,g,tenant_id)
        v=get_validity(conn,r,b['validity_period_id'])
        trigger=validate_trigger(conn,r,g,tenant_id)
        rollover=get_rollover_config(conn,r,g,tenant_id,b['bucket_uom_id'])

        # Persist engine resolution in BUCKET_REQUEST for easy backtracking.
        update_bucket_request_resolution(
            conn,r,usage_spec_id,g,po_map,b,trigger,rollover
        )

        bids.append(
            insert_bucket(conn,r,g,tenant_id,b,v,po_map,trigger,rollover)
        )

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
    logger.info('Python=%s', sys.version.replace('\n',' '))
    logger.info('Script path=%s | Log path=%s', SCRIPT_DIR, LOG_FILE)
    logger.info(
        'DB host=%s port=%s db=%s user=%s schema=%s',
        DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_SCHEMA
    )

    inbound_received = 0
    inbound_submitted = 0
    inbound_rejected = 0
    inbound_synced = 0
    total = 0
    success = 0
    failed = 0
    runtime_errors = 0

    try:
        validate_database_structure()
    except Exception:
        logger.exception('STARTUP FAILED')
        runtime_errors = 1
        print("")
        print("============================================================")
        print("BUCKET ENGINE V1 - RUN SUMMARY")
        print("============================================================")
        print("Inbound Received : 0")
        print("Inbound Submitted: 0")
        print("Inbound Rejected : 0")
        print("Inbound Synced   : 0")
        print("Total Requests   : 0")
        print("Success Count    : 0")
        print("Failed Count     : 0")
        print("Runtime Errors   : 1")
        print("============================================================")
        print("")
        print("Bucket Engine V1 completed with errors")
        print(f"Log File         : {LOG_FILE}")
        sys.exit(1)

    try:
        inbound_received, inbound_submitted, inbound_rejected = process_inbound_requests()
    except Exception:
        logger.exception('STAGE1 INBOUND PROCESSING ERROR')
        runtime_errors += 1

    try:
        total, success, failed = process_batch()
    except Exception:
        logger.exception('STAGE2 BUCKET PROCESSING ERROR')
        runtime_errors += 1

    try:
        inbound_synced = sync_inbound_results()
    except Exception:
        logger.exception('STAGE3 REVERSE SYNC ERROR')
        runtime_errors += 1

    logger.info(
        'RUN SUMMARY | inbound_received=%s | inbound_submitted=%s | '
        'inbound_rejected=%s | total=%s | '
        'success=%s | failed=%s | runtime_errors=%s',
        inbound_received, inbound_submitted, inbound_rejected,
        total, success, failed, runtime_errors
    )

    print("")
    print("============================================================")
    print("BUCKET ENGINE V1 - RUN SUMMARY")
    print("============================================================")
    print(f"Inbound Received : {inbound_received}")
    print(f"Inbound Submitted: {inbound_submitted}")
    print(f"Inbound Rejected : {inbound_rejected}")
    print(f"Total Requests   : {total}")
    print(f"Success Count    : {success}")
    print(f"Failed Count     : {failed}")
    print(f"Runtime Errors   : {runtime_errors}")
    print("============================================================")
    print("")

    overall_errors = inbound_rejected + failed + runtime_errors

    if inbound_received == 0 and total == 0 and runtime_errors == 0:
        logger.info('Bucket Engine V1 completed - No pending requests')
        print("Bucket Engine V1 completed - No pending requests")
    elif overall_errors == 0:
        logger.info('Bucket Engine V1 completed successfully')
        print("Bucket Engine V1 completed successfully")
    elif success > 0:
        logger.warning(
            'Bucket Engine V1 partially completed | success=%s failed=%s runtime_errors=%s',
            success, failed, runtime_errors
        )
        print("Bucket Engine V1 partially completed")
    else:
        logger.error(
            'Bucket Engine V1 completed with errors | failed=%s runtime_errors=%s',
            failed, runtime_errors
        )
        print("Bucket Engine V1 completed with errors")

    print(f"Log File         : {LOG_FILE}")

    if runtime_errors > 0:
        sys.exit(1)


if __name__=='__main__': main()
