"""
PRODUCTION INVENTORY SERVER — Standalone (v2 Clean Rebuild)
============================================================
Tracks inventory based on production data, not warehouse counts.

Architecture:
  - ATS incoming column → displayed as "incoming" (pass-through, no aggregation)
  - Style ledger → arrival detection only (when POs fall off = arrived)
  - Arrived stock → locked-in production units from snapshot (what factory shipped)
  - Committed/Allocated → from ATS sheet (pass-through)
  - Shipped ledger → tracks cumulative fulfillment to prevent sold-out rebound

The style ledger NEVER contributes to displayed incoming numbers.
It only tells us WHEN something arrives and HOW MANY units the factory claimed.
"""

try:
    from gevent import monkey
    monkey.patch_all()
except ImportError:
    pass

import os, re, json, time, threading
from datetime import datetime, timedelta
from io import BytesIO
import boto3
from flask import Flask, request, jsonify
from flask_cors import CORS
import requests as http_requests
import openpyxl

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*", "methods": ["GET","POST","OPTIONS"], "allow_headers": ["Content-Type"]}})

# ── CONFIG ──
AWS_REGION = os.environ.get('AWS_REGION', 'us-east-2')
S3_BUCKET = os.environ.get('S3_BUCKET', 'nauticaslimfit')
S3_SNAPSHOT_KEY = 'production-inventory/snapshot.json'
S3_SHIPPED_KEY = 'production-inventory/shipped_ledger.json'
S3_ARRIVALS_KEY = 'production-inventory/arrivals_log.json'
S3_MANUAL_ALLOC_KEY = 'production-inventory/manual_allocations.json'

DROPBOX_APP_KEY = os.environ.get('DROPBOX_APP_KEY', '')
DROPBOX_APP_SECRET = os.environ.get('DROPBOX_APP_SECRET', '')
DROPBOX_REFRESH_TOKEN = os.environ.get('DROPBOX_REFRESH_TOKEN', '')
DROPBOX_LEDGER_FOLDER = os.environ.get('DROPBOX_LEDGER_FOLDER', '/Versa Share Files/David - Dropbox/Style Ledger')
DROPBOX_ATS_PATH = os.environ.get('DROPBOX_ATS_PATH', '/Versa Share Files/Hourly ATS/Inventory_ATS.xlsx')
DROPBOX_ARCHIVE_PATH = os.environ.get('DROPBOX_ARCHIVE_PATH', '/Versa Share Files/Hourly ATS/ATS_Archive')

ORDERS_API_URL = os.environ.get('ORDERS_API_URL', 'https://open-orders-api.onrender.com')
MAIN_API_URL = os.environ.get('MAIN_API_URL', 'https://versa-inventory-api.onrender.com')
MISS_THRESHOLD = int(os.environ.get('MISS_THRESHOLD', '3'))
SYNC_INTERVAL = int(os.environ.get('SYNC_INTERVAL', '3600'))
ARCHIVE_COMPARE_HOURS = int(os.environ.get('ARCHIVE_COMPARE_HOURS', '24'))

BRAND_CODE_MAP = {'US':'USPA','GB':'BEENE','VC':'VINCE','VD':'VD','BEN':'BEN','CHAPS':'CHAPS','DKNY':'DKNY','EB':'EB','JNY':'JNY','KLP':'KLP','LUCKY':'LUCKY','NAUTICA':'NAUTICA','NE':'NE','RB':'RB','RG':'RG','SHAQ':'SHAQ'}
BRAND_FULL_NAMES = {'NAUTICA':'Nautica','DKNY':'DKNY','EB':'Eddie Bauer','VINCE':'Vince Camuto','BEN':'Ben Sherman','USPA':'U.S. Polo Assn.','CHAPS':'Chaps','LUCKY':'Lucky Brand','JNY':'Jones New York','BEENE':'Geoffrey Beene','SHAQ':"Shaquille O'Neal",'VD':'Von Dutch','KLP':'Karl Lagerfeld Paris','NE':'Neiman Marcus','RB':'Reebok','RG':'Robert Graham'}

# ── DROPBOX AUTH ──
_dbx_token = ''
_dbx_token_expires = 0
def get_dropbox_token():
    global _dbx_token, _dbx_token_expires
    if not DROPBOX_REFRESH_TOKEN or not DROPBOX_APP_KEY: return ''
    if time.time() < _dbx_token_expires - 300: return _dbx_token
    try:
        resp = http_requests.post('https://api.dropbox.com/oauth2/token', data={'grant_type':'refresh_token','refresh_token':DROPBOX_REFRESH_TOKEN,'client_id':DROPBOX_APP_KEY,'client_secret':DROPBOX_APP_SECRET}, timeout=15)
        if resp.status_code == 200:
            data = resp.json(); _dbx_token = data['access_token']; _dbx_token_expires = time.time() + data.get('expires_in', 14400)
            print(f"[Dropbox] Token refreshed", flush=True); return _dbx_token
    except Exception as e: print(f"[Dropbox] Error: {e}", flush=True)
    return _dbx_token

# ── S3 ──
_s3 = None
def get_s3():
    global _s3
    if _s3 is None: _s3 = boto3.client('s3', region_name=AWS_REGION)
    return _s3

def s3_get_json(key, default=None):
    try:
        resp = get_s3().get_object(Bucket=S3_BUCKET, Key=key)
        return json.loads(resp['Body'].read().decode('utf-8'))
    except Exception as e:
        if 'NoSuchKey' in str(e): return default
        raise

def s3_put_json(key, data):
    get_s3().put_object(Bucket=S3_BUCKET, Key=key, Body=json.dumps(data).encode('utf-8'), ContentType='application/json')

# ── DROPBOX HELPERS ──
def _dbx_download(path):
    token = get_dropbox_token()
    if not token: return None
    resp = http_requests.post('https://content.dropboxapi.com/2/files/download',
        headers={'Authorization':f'Bearer {token}','Dropbox-API-Arg':json.dumps({'path':path})}, timeout=60)
    if resp.status_code == 401:
        global _dbx_token_expires; _dbx_token_expires = 0; token = get_dropbox_token()
        resp = http_requests.post('https://content.dropboxapi.com/2/files/download',
            headers={'Authorization':f'Bearer {token}','Dropbox-API-Arg':json.dumps({'path':path})}, timeout=60)
    return resp.content if resp.status_code == 200 else None

# ══════════════════════════════════════════════════
#  STYLE LEDGER — For arrival detection ONLY
#  NOT used for incoming quantities
# ══════════════════════════════════════════════════
def fetch_style_ledger():
    token = get_dropbox_token()
    if not token: return []
    try:
        resp = http_requests.post('https://api.dropboxapi.com/2/files/list_folder',
            headers={'Authorization':f'Bearer {token}','Content-Type':'application/json'},
            json={'path':DROPBOX_LEDGER_FOLDER,'recursive':False,'limit':50}, timeout=20)
        if resp.status_code != 200: return []
        entries = resp.json().get('entries', [])
        xlsx = [e for e in entries if e.get('.tag')=='file' and e['name'].lower().endswith('.xlsx')]
        if not xlsx: return []
        xlsx.sort(key=lambda e: (0 if 'ledger' in e['name'].lower() else 1, e['name']))
        chosen = xlsx[0]; print(f"  Downloading: {chosen['name']}")
        data = _dbx_download(chosen['path_display'])
        if not data: return []
        wb = openpyxl.load_workbook(BytesIO(data), read_only=False, data_only=True)
        ws = wb[wb.sheetnames[0]]; results = []
        for row in ws.iter_rows(min_row=2, max_col=6, values_only=True):
            style = str(row[2] or '').strip().upper()
            if not style: continue
            etd = None
            if row[5]: etd = row[5].strftime('%Y-%m-%d') if isinstance(row[5], datetime) else str(row[5])
            try: units = int(row[3] or 0)
            except: units = 0
            results.append({'production':str(row[0] or '').strip(),'poName':str(row[1] or '').strip(),'style':style,'units':units,'brand':str(row[4] or '').strip().upper(),'etd':etd})
        wb.close(); print(f"  {len(results)} production rows"); return results
    except Exception as e: print(f"  Ledger error: {e}"); return []

# ══════════════════════════════════════════════════
#  ATS SHEET — Source of truth for incoming/committed/allocated
# ══════════════════════════════════════════════════
def fetch_ats_full():
    data = _dbx_download(DROPBOX_ATS_PATH)
    if not data: return []
    try:
        wb = openpyxl.load_workbook(BytesIO(data), read_only=True, data_only=True)
        ws = wb[wb.sheetnames[0]]; items = []; first = True
        for row in ws.iter_rows(values_only=True):
            if first: first = False; continue
            if not row[0]: continue
            sku = str(row[0]).strip().upper(); brand = str(row[1] or '').strip().upper()
            incoming = int(row[5] or 0) if len(row)>5 else 0
            jtw = int(row[6] or 0) if len(row)>6 else 0; tr = int(row[7] or 0) if len(row)>7 else 0
            dcw = int(row[8] or 0) if len(row)>8 else 0; qa = int(row[9] or 0) if len(row)>9 else 0
            committed = int(row[10] or 0) if len(row)>10 else 0; allocated = int(row[11] or 0) if len(row)>11 else 0
            items.append({'sku':sku,'brand':brand,'incoming':incoming,'jtw':jtw,'tr':tr,'dcw':dcw,'qa':qa,'warehouse':jtw+tr+dcw+qa,'committed':committed,'allocated':allocated})
        wb.close(); print(f"  {len(items)} ATS items"); return items
    except Exception as e: print(f"  ATS error: {e}"); return []

# ── ARCHIVE INDEX ──
_archive_index = {'files':[],'last_refresh':0}
_archive_lock = threading.Lock()
def refresh_archive_index():
    with _archive_lock:
        if _archive_index['files'] and (time.time() - _archive_index['last_refresh'] < 21600): return _archive_index['files']
    token = get_dropbox_token()
    if not token: return []
    try:
        all_entries = []; headers = {'Authorization':f'Bearer {token}','Content-Type':'application/json'}
        resp = http_requests.post('https://api.dropboxapi.com/2/files/list_folder', headers=headers, json={'path':DROPBOX_ARCHIVE_PATH,'recursive':False,'limit':2000}, timeout=30)
        if resp.status_code != 200: return []
        data = resp.json(); all_entries.extend(data.get('entries',[])) 
        while data.get('has_more') and data.get('cursor'):
            resp = http_requests.post('https://api.dropboxapi.com/2/files/list_folder/continue', headers=headers, json={'cursor':data['cursor']}, timeout=30)
            if resp.status_code != 200: break
            data = resp.json(); all_entries.extend(data.get('entries',[]))
        files = []
        for e in all_entries:
            if e.get('.tag')!='file' or not e['name'].lower().endswith('.xlsx'): continue
            m = re.search(r'(\d{4}-\d{2}-\d{2})_(\d{6})', e['name'])
            if m:
                try: dt = datetime.strptime(f"{m.group(1)}_{m.group(2)}", '%Y-%m-%d_%H%M%S'); files.append({'name':e['name'],'path':e.get('path_display',''),'dt':dt})
                except: pass
        files.sort(key=lambda f: f['dt'], reverse=True)
        with _archive_lock: _archive_index['files'] = files; _archive_index['last_refresh'] = time.time()
        print(f"  Archive: {len(files)} files"); return files
    except Exception as e: print(f"  Archive error: {e}"); return []

def fetch_archive_snapshot(path):
    data = _dbx_download(path)
    if not data: return None
    try:
        wb = openpyxl.load_workbook(BytesIO(data), read_only=True, data_only=True)
        ws = wb[wb.sheetnames[0]]; result = {}; first = True
        for row in ws.iter_rows(values_only=True):
            if first: first = False; continue
            if not row[0]: continue
            sku = str(row[0]).strip().upper()
            result[sku] = {'incoming':int(row[5] or 0) if len(row)>5 else 0, 'warehouse':sum(int(row[i] or 0) for i in range(6,10) if len(row)>i)}
        wb.close(); return result
    except: return None

# ══════════════════════════════════════════════════
#  SNAPSHOT — PO lifecycle tracking
#  Key: production::style (aggregated — duplicates summed)
# ══════════════════════════════════════════════════
def load_snapshot(): return s3_get_json(S3_SNAPSHOT_KEY, {'last_check':None,'orders':{}})
def save_snapshot(s): s['last_check'] = datetime.utcnow().isoformat()+'Z'; s3_put_json(S3_SNAPSHOT_KEY, s)

# ══════════════════════════════════════════════════
#  DIFF ENGINE — Detects arrivals
# ══════════════════════════════════════════════════
def run_diff(ledger_rows, archive_data=None, ats_items=None):
    snapshot = load_snapshot(); now = datetime.utcnow().isoformat()+'Z'
    # Aggregate ledger by production::style
    current = {}
    for row in ledger_rows:
        prod = str(row.get('production','')).strip(); style = str(row.get('style','')).strip().upper()
        if not prod or not style: continue
        key = f"{prod}::{style}"; brand_code = str(row.get('brand','')).strip().upper()
        units = int(row.get('units',0) or 0); etd = row.get('etd')
        if key in current: current[key]['units'] += units
        else: current[key] = {'production':prod,'style':style,'units':units,'brand_code':brand_code,'brand':BRAND_CODE_MAP.get(brand_code,brand_code),'etd':etd,'poName':str(row.get('poName','')).strip()}
    current_keys = set(current.keys())
    changes = {'new':0,'updated':0,'arrived':0,'arrived_confirmed':0,'arrived_denied':0,'miss_inc':0,'incoming':0,'in_stock':0}
    ats_lookup = {i['sku']:i for i in (ats_items or [])}
    # Step 1: Process current
    for key, d in current.items():
        if key in snapshot['orders']:
            ex = snapshot['orders'][key]; ex['last_seen'] = now; ex['consecutive_misses'] = 0
            if ex.get('status') == 'in_stock': ex['status'] = 'incoming'; ex['fell_off_date'] = None
            if ex['units'] != d['units']: ex['units'] = d['units']; changes['updated'] += 1
            ex['etd'] = d['etd']; changes['incoming'] += 1
        else:
            snapshot['orders'][key] = {'production':d['production'],'po_name':d['poName'],'style':d['style'],'units':d['units'],'brand_code':d['brand_code'],'brand':d['brand'],'etd':d['etd'],'status':'incoming','first_seen':now,'last_seen':now,'fell_off_date':None,'consecutive_misses':0}
            changes['new'] += 1
    # Step 2: Detect arrivals
    for key, order in snapshot['orders'].items():
        if order.get('status') != 'incoming': changes['in_stock'] += 1; continue
        if key not in current_keys:
            misses = order.get('consecutive_misses',0) + 1; order['consecutive_misses'] = misses
            if misses >= MISS_THRESHOLD:
                should_flip = True; sku = order.get('style','').upper()
                if archive_data and sku in ats_lookup:
                    old = archive_data.get(sku); cur = ats_lookup.get(sku,{})
                    if old and old.get('incoming',0) > 0 and cur.get('incoming',0) > 0:
                        should_flip = False; changes['arrived_denied'] += 1
                    elif old and old.get('incoming',0) > 0 and cur.get('incoming',0) == 0:
                        changes['arrived_confirmed'] += 1
                if should_flip:
                    order['status'] = 'in_stock'; order['fell_off_date'] = now; changes['arrived'] += 1
                    print(f"  ARRIVED: {key} — {order.get('brand')} {order.get('units',0):,} units")
            else: changes['miss_inc'] += 1
    save_snapshot(snapshot); return changes, snapshot

# ── SHIPPED LEDGER ──
def load_shipped(): return s3_get_json(S3_SHIPPED_KEY, {'skus':{}})
def save_shipped(l): l['updated'] = datetime.utcnow().isoformat()+'Z'; s3_put_json(S3_SHIPPED_KEY, l)
def update_shipped(ats_items, arrived_by_sku):
    ledger = load_shipped(); ats_lookup = {i['sku']:i for i in ats_items}; updated = 0
    for sku, arrived in arrived_by_sku.items():
        if arrived <= 0: continue
        ats = ats_lookup.get(sku, {}); cc = abs(ats.get('committed',0) or 0); ca = abs(ats.get('allocated',0) or 0)
        rec = ledger['skus'].get(sku, {'shipped':0,'peak_c':0,'peak_a':0})
        np_c = max(rec.get('peak_c',0), cc); np_a = max(rec.get('peak_a',0), ca)
        total_from_peaks = (np_c - cc) + (np_a - ca)
        new_shipped = min(arrived, max(rec.get('shipped',0), total_from_peaks))
        if new_shipped != rec.get('shipped',0): updated += 1
        rec['shipped'] = new_shipped; rec['peak_c'] = np_c; rec['peak_a'] = np_a; rec['arrived'] = arrived
        ledger['skus'][sku] = rec
    save_shipped(ledger)
    if updated: print(f"  Shipped: {updated} SKUs")
    return ledger

# ══════════════════════════════════════════════════
#  INVENTORY BUILDER
#  incoming = ATS incoming (pass-through)
#  warehouse = arrived production units - shipped
# ══════════════════════════════════════════════════
def build_inventory(snapshot, ats_items, shipped_ledger):
    arrived_by_sku = {}
    for order in snapshot.get('orders',{}).values():
        if order.get('status') == 'in_stock':
            sku = order.get('style','').upper()
            arrived_by_sku[sku] = arrived_by_sku.get(sku, 0) + order.get('units', 0)
    shipped_lookup = shipped_ledger.get('skus', {})
    inventory = []; seen = set()
    for item in ats_items:
        sku = item['sku']; brand = item['brand']; seen.add(sku)
        incoming = item.get('incoming', 0)  # STRAIGHT FROM ATS
        arrived_raw = arrived_by_sku.get(sku, 0)
        shipped = shipped_lookup.get(sku, {}).get('shipped', 0)
        arrived = max(0, arrived_raw - shipped)
        committed = item.get('committed', 0); allocated = item.get('allocated', 0)
        if incoming == 0 and arrived == 0: continue
        inventory.append({'sku':sku,'brand':brand,'brand_abbr':brand,'brand_full':BRAND_FULL_NAMES.get(brand,brand),'name':f"{brand} {sku}",'jtw':arrived,'tr':0,'dcw':0,'qa':0,'incoming':incoming,'committed':committed,'allocated':allocated,'total_ats':arrived+incoming-abs(committed)-abs(allocated),'total_warehouse':arrived,'container':'','receive_date':'','lot_number':'','image':''})
    # Arrived SKUs not on ATS anymore
    for sku, raw in arrived_by_sku.items():
        if sku in seen: continue
        shipped = shipped_lookup.get(sku,{}).get('shipped',0); arrived = max(0, raw - shipped)
        if arrived <= 0: continue
        brand = ''
        for o in snapshot.get('orders',{}).values():
            if o.get('style','').upper() == sku and o.get('status')=='in_stock': brand = o.get('brand',''); break
        inventory.append({'sku':sku,'brand':brand,'brand_abbr':brand,'brand_full':BRAND_FULL_NAMES.get(brand,brand),'name':f"{brand} {sku}",'jtw':arrived,'tr':0,'dcw':0,'qa':0,'incoming':0,'committed':0,'allocated':0,'total_ats':arrived,'total_warehouse':arrived,'container':'','receive_date':'','lot_number':'','image':''})
    return inventory

# ══════════════════════════════════════════════════
#  SYNC ENGINE
# ══════════════════════════════════════════════════
_cache = {'inventory':[],'snapshot':None,'shipped':None,'last_sync':None,'syncing':False}
_lock = threading.Lock()
def do_sync():
    with _lock:
        if _cache['syncing']: return None
        _cache['syncing'] = True
    start = time.time()
    try:
        print("\n══════ PRODUCTION SYNC ══════\n")
        print("  Fetching style ledger..."); ledger = fetch_style_ledger()
        if not ledger: print("  No ledger"); 
        print("  Fetching ATS..."); ats = fetch_ats_full()
        archive_data = None; archive_name = None
        try:
            files = refresh_archive_index()
            if files:
                target = datetime.utcnow() - timedelta(hours=ARCHIVE_COMPARE_HOURS)
                best = min(files, key=lambda f: abs((f['dt']-target).total_seconds()))
                print(f"  Archive: {best['name']}"); archive_data = fetch_archive_snapshot(best['path']); archive_name = best['name']
        except Exception as e: print(f"  Archive skip: {e}")
        if not ledger:
            with _lock: _cache['syncing'] = False
            return None
        print("  Running diff..."); changes, snapshot = run_diff(ledger, archive_data, ats)
        print("  Updating shipped..."); arrived_by_sku = {}
        for o in snapshot.get('orders',{}).values():
            if o.get('status')=='in_stock': sku=o.get('style','').upper(); arrived_by_sku[sku]=arrived_by_sku.get(sku,0)+o.get('units',0)
        shipped = update_shipped(ats, arrived_by_sku)
        print("  Building inventory..."); inventory = build_inventory(snapshot, ats, shipped)
        # Validate
        our_inc = sum(i['incoming'] for i in inventory); ats_inc = sum(i['incoming'] for i in ats if i['incoming']>0)
        if our_inc == ats_inc: print(f"  Incoming validated: {our_inc:,}")
        else: print(f"  MISMATCH: ours={our_inc:,} ats={ats_inc:,}")
        with _lock: _cache.update({'inventory':inventory,'snapshot':snapshot,'shipped':shipped,'last_sync':datetime.utcnow().isoformat()+'Z','syncing':False})
        elapsed = time.time()-start
        print(f"\n  DONE ({elapsed:.1f}s) — {len(inventory)} SKUs | New:{changes['new']} Arrived:{changes['arrived']} Incoming:{changes['incoming']} InStock:{changes['in_stock']}")
        return changes
    except Exception as e:
        import traceback; print(f"  FAILED: {e}"); traceback.print_exc()
        with _lock: _cache['syncing'] = False
        return None

# ══════════════════════════════════════════════════
#  ROUTES
# ══════════════════════════════════════════════════
@app.route('/', methods=['GET'])
def health():
    with _lock: return jsonify({'status':'ok','service':'Production Inventory API','inventory_count':len(_cache['inventory']),'last_sync':_cache['last_sync']})

@app.route('/production-inventory', methods=['GET','OPTIONS'])
def r_inv():
    if request.method=='OPTIONS': return '',204
    with _lock: inv=list(_cache['inventory']); last=_cache['last_sync']
    if not inv: do_sync()
    with _lock: inv=list(_cache['inventory']); last=_cache['last_sync']
    return jsonify({'status':'ok','inventory':inv,'item_count':len(inv),'last_sync':last})

@app.route('/production-sync', methods=['GET','POST','OPTIONS'])
def r_sync():
    if request.method=='OPTIONS': return '',204
    changes = do_sync()
    with _lock: return jsonify({'status':'ok' if changes else 'error','inventory':_cache['inventory'],'item_count':len(_cache['inventory']),'last_sync':_cache['last_sync'],'changes':changes})

@app.route('/production-snapshot', methods=['GET','OPTIONS'])
def r_snap():
    if request.method=='OPTIONS': return '',204
    s = load_snapshot(); orders = list(s.get('orders',{}).values())
    inc = [o for o in orders if o.get('status')=='incoming']; stk = [o for o in orders if o.get('status')=='in_stock']
    return jsonify({'total':len(orders),'incoming_count':len(inc),'incoming_units':sum(o.get('units',0) for o in inc),'in_stock_count':len(stk),'in_stock_units':sum(o.get('units',0) for o in stk),'orders':s.get('orders',{})})

@app.route('/production-status', methods=['GET','OPTIONS'])
def r_status():
    if request.method=='OPTIONS': return '',204
    with _lock: return jsonify({'status':'ok','last_sync':_cache['last_sync'],'syncing':_cache['syncing'],'inventory_count':len(_cache['inventory'])})

@app.route('/production-reset', methods=['POST','GET','OPTIONS'])
def r_reset():
    if request.method=='OPTIONS': return '',204
    wipe_all = request.args.get('wipe_all','').lower() in ('true','1')
    save_snapshot({'last_check':None,'orders':{}}); 
    if wipe_all: save_shipped({'skus':{}}); s3_put_json(S3_ARRIVALS_KEY,{'receipts':{},'pending':{}})
    with _lock: _cache.update({'inventory':[],'snapshot':None,'shipped':None,'last_sync':None})
    print("Snapshot wiped"); changes = do_sync()
    with _lock: return jsonify({'status':'ok','inventory_count':len(_cache['inventory']),'last_sync':_cache['last_sync'],'changes':changes})

@app.route('/production-shipped-ledger', methods=['GET','OPTIONS'])
def r_shipped():
    if request.method=='OPTIONS': return '',204
    l = load_shipped(); skus = l.get('skus',{})
    return jsonify({'tracked':len(skus),'total_shipped':sum(r.get('shipped',0) for r in skus.values()),'skus':skus})

@app.route('/production-force-arrive', methods=['POST','OPTIONS'])
def r_farr():
    if request.method=='OPTIONS': return '',204
    d = request.get_json(force=True,silent=True) or {}; prod = d.get('production','')
    if not prod: return jsonify({'error':'production required'}),400
    s = load_snapshot(); u = 0
    for o in s['orders'].values():
        if o['production']!=prod or o['status']=='in_stock': continue
        o['status']='in_stock'; o['fell_off_date']=datetime.utcnow().isoformat()+'Z'; u+=1
    if u: save_snapshot(s)
    with _lock: _cache['inventory']=[]
    return jsonify({'ok':True,'updated':u})

@app.route('/production-force-incoming', methods=['POST','OPTIONS'])
def r_finc():
    if request.method=='OPTIONS': return '',204
    d = request.get_json(force=True,silent=True) or {}; prod = d.get('production','')
    if not prod: return jsonify({'error':'production required'}),400
    s = load_snapshot(); u = 0
    for o in s['orders'].values():
        if o['production']!=prod or o['status']=='incoming': continue
        o['status']='incoming'; o['fell_off_date']=None; o['consecutive_misses']=0; u+=1
    if u: save_snapshot(s)
    with _lock: _cache['inventory']=[]
    return jsonify({'ok':True,'updated':u})

@app.route('/production-archive-index', methods=['GET','OPTIONS'])
def r_arch():
    if request.method=='OPTIONS': return '',204
    files = refresh_archive_index(); limit = int(request.args.get('limit',50))
    return jsonify({'total':len(files),'files':[{'name':f['name'],'date':f['dt'].strftime('%Y-%m-%d')} for f in files[:limit]]})

@app.route('/production-discrepancies', methods=['GET','OPTIONS'])
def r_discrep():
    if request.method=='OPTIONS': return '',204
    try:
        s = load_snapshot(); ats = fetch_ats_full(); ats_lk = {i['sku']:i for i in ats}
        arrived = {}
        for o in s.get('orders',{}).values():
            if o.get('status')=='in_stock': sku=o.get('style','').upper(); arrived[sku]=arrived.get(sku,0)+o.get('units',0)
        discreps = []
        for sku, pt in arrived.items():
            item = ats_lk.get(sku,{}); wh = item.get('warehouse',0); diff = pt-wh
            if abs(diff)>=10: discreps.append({'sku':sku,'brand':item.get('brand',''),'production_arrived':pt,'warehouse_actual':wh,'difference':diff,'committed':abs(item.get('committed',0)),'allocated':abs(item.get('allocated',0)),'notes':'Factory shipped more than WH received' if diff>0 else 'WH has more than production recorded'})
        discreps.sort(key=lambda d: abs(d['difference']), reverse=True)
        return jsonify({'status':'ok','count':len(discreps),'checked_skus':len(arrived),'discrepancies':discreps,'note':'Warehouse data used for comparison only'})
    except Exception as e: return jsonify({'error':str(e)}),500

# ── MANUAL ALLOCATIONS (own S3) ──
@app.route('/manual-allocations', methods=['GET','POST','OPTIONS'])
def r_malloc():
    if request.method=='OPTIONS': return '',204
    if request.method=='GET':
        d = s3_get_json(S3_MANUAL_ALLOC_KEY, {'allocations':[]}); return jsonify(d.get('allocations',[]))
    body = request.get_json(force=True,silent=True) or {}; allocs = body.get('allocations',[])
    s3_put_json(S3_MANUAL_ALLOC_KEY, {'allocations':allocs,'updated':datetime.utcnow().isoformat()+'Z'})
    return jsonify({'success':True,'count':len(allocs)})

# ── READ-ONLY PROXY ──
READONLY_ENDPOINTS = ['/overrides','/banner-rules','/prepack-defaults','/suppression-overrides','/deduction-assignments','/allocations','/saved-catalogs']
@app.before_request
def block_writes():
    if request.method in ('POST','PUT','DELETE'):
        for ep in READONLY_ENDPOINTS:
            if request.path==ep or request.path.startswith(ep+'/'): return jsonify({'error':'read_only','message':'Read-only. Edit on main platform.'}),403

def _proxy(path, default):
    try: resp = http_requests.get(f'{MAIN_API_URL}{path}', timeout=15); return jsonify(resp.json())
    except: return jsonify(default)

@app.route('/production', methods=['GET','OPTIONS'])
def p1():
    if request.method=='OPTIONS': return '',204
    return _proxy('/production',{'production':[]})
@app.route('/overrides', methods=['GET','OPTIONS'])
def p2():
    if request.method=='OPTIONS': return '',204
    return _proxy('/overrides',{'overrides':{}})
@app.route('/allocations', methods=['GET','OPTIONS'])
def p3():
    if request.method=='OPTIONS': return '',204
    return _proxy('/allocations',[])
@app.route('/suppression-overrides', methods=['GET','OPTIONS'])
def p4():
    if request.method=='OPTIONS': return '',204
    return _proxy('/suppression-overrides',{'overrides':[]})
@app.route('/banner-rules', methods=['GET','OPTIONS'])
def p5():
    if request.method=='OPTIONS': return '',204
    return _proxy('/banner-rules',{'rules':[]})
@app.route('/prepack-defaults', methods=['GET','OPTIONS'])
def p6():
    if request.method=='OPTIONS': return '',204
    return _proxy('/prepack-defaults',{'defaults':{}})
@app.route('/deduction-assignments', methods=['GET','OPTIONS'])
def p7():
    if request.method=='OPTIONS': return '',204
    return _proxy('/deduction-assignments',{})
@app.route('/apo', methods=['GET','OPTIONS'])
def p8():
    if request.method=='OPTIONS': return '',204
    return _proxy('/apo',{'apo':[]})
@app.route('/dropbox-photos', methods=['GET','OPTIONS'])
def p9():
    if request.method=='OPTIONS': return '',204
    return _proxy('/dropbox-photos',{'codes':[]})
@app.route('/image/<path:sku>', methods=['GET'])
def p10(sku):
    try:
        brand = request.args.get('brand','')
        resp = http_requests.get(f'{MAIN_API_URL}/image/{sku}?brand={brand}', timeout=10, stream=True)
        return resp.content, resp.status_code, {'Content-Type':resp.headers.get('Content-Type','image/jpeg')}
    except: return '',404

# ── CRON ──
def start_cron():
    def loop():
        time.sleep(8); print("Initial sync...", flush=True); do_sync()
        while True: time.sleep(SYNC_INTERVAL); print("Hourly sync...", flush=True); do_sync()
    threading.Thread(target=loop, daemon=True).start()
start_cron()

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5001))
    print(f"\nProduction Inventory v2 — port {port}\n  Ledger: {DROPBOX_LEDGER_FOLDER}\n  ATS: {DROPBOX_ATS_PATH}\n  S3: {S3_BUCKET}/{S3_SNAPSHOT_KEY}\n")
    app.run(host='0.0.0.0', port=port, debug=False)
