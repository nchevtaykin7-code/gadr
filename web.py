import json,urllib.request,urllib.parse,ssl,time,datetime,threading
from http.server import HTTPServer,BaseHTTPRequestHandler
from concurrent.futures import ThreadPoolExecutor,as_completed

SPAS_URL=""
SPAS_TOKEN=""
C=ssl.create_default_context()
C.check_hostname=False
C.verify_mode=ssl.CERT_NONE
ALL=[]
KLADR_IDX={}
ALL_LOCK=threading.Lock()
STATUS={'done':False,'n':0,'msg':'Запуск...','loading':True}

def log(msg):print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] {msg}")

def init_spas():
    global SPAS_URL,SPAS_TOKEN
    if SPAS_URL:return
    u='https://fias.nalog.ru/Home/GetSpasSettings?'+urllib.parse.urlencode({'url':'https://fias.nalog.ru/'})
    r=urllib.request.Request(u)
    r.add_header('User-Agent','Mozilla/5.0')
    with urllib.request.urlopen(r,context=C,timeout=15) as x:
        d=json.loads(x.read())
        SPAS_URL=d['Url'];SPAS_TOKEN=d['Token']
        log("SPAS OK")

def api_post(body):
    init_spas()
    u=SPAS_URL+'/api/spas/v2.0/GetAddressItems'
    b=json.dumps(body).encode()
    for i in range(3):
        try:
            r=urllib.request.Request(u,data=b,method='POST')
            r.add_header('User-Agent','Mozilla/5.0')
            r.add_header('master-token',SPAS_TOKEN)
            r.add_header('Content-Type','application/json')
            with urllib.request.urlopen(r,context=C,timeout=30) as x:
                return json.loads(x.read())
        except:
            if i==2:raise
            time.sleep(0.3)

def api_get(method,params):
    init_spas()
    u=SPAS_URL+method+'?'+urllib.parse.urlencode(params)
    for i in range(3):
        try:
            r=urllib.request.Request(u)
            r.add_header('User-Agent','Mozilla/5.0')
            r.add_header('master-token',SPAS_TOKEN)
            with urllib.request.urlopen(r,context=C,timeout=30) as x:
                return json.loads(x.read())
        except:
            if i==2:raise
            time.sleep(0.3)

def get_children(path):
    return api_post({'address_type':2,'path':path}).get('addresses',[])

def hint(q):
    return api_get('/api/spas/v2.0/GetAddressHint',{'search_string':q,'address_type':'2'})

def get_by_id(oid):
    r=api_get('/api/spas/v2.0/GetAddressItemById',{'object_id':str(oid),'address_type':'2'})
    return r['addresses'][0] if r.get('addresses') else None

def fmt(a):
    if not a:return{'error':'Не найден'}
    d=a.get('address_details')or{}
    return{'gar':{'object_id':a.get('object_id'),'object_guid':a.get('object_guid'),
        'full_name':a.get('full_name'),'level':a.get('object_level_id'),
        'region_code':a.get('region_code'),'is_active':a.get('is_active')},
        'kladr':{'kladr_code':d.get('kladr_code',''),'postal_code':d.get('postal_code',''),
        'okato':d.get('okato',''),'oktmo':d.get('oktmo',''),
        'ifns_fl':d.get('ifns_fl',''),'ifns_ul':d.get('ifns_ul','')},
        'hierarchy':[{'level':h.get('object_level_id'),'name':h.get('full_name'),
        'type':h.get('object_type'),'kladr_code':h.get('kladr_code')}for h in a.get('hierarchy',[])]}

def do_g2k(q):
    a=None
    if q.isdigit():a=get_by_id(int(q))
    elif len(q)==36 and '-' in q:
        r=api_get('/api/spas/v2.0/GetAddressItemByGuid',{'object_guid':q,'address_type':'2'})
        if r.get('addresses'):a=r['addresses'][0]
    else:
        r=hint(q)
        if r.get('hints'):a=get_by_id(r['hints'][0]['object_id'])
    return fmt(a)

def do_k2g(code):
    code=code.strip()
    with ALL_LOCK:
        if code in KLADR_IDX:
            oid=KLADR_IDX[code]
            a=get_by_id(oid)
            return fmt(a)
        for row in ALL:
            if row['kladr']==code:
                a=get_by_id(row['id'])
                return fmt(a)
    return{'error':'КЛАДР код '+code+' не найден в реестре Мордовии. Дождитесь окончания загрузки.'}

LVL={1:'Регион',3:'Район',4:'Поселение',5:'Город',6:'Нас.пункт',7:'Территория',8:'Улица'}

def last_part(full):
    p=full.rsplit(', ',1)
    return p[-1] if len(p)>1 else full

def process_path(path,hi,depth):
    if depth>4:return[]
    try:ch=get_children(path)
    except Exception as e:
        log(f"ERR {path}: {e}");return[]
    tasks=[]
    for a in ch:
        det=a.get('address_details')or{}
        lvl=a.get('object_level_id',0)
        short=last_part(a.get('full_name',''))
        p=a.get('path','')
        h=dict(hi)
        if lvl==3:h['rayon']=short
        elif lvl==4:h['city']=short
        elif lvl in(5,6):h['np']=short
        elif lvl==7:h['city']=short
        elif lvl==8:h['street']=short
        kl=det.get('kladr_code','')
        oid=a.get('object_id','')
        with ALL_LOCK:
            ALL.append({'id':oid,'guid':a.get('object_guid',''),
                'level':lvl,'type':LVL.get(lvl,''),
                'kladr':kl,'okato':det.get('okato',''),
                'oktmo':det.get('oktmo',''),'postal':det.get('postal_code',''),
                'ifns':det.get('ifns_fl',''),
                'rayon':h.get('rayon',''),'city':h.get('city',''),
                'np':h.get('np',''),'street':h.get('street','')})
            if kl:KLADR_IDX[kl]=oid
            STATUS['n']=len(ALL)
            if len(ALL)%100==0:
                log(f"  {len(ALL)} объектов")
                STATUS['msg']=f'Сбор... {len(ALL)}'
        if lvl<8:
            tasks.append((p,h,depth+1))
    return tasks

def load_all():
    global ALL,KLADR_IDX
    ALL=[];KLADR_IDX={};STATUS['done']=False;STATUS['n']=0;STATUS['msg']='Запуск...'
    t0=time.time()
    log("=== Сбор Мордовии ===")
    queue=[('144932',{'rayon':'','city':'','np':'','street':''},0)]
    with ThreadPoolExecutor(max_workers=10) as pool:
        while queue:
            batch=queue[:10];queue=queue[10:]
            futs={pool.submit(process_path,p,h,d):1 for p,h,d in batch}
            for f in as_completed(futs):
                try:queue.extend(f.result())
                except:pass
    elapsed=time.time()-t0
    STATUS['done']=True;STATUS['msg']=f'Готово: {len(ALL)} объектов за {elapsed:.0f}с | КЛАДР индекс: {len(KLADR_IDX)}'
    log(f"=== ГОТОВО: {len(ALL)} за {elapsed:.0f}s, КЛАДР: {len(KLADR_IDX)} ===")

def csv_out():
    def t(v):
        s=str(v) if v else ''
        if s and s.isdigit() and len(s)>5:return '="'+s+'"'
        return s
    lines=['ID;GUID;Уровень;Тип;Район;Город/Поселение;Нас.пункт;Улица;КЛАДР;ОКАТО;ОКТМО;Индекс;ИФНС']
    for r in ALL:
        lines.append(';'.join([str(r['id']),str(r['guid']),str(r['level']),r['type'],
            '"'+r['rayon']+'"','"'+r['city']+'"','"'+r['np']+'"','"'+r['street']+'"',
            t(r['kladr']),t(r['okato']),t(r['oktmo']),t(r['postal']),t(r['ifns'])]))
    return '\n'.join(lines)

def qs(s):
    r={}
    for p in s.split('&'):
        if '=' in p:k,v=p.split('=',1);r[k]=urllib.parse.unquote_plus(v)
    return r

BD=r'C:\Хранилище\Колледж\Практики\гос\gadr'

class W(BaseHTTPRequestHandler):
    def log_message(self,*a):pass
    def do_GET(self):
        p=self.path.split('?')[0];q=qs(self.path.split('?')[1] if '?' in self.path else '')
        try:
            if p=='/':
                with open(BD+'/index.html','r',encoding='utf-8') as f:h=f.read()
                self._h(h)
            elif p=='/api/g2k':self._j(do_g2k(q.get('q','')))
            elif p=='/api/k2g':self._j(do_k2g(q.get('code','')))
            elif p=='/api/hint':self._j(hint(q.get('q','')))
            elif p=='/api/search':self._j(hint(q.get('q','')))
            elif p=='/api/status':self._j(STATUS)
            elif p=='/api/data':
                with ALL_LOCK:self._j(list(ALL))
            elif p=='/api/csv':
                c=csv_out()
                self.send_response(200)
                self.send_header('Content-Type','text/csv;charset=utf-8')
                self.send_header('Content-Disposition','attachment;filename=mordovia.csv')
                self.end_headers();self.wfile.write(c.encode('utf-8-sig'));return
            else:self.send_response(404);self.end_headers()
        except Exception as e:
            log(f"ERR: {e}");self._j({'error':str(e)})
    def _j(self,d):
        self.send_response(200);self.send_header('Content-Type','application/json;charset=utf-8')
        self.end_headers();self.wfile.write(json.dumps(d,ensure_ascii=False).encode())
    def _h(self,h):
        self.send_response(200);self.send_header('Content-Type','text/html;charset=utf-8')
        self.end_headers();self.wfile.write(h.encode())

threading.Thread(target=load_all,daemon=True).start()
print('='*40)
print('  ГАР Мордовия | http://localhost:8080')
print('='*40)
s=HTTPServer(('0.0.0.0',8080),W)
import webbrowser;webbrowser.open('http://localhost:8080')
try:s.serve_forever()
except KeyboardInterrupt:print('Stop')
