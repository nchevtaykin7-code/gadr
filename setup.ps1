# setup.ps1 - Создание проекта конвертера ГАР <-> КЛАДР
# Сохранить как: C:\Хранилище\Колледж\Практики\гос\gadr\setup.ps1
# Запуск: powershell -ExecutionPolicy Bypass -File "C:\Хранилище\Колледж\Практики\гос\gadr\setup.ps1"

$root = "C:\Хранилище\Колледж\Практики\гос\gadr"

# Создаём папки
@("$root", "$root\data\gar_input", "$root\data\kladr_input", "$root\data\output") | ForEach-Object {
    if (-not (Test-Path $_)) { New-Item -ItemType Directory -Path $_ -Force | Out-Null; Write-Host "Папка: $_" -ForegroundColor Green }
}

# requirements.txt
Set-Content "$root\requirements.txt" "dbfread>=2.0.7`nlxml>=4.9.0" -Encoding UTF8
Write-Host "requirements.txt создан" -ForegroundColor Cyan

# Основной скрипт конвертера - ВСЁ В ОДНОМ ФАЙЛЕ
$pyContent = @'
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Конвертер ГАР <-> КЛАДР (всё в одном файле).

Использование:
    python converter.py gar2kladr --gar-dir ./data/gar_input --output ./data/output
    python converter.py kladr2gar --kladr-dir ./data/kladr_input --output ./data/output
    python converter.py demo
"""

import os
import sys
import re
import struct
import datetime
import uuid
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import List, Dict, Optional, Tuple

# === Попытка импорта зависимостей ===
try:
    from lxml import etree
    HAS_LXML = True
except ImportError:
    HAS_LXML = False

try:
    from dbfread import DBF
    HAS_DBFREAD = True
except ImportError:
    HAS_DBFREAD = False


# ====================================================================
# МОДЕЛИ ДАННЫХ
# ====================================================================

@dataclass
class KladrRecord:
    """Kladr.dbf - уровни 1-4."""
    name: str = ""
    socr: str = ""
    code: str = ""
    index: str = ""
    gninmb: str = ""
    uno: str = ""
    ocatd: str = ""
    status: str = "0"

    @property
    def region_code(self): return self.code[:2] if len(self.code) >= 2 else ""
    @property
    def district_code(self): return self.code[2:5] if len(self.code) >= 5 else ""
    @property
    def city_code(self): return self.code[5:8] if len(self.code) >= 8 else ""
    @property
    def locality_code(self): return self.code[8:11] if len(self.code) >= 11 else ""
    @property
    def actuality(self): return self.code[11:13] if len(self.code) >= 13 else "00"
    @property
    def is_actual(self): return self.actuality == "00"
    @property
    def level(self):
        if len(self.code) < 13: return 0
        if self.code[8:11] != "000": return 4
        if self.code[5:8] != "000": return 3
        if self.code[2:5] != "000": return 2
        return 1


@dataclass
class KladrStreetRecord:
    """Street.dbf - уровень 5."""
    name: str = ""
    socr: str = ""
    code: str = ""
    index: str = ""
    gninmb: str = ""
    uno: str = ""
    ocatd: str = ""

    @property
    def parent_code(self): return self.code[:11] + "00" if len(self.code) >= 11 else ""
    @property
    def street_code(self): return self.code[11:15] if len(self.code) >= 15 else ""
    @property
    def actuality(self): return self.code[15:17] if len(self.code) >= 17 else "00"
    @property
    def is_actual(self): return self.actuality == "00"


@dataclass
class KladrHouseRecord:
    """Doma.dbf - уровень 6."""
    name: str = ""
    korp: str = ""
    socr: str = ""
    code: str = ""
    index: str = ""
    gninmb: str = ""
    uno: str = ""
    ocatd: str = ""


@dataclass
class GarAddrObject:
    """Адресный объект ГАР."""
    objectid: int = 0
    objectguid: str = ""
    name: str = ""
    typename: str = ""
    level: int = 0
    isactual: int = 1
    isactive: int = 1


@dataclass
class GarHouse:
    """Дом ГАР."""
    objectid: int = 0
    objectguid: str = ""
    housenum: str = ""
    housetype: int = 0
    addnum1: str = ""
    addtype1: int = 0
    isactual: int = 1
    isactive: int = 1


@dataclass
class GarHierarchy:
    """Иерархия ГАР."""
    objectid: int = 0
    parentobjid: int = 0
    regioncode: str = ""
    areacode: str = ""
    citycode: str = ""
    placecode: str = ""
    streetcode: str = ""
    isactive: int = 1


# ====================================================================
# МАППИНГ ТИПОВ
# ====================================================================

GAR_LEVEL_TO_KLADR = {1:1, 2:2, 3:2, 4:3, 5:3, 6:4, 7:4, 8:5, 10:6}

SOCR_MAP = {
    1: {"Респ":"Респ","Республика":"Респ","край":"край","Край":"край",
        "обл":"обл","Область":"обл","г":"г","Город":"г","г.":"г",
        "АО":"АО","а.окр":"АО","Аобл":"Аобл","а.обл":"Аобл"},
    2: {"р-н":"р-н","Район":"р-н","район":"р-н","м.р-н":"р-н",
        "у":"у","тер":"тер","тер.":"тер"},
    3: {"г":"г","г.":"г","пгт":"пгт","пгт.":"пгт","рп":"рп","р.п.":"рп",
        "кп":"кп","к.п.":"кп","дп":"дп","д.п.":"дп",
        "с/с":"с/с","с/о":"с/о","с/а":"с/а","с/пос":"с/пос",
        "с/тер":"с/тер","с/мо":"с/мо","волость":"волость",
        "тер":"тер","тер.":"тер","п/о":"п/о",
        "сп":"с/пос","гп":"г","с.п.":"с/пос","г.п.":"пгт"},
    4: {"с":"с","с.":"с","д":"д","д.":"д","п":"п","п.":"п",
        "х":"х","х.":"х","ст-ца":"ст-ца","ст":"ст","рзд":"рзд",
        "аул":"аул","аал":"аал","г":"г","г.":"г","пгт":"пгт",
        "рп":"рп","р.п.":"рп","кп":"кп","дп":"дп",
        "сл":"сл","м":"м","мкр":"мкр","нп":"нп",
        "городок":"городок","тер":"тер","тер.":"тер",
        "починок":"починок","промзона":"промзона",
        "казарма":"казарма","заимка":"заимка",
        "п/о":"п/о","п/р":"п/р","п/ст":"п/ст"},
    5: {"ул":"ул","ул.":"ул","пер":"пер","пер.":"пер",
        "пр-кт":"пр-кт","пл":"пл","пл.":"пл",
        "б-р":"б-р","наб":"наб","наб.":"наб",
        "ш":"ш","ш.":"ш","проезд":"проезд","туп":"туп",
        "аллея":"аллея","линия":"линия","км":"км",
        "кв-л":"кв-л","тракт":"тракт","дор":"дор",
        "въезд":"въезд","мкр":"мкр","тер":"тер","тер.":"тер",
        "сквер":"сквер","парк":"парк","стр":"стр",
        "уч-к":"уч-к","сад":"сад"},
}

SOCR_TO_TYPENAME = {
    "Респ":"Респ","край":"край","обл":"обл","г":"г.",
    "АО":"а.окр.","Аобл":"а.обл.","р-н":"р-н","у":"у",
    "тер":"тер.","пгт":"пгт.","рп":"р.п.","кп":"к.п.","дп":"д.п.",
    "с/с":"с/с","с/о":"с/о","с/а":"с/а","с/пос":"с.п.",
    "с":"с.","д":"д.","п":"п.","х":"х.","ст-ца":"ст-ца",
    "ст":"ст.","рзд":"рзд","аул":"аул","аал":"аал",
    "сл":"сл","м":"м","мкр":"мкр","нп":"нп",
    "ул":"ул.","пер":"пер.","пр-кт":"пр-кт","пл":"пл.",
    "б-р":"б-р","наб":"наб.","ш":"ш.","проезд":"проезд",
    "туп":"туп.","аллея":"аллея","линия":"линия",
    "км":"км","кв-л":"кв-л","тракт":"тракт","дор":"дор.",
}

REGION_CODES = {
    "01":"Республика Адыгея","02":"Республика Башкортостан",
    "03":"Республика Бурятия","04":"Республика Алтай",
    "05":"Республика Дагестан","06":"Республика Ингушетия",
    "07":"Кабардино-Балкарская Республика","08":"Республика Калмыкия",
    "09":"Карачаево-Черкесская Республика","10":"Республика Карелия",
    "11":"Республика Коми","12":"Республика Марий Эл",
    "13":"Республика Мордовия","14":"Республика Саха (Якутия)",
    "15":"Республика Северная Осетия - Алания",
    "16":"Республика Татарстан","17":"Республика Тыва",
    "18":"Удмуртская Республика","19":"Республика Хакасия",
    "20":"Чеченская Республика","21":"Чувашская Республика",
    "22":"Алтайский край","23":"Краснодарский край",
    "24":"Красноярский край","25":"Приморский край",
    "26":"Ставропольский край","27":"Хабаровский край",
    "28":"Амурская область","29":"Архангельская область",
    "30":"Астраханская область","31":"Белгородская область",
    "32":"Брянская область","33":"Владимирская область",
    "34":"Волгоградская область","35":"Вологодская область",
    "36":"Воронежская область","37":"Ивановская область",
    "38":"Иркутская область","39":"Калининградская область",
    "40":"Калужская область","41":"Камчатский край",
    "42":"Кемеровская область","43":"Кировская область",
    "44":"Костромская область","45":"Курганская область",
    "46":"Курская область","47":"Ленинградская область",
    "48":"Липецкая область","49":"Магаданская область",
    "50":"Московская область","51":"Мурманская область",
    "52":"Нижегородская область","53":"Новгородская область",
    "54":"Новосибирская область","55":"Омская область",
    "56":"Оренбургская область","57":"Орловская область",
    "58":"Пензенская область","59":"Пермский край",
    "60":"Псковская область","61":"Ростовская область",
    "62":"Рязанская область","63":"Самарская область",
    "64":"Саратовская область","65":"Сахалинская область",
    "66":"Свердловская область","67":"Смоленская область",
    "68":"Тамбовская область","69":"Тверская область",
    "70":"Томская область","71":"Тульская область",
    "72":"Тюменская область","73":"Ульяновская область",
    "74":"Челябинская область","75":"Забайкальский край",
    "76":"Ярославская область","77":"г. Москва",
    "78":"г. Санкт-Петербург","79":"Еврейская АО",
    "86":"ХМАО - Югра","87":"Чукотский АО",
    "89":"ЯНАО","91":"Республика Крым","92":"г. Севастополь",
}


# ====================================================================
# УТИЛИТЫ
# ====================================================================

def zfill(s, n):
    return str(s)[:n].zfill(n)

def build_code(reg, dist="000", city="000", loc="000", act="00"):
    return zfill(reg,2)+zfill(dist,3)+zfill(city,3)+zfill(loc,3)+zfill(act,2)

def build_street_code(reg, dist="000", city="000", loc="000", st="0000", act="00"):
    return zfill(reg,2)+zfill(dist,3)+zfill(city,3)+zfill(loc,3)+zfill(st,4)+zfill(act,2)

def build_house_code(reg, dist="000", city="000", loc="000", st="0000", house="0000"):
    return zfill(reg,2)+zfill(dist,3)+zfill(city,3)+zfill(loc,3)+zfill(st,4)+zfill(house,4)

def get_socr(typename, kladr_level):
    t = typename.strip().rstrip('.')
    m = SOCR_MAP.get(kladr_level, {})
    return m.get(t, m.get(t+".", m.get(typename.strip(), t[:10])))

def parse_kladr_code(code):
    r = {"region":"","district":"","city":"","locality":"",
         "street":"","house":"","actuality":"","level":0}
    c = code.strip()
    if len(c) >= 13:
        r["region"]=c[0:2]; r["district"]=c[2:5]
        r["city"]=c[5:8]; r["locality"]=c[8:11]; r["actuality"]=c[11:13]
        if r["locality"]!="000": r["level"]=4
        elif r["city"]!="000": r["level"]=3
        elif r["district"]!="000": r["level"]=2
        else: r["level"]=1
    if len(c) >= 17:
        r["street"]=c[11:15]; r["actuality"]=c[15:17]; r["level"]=5
    if len(c) >= 19:
        r["street"]=c[11:15]; r["house"]=c[15:19]; r["level"]=6
    return r


# ====================================================================
# DBF WRITER (простой dBASE III)
# ====================================================================

class DbfWriter:
    def __init__(self, path, fields):
        self.path = path
        self.fields = fields  # [(name, type, len, dec), ...]
        self.records = []

    def add(self, data):
        self.records.append(data)

    def write(self):
        with open(self.path, 'wb') as f:
            nr = len(self.records)
            nf = len(self.fields)
            hs = 32 + nf*32 + 1
            rs = 1 + sum(fl[2] for fl in self.fields)
            now = datetime.datetime.now()

            f.write(struct.pack('<B', 3))
            f.write(struct.pack('<3B', now.year-1900, now.month, now.day))
            f.write(struct.pack('<I', nr))
            f.write(struct.pack('<H', hs))
            f.write(struct.pack('<H', rs))
            f.write(b'\x00'*20)

            for nm, ft, ln, dc in self.fields:
                fn = nm.encode('ascii','replace')[:11]
                fn += b'\x00'*(11-len(fn))
                f.write(fn)
                f.write(ft.encode('ascii'))
                f.write(b'\x00'*4)
                f.write(struct.pack('<B', ln))
                f.write(struct.pack('<B', dc))
                f.write(b'\x00'*14)

            f.write(b'\r')

            for rec in self.records:
                f.write(b' ')
                for nm, ft, ln, dc in self.fields:
                    v = str(rec.get(nm, ''))
                    if ft == 'C':
                        enc = v.encode('cp866','replace')[:ln]
                        enc += b' '*(ln-len(enc))
                        f.write(enc)
                    else:
                        f.write(v[:ln].rjust(ln).encode('ascii','replace'))

            f.write(b'\x1a')
        print(f"  Записан: {os.path.basename(self.path)} ({nr} записей)")


# ====================================================================
# ГАР -> КЛАДР
# ====================================================================

class GarToKladr:
    def __init__(self, gar_dir, out_dir):
        self.gar_dir = gar_dir
        self.out_dir = out_dir
        self.objects_by_id = {}
        self.children = defaultdict(list)
        self.obj_to_code = {}
        self.counters = {"d": defaultdict(int), "c": defaultdict(int),
                         "l": defaultdict(int), "s": defaultdict(int),
                         "h": defaultdict(int)}

    def convert(self):
        if not HAS_LXML:
            print("ОШИБКА: pip install lxml")
            return

        print("="*60)
        print("КОНВЕРТАЦИЯ ГАР -> КЛАДР")
        print("="*60)
        os.makedirs(self.out_dir, exist_ok=True)

        print("\n[1/4] Чтение адресных объектов...")
        objects = self._read_objects()

        print("\n[2/4] Чтение иерархии...")
        hierarchy = self._read_hierarchy()

        print("\n[3/4] Чтение домов...")
        houses = self._read_houses()

        print("\n[4/4] Конвертация...")
        self._do_convert(objects, hierarchy, houses)

        print("\n" + "="*60)
        print(f"ГОТОВО! Результат: {self.out_dir}")
        print("="*60)

    def _find_xml(self, prefix):
        if not os.path.isdir(self.gar_dir):
            return None
        for f in os.listdir(self.gar_dir):
            if f.upper().startswith(prefix.upper()) and f.lower().endswith('.xml'):
                return os.path.join(self.gar_dir, f)
        return None

    def _read_objects(self):
        fp = self._find_xml("AS_ADDR_OBJ_2") or self._find_xml("AS_ADDR_OBJ")
        if not fp:
            print("  [!] AS_ADDR_OBJ не найден!")
            return []
        print(f"  Файл: {os.path.basename(fp)}")
        objs = []
        for _, el in etree.iterparse(fp, events=('end',), tag='OBJECT'):
            try:
                o = GarAddrObject(
                    objectid=int(el.get('OBJECTID',0)),
                    objectguid=el.get('OBJECTGUID',''),
                    name=el.get('NAME',''),
                    typename=el.get('TYPENAME',''),
                    level=int(el.get('LEVEL',0)),
                    isactual=int(el.get('ISACTUAL',1)),
                    isactive=int(el.get('ISACTIVE',1)),
                )
                if o.isactual == 1 and o.isactive == 1:
                    objs.append(o)
            except: pass
            el.clear()
        print(f"  Объектов: {len(objs)}")
        return objs

    def _read_hierarchy(self):
        fp = self._find_xml("AS_ADM_HIERARCHY_2") or self._find_xml("AS_ADM_HIERARCHY")
        if not fp:
            print("  [!] AS_ADM_HIERARCHY не найден!")
            return []
        print(f"  Файл: {os.path.basename(fp)}")
        items = []
        for _, el in etree.iterparse(fp, events=('end',), tag='ITEM'):
            try:
                h = GarHierarchy(
                    objectid=int(el.get('OBJECTID',0)),
                    parentobjid=int(el.get('PARENTOBJID',0)),
                    regioncode=el.get('REGIONCODE',''),
                    areacode=el.get('AREACODE',''),
                    citycode=el.get('CITYCODE',''),
                    placecode=el.get('PLACECODE',''),
                    streetcode=el.get('STREETCODE',''),
                    isactive=int(el.get('ISACTIVE',1)),
                )
                if h.isactive == 1:
                    items.append(h)
            except: pass
            el.clear()
        print(f"  Записей: {len(items)}")
        return items

    def _read_houses(self):
        fp = self._find_xml("AS_HOUSES_2") or self._find_xml("AS_HOUSES")
        if not fp:
            print("  [!] AS_HOUSES не найден")
            return []
        print(f"  Файл: {os.path.basename(fp)}")
        houses = []
        for _, el in etree.iterparse(fp, events=('end',), tag='HOUSE'):
            try:
                h = GarHouse(
                    objectid=int(el.get('OBJECTID',0)),
                    objectguid=el.get('OBJECTGUID',''),
                    housenum=el.get('HOUSENUM','') or '',
                    housetype=int(el.get('HOUSETYPE',0) or 0),
                    addnum1=el.get('ADDNUM1','') or '',
                    addtype1=int(el.get('ADDTYPE1',0) or 0),
                    isactual=int(el.get('ISACTUAL',1)),
                    isactive=int(el.get('ISACTIVE',1)),
                )
                if h.isactual == 1 and h.isactive == 1:
                    houses.append(h)
            except: pass
            el.clear()
        print(f"  Домов: {len(houses)}")
        return houses

    def _do_convert(self, objects, hierarchy, houses):
        for o in objects:
            self.objects_by_id[o.objectid] = o
        for h in hierarchy:
            self.children[h.parentobjid].append(h.objectid)

        # Определяем регион
        reg = "00"
        for h in hierarchy:
            if h.regioncode:
                reg = zfill(h.regioncode, 2)
                break
        if reg == "00":
            dn = os.path.basename(self.gar_dir.rstrip('/\\'))
            if dn.isdigit() and 1 <= int(dn) <= 99:
                reg = zfill(dn, 2)
        print(f"  Регион: {reg}")

        # Назначаем коды по BFS
        hier_map = {}
        for h in hierarchy:
            hier_map[h.objectid] = h

        kladr_recs = []     # уровни 1-4
        street_recs = []    # уровень 5
        house_recs = []     # уровень 6

        # Регион (уровень 1)
        roots = [h.objectid for h in hierarchy
                 if h.parentobjid == 0 and h.objectid in self.objects_by_id]

        for rid in roots:
            obj = self.objects_by_id[rid]
            kl = GAR_LEVEL_TO_KLADR.get(obj.level, 0)
            if kl == 1:
                code = build_code(reg)
                self.obj_to_code[rid] = code
                kladr_recs.append({
                    "NAME": obj.name[:40], "SOCR": get_socr(obj.typename, 1),
                    "CODE": code, "INDEX":"","GNINMB":"","UNO":"",
                    "OCATD":"","STATUS":"0"
                })

        # BFS
        queue = deque()
        for rid in roots:
            for cid in self.children.get(rid, []):
                queue.append((cid, rid))

        while queue:
            objid, parent_id = queue.popleft()
            if objid not in self.objects_by_id:
                for cid in self.children.get(objid, []):
                    queue.append((cid, objid))
                continue

            obj = self.objects_by_id[objid]
            kl = GAR_LEVEL_TO_KLADR.get(obj.level, 0)
            parent_code = self.obj_to_code.get(parent_id, build_code(reg))
            pc = parse_kladr_code(parent_code)

            if kl == 2:  # Район
                key = pc["region"]
                self.counters["d"][key] += 1
                dist = zfill(str(self.counters["d"][key]), 3)
                code = build_code(pc["region"], dist)
                self.obj_to_code[objid] = code
                kladr_recs.append({
                    "NAME": obj.name[:40], "SOCR": get_socr(obj.typename, 2),
                    "CODE": code, "INDEX":"","GNINMB":"","UNO":"",
                    "OCATD":"","STATUS":"0"
                })

            elif kl == 3:  # Город
                key = pc["region"] + pc["district"]
                self.counters["c"][key] += 1
                city = zfill(str(self.counters["c"][key]), 3)
                code = build_code(pc["region"], pc["district"], city)
                self.obj_to_code[objid] = code
                kladr_recs.append({
                    "NAME": obj.name[:40], "SOCR": get_socr(obj.typename, 3),
                    "CODE": code, "INDEX":"","GNINMB":"","UNO":"",
                    "OCATD":"","STATUS":"0"
                })

            elif kl == 4:  # Нас.пункт
                key = pc["region"] + pc["district"] + pc.get("city","000")
                self.counters["l"][key] += 1
                loc = zfill(str(self.counters["l"][key]), 3)
                code = build_code(pc["region"], pc["district"],
                                  pc.get("city","000"), loc)
                self.obj_to_code[objid] = code
                kladr_recs.append({
                    "NAME": obj.name[:40], "SOCR": get_socr(obj.typename, 4),
                    "CODE": code, "INDEX":"","GNINMB":"","UNO":"",
                    "OCATD":"","STATUS":"0"
                })

            elif kl == 5:  # Улица
                base = parent_code[:11] if len(parent_code) >= 11 else parent_code
                key = base
                self.counters["s"][key] += 1
                st = zfill(str(self.counters["s"][key]), 4)
                code = build_street_code(
                    pc["region"], pc["district"],
                    pc.get("city","000"), pc.get("locality","000"), st)
                self.obj_to_code[objid] = code
                street_recs.append({
                    "NAME": obj.name[:40], "SOCR": get_socr(obj.typename, 5),
                    "CODE": code, "INDEX":"","GNINMB":"","UNO":"","OCATD":""
                })

            else:
                # Пропускаем неизвестные уровни, но проходим дочерних
                self.obj_to_code[objid] = parent_code

            # Добавляем дочерних в очередь
            for cid in self.children.get(objid, []):
                queue.append((cid, objid))

        # Дома: ищем родителя через иерархию
        house_parents = {}
        for h in hierarchy:
            house_parents[h.objectid] = h.parentobjid

        for house in houses:
            parent_id = house_parents.get(house.objectid, 0)
            parent_code = self.obj_to_code.get(parent_id, "")
            if not parent_code or len(parent_code) < 11:
                continue
            ppc = parse_kladr_code(parent_code)
            key = parent_code[:15] if len(parent_code) >= 15 else parent_code[:11]+"0000"
            self.counters["h"][key] += 1
            hnum = zfill(str(self.counters["h"][key]), 4)

            st_part = parent_code[11:15] if len(parent_code) >= 15 else "0000"
            hcode = build_house_code(
                ppc["region"], ppc["district"],
                ppc.get("city","000"), ppc.get("locality","000"),
                st_part, hnum)
            house_recs.append({
                "NAME": house.housenum[:40], "KORP":"",
                "SOCR":"ДОМ","CODE": hcode,
                "INDEX":"","GNINMB":"","UNO":"","OCATD":""
            })

        # Запись DBF
        print(f"\n  Записи: адр.объектов={len(kladr_recs)}, улиц={len(street_recs)}, домов={len(house_recs)}")

        # Kladr.dbf
        w = DbfWriter(os.path.join(self.out_dir, "Kladr.dbf"), [
            ("NAME","C",40,0),("SOCR","C",10,0),("CODE","C",13,0),
            ("INDEX","C",6,0),("GNINMB","C",4,0),("UNO","C",4,0),
            ("OCATD","C",11,0),("STATUS","C",1,0)])
        for r in kladr_recs: w.add(r)
        w.write()

        # Street.dbf
        w = DbfWriter(os.path.join(self.out_dir, "Street.dbf"), [
            ("NAME","C",40,0),("SOCR","C",10,0),("CODE","C",17,0),
            ("INDEX","C",6,0),("GNINMB","C",4,0),("UNO","C",4,0),
            ("OCATD","C",11,0)])
        for r in street_recs: w.add(r)
        w.write()

        # Doma.dbf
        w = DbfWriter(os.path.join(self.out_dir, "Doma.dbf"), [
            ("NAME","C",40,0),("KORP","C",10,0),("SOCR","C",10,0),
            ("CODE","C",19,0),("INDEX","C",6,0),("GNINMB","C",4,0),
            ("UNO","C",4,0),("OCATD","C",11,0)])
        for r in house_recs: w.add(r)
        w.write()

        # Socrbase.dbf
        w = DbfWriter(os.path.join(self.out_dir, "Socrbase.dbf"), [
            ("LEVEL","C",5,0),("SCNAME","C",10,0),
            ("SOCRNAME","C",29,0),("KOD_T_ST","C",3,0)])
        socr_entries = [
            ("1","Респ","Республика","106"),("1","край","Край","104"),
            ("1","обл","Область","105"),("1","г","Город","103"),
            ("1","АО","Автономный округ","101"),("1","Аобл","Автономная область","102"),
            ("2","р-н","Район","201"),("2","у","Улус","202"),
            ("3","г","Город","301"),("3","пгт","Поселок гор. типа","302"),
            ("3","рп","Рабочий поселок","303"),("3","с/с","Сельсовет","306"),
            ("3","с/о","Сельский округ","309"),("3","с/пос","Сельское поселение","314"),
            ("4","с","Село","430"),("4","д","Деревня","406"),
            ("4","п","Поселок","421"),("4","х","Хутор","435"),
            ("4","ст-ца","Станица","433"),("4","ст","Станция","432"),
            ("4","мкр","Микрорайон","418"),("4","нп","Населенный пункт","419"),
            ("5","ул","Улица","529"),("5","пер","Переулок","514"),
            ("5","пр-кт","Проспект","519"),("5","пл","Площадь","516"),
            ("5","б-р","Бульвар","502"),("5","наб","Набережная","511"),
            ("5","ш","Шоссе","531"),("5","проезд","Проезд","518"),
            ("5","туп","Тупик","528"),("6","ДОМ","Дом","601"),
        ]
        for lv, sc, sn, kt in socr_entries:
            w.add({"LEVEL":lv,"SCNAME":sc,"SOCRNAME":sn,"KOD_T_ST":kt})
        w.write()


# ====================================================================
# КЛАДР -> ГАР
# ====================================================================

class KladrToGar:
    def __init__(self, kladr_dir, out_dir):
        self.kladr_dir = kladr_dir
        self.out_dir = out_dir

    def convert(self):
        if not HAS_DBFREAD:
            print("ОШИБКА: pip install dbfread")
            return

        print("="*60)
        print("КОНВЕРТАЦИЯ КЛАДР -> ГАР")
        print("="*60)
        os.makedirs(self.out_dir, exist_ok=True)

        print("\n[1/3] Чтение КЛАДР...")
        kladr_recs = self._read_dbf("kladr.dbf")
        street_recs = self._read_dbf("street.dbf")
        house_recs = self._read_dbf("doma.dbf")

        print(f"  Kladr: {len(kladr_recs)}, Street: {len(street_recs)}, Doma: {len(house_recs)}")

        print("\n[2/3] Конвертация...")
        addr_objects = []
        houses_out = []
        hierarchy = []

        obj_counter = [0]
        code_to_objid = {}
        code_to_guid = {}

        def next_id():
            obj_counter[0] += 1
            return obj_counter[0]

        def get_gar_level(kladr_level):
            return {1:1, 2:2, 3:5, 4:6, 5:8, 6:10}.get(kladr_level, 1)

        # Адресные объекты (уровни 1-4)
        for row in kladr_recs:
            code = str(row.get('CODE','')).strip()
            name = str(row.get('NAME','')).strip()
            socr = str(row.get('SOCR','')).strip()
            if not code or len(code) < 13: continue
            if code[11:13] != "00": continue  # неактуальные

            oid = next_id()
            guid = str(uuid.uuid4())
            code_key = code[:11]  # без актуальности
            code_to_objid[code_key] = oid
            code_to_guid[code_key] = guid

            # Определяем уровень
            if code[8:11] != "000": kl = 4
            elif code[5:8] != "000": kl = 3
            elif code[2:5] != "000": kl = 2
            else: kl = 1

            typename = SOCR_TO_TYPENAME.get(socr, socr)

            addr_objects.append({
                "id": oid, "guid": guid, "name": name,
                "typename": typename, "level": get_gar_level(kl),
                "code_key": code_key
            })

            # Иерархия
            if kl == 1:
                parent_key = ""
            elif kl == 2:
                parent_key = code[:2] + "000000000"
            elif kl == 3:
                parent_key = code[:5] + "000000"
            elif kl == 4:
                parent_key = code[:8] + "000"

            parent_oid = code_to_objid.get(parent_key, 0)
            hierarchy.append({"objectid": oid, "parentobjid": parent_oid,
                              "regioncode": code[:2]})

        # Улицы (уровень 5)
        for row in street_recs:
            code = str(row.get('CODE','')).strip()
            name = str(row.get('NAME','')).strip()
            socr = str(row.get('SOCR','')).strip()
            if not code or len(code) < 17: continue
            if code[15:17] != "00": continue

            oid = next_id()
            guid = str(uuid.uuid4())
            code_key = code[:15]
            code_to_objid[code_key] = oid
            code_to_guid[code_key] = guid

            typename = SOCR_TO_TYPENAME.get(socr, socr)
            addr_objects.append({
                "id": oid, "guid": guid, "name": name,
                "typename": typename, "level": 8,
                "code_key": code_key
            })

            parent_key = code[:11]
            parent_oid = code_to_objid.get(parent_key, 0)
            hierarchy.append({"objectid": oid, "parentobjid": parent_oid,
                              "regioncode": code[:2]})

        # Дома (уровень 6)
        for row in house_recs:
            code = str(row.get('CODE','')).strip()
            name = str(row.get('NAME','')).strip()
            if not code or len(code) < 19: continue

            oid = next_id()
            guid = str(uuid.uuid4())

            houses_out.append({
                "id": oid, "guid": guid, "housenum": name,
                "housetype": 2
            })

            parent_key = code[:15]
            parent_oid = code_to_objid.get(parent_key, 0)
            if parent_oid == 0:
                parent_key = code[:11]
                parent_oid = code_to_objid.get(parent_key, 0)
            hierarchy.append({"objectid": oid, "parentobjid": parent_oid,
                              "regioncode": code[:2]})

        print(f"  Объектов: {len(addr_objects)}, домов: {len(houses_out)}")

        print("\n[3/3] Запись XML...")
        self._write_addr_obj_xml(addr_objects)
        self._write_houses_xml(houses_out)
        self._write_hierarchy_xml(hierarchy)

        print("\n" + "="*60)
        print(f"ГОТОВО! Результат: {self.out_dir}")
        print("="*60)

    def _read_dbf(self, filename):
        for f in os.listdir(self.kladr_dir):
            if f.lower() == filename.lower():
                fp = os.path.join(self.kladr_dir, f)
                print(f"  Чтение: {f}")
                try:
                    table = DBF(fp, encoding='cp866', char_decode_errors='replace')
                    return list(table)
                except Exception as e:
                    print(f"  Ошибка: {e}")
                    return []
        print(f"  [!] {filename} не найден")
        return []

    def _write_addr_obj_xml(self, objects):
        fp = os.path.join(self.out_dir, "AS_ADDR_OBJ.xml")
        with open(fp, 'w', encoding='utf-8') as f:
            f.write('<?xml version="1.0" encoding="utf-8"?>\n')
            f.write('<ADDRESSOBJECTS>\n')
            for o in objects:
                f.write(f'<OBJECT OBJECTID="{o["id"]}" OBJECTGUID="{o["guid"]}" '
                        f'NAME="{self._esc(o["name"])}" TYPENAME="{self._esc(o["typename"])}" '
                        f'LEVEL="{o["level"]}" ISACTUAL="1" ISACTIVE="1"/>\n')
            f.write('</ADDRESSOBJECTS>\n')
        print(f"  Записан: AS_ADDR_OBJ.xml ({len(objects)} объектов)")

    def _write_houses_xml(self, houses):
        fp = os.path.join(self.out_dir, "AS_HOUSES.xml")
        with open(fp, 'w', encoding='utf-8') as f:
            f.write('<?xml version="1.0" encoding="utf-8"?>\n')
            f.write('<HOUSES>\n')
            for h in houses:
                f.write(f'<HOUSE OBJECTID="{h["id"]}" OBJECTGUID="{h["guid"]}" '
                        f'HOUSENUM="{self._esc(h["housenum"])}" '
                        f'HOUSETYPE="{h["housetype"]}" '
                        f'ISACTUAL="1" ISACTIVE="1"/>\n')
            f.write('</HOUSES>\n')
        print(f"  Записан: AS_HOUSES.xml ({len(houses)} домов)")

    def _write_hierarchy_xml(self, hierarchy):
        fp = os.path.join(self.out_dir, "AS_ADM_HIERARCHY.xml")
        with open(fp, 'w', encoding='utf-8') as f:
            f.write('<?xml version="1.0" encoding="utf-8"?>\n')
            f.write('<ITEMS>\n')
            for i, h in enumerate(hierarchy, 1):
                f.write(f'<ITEM ID="{i}" OBJECTID="{h["objectid"]}" '
                        f'PARENTOBJID="{h["parentobjid"]}" '
                        f'REGIONCODE="{h["regioncode"]}" '
                        f'ISACTIVE="1"/>\n')
            f.write('</ITEMS>\n')
        print(f"  Записан: AS_ADM_HIERARCHY.xml ({len(hierarchy)} связей)")

    def _esc(self, s):
        return str(s).replace('&','&amp;').replace('<','&lt;').replace('>','&gt;').replace('"','&quot;')


# ====================================================================
# ДЕМО-РЕЖИМ (создаёт тестовые данные)
# ====================================================================

def create_demo():
    """Создать демонстрационные XML-файлы ГАР для тестирования."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    demo_dir = os.path.join(script_dir, "data", "gar_input")
    os.makedirs(demo_dir, exist_ok=True)

    print("Создание демо-данных ГАР...")

    # AS_ADDR_OBJ.xml
    with open(os.path.join(demo_dir, "AS_ADDR_OBJ.xml"), 'w', encoding='utf-8') as f:
        f.write('<?xml version="1.0" encoding="utf-8"?>\n<ADDRESSOBJECTS>\n')
        demo_objects = [
            (1, "region-guid-001", "Московская", "обл.", 1),
            (2, "dist-guid-001", "Подольский", "р-н", 2),
            (3, "dist-guid-002", "Одинцовский", "р-н", 2),
            (4, "city-guid-001", "Подольск", "г.", 5),
            (5, "city-guid-002", "Одинцово", "г.", 5),
            (6, "loc-guid-001", "Климовск", "д.", 6),
            (7, "loc-guid-002", "Лесное", "с.", 6),
            (10, "street-guid-001", "Ленина", "ул.", 8),
            (11, "street-guid-002", "Мира", "пр-кт", 8),
            (12, "street-guid-003", "Садовая", "ул.", 8),
            (13, "street-guid-004", "Центральная", "ул.", 8),
        ]
        for oid, guid, name, tn, lv in demo_objects:
            f.write(f'<OBJECT OBJECTID="{oid}" OBJECTGUID="{guid}" '
                    f'NAME="{name}" TYPENAME="{tn}" LEVEL="{lv}" '
                    f'ISACTUAL="1" ISACTIVE="1" OPERTYPEID="1" '
                    f'PREVID="0" NEXTID="0"/>\n')
        f.write('</ADDRESSOBJECTS>\n')

    # AS_ADM_HIERARCHY.xml
    with open(os.path.join(demo_dir, "AS_ADM_HIERARCHY.xml"), 'w', encoding='utf-8') as f:
        f.write('<?xml version="1.0" encoding="utf-8"?>\n<ITEMS>\n')
        demo_hier = [
            (1, 1, 0, "50"),    # Московская обл - корень
            (2, 2, 1, "50"),    # Подольский р-н -> Моск.обл
            (3, 3, 1, "50"),    # Одинцовский р-н -> Моск.обл
            (4, 4, 2, "50"),    # Подольск -> Подольский р-н
            (5, 5, 3, "50"),    # Одинцово -> Одинцовский р-н
            (6, 6, 4, "50"),    # Климовск -> Подольск
            (7, 7, 5, "50"),    # Лесное -> Одинцово
            (10, 10, 4, "50"),  # ул.Ленина -> Подольск
            (11, 11, 4, "50"),  # пр-кт Мира -> Подольск
            (12, 12, 5, "50"),  # ул.Садовая -> Одинцово
            (13, 13, 7, "50"),  # ул.Центральная -> Лесное
        ]
        for hid, oid, pid, rc in demo_hier:
            f.write(f'<ITEM ID="{hid}" OBJECTID="{oid}" PARENTOBJID="{pid}" '
                    f'REGIONCODE="{rc}" ISACTIVE="1"/>\n')
        f.write('</ITEMS>\n')

    # AS_HOUSES.xml
    with open(os.path.join(demo_dir, "AS_HOUSES.xml"), 'w', encoding='utf-8') as f:
        f.write('<?xml version="1.0" encoding="utf-8"?>\n<HOUSES>\n')
        demo_houses = [
            (100, "house-guid-001", "1", 2),
            (101, "house-guid-002", "2", 2),
            (102, "house-guid-003", "3А", 2),
            (103, "house-guid-004", "15", 2),
            (104, "house-guid-005", "7", 2),
        ]
        # Добавим иерархию домов в основной файл
        for hid, guid, num, ht in demo_houses:
            f.write(f'<HOUSE OBJECTID="{hid}" OBJECTGUID="{guid}" '
                    f'HOUSENUM="{num}" HOUSETYPE="{ht}" '
                    f'ISACTUAL="1" ISACTIVE="1"/>\n')
        f.write('</HOUSES>\n')

    # Дополним иерархию домами
    with open(os.path.join(demo_dir, "AS_ADM_HIERARCHY.xml"), 'r', encoding='utf-8') as f:
        content = f.read()
    house_hier = ""
    house_hier_items = [
        (100, 100, 10, "50"),  # дом 1 -> ул.Ленина
        (101, 101, 10, "50"),  # дом 2 -> ул.Ленина
        (102, 102, 10, "50"),  # дом 3А -> ул.Ленина
        (103, 103, 12, "50"),  # дом 15 -> ул.Садовая
        (104, 104, 13, "50"),  # дом 7 -> ул.Центральная
    ]
    for hid, oid, pid, rc in house_hier_items:
        house_hier += (f'<ITEM ID="{hid}" OBJECTID="{oid}" PARENTOBJID="{pid}" '
                       f'REGIONCODE="{rc}" ISACTIVE="1"/>\n')
    content = content.replace('</ITEMS>', house_hier + '</ITEMS>')
    with open(os.path.join(demo_dir, "AS_ADM_HIERARCHY.xml"), 'w', encoding='utf-8') as f:
        f.write(content)

    print(f"Демо-данные созданы в: {demo_dir}")
    print("\nТеперь запустите конвертацию:")
    print(f'  python converter.py gar2kladr --gar-dir "{demo_dir}" --output "{os.path.join(os.path.dirname(demo_dir), "output")}"')


# ====================================================================
# MAIN
# ====================================================================

def print_help():
    print("""
╔══════════════════════════════════════════════════════════╗
║          КОНВЕРТЕР ГАР <-> КЛАДР v1.0                   ║
╠══════════════════════════════════════════════════════════╣
║                                                          ║
║  Использование:                                          ║
║                                                          ║
║  1) ГАР -> КЛАДР:                                       ║
║     python converter.py gar2kladr                        ║
║       --gar-dir <путь к XML ГАР>                        ║
║       --output <путь для DBF>                            ║
║                                                          ║
║  2) КЛАДР -> ГАР:                                       ║
║     python converter.py kladr2gar                        ║
║       --kladr-dir <путь к DBF КЛАДР>                    ║
║       --output <путь для XML>                            ║
║                                                          ║
║  3) Создать демо-данные:                                 ║
║     python converter.py demo                             ║
║                                                          ║
║  Зависимости: pip install lxml dbfread                   ║
╚══════════════════════════════════════════════════════════╝
""")


def main():
    args = sys.argv[1:]

    if not args or args[0] in ('-h', '--help', 'help'):
        print_help()
        return

    cmd = args[0].lower()

    if cmd == 'demo':
        create_demo()
        return

    if cmd == 'gar2kladr':
        gar_dir = ""
        out_dir = ""
        for i, a in enumerate(args[1:], 1):
            if a == '--gar-dir' and i < len(args):
                gar_dir = args[i+1] if i+1 <= len(args) else ""
            elif a == '--output' and i < len(args):
                out_dir = args[i+1] if i+1 <= len(args) else ""

        # Парсим аргументы проще
        i = 1
        while i < len(args):
            if args[i] == '--gar-dir' and i+1 < len(args):
                gar_dir = args[i+1]; i += 2
            elif args[i] == '--output' and i+1 < len(args):
                out_dir = args[i+1]; i += 2
            else:
                i += 1

        if not gar_dir:
            gar_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                   "data", "gar_input")
        if not out_dir:
            out_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                   "data", "output")

        conv = GarToKladr(gar_dir, out_dir)
        conv.convert()

    elif cmd == 'kladr2gar':
        kladr_dir = ""
        out_dir = ""
        i = 1
        while i < len(args):
            if args[i] == '--kladr-dir' and i+1 < len(args):
                kladr_dir = args[i+1]; i += 2
            elif args[i] == '--output' and i+1 < len(args):
                out_dir = args[i+1]; i += 2
            else:
                i += 1

        if not kladr_dir:
            kladr_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                     "data", "kladr_input")
        if not out_dir:
            out_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                   "data", "output")

        conv = KladrToGar(kladr_dir, out_dir)
        conv.convert()

    else:
        print(f"Неизвестная команда: {cmd}")
        print_help()


if __name__ == '__main__':
    main()
'@

Set-Content -Path "$root\converter.py" -Value $pyContent -Encoding UTF8
Write-Host "`nСоздан: converter.py" -ForegroundColor Yellow

# Скрипт быстрого запуска
$runContent = @"
@echo off
chcp 65001 >nul
echo ========================================
echo  Конвертер ГАР - КЛАДР
echo ========================================
echo.
echo 1. Создать демо-данные
echo 2. ГАР -^> КЛАДР (из data\gar_input)
echo 3. КЛАДР -^> ГАР (из data\kladr_input)
echo 4. Установить зависимости
echo 5. Выход
echo.
set /p choice="Выберите (1-5): "

if "%choice%"=="1" python converter.py demo
if "%choice%"=="2" python converter.py gar2kladr
if "%choice%"=="3" python converter.py kladr2gar
if "%choice%"=="4" pip install lxml dbfread
if "%choice%"=="5" exit

pause
"@
Set-Content -Path "$root\run.bat" -Value $runContent -Encoding Default
Write-Host "Создан: run.bat" -ForegroundColor Yellow

Write-Host "`n========================================" -ForegroundColor White
Write-Host " ПРОЕКТ СОЗДАН УСПЕШНО!" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor White
Write-Host "`nСтруктура:" -ForegroundColor Cyan
Write-Host "  $root\"
Write-Host "    converter.py     - основной скрипт"
Write-Host "    run.bat          - меню запуска"
Write-Host "    requirements.txt - зависимости"
Write-Host "    data\"
Write-Host "      gar_input\     - сюда XML файлы ГАР"
Write-Host "      kladr_input\   - сюда DBF файлы КЛАДР"
Write-Host "      output\        - результат конвертации"
Write-Host ""
Write-Host "Следующие шаги:" -ForegroundColor Yellow
Write-Host "  1. pip install lxml dbfread"
Write-Host "  2. cd `"$root`""
Write-Host "  3. python converter.py demo        # создать тестовые данные"
Write-Host "  4. python converter.py gar2kladr   # конвертировать ГАР->КЛАДР"
Write-Host "  5. python converter.py kladr2gar   # конвертировать КЛАДР->ГАР"
Write-Host ""