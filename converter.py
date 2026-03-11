#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Конвертер ГАР <-> КЛАДР через официальный API ФИАС (SPAS API v2.0)
https://fias.nalog.ru/api/spas/v2.0/

API возвращает реальные данные ГАР с kladr_code, okato, oktmo, ifns и т.д.
"""
import json, urllib.request, urllib.parse, ssl, time, sys

API = "https://fias.nalog.ru/api/spas/v2.0"
CTX = ssl.create_default_context()
CTX.check_hostname = False
CTX.verify_mode = ssl.CERT_NONE


def api_get(endpoint, params=None):
    """GET-запрос к SPAS API."""
    url = f"{API}/{endpoint}"
    if params:
        url += "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url)
    req.add_header('User-Agent', 'GAR-KLADR-Converter/1.0')
    req.add_header('Accept', 'application/json')
    with urllib.request.urlopen(req, context=CTX, timeout=30) as resp:
        return json.loads(resp.read().decode('utf-8'))


def api_post(endpoint, data):
    """POST-запрос к SPAS API."""
    url = f"{API}/{endpoint}"
    body = json.dumps(data).encode('utf-8')
    req = urllib.request.Request(url, data=body, method='POST')
    req.add_header('User-Agent', 'GAR-KLADR-Converter/1.0')
    req.add_header('Content-Type', 'application/json')
    req.add_header('Accept', 'application/json')
    with urllib.request.urlopen(req, context=CTX, timeout=30) as resp:
        return json.loads(resp.read().decode('utf-8'))


def get_regions():
    """Получить список всех регионов."""
    return api_get("GetRegions")


def get_children(path, level=None, name_part=None, address_type=1):
    """Получить дочерние элементы."""
    data = {"path": path, "address_type": address_type}
    if level is not None:
        data["address_level"] = level
    if name_part:
        data["name_part"] = name_part
    return api_post("GetAddressItems", data)


def get_details(object_id):
    """Получить детали объекта (kladr_code, okato, oktmo и т.д.)."""
    return api_get("GetDetails", {"object_id": object_id})


def get_by_id(object_id, address_type=1):
    """Получить объект по ID."""
    return api_get("GetAddressItemById", {
        "object_id": object_id, "address_type": address_type})


def get_by_guid(guid, address_type=1):
    """Получить объект по GUID."""
    return api_get("GetAddressItemByGuid", {
        "object_guid": guid, "address_type": address_type})


def search_address(search_string, address_type=1):
    """Поиск адреса по строке."""
    return api_get("SearchAddressItems", {
        "search_string": search_string, "address_type": address_type})


def search_single(search_string, address_type=1):
    """Найти один адресный элемент по строке."""
    return api_get("SearchAddressItem", {
        "search_string": search_string, "address_type": address_type})


def get_hints(search_string, address_type=1):
    """Подсказки при вводе адреса."""
    return api_get("GetAddressHint", {
        "search_string": search_string, "address_type": address_type})


def search_by_parts(**kwargs):
    """Поиск по частям адреса."""
    data = {}
    for key in ["region", "district", "city", "settlement", "street"]:
        if key in kwargs and kwargs[key]:
            val = kwargs[key]
            if isinstance(val, str):
                data[key] = {"name": val}
            else:
                data[key] = val
    if "house" in kwargs and kwargs["house"]:
        h = kwargs["house"]
        if isinstance(h, str):
            data["house"] = {"number": h}
        else:
            data["house"] = h
    if "kladr_code" in kwargs:
        data["kladr_code"] = kwargs["kladr_code"]
    if "postal_code" in kwargs:
        data["postal_code"] = kwargs["postal_code"]
    return api_post("SearchByParts", data)


def get_object_types():
    """Получить типы объектов ФИАС."""
    return api_get("GetFiasObjectTypes")


# ====================================================================
# КОНВЕРТАЦИЯ
# ====================================================================

def gar_to_kladr(object_id=None, guid=None, search=None):
    """
    ГАР -> КЛАДР: по object_id, GUID или строке адреса
    получить kladr_code и все реквизиты КЛАДР.
    """
    addr = None

    if object_id:
        result = get_by_id(object_id)
        if result.get("addresses"):
            addr = result["addresses"][0]

    elif guid:
        result = get_by_guid(guid)
        if result.get("addresses"):
            addr = result["addresses"][0]

    elif search:
        result = search_address(search)
        if result.get("addresses"):
            addr = result["addresses"][0]

    if not addr:
        return {"error": "Объект не найден"}

    # Получаем детали
    details = {}
    try:
        det = get_details(addr["object_id"])
        details = det.get("address_details", {})
    except:
        details = addr.get("address_details", {})

    return {
        "gar": {
            "object_id": addr.get("object_id"),
            "object_guid": addr.get("object_guid"),
            "full_name": addr.get("full_name"),
            "level": addr.get("object_level_id"),
            "region_code": addr.get("region_code"),
            "is_active": addr.get("is_active"),
            "path": addr.get("path"),
        },
        "kladr": {
            "kladr_code": details.get("kladr_code", ""),
            "postal_code": details.get("postal_code", ""),
            "okato": details.get("okato", ""),
            "oktmo": details.get("oktmo", ""),
            "ifns_fl": details.get("ifns_fl", ""),
            "ifns_ul": details.get("ifns_ul", ""),
            "ifns_tfl": details.get("ifns_tfl", ""),
            "ifns_tul": details.get("ifns_tul", ""),
            "cadastral_number": details.get("cadastral_number", ""),
        },
        "hierarchy": [
            {
                "level": h.get("object_level_id"),
                "name": h.get("full_name"),
                "short": h.get("full_name_short"),
                "kladr_code": h.get("kladr_code"),
                "type": h.get("object_type"),
            }
            for h in addr.get("hierarchy", [])
        ]
    }


def kladr_to_gar(kladr_code):
    """
    КЛАДР -> ГАР: по коду КЛАДР найти объект ГАР.
    """
    result = search_by_parts(kladr_code=kladr_code)

    if result.get("error"):
        return {"error": result["error"], "description": result.get("description", "")}

    addr = result.get("address_item")
    if not addr:
        return {"error": "Объект не найден по КЛАДР коду " + kladr_code}

    details = addr.get("address_details", {})

    return {
        "gar": {
            "object_id": addr.get("object_id"),
            "object_guid": addr.get("object_guid"),
            "full_name": addr.get("full_name"),
            "level": addr.get("object_level_id"),
            "region_code": addr.get("region_code"),
            "is_active": addr.get("is_active"),
            "path": addr.get("path"),
        },
        "kladr": {
            "kladr_code": details.get("kladr_code", kladr_code),
            "postal_code": details.get("postal_code", ""),
            "okato": details.get("okato", ""),
            "oktmo": details.get("oktmo", ""),
            "ifns_fl": details.get("ifns_fl", ""),
            "ifns_ul": details.get("ifns_ul", ""),
        },
        "hierarchy": [
            {
                "level": h.get("object_level_id"),
                "name": h.get("full_name"),
                "short": h.get("full_name_short"),
                "kladr_code": h.get("kladr_code"),
                "type": h.get("object_type"),
            }
            for h in addr.get("hierarchy", [])
        ]
    }


def batch_kladr_to_gar(kladr_codes):
    """Массовая конвертация КЛАДР -> ГАР."""
    results = []
    for i, code in enumerate(kladr_codes):
        print(f"  [{i+1}/{len(kladr_codes)}] {code}...", end=" ", flush=True)
        try:
            r = kladr_to_gar(code)
            if "error" not in r:
                print(f"-> {r['gar']['full_name']} (GUID: {r['gar']['object_guid']})")
            else:
                print(f"-> {r['error']}")
            results.append({"kladr_code": code, "result": r})
        except Exception as e:
            print(f"-> ОШИБКА: {e}")
            results.append({"kladr_code": code, "result": {"error": str(e)}})
        time.sleep(0.3)  # Пауза чтобы не нагружать API
    return results


def batch_gar_to_kladr(object_ids):
    """Массовая конвертация ГАР -> КЛАДР."""
    results = []
    for i, oid in enumerate(object_ids):
        print(f"  [{i+1}/{len(object_ids)}] ID={oid}...", end=" ", flush=True)
        try:
            r = gar_to_kladr(object_id=oid)
            if "error" not in r:
                print(f"-> КЛАДР: {r['kladr']['kladr_code']} ({r['gar']['full_name']})")
            else:
                print(f"-> {r['error']}")
            results.append({"object_id": oid, "result": r})
        except Exception as e:
            print(f"-> ОШИБКА: {e}")
            results.append({"object_id": oid, "result": {"error": str(e)}})
        time.sleep(0.3)
    return results


def export_region_kladr(region_code, max_depth=4):
    """Выгрузить все объекты региона с КЛАДР кодами."""
    print(f"\nВыгрузка региона {region_code}...")

    regions = get_regions()
    region = None
    for r in regions.get("addresses", []):
        if r.get("region_code") == region_code:
            region = r
            break

    if not region:
        print(f"Регион {region_code} не найден!")
        return []

    print(f"Регион: {region['full_name']}")
    all_objects = []

    def collect(path, depth=0):
        if depth >= max_depth:
            return
        try:
            children = get_children(path)
            for addr in children.get("addresses", []):
                det = addr.get("address_details", {})
                obj = {
                    "object_id": addr.get("object_id"),
                    "object_guid": addr.get("object_guid"),
                    "full_name": addr.get("full_name"),
                    "level": addr.get("object_level_id"),
                    "kladr_code": det.get("kladr_code", ""),
                    "okato": det.get("okato", ""),
                    "oktmo": det.get("oktmo", ""),
                    "postal_code": det.get("postal_code", ""),
                    "ifns": det.get("ifns_fl", ""),
                    "path": addr.get("path", ""),
                }
                all_objects.append(obj)
                print(f"  [{len(all_objects)}] {obj['full_name']} | КЛАДР: {obj['kladr_code']}")

                if addr.get("path"):
                    time.sleep(0.2)
                    collect(addr["path"], depth + 1)
        except Exception as e:
            print(f"  Ошибка: {e}")

    collect(region["path"], 0)
    return all_objects


# ====================================================================
# CLI
# ====================================================================

def pp(data):
    """Pretty print JSON."""
    print(json.dumps(data, ensure_ascii=False, indent=2))


def main():
    args = sys.argv[1:]

    if not args or args[0] in ('-h', '--help', 'help'):
        print("""
╔══════════════════════════════════════════════════════════════╗
║       КОНВЕРТЕР ГАР <-> КЛАДР (через API ФИАС)             ║
║       Реальные данные с https://fias.nalog.ru               ║
╠══════════════════════════════════════════════════════════════╣
║                                                              ║
║  Команды:                                                    ║
║                                                              ║
║  regions                  - список регионов                  ║
║  search <адрес>           - поиск адреса                     ║
║  hint <начало адреса>     - подсказки ввода                  ║
║                                                              ║
║  gar2kladr <object_id>    - ГАР ID -> КЛАДР код             ║
║  guid2kladr <guid>        - ГАР GUID -> КЛАДР код           ║
║  addr2kladr <адрес>       - адрес строкой -> КЛАДР           ║
║                                                              ║
║  kladr2gar <kladr_code>   - КЛАДР код -> ГАР                ║
║                                                              ║
║  details <object_id>      - детали объекта                   ║
║  children <path>          - дочерние элементы                ║
║  types                    - типы объектов ФИАС               ║
║                                                              ║
║  export <region_code>     - выгрузка региона                 ║
║                                                              ║
║  Примеры:                                                    ║
║  python converter.py regions                                 ║
║  python converter.py search "Москва Тверская 1"              ║
║  python converter.py kladr2gar 7700000000000                 ║
║  python converter.py addr2kladr "г Москва ул Тверская"       ║
║  python converter.py export 77                               ║
╚══════════════════════════════════════════════════════════════╝
""")
        return

    cmd = args[0].lower()

    try:
        if cmd == 'regions':
            data = get_regions()
            for addr in data.get("addresses", []):
                det = addr.get("address_details", {})
                kc = det.get("kladr_code", "") if det else ""
                print(f"  {addr.get('region_code',0):02d} | "
                      f"ID:{addr['object_id']:>8} | "
                      f"КЛАДР:{kc:>15} | "
                      f"{addr['full_name']}")

        elif cmd == 'search' and len(args) > 1:
            query = " ".join(args[1:])
            print(f"Поиск: {query}\n")
            data = search_address(query)
            for addr in data.get("addresses", []):
                det = addr.get("address_details", {})
                kc = det.get("kladr_code", "") if det else ""
                print(f"  ID:{addr['object_id']} | GUID:{addr.get('object_guid','')} | "
                      f"КЛАДР:{kc} | {addr['full_name']}")

        elif cmd == 'hint' and len(args) > 1:
            query = " ".join(args[1:])
            data = get_hints(query)
            for h in data.get("hints", []):
                print(f"  ID:{h.get('object_id','')} | {h.get('full_name','')}")

        elif cmd == 'gar2kladr' and len(args) > 1:
            result = gar_to_kladr(object_id=int(args[1]))
            pp(result)

        elif cmd == 'guid2kladr' and len(args) > 1:
            result = gar_to_kladr(guid=args[1])
            pp(result)

        elif cmd == 'addr2kladr' and len(args) > 1:
            query = " ".join(args[1:])
            result = gar_to_kladr(search=query)
            pp(result)

        elif cmd == 'kladr2gar' and len(args) > 1:
            result = kladr_to_gar(args[1])
            pp(result)

        elif cmd == 'details' and len(args) > 1:
            data = get_details(int(args[1]))
            pp(data)

        elif cmd == 'children' and len(args) > 1:
            path = args[1]
            data = get_children(path)
            for addr in data.get("addresses", []):
                det = addr.get("address_details", {})
                kc = det.get("kladr_code", "") if det else ""
                print(f"  ID:{addr['object_id']:>8} | L{addr.get('object_level_id',0)} | "
                      f"КЛАДР:{kc:>17} | {addr['full_name']}")
                print(f"    path: {addr.get('path','')}")

        elif cmd == 'types':
            data = get_object_types()
            for t in data.get("types", []):
                if t.get("is_active"):
                    print(f"  L{t['address_level']:>2} | {t['type_short_name']:>10} | {t['type_name']}")

        elif cmd == 'export' and len(args) > 1:
            rc = int(args[1])
            depth = int(args[2]) if len(args) > 2 else 3
            objects = export_region_kladr(rc, depth)
            # Сохраняем в JSON
            outfile = f"region_{rc:02d}.json"
            with open(outfile, 'w', encoding='utf-8') as f:
                json.dump(objects, f, ensure_ascii=False, indent=2)
            print(f"\nСохранено {len(objects)} объектов в {outfile}")

        else:
            print(f"Неизвестная команда: {cmd}")
            print("Используйте: python converter.py --help")

    except urllib.error.HTTPError as e:
        print(f"Ошибка HTTP {e.code}: {e.reason}")
        try:
            print(e.read().decode())
        except:
            pass
    except urllib.error.URLError as e:
        print(f"Ошибка сети: {e.reason}")
    except Exception as e:
        print(f"Ошибка: {e}")
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    main()