#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
家宽订阅 · 全流程脚本（GitHub Actions 里跑）
抓公开代理池 -> 去重 -> 真握手测活 -> GEO 判归属 -> 判家宽/机房 -> 生成订阅 + 页面

用法:
    python run_all.py                      # 正常跑
    SKIP_PROBE=1 python run_all.py         # 跳过测活（本地网络摸不到代理端口时用）
    CANDIDATES=1500 python run_all.py      # 控制候选数量
"""
import base64
import collections
import json
import os
import random
import re
import socket
import sys
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                    "Chrome/124 Safari/537.36"}

OUT_DIR = os.environ.get("OUT_DIR", "docs")
CANDIDATES = int(os.environ.get("CANDIDATES", "5000"))
PROBE_THREADS = int(os.environ.get("PROBE_THREADS", "200"))
PROBE_TIMEOUT = float(os.environ.get("PROBE_TIMEOUT", "5"))
SKIP_PROBE = os.environ.get("SKIP_PROBE", "0") == "1"
SEED = int(os.environ.get("SEED", "20261124"))

SOURCES = [
    ("speedx-socks5", "socks5", "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/socks5.txt"),
    ("speedx-http", "http", "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt"),
    ("monosans-socks5", "socks5", "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks5.txt"),
    ("monosans-http", "http", "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt"),
    ("hookzof", "socks5", "https://raw.githubusercontent.com/hookzof/socks5_list/master/proxy.txt"),
    ("proxifly-socks5", "socks5", "https://raw.githubusercontent.com/proxifly/free-proxy-list/main/proxies/protocols/socks5/data.txt"),
    ("proxifly-http", "http", "https://raw.githubusercontent.com/proxifly/free-proxy-list/main/proxies/protocols/http/data.txt"),
    ("shiftytr-socks5", "socks5", "https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/socks5.txt"),
    ("shiftytr-http", "http", "https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/http.txt"),
    ("roosterkid-socks5", "socks5", "https://raw.githubusercontent.com/roosterkid/openproxylist/main/SOCKS5_RAW.txt"),
    ("roosterkid-http", "http", "https://raw.githubusercontent.com/roosterkid/openproxylist/main/HTTPS_RAW.txt"),
    ("zloi-socks5", "socks5", "https://raw.githubusercontent.com/zloi-user/hideip.me/main/socks5.txt"),
    ("zloi-http", "http", "https://raw.githubusercontent.com/zloi-user/hideip.me/main/http.txt"),
    ("mmpx12-socks5", "socks5", "https://raw.githubusercontent.com/mmpx12/proxy-list/master/socks5.txt"),
    ("mmpx12-http", "http", "https://raw.githubusercontent.com/mmpx12/proxy-list/master/http.txt"),
    ("proxyscrape-socks5", "socks5", "https://api.proxyscrape.com/v2/?request=getproxies&protocol=socks5&timeout=10000&country=all"),
    ("proxyscrape-http", "http", "https://api.proxyscrape.com/v2/?request=getproxies&protocol=http&timeout=10000&country=all"),
    ("openproxy-socks5", "socks5", "https://openproxylist.xyz/socks5.txt"),
    ("openproxy-http", "http", "https://openproxylist.xyz/http.txt"),
    ("vakhov-socks5", "socks5", "https://raw.githubusercontent.com/vakhov/fresh-proxy-list/master/socks5.txt"),
    ("ellite-socks5", "socks5", "https://raw.githubusercontent.com/elliottopnell/free-proxy-list/main/socks5.txt"),
]

IPPORT = re.compile(r"^(\d{1,3}(?:\.\d{1,3}){3}):(\d{2,5})")

# ---------------- 抓取 ----------------

def parse(text, proto):
    out = []
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#") or line.startswith("//"):
            continue
        line = re.sub(r"^(socks5h?|socks4a?|https?)://", "", line, flags=re.I)
        m = IPPORT.match(line)
        if not m:
            continue
        ip, port = m.group(1), int(m.group(2))
        if not (1 <= port <= 65535):
            continue
        if ip.startswith(("10.", "127.", "192.168.", "0.", "169.254.")):
            continue
        out.append((ip, port, proto))
    return out


def fetch_all():
    pool, stats = {}, {}
    with ThreadPoolExecutor(max_workers=10) as ex:
        futs = {ex.submit(_fetch_one, name, proto, url): name for name, proto, url in SOURCES}
        for f in as_completed(futs):
            name = futs[f]
            try:
                items = f.result()
            except Exception as e:
                stats[name] = "ERR " + type(e).__name__
                print(f"[src] {name:<22} ERR {type(e).__name__}", flush=True)
                continue
            stats[name] = len(items)
            for ip, port, p in items:
                key = (ip, port)
                if key not in pool:
                    pool[key] = [p, 0]
                pool[key][1] += 1
            print(f"[src] {name:<22} {len(items):>6}", flush=True)
    return pool, stats


def _fetch_one(name, proto, url):
    r = requests.get(url, headers=UA, timeout=40)
    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code}")
    return parse(r.text, proto)


# ---------------- 真握手测活 ----------------
# 这一环是本地沙箱做不到、GitHub 的机器能做的：直连代理端口，跑一次真握手。

def _http_ok(s, target_host="www.gstatic.com", target_port=80):
    req = (f"CONNECT {target_host}:{target_port} HTTP/1.1\r\n"
           f"Host: {target_host}:{target_port}\r\n\r\n").encode()
    s.sendall(req)
    buf = b""
    while len(buf) < 200:
        chunk = s.recv(200 - len(buf))
        if not chunk:
            break
        buf += chunk
        if b"\r\n\r\n" in buf:
            break
    line = buf.split(b"\r\n", 1)[0].decode("latin1", "ignore")
    return " 200" in line


def _socks5_ok(s, target_host="www.gstatic.com", target_port=80):
    s.sendall(b"\x05\x01\x00")
    resp = s.recv(2)
    if len(resp) < 2 or resp[0] != 0x05 or resp[1] != 0x00:
        return False
    host = target_host.encode()
    req = b"\x05\x01\x00\x03" + bytes([len(host)]) + host + target_port.to_bytes(2, "big")
    s.sendall(req)
    resp = s.recv(4)
    if len(resp) < 4 or resp[1] != 0x00:
        return False
    atyp = resp[3]
    if atyp == 0x01:
        s.recv(6)
    elif atyp == 0x03:
        n = s.recv(1)
        s.recv(n[0] + 2)
    elif atyp == 0x04:
        s.recv(18)
    return True


def probe_one(item):
    ip, port, proto = item
    try:
        s = socket.create_connection((ip, port), timeout=PROBE_TIMEOUT)
    except Exception:
        return None
    try:
        s.settimeout(PROBE_TIMEOUT)
        ok = _socks5_ok(s) if proto == "socks5" else _http_ok(s)
        return item if ok else None
    except Exception:
        return None
    finally:
        try:
            s.close()
        except Exception:
            pass


def probe_all(items):
    alive, t0 = [], time.time()
    with ThreadPoolExecutor(max_workers=PROBE_THREADS) as ex:
        futs = [ex.submit(probe_one, it) for it in items]
        done = 0
        for f in as_completed(futs):
            done += 1
            r = f.result()
            if r:
                alive.append(r)
            if done % 300 == 0:
                print(f"[probe] {done}/{len(items)}  活 {len(alive)}  {time.time()-t0:.0f}s", flush=True)
    print(f"[probe] 完成：{len(alive)}/{len(items)} 存活  {time.time()-t0:.0f}s", flush=True)
    return alive


# ---------------- 家宽 / 机房 ----------------

HOSTING = ["amazon", "aws", "google", "microsoft", "azure", "digitalocean", "hetzner", "ovh", "vultr",
           "linode", "contabo", "alibaba", "aliyun", "tencent", "oracle", "leaseweb", "m247",
           "datacamp", "hostinger", "colocrossing", "psychz", "zenlayer", "kamatera", "akamai",
           "cloudflare", "choopa", "serverius", "ionos", "godaddy", "namecheap", "rackspace",
           "quadranet", "internap", "hostwinds", "hostnoc", "cherryservers", "scaleway", "upcloud",
           "interserver", "datacenter", "data center", "server", "hosting", "vps", "dedicated",
           "colo", "cdn", "edgecast", "fastly", "netlify", "vercel", "heroku", "github",
           "digital ocean", "gcore", "ddos-guard", "dmit", "bandwagon", "racknerd", "buyvm",
           "frantech", "cogent", "ntt communications", "level3", "gtt", "telia carrier", "lumen",
           "centric", "zayo", "crowncastle"]
RESI = ["comcast", "xfinity", "verizon", "at&t", "att ", "charter", "spectrum", "cox ", "centurylink",
        "frontier", "windstream", "mediacom", "cable one", "suddenlink", "optimum", "altice", "wow!",
        "cablevision", "deutsche telekom", "vodafone", "1&1", "telefonica", "movistar", "orange",
        "free sas", "bouygues", "sfr", "bt ", "british telecom", "sky broadband", "talktalk",
        "virgin media", "plusnet", "kpn", "ziggo", "xs4all", "proximus", "telenet", "telia", "tele2",
        "elisa", "telenor", "bahnhof", "com hem", "swisscom", "sunrise", "a1 telekom", "magenta",
        "kabel", "unitymedia", "netcologne", "ewe tel", "wilhelm.tel", "china telecom", "china unicom",
        "china mobile", "chinanet", "cernet", "ntt", "kddi", "softbank", "ocn", "biglobe", "so-net",
        "kt ", "korea telecom", "sk broadband", "lg u+", "sk telecom", "singtel", "starhub",
        "m1 limited", "maxis", "time dotcom", "true internet", "ais ", "3bb", "tot ", "pldt",
        "globe telecom", "converge", "viettel", "vnpt", "fpt telecom", "telkom", "indihome", "biznet",
        "first media", "turk telekom", "ttnet", "superonline", "rostelecom", "mts ", "beeline",
        "megafon", "kyivstar", "ukrtelecom", "telstra", "optus", "tpg", "iinet", "rogers",
        "bell canada", "telus", "videotron", "shaw ", "cogeco", "claro", "vivo", "tim ",
        "infostrada", "fastweb", "wind tre", "iliad", "sky italia", "airtel", "jio", "bsnl",
        "act fibernet", "hathway", "etisalat", "du ", "stc ", "mobily", "zain", "ooredoo", "batelco",
        "cyta", "cosmote", "ote ", "telekom", "siol", "telemach", "t-2", "starman", "elion", "tet ",
        "lattelecom", "telia latvia"]


def classify(isp, org, asn, hosting, mobile):
    blob = f"{isp} {org} {asn}".lower()
    if hosting:
        return "idc"
    hit_host = any(k in blob for k in HOSTING)
    hit_resi = any(k in blob for k in RESI)
    if mobile:
        return "resi"
    if hit_resi and not hit_host:
        return "resi"
    if hit_host and not hit_resi:
        return "idc"
    return "idc" if hit_host else "unknown"


def geo_batch(ips):
    res = {}
    url = ("http://ip-api.com/batch?fields=status,country,countryCode,isp,org,as,"
           "mobile,hosting,proxy,query")
    uniq = list(dict.fromkeys(ips))
    for i in range(0, len(uniq), 100):
        chunk = uniq[i:i + 100]
        for _ in range(3):
            try:
                r = requests.post(url, json=chunk, timeout=30)
                for d in r.json():
                    if d.get("status") == "success":
                        res[d["query"]] = d
                break
            except Exception:
                time.sleep(2)
        if (i // 100) % 5 == 0:
            print(f"[geo] {min(i+100, len(uniq))}/{len(uniq)}", flush=True)
        time.sleep(1.4)   # ip-api 免费额度 45 次/分钟
    return res


# ---------------- 生成订阅 ----------------

FLAG = lambda cc: "".join(chr(0x1F1E6 + ord(c) - 65) for c in cc) if len(cc) == 2 and cc.isalpha() else "🏴"

CN_NAME = {"US": "美国", "HK": "香港", "TW": "台湾", "JP": "日本", "SG": "新加坡", "KR": "韩国",
           "GB": "英国", "DE": "德国", "FR": "法国", "NL": "荷兰", "CA": "加拿大", "AU": "澳大利亚",
           "RU": "俄罗斯", "VN": "越南", "TH": "泰国", "ID": "印尼", "PH": "菲律宾", "TR": "土耳其",
           "IN": "印度", "BR": "巴西", "IT": "意大利", "ES": "西班牙", "SE": "瑞典", "FI": "芬兰",
           "PL": "波兰", "CH": "瑞士", "AT": "奥地利", "BE": "比利时", "DK": "丹麦", "NO": "挪威",
           "IE": "爱尔兰", "PT": "葡萄牙", "CZ": "捷克", "RO": "罗马尼亚", "HU": "匈牙利",
           "UA": "乌克兰", "MX": "墨西哥", "AR": "阿根廷", "CL": "智利", "ZA": "南非", "AE": "阿联酋",
           "IL": "以色列", "MY": "马来西亚", "EG": "埃及", "RS": "塞尔维亚", "BG": "保加利亚",
           "GR": "希腊", "LV": "拉脱维亚", "LT": "立陶宛", "EE": "爱沙尼亚", "SK": "斯洛伐克",
           "HR": "克罗地亚", "SI": "斯洛文尼亚", "NZ": "新西兰", "PK": "巴基斯坦", "BD": "孟加拉",
           "NG": "尼日利亚", "KE": "肯尼亚", "PE": "秘鲁", "CO": "哥伦比亚", "EC": "厄瓜多尔",
           "MD": "摩尔多瓦", "GE": "格鲁吉亚", "AL": "阿尔巴尼亚", "BA": "波黑", "CY": "塞浦路斯",
           "LU": "卢森堡", "IS": "冰岛", "MT": "马耳他", "CN": "中国", "MO": "澳门", "KZ": "哈萨克",
           "SA": "沙特", "QA": "卡塔尔", "OM": "阿曼", "JO": "约旦", "AM": "亚美尼亚",
           "AZ": "阿塞拜疆", "BY": "白俄罗斯", "CR": "哥斯达黎加", "PA": "巴拿马", "UY": "乌拉圭",
           "PR": "波多黎各", "TN": "突尼斯", "MA": "摩洛哥", "NP": "尼泊尔", "LK": "斯里兰卡",
           "MM": "缅甸", "KH": "柬埔寨", "MN": "蒙古", "ME": "黑山", "MK": "北马其顿", "XK": "科索沃", "BO": "玻利维亚", "VE": "委内瑞拉", "DO": "多米尼加", "GT": "危地马拉", "TZ": "坦桑尼亚", "GH": "加纳", "ZW": "津巴布韦", "LA": "老挝", "MM2": "缅甸"}

PREF = ["US", "JP", "HK", "SG", "TW", "KR", "GB", "DE", "NL", "CA", "FR", "AU", "RU", "IT", "SE",
        "CH", "PL", "FI", "TR", "IN", "BR", "VN", "TH", "ID", "PH", "AE", "IL", "MY", "ES", "BE",
        "AT", "DK", "IE", "NO", "PT", "CZ", "RO", "HU", "UA", "MX", "AR", "ZA", "NZ", "LT", "LV",
        "EE", "GR", "HR", "BG", "RS", "SK", "SI", "LU", "IS", "MD", "GE", "CL", "CO", "PE", "EC"]
ORDER = {c: i for i, c in enumerate(PREF)}
cname = lambda cc: CN_NAME.get(cc, cc)


def clean_isp(isp):
    s = (isp or "未知").strip()
    for bad in ("株式会社", "（", "）", "(", ")"):
        s = s.replace(bad, " ")
    for bad in (",", ":", "#", "|"):
        s = s.replace(bad, " ")
    return " ".join(s.split())[:24].strip()


def build(nodes, pool, stats_src, out_dir=OUT_DIR):
    resi = [n for n in nodes if n["cls"] == "resi" and n["cc"] != "CN"]
    idc = [n for n in nodes if n["cls"] == "idc" and n["cc"] != "CN"]
    if not resi:
        raise SystemExit("[build] 本轮没筛出可用家宽节点，保留上一版订阅不动（不部署）。"
                         "可以手动重跑一次，或把 CANDIDATES 调大。")
    idc = [n for n in idc if n["cc"] in PREF]

    def prio(n):
        return (ORDER.get(n["cc"], 999), 0 if n["proto"] == "socks5" else 1,
                -int(n.get("hits", 1)), n["isp"])

    resi.sort(key=prio)
    idc.sort(key=prio)
    idc = idc[:220]

    used = set()

    def uniq(name):
        base, i = name, 2
        while name in used:
            name = f"{base} {i}"
            i += 1
        used.add(name)
        return name

    proxies, node_meta = [], []
    for group, items, kind in (("resi", resi, "家"), ("idc", idc, "房")):
        idx = collections.Counter()
        for n in items:
            idx[n["cc"]] += 1
            name = uniq(f"{FLAG(n['cc'])} {n['cc']}·{kind}·{clean_isp(n['isp'])}{idx[n['cc']]:02d}")
            proxies.append({"name": name, "type": "socks5" if n["proto"] == "socks5" else "http",
                            "server": n["ip"], "port": n["port"]})
            node_meta.append({"name": name, "cc": n["cc"], "country": cname(n["cc"]), "cls": group,
                              "isp": n["isp"], "proto": n["proto"], "ip": n["ip"], "port": n["port"]})

    resi_names = [p["name"] for p in proxies if "·家·" in p["name"]]
    idc_names = [p["name"] for p in proxies if "·房·" in p["name"]]

    by_cc = collections.defaultdict(list)
    for m in node_meta:
        if m["cls"] == "resi":
            by_cc[m["cc"]].append(m["name"])
    cc_groups = [(cc, sorted(v)) for cc, v in sorted(by_cc.items(),
                 key=lambda kv: (ORDER.get(kv[0], 999), -len(kv[1]))) if len(v) >= 3][:14]

    groups = []
    head = ["♻️ 家宽自动"] + (["♻️ 机房自动"] if idc_names else []) + ["DIRECT"]
    head += [f"{FLAG(cc)} {cname(cc)}" for cc, _ in cc_groups]
    groups.append({"name": "🚀 节点选择", "type": "select", "proxies": head})
    groups.append({"name": "♻️ 家宽自动", "type": "url-test", "url": "http://www.gstatic.com/generate_204",
                   "interval": 300, "tolerance": 60, "lazy": False, "proxies": resi_names})
    if idc_names:
        groups.append({"name": "♻️ 机房自动", "type": "url-test",
                       "url": "http://www.gstatic.com/generate_204", "interval": 300,
                       "tolerance": 60, "lazy": True, "proxies": idc_names})
    for cc, names in cc_groups:
        groups.append({"name": f"{FLAG(cc)} {cname(cc)}", "type": "url-test",
                       "url": "http://www.gstatic.com/generate_204", "interval": 300,
                       "tolerance": 60, "lazy": True, "proxies": names})

    ts = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(time.time() + 8 * 3600))
    RULES = """
rules:
  - DOMAIN-SUFFIX,local,DIRECT
  - IP-CIDR,127.0.0.0/8,DIRECT,no-resolve
  - IP-CIDR,192.168.0.0/16,DIRECT,no-resolve
  - IP-CIDR,10.0.0.0/8,DIRECT,no-resolve
  - GEOIP,CN,DIRECT
  - MATCH,🚀 节点选择
"""

    def render(ps, gs, title):
        L = [f"# {title}\n", f"# 生成时间(北京): {ts}\n",
             "# 客户端订阅更新间隔建议 6 小时\n",
             "mixed-port: 7890\nallow-lan: false\nmode: rule\nlog-level: warning\nipv6: false\n",
             "proxies:\n"]
        for p in ps:
            L.append('  - {name: %s, type: %s, server: %s, port: %d, udp: false}\n'
                     % (json.dumps(p["name"], ensure_ascii=False), p["type"], p["server"], p["port"]))
        L.append("\nproxy-groups:\n")
        for g in gs:
            L.append("  - name: %s\n    type: %s\n"
                     % (json.dumps(g["name"], ensure_ascii=False), g["type"]))
            if g["type"] == "url-test":
                L.append("    url: %s\n    interval: %d\n    tolerance: %d\n    lazy: %s\n"
                         % (g["url"], g["interval"], g["tolerance"],
                            "false" if not g["lazy"] else "true"))
            L.append("    proxies:\n")
            for n in g["proxies"]:
                L.append("      - %s\n" % json.dumps(n, ensure_ascii=False))
        L.append(RULES)
        return "".join(L)

    clash = render(proxies, groups, "家宽出口订阅 · 自动更新")

    # 精简版：美国家宽全留 + 各国最多 3 条
    lite_sel, cnt = [], collections.Counter()
    for m in node_meta:
        if m["cls"] != "resi":
            continue
        if m["cc"] == "US":
            lite_sel.append(m)
        elif cnt[m["cc"]] < 3:
            cnt[m["cc"]] += 1
            lite_sel.append(m)
    name2proxy = {p["name"]: p for p in proxies}
    lite_proxies = [name2proxy[m["name"]] for m in lite_sel]
    lite_groups = [
        {"name": "🚀 节点选择", "type": "select", "proxies": ["♻️ 家宽自动", "DIRECT"]},
        {"name": "♻️ 家宽自动", "type": "url-test", "url": "http://www.gstatic.com/generate_204",
         "interval": 180, "tolerance": 60, "lazy": False,
         "proxies": [m["name"] for m in lite_sel]},
    ]
    lite = render(lite_proxies, lite_groups, "精简版 · 美国家宽全留 + 各国代表节点")

    # v2ray / NekoBox：base64 的 socks 链接
    lines = []
    for m in node_meta:
        if m["cls"] != "resi":
            continue
        enc = base64.b64encode(f"{m['ip']}:{m['port']}".encode()).decode()
        lines.append(f"socks://{enc}#{urllib.parse.quote(m['name'])}")
    v2ray = base64.b64encode("\n".join(lines).encode()).decode()

    # sing-box
    outs = [{"type": "socks" if p["type"] == "socks5" else "http", "tag": p["name"],
             "server": p["server"], "server_port": p["port"]} for p in proxies]
    sb = {
        "log": {"level": "warn"},
        "inbounds": [{"type": "mixed", "tag": "mixed-in", "listen": "127.0.0.1", "listen_port": 2080}],
        "outbounds": [
            {"type": "selector", "tag": "🚀 节点选择",
             "outbounds": ["♻️ 家宽自动"] + (["♻️ 机房自动"] if idc_names else []) + ["DIRECT"]},
            {"type": "urltest", "tag": "♻️ 家宽自动", "outbounds": resi_names[:400],
             "url": "http://www.gstatic.com/generate_204", "interval": "5m", "tolerance": 60},
        ] + outs + [{"type": "direct", "tag": "DIRECT"}],
        "route": {"rules": [{"geoip": "cn", "outbound": "DIRECT"}], "final": "🚀 节点选择"},
    }
    if idc_names:
        sb["outbounds"].insert(2, {"type": "urltest", "tag": "♻️ 机房自动", "outbounds": idc_names[:200],
                                   "url": "http://www.gstatic.com/generate_204",
                                   "interval": "5m", "tolerance": 60})

    ctry = [{"cc": cc, "name": cname(cc), "flag": FLAG(cc), "count": len(v)}
            for cc, v in sorted(by_cc.items(), key=lambda kv: (ORDER.get(kv[0], 999), -len(kv[1])))]

    data = {
        "stats": {"generated": int(time.time()), "raw": pool["total_raw"],
                  "alive": pool["alive"], "resi": len(resi), "idc": len(idc),
                  "countries": len(by_cc), "probed": pool.get("probed", 0),
                  "lite": len(lite_sel)},
        "countries": ctry,
        "nodes": node_meta,
        "sources": stats_src,
        "built_at": ts,
    }

    os.makedirs(f"{out_dir}/sub", exist_ok=True)
    open(f"{out_dir}/sub/clash.yaml", "w", encoding="utf-8").write(clash)
    open(f"{out_dir}/sub/clash-lite.yaml", "w", encoding="utf-8").write(lite)
    open(f"{out_dir}/sub/v2ray.txt", "w", encoding="utf-8").write(v2ray)
    json.dump(sb, open(f"{out_dir}/sub/singbox.json", "w", encoding="utf-8"),
              ensure_ascii=False, indent=1)
    json.dump(data, open(f"{out_dir}/data.json", "w", encoding="utf-8"), ensure_ascii=False)
    open(f"{out_dir}/.nojekyll", "w").write("")
    write_index(data, out_dir)

    print(f"[build] 家宽 {len(resi)} / 机房 {len(idc)} / 国家 {len(by_cc)} / 精简 {len(lite_sel)}")
    return data


# ---------------- 页面 ----------------

INDEX_HTML = """<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>家宽订阅 · 自动更新</title>
<style>
*{box-sizing:border-box}
body{margin:0;background:#080d16;color:#dbe6f2;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI","PingFang SC","Microsoft YaHei",sans-serif;line-height:1.6}
.wrap{max-width:920px;margin:0 auto;padding:32px 18px 64px}
h1{font-size:26px;margin:0 0 6px;letter-spacing:.5px}
.sub{color:#7d90a8;font-size:13px;margin-bottom:22px}
.stats{display:grid;grid-template-columns:repeat(auto-fit,minmax(120px,1fr));gap:10px;margin-bottom:26px}
.stat{background:#0e1727;border:1px solid #1c2a3f;border-radius:12px;padding:12px 14px}
.stat b{display:block;font-size:22px;color:#5ef0c0;font-weight:600}
.stat span{font-size:12px;color:#7d90a8}
.card{background:#0e1727;border:1px solid #1c2a3f;border-radius:14px;padding:16px;margin-bottom:12px}
.card h3{margin:0 0 4px;font-size:15px;color:#eaf3ff}
.card p{margin:0 0 10px;font-size:12.5px;color:#7d90a8}
.row{display:flex;gap:8px;align-items:center}
input{flex:1;min-width:0;background:#060b13;border:1px solid #1c2a3f;border-radius:9px;padding:10px 12px;color:#9fb3cc;font-size:12.5px;font-family:ui-monospace,Menlo,Consolas,monospace}
button{background:#5ef0c0;color:#04140f;border:0;border-radius:9px;padding:10px 16px;font-weight:600;font-size:13px;cursor:pointer;white-space:nowrap}
button:active{transform:scale(.97)}
.tip{font-size:12px;color:#5d7089;margin-top:8px}
.tag{display:inline-block;background:#132033;border:1px solid #1c2a3f;border-radius:6px;padding:1px 7px;font-size:11px;color:#7d90a8;margin-right:6px}
.hl{border-color:#2a5c4d;background:#0c1a17}
.hl h3{color:#5ef0c0}
footer{margin-top:26px;font-size:11.5px;color:#4c5e75;border-top:1px solid #16233a;padding-top:14px}
code{background:#132033;padding:1px 5px;border-radius:4px;font-size:11.5px;color:#9fb3cc}
</style></head><body><div class="wrap">
<h1>家宽订阅</h1>
<div class="sub">GitHub Actions 自动更新 · 上次生成 __BUILT__（北京时间）</div>
<div class="stats">
<div class="stat"><b>__RESI__</b><span>家宽节点</span></div>
<div class="stat"><b>__IDC__</b><span>机房兜底</span></div>
<div class="stat"><b>__CC__</b><span>国家/地区</span></div>
<div class="stat"><b>__ALIVE__</b><span>本次握手通过</span></div>
</div>

<div class="card hl">
<h3>Clash / mihomo · 精简版 <span class="tag">先导这条</span></h3>
<p>美国家宽全留 + 各国最多 3 条，共 __LITE__ 条。手机测速快，连不上时先用它。</p>
<div class="row"><input readonly value="__URL_LITE__"><button onclick="cp(this)">复制</button></div>
</div>

<div class="card">
<h3>Clash / mihomo · 全量</h3>
<p>全部家宽 + 机房兜底，分组齐全。</p>
<div class="row"><input readonly value="__URL_CLASH__"><button onclick="cp(this)">复制</button></div>
</div>

<div class="card">
<h3>v2rayN / NekoBox</h3>
<p>base64 编码的 socks 链接（仅家宽）。</p>
<div class="row"><input readonly value="__URL_V2RAY__"><button onclick="cp(this)">复制</button></div>
</div>

<div class="card">
<h3>sing-box</h3>
<p>完整配置 JSON。</p>
<div class="row"><input readonly value="__URL_SB__"><button onclick="cp(this)">复制</button></div>
</div>

<div class="tip">在客户端里把订阅更新间隔设成 6 小时，地址不用换。</div>

<footer>
__FLAGS__
<div style="margin-top:12px">节点全部经真实 TCP + 协议握手验活；仍可能有部分在客户端侧失效，以客户端测速为准。公开池以明文 HTTP 代理为主，别在上面登网银、邮箱。</div>
</footer>
</div>
<script>
document.querySelectorAll('input[readonly]').forEach(function(i){
  try{i.value=new URL(i.getAttribute('value'),location.href).href}catch(e){}
});
function cp(b){const i=b.previousElementSibling;i.select();navigator.clipboard.writeText(i.value).then(()=>{const t=b.textContent;b.textContent='已复制';setTimeout(()=>b.textContent=t,1200)});if(navigator.clipboard&&navigator.clipboard.writeText)return;try{document.execCommand('copy')}catch(e){}}
</script></body></html>
"""


def write_index(data, out_dir):
    base = os.environ.get("SUB_BASE", "").rstrip("/")
    s = data["stats"]
    flags = " ".join(f'{c["flag"]}{c["name"]} {c["count"]}' for c in data["countries"][:18])
    html = (INDEX_HTML
            .replace("__BUILT__", data["built_at"])
            .replace("__RESI__", str(s["resi"]))
            .replace("__IDC__", str(s["idc"]))
            .replace("__CC__", str(s["countries"]))
            .replace("__ALIVE__", str(s["alive"]))
            .replace("__LITE__", str(s.get("lite", 0)))
            .replace("__URL_LITE__", (base + "/sub/clash-lite.yaml") if base else "sub/clash-lite.yaml")
            .replace("__URL_CLASH__", (base + "/sub/clash.yaml") if base else "sub/clash.yaml")
            .replace("__URL_V2RAY__", (base + "/sub/v2ray.txt") if base else "sub/v2ray.txt")
            .replace("__URL_SB__", (base + "/sub/singbox.json") if base else "sub/singbox.json")
            .replace("__FLAGS__", flags))
    open(f"{out_dir}/index.html", "w", encoding="utf-8").write(html)


# ---------------- 主流程 ----------------

def _summary(data):
    p = os.environ.get("GITHUB_STEP_SUMMARY")
    if not p:
        return
    s = data["stats"]
    lines = ["## 本次更新结果", "",
             f"- 生成时间（北京）：{data['built_at']}",
             f"- 去重总量：{s['raw']}　握手存活：{s['alive']}",
             f"- 家宽：{s['resi']}　机房兜底：{s['idc']}　国家/地区：{s['countries']}",
             f"- 精简订阅：{s.get('lite', 0)} 条"]
    try:
        with open(p, "a", encoding="utf-8") as f:
            f.write("\n".join(lines) + "\n")
    except Exception:
        pass


def main():
    random.seed(SEED)
    t0 = time.time()
    print(f"[1/5] 抓源（候选上限 {CANDIDATES}，测活={'关' if SKIP_PROBE else '开'}）", flush=True)
    pool, src_stats = fetch_all()
    total_raw = len(pool)
    print(f"[1/5] 去重后 {total_raw} 条", flush=True)

    keys = list(pool.keys())
    weights = [1 + pool[k][1] for k in keys]
    picked = list(dict.fromkeys(random.choices(keys, weights=weights,
                                               k=min(len(keys), CANDIDATES * 3))))[:CANDIDATES]
    print(f"[2/5] 候选 {len(picked)} 条", flush=True)

    items = [(ip, port, pool[(ip, port)][0]) for ip, port in picked]
    if SKIP_PROBE:
        alive_items = items
    else:
        alive_items = probe_all(items)
    alive_ips = {(i[0], i[1]) for i in alive_items}
    print(f"[2/5] 存活 {len(alive_items)}", flush=True)

    print("[3/5] 查归属…", flush=True)
    geo = geo_batch([i[0] for i in alive_items])
    nodes = []
    for ip, port, proto in alive_items:
        g = geo.get(ip)
        if not g:
            continue
        cls = classify(g.get("isp", ""), g.get("org", ""), g.get("as", ""),
                       g.get("hosting", False), g.get("mobile", False))
        nodes.append({"ip": ip, "port": port, "proto": proto, "hits": pool[(ip, port)][1],
                      "cc": g.get("countryCode") or "??", "country": g.get("country") or "",
                      "isp": (g.get("isp") or "").strip(),
                      "asn": (g.get("as") or "").split()[0] if g.get("as") else "", "cls": cls})
    print(f"[3/5] 标注 {len(nodes)} 条", flush=True)

    print("[4/5] 生成订阅…", flush=True)
    data = build(nodes, {"total_raw": total_raw, "alive": len(alive_items),
                         "probed": len(items)}, src_stats)

    _summary(data)
    json.dump({"total_raw": total_raw, "alive": len(alive_items),
               "resi": data["stats"]["resi"], "idc": data["stats"]["idc"],
               "countries": data["stats"]["countries"]},
              open(f"{OUT_DIR}/data.json.summary", "w", encoding="utf-8"), ensure_ascii=False)
    print(f"[5/5] 完成，用时 {time.time()-t0:.0f}s", flush=True)


if __name__ == "__main__":
    main()
