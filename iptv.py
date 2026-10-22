import eventlet
eventlet.monkey_patch()
import time
import datetime
import ipaddress
import threading
from threading import Thread
import os
import re
from queue import Queue
import requests
import requests.exceptions
from concurrent.futures import ThreadPoolExecutor, as_completed


# ====== 并发参数（可调） ======
SCAN_WORKERS = 400       # 扫描 IP 段的并发数
VERIFY_WORKERS = 100     # 验证 m3u8 的并发数
EXTRACT_WORKERS = 50     # 并发解析频道的上限
SCAN_TIMEOUT = 2         # 普通端点超时（秒）
LIST_TIMEOUT = 6         # /streamer/list（JSON 较大）
VERIFY_TIMEOUT = 4       # m3u8 验证超时


# ====== 线程局部 Session（TCP 连接复用） ======
_thread_local = threading.local()


def get_session():
    if not hasattr(_thread_local, 'session'):
        s = requests.Session()
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=VERIFY_WORKERS,
            pool_maxsize=VERIFY_WORKERS,
            max_retries=0,
        )
        s.mount('http://', adapter)
        s.mount('https://', adapter)
        _thread_local.session = s
    return _thread_local.session


# ====== 频道名归一化 ======
class ChannelNormalizer:
    QUALITY_TOKENS = ('超高清', '超清', '高清', '标清', 'HD')

    def __init__(self, config_file):
        self.alias_to_std = {}
        self.conflicts = []
        self._load_config(config_file)

    def _load_config(self, config_file):
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                for lineno, line in enumerate(f, 1):
                    line = line.strip()
                    if not line or line.startswith('#'):
                        continue
                    parts = [p.strip() for p in line.split(',')]
                    parts = [p for p in parts if p]
                    if not parts:
                        continue
                    std = parts[0]
                    for alias in [std] + parts[1:]:
                        key = alias.lower()
                        if key in self.alias_to_std and self.alias_to_std[key] != std:
                            self.conflicts.append(
                                f"第 {lineno} 行 '{alias}' 已映射到 "
                                f"'{self.alias_to_std[key]}'，忽略到 '{std}' 的映射"
                            )
                            continue
                        self.alias_to_std[key] = std
        except FileNotFoundError:
            print(f"警告：配置文件不存在: {config_file}")
        for c in self.conflicts:
            print(f"[配置冲突] {c}")

    def _lookup(self, name):
        if not name:
            return None
        return self.alias_to_std.get(name.strip().lower())

    def normalize(self, name):
        if not name:
            return name
        name1 = name.replace('*', '').replace('＊', '').strip()
        hit = self._lookup(name1)
        if hit:
            return hit
        name2 = name1
        for token in self.QUALITY_TOKENS:
            name2 = name2.replace(token, '')
        name2 = name2.strip()
        hit = self._lookup(name2)
        if hit:
            return hit
        return name2


# ====== 读取配置文件 ======
def read_config(config_file):
    host_configs = []
    try:
        with open(config_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                if ':' not in line:
                    print(f"跳过无效行（缺少端口）: {line}")
                    continue
                host, port = line.rsplit(':', 1)
                host, port = host.strip(), port.strip()
                if not host or not port.isdigit():
                    print(f"跳过无效行（格式错误）: {line}")
                    continue
                host_configs.append((host, port))
    except Exception as e:
        print(f"读取文件错误: {e}")
    return host_configs


# ====== 展开扫描目标 ======
def expand_targets(host, port):
    try:
        ipaddress.IPv4Address(host)
        a, b, c, _ = host.split('.')
        return [f"{a}.{b}.{c}.{x}:{port}" for x in range(1, 255)]
    except ValueError:
        return [f"{host}:{port}"]


# ====== 探测单个 host:port ======
def check_ip_port(ip_port, url_end):
    url = f"http://{ip_port}{url_end}"
    s = get_session()
    try:
        timeout = LIST_TIMEOUT if url_end.endswith("/streamer/list") else SCAN_TIMEOUT
        resp = s.get(url, timeout=timeout)
        resp.raise_for_status()
        text = resp.text

        if url_end.endswith("/streamer/list"):
            try:
                data = resp.json()
                if isinstance(data, list) and data:
                    print(f"✅ {url} （streamer/list，{len(data)} 条）")
                    return url
            except ValueError:
                pass
            return None

        if "tsfile" in text or "hls" in text:
            print(f"✅ {url}")
            return url

    except requests.exceptions.ConnectTimeout:
        if url_end.endswith("/streamer/list"):
            print(f"⏱️  {url} 连接超时")
        return None
    except requests.exceptions.ConnectionError as e:
        if url_end.endswith("/streamer/list"):
            print(f"🔌 {url} 连接失败: {e}")
        return None
    except requests.exceptions.HTTPError as e:
        code = e.response.status_code if e.response is not None else '?'
        print(f"⚠️  {url} HTTP {code}（端口活着，路径可能不对）")
        return None
    except Exception as e:
        print(f"❌ {url} {type(e).__name__}: {e}")
        return None
    return None


# ====== 解析频道信息 ======
def extract_channels(url):
    hotel_channels = []
    s = get_session()
    try:
        json_url = f"{url}"
        urls = url.split('/', 3)
        url_x = f"{urls[0]}//{urls[2]}"

        if "streamer/list" in json_url:
            response = s.get(json_url, timeout=LIST_TIMEOUT)
            json_data = response.json()
            if isinstance(json_data, list):
                for item in json_data:
                    if not isinstance(item, dict):
                        continue
                    if not item.get('status', True):
                        continue
                    name = (item.get('name') or '').strip()
                    key = str(item.get('key') or '').strip()
                    if name and key:
                        channel_url = f"{url_x}/hls/{key}/index.m3u8"
                        hotel_channels.append((name, channel_url))
            print(f"[streamer/list] {json_url} 解析出 {len(hotel_channels)} 个频道")
            return hotel_channels

        if "iptv" in json_url:
            response = s.get(json_url, timeout=LIST_TIMEOUT)
            json_data = response.json()
            for item in json_data['data']:
                if isinstance(item, dict):
                    name = item.get('name')
                    urlx = item.get('url')
                    if "tsfile" in urlx:
                        urld = f"{url_x}{urlx}"
                        hotel_channels.append((name, urld))

        elif "ZHGXTV" in json_url:
            response = s.get(json_url, timeout=LIST_TIMEOUT)
            json_data = response.content.decode('utf-8')
            data_lines = json_data.split('\n')
            for line in data_lines:
                if "," in line and "hls" in line:
                    name, channel_url = line.strip().split(',')
                    parts = channel_url.split('/', 3)
                    if len(parts) >= 4:
                        urld = f"{url_x}/{parts[3]}"
                        hotel_channels.append((name, urld))
        return hotel_channels
    except Exception as e:
        print(f"[extract_channels] {url} 异常: {type(e).__name__}: {e}")
        return []


# ====== 验证 m3u8 可用性（200 + #EXTM3U 即收录） ======
def speed_test(channels):
    def show_progress():
        while checked[0] < len(channels):
            numberx = checked[0] / len(channels) * 100 if channels else 0
            print(f"已测试{checked[0]}/{len(channels)}，可用频道:{len(results)}个，进度:{numberx:.2f}%")
            time.sleep(5)

    def worker():
        while True:
            channel_name, channel_url = task_queue.get()
            try:
                r = get_session().get(channel_url, timeout=VERIFY_TIMEOUT, stream=True)
                if r.status_code != 200:
                    r.close()
                    print(f"[无效] {channel_name} HTTP {r.status_code}: {channel_url}")
                    continue
                chunk = next(r.iter_content(512), b'').decode('utf-8', 'ignore')
                r.close()
                if '#EXTM3U' in chunk:
                    results.append((channel_name, channel_url, "1.0"))
                else:
                    print(f"[无效] {channel_name} 非 m3u8 响应: {channel_url}")
            except Exception as e:
                print(f"[失败] {channel_name}: {type(e).__name__}: {e}")
            finally:
                checked[0] += 1
                task_queue.task_done()

    task_queue = Queue()
    results = []
    checked = [0]
    Thread(target=show_progress, daemon=True).start()
    for _ in range(VERIFY_WORKERS):
        Thread(target=worker, daemon=True).start()
    for channel in channels:
        task_queue.put(channel)
    task_queue.join()
    return results


# ====== 统一频道名 ======
def unify_channel_name(channels_list, normalizer):
    out = []
    for name, channel_url, speed in channels_list:
        name = normalizer.normalize(name)
        out.append(f"{name},{channel_url}\n")
    return out


# ====== 排序 key ======
def channel_key(channel_name):
    match = re.search(r'\d+', channel_name)
    return int(match.group()) if match else float('inf')


# ====== 分组 ======
def classify_channels(input_file, output_file, keywords):
    keywords_list = keywords.split(',')
    pattern = '|'.join(re.escape(keyword) for keyword in keywords_list)
    extracted_lines = []
    with open(input_file, 'r', encoding='utf-8') as f:
        for line in f:
            if "genre" not in line:
                if re.search(pattern, line):
                    extracted_lines.append(line)
    with open(output_file, 'w', encoding='utf-8') as out_file:
        out_file.write(f"{keywords_list[0]},#genre#\n")
        out_file.writelines(extracted_lines)


# ====== 兜底分类 ======
def classify_unmatched(input_file, output_file, used_files):
    used_lines = set()
    for fp in used_files:
        if os.path.exists(fp):
            with open(fp, 'r', encoding='utf-8') as f:
                for line in f:
                    used_lines.add(line)

    with open(input_file, 'r', encoding='utf-8') as f:
        remaining = [line for line in f
                     if line.strip()
                     and "genre" not in line
                     and line not in used_lines]

    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("其他频道,#genre#\n")
        f.writelines(remaining)
    print(f"[兜底] {output_file} 捕获 {len(remaining)} 条未分组频道")


# ====== 酒店源主流程 ======
def hotel_iptv(config_file, normalizer):
    host_configs = list(dict.fromkeys(read_config(config_file)))
    if not host_configs:
        print(f"{config_file} 无有效配置，跳过")
        return

    url_ends = [
        "/iptv/live/1000.json?key=txiptv",
        "/ZHGXTV/Public/json/live_interface.txt",
        "/streamer/list",
    ]

    tasks_set = set()
    n_ip = n_domain = 0
    for host, port in host_configs:
        try:
            ipaddress.IPv4Address(host)
            n_ip += 1
        except ValueError:
            n_domain += 1

        for url_end in url_ends:
            if url_end == "/streamer/list":
                targets = [f"{host}:{port}"]
            else:
                targets = expand_targets(host, port)
            for target in targets:
                tasks_set.add((target, url_end))

    tasks = list(tasks_set)
    print(f"\n===== {config_file}: IPv4 {n_ip} 条, 域名 {n_domain} 条, "
          f"展开后共 {len(tasks)} 个探测任务（{SCAN_WORKERS} 并发） =====")

    t0 = time.time()
    valid_urls = []
    with ThreadPoolExecutor(max_workers=SCAN_WORKERS) as executor:
        futures = {
            executor.submit(check_ip_port, target, url_end): (target, url_end)
            for target, url_end in tasks
        }
        for future in as_completed(futures):
            result = future.result()
            if result:
                valid_urls.append(result)
    scan_time = time.time() - t0

    print(f"扫描完成：{len(tasks)} 个任务 → 有效 {len(valid_urls)} 个，耗时 {scan_time:.1f}s")

    channels = []
    if valid_urls:
        t1 = time.time()
        with ThreadPoolExecutor(max_workers=min(EXTRACT_WORKERS, len(valid_urls))) as executor:
            futures = [executor.submit(extract_channels, u) for u in valid_urls]
            for f in as_completed(futures):
                try:
                    channels.extend(f.result())
                except Exception as e:
                    print(f"[extract_channels] 并发异常: {type(e).__name__}: {e}")
        print(f"解析完成：{len(channels)} 个频道，耗时 {time.time() - t1:.1f}s")

    print(f"\n开始验证 m3u8 可用性（{VERIFY_WORKERS} 并发）")
    t2 = time.time()
    results = speed_test(channels)
    verify_time = time.time() - t2

    # ↓↓↓ 唯一修改：先按频道名（channel_key 保持 CCTV-1/2/10 顺序），
    #     同名频道再按速度降序（快的在前）
    results.sort(key=lambda x: (channel_key(x[0]), -float(x[2])))

    with open('1.txt', 'a', encoding='utf-8') as f:
        f.writelines(unify_channel_name(results, normalizer))
    print(f"验证完成：可用频道 {len(results)} 个，耗时 {verify_time:.1f}s")


def main():
    for f in ["1.txt", "iptv.txt"]:
        if os.path.exists(f):
            os.remove(f)

    normalizer = ChannelNormalizer("config.txt")
    print(f"频道配置加载完成：{len(normalizer.alias_to_std)} 个别名")

    hotel_config_files = ["ip/酒店高清.ip", "ip/酒店标清.ip"]
    for config_file in hotel_config_files:
        if not os.path.exists(config_file):
            print(f"警告：配置文件不存在，跳过: {config_file}")
            continue
        hotel_iptv(config_file, normalizer)

    classify_channels('1.txt', '央视.txt', keywords="央视频道,CCTV,风云剧场,怀旧剧场,第一剧场,兵器,女性,地理,央视文化,风云音乐,央视,CGTN,中央,CETV,CHC")
    classify_channels('1.txt', '卫视.txt', keywords="卫视频道,卫视")
    classify_channels('1.txt', '北京.txt', keywords="北京频道,北京,BRTV,优优宝贝,四海钓鱼,环球旅游,中华特产,通州,门头沟,朝阳融媒,顺义")
    classify_channels('1.txt', '湖南.txt', keywords="湖南频道,湖南,金鹰,潇湘,长沙,南县,快乐")
    classify_channels('1.txt', '广东.txt', keywords="广东频道,广东,客家,广州,珠江,嘉佳,南方购物,岭南戏曲,五华,梅县,梅州")
    classify_channels('1.txt', '河南.txt', keywords="河南频道,河南,信阳,漯河,郑州,驻马店,平顶山,安阳,武术世界,文物宝库,梨园,南阳")
    classify_channels('1.txt', '广西.txt', keywords="广西频道,广西,南宁,玉林,桂林,北流,平南,贵港,北海")
    classify_channels('1.txt', '陕西.txt', keywords="陕西频道,陕西,西安")
    classify_channels('1.txt', '黑龙江.txt', keywords="黑龙江频道,黑龙江,齐齐哈尔")
    classify_channels('1.txt', '江西.txt', keywords="江西频道,江西,南昌,赣州,章贡,石城")
    classify_channels('1.txt', '海南.txt', keywords="海南频道,海南,齐齐哈尔")
    classify_channels('1.txt', '山东.txt', keywords="山东频道,山东,济宁,兰陵,临沂,济南,烟台")
    classify_channels('1.txt', '山西.txt', keywords="山西频道,山西,朔州,太原,阳泉,大同,晋城,长治,吕梁,晋中,忻州,临汾,运城")
    classify_channels('1.txt', '吉林.txt', keywords="吉林频道,吉林,松原,延边,四平,通化,辉南")
    classify_channels('1.txt', '新疆.txt', keywords="新疆频道,新疆,乌鲁木齐")
    classify_channels('1.txt', '安徽.txt', keywords="安徽频道,安徽,合肥,安庆,宣城,广德,亳州")
    classify_channels('1.txt', '内蒙古.txt', keywords="内蒙古频道,内蒙古,赤峰,包头,兴安盟")
    classify_channels('1.txt', '上海.txt', keywords="上海频道,上海,东方财经,第一财经,动漫秀场,乐游,新纪实,金色学堂,都市剧场,游戏风云,生活时尚,法治天地,东方影视,五星体育,哈哈炫动,多彩文体")
    classify_channels('1.txt', '湖北.txt', keywords="湖北频道,湖北,荆门,荆州,天门,神农架,精彩影视")
    classify_channels('1.txt', '福建.txt', keywords="福建频道,福建,厦门,泉州,漳州,新视觉")
    classify_channels('1.txt', '贵州.txt', keywords="贵州频道,贵州,贵阳,黔")
    classify_channels('1.txt', '河北.txt', keywords="河北频道,河北,秦皇岛,保定,石家庄,唐山,涿州,承德,廊坊")
    classify_channels('1.txt', '港台.txt', keywords="香港频道,凤凰,香港,明珠台,翡翠台,星河")

    classify_unmatched('1.txt', '其他.txt', used_files=[
        "央视.txt", "卫视.txt", "北京.txt", "湖南.txt", "广东.txt",
        "河南.txt", "广西.txt", "陕西.txt", "黑龙江.txt", "江西.txt",
        "海南.txt", "山东.txt", "山西.txt", "吉林.txt", "新疆.txt", "安徽.txt", "内蒙古.txt", "上海.txt", "湖北.txt", "福建.txt", "贵州.txt", "河北.txt", "港台.txt",
    ])

    file_contents = []
    file_paths = ["央视.txt", "卫视.txt", "北京.txt", "湖南.txt", "广东.txt",
                  "河南.txt", "广西.txt", "陕西.txt", "黑龙江.txt", "江西.txt", "海南.txt",
                  "山东.txt", "山西.txt", "吉林.txt", "新疆.txt", "安徽.txt", "内蒙古.txt", "上海.txt", "湖北.txt", "福建.txt","贵州.txt", "河北.txt","港台.txt",
                  "其他.txt", "txt/浙江.txt"]
    for file_path in file_paths:
        if os.path.exists(file_path):
            with open(file_path, 'r', encoding="utf-8") as f:
                file_contents.append(f.read())
        else:
            print(f"警告：文件不存在，跳过合并: {file_path}")

    now = datetime.datetime.now(datetime.UTC) + datetime.timedelta(hours=8)
    current_time = now.strftime("%Y/%m/%d %H:%M")
    with open("1.txt", "w", encoding="utf-8") as f:
        f.write(f"{current_time}更新,#genre#\n")
        f.write(f"浙江卫视,http://ali-m-l.cztv.com/channels/lantian/channel001/1080p.m3u8\n")
        f.write('\n'.join(file_contents))

    with open('1.txt', 'r', encoding="utf-8") as f:
        lines = f.readlines()
    unique_lines = []
    seen_lines = set()
    for line in lines:
        if line not in seen_lines:
            unique_lines.append(line)
            seen_lines.add(line)
    with open('iptv.txt', 'w', encoding="utf-8") as f:
        f.writelines(unique_lines)

    files_to_remove = ["1.txt", "央视.txt", "卫视.txt", "北京.txt", "湖南.txt", "广东.txt",
                       "河南.txt", "广西.txt", "陕西.txt", "黑龙江.txt", "江西.txt", "海南.txt",
                       "山东.txt", "山西.txt", "吉林.txt", "新疆.txt", "安徽.txt", "港台.txt","内蒙古.txt", "上海.txt","湖北.txt", "福建.txt","贵州.txt", "河北.txt","其他.txt"]
    for file in files_to_remove:
        if os.path.exists(file):
            os.remove(file)
    print("任务运行完毕，所有频道合并到iptv.txt")


if __name__ == "__main__":
    main()