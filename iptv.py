import eventlet
eventlet.monkey_patch()
import time
import datetime
import ipaddress
from threading import Thread
import os
import re
from queue import Queue
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed


# ====== 频道名归一化（基于 config.txt，完整匹配） ======
class ChannelNormalizer:
    """按 config.txt 的「标准名,别名1,别名2,...」规则归一化频道名。

    - 每行第一个字段为标准名
    - 其余字段为别名，只有**完整匹配**时才替换为标准名（大小写不敏感）
    - 先尝试原始名匹配；失败则清理画质词后再尝试完整匹配
    - 仍未匹配到任何规则的，保留清理画质词后的名字
    """

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
        """完整匹配查表，命中返回标准名，否则返回 None。"""
        if not name:
            return None
        return self.alias_to_std.get(name.strip().lower())

    def normalize(self, name):
        if not name:
            return name

        # 1) 原始名（去星号空白）完整匹配
        name1 = name.replace('*', '').replace('＊', '').strip()
        hit = self._lookup(name1)
        if hit:
            return hit

        # 2) 清理画质词后再完整匹配
        name2 = name1
        for token in self.QUALITY_TOKENS:
            name2 = name2.replace(token, '')
        name2 = name2.strip()
        hit = self._lookup(name2)
        if hit:
            return hit

        # 3) 都未命中，保留清理画质词后的名字
        return name2


# ====== 读取配置文件 ======
def read_config(config_file):
    """返回 [(host, port), ...]，host 可以是 IPv4 或域名；任何异常都返回 list。"""
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


# ====== 展开扫描目标（IPv4 扫整段，域名只扫自己） ======
def expand_targets(host, port):
    try:
        ipaddress.IPv4Address(host)
        a, b, c, _ = host.split('.')
        return [f"{a}.{b}.{c}.{x}:{port}" for x in range(1, 255)]
    except ValueError:
        return [f"{host}:{port}"]


# ====== 探测单个 host:port 是否可访问 ======
def check_ip_port(ip_port, url_end):
    try:
        url = f"http://{ip_port}{url_end}"
        resp = requests.get(url, timeout=2)
        resp.raise_for_status()
        if "tsfile" in resp.text or "hls" in resp.text:
            print(f"{url} 访问成功")
            return url
    except Exception:
        return None
    return None


# ====== 解析频道信息 ======
def extract_channels(url):
    hotel_channels = []
    try:
        json_url = f"{url}"
        urls = url.split('/', 3)
        url_x = f"{urls[0]}//{urls[2]}"
        if "iptv" in json_url:
            response = requests.get(json_url, timeout=2)
            json_data = response.json()
            for item in json_data['data']:
                if isinstance(item, dict):
                    name = item.get('name')
                    urlx = item.get('url')
                    if "tsfile" in urlx:
                        urld = f"{url_x}{urlx}"
                        hotel_channels.append((name, urld))
        elif "ZHGXTV" in json_url:
            response = requests.get(json_url, timeout=2)
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
    except Exception:
        return []


# ====== 测速 ======
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
                cont = None
                resp_time = 0.0
                completed = False

                channel_url_t = channel_url.rstrip(channel_url.split('/')[-1])
                lines = requests.get(channel_url, timeout=2).text.strip().split('\n')
                ts_lists = [line.split('/')[-1] for line in lines if not line.startswith('#')]
                if not ts_lists:
                    continue

                ts_url = channel_url_t + ts_lists[0]
                ts_lists_0 = ts_lists[0].rstrip(ts_lists[0].split('.ts')[-1])

                with eventlet.Timeout(5, False):
                    start_time = time.time()
                    cont = requests.get(ts_url, timeout=2).content
                    resp_time = time.time() - start_time
                    completed = True

                if completed and cont and resp_time > 0:
                    with open(ts_lists_0, 'ab') as f:
                        f.write(cont)
                    normalized_speed = max(len(cont) / resp_time / 1024 / 1024, 0.001)
                    if os.path.exists(ts_lists_0):
                        os.remove(ts_lists_0)
                    results.append((channel_name, channel_url, f"{normalized_speed:.3f}"))
            except Exception:
                pass
            finally:
                checked[0] += 1
                task_queue.task_done()

    task_queue = Queue()
    results = []
    checked = [0]
    Thread(target=show_progress, daemon=True).start()
    for _ in range(20):
        Thread(target=worker, daemon=True).start()
    for channel in channels:
        task_queue.put(channel)
    task_queue.join()
    return results


# ====== 统一频道名（调用 ChannelNormalizer） ======
def unify_channel_name(channels_list, normalizer):
    new_channels_list = []
    for name, channel_url, speed in channels_list:
        name = normalizer.normalize(name)
        new_channels_list.append(f"{name},{channel_url}\n")
    return new_channels_list


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


# ====== 酒店源主流程 ======
def hotel_iptv(config_file, normalizer):
    host_configs = list(dict.fromkeys(read_config(config_file)))   # 保序去重
    if not host_configs:
        print(f"{config_file} 无有效配置，跳过")
        return

    url_ends = ["/iptv/live/1000.json?key=txiptv", "/ZHGXTV/Public/json/live_interface.txt"]

    tasks_set = set()
    n_ip = n_domain = 0
    for host, port in host_configs:
        try:
            ipaddress.IPv4Address(host)
            n_ip += 1
        except ValueError:
            n_domain += 1
        for target in expand_targets(host, port):
            for url_end in url_ends:
                tasks_set.add((target, url_end))
    tasks = list(tasks_set)
    print(f"{config_file}: IPv4 {n_ip} 条, 域名 {n_domain} 条, 展开后共 {len(tasks)} 个探测任务")

    valid_urls = []
    with ThreadPoolExecutor(max_workers=200) as executor:
        futures = {
            executor.submit(check_ip_port, target, url_end): (target, url_end)
            for target, url_end in tasks
        }
        for future in as_completed(futures):
            result = future.result()
            if result:
                valid_urls.append(result)

    print(f"扫描完成，获取有效url共：{len(valid_urls)}个")
    channels = []
    for valid_url in valid_urls:
        channels.extend(extract_channels(valid_url))

    print(f"共获取频道：{len(channels)}个\n开始测速")
    results = speed_test(channels)

    # 先按速度降序，再按频道号排序（保持原逻辑）
    results.sort(key=lambda x: -float(x[2]))
    results.sort(key=lambda x: channel_key(x[0]))
    with open('1.txt', 'a', encoding='utf-8') as f:
        f.writelines(unify_channel_name(results, normalizer))
    print("测速完成")


def main():
    # 清理上次残留
    for f in ["1.txt", "iptv.txt"]:
        if os.path.exists(f):
            os.remove(f)

    # 初始化频道归一化器（依赖 config.txt）
    normalizer = ChannelNormalizer("config.txt")
    print(f"频道配置加载完成：{len(normalizer.alias_to_std)} 个别名")

    hotel_config_files = ["ip/酒店高清.ip", "ip/酒店标清.ip"]
    for config_file in hotel_config_files:
        if not os.path.exists(config_file):
            print(f"警告：配置文件不存在，跳过: {config_file}")
            continue
        hotel_iptv(config_file, normalizer)

    classify_channels('1.txt', '央视.txt', keywords="央视频道,CCTV,风云剧场,怀旧剧场,第一剧场,兵器,女性,地理,央视文化,风云音乐,央视,CGTN,CETV,CHC")
    classify_channels('1.txt', '卫视.txt', keywords="卫视频道,卫视")
    classify_channels('1.txt', '少儿.txt', keywords="少儿频道,少儿,卡通,动漫,炫动")
    classify_channels('1.txt', '湖南.txt', keywords="湖南频道,湖南,金鹰,潇湘,长沙,南县,快乐")
    classify_channels('1.txt', '广东.txt', keywords="广东频道,广东,客家,广州,珠江,嘉佳")
    classify_channels('1.txt', '河南.txt', keywords="河南频道,河南,信阳,漯河,郑州,驻马店,平顶山,安阳,武术世界,文物宝库,梨园,南阳")
    classify_channels('1.txt', '广西.txt', keywords="广西频道,广西,南宁,玉林,桂林,北流,平南,贵港")
    classify_channels('1.txt', '陕西.txt', keywords="陕西频道,陕西,西安")
    classify_channels('1.txt', '黑龙江.txt', keywords="黑龙江频道,黑龙江,齐齐哈尔")
    classify_channels('1.txt', '江西.txt', keywords="江西频道,江西,南昌")
    classify_channels('1.txt', '海南.txt', keywords="海南频道,海南,齐齐哈尔")
    classify_channels('1.txt', '山东.txt', keywords="山东频道,山东,济宁,兰陵,临沂")
    classify_channels('1.txt', '吉林.txt', keywords="吉林频道,吉林,松原,延边,四平")
    classify_channels('1.txt', '新疆.txt', keywords="新疆频道,新疆,乌鲁木齐")
    classify_channels('1.txt', '安徽.txt', keywords="安徽频道,安徽,合肥,安庆")
    classify_channels('1.txt', '港台.txt', keywords="香港频道,凤凰,香港,明珠台,翡翠台,星河")
    classify_channels('1.txt', '其他.txt', keywords="其他频道,tsfile")

    # 合并写入文件
    file_contents = []
    file_paths = ["央视.txt", "卫视.txt", "少儿.txt", "湖南.txt", "广东.txt",
                  "河南.txt", "广西.txt", "陕西.txt", "黑龙江.txt","江西.txt","海南.txt","山东.txt","吉林.txt","新疆.txt","安徽.txt","港台.txt", "其他.txt", "txt/浙江.txt"]
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

    # 原始顺序去重
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

    # 移除过程文件
    files_to_remove = ["1.txt", "央视.txt", "卫视.txt", "少儿.txt", "湖南.txt", "广东.txt",
                       "河南.txt", "广西.txt", "陕西.txt","黑龙江.txt","江西.txt","海南.txt","山东.txt","吉林.txt","新疆.txt","安徽.txt", "港台.txt", "其他.txt"]
    for file in files_to_remove:
        if os.path.exists(file):
            os.remove(file)
    print("任务运行完毕，所有频道合并到iptv.txt")


if __name__ == "__main__":
    main()