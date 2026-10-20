#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
辽宁省新能源政策跟踪 · 每日云端检索与研判（GitHub Actions 运行）
流程：85 渠道列表页抓取 → 关键词粗筛 → 与库去重 → 候选正文抓取
      → GLM（智谱 glm-5.3-flash）研判 → 字段校验 → 写入 policy-data.js
      → 刷新版头两字段（每次运行必刷）→ 追加 runlog.md 留痕
安全：密钥只读环境变量 ZHIPU_API_KEY，任何日志不打印密钥。
"""
import os
import re
import sys
import json
import html
import time
import base64
import argparse
import logging
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

BJT = timezone(timedelta(hours=8))
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(BASE_DIR)
DATA_FILE = os.path.join(ROOT, 'policy-data.js')
RUNLOG_FILE = os.path.join(ROOT, 'runlog.md')
RESOURCE_FILE = os.path.join(ROOT, 'resource-data.js')
API_URL = 'https://open.bigmodel.cn/api/paas/v4/chat/completions'
MODEL = 'glm-5.3-flash'

LEVELS = {'national', 'liaoning', 'city'}
FIELDS = {'competitive', 'retrofit', 'distribution', 'pricing', 'storage',
          'pv', 'plan', 'procedure', 'other'}
STAGES = {'approval', 'land', 'sea', 'forest', 'eia', 'navigation',
          'mineral', 'water', 'grid', 'other'}
CITIES = {'沈阳', '大连', '鞍山', '抚顺', '本溪', '丹东', '锦州', '营口',
          '阜新', '辽阳', '盘锦', '铁岭', '朝阳', '葫芦岛'}
COUNTIES = {'岫岩', '绥中'}

KW_ENERGY = [
    '风电', '光伏', '新能源', '储能', '电力', '电网', '电价', '上网电价',
    '机制电价', '绿证', '虚拟电厂', '消纳', '竞争性配置', '竞配', '竞价',
    '核准', '备案', '改造升级', '老旧机组', '分散式', '海上风电', '风光',
    '沐光', '驭风', '增量配电', '容量交易', '绿电', '绿证核发', '能源',
]
KW_PROC = KW_ENERGY + [
    '批复', '审批决定', '批后', '同意书', '许可', '出让', '成交',
    '海域使用', '用地预审', '选址意见', '征地', '农转用', '林地', '林草',
    '水土保持', '水保', '环评', '环境影响', '压覆矿产', '航行通告',
    '航标动态', '水上水下', '接入系统', '送出工程',
]

# 重点项目业主关键词：候选标题命中者在候选池排序中优先（手续链积累，
# 名单取自已入库竞配/建设方案中的主要业主，见交接文档第六节）
KEY_PROJECTS = [
    '中广核', '三峡', '华电', '大唐', '国家电投', '国电投', '国能', '华能',
    '华润电力', '华润新能源', '国家能源集团', '京能', '深能', '辽能',
    '惠天热电', '东方风力', '虹京实业', '科安隆', '迎新节能', '沣泽',
]

HEADERS = {
    'User-Agent': ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                   'AppleWebKit/537.36 (KHTML, like Gecko) '
                   'Chrome/124.0.0.0 Safari/537.36'),
    'Accept': 'text/html,application/xhtml+xml,*/*;q=0.8',
    'Accept-Language': 'zh-CN,zh;q=0.9',
}

# 渠道表：(id, 名称, city, county, [列表页URL（首页兜底）], 关键词模式)
CHANNELS = [
    # —— 国家部委（6）——
    ('ndrc', '国家发展改革委', None, None,
     ['https://www.ndrc.gov.cn/xwdt/tzgg/', 'https://www.ndrc.gov.cn/xxgk/zcfb/'], 'energy'),
    ('nea', '国家能源局', None, None,
     ['https://www.nea.gov.cn/'], 'energy'),
    ('dbj', '东北能源监管局', None, None,
     ['https://dbj.nea.gov.cn/xxgk/zcfg/', 'https://dbj.nea.gov.cn/'], 'energy'),
    ('mnr', '自然资源部', None, None,
     ['https://www.mnr.gov.cn/gk/tzgg/', 'https://www.mnr.gov.cn/'], 'proc'),
    ('forestry', '国家林业和草原局', None, None,
     ['https://www.forestry.gov.cn/'], 'proc'),
    ('lnmsa', '辽宁海事局', None, None,
     ['https://www.ln.msa.gov.cn/'], 'proc'),
    # —— 省级部门（8）——
    ('ln-gov', '辽宁省人民政府', None, None,
     ['https://www.ln.gov.cn/zwgk/zfwj/', 'https://www.ln.gov.cn/'], 'energy'),
    ('ln-fgw', '辽宁省发展改革委', None, None,
     ['https://fgw.ln.gov.cn/fgw/index/wsgs/index.shtml',
      'https://fgw.ln.gov.cn/fgw/index/tzgg/index.shtml',
      'https://fgw.ln.gov.cn/fgw/zc/zcwjk/index.shtml',
      'https://fgw.ln.gov.cn/'], 'energy'),
    ('ln-zrzy', '辽宁省自然资源厅', None, None,
     ['https://zrzy.ln.gov.cn/'], 'proc'),
    ('ln-sthj', '辽宁省生态环境厅', None, None,
     ['https://sthj.ln.gov.cn/'], 'proc'),
    ('ln-lyt', '辽宁省林业和草原局', None, None,
     ['https://lyt.ln.gov.cn/'], 'proc'),
    ('ln-slt', '辽宁省水利厅', None, None,
     ['https://slt.ln.gov.cn/slt/zfxxgk/fdzdgknr/xzxkjd/', 'https://slt.ln.gov.cn/'], 'proc'),
    ('ln-sgcc', '国网辽宁电力', None, None,
     ['http://www.ln.sgcc.cn/'], 'energy'),
    ('ln-zdpt', '辽宁省征地信息平台', None, None,
     ['https://zrzy.ln.gov.cn/zdxx/land-publish/home/internet-page'], 'proc'),
    # —— 沈阳市（4）——
    ('sy-fgw', '市发展改革委', '沈阳', None,
     ['https://fgw.shenyang.gov.cn/tzgg/', 'https://fgw.shenyang.gov.cn/zwgk/fdzdgknr/bmwj/'], 'energy'),
    ('sy-zrzy', '市自然资源局', '沈阳', None,
     ['https://zrzyj.shenyang.gov.cn/zxbs/gsgg/', 'https://zrzyj.shenyang.gov.cn/'], 'proc'),
    ('sy-sthj', '市生态环境局', '沈阳', None,
     ['https://sthjj.shenyang.gov.cn/zwgk/fdzdgknr/hjyxpjgs/jsxmhjyxpjwjspjdgg/'], 'proc'),
    ('sy-sswj', '市水务局', '沈阳', None,
     ['https://sswj.shenyang.gov.cn/'], 'proc'),
    # —— 大连市（5）——
    ('dl-fgw', '市发展改革委', '大连', None,
     ['http://www.pc.dl.gov.cn/'], 'energy'),
    ('dl-zrzy', '市自然资源局', '大连', None,
     ['https://zrzy.dl.gov.cn/col/col3082/index.html', 'https://zrzy.dl.gov.cn/'], 'proc'),
    ('dl-sthj', '市生态环境局', '大连', None,
     ['https://epb.dl.gov.cn/'], 'proc'),
    ('dl-swj', '市水务局', '大连', None,
     ['https://swj.dl.gov.cn/'], 'proc'),
    ('dl-hyfz', '海洋发展局', '大连', None,
     ['https://hyfz.dl.gov.cn/'], 'proc'),
    # —— 鞍山市（4）——
    ('as-fgw', '市发展改革委', '鞍山', None,
     ['http://fgw.anshan.gov.cn/'], 'energy'),
    ('as-zrzy', '市自然资源局', '鞍山', None,
     ['http://zrzyj.anshan.gov.cn/asszrzyj/zwgk/ncjttdzs/glist.html',
      'http://zrzyj.anshan.gov.cn/'], 'proc'),
    ('as-sthj', '市生态环境局', '鞍山', None,
     ['http://sthjj.anshan.gov.cn/asshjbhj/tzgg/glist.html',
      'http://sthjj.anshan.gov.cn/'], 'proc'),
    ('as-slj', '市水利局', '鞍山', None,
     ['http://slj.anshan.gov.cn/slj/zwgkzdgz/zcyjd/glist.html',
      'http://slj.anshan.gov.cn/'], 'proc'),
    # —— 岫岩满族自治县（1）——
    ('xy-gov', '县人民政府', '鞍山', '岫岩',
     ['http://www.xiuyan.gov.cn/'], 'energy'),
    # —— 抚顺市（5）——
    ('fs-fgw', '市发展改革委', '抚顺', None,
     ['https://fsfgw.fushun.gov.cn/xwdt/001005/moreinfo.html',
      'https://fsfgw.fushun.gov.cn/'], 'energy'),
    ('fs-zrzy', '市自然资源局', '抚顺', None,
     ['https://zrzy.fushun.gov.cn/bsfw/004002/moreinfo.html',
      'https://zrzy.fushun.gov.cn/'], 'proc'),
    ('fs-lczy', '市林业和草原局', '抚顺', None,
     ['http://www.fushun.gov.cn/zwgk/002005/002005002/002005002038/moreinfo.html'], 'proc'),
    ('fs-sthj', '市生态环境局', '抚顺', None,
     ['https://sthj.fushun.gov.cn/hbsp%EF%BC%88zdjsxmpzhss%EF%BC%89/014003/moreinfo.html',
      'https://sthj.fushun.gov.cn/'], 'proc'),
    ('fs-sswj', '市水务局', '抚顺', None,
     ['https://fsswj.fushun.gov.cn/'], 'proc'),
    # —— 本溪市（5）——
    ('bx-fgw', '市发展改革委', '本溪', None,
     ['https://fgw.benxi.gov.cn/publicity/ywgz/tzgg', 'https://fgw.benxi.gov.cn/'], 'energy'),
    ('bx-zrzy', '市自然资源局', '本溪', None,
     ['https://zrzyj.benxi.gov.cn/publicity/ywgz/tdzsxx',
      'https://zrzyj.benxi.gov.cn/publicity/ghjh2/xggh',
      'https://zrzyj.benxi.gov.cn/'], 'proc'),
    ('bx-lcj', '市林业和草原局', '本溪', None,
     ['https://lcj.benxi.gov.cn/publicity_1', 'https://lcj.benxi.gov.cn/'], 'proc'),
    ('bx-sthj', '市生态环境局', '本溪', None,
     ['https://sthjj.benxi.gov.cn/zwgk/hzspxxgk', 'https://sthjj.benxi.gov.cn/'], 'proc'),
    ('bx-swj', '市水务局', '本溪', None,
     ['https://swj.benxi.gov.cn/publicity_1', 'https://swj.benxi.gov.cn/'], 'proc'),
    # —— 丹东市（6）——
    ('dd-fgw', '市发展改革委', '丹东', None,
     ['https://fgw.dandong.gov.cn/ddsfzhggwyh/tzgg/glist.html',
      'https://fgw.dandong.gov.cn/'], 'energy'),
    ('dd-zrzy', '市自然资源局', '丹东', None,
     ['https://zrzy.dandong.gov.cn/ddszrzyj/tzgg/glist.html',
      'https://zrzy.dandong.gov.cn/'], 'proc'),
    ('dd-lyj', '市林业和草原局', '丹东', None,
     ['http://lyj.dandong.gov.cn/'], 'proc'),
    ('dd-sthj', '市生态环境局', '丹东', None,
     ['https://ddepb.dandong.gov.cn/ddsthjj/tzgg/glist.html',
      'https://ddepb.dandong.gov.cn/ddsthjj/'], 'proc'),
    ('dd-swj', '市水务局', '丹东', None,
     ['https://www.dandong.gov.cn/'], 'proc'),
    ('dd-hyy', '海洋与渔业局', '丹东', None,
     ['https://hyyy.dandong.gov.cn/ddyyj/tzgg/glist.html',
      'https://hyyy.dandong.gov.cn/'], 'proc'),
    # —— 锦州市（6）——
    ('jz-fgw', '市发展改革委', '锦州', None,
     ['https://fgw.jz.gov.cn/tzgg.htm',
      'https://fgw.jz.gov.cn/xxgk/fdzdgknr/gsgg.htm',
      'https://fgw.jz.gov.cn/'], 'energy'),
    ('jz-zrzy', '市自然资源局', '锦州', None,
     ['http://zrzyj.jz.gov.cn/gsgg1.htm', 'http://zrzyj.jz.gov.cn/zxgz/pzjg.htm',
      'http://zrzyj.jz.gov.cn/'], 'proc'),
    ('jz-lcj', '市林业和草原局', '锦州', None,
     ['http://lcj.jz.gov.cn/'], 'proc'),
    ('jz-stj', '市生态环境局', '锦州', None,
     ['https://stj.jz.gov.cn/xxgk/fdzdgknr/xxgs.htm', 'https://stj.jz.gov.cn/'], 'proc'),
    ('jz-slj', '市水利局', '锦州', None,
     ['https://slj.jz.gov.cn/'], 'proc'),
    ('jz-hyy', '海洋与渔业局', '锦州', None,
     ['https://www.jz.gov.cn/info/1017/', 'https://www.jz.gov.cn/'], 'proc'),
    # —— 营口市（6）——
    ('yk-fgw', '市发展改革委', '营口', None,
     ['https://fgw.yingkou.gov.cn/004/about.html', 'https://fgw.yingkou.gov.cn/'], 'energy'),
    ('yk-zrzy', '市自然资源局', '营口', None,
     ['http://zrzyj.yingkou.gov.cn/dynamic/zw/openlist.html?categorynum=003049014',
      'http://zrzyj.yingkou.gov.cn/'], 'proc'),
    ('yk-lycyj', '市林业和草原局', '营口', None,
     ['https://lycyj.yingkou.gov.cn/govxxgk/lyhcyj', 'https://lycyj.yingkou.gov.cn/'], 'proc'),
    ('yk-sthjj', '市生态环境局', '营口', None,
     ['https://sthjj.yingkou.gov.cn/003/003003/about.html',
      'https://sthjj.yingkou.gov.cn/'], 'proc'),
    ('yk-slj', '市水利局', '营口', None,
     ['http://www.yingkou.gov.cn/govxxgk/slj/'], 'proc'),
    ('yk-hyy', '海洋与渔业局', '营口', None,
     ['https://www.yingkou.gov.cn/'], 'proc'),
    # —— 阜新市（4）——
    ('fx-fgw', '市发展改革委', '阜新', None,
     ['https://fgw.fuxin.gov.cn/'], 'energy'),
    ('fx-zrzy', '市自然资源局', '阜新', None,
     ['https://zrzy.fuxin.gov.cn/channel/list/21082.html',
      'https://zrzy.fuxin.gov.cn/'], 'proc'),
    ('fx-sthj', '市生态环境局', '阜新', None,
     ['https://sthj.fuxin.gov.cn/channel/14412/index.html',
      'https://sthj.fuxin.gov.cn/'], 'proc'),
    ('fx-slj', '市水利局', '阜新', None,
     ['https://slj.fuxin.gov.cn/'], 'proc'),
    # —— 辽阳市（5）——
    ('ly-fgw', '市发展改革委', '辽阳', None,
     ['http://fgw.liaoyang.gov.cn/zxdt/list.html', 'http://fgw.liaoyang.gov.cn/'], 'energy'),
    ('ly-zrzy', '市自然资源局', '辽阳', None,
     ['http://zrzyj.liaoyang.gov.cn/column_list.html?categorynum=006002&deptcode=001',
      'http://zrzyj.liaoyang.gov.cn/'], 'proc'),
    ('ly-lczy', '市林业和草原局', '辽阳', None,
     ['http://www.liaoyang.gov.cn/lnly_shilincaoju/'], 'proc'),
    ('ly-sthjj', '市生态环境局', '辽阳', None,
     ['http://sthjj.liaoyang.gov.cn/column_list.html?categorynum=007&deptcode=001',
      'http://sthjj.liaoyang.gov.cn/'], 'proc'),
    ('ly-slj', '市水利局', '辽阳', None,
     ['http://slj.liaoyang.gov.cn/'], 'proc'),
    # —— 盘锦市（5）——
    ('pj-fgw', '市发展改革委', '盘锦', None,
     ['https://fgw.panjin.gov.cn/13219/', 'https://fgw.panjin.gov.cn/11817/'], 'energy'),
    ('pj-zrzy', '市自然资源局', '盘锦', None,
     ['https://zrzyj.panjin.gov.cn/14540/', 'https://zrzyj.panjin.gov.cn/13404/'], 'proc'),
    ('pj-lin', '市林业和湿地保护管理局', '盘锦', None,
     ['https://www.panjin.gov.cn/html/2292/'], 'proc'),
    ('pj-sthjj', '市生态环境局', '盘锦', None,
     ['https://sthjj.panjin.gov.cn/13436/'], 'proc'),
    ('pj-slj', '市水利局', '盘锦', None,
     ['https://slj.panjin.gov.cn/'], 'proc'),
    # —— 铁岭市（4）——
    ('tl-fgw', '市发展改革委', '铁岭', None,
     ['http://www.tieling.gov.cn/eportal/ui?pageId=648907717d2c42bba4a0a6ed562b85b3'], 'energy'),
    ('tl-zrzy', '市自然资源局', '铁岭', None,
     ['http://www.tieling.gov.cn/eportal/ui?pageId=9fe12c057b32460ba6616d9b11e9435e'], 'proc'),
    ('tl-sthj', '市生态环境局', '铁岭', None,
     ['http://www.tieling.gov.cn/tieling/zwgk/zfxxgk/fdzdgknr/zdmsxx/hjbh/jsxmhjspqkgs/index.html'], 'proc'),
    ('tl-slj', '市水利局', '铁岭', None,
     ['http://www.tieling.gov.cn/tieling/zwgk/zfxxgk/fdzdgknr/zdmsxx/tlsl/index.html'], 'proc'),
    # —— 朝阳市（5）——
    ('cy-fgw', '市发展改革委', '朝阳', None,
     ['https://fgw.chaoyang.gov.cn/cysfzhggwyh/tzgg/glist.html',
      'https://fgw.chaoyang.gov.cn/cysfzhggwyh/'], 'energy'),
    ('cy-zrzy', '市自然资源局', '朝阳', None,
     ['https://zrzyj.chaoyang.gov.cn/cyszrzyj/gk/tzgg/glist.html',
      'https://zrzyj.chaoyang.gov.cn/cyszrzyj/'], 'proc'),
    ('cy-lcj', '市林业和草原局', '朝阳', None,
     ['https://www.chaoyang.gov.cn/cyszf/zwgk/bmzfxxgkpt/lcj/'], 'proc'),
    ('cy-sthjj', '市生态环境局', '朝阳', None,
     ['https://sthjj.chaoyang.gov.cn/cyssthjj/hjxx/jsxmhjyxpjxx/ypzxmgs/glist.html',
      'https://sthjj.chaoyang.gov.cn/'], 'proc'),
    ('cy-swj', '市水务局', '朝阳', None,
     ['https://swj.chaoyang.gov.cn/'], 'proc'),
    # —— 葫芦岛市（5）——
    ('hld-fgw', '市发展改革委', '葫芦岛', None,
     ['https://fgw.hld.gov.cn/'], 'energy'),
    ('hld-zrzy', '市自然资源局', '葫芦岛', None,
     ['https://zrzy.hld.gov.cn/zwgk/zfxxgk/fdzdgknr/ghxx/ydghxk/',
      'https://zrzy.hld.gov.cn/'], 'proc'),
    ('hld-sthj', '市生态环境局', '葫芦岛', None,
     ['https://sthj.hld.gov.cn/zwgk/zfxxgk/fdzdgknr/jsxm/index.html',
      'https://sthj.hld.gov.cn/'], 'proc'),
    ('hld-slj', '市水利局', '葫芦岛', None,
     ['https://slj.hld.gov.cn/xwzx/tzgg/', 'https://slj.hld.gov.cn/'], 'proc'),
    ('hld-hyy', '海洋与渔业局', '葫芦岛', None,
     ['https://www.hld.gov.cn/xwdt/tzgg/'], 'proc'),
    # —— 绥中县（1）——
    ('sz-gov', '县人民政府', '葫芦岛', '绥中',
     ['https://www.szx.gov.cn/'], 'energy'),
]

log = logging.getLogger('daily')
SESSION = requests.Session()
SESSION.headers.update(HEADERS)

A_RE = re.compile(r'<a\b[^>]*?href\s*=\s*["\']([^"\'#]+)["\'][^>]*>(.*?)</a>',
                  re.S | re.I)
TAG_RE = re.compile(r'<[^>]+>')
DATE_RE = re.compile(r'(20\d{2})[-/.年]\s*(\d{1,2})[-/.月]\s*(\d{1,2})')
SCRIPT_RE = re.compile(r'<(script|style)\b[\s\S]*?</\1>', re.I)
SKIP_EXT = ('.pdf', '.doc', '.docx', '.xls', '.xlsx', '.zip', '.rar',
            '.jpg', '.jpeg', '.png', '.gif', '.wgt', '.css', '.js')
NAV_TEXTS = {'首页', '上一页', '下一页', '末页', '更多', '详情', '返回', '关闭',
             '打印', '收藏', '【关闭窗口】', '【打印本页】', '无标题文档'}

# 官方页恢复监控清单：条目因官方站文章清理被删除后每日探测原 URL，
# 恢复可访问且正文含全部关键词时自动重新入库（id 按当日运行重新生成）。
# 字段为被删条目的原始内容；新增监控项照此格式追加即可。
WATCH_RECOVER = [
    {
        'title': '关于锦州市50万千瓦风电项目竞争性配置结果公示',
        'docno': '', 'org': '锦州市发展和改革委员会', 'date': '2026-10-14',
        'url': 'https://fgw.jz.gov.cn/info/1058/7086.htm',
        'level': 'city', 'city': '锦州', 'field': 'competitive',
        'digest': '按《辽宁省新能源项目竞争性配置指导意见》有关要求，经第三方机构组织专家评审，推荐各标段中选企业：LH-1标段锦州海胜电力新能源有限公司20万千瓦、BZ-1标段锦州宸朔新能源有限公司15万千瓦、YX-1标段辽宁大唐国际新能源有限公司15万千瓦，公示期2026年10月14日至10月16日。',
        'impact': '锦州50万千瓦风电指标完成业主优选，与鞍山、葫芦岛同为50万千瓦体量批次；中选企业后续核准、用地等前期手续动态需持续跟踪。',
        'ref': '锦州市发展改革委竞争性配置结果公示',
        'probe_kw': ('锦州', '竞争性配置'),
    },
]


def setup_log():
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(levelname)s %(message)s',
                        datefmt='%H:%M:%S')
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        pass


def http_get(url, timeout=25):
    last = None
    for i, t in enumerate((timeout, timeout + 15)):
        try:
            r = SESSION.get(url, timeout=t, verify=False, allow_redirects=True)
        except requests.RequestException as e:
            last = e
            continue
        if r.status_code == 200:
            return decode_bytes(r.content)
        last = RuntimeError('HTTP %s' % r.status_code)
        if r.status_code < 500:
            break
        time.sleep(4)
    raise RuntimeError(str(last)[:120])


def decode_bytes(data):
    m = re.search(r'charset=([\w-]+)', (data[:2048]).decode('ascii', 'ignore'),
                  re.I)
    encs = ([m.group(1)] if m else []) + ['utf-8', 'gb18030']
    for e in encs:
        try:
            return data.decode(e)
        except (UnicodeDecodeError, LookupError):
            continue
    return data.decode('utf-8', 'ignore')


def norm_url(u):
    return re.sub(r'\s+', '', u.strip()).lower()


def parse_date(s):
    m = DATE_RE.search(s or '')
    if not m:
        return None
    try:
        return datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)),
                        tzinfo=BJT).date()
    except ValueError:
        return None


def extract_links(page, base):
    out = []
    for m in A_RE.finditer(page):
        href = m.group(1).strip()
        text = re.sub(r'\s+', '', TAG_RE.sub('', m.group(2)))
        if not href or href.startswith(('javascript:', 'mailto:', 'tel:')):
            continue
        text = html.unescape(text)
        url = urljoin(base, href)
        if not url.startswith(('http://', 'https://')):
            continue
        path = url.split('?')[0].split('#')[0]
        if path.lower().endswith(SKIP_EXT):
            continue
        if re.search(r'index(_\d+)?\.s?html?$', path, re.I):
            continue
        if not text or len(text) < 8 or text in NAV_TEXTS:
            continue
        out.append((url, text))
    return out


def crawl_channel(ch, existing_urls, cutoff):
    """返回 (candidates, error)。candidates: [{url,title,dt}]"""
    cid, name, city, county, urls, mode = ch
    kws = KW_PROC if mode in ('proc', 'nav') else KW_ENERGY
    cands, seen = [], set()
    error = None
    for url in urls:
        try:
            html_txt = http_get(url)
            error = None
        except Exception as e:
            error = '%s: %s' % (url.split('/')[2] if '://' in url else url, e)
            continue
        for link_url, text in extract_links(html_txt, url):
            if not any(k in text for k in kws):
                continue
            nu = norm_url(link_url)
            if nu in seen or nu in existing_urls:
                continue
            dt = parse_date(html_txt[max(0, html_txt.find(text) - 260):
                                     html_txt.find(text) + 300] if text in html_txt else '')
            if dt and dt < cutoff.date():
                continue
            seen.add(nu)
            cands.append({'url': link_url, 'title': text, 'dt': dt,
                          'key_hit': any(k in text for k in KEY_PROJECTS)})
            # 上限为通过关键词粗筛且在时间窗内的新候选数（与库去重后），
            # 30 条用于吸收环评按月批次、竞配结果日等集中挂网场景
            if len(cands) >= 30:
                break
    if not cands and error:
        return cands, error
    return cands, error


def fetch_article(cand):
    try:
        page = http_get(cand['url'])
    except Exception as e:
        return None, 'fetch: %s' % e
    title = ''
    mt = re.search(r'<title[^>]*>([\s\S]*?)</title>', page, re.I)
    if mt:
        title = TAG_RE.sub('', mt.group(1)).strip()
    body = SCRIPT_RE.sub(' ', page)
    text = TAG_RE.sub(' ', body)
    text = html.unescape(text)
    text = re.sub(r'[ \t\r\f\v]+', ' ', text)
    text = re.sub(r'\n\s*\n+', '\n', text).strip()
    if len(text) < 300:
        return None, 'page too short (%d chars), likely JS-rendered' % len(text)
    return {'title': title, 'text': text[:4500]}, None


SYS_PROMPT = '''你是新能源政策收录研判员，为"辽宁省新能源政策跟踪"数据库研判候选网页。输出严格的 JSON 对象，不要输出任何 JSON 以外的文字（不要 markdown 代码块）。字段定义：
collect: boolean，是否收录
reason: string，30字内判定理由（collect=false 也必填）
collect=true 时以下字段必填，否则填 null：
level: "national"|"liaoning"|"city"（发布机关层级：国家部委=national，辽宁省省级机关=liaoning，市/县级机关=city）
field: "competitive"竞争性配置|"retrofit"老旧机组改造|"distribution"增量配电|"pricing"电价市场化|"storage"储能与消纳|"pv"分布式光伏|"plan"发展规划|"procedure"前期手续|"other"兜底
city: 发布机关所属地级市名（如"大连"），仅 level=city 时填写，否则 null
county: 仅当发布机关为岫岩满族自治县时填"岫岩"、绥中县时填"绥中"，否则 null
stage: field=procedure 时必填："approval"核准备案|"land"用地|"sea"用海|"forest"用林用草|"eia"环评|"navigation"通航|"mineral"压覆矿产|"water"水土保持|"grid"电网接入|"other"其他手续，否则 null
project: field=procedure 时必填：涉及的单一项目规范化名称，格式为"业主+规模+类型(+地点)"（如"中广核绥中80万千瓦海上风电"、"辽宁华电昌图一期450MW离网风电"），从标题与正文提取，不得臆造或改写规模数字；面向全行业的政策文件（通知/方案/规则）或涉及多个项目的批次汇总填 null
title: 官方文件标题（照原文，不含书名号外引号）
docno: 文号（如"辽发改能源〔2025〕86号"），无则 null
org: 发文机构全称
date: 成文或发布日期 YYYY-MM-DD，原文无法确定则 null（不得猜测）
digest: 80~150字，仅转述原文内容，数字必须与原文一致，单行无换行
impact: 40~80字，写对辽宁新能源项目投资建设与前期手续的影响，不得写原文没有的推断，单行无换行
ref: 条款出处（如"发改价格〔2025〕136号二（一）"），全文性文件写文号+"全文"，无则 null
key: boolean，仅国家或省级重大改革文件为 true，一般批复/公告为 false

收录规则（宁缺毋滥）：
1. 只收录 2025-01-01 及以后发布的条目，更早的 collect=false。
2. 只收结论性节点文件：批复、同意书、许可、核准、备案、成交结果、正式印发的政策文件；受理公示、拟审批公示、征地启动公告、补偿安置公告、招标公告（非结果）、过程性公示、新闻报道、会议动态一律 collect=false。
3. 与风电、光伏、新能源、储能、电力市场、电网及其用地用海用林环评水保通航等手续无关的 collect=false。
4. 纯转发上级文件且无本地补充内容的市级文件 collect=false。
5. 页面为栏目列表页、无实质文件内容的 collect=false。
6. 信息不足以判断时 collect=false，不得编造。'''


def glm_call(api_key, user_content):
    base = {
        'model': MODEL,
        'messages': [
            {'role': 'system', 'content': SYS_PROMPT},
            {'role': 'user', 'content': user_content},
        ],
        'temperature': 0.2,
        'max_tokens': 8192,
    }
    # glm-5.3-flash 始终思考、不支持 thinking:disabled（传了返回 400 code 1210），
    # 故不带 thinking 参数；json_object 优先，退化为自由文本再解析
    variants = [
        dict(base, response_format={'type': 'json_object'}),
        dict(base),
    ]
    last_err = None
    for body in variants:
        for retry in range(3):
            try:
                r = requests.post(API_URL, json=body, timeout=180,
                                  headers={'Authorization': 'Bearer ' + api_key,
                                           'Content-Type': 'application/json'})
                if r.status_code in (429, 500, 502, 503, 504) and retry < 2:
                    last_err = 'HTTP %s(重试%d)' % (r.status_code, retry + 1)
                    time.sleep(8 * (retry + 1))
                    continue
                if r.status_code >= 400:
                    last_err = 'HTTP %s %s' % (r.status_code, r.text[:160])
                    break
                data = r.json()
                content = (data.get('choices') or [{}])[0].get('message', {}) \
                    .get('content') or ''
                obj = parse_json_obj(content)
                if obj is None and retry < 2:
                    last_err = '模型返回空/非JSON(重试%d)' % (retry + 1)
                    time.sleep(5)
                    continue
                return obj, None if obj is not None else last_err
            except Exception as e:
                last_err = str(e)
                if retry < 2:
                    time.sleep(8 * (retry + 1))
    return None, last_err


def parse_json_obj(content):
    content = content.strip()
    content = re.sub(r'^```(?:json)?\s*', '', content)
    content = re.sub(r'\s*```$', '', content)
    try:
        return json.loads(content)
    except Exception:
        pass
    i, j = content.find('{'), content.rfind('}')
    if 0 <= i < j:
        try:
            return json.loads(content[i:j + 1])
        except Exception:
            pass
    return None


def js_str(s):
    s = (s or '').replace('\\', '\\\\').replace("'", "\\'")
    return s.replace('<', '〈').replace('>', '〉').replace('\r', ' ').replace('\n', ' ')


def clean_text(s, limit):
    s = re.sub(r'\s+', '', s or '')
    return s[:limit]


def load_existing(text):
    urls = {norm_url(u) for u in re.findall(r"url:'([^']+)'", text)}
    docnos = {re.sub(r'\s+', '', d) for d in re.findall(r"docno:'([^']*)'", text)
              if d.strip()}
    titles = set()
    for t in re.findall(r"title:'([^']+)'", text):
        titles.add(re.sub(r'\s+', '', t))
    ids = re.findall(r"id:'d(\d{8})-(\d+)'", text)
    return urls, docnos, titles, ids


def build_entry_js(e):
    lines = [
        "    id:'%s', level:'%s', field:'%s', top:%s," % (
            e['id'], e['level'], e['field'], 'true' if e.get('key') else 'false'),
    ]
    if e.get('city'):
        lines.append("    city:'%s'," % js_str(e['city']))
    if e.get('county'):
        lines.append("    county:'%s'," % js_str(e['county']))
    if e.get('stage'):
        lines.append("    stage:'%s'," % e['stage'])
    if e.get('project'):
        lines.append("    project:'%s'," % js_str(e['project']))
    lines += [
        "    title:'%s'," % js_str(e['title']),
        "    docno:'%s', org:'%s', date:'%s'," % (
            js_str(e.get('docno') or ''), js_str(e['org']), e['date']),
        "    url:'%s'," % e['url'],
        "    digest:'%s'," % js_str(e['digest']),
        "    impact:'%s'," % js_str(e['impact']),
    ]
    if e.get('ref'):
        lines.append("    ref:'%s'," % js_str(e['ref']))
    if e.get('key'):
        lines.append("    status:'key',")
    lines.append("    createdAt:'%s'" % e['createdAt'])
    return '  {\n' + '\n'.join(lines) + '\n  }'


def validate_entry(d, cand, today):
    if not isinstance(d, dict) or not d.get('collect'):
        return None, (d or {}).get('reason') or 'collect=false'
    level = d.get('level')
    field = d.get('field')
    if level not in LEVELS:
        return None, '非法 level=%s' % level
    if field not in FIELDS:
        return None, '非法 field=%s' % field
    if level == 'city' and d.get('city') not in CITIES:
        return None, 'city 层级缺少合法城市'
    stage = d.get('stage')
    if field == 'procedure' and stage not in STAGES:
        return None, 'procedure 缺 stage'
    date = d.get('date')
    if not (isinstance(date, str) and re.match(r'^20\d{2}-\d{2}-\d{2}$', date)
            and '2025-01-01' <= date <= str(today)):
        return None, '日期缺失或超出收录窗口'
    title = clean_text(d.get('title'), 150)
    org = clean_text(d.get('org'), 100)
    digest = re.sub(r'\s+', ' ', (d.get('digest') or '').strip())
    impact = re.sub(r'\s+', ' ', (d.get('impact') or '').strip())
    if not title or not org or not digest:
        return None, 'title/org/digest 缺失'
    entry = {
        'level': level, 'field': field, 'title': title, 'org': org,
        'date': date, 'url': cand['url'], 'digest': digest,
        'impact': impact[:300] if impact else digest[:120],
        'createdAt': date,
    }
    if level == 'city':
        entry['city'] = d['city']
        if field == 'procedure':
            entry['stage'] = stage
        elif stage in STAGES:
            entry['stage'] = stage
    elif field == 'procedure':
        entry['stage'] = stage
    if field == 'procedure' and isinstance(d.get('project'), str) \
            and d['project'].strip():
        entry['project'] = clean_text(d['project'], 60)
    if d.get('county') in COUNTIES:
        entry['county'] = d['county']
    if isinstance(d.get('docno'), str) and d['docno'].strip():
        entry['docno'] = clean_text(d['docno'], 60)
    if isinstance(d.get('ref'), str) and d['ref'].strip():
        entry['ref'] = d['ref'].strip()[:120]
    if d.get('key') is True:
        entry['key'] = True
    return entry, 'ok'


def gh_token():
    """GitHub 令牌：服务器读 /opt/policy/github_token（600 权限），
    本地测试用环境变量 POLICY_GH_TOKEN；都没有则返回空串（跳过推送）。"""
    p = '/opt/policy/github_token'
    if os.path.exists(p):
        try:
            return open(p).read().strip()
        except OSError:
            return ''
    return os.environ.get('POLICY_GH_TOKEN', '')


def gh_push_file(local_path, remote, msg):
    """自包含 contents API 推送（资源跟踪数据文件专用）。返回 (ok, 说明)。"""
    token = gh_token()
    if not token:
        return False, '无令牌，跳过推送'
    api = 'https://api.github.com/repos/NEDaily/policy-tracker/contents/' + remote
    body = {'message': msg,
            'content': base64.b64encode(open(local_path, 'rb').read()).decode()}
    hd = {'Authorization': 'Bearer ' + token}
    try:
        r = requests.get(api, headers=hd, timeout=30)
        if r.status_code == 200:
            body['sha'] = r.json().get('sha')
        for attempt in range(3):
            rr = requests.put(api, headers=hd, json=body, timeout=30)
            if rr.status_code in (200, 201):
                return True, 'commit %s' % rr.json()['commit']['sha'][:8]
            if rr.status_code in (409, 422) and attempt < 2:
                time.sleep(5)
                r2 = requests.get(api, headers=hd, timeout=30)
                if r2.status_code == 200:
                    body['sha'] = r2.json().get('sha')
                continue
            return False, 'HTTP %d' % rr.status_code
    except Exception as e:
        return False, str(e)[:80]
    return False, '未知状态'


def recover_watch(item, existing_urls):
    """恢复监控：原 URL 重新可访问且正文含全部关键词 → 返回条目 dict。"""
    if norm_url(item['url']) in existing_urls:
        return None
    try:
        body = http_get(item['url'], timeout=20)
    except Exception:
        return None
    if not all(kw in body for kw in item['probe_kw']):
        return None
    ent = {k: item[k] for k in ('title', 'docno', 'org', 'date', 'url',
                                'level', 'field', 'digest', 'impact', 'ref')}
    if item.get('city'):
        ent['city'] = item['city']
    return ent


def check_wind_track(new_entries):
    """资源跟踪板块联动：本窗口新入库的市级竞配「结果公示」条目 →
    自动更新 WIND_TRACK 对应市 done 字段（仅限完成态为 —/待补录/结果未查见 的市）。
    竞配公告类（非结果公示）只在留痕中提示，不动数据。返回变更描述列表。"""
    if not new_entries or not os.path.exists(RESOURCE_FILE):
        return []
    text = open(RESOURCE_FILE, encoding='utf-8').read()
    lines = text.splitlines(True)
    done_open = ('—', '待补录', '结果未查见')
    changes = []
    for e in new_entries:
        if e.get('field') != 'competitive' or e.get('level') != 'city':
            continue
        city = e.get('city') or ''
        title = e.get('title') or ''
        is_result = '结果公示' in title or ('优选' in title and '公示' in title)
        if not is_result:
            if '竞争性配置' in title or '竞配' in title:
                changes.append('%s竞配公告类条目入库（%s），完成态未动' % (city, title[:24]))
            continue
        hit = None
        for i, ln in enumerate(lines):
            if re.search(r"city:\s*'%s'" % city, ln) and 'done:' in ln:
                m = re.search(r"done:\s*\{v:\s*'([^']*)'", ln)
                if m and (m.group(1) in done_open or '未' in m.group(1)):
                    hit = i
                break
        if hit is None:
            continue
        mm = (re.search(r'(\d+(?:\.\d+)?)\s*万千瓦', title)
              or re.search(r'(\d+(?:\.\d+)?)\s*万千瓦', e.get('digest') or ''))
        v = (mm.group(1) + '万千瓦') if mm else '已优选'
        mdate = re.match(r'(20\d{2})-(\d{1,2})-(\d{1,2})', e.get('date') or '')
        src = ('%d.%d.%d 结果公示' % (int(mdate.group(1)), int(mdate.group(2)),
                                     int(mdate.group(3))) if mdate else '竞配结果公示')
        lines[hit] = re.sub(r"done:\s*\{[^}]*\}",
                            "done:{v:'%s', src:'%s'}" % (v, src),
                            lines[hit], count=1)
        changes.append('%s完成竞配已自动更新为 %s（%s）' % (city, v, src))
    if changes:
        open(RESOURCE_FILE, 'w', encoding='utf-8').write(''.join(lines))
    return changes


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--dry', action='store_true', help='只抓取不写文件不调GLM')
    ap.add_argument('--analyze', type=int, default=0,
                    help='dry 模式下对前 N 个候选调用 GLM 并打印结果')
    ap.add_argument('--window-hours', type=int, default=72)
    ap.add_argument('--max-glm', type=int, default=60)
    ap.add_argument('--limit-channels', type=int, default=0)
    args = ap.parse_args()

    setup_log()
    now = datetime.now(BJT)
    cutoff = now - timedelta(hours=args.window_hours)
    ts = now.strftime('%Y-%m-%d %H:%M')
    log.info('运行开始 %s（窗口 %d 小时）', ts, args.window_hours)

    with open(DATA_FILE, 'r', encoding='utf-8') as f:
        data_text = f.read()
    existing_urls, existing_docnos, existing_titles, all_ids = \
        load_existing(data_text)
    today_prefix = now.strftime('%Y%m%d')
    seq = max([int(n) for p, n in all_ids if p == today_prefix] or [0])

    channels = CHANNELS[:args.limit_channels] if args.limit_channels else CHANNELS
    results, all_cands = {}, []
    seen_global = set()
    with ThreadPoolExecutor(max_workers=10) as pool:
        futs = {pool.submit(crawl_channel, ch, existing_urls, cutoff): ch
                for ch in channels}
        for fut in as_completed(futs):
            ch = futs[fut]
            try:
                cands, err = fut.result()
            except Exception as e:
                cands, err = [], 'internal: %s' % e
            results[ch[0]] = (ch[1], len(cands), err)
            for c in cands:
                c['ch'] = ch
                if norm_url(c['url']) not in seen_global:
                    seen_global.add(norm_url(c['url']))
                    all_cands.append(c)
    ok_ch = sum(1 for v in results.values() if not v[2])
    fail_list = ['%s(%s)' % (v[0], v[2].split(':')[0]) for v in results.values() if v[2]]
    log.info('渠道：成功 %d/%d，候选 %d', ok_ch, len(channels), len(all_cands))
    for cid, (name, n, err) in results.items():
        if err:
            log.info('渠道异常 %s → %s', name, err)
    if args.dry and args.analyze == 0:
        log.info('[dry] 候选清单（前40条）:')
        for c in all_cands[:40]:
            log.info('  [%s] %s | %s', c['ch'][2] or c['ch'][1],
                     c.get('dt') or '无日期', c['title'][:60])
        log.info('[dry] 结束，未调用 GLM、未写文件')
        return 0

    # 重点项目业主命中者优先进入研判候选（稳定排序，保持渠道内原有次序）
    all_cands.sort(key=lambda c: 0 if c.get('key_hit') else 1)
    all_cands = all_cands[:80]

    api_key = os.environ.get('ZHIPU_API_KEY', '')
    if not api_key:
        log.error('ZHIPU_API_KEY 未设置，无法研判')
    articles = {}
    with ThreadPoolExecutor(max_workers=8) as pool:
        futs = {pool.submit(fetch_article, c): c for c in all_cands}
        for fut in as_completed(futs):
            c = futs[fut]
            art, err = fut.result()
            if err:
                log.info('正文跳过 %s → %s', c['title'][:40], err)
            else:
                articles[c['url']] = art
    log.info('正文可用 %d/%d', len(articles), len(all_cands))

    new_entries, rejected, glm_calls, err_glm = [], [], 0, 0
    key_used = False
    for c in all_cands:
        if len(new_entries) >= 40 or glm_calls >= args.max_glm:
            break
        art = articles.get(c['url'])
        if not art:
            continue
        if not api_key:
            break
        glm_calls += 1
        ch_city = c['ch'][2]
        hint = '渠道：%s%s' % (ch_city or '', c['ch'][1] if not ch_city else c['ch'][1])
        user = '渠道归属：%s\n来源URL：%s\n\n页面标题：%s\n\n页面正文：\n%s' % (
            hint, c['url'], art['title'][:200], art['text'])
        obj, gerr = glm_call(api_key, user)
        if gerr:
            err_glm += 1
        if obj is None:
            log.info('GLM 解析失败(%s) → %s', (gerr or '模型返回空/非JSON')[:100],
                     c['title'][:40])
            continue
        entry, why = validate_entry(obj, c, now.date())
        if not entry:
            rejected.append('%s:%s' % (c['title'][:30], why[:40]))
            continue
        if entry.get('key') and key_used:
            entry.pop('key', None)
        if entry.get('key'):
            key_used = True
        tkey = re.sub(r'\s+', '', entry['title'])
        dkey = re.sub(r'\s+', '', entry.get('docno') or '')
        if tkey in existing_titles or dkey and dkey in existing_docnos:
            rejected.append('%s:与库内重复' % c['title'][:30])
            continue
        tset = {re.sub(r'\s+', '', e['title']) for e in new_entries}
        dset = {re.sub(r'\s+', '', e.get('docno') or '') for e in new_entries}
        if tkey in tset or (dkey and dkey in dset):
            continue
        seq += 1
        entry['id'] = 'd%s-%d' % (today_prefix, seq)
        new_entries.append(entry)
        log.info('收录 %s | %s | %s %s', entry['id'], entry['title'][:36],
                 entry['level'], entry.get('city') or '')

    for r in rejected[:12]:
        log.info('不收录 → %s', r)

    # 官方页恢复监控：WATCH_RECOVER 清单 URL 恢复可访问则自动重新入库
    rec_n = 0
    for w in WATCH_RECOVER:
        try:
            ent = recover_watch(w, existing_urls)
        except Exception:
            ent = None
        if not ent:
            continue
        if re.sub(r'\s+', '', ent['title']) in existing_titles:
            continue
        seq += 1
        ent['id'] = 'd%s-%d' % (today_prefix, seq)
        ent['createdAt'] = ent['date']
        new_entries.append(ent)
        rec_n += 1
        log.info('恢复入库 %s | %s', ent['id'], ent['title'][:36])
    log.info('恢复监控 %d 项，恢复入库 %d 条', len(WATCH_RECOVER), rec_n)

    new_count = len(new_entries)
    updated_at = ts
    data_text = re.sub(r"window\.POLICY_UPDATED_AT = '[^']*';",
                       "window.POLICY_UPDATED_AT = '%s';" % updated_at,
                       data_text, count=1)
    data_text = re.sub(r'window\.POLICY_UPDATED_COUNT = \d+;',
                       'window.POLICY_UPDATED_COUNT = %d;' % new_count,
                       data_text, count=1)
    if new_entries:
        blocks = ',\n'.join(build_entry_js(e) for e in new_entries)
        pos = data_text.rfind('\n];')
        if pos < 0:
            log.error('未找到数据数组结尾，放弃写入')
            return 1
        data_text = data_text[:pos] + ',\n' + blocks + '\n];' + data_text[pos + 3:]
    if args.dry:
        log.info('[dry] 模拟结果：新增 %d 条（未写文件）', new_count)
        for e in new_entries:
            log.info('[dry] %s\n%s\n', e['id'], build_entry_js(e))
        return 0
    with open(DATA_FILE, 'w', encoding='utf-8') as f:
        f.write(data_text)

    # 资源跟踪板块联动：新竞配结果公示 → 更新 WIND_TRACK → 独立推送
    wind_changes = check_wind_track(new_entries)
    if not wind_changes:
        log.info('资源跟踪联动：本窗口无市级竞配结果公示条目，WIND_TRACK 未变动')
    for c in wind_changes:
        log.info('资源跟踪：%s', c)
    if wind_changes:
        ok, info = gh_push_file(RESOURCE_FILE, 'resource-data.js',
                                '资源跟踪自动更新 %s（腾讯云）' % ts)
        log.info('资源跟踪数据推送%s：%s', '成功' if ok else '未完成', info)

    status = '完成' if not fail_list and err_glm == 0 else (
        '完成(部分异常)' if new_count or ok_ch else '异常')
    log_line = ('- [%s] %s 新增%d条｜渠道 成功%d/%d｜候选%d｜GLM %d次｜%s'
                % (status, ts, new_count, ok_ch, len(channels),
                   len(all_cands), glm_calls,
                   ('失败渠道：' + '、'.join(fail_list[:8])) if fail_list else '渠道全部正常'))
    if wind_changes:
        log_line += '｜资源跟踪 ' + '；'.join(wind_changes[:3])
    if not os.path.exists(RUNLOG_FILE):
        with open(RUNLOG_FILE, 'w', encoding='utf-8') as f:
            f.write('# 每日任务运行日志（云端 GitHub Actions）\n\n'
                    '每天 14:30（北京时间）自动运行；每次运行无论有无新增都刷新版头'
                    '并推送。本地任务留痕见任务清单「每日任务运行日志」小节。\n\n')
    with open(RUNLOG_FILE, 'a', encoding='utf-8') as f:
        f.write(log_line + '\n')

    ghenv = os.environ.get('GITHUB_ENV')
    if ghenv:
        with open(ghenv, 'a', encoding='utf-8') as f:
            f.write('NEW_COUNT=%d\nRUN_TS=%s\n' % (new_count, ts))
    log.info('[完成] 新增 %d 条；版头已刷新为 %s / 新增 %d 条',
             new_count, updated_at, new_count)
    return 0


if __name__ == '__main__':
    sys.exit(main())
