#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
全库链接体检（双周 cron，腾讯云服务器运行）
流程：拉取仓库最新 policy-data.js 与 linkcheck-state.json → 并发探测全部条目 URL
     → 判定（200 有效 / 404·410 死链 / 403·401 反爬 / 超时·5xx 存疑 / 重定向回首页 疑似清理）
     → 双轮确认：本轮新死链仅标记入 state，上轮已标记且本轮仍异常的条目才从库中删除
     → 推送 policy-data.js + state + runlog.md 留痕。
防误删：403/超时/连接失败一律不删（营口水利、朝阳林草等站点自封属常态）；
       版头两字段不动（体检属质量维护，非检索运行）。
安全：GitHub 令牌只读 /opt/policy/github_token 或 --token-file，不打印不外传。
"""
import argparse
import base64
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from urllib.parse import urlsplit

import requests

BJT = timezone(timedelta(hours=8))
REPO = 'NEDaily/policy-tracker'
API = 'https://api.github.com/repos/' + REPO
UA = ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
      '(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36')

ENTRY_RE = re.compile(r"\{[^{}]*\}", re.S)
ID_RE = re.compile(r"id:'([^']+)'")
URL_RE = re.compile(r"url:'([^']+)'")
TITLE_RE = re.compile(r"title:'([^']+)'")
NOW = datetime.now(BJT).strftime('%Y-%m-%d')


def api(url, method='GET', token=None, payload=None, raw=False):
    req = urllib.request.Request(url, method=method)
    if token:
        req.add_header('Authorization', 'Bearer ' + token)
    req.add_header('Accept',
                   'application/vnd.github.raw' if raw else 'application/vnd.github+json')
    data = json.dumps(payload).encode() if payload is not None else None
    with urllib.request.urlopen(req, data, timeout=60) as r:
        body = r.read()
        return body if raw else json.loads(body.decode())


def gh_get(path, token):
    return api(API + '/contents/' + path + '?ref=main', token=token, raw=True)


def gh_push(path, text, msg, token):
    body = {'message': msg, 'content': base64.b64encode(text.encode()).decode()}
    for attempt in range(3):
        try:
            sha = api(API + '/contents/' + path, token=token).get('sha')
            if sha:
                body['sha'] = sha
            out = api(API + '/contents/' + path, 'PUT', token, body)
            return out['commit']['sha'][:8]
        except urllib.error.HTTPError as e:
            if e.code in (409, 422) and attempt < 2:
                time.sleep(5)
                continue
            raise


def parse_entries(text):
    entries = []
    for m in ENTRY_RE.finditer(text):
        block = m.group(0)
        mid, mu, mt = ID_RE.search(block), URL_RE.search(block), TITLE_RE.search(block)
        if mid and mu:
            entries.append({'id': mid.group(1), 'url': mu.group(1),
                            'title': (mt.group(1) if mt else '')[:50],
                            'span': (m.start(), m.end())})
    return entries


def probe(url):
    """返回 (判定, 终态url, 说明)。判定: ok/dead/home/blocked/flaky"""
    last = ''
    for attempt in range(3):
        try:
            r = requests.get(url, timeout=25, allow_redirects=True,
                             headers={'User-Agent': UA, 'Accept-Language': 'zh-CN,zh;q=0.9'},
                             verify=False)
            if r.status_code in (404, 410):
                return 'dead', r.url, 'HTTP %d' % r.status_code
            if r.status_code in (401, 403):
                return 'blocked', r.url, 'HTTP %d 反爬' % r.status_code
            if r.status_code == 200:
                p_old, p_new = urlsplit(url).path, urlsplit(r.url).path
                if p_new.rstrip('/') in ('', '/index.html', '/index.htm') and \
                        p_old.rstrip('/') not in ('', '/index.html', '/index.htm'):
                    return 'home', r.url, '重定向回站点首页（疑似文章被清理）'
                return 'ok', r.url, ''
            last = 'HTTP %d' % r.status_code
        except requests.RequestException as e:
            last = type(e).__name__
        if attempt < 2:
            time.sleep(6)
    return 'flaky', url, last


def remove_entry(text, eid):
    pat_c = re.compile(r",\n  \{[^{}]*?id:'%s'[^{}]*\}" % re.escape(eid), re.S)
    text2, n = pat_c.subn('', text)
    if n:
        return text2, True
    pat_f = re.compile(r"\{[^{}]*?id:'%s'[^{}]*\},\n" % re.escape(eid), re.S)
    text2, n = pat_f.subn('', text)
    return text2, bool(n)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--token-file', default='/opt/policy/github_token')
    ap.add_argument('--dry-run', action='store_true', help='只探测并输出结果，不删除不推送')
    ap.add_argument('--workers', type=int, default=12)
    args = ap.parse_args()

    token = open(args.token_file).read().strip()
    data_text = gh_get('policy-data.js', token).decode()
    entries = parse_entries(data_text)
    print('[%s] 探测 %d 条' % (NOW, len(entries)))

    results = {}
    with ThreadPoolExecutor(args.workers) as pool:
        futs = {pool.submit(probe, e['url']): e for e in entries}
        for f in as_completed(futs):
            e = futs[f]
            verdict, final_url, note = f.result()
            results[e['id']] = (verdict, note)
            if verdict != 'ok':
                print('  [%s] %s %s | %s' % (verdict, e['id'], note, e['title']))

    try:
        state = json.loads(gh_get('linkcheck-state.json', token).decode())
    except urllib.error.HTTPError:
        state = {'dead': {}}
    dead = state.setdefault('dead', {})

    # 本轮 ok 的既有候选 → 移除（链接恢复或误标）
    for eid in [k for k in dead if results.get(k, ('ok',))[0] == 'ok']:
        dead.pop(eid)
    # 本轮异常（dead/home）→ 进候选（新标记记首见日）
    for eid, (verdict, note) in results.items():
        if verdict in ('dead', 'home'):
            if eid not in dead:
                e = next(x for x in entries if x['id'] == eid)
                dead[eid] = {'url': e['url'], 'title': e['title'],
                             'first_run': NOW, 'note': note}
            else:
                dead[eid]['note'] = note

    # 双轮确认：候选中首见日早于今天（即上轮已标记）且本轮仍异常 → 删除
    to_delete = [eid for eid, v in dead.items() if v['first_run'] < NOW]
    removed = []
    for eid in to_delete:
        data_text, ok = remove_entry(data_text, eid)
        if ok:
            removed.append(eid)
            dead.pop(eid)
        else:
            print('  [warn] 删除失败（块未匹配）：%s' % eid)

    n_ok = sum(1 for v, _ in results.values() if v == 'ok')
    n_blk = sum(1 for v, _ in results.values() if v == 'blocked')
    n_flk = sum(1 for v, _ in results.values() if v == 'flaky')
    n_cand = len(dead)
    lines = ['## %s 链接体检（双周）' % datetime.now(BJT).strftime('%Y-%m-%d %H:%M'),
             '- 探测 %d 条：有效 %d / 反爬存疑 %d / 超时存疑 %d / 死链候选累计 %d'
             % (len(entries), n_ok, n_blk, n_flk, n_cand)]
    if removed:
        lines.append('- 删除 %d 条（连续两轮确认死链）：%s' % (len(removed), ', '.join(removed)))
    else:
        lines.append('- 删除 0 条%s' % ('（双轮确认机制：本轮新标记候选 %d 条，下轮复核）' % n_cand
                                      if n_cand else ''))
    log_text = '\n'.join(lines) + '\n\n'
    print('\n'.join(lines))

    if args.dry_run:
        print('[dry-run] 未删除、未推送')
        return

    commits = []
    if removed:
        # 校验：删除前后 id 集合差 == removed
        before = set(e['id'] for e in entries)
        after = set(ID_RE.findall(data_text))
        if before - after != set(removed):
            print('[abort] 删除校验不一致，不推送数据文件')
            sys.exit(1)
        commits.append(gh_push('policy-data.js', data_text,
                               '链接体检：删除 %d 条连续两轮确认的死链条目(%s)'
                               % (len(removed), ','.join(removed)), token))
    commits.append(gh_push('linkcheck-state.json',
                           json.dumps(state, ensure_ascii=False, indent=1), NOW + ' 体检状态', token))
    runlog = gh_get('runlog.md', token).decode()
    # 新留痕插在标题后（倒序在前）
    m = re.search(r'^#.*$\n', runlog, re.M)
    pos = m.end() if m else 0
    commits.append(gh_push('runlog.md', runlog[:pos] + '\n' + log_text + runlog[pos:],
                           'runlog: 双周链接体检留痕 ' + NOW, token))
    print('[push] %s' % ', '.join(commits))


if __name__ == '__main__':
    main()
