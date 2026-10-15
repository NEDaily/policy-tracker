# -*- coding: utf-8 -*-
"""腾讯云服务器每日任务入口。
流程：拉取最新脚本与数据文件 → 运行 policy_daily.py（爬取+GLM 研判+写库+留痕）→
将 policy-data.js 与 runlog.md 推送回仓库（contents API，冲突自动重试）。
安全：GitHub 令牌与智谱密钥分别读 /opt/policy/github_token、/opt/policy/zhipu_key，不打印不外传。"""
import base64
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone

REPO = 'NEDaily/policy-tracker'
API = 'https://api.github.com/repos/' + REPO
HOME = '/opt/policy'
WORK = os.path.join(HOME, 'work')
BJT = timezone(timedelta(hours=8))


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


def download(remote, dest):
    try:
        body = api(API + '/contents/' + remote + '?ref=main', raw=True)
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return False
        raise
    with open(dest, 'wb') as f:
        f.write(body)
    return True


def push(local, remote, msg, token):
    content = open(local, 'rb').read()
    body = {'message': msg, 'content': base64.b64encode(content).decode()}
    for attempt in range(3):
        try:
            sha = api(API + '/contents/' + remote).get('sha')
            if sha:
                body['sha'] = sha
            result = api(API + '/contents/' + remote, 'PUT', token, body)
            print('[push] %s -> %s' % (remote, result['commit']['sha'][:8]))
            return True
        except urllib.error.HTTPError as e:
            if e.code in (409, 422) and attempt < 2:
                print('[push] %s 冲突，重取 sha 重试' % remote)
                time.sleep(5)
                continue
            raise
    return False


def main():
    token = open(os.path.join(HOME, 'github_token')).read().strip()
    zhipu = open(os.path.join(HOME, 'zhipu_key')).read().strip()
    os.makedirs(os.path.join(WORK, 'scripts'), exist_ok=True)

    pd_path = os.path.join(WORK, 'policy-data.js')
    rl_path = os.path.join(WORK, 'runlog.md')
    if not download('scripts/policy_daily.py', os.path.join(WORK, 'scripts', 'policy_daily.py')):
        print('下载 policy_daily.py 失败')
        sys.exit(1)
    download('policy-data.js', pd_path)
    download('runlog.md', rl_path)
    with open(pd_path, 'r', encoding='utf-8') as f:
        old_text = f.read()

    env = dict(os.environ, ZHIPU_API_KEY=zhipu)
    # 透传命令行参数（如 --window-hours 168 周度回扫）
    rc = subprocess.call([sys.executable, os.path.join(WORK, 'scripts', 'policy_daily.py')]
                         + sys.argv[1:], cwd=WORK, env=env)
    if rc != 0:
        print('[fail] policy_daily.py 退出码 %d，本次不推送' % rc)
        sys.exit(1)

    with open(pd_path, 'r', encoding='utf-8') as f:
        new_text = f.read()
    n_old, n_new = old_text.count("id:'d"), new_text.count("id:'d")
    ts = datetime.now(BJT).strftime('%Y-%m-%d %H:%M')
    push(pd_path, 'policy-data.js', '每日更新 %s 新增%d条（腾讯云）' % (ts, max(0, n_new - n_old)), token)
    push(rl_path, 'runlog.md', '运行留痕 %s（腾讯云）' % ts, token)
    print('[done] 新增 %d 条' % max(0, n_new - n_old))


if __name__ == '__main__':
    main()
