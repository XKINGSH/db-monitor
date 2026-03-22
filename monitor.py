#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DB Monitor — 数据库异常监控 & 企业微信推送  v6.1
=================================================
特殊列（全部写在 SELECT 中，无需配置文件额外配置）：
  _webhook        推送目标群机器人地址，逗号分隔多个
  _jump_url       点击消息卡片的跳转链接
  _interval       推送冷却时间（分钟），0=不限
  _title          自定义告警消息标题
  _time_window    推送时间段，格式 'HH:MM-HH:MM'，如 '09:00-18:00'
  _push_days      推送日期控制：all | workday | weekday | Mon,Tue,...
  _bitable_table  飞书多维表格 table_id，'skip' 表示不记录此 SQL
  _check_type     检查类型标签，自由文本，如 '数据一致性' '超时告警' 等

v6.0 新增：
  · 多数据库支持：每条 SQL 可在 query 块内配置独立 database，连接不同库
  · 同库 SQL 分组共用一个连接，整组完成后统一关闭，附带超时保护
  · 飞书多维表格新增字段：数据库、SQL内容、运行时长、检查类型

v5.0 新增：
  · 法定节假日数据自动更新（chinesecalendar 自动升级 + timor.tools 在线 API 兜底，本地缓存）
  · SQL 查询结果为空时直接跳过，不进行任何推送
"""

import sys
import os
import subprocess
import importlib
from pathlib import Path

# ═══════════════════════════════════════════════════════════
#  自动安装基础依赖
# ═══════════════════════════════════════════════════════════

_BASIC_DEPS = {
    'yaml':     'pyyaml',
    'requests': 'requests',
    'schedule': 'schedule',
    'tabulate': 'tabulate',
    'colorama': 'colorama',
}

def _auto_install(imp_name: str, pkg_name: str, silent: bool = False) -> bool:
    try:
        importlib.import_module(imp_name)
        return True
    except ImportError:
        if not silent:
            print(f'[安装] 正在安装 {pkg_name}，请稍候...')
        env = os.environ.copy()
        env.pop('PYTHONPATH', None)
        try:
            subprocess.check_call(
                [sys.executable, '-m', 'pip', 'install', pkg_name, '-q',
                 '--no-warn-script-location'],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=env,
            )
        except Exception:
            return False
        # 安装后二次验证：确认模块真正可导入（防止 pkg_name 与 imp_name 不一致）
        try:
            importlib.import_module(imp_name)
            return True
        except ImportError:
            if not silent:
                print(f'[警告] {pkg_name} 安装完成但无法导入 {imp_name}，请手动检查')
            return False

for _imp, _pkg in _BASIC_DEPS.items():
    _auto_install(_imp, _pkg)

# ═══════════════════════════════════════════════════════════
#  正式导入
# ═══════════════════════════════════════════════════════════

import json
import time
import logging
import traceback
import argparse
import sqlite3
import threading
import concurrent.futures
from collections import deque, OrderedDict
from datetime import datetime, timedelta, date
from typing import List, Dict, Any, Tuple, Optional, Set, Deque

import yaml
import requests
import schedule
from tabulate import tabulate
from colorama import init as _colorama_init, Fore, Style

_colorama_init(autoreset=True)

# ═══════════════════════════════════════════════════════════
#  日志
# ═══════════════════════════════════════════════════════════

def setup_logging(level: str = 'INFO', log_file: Optional[str] = None):
    fmt = '%(asctime)s [%(levelname)s] %(message)s'
    handlers: List[logging.Handler] = [logging.StreamHandler()]
    if log_file:
        handlers.append(logging.FileHandler(log_file, encoding='utf-8'))
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format=fmt, handlers=handlers,
    )

log = logging.getLogger('db_monitor')

# ═══════════════════════════════════════════════════════════
#  默认配置模板（首次运行生成）
# ═══════════════════════════════════════════════════════════

DEFAULT_CONFIG = r"""# ================================================================
#  DB Monitor v6.0 配置文件
# ================================================================


# ─────────────────────────────────────────────────────────────────
#  一、数据库连接
# ─────────────────────────────────────────────────────────────────
#
#  支持：mysql | oracle | postgresql | mssql | sqlite
#
#  ▶ MySQL 示例：
#    database:
#      type: mysql
#      host: 192.168.1.10
#      port: 3306
#      database: mydb
#      username: root
#      password: "MyPass@123"
#      charset: utf8mb4
#
#  ▶ Oracle（service_name 方式，推荐）：
#    database:
#      type: oracle
#      host: 192.168.1.20
#      port: 1521
#      service_name: ORCL        # 与 sid 二选一
#      username: system
#      password: "OraPass@456"
#
#  ▶ Oracle（SID 方式）：
#    database:
#      type: oracle
#      host: 192.168.1.20
#      port: 1521
#      sid: ORCLDB
#      username: system
#      password: "OraPass@456"
#
# ─────────────────────────────────────────────────────────────────

database:
  type: mysql
  host: 127.0.0.1
  port: 3306
  database: mydb
  username: root
  password: "your_password"
  charset: utf8mb4        # MySQL 专用，其他数据库删除此行


# ─────────────────────────────────────────────────────────────────
#  二、检测 SQL 列表（可添加任意多条，各自独立运行）
# ─────────────────────────────────────────────────────────────────
#
#  ╔══════════════════════════════════════════════════════════════╗
#  ║  SQL 特殊列速查表（写在 SELECT 中，程序自动识别并处理）      ║
#  ╠══════════════╦══════════════════════════════════════════════╣
#  ║ 列名         ║ 说明及写法                                   ║
#  ╠══════════════╬══════════════════════════════════════════════╣
#  ║ _webhook     ║ 推送目标 Webhook，逗号分隔多个               ║
#  ║              ║ MySQL:  'URL1,URL2'  AS _webhook             ║
#  ║              ║ Oracle: 'URL1,URL2'  AS "_webhook"           ║
#  ╠══════════════╬══════════════════════════════════════════════╣
#  ║ _jump_url    ║ 点击消息卡片时跳转的链接                     ║
#  ║              ║ MySQL:  'https://...' AS _jump_url           ║
#  ║              ║ Oracle: 'https://...' AS "_jump_url"         ║
#  ╠══════════════╬══════════════════════════════════════════════╣
#  ║ _interval    ║ 推送冷却（分钟），0=每次都推                 ║
#  ║              ║ MySQL:  30  AS _interval                     ║
#  ║              ║ Oracle: 30  AS "_interval"                   ║
#  ╠══════════════╬══════════════════════════════════════════════╣
#  ║ _title       ║ 自定义告警消息标题（不填则自动生成）         ║
#  ║              ║ MySQL:  '超时订单告警' AS _title             ║
#  ║              ║ Oracle: '超时订单告警' AS "_title"           ║
#  ╠══════════════╬══════════════════════════════════════════════╣
#  ║ _time_window ║ 推送时段，格式 'HH:MM-HH:MM'                ║
#  ║              ║ '09:00-18:00' = 仅9~18点推送                ║
#  ║              ║ '22:00-06:00' = 跨午夜时段                  ║
#  ║              ║ 'all' 或不填 = 不限时段                     ║
#  ║              ║ MySQL:  '09:00-18:00' AS _time_window        ║
#  ║              ║ Oracle: '09:00-18:00' AS "_time_window"      ║
#  ╠══════════════╬══════════════════════════════════════════════╣
#  ║ _push_days   ║ 推送日期控制                                 ║
#  ║              ║ 'all'     = 每天（默认）                     ║
#  ║              ║ 'workday' = 工作日（排除法定节假日）         ║
#  ║              ║ 'weekday' = 周一~周五（不查节假日）          ║
#  ║              ║ 'Mon,Tue,Wed,Thu,Fri' = 指定星期几           ║
#  ║              ║ MySQL:  'workday' AS _push_days              ║
#  ║              ║ Oracle: 'workday' AS "_push_days"            ║
#  ╠══════════════╬══════════════════════════════════════════════╣
#  ║ _bitable_    ║ 飞书多维表格 table_id（覆盖全局默认）        ║
#  ║ table        ║ 'skip' = 此 SQL 不写入飞书                   ║
#  ║              ║ MySQL:  'tblXXXXX' AS _bitable_table         ║
#  ║              ║ Oracle: 'tblXXXXX' AS "_bitable_table"       ║
#  ╚══════════════╩══════════════════════════════════════════════╝
#
#  注意：以上特殊列会自动从消息内容中移除，不影响展示。
#
#  ─────────────── MySQL SQL 完整示例 ───────────────────────────
#
#  queries:
#    - name: "超时未支付订单"
#      description: "下单超过24小时未支付"
#      sql: |
#        SELECT
#          order_id, user_id, amount, created_at,
#          'https://qyapi.weixin.qq.com/...?key=KEY_A'  AS _webhook,
#          'https://your-system.com/orders'             AS _jump_url,
#          30                                           AS _interval,
#          '超时订单告警'                               AS _title,
#          '09:00-18:00'                                AS _time_window,
#          'workday'                                    AS _push_days
#        FROM orders
#        WHERE status = 'pending'
#          AND created_at < NOW() - INTERVAL 1 DAY
#        LIMIT 50
#      threshold: 0
#
#  ─────────────── 独立数据库（per-query database）─────────
#
#  每条 SQL 可单独配置 database，完整覆盖全局配置，连接不同的数据库。
#  同一 database 配置的 SQL 会自动分组，共用一个连接，整组完成后统一关闭。
#
#  queries:
#    - name: "跨库检查示例"
#      description: "连接另一台 MySQL 服务器做检测"
#      database:               # ← 覆盖全局 database，此 SQL 独立连接
#        type: mysql
#        host: 192.168.2.20
#        port: 3306
#        database: finance_db
#        username: report
#        password: "FinPass@789"
#      sql: |
#        SELECT order_no, amount, status, '财务异常告警' AS _check_type
#        FROM finance_orders WHERE status = 'ERROR' LIMIT 50
#      threshold: 0
#
#  ─────────────── Oracle SQL 完整示例 ──────────────────────────
#
#  queries:
#    - name: "应收账款超期"
#      description: "账期超过90天"
#      sql: |
#        SELECT
#          invoice_no, customer_name, amount, due_date,
#          'https://qyapi.weixin.qq.com/...?key=KEY_X'  AS "_webhook",
#          'https://erp.your-company.com/ar'            AS "_jump_url",
#          60                                           AS "_interval",
#          '应收超期告警'                               AS "_title",
#          '08:30-17:30'                                AS "_time_window",
#          'workday'                                    AS "_push_days"
#        FROM ar_invoices
#        WHERE due_date < SYSDATE - 90
#          AND status = 'OPEN'
#          AND ROWNUM <= 50
#      threshold: 0
#
#  ─────────────── 多群推送（逗号分隔 webhook）─────────────────
#
#      CONCAT(
#        'https://qyapi.weixin.qq.com/...?key=KEY_A,',
#        'https://qyapi.weixin.qq.com/...?key=KEY_B'
#      ) AS _webhook
#
# ─────────────────────────────────────────────────────────────────

queries:

  - name: "超时未支付订单"
    description: "下单超过 24 小时仍未支付的订单"
    sql: |
      SELECT
        order_id, user_id, amount, created_at,
        'https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=YOUR_WEBHOOK_KEY' AS _webhook,
        'https://www.example.com/orders'  AS _jump_url,
        30                               AS _interval,
        '超时订单告警'                   AS _title,
        '09:00-18:00'                    AS _time_window,
        'workday'                        AS _push_days
      FROM orders
      WHERE status = 'pending'
        AND created_at < NOW() - INTERVAL 1 DAY
      LIMIT 50
    threshold: 0

  - name: "库存预警"
    description: "库存低于安全库存线"
    sql: |
      SELECT
        sku_code, stock, safety_stock,
        'https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=YOUR_WEBHOOK_KEY' AS _webhook,
        'https://www.example.com/inventory' AS _jump_url,
        60                                  AS _interval,
        '库存预警'                          AS _title,
        'all'                               AS _time_window,
        'all'                               AS _push_days
      FROM products
      WHERE stock < safety_stock
      LIMIT 50
    threshold: 0


# ─────────────────────────────────────────────────────────────────
#  三、企业微信全局默认（SQL 中未指定时的兜底配置）
# ─────────────────────────────────────────────────────────────────

wecom:
  webhook:  "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=YOUR_WEBHOOK_KEY"
  jump_url: "https://www.example.com/dashboard"
  pic_url:  ""
  default_push_interval_minutes: 0    # 0 = 不限制，有异常就推


# ─────────────────────────────────────────────────────────────────
#  四、飞书多维表格（告警日志记录）
#
#  【开通步骤】
#  1. 打开飞书开放平台 https://open.feishu.cn/app
#     → 找到你的应用 → 「权限管理」→ 搜索并开通：
#       · bitable:app（多维表格数据读写）
#     → 发布应用版本使权限生效
#
#  2. 在飞书中创建一个多维表格（多维表格 App）
#     → 打开多维表格 → 右上角「...」→「添加应用」→ 搜索你的应用并添加
#       （或在表格设置里把应用设为管理员/编辑权限）
#
#  3. 在多维表格中新建一张数据表，添加以下字段：
#     字段名        类型
#     ──────────    ──────
#     告警时间      日期时间（必须，精确到秒）
#     SQL名称       文本
#     告警标题      文本
#     异常条数      数字
#     推送状态      单选（选项：成功 / 部分成功 / 冷却 / 时间限制 / 日期限制 / 无Webhook）
#     数据库        文本（v6.0新增）连接的数据库标识，如 mysql://host/db
#     SQL内容       多行文本（v6.0新增）实际执行的 SQL 语句
#     运行时长       数字（v6.0新增）SQL 执行耗时，单位秒
#     检查类型       单选（v6.0新增）来自 _check_type 特殊列，如 数据一致性
#     跳转链接      网页
#     推送群数      数字
#     数据预览      多行文本
#
#  4. 获取 app_token 和 table_id：
#     · app_token：打开多维表格，URL 中 /base/ 后面的字符串
#       示例：https://your-company.feishu.cn/base/FabcXXXXXXX
#       app_token = FabcXXXXXXX
#     · table_id：在飞书网页端，点击数据表标签页右键 → 「复制链接」
#       URL 参数中 table= 后面的值，或通过下方命令查询
#       python monitor.py --list-tables
#
# ─────────────────────────────────────────────────────────────────

feishu:
  app_id:     "YOUR_APP_ID"          # 飞书应用 App ID
  app_secret: "YOUR_APP_SECRET"      # 飞书应用 App Secret
  bitable:
    enabled:   false                  # true = 开启飞书日志记录
    app_token: "YOUR_APP_TOKEN"       # 多维表格 App Token
    table_id:  "YOUR_TABLE_ID"        # 数据表 ID


# ─────────────────────────────────────────────────────────────────
#  五、检测周期
# ─────────────────────────────────────────────────────────────────

schedule:
  enabled: false
  check_interval_seconds: 60         # 主循环每隔多少秒检测一次（建议 30~300）

query_timeout: 60                    # 单条 SQL 最长执行时间（秒），0 = 不限制
                                     # 超时后该条 SQL 跳过，不影响同组其他 SQL


# ─────────────────────────────────────────────────────────────────
#  六、日志
# ─────────────────────────────────────────────────────────────────

log:
  level: INFO
  file:  monitor.log
"""

# ═══════════════════════════════════════════════════════════
#  路径
# ═══════════════════════════════════════════════════════════

SCRIPT_DIR           = Path(__file__).parent.resolve()
CONFIG_PATH          = SCRIPT_DIR / 'config.yaml'
STATE_FILE           = SCRIPT_DIR / 'monitor_state.json'
HOLIDAYS_CACHE_FILE  = SCRIPT_DIR / 'holiday_cache.json'

def load_config() -> dict:
    if not CONFIG_PATH.exists():
        CONFIG_PATH.write_text(DEFAULT_CONFIG, encoding='utf-8')
        print(Fore.YELLOW + f'\n[提示] 已生成配置文件: {CONFIG_PATH}')
        print(Fore.YELLOW +  '       请编辑后重新运行。\n')
        input('按 Enter 键退出...')
        sys.exit(0)
    with open(CONFIG_PATH, encoding='utf-8') as f:
        cfg = yaml.safe_load(f)
    if not cfg:
        print(Fore.RED + '[错误] config.yaml 为空或格式有误。')
        sys.exit(1)
    return cfg

# ═══════════════════════════════════════════════════════════
#  推送状态持久化
# ═══════════════════════════════════════════════════════════

def load_state() -> Dict[str, str]:
    if STATE_FILE.exists():
        try:
            with open(STATE_FILE, encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            pass
    return {}

def save_state(state: Dict[str, str]):
    with open(STATE_FILE, 'w', encoding='utf-8') as f:
        json.dump(state, f, indent=2, ensure_ascii=False)

def should_push(name: str, interval_min: int,
                state: Dict[str, str], force: bool = False) -> Tuple[bool, str]:
    if force or interval_min <= 0:
        return True, ''
    last_str = state.get(name)
    if not last_str:
        return True, ''
    try:
        elapsed  = datetime.now() - datetime.fromisoformat(last_str)
        required = timedelta(minutes=interval_min)
        if elapsed >= required:
            return True, ''
        rem = required - elapsed
        m, s = divmod(int(rem.total_seconds()), 60)
        return False, f'距下次推送还有 {m}分{s:02d}秒（冷却={interval_min}min）'
    except Exception:
        return True, ''

def mark_pushed(name: str, state: Dict[str, str]):
    state[name] = datetime.now().isoformat(timespec='seconds')

# ═══════════════════════════════════════════════════════════
#  企业微信频率限制器
#  企业微信群机器人限制：每个 webhook 每分钟最多 20 条
# ═══════════════════════════════════════════════════════════

class WecomRateLimiter:
    """
    滑动窗口限速器，每个 webhook 独立计数。
    策略：
      · 每分钟最多发 _MAX_PER_MIN 条（官方上限 20，留余量设 18）
      · 同一 webhook 连续两次发送至少间隔 _MIN_GAP 秒
      · 超过限制时自动 sleep 等待，不丢消息
    """
    _MAX_PER_MIN = 18
    _MIN_GAP     = 3.5   # 秒

    def __init__(self):
        self._lock  = threading.Lock()
        self._times: Dict[str, Deque[float]] = {}

    def wait(self, webhook: str):
        with self._lock:
            now = time.time()
            dq  = self._times.setdefault(webhook, deque())

            # 清理 1 分钟以前的记录
            while dq and now - dq[0] > 60:
                dq.popleft()

            # 达到每分钟上限 → 等到最早一条超过 1 分钟
            if len(dq) >= self._MAX_PER_MIN:
                wait_sec = 61.0 - (now - dq[0])
                if wait_sec > 0:
                    log.info(f'[限速] {webhook[-20:]} 达到 {self._MAX_PER_MIN}/min 上限，等待 {wait_sec:.1f}s')
                    time.sleep(wait_sec)
                    now = time.time()
                    while dq and now - dq[0] > 60:
                        dq.popleft()

            # 最小间隔
            if dq:
                gap = now - dq[-1]
                if gap < self._MIN_GAP:
                    time.sleep(self._MIN_GAP - gap)
                    now = time.time()

            dq.append(time.time())

_rate_limiter = WecomRateLimiter()

# ═══════════════════════════════════════════════════════════
#  节假日管理（自动更新）
# ═══════════════════════════════════════════════════════════

class HolidayManager:
    """
    中国法定节假日管理器（含调休补班）

    数据源优先级：
      1. chinesecalendar 本地包
         · 若年份超出支持范围，自动 pip upgrade 后重试
      2. timor.tools 在线 API
         · 数据缓存到 holiday_cache.json，有效期 30 天
         · 跨年自动拉取新年数据
      3. 降级兜底：周末（weekday >= 5）判断

    调用方式：
      _is_cn_holiday(date.today())
    """

    _CACHE_DAYS = 30
    _ONLINE_URL = 'https://timor.tools/api/holiday/year/{year}'

    def __init__(self, cache_file: Path):
        self._f         = cache_file
        self._cache: Dict[str, Any] = {}   # year_str -> {fetched_at, days: {MM-DD: bool}}
        self._pkg_ok: Optional[bool] = None
        self._load()

    # ── 缓存 IO ──────────────────────────────────────────

    def _load(self):
        if self._f.exists():
            try:
                with open(self._f, encoding='utf-8') as f:
                    self._cache = json.load(f)
            except Exception:
                self._cache = {}

    def _save(self):
        try:
            with open(self._f, 'w', encoding='utf-8') as f:
                json.dump(self._cache, f, ensure_ascii=False, indent=2)
        except Exception:
            pass

    # ── 数据源 1：chinesecalendar 包 ─────────────────────

    def _try_pkg(self, d: date) -> Optional[bool]:
        if self._pkg_ok is None:
            self._pkg_ok = _auto_install('chinese_calendar', 'chinesecalendar', silent=True)
        if not self._pkg_ok:
            return None

        for attempt in range(2):
            try:
                if attempt > 0 and 'chinese_calendar' in sys.modules:
                    importlib.reload(sys.modules['chinese_calendar'])
                from chinese_calendar import is_holiday
                return is_holiday(d)
            except Exception as e:
                msg = str(e).lower()
                if attempt == 0 and ('range' in msg or str(d.year) in str(e)):
                    # 年份超出包的支持范围 → 尝试升级
                    log.info(f'[节假日] chinesecalendar 不支持 {d.year} 年，自动升级中...')
                    env = os.environ.copy()
                    env.pop('PYTHONPATH', None)
                    try:
                        subprocess.check_call(
                            [sys.executable, '-m', 'pip', 'install',
                             '--upgrade', 'chinesecalendar', '-q',
                             '--no-warn-script-location'],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=env,
                        )
                        log.info('[节假日] chinesecalendar 升级完成，重新校验...')
                    except Exception:
                        self._pkg_ok = False
                        return None
                else:
                    self._pkg_ok = False
                    return None
        return None

    # ── 数据源 2：timor.tools 在线 API + 本地缓存 ────────

    def _try_online(self, d: date) -> Optional[bool]:
        year_str = str(d.year)
        date_key = d.strftime('%m-%d')
        entry    = self._cache.get(year_str, {})

        # 缓存命中且未过期
        if entry.get('days') is not None:
            try:
                fetched = datetime.fromisoformat(entry['fetched_at'])
                if (datetime.now() - fetched).days < self._CACHE_DAYS:
                    days = entry['days']
                    # True/False 已明确；None 代表非假日（未在 API 返回列表中）
                    return days.get(date_key, False)
            except Exception:
                pass

        # 从在线 API 拉取
        url = self._ONLINE_URL.format(year=d.year)
        try:
            r    = requests.get(url, timeout=8)
            data = r.json()
            if data.get('code') == 0 and 'holiday' in data:
                days: Dict[str, bool] = {}
                for k, v in data['holiday'].items():
                    days[k] = bool(v.get('holiday', False))
                self._cache[year_str] = {
                    'fetched_at': datetime.now().isoformat(),
                    'source':     'timor.tools',
                    'days':       days,
                }
                self._save()
                log.info(f'[节假日] {d.year} 年节假日数据已从在线 API 更新并缓存至本地')
                return days.get(date_key, False)
        except Exception as e:
            log.debug(f'[节假日] 在线 API 请求失败: {e}')
        return None

    # ── 公开接口 ─────────────────────────────────────────

    def is_holiday(self, d: date) -> bool:
        """
        判断 d 是否为中国法定节假日。
        · True  = 节假日（含调休后的假日）
        · False = 工作日（含调休补班的周末）
        数据源全部失效时降级为纯周末判断。
        """
        v = self._try_pkg(d)
        if v is not None:
            return v
        v = self._try_online(d)
        if v is not None:
            return v
        log.debug(f'[节假日] 所有数据源均失效，降级为周末判断（{d}）')
        return d.weekday() >= 5

    def refresh(self, year: int):
        """主动清除缓存并重新从在线 API 拉取指定年份数据"""
        self._cache.pop(str(year), None)
        self._try_online(date(year, 1, 1))


_holiday_mgr: Optional[HolidayManager] = None

def _get_holiday_mgr() -> HolidayManager:
    global _holiday_mgr
    if _holiday_mgr is None:
        _holiday_mgr = HolidayManager(HOLIDAYS_CACHE_FILE)
    return _holiday_mgr

def _is_cn_holiday(d: date) -> bool:
    """判断是否为中国法定节假日（含调休），支持自动更新数据"""
    return _get_holiday_mgr().is_holiday(d)


def check_time_window(tw: str) -> Tuple[bool, str]:
    """
    tw 格式：'HH:MM-HH:MM'，支持跨午夜（如 '22:00-06:00'）
    返回 (allowed, reason)
    """
    if not tw or tw.lower() == 'all':
        return True, ''
    try:
        parts   = tw.strip().split('-', 1)
        start_t = datetime.strptime(parts[0].strip(), '%H:%M').time()
        end_t   = datetime.strptime(parts[1].strip(), '%H:%M').time()
        now_t   = datetime.now().time()
        in_window = (
            (start_t <= now_t <= end_t)
            if start_t <= end_t
            else (now_t >= start_t or now_t <= end_t)   # 跨午夜
        )
        if not in_window:
            return False, f'当前 {now_t.strftime("%H:%M")} 不在推送时段 {tw}'
        return True, ''
    except Exception as e:
        log.warning(f'_time_window 格式解析失败: {tw!r} — {e}')
        return True, ''


_DAY_MAP = {'mon': 0, 'tue': 1, 'wed': 2, 'thu': 3,
            'fri': 4, 'sat': 5, 'sun': 6}
_DAY_CN  = ['周一', '周二', '周三', '周四', '周五', '周六', '周日']

def check_push_days(pd_str: str) -> Tuple[bool, str]:
    """
    pd_str 支持：
      all / workday / weekday / Mon,Tue,Wed,Thu,Fri
    """
    if not pd_str or pd_str.lower() == 'all':
        return True, ''

    now     = datetime.now()
    wd      = now.weekday()   # 0=Mon … 6=Sun
    day_cn  = _DAY_CN[wd]
    pd      = pd_str.lower().strip()

    if pd == 'workday':
        if wd >= 5:
            return False, f'今日({day_cn})为周末，不推送'
        if _is_cn_holiday(now.date()):
            return False, f'今日({now.strftime("%m-%d")})为法定节假日，不推送'
        return True, ''

    if pd == 'weekday':
        if wd >= 5:
            return False, f'今日({day_cn})为周末，不推送'
        return True, ''

    # 解析 'Mon,Tue,...'
    allowed: Set[int] = set()
    for tok in pd_str.split(','):
        key = tok.strip().lower()[:3]
        if key in _DAY_MAP:
            allowed.add(_DAY_MAP[key])
    if allowed and wd not in allowed:
        return False, f'今日({day_cn})不在推送日设定({pd_str})'
    return True, ''

# ═══════════════════════════════════════════════════════════
#  数据库连接
# ═══════════════════════════════════════════════════════════

def get_connection(db_cfg: dict):
    db_type = db_cfg.get('type', 'mysql').lower()

    if db_type == 'sqlite':
        conn = sqlite3.connect(str(db_cfg.get('sqlite_path', ':memory:')))
        conn.row_factory = sqlite3.Row
        return conn

    host, port = db_cfg.get('host', '127.0.0.1'), db_cfg.get('port')
    db   = db_cfg.get('database', '')
    user = db_cfg.get('username', '')
    pwd  = str(db_cfg.get('password', ''))

    if db_type == 'mysql':
        _auto_install('pymysql', 'pymysql')
        import pymysql
        return pymysql.connect(
            host=host, port=int(port or 3306),
            database=db, user=user, password=pwd,
            charset=db_cfg.get('charset', 'utf8mb4'),
            cursorclass=pymysql.cursors.DictCursor,
            connect_timeout=15, autocommit=True,
        )

    if db_type == 'oracle':
        _auto_install('oracledb', 'oracledb')
        import oracledb
        svc = db_cfg.get('service_name')
        sid = db_cfg.get('sid')
        op  = int(port or 1521)
        if svc:
            dsn = f'{host}:{op}/{svc}'
        elif sid:
            dsn = oracledb.makedsn(host, op, sid=sid)
        else:
            raise ValueError('Oracle 需配置 service_name 或 sid')
        return oracledb.connect(user=user, password=pwd, dsn=dsn)

    if db_type in ('postgresql', 'postgres'):
        _auto_install('psycopg2', 'psycopg2-binary')
        import psycopg2
        conn = psycopg2.connect(
            host=host, port=int(port or 5432),
            dbname=db, user=user, password=pwd, connect_timeout=15,
        )
        conn.autocommit = True
        return conn

    if db_type == 'mssql':
        _auto_install('pyodbc', 'pyodbc')
        import pyodbc
        cs = (f'DRIVER={{ODBC Driver 17 for SQL Server}};'
              f'SERVER={host},{int(port or 1433)};'
              f'DATABASE={db};UID={user};PWD={pwd}')
        return pyodbc.connect(cs, timeout=15)

    raise ValueError(f'不支持的数据库类型: {db_type}')

# ═══════════════════════════════════════════════════════════
#  执行 SQL
# ═══════════════════════════════════════════════════════════

def execute_query(conn, sql: str, db_type: str) -> Tuple[List[str], List[List[Any]]]:
    if db_type == 'sqlite':
        cur  = conn.execute(sql)
        cols = [d[0] for d in cur.description] if cur.description else []
        return cols, [list(r) for r in cur.fetchall()]

    with conn.cursor() as cur:
        cur.execute(sql)
        if not cur.description:
            return [], []
        cols = [d[0] for d in cur.description]
        if db_type == 'mysql':
            raw = cur.fetchall()
            if not raw:
                return cols, []
            rows = ([list(r.values()) for r in raw]
                    if isinstance(raw[0], dict)
                    else [list(r) for r in raw])
        else:
            rows = [list(r) for r in cur.fetchall()]
        return cols, rows

# ═══════════════════════════════════════════════════════════
#  提取特殊列
# ═══════════════════════════════════════════════════════════

# 列名映射（全部小写，去掉引号和空格后匹配）
_COL_MAP = {
    '_webhook':       '_webhook',
    '_webhook_url':   '_webhook',
    '_webhooks':      '_webhook',
    '_jump_url':      '_jump_url',
    '_url':           '_jump_url',
    '_link':          '_jump_url',
    '_interval':      '_interval',
    '_interval_minutes': '_interval',
    '_push_interval': '_interval',
    '_title':         '_title',
    '_alert_title':   '_title',
    '_time_window':   '_time_window',
    '_push_window':   '_time_window',
    '_push_days':     '_push_days',
    '_days':          '_push_days',
    '_bitable_table': '_bitable_table',
    '_table_id':      '_bitable_table',
    # v6.0: 检查类型标签（自由文本，写入飞书 检查类型 字段）
    '_check_type':    '_check_type',
    '_type':          '_check_type',
    '_alert_type':    '_check_type',
}

def _norm_col(name: str) -> str:
    return name.lower().strip('"').strip("'").strip()

def extract_meta(
    cols: List[str], rows: List[List[Any]]
) -> Tuple[Dict[str, Any], List[str], List[List[Any]]]:
    """
    从结果集中提取所有特殊列。
    返回:
        meta         : {logical_name: value_from_first_row}
        display_cols : 去掉特殊列后的列名列表
        display_rows : 去掉特殊列后的数据行
    """
    meta_idx: Dict[int, str] = {}   # col_index -> logical_name

    for i, c in enumerate(cols):
        logical = _COL_MAP.get(_norm_col(c))
        if logical:
            meta_idx[i] = logical

    meta: Dict[str, Any] = {}
    if rows:
        first = rows[0]
        for idx, key in meta_idx.items():
            val = first[idx]
            if val is not None and str(val).strip():
                meta[key] = str(val).strip()

    remove = set(meta_idx.keys())
    display_cols = [c for i, c in enumerate(cols) if i not in remove]
    display_rows = [
        [cell for j, cell in enumerate(row) if j not in remove]
        for row in rows
    ]
    return meta, display_cols, display_rows


def _to_int(v, default: int) -> int:
    """将値转为非负整数；转换失败返回 default。"""
    try:
        return max(0, int(v))
    except Exception:
        return default


def resolve(meta: Dict[str, Any], query_cfg: dict,
            wecom_cfg: dict) -> Dict[str, Any]:
    """将特殊列值、query级配置、全局配置合并，返回最终参数字典"""

    # ── webhook: SQL > query.webhooks/webhook > 全局（三者叠加）──
    wh_list: List[str] = []
    if meta.get('_webhook'):
        wh_list += [w.strip() for w in meta['_webhook'].split(',') if w.strip()]
    for u in query_cfg.get('webhooks', []):
        if u: wh_list.append(str(u).strip())
    if query_cfg.get('webhook'):
        wh_list.append(str(query_cfg['webhook']).strip())
    if not wh_list and wecom_cfg.get('webhook'):
        wh_list.append(wecom_cfg['webhook'])
    seen: Set[str] = set()
    webhooks = [u for u in wh_list if u and not (u in seen or seen.add(u))]

    # ── jump_url: SQL > query > 全局 ──
    jump_url = (meta.get('_jump_url')
                or query_cfg.get('jump_url')
                or wecom_cfg.get('jump_url', 'https://example.com'))

    # ── interval: SQL > query > 全局默认 ──
    interval = _to_int(
        meta.get('_interval'),
        _to_int(query_cfg.get('interval_minutes'),
                _to_int(wecom_cfg.get('default_push_interval_minutes'), 0))
    )

    # ── title: SQL > 自动生成（在调用方填充）──
    title = meta.get('_title', '')

    # ── time_window / push_days ──
    time_window = meta.get('_time_window', query_cfg.get('time_window', 'all'))
    push_days   = meta.get('_push_days',   query_cfg.get('push_days',   'all'))

    # ── bitable_table: SQL > query > 全局 ──
    bitable_table = (meta.get('_bitable_table')
                     or query_cfg.get('bitable_table')
                     or '')

    # ── check_type: SQL > query ──
    # 检查类型标签，自由文本，写入飞书 检查类型 字段，用于按类型筛选告警记录。
    # 在 SQL 中用  '数据一致性' AS _check_type  声明；或在 query 块写 check_type: xxx
    check_type = (meta.get('_check_type')
                  or query_cfg.get('check_type', ''))

    return dict(
        webhooks=webhooks, jump_url=jump_url, interval=interval,
        title=title, time_window=time_window, push_days=push_days,
        bitable_table=bitable_table, check_type=check_type,
        pic_url=wecom_cfg.get('pic_url', ''),
    )

# ═══════════════════════════════════════════════════════════
#  飞书多维表格（告警日志）
# ═══════════════════════════════════════════════════════════

_fs_token_cache: Dict[str, Any] = {'token': None, 'exp': 0}

def _get_feishu_token(app_id: str, app_secret: str) -> Optional[str]:
    now = time.time()
    if _fs_token_cache['token'] and now < _fs_token_cache['exp']:
        return _fs_token_cache['token']
    try:
        r = requests.post(
            'https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal',
            json={'app_id': app_id, 'app_secret': app_secret}, timeout=10,
        )
        d = r.json()
        if d.get('code') == 0:
            _fs_token_cache['token'] = d['tenant_access_token']
            _fs_token_cache['exp']   = now + d.get('expire', 7200) - 120
            return _fs_token_cache['token']
        log.error(f'飞书 token 获取失败: {d}')
    except Exception as e:
        log.error(f'飞书 token 请求异常: {e}')
    return None


def log_to_bitable(feishu_cfg: dict, record: dict,
                   override_table_id: str = '') -> bool:
    """
    将告警记录写入飞书多维表格。
    override_table_id: 若非空且非 'skip'，覆盖全局 table_id。
    """
    if override_table_id and override_table_id.lower() == 'skip':
        return True   # 此 SQL 主动跳过记录

    bt = feishu_cfg.get('bitable', {})
    if not bt.get('enabled', False):
        return True

    app_id     = feishu_cfg.get('app_id', '')
    app_secret = feishu_cfg.get('app_secret', '')
    app_token  = bt.get('app_token', '')
    table_id   = override_table_id or bt.get('table_id', '')

    if not all([app_id, app_secret, app_token, table_id]):
        log.debug('飞书多维表格配置不完整，跳过记录')
        return False

    if any(v.startswith('YOUR_') for v in [app_id, app_secret, app_token, table_id]):
        log.debug('飞书配置尚未填写，跳过记录')
        return False

    token = _get_feishu_token(app_id, app_secret)
    if not token:
        return False

    # 构造字段（字段名须与飞书表格列名完全一致）
    fields: Dict[str, Any] = {
        '告警时间':  int(datetime.now().timestamp() * 1000),  # 毫秒时间戳
        'SQL名称':   record.get('name', ''),
        '告警标题':  record.get('title', ''),
        '异常条数':  record.get('count', 0),
        '推送状态':  record.get('push_status', ''),
        '跳转链接':  [{'text': '查看详情', 'link': record.get('jump_url', '')}],
        '推送群数':  record.get('webhook_count', 0),
        '数据预览':  record.get('preview_text', ''),
        # v6.0 新增字段（表格中无对应列时飞书会忽略，不报错）
        '数据库':    record.get('db_label', ''),
        'SQL内容':   record.get('sql_text', ''),
        '运行时长':  record.get('elapsed', 0),
        '检查类型':  record.get('check_type', ''),
    }

    try:
        r = requests.post(
            f'https://open.feishu.cn/open-apis/bitable/v1'
            f'/apps/{app_token}/tables/{table_id}/records',
            headers={'Authorization': f'Bearer {token}',
                     'Content-Type': 'application/json'},
            json={'fields': fields}, timeout=10,
        )
        result = r.json()
        if result.get('code') == 0:
            return True
        log.error(f'飞书多维表格写入失败: {result}')
        return False
    except Exception as e:
        log.error(f'飞书多维表格请求异常: {e}')
        return False


def list_bitable_tables(feishu_cfg: dict):
    """辅助命令：列出多维表格中的数据表（用于获取 table_id）"""
    bt = feishu_cfg.get('bitable', {})
    app_id     = feishu_cfg.get('app_id', '')
    app_secret = feishu_cfg.get('app_secret', '')
    app_token  = bt.get('app_token', '')

    if not all([app_id, app_secret, app_token]):
        print(Fore.RED + '[错误] 请先在 config.yaml 填写 feishu.app_id / app_secret / bitable.app_token')
        return

    token = _get_feishu_token(app_id, app_secret)
    if not token:
        print(Fore.RED + '[错误] 获取飞书 token 失败，请检查 app_id / app_secret')
        return

    try:
        r = requests.get(
            f'https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables',
            headers={'Authorization': f'Bearer {token}'}, timeout=10,
        )
        d = r.json()
    except Exception as e:
        print(Fore.RED + f'[错误] 查询飞书表格列表失败: {e}')
        return
    if d.get('code') != 0:
        print(Fore.RED + f'[错误] 查询失败: {d}')
        return

    items = d.get('data', {}).get('items', [])
    if not items:
        print(Fore.YELLOW + '该多维表格中没有数据表，请先在飞书中创建。')
        return

    print(Fore.CYAN + '\n多维表格数据表列表：')
    rows = [[t.get('name', ''), t.get('table_id', '')] for t in items]
    print(tabulate(rows, headers=['表名', 'table_id'], tablefmt='rounded_outline'))
    print(Fore.WHITE + '\n将对应的 table_id 填入 config.yaml → feishu.bitable.table_id\n')

# ═══════════════════════════════════════════════════════════
#  企业微信推送
# ═══════════════════════════════════════════════════════════

def _wecom_post(webhook: str, payload: dict) -> bool:
    _rate_limiter.wait(webhook)   # 限速等待
    try:
        resp = requests.post(webhook, json=payload, timeout=10)
        resp.raise_for_status()
        result = resp.json()
        if result.get('errcode') == 0:
            return True
        log.error(f'企业微信返回错误: {result}')
        return False
    except Exception as e:
        log.error(f'企业微信推送失败: {e}')
        return False

def send_news_card(webhook: str, title: str, description: str,
                   url: str, pic_url: str = '') -> bool:
    return _wecom_post(webhook, {
        'msgtype': 'news',
        'news': {'articles': [{
            'title':       title,
            'description': description,
            'url':         url,
            'picurl':      pic_url or '',
        }]},
    })

def send_text(webhook: str, content: str) -> bool:
    return _wecom_post(webhook, {'msgtype': 'text', 'text': {'content': content}})

# ═══════════════════════════════════════════════════════════

# ═══════════════════════════════════════════════════════
#  多数据库支持辅助函数（v6.0）
# ═══════════════════════════════════════════════════════

def _get_db_cfg(query_cfg: dict, global_db_cfg: dict) -> dict:
    """
    获取单条 SQL 实际使用的数据库配置。
    优先级：query 级别的 database 块 > 全局 database 块。
    若 query.database 定义了 type 字段，则完整替换全局配置（不做字段级合并）。
    这样不同 SQL 可以连接完全不同的数据库服务器。
    """
    q_db = query_cfg.get('database')
    if q_db and isinstance(q_db, dict) and q_db.get('type'):
        return q_db
    return global_db_cfg


def _db_key(db_cfg: dict) -> str:
    """
    生成数据库连接的唯一标识字符串，用于将同库 SQL 分组。
    包含所有影响连接目标的关键字段；密码不参与 key（安全考虑）。
    示例：mysql|192.168.1.10|3306|mydb|root||
    """
    t = db_cfg.get('type', 'mysql').lower()
    if t == 'sqlite':
        return f'sqlite|{db_cfg.get("sqlite_path", ":memory:")}'
    return '|'.join([
        t,
        str(db_cfg.get('host', '')),
        str(db_cfg.get('port', '')),
        str(db_cfg.get('database', '')),
        str(db_cfg.get('username', '')),
        str(db_cfg.get('service_name', '')),
        str(db_cfg.get('sid', '')),
    ])


def _db_label(db_cfg: dict) -> str:
    """
    生成便于阅读的数据库标签，用于日志显示和飞书 数据库 字段。
    示例：mysql://192.168.1.10:3306/mydb
    """
    t = db_cfg.get('type', 'mysql').lower()
    if t == 'sqlite':
        return f'sqlite://{db_cfg.get("sqlite_path", ":memory:")}'
    host     = db_cfg.get('host', '')
    port     = db_cfg.get('port', '')
    db_name  = (db_cfg.get('database')
                or db_cfg.get('service_name')
                or db_cfg.get('sid', ''))
    port_str = f':{port}' if port else ''
    return f'{t}://{host}{port_str}/{db_name}'


def _execute_with_timeout(
    conn, sql: str, db_type: str, timeout_secs: int
) -> 'Tuple[List[str], List[List[Any]]]':
    """
    带超时保护的 SQL 执行。
    使用独立线程运行查询；超过 timeout_secs 秒后抛出 TimeoutError。
    · timeout_secs <= 0 时不限制，直接执行。
    · 超时后使用 shutdown(wait=False) 放弃等待后台线程，防止程序挂起。
      实际连接关闭由调用方 finally 块负责，会中断后台线程的 IO 等待。
    注意：不使用 `with executor` 语法，原因是 __exit__ 默认 shutdown(wait=True)，
          若 SQL 线程卡死在 DB IO，wait=True 会使整个监控进程永久挂起。
    """
    if timeout_secs <= 0:
        return execute_query(conn, sql, db_type)
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    try:
        future = executor.submit(execute_query, conn, sql, db_type)
        return future.result(timeout=timeout_secs)
    except concurrent.futures.TimeoutError:
        raise TimeoutError(f'SQL 执行超时（超过 {timeout_secs}s）')
    finally:
        # wait=False：不阻塞等待超时线程结束，连接关闭由调用方负责中断线程 IO
        executor.shutdown(wait=False)

#  核心监控逻辑
# ═══════════════════════════════════════════════════════════

def run_monitor(config: dict, state: Dict[str, str], force: bool = False) -> bool:
    global_db_cfg = config.get('database', {})
    queries       = config.get('queries', [])
    wecom_cfg     = config.get('wecom', {})
    feishu_cfg    = config.get('feishu', {})
    global_wh     = wecom_cfg.get('webhook', '')
    q_timeout     = int(config.get('query_timeout', 60))   # 单条 SQL 超时秒数

    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    _print_header(now_str)

    pushed_any = False

    # ── 按数据库配置分组：同库 SQL 共用一个连接，整组执行完再关闭 ──
    # 保持配置文件中 SQL 的书写顺序（OrderedDict 保序）
    groups: OrderedDict = OrderedDict()   # key -> (db_cfg, [query_cfg, ...])
    for q in queries:
        if not (q.get('sql') or '').strip():
            continue
        db  = _get_db_cfg(q, global_db_cfg)
        key = _db_key(db)
        if key not in groups:
            groups[key] = (db, [])
        groups[key][1].append(q)

    for db_cfg, group_queries in groups.values():
        db_type  = db_cfg.get('type', 'mysql').lower()
        db_label = _db_label(db_cfg)

        # ── 建立本组共用连接 ─────────────────────────────
        conn = None
        try:
            conn = get_connection(db_cfg)
            log.debug(f'数据库连接已建立: {db_label}')
        except Exception as e:
            _print_err(f'数据库连接失败 [{db_label}]: {e}')
            # 通知渠道：优先全局 webhook；若未配置则收集本组各 SQL 的 webhook 发送
            notify_whs = [global_wh] if global_wh else []
            if not notify_whs:
                for _q in group_queries:
                    _qwh = _q.get('webhooks') or []
                    if isinstance(_qwh, str):
                        _qwh = [w.strip() for w in _qwh.split(',') if w.strip()]
                    for _w in _qwh:
                        if _w and _w not in notify_whs:
                            notify_whs.append(_w)
            for _wh in notify_whs:
                send_text(_wh,
                          f'🔴 DB Monitor 连接失败\n数据库: {db_label}\n{e}\n时间: {now_str}')
            continue   # 本组所有 SQL 全部跳过

        try:
            # ── 逐条执行本组 SQL ───────────────────────────
            for q in group_queries:
                name      = q.get('name', 'unnamed')
                desc      = q.get('description', '')
                sql       = (q.get('sql') or '').strip()
                threshold = int(q.get('threshold', 0))

                print()
                _print_step(f'检测: {Fore.WHITE}{name}  {Style.DIM}[{db_label}]')
                if desc:
                    print(f'  {Style.DIM}{desc}')

                # ── 执行 SQL（带超时保护）────────────────────
                t_start = time.time()
                try:
                    cols, rows = _execute_with_timeout(conn, sql, db_type, q_timeout)
                    elapsed = round(time.time() - t_start, 2)
                except TimeoutError as e:
                    elapsed = round(time.time() - t_start, 2)
                    _print_err(f'SQL 超时（{elapsed}s）: {e}')
                    if global_wh:
                        send_text(global_wh,
                                  f'🔴 DB Monitor [{name}] SQL超时（{elapsed}s）\n'
                                  f'数据库: {db_label}\n时间: {now_str}')
                    continue
                except Exception as e:
                    elapsed = round(time.time() - t_start, 2)
                    _print_err(f'SQL 执行失败: {e}')
                    log.debug(traceback.format_exc())
                    continue

                meta, d_cols, d_rows = extract_meta(cols, rows)
                p     = resolve(meta, q, wecom_cfg)   # 最终参数
                count = len(d_rows)

                # 查询结果为空 → 无异常，直接跳过，不推送
                if count == 0:
                    _print_ok(f'查询结果为空，无异常数据（耗时 {elapsed}s）')

                elif count > threshold:
                    _print_warn(
                        f'⚠  发现 {count} 条异常数据'
                        f'（阈值={threshold}，耗时={elapsed}s）'
                    )
                    if d_cols and d_rows:
                        print(tabulate(
                            _trunc(d_rows[:5], d_cols),
                            headers=d_cols, tablefmt='rounded_outline', maxcolwidths=22,
                        ))

                    # ── 时间窗口 & 推送日检查 ─────────────────
                    ok_tw, reason_tw = check_time_window(p['time_window'])
                    ok_pd, reason_pd = check_push_days(p['push_days'])
                    ok_iv, reason_iv = should_push(name, p['interval'], state, force)

                    push_status = '成功'
                    if not ok_tw:
                        _print_warn(f'时间限制 — {reason_tw}')
                        push_status = '时间限制'
                    elif not ok_pd:
                        _print_warn(f'日期限制 — {reason_pd}')
                        push_status = '日期限制'
                    elif not ok_iv:
                        _print_warn(f'推送冷却 — {reason_iv}')
                        push_status = '冷却'
                    elif not p['webhooks']:
                        _print_warn('未配置 webhook，跳过推送')
                        push_status = '无Webhook'
                    else:
                        # ── 自动生成标题 ────────────────────
                        title    = p['title'] or f'🚨 {name}：发现 {count} 条异常数据'
                        iv_label = f'间隔={p["interval"]}min' if p['interval'] > 0 else '不限间隔'
                        _print_step(
                            f'推送至 {len(p["webhooks"])} 个群  '
                            f'跳转: {p["jump_url"][:45]}'
                            f'{"…" if len(p["jump_url"]) > 45 else ""}  '
                            f'[{iv_label}]'
                        )

                        # ── 推送每个 webhook ───────────────────
                        success_count = 0
                        description   = _build_desc(
                            title, count, desc, d_cols, d_rows, now_str
                        )
                        for idx, wh in enumerate(p['webhooks'], 1):
                            label = (f'({idx}/{len(p["webhooks"])}) '
                                     if len(p['webhooks']) > 1 else '')
                            short = wh[:55] + '…' if len(wh) > 55 else wh
                            _print_step(f'  推送 {label}{short}')
                            ok = send_news_card(wh, title, description,
                                                p['jump_url'], p['pic_url'])
                            if ok:
                                success_count += 1
                                _print_ok('   消息已推送 ✓')
                            else:
                                _print_err('   推送失败')

                        if success_count > 0:
                            mark_pushed(name, state)
                            save_state(state)
                            pushed_any  = True
                            push_status = ('成功'
                                           if success_count == len(p['webhooks'])
                                           else '部分成功')

                    # ── 写入飞书多维表格 ───────────────────────
                    preview_text = _preview_text(d_cols, d_rows)
                    log_to_bitable(feishu_cfg, {
                        'name':          name,
                        'title':         p['title'] or f'{name}：{count}条',
                        'count':         count,
                        'jump_url':      p['jump_url'],
                        'webhook_count': len(p['webhooks']),
                        'push_status':   push_status,
                        'preview_text':  preview_text,
                        # v6.0 新增
                        'db_label':      db_label,
                        'sql_text':      sql,
                        'elapsed':       elapsed,
                        'check_type':    p.get('check_type', ''),
                    }, override_table_id=p.get('bitable_table', ''))

                else:
                    _print_ok(
                        f'正常（{count} 行 ≤ 阈值 {threshold}，耗时={elapsed}s）'
                    )

        finally:
            # ── 本组全部 SQL 执行完毕，关闭连接 ───────────────
            if conn:
                try:
                    conn.close()
                    log.debug(f'数据库连接已关闭: {db_label}')
                except Exception:
                    pass

    print()
    _print_ok('本轮检测完成' + ('，已推送告警' if pushed_any else '，无新告警'))
    _print_footer()
    return pushed_any


def _build_desc(title: str, count: int, desc: str,
                cols: List[str], rows: List[List], now_str: str) -> str:
    lines = [f'检测时间：{now_str}', '']
    if desc:
        lines.append(desc)
    lines.append(f'异常数据：{count} 条')
    if cols and rows:
        lines.append('')
        for col, val in zip(cols[:4], rows[0][:4]):
            lines.append(f'  {col}: {str(val)[:30]}')
        if count > 1:
            lines.append(f'  ……共 {count} 条')
    lines += ['', '↓ 点击卡片查看详情']
    desc_str = '\n'.join(lines)
    return desc_str[:497] + '...' if len(desc_str) > 500 else desc_str


def _preview_text(cols: List[str], rows: List[List], max_rows: int = 3) -> str:
    if not cols or not rows:
        return ''
    lines = []
    for row in rows[:max_rows]:
        parts = [f'{c}={str(v)[:20]}' for c, v in zip(cols[:4], row[:4])]
        lines.append(' | '.join(parts))
    if len(rows) > max_rows:
        lines.append(f'... 共 {len(rows)} 条')
    return '\n'.join(lines)

# ═══════════════════════════════════════════════════════════
#  打印辅助
# ═══════════════════════════════════════════════════════════

def _print_header(ts: str):
    w = 62
    print(f'\n{Fore.CYAN}╔{"═"*w}╗')
    print(f'{Fore.CYAN}║  DB Monitor v6.1 — 数据库异常监控{" "*(w-34)}║')
    print(f'{Fore.CYAN}║  {ts}{" "*(w-len(ts)-2)}║')
    print(f'{Fore.CYAN}╚{"═"*w}╝')

def _print_footer():
    print(f'{Fore.CYAN}{"─"*64}')

def _print_step(msg: str): print(f'{Fore.WHITE}  → {msg}')
def _print_ok(msg: str):   print(f'{Fore.GREEN}  ✓ {msg}')
def _print_warn(msg: str): print(f'{Fore.YELLOW}  ！{msg}')
def _print_err(msg: str):  print(f'{Fore.RED}  ✗ {msg}')

def _wait_exit():
    print()
    input(f'{Fore.WHITE}按 Enter 键退出...')

def _trunc(rows, cols, max_len=25):
    def _cell(c):
        if c is None:
            return 'NULL'
        s = str(c)
        return s[:max_len] + '…' if len(s) > max_len else s
    return [[_cell(c) for c in row] for row in rows]

# ═══════════════════════════════════════════════════════════
#  入口
# ═══════════════════════════════════════════════════════════

def main():
    global CONFIG_PATH, STATE_FILE  # noqa: PLW0603

    parser = argparse.ArgumentParser(
        description='DB Monitor v6.1 — 数据库异常监控 & 企业微信推送',
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument('--config',      default=str(CONFIG_PATH),
                        help='配置文件路径（默认: config.yaml）')
    parser.add_argument('--once',        action='store_true',
                        help='只执行一次后退出')
    parser.add_argument('--force',       action='store_true',
                        help='忽略推送冷却限制，强制推送（用于测试）')
    parser.add_argument('--list-tables', action='store_true',
                        help='列出飞书多维表格中的数据表（获取 table_id 用）')
    args = parser.parse_args()

    config_arg = Path(args.config)
    if config_arg != CONFIG_PATH:
        CONFIG_PATH = config_arg
        STATE_FILE  = config_arg.parent / 'monitor_state.json'

    config = load_config()

    log_cfg = config.get('log', {})
    setup_logging(level=log_cfg.get('level', 'INFO'),
                  log_file=log_cfg.get('file') or None)

    # ── 辅助命令 ──────────────────────────────────────────
    if args.list_tables:
        list_bitable_tables(config.get('feishu', {}))
        return

    sched_cfg  = config.get('schedule', {})
    use_sched  = sched_cfg.get('enabled', False) and not args.once
    check_secs = max(10, int(sched_cfg.get('check_interval_seconds', 60)))

    state = load_state()

    if not use_sched:
        run_monitor(config, state, force=args.force)
        _wait_exit()
    else:
        print(
            f'\n{Fore.CYAN}[定时] 每 {check_secs}s 检测一次'
            f'  |  各 SQL 推送间隔由 _interval 独立控制'
            f'\n{Fore.CYAN}       Ctrl+C 停止\n'
        )
        run_monitor(config, state, force=args.force)
        schedule.every(check_secs).seconds.do(run_monitor, config, state, args.force)
        try:
            while True:
                schedule.run_pending()
                time.sleep(1)
        except KeyboardInterrupt:
            print(f'\n{Fore.YELLOW}[定时] 已停止。')


if __name__ == '__main__':
    main()
