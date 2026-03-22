# DB Monitor v6.1

数据库异常监控 & 企业微信推送工具。定时执行 SQL 检测，结果超阈值时自动推送企业微信群，并可选记录到飞书多维表格。

## 功能特性

- **多数据库支持**：MySQL、Oracle、PostgreSQL、MSSQL、SQLite，依赖自动安装
- **多数据库连接**：每条 SQL 可独立配置数据库，同库 SQL 自动分组共用连接
- **SQL 内嵌控制**：通过 SELECT 中的特殊列控制推送行为，无需修改配置文件
- **推送频率限速**：滑动窗口限速，每 Webhook 最多 18 条/分钟，不丢消息
- **节假日感知**：支持 `workday` 模式，自动识别中国法定节假日（含调休）
- **飞书多维表格**：告警记录自动写入飞书 Bitable，含 12 个字段
- **SQL 超时保护**：单条 SQL 超时后自动跳过，不影响同组其他 SQL
- **零配置启动**：首次运行自动生成 `config.yaml` 模板

## 快速开始

### 1. 安装

```bash
# Python 3.8+ 即可，依赖会自动安装
git clone https://github.com/XKINGSH/db-monitor.git
cd db-monitor
```

### 2. 生成配置文件

```bash
python monitor.py
# 首次运行时会自动生成 config.yaml，按提示编辑后重新运行
```

### 3. 编辑配置

打开 `config.yaml`，按照注释说明填写：

```yaml
# 数据库连接
database:
  type: mysql
  host: 127.0.0.1
  port: 3306
  database: mydb
  username: root
  password: "your_password"

# 企业微信 Webhook
wecom:
  webhook: "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=YOUR_WEBHOOK_KEY"
```

### 4. 运行

```bash
# 执行一次检测
python monitor.py --once

# 持续定时检测（需在 config.yaml 开启 schedule.enabled: true）
python monitor.py

# 强制推送（忽略冷却限制，用于测试）
python monitor.py --once --force

# 查看飞书多维表格的 table_id
python monitor.py --list-tables
```

## SQL 特殊列

在 SELECT 中加入以下列名，程序会自动识别并处理（不会出现在推送消息里）：

| 列名 | 说明 | 示例值 |
|------|------|--------|
| `_webhook` | 推送目标 Webhook，逗号分隔多个 | `'https://qyapi.weixin.qq.com/...?key=XXX'` |
| `_jump_url` | 点击卡片时跳转的链接 | `'https://your-system.com/orders'` |
| `_interval` | 推送冷却（分钟），0=不限 | `30` |
| `_title` | 自定义告警标题 | `'超时订单告警'` |
| `_time_window` | 推送时间段，格式 `HH:MM-HH:MM` | `'09:00-18:00'` |
| `_push_days` | 推送日期：`all`/`workday`/`weekday`/`Mon,Tue,...` | `'workday'` |
| `_bitable_table` | 飞书表格 table_id，`skip` 跳过记录 | `'tblXXXXX'` |
| `_check_type` | 检查类型标签，自由文本 | `'数据一致性'` |

**MySQL 示例：**

```sql
SELECT
  order_id, user_id, amount, created_at,
  'https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=YOUR_KEY' AS _webhook,
  'https://your-system.com/orders'  AS _jump_url,
  30                                AS _interval,
  '超时订单告警'                    AS _title,
  '09:00-18:00'                     AS _time_window,
  'workday'                         AS _push_days
FROM orders
WHERE status = 'pending'
  AND created_at < NOW() - INTERVAL 1 DAY
LIMIT 50
```

**Oracle 列别名需加双引号：**

```sql
SELECT
  invoice_no, amount,
  'https://qyapi.weixin.qq.com/...?key=YOUR_KEY' AS "_webhook",
  '08:30-17:30'                                   AS "_time_window"
FROM ar_invoices WHERE due_date < SYSDATE - 90
```

## 多数据库连接

每条 SQL 可独立指定数据库，支持跨库检测：

```yaml
queries:
  - name: "财务库异常检查"
    database:               # 覆盖全局 database 配置
      type: mysql
      host: 192.168.2.20
      port: 3306
      database: finance_db
      username: report
      password: "your_password"
    sql: |
      SELECT order_no, amount FROM finance_orders WHERE status = 'ERROR'
    threshold: 0
```

## 飞书多维表格配置

在 `config.yaml` 中填写飞书应用信息：

```yaml
feishu:
  app_id:     "YOUR_APP_ID"
  app_secret: "YOUR_APP_SECRET"
  bitable:
    enabled:   true
    app_token: "YOUR_APP_TOKEN"
    table_id:  "YOUR_TABLE_ID"
```

飞书多维表格需建立以下字段：

| 字段名 | 类型 |
|--------|------|
| 告警时间 | 日期时间 |
| SQL名称 | 文本 |
| 告警标题 | 文本 |
| 异常条数 | 数字 |
| 推送状态 | 单选 |
| 数据库 | 文本 |
| SQL内容 | 多行文本 |
| 运行时长 | 数字 |
| 检查类型 | 单选 |
| 跳转链接 | 网页 |
| 推送群数 | 数字 |
| 数据预览 | 多行文本 |

## 命令行参数

| 参数 | 说明 |
|------|------|
| `--config PATH` | 指定配置文件路径（默认 `config.yaml`） |
| `--once` | 只执行一次后退出 |
| `--force` | 强制推送，忽略冷却限制 |
| `--list-tables` | 列出飞书多维表格的数据表 |

## 版本历史

| 版本 | 主要更新 |
|------|----------|
| v6.1 | 修复超时死锁（`shutdown(wait=False)`）、安装后验证、连接失败兜底通知、typo 修复 |
| v6.0 | 多数据库支持、同库连接分组、SQL 超时保护、`_check_type` 列、飞书新增 4 字段 |
| v5.0 | 法定节假日自动更新、空结果跳过推送 |
| v4.0 | 特殊列机制、推送冷却、时间窗口、飞书日志 |

## 系统要求

- Python 3.8+
- 网络访问：企业微信 Webhook、飞书 API（若开启）

数据库驱动按需自动安装：
- MySQL：`PyMySQL`
- Oracle：`oracledb`
- PostgreSQL：`psycopg2-binary`
- MSSQL：`pymssql`

## License

MIT
