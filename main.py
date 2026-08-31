# -*- coding: utf-8 -*-
"""astrbot_plugin_lurker_watcher —— 群潜水监测插件（主类）

功能总览
========
1. 插件加载后自动获取机器人所在的所有群，并将每个群的全部成员纳入监控；
2. 监听群消息，实时刷新成员的「最后发言时间」；
3. 每天固定时间（默认 08:00，可在 WebUI 修改）向各群推送潜水时长排行榜；
4. 成员潜水天数达到「阈值天数 - 预警天数」时，自动 @ 该成员发送警告；
5. 潜水天数超过阈值后，可选调用 LLM 智能判断是否踢人，确认后自动移出群聊；
6. 支持白名单（不警告不踢、排行榜 ⭐ 标识）与多群独立配置；
7. 所有参数均可在 AstrBot WebUI 的插件配置面板中可视化修改：
   点击保存后 AstrBot 会热重载本插件，新配置立即生效，无需改任何代码。

技术要点
========
* 配置：插件目录下的 _conf_schema.json 定义 Schema，AstrBot 检测到该文件后
  会自动构造 AstrBotConfig 并注入本类构造函数（第二个参数），
  运行期一律通过 self.cfg 读取，绝不硬编码；
* 存储：使用 Star 基类混入的 PluginKVStoreMixin（put_kv_data /
  get_kv_data / delete_kv_data），数据落在 AstrBot 内置数据库，
  重启不丢失；结构为 {群号: {用户ID: {first_seen, last_message_time,
  username, warned_at, ...}}}，详见 storage.py；
* 定时任务：AstrBot v4 的 Context 未内置 cron_manager，本插件实现了轻量级
  PluginCronManager（asyncio 心跳循环）承担同等职责：
    - check_interval 间隔任务：扫描全部受监控群的潜水状态；
    - 每日 HH:MM 任务：逐群判断报告时间并发送日报；
    - 60 秒心跳任务：把内存中的脏数据批量落盘。
* 平台接口：get_group_list / get_group_member_list / set_group_kick 等
  群管理能力基于 OneBot v11（aiocqhttp：NapCat / Lagrange 等），
  已封装在 member_fetcher.MemberFetcher 中（即需求中的
  kick_group_member 等方法的落地点）。

指令一览（/lurker 可查看自动生成的指令树）
==========================================
    /lurker list [群号]                          查看潜水排行榜
    /lurker set_threshold <天数> [群号]          设置阈值（管理员）
    /lurker set_warning <天数> [群号]            设置预警天数（管理员）
    /lurker whitelist add/remove <@用户|QQ号>    群级白名单管理（管理员）
    /lurker whitelist show                       查看本群白名单
    /lurker report [群号]                        手动发送监测报告
    /lurker init [群号]                          重新初始化成员列表（管理员）
"""

import asyncio
import json
import re
import time
import traceback
from datetime import datetime, timedelta

from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.star import Context, Star
from astrbot.api.web import error_response, json_response, request

try:
    # 插件以 data.plugins.<目录名>.main 包形式被导入（AstrBot 标准方式）
    from .config import PluginConfig
    from .member_fetcher import MemberFetcher
    from .notifier import Notifier
    from .storage import LurkerStorage, new_member_record
except ImportError:  # 兜底：平铺目录导入
    from config import PluginConfig
    from member_fetcher import MemberFetcher
    from notifier import Notifier
    from storage import LurkerStorage, new_member_record

PLUGIN_VERSION = "v1.0.3"
PLUGIN_NAME = "astrbot_plugin_lurker_watcher"

DAY_SECONDS = 86400

# ---- 防打扰 / 防重复的节流常量 ----
WARN_REPEAT_INTERVAL = DAY_SECONDS          # 同一成员两次 @ 警告的最小间隔
KICK_RECHECK_INTERVAL = DAY_SECONDS         # 同一成员两次踢人评估的最小间隔
INIT_GRACE_SECONDS = DAY_SECONDS            # 初始化保护期：纳管后 24h 内不踢人
KICK_MAX_FAILS = 3                          # 连续踢出失败次数上限，超过则停止跟踪该成员
# 每轮警告/评估人数上限已改为 WebUI 可配置项：
#   max_warns_per_round / max_kick_evals_per_round（0 = 不限制，即预警区/达阈值的所有人）


class PluginCronManager:
    """插件内置的极简定时任务管理器（替代尚不存在的 context.cron_manager）。

    用 asyncio 后台心跳任务实现「间隔执行」语义：
        add_interval(名字, 间隔秒数, 协程函数) -> 每隔指定秒数执行一次
    「每日 HH:MM 执行」语义由本插件的 _daily_report_job 在 60 秒间隔任务里
    逐群判断（这样可以支持每个群配置不同的报告时间）。

    生命周期与插件一致：initialize() 中 start()，terminate() 中 stop()。
    """

    TICK_SECONDS = 20  # 心跳节拍

    def __init__(self):
        self._jobs = []            # [dict(name, interval, fn, last_run)]
        self._task: asyncio.Task | None = None
        self._stopping = asyncio.Event()

    def add_interval(self, name: str, interval: int, fn):
        """注册一个间隔任务。fn 必须是无参协程函数。"""
        self._jobs.append(
            {"name": name, "interval": max(5, int(interval)), "fn": fn, "last_run": 0.0}
        )

    async def start(self):
        if self._task and not self._task.done():
            return
        self._stopping.clear()
        self._task = asyncio.create_task(self._loop(), name="lurker_cron")

    async def stop(self):
        self._stopping.set()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
            self._task = None

    async def _loop(self):
        """心跳主循环：每到节拍检查所有间隔任务是否到期。"""
        while not self._stopping.is_set():
            await asyncio.sleep(self.TICK_SECONDS)
            now = time.time()
            for job in self._jobs:
                if now - job["last_run"] >= job["interval"]:
                    job["last_run"] = now
                    try:
                        await job["fn"]()
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        logger.error(
                            f"[lurker_watcher] 定时任务 {job['name']} 执行出错：\n"
                            + traceback.format_exc()
                        )


class LurkerWatcherPlugin(Star):
    """潜水监测插件主类（Star 子类，AstrBot 自动识别并加载）。"""

    def __init__(self, context: Context, config: AstrBotConfig = None):
        # AstrBot 检测到 _conf_schema.json 后会把 AstrBotConfig 注入 config 参数；
        # Star 基类签名为 __init__(context, config=None)，直接透传即可。
        super().__init__(context, config)
        self.config: AstrBotConfig = config

        # 组件统一在 initialize() 中构建：plugin_id 由 AstrBot 在实例化之后才
        # 赋值给实例，而 KV 存储依赖 plugin_id，因此这里只做占位。
        self.cfg: PluginConfig | None = None
        self.storage: LurkerStorage | None = None
        self.fetcher: MemberFetcher | None = None
        self.notifier: Notifier | None = None
        self.cron: PluginCronManager | None = None
        self._bg_tasks: list = []          # 后台任务句柄（自动初始化等）
        self._pending_group_inits: set = set()  # 待初始化群去重
        self._llm_warned = False           # LLM 不可用时只提示一次
        context.register_web_api(f"/{PLUGIN_NAME}/dashboard", self.page_dashboard, ["GET"], "Lurker dashboard data")
        context.register_web_api(f"/{PLUGIN_NAME}/groups/<group_id>", self.page_group, ["GET"], "Lurker group details")
        context.register_web_api(f"/{PLUGIN_NAME}/groups/<group_id>/whitelist", self.page_whitelist, ["POST"], "Update group whitelist")

    async def page_dashboard(self):
        """Return a compact, read-only overview for the Plugin Page."""
        if self.storage is None or self.cfg is None:
            return error_response("Plugin is still initializing", status_code=503)
        now = time.time()
        groups = []
        for gid, info in self.storage.list_groups().items():
            members = self.storage.get_members(gid)
            threshold = max(1, int(self.cfg.get_group("threshold_days", gid)))
            warning = max(0, min(int(self.cfg.get_group("warning_days", gid)), threshold - 1))
            warning_count = overdue_count = 0
            for rec in members.values():
                days = max(0.0, (now - float(rec.get("last_message_time") or now)) / DAY_SECONDS)
                if days >= threshold:
                    overdue_count += 1
                elif days >= threshold - warning:
                    warning_count += 1
            groups.append({"id": gid, "name": info.get("group_name") or gid, "members": len(members), "warning_count": warning_count, "overdue_count": overdue_count, "threshold": threshold, "monitored": self._is_monitored(gid)})
        return json_response({"version": PLUGIN_VERSION, "groups": groups, "group_count": len(groups), "updated_at": int(now)})

    async def page_group(self, group_id: str):
        """Return one group's effective settings and idle-member ranking."""
        if self.storage is None or self.cfg is None:
            return error_response("Plugin is still initializing", status_code=503)
        gid = str(group_id)
        if not self.storage.has_group(gid):
            return error_response("Group is not monitored", status_code=404)
        now = time.time()
        whitelist = self.cfg.get_whitelist(gid)
        members = []
        for uid, rec in self.storage.get_members(gid).items():
            days = max(0.0, (now - float(rec.get("last_message_time") or now)) / DAY_SECONDS)
            members.append({"id": uid, "name": rec.get("username") or uid, "role": rec.get("role") or "member", "idle_days": round(days, 1), "whitelisted": uid in whitelist})
        members.sort(key=lambda item: item["idle_days"], reverse=True)
        return json_response({"id": gid, "name": self.storage.get_group_name(gid) or gid, "members": members, "group_whitelist": self.storage.get_group_config(gid).get("whitelist", []), "settings": {key: self.cfg.get_group(key, gid) for key in ("threshold_days", "warning_days", "enable_llm_decision", "warn_before_kick", "max_warns_per_round", "max_kick_evals_per_round")}})

    async def page_whitelist(self, group_id: str):
        """Replace the group-local whitelist. Dashboard auth is enforced upstream."""
        if self.storage is None or not self.storage.has_group(group_id):
            return error_response("Group is not monitored", status_code=404)
        payload = await request.json(default={})
        users = payload.get("users", []) if isinstance(payload, dict) else []
        if not isinstance(users, list) or any(not str(uid).strip().isdigit() for uid in users):
            return error_response("users must be a list of numeric QQ IDs", status_code=400)
        cleaned = list(dict.fromkeys(str(uid).strip() for uid in users))
        await self.storage.set_group_config(group_id, "whitelist", cleaned)
        return json_response({"saved": True, "users": cleaned})

    # ==================================================================
    # 生命周期
    # ==================================================================
    async def initialize(self):
        """插件加载（含 WebUI 保存配置后的热重载）时调用。"""
        # 1. 组装各组件（storage 依赖 plugin_id，必须放在这里）
        self.storage = LurkerStorage(self)
        await self.storage.load()
        self.cfg = PluginConfig(self.config, self.storage)
        self.fetcher = MemberFetcher(self.context)
        self.notifier = Notifier(self.context, self.fetcher)

        # 2. 注册定时任务（相当于 context.cron_manager 的职责）
        self.cron = PluginCronManager()
        self.cron.add_interval("flush", 60, self._flush_job)
        self.cron.add_interval("check", self.cfg.get_check_interval(), self._interval_check_job)
        self.cron.add_interval("daily_report", 60, self._daily_report_job)
        await self.cron.start()

        # 3. 自动初始化：拉取所有群 + 全量成员（后台执行，不阻塞加载流程）
        self._spawn(self._auto_init_task())

        logger.info(
            f"[lurker_watcher] {PLUGIN_VERSION} 已加载｜受监控群 {len(self.storage.list_groups())} 个"
            f"｜阈值 {self.cfg.get_global('threshold_days')} 天"
            f"｜每日报告 {self.cfg.get_global('daily_report_time')}"
            f"｜LLM 决策 {'开' if self.cfg.get_global('enable_llm_decision') else '关'}"
        )

    async def terminate(self):
        """插件被禁用/重载时调用：停任务、落盘，保证数据安全。"""
        if self.cron:
            await self.cron.stop()
        for task in self._bg_tasks:
            task.cancel()
        self._bg_tasks.clear()
        if self.storage:
            await self.storage.flush()
        logger.info("[lurker_watcher] 插件已卸载，数据已落盘")

    def _spawn(self, coro):
        """创建后台任务并登记，便于 terminate 时统一取消。"""
        task = asyncio.create_task(coro)
        self._bg_tasks.append(task)
        return task

    # ==================================================================
    # 自动初始化
    # ==================================================================
    async def _auto_init_task(self):
        """加载后自动拉取群列表与全量成员并纳入监控（带重试）。

        协议端（NapCat 等）的 WebSocket 可能断线重连、或在插件加载后才连上，
        因此持续重试约 7.5 分钟（30 次 × 15 秒）；期间日志降噪，每 5 次提示一次。
        即使全部失败，机器人进群收到的首条消息也会触发 _maybe_adopt_group 自动纳管。
        """
        max_attempts = 30
        for attempt in range(1, max_attempts + 1):
            await asyncio.sleep(5 if attempt == 1 else 15)  # 首次 5 秒后尝试，之后每 15 秒
            try:
                groups = await self.fetcher.get_group_list()
            except Exception:
                logger.error("[lurker_watcher] 获取群列表异常：\n" + traceback.format_exc())
                continue
            if groups:
                monitor = set(self.cfg.get_monitor_groups())
                total_members = 0
                inited = 0
                for gid, info in groups.items():
                    if monitor and gid not in monitor:
                        continue  # 不在 WebUI 配置的监控名单里
                    if await self._init_group(gid, info):
                        inited += 1
                        total_members += len(self.storage.get_members(gid))
                logger.info(
                    f"[lurker_watcher] 自动初始化完成（第 {attempt} 次尝试）："
                    f"{inited} 个群，{total_members} 名成员纳入监控"
                )
                return
            if attempt == 1 or attempt % 5 == 0:
                tip = (
                    "（未发现 aiocqhttp 平台适配器，请确认已接入 NapCat/Lagrange 等 OneBot v11 协议端）"
                    if not self.fetcher.has_adapter()
                    else "（协议端可能尚未连接就绪，继续等待）"
                )
                logger.warning(f"[lurker_watcher] 暂未获取到群列表（第 {attempt}/{max_attempts} 次），{tip}")
        logger.warning("[lurker_watcher] 自动初始化未获取到任何群，机器人收到群消息时会自动纳管，也可手动执行 /lurker init")

    async def _init_group(self, gid: str, info: dict) -> bool:
        """拉取单个群的全量成员并写入存储（保留已有成员的活跃数据）。"""
        gid = str(gid)
        members_raw = await self.fetcher.get_group_member_list(info.get("platform_id", ""), gid)
        if members_raw is None:
            return False
        # 获取机器人自身 QQ 号（OneBot get_login_info），从监控表中排除，
        # 避免把机器人自己统计成潜水成员甚至尝试踢出自己
        self_ids = await self.fetcher.get_bot_self_ids()
        now = time.time()
        existing = self.storage.get_members(gid)
        mapping = {}
        for m in members_raw:
            uid = str(m.get("user_id", "")).strip()
            if not uid:
                continue
            if uid in self_ids:
                continue  # 跳过机器人自己
            username = str(m.get("card") or m.get("nickname") or uid)
            role = str(m.get("role") or "member")
            if uid in existing:
                # 已纳管：刷新昵称/群身份，保留历史活跃/警告状态
                rec = existing[uid]
                rec["username"] = username
                rec["role"] = role
                mapping[uid] = rec
            else:
                # 新纳管：一律从当前时间起算（完全宽限）。
                # 不采信协议端的 last_sent_time：该字段在部分协议端实现中不可靠
                # （恒为 0 或过期），且机器人入群前的历史无法核实——以其作为
                # 惩罚依据会造成"刚入群就误读潜水状态"的误判。
                # 只有机器人自己观察到的沉默才计入潜水天数；
                # /lurker init 重初始化时，已跟踪成员的历史活跃数据仍会保留
                # （那是机器人观测到的真实数据，见上方 existing 分支）。
                mapping[uid] = new_member_record(now, now, username, role)

        self.storage.init_members(gid, mapping)
        self.storage.upsert_group(gid, info.get("platform_id", ""), info.get("group_name", ""))
        meta = self.storage.get_group_meta(gid)
        # initialized_at：首次纳管时间（踢人保护期的起点），重初始化不覆盖
        initialized_at = meta.get("initialized_at") or now
        self.storage.set_group_meta(gid, "initialized_at", initialized_at)
        self.storage.set_group_meta(gid, "member_count", len(mapping))
        await self.storage.flush()
        logger.info(f"[lurker_watcher] 群 {gid} 初始化完成：{len(mapping)} 名成员")
        return True

    # ==================================================================
    # 消息监听：刷新活跃时间
    # ==================================================================
    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    async def on_group_message(self, event: AstrMessageEvent):
        """监听所有群消息：更新成员最后发言时间（只记录，不拦截、不回复）。"""
        try:
            gid = str(event.get_group_id() or "").strip()
            uid = str(event.get_sender_id() or "").strip()
            if not gid or not uid:
                return
            if uid == str(event.get_self_id() or ""):
                return  # 忽略机器人自己的消息
            if self.storage is None or not self.storage.has_group(gid):
                # 未纳管的群（新入群 / 新增平台）：尝试在后台自动纳管
                self._maybe_adopt_group(gid)
                return
            # 群名片优先级：OneBot 原始消息的 sender.card（群名片）
            # > AstrBotMessage.sender.card > nickname > 空串
            username = ""
            raw = getattr(event.message_obj, "raw_message", None)
            if isinstance(raw, dict):
                username = str((raw.get("sender") or {}).get("card") or "")
            sender = getattr(event.message_obj, "sender", None)
            if not username and sender is not None:
                username = (
                    getattr(sender, "card", None)
                    or getattr(sender, "nickname", None)
                    or ""
                )
            self.storage.touch_member(gid, uid, str(username), time.time())
        except Exception as e:
            logger.error(f"[lurker_watcher] 处理群消息出错: {e}\n" + traceback.format_exc())

    def _maybe_adopt_group(self, gid: str):
        """机器人进入新群后收到首条消息时，自动把该群纳入监控（去重 + 后台执行）。"""
        if gid in self._pending_group_inits or not self.storage or not self.cfg:
            return
        monitor = set(self.cfg.get_monitor_groups()) if self.cfg else set()
        if monitor and gid not in monitor:
            return  # 用户明确只监控部分群
        self._pending_group_inits.add(gid)

        async def _adopt():
            try:
                groups = await self.fetcher.get_group_list()
                info = groups.get(gid)
                if info:
                    await self._init_group(gid, info)
            except Exception:
                logger.error(f"[lurker_watcher] 自动纳管群 {gid} 失败：\n" + traceback.format_exc())
            finally:
                self._pending_group_inits.discard(gid)

        self._spawn(_adopt())

    # ==================================================================
    # 定时任务
    # ==================================================================
    async def _flush_job(self):
        """每 60 秒把内存脏数据落盘一次。"""
        if self.storage:
            await self.storage.flush()

    async def _interval_check_job(self):
        """按 check_interval 扫描所有受监控群，触发 @ 警告与踢人评估。"""
        for gid, info in list(self.storage.list_groups().items()):
            if not self._is_monitored(gid):
                continue
            try:
                await self._check_group(gid, info)
            except Exception:
                logger.error(f"[lurker_watcher] 检查群 {gid} 时出错：\n" + traceback.format_exc())

    async def _check_group(self, gid: str, info: dict):
        """单个群的潜水状态检查：先预警、后踢人评估。"""
        members = self.storage.get_members(gid)
        if not members:
            return
        now = time.time()
        threshold = int(self.cfg.get_group("threshold_days", gid))
        if threshold <= 0:
            threshold = 7  # WebUI 脏配置兜底
        # 预警天数必须小于阈值：脏配置（warning_days >= threshold）时自动钳制，
        # 避免出现「潜水满 1 天就 @ 警告」的怪异行为
        warning_days = min(int(self.cfg.get_group("warning_days", gid)), threshold - 1)
        if warning_days < 0:
            warning_days = 0
        warn_line = max(1, threshold - warning_days)
        whitelist = self.cfg.get_whitelist(gid)
        meta = self.storage.get_group_meta(gid)

        # 防御：纳管时间丢失（数据迁移/损坏）时补写并跳过本轮踢人评估，
        # 保证 24h 初始化保护期永远生效
        initialized_at = meta.get("initialized_at") or 0
        if not initialized_at:
            self.storage.set_group_meta(gid, "initialized_at", now)
            logger.warning(f"[lurker_watcher] 群 {gid} 缺少纳管时间，已补写并跳过本轮踢人评估")
            initialized_at = now

        warn_candidates = []   # 预警区成员
        kick_candidates = []   # 达到阈值的成员
        for uid, rec in list(members.items()):
            if uid in whitelist:
                continue  # 白名单不警告不踢
            if str(rec.get("role") or "").lower() in ("owner", "admin"):
                continue  # 群主/管理员：OneBot 无法踢出，不浪费警告与 LLM 评估
            try:
                days = (now - float(rec.get("last_message_time") or now)) / DAY_SECONDS
            except (TypeError, ValueError):
                continue
            if days >= threshold:
                kick_candidates.append((days, uid, rec))
            elif days >= warn_line:
                warn_candidates.append((days, uid, rec))

        # ---- 1. 预警：@ 警告（max_warns_per_round=0 表示不限量，默认全部警告） ----
        max_warns = int(self.cfg.get_group("max_warns_per_round", gid))
        warned = 0
        for days, uid, rec in sorted(warn_candidates, key=lambda x: x[0], reverse=True):
            if max_warns > 0 and warned >= max_warns:
                break
            last_warn = rec.get("warned_at") or 0
            if now - last_warn < WARN_REPEAT_INTERVAL:
                continue  # 24h 内已警告过，不再重复打扰
            chain = self.notifier.build_warn_chain(
                uid,
                rec.get("username", ""),
                days,
                threshold,
                warning_days,
                template=self.cfg.get_group("warn_template", gid),
                group_id=gid,
            )
            ok = await self.notifier.send_group_chain(info.get("platform_id", ""), gid, chain)
            if ok:
                self.storage.set_member_fields(gid, uid, warned_at=now)
                logger.info(f"[lurker_watcher] 群 {gid} 已 @ 警告 {uid}（潜水 {days:.1f} 天）")
            warned += 1

        # ---- 2. 踢人评估：初始化保护期后才开始，避免首轮误杀 ----
        if now - initialized_at < INIT_GRACE_SECONDS:
            if kick_candidates:
                logger.debug(f"[lurker_watcher] 群 {gid} 处于初始化保护期，本轮跳过 {len(kick_candidates)} 名候选")
            return

        evaluated = 0
        # max_kick_evals_per_round=0 表示不限量，默认所有达阈值成员都会进入评估
        max_evals = int(self.cfg.get_group("max_kick_evals_per_round", gid))
        for days, uid, rec in sorted(kick_candidates, key=lambda x: x[0], reverse=True):
            if max_evals > 0 and evaluated >= max_evals:
                break
            last_eval = rec.get("evaluated_at") or 0
            if now - last_eval < KICK_RECHECK_INTERVAL:
                continue  # 24h 内已评估过
            # 先记录评估时间，无论成功失败，下一轮（次日）才会重试
            self.storage.set_member_fields(gid, uid, evaluated_at=now)
            await self._evaluate_and_maybe_kick(gid, info, uid, rec, days, threshold)
            evaluated += 1

    async def _evaluate_and_maybe_kick(self, gid, info, uid, rec, days, threshold):
        """达到阈值的成员：可选 LLM 智能决策 -> 确认后执行移出。"""
        enable_llm = bool(self.cfg.get_group("enable_llm_decision", gid))
        decision_desc = "规则判定"
        reason = f"已连续 {days:.0f} 天未发言，达到 {threshold} 天阈值"

        if enable_llm:
            decision = await self._llm_decide(gid, uid, rec, days, threshold)
            if decision is None:
                # LLM 不可用/解析失败：安全起见本轮不踢，次日重新评估
                logger.warning(f"[lurker_watcher] 群 {gid} 成员 {uid} 的 LLM 决策失败，本轮跳过")
                return
            kick, reason = decision
            decision_desc = "LLM 智能决策"
            if not kick:
                # LLM 决定保留：不打扰群里，仅记录日志
                logger.info(f"[lurker_watcher] LLM 决定保留群 {gid} 成员 {uid}：{reason}")
                return

        # 执行移出（warn_before_kick 决定是否先发最终通牒）
        await self._execute_kick(gid, info, uid, rec, days, reason, decision_desc)

    async def _llm_decide(self, gid, uid, rec, days, threshold):
        """调用 LLM 判断是否应踢出某成员。

        返回 (是否踢出, 理由)；LLM 不可用或结果无法解析时返回 None。
        """
        try:
            provider = self.context.get_using_provider()
        except Exception:
            provider = None
        if provider is None:
            if not self._llm_warned:
                self._llm_warned = True
                logger.warning(
                    "[lurker_watcher] 未配置可用的 LLM 提供商，LLM 智能决策不可用；"
                    "请在 WebUI 配置提供商，或关闭 enable_llm_decision 改为规则直接踢出"
                )
            return None

        first_seen = rec.get("first_seen") or 0
        span_days = max(0.0, (time.time() - first_seen) / DAY_SECONDS)
        username = rec.get("username") or uid
        prompt = (
            "请根据以下数据，判断是否应将该群成员移出群聊：\n"
            f"- 群号：{gid}\n"
            f"- 成员：{username}（QQ {uid}）\n"
            f"- 纳管时长：{span_days:.1f} 天\n"
            f"- 最后发言距今：{days:.1f} 天\n"
            f"- 群潜水阈值：{threshold} 天\n"
            "参考原则：数据零散、偶尔活跃的新人可以宽容；长期为零且曾被警告仍不改的再考虑移出；"
            "存在疑问时倾向保留。"
        )
        system_prompt = (
            "你是 QQ 群管理助手，负责潜水成员的移出裁决。"
            '你必须且只能输出一个 JSON 对象，格式：{"action": "kick" 或 "keep", "reason": "不超过40字的理由"}。'
            "不要输出任何其他内容。"
        )
        try:
            resp = await provider.text_chat(prompt=prompt, system_prompt=system_prompt)
        except Exception as e:
            logger.error(f"[lurker_watcher] LLM 请求失败: {e}")
            return None

        text = str(getattr(resp, "completion_text", "") or "")
        parsed = self._parse_decision_json(text)
        if parsed is None:
            logger.warning(f"[lurker_watcher] LLM 返回内容无法解析为 JSON：{text[:200]}")
            return None
        action = str(parsed.get("action", "")).strip().lower()
        reason = str(parsed.get("reason", "")).strip() or reason
        if action == "kick":
            return True, reason
        if action == "keep":
            return False, reason
        logger.warning(f"[lurker_watcher] LLM 返回了未知 action：{action}")
        return None

    @staticmethod
    def _parse_decision_json(text: str):
        """从 LLM 回复中鲁棒地提取决策 JSON（容忍 ```json 包裹、前后缀文本等）。"""
        if not text:
            return None
        match = re.search(r"\{.*\}", text, re.DOTALL)
        if not match:
            return None
        try:
            data = json.loads(match.group(0))
            return data if isinstance(data, dict) else None
        except json.JSONDecodeError:
            return None

    async def _execute_kick(self, gid, info, uid, rec, days, reason, decision_desc):
        """执行移出群聊：可选最终警告 -> 踢人 -> 群通知 -> 清理数据。"""
        gid = str(gid)
        platform_id = info.get("platform_id", "")
        username = rec.get("username", "")
        threshold = int(self.cfg.get_group("threshold_days", gid))

        # 1. 最终警告（可选）
        if bool(self.cfg.get_group("warn_before_kick", gid)):
            try:
                chain = self.notifier.build_final_warning_chain(uid, username, days, threshold, reason)
                await self.notifier.send_group_chain(platform_id, gid, chain)
                await asyncio.sleep(1.5)  # 略作间隔，保证两条消息顺序到达
            except Exception:
                logger.error("[lurker_watcher] 发送最终警告失败：\n" + traceback.format_exc())

        # 2. 调用平台接口移出群聊
        ok, err = await self.fetcher.kick_group_member(platform_id, gid, uid)
        if ok:
            self.storage.remove_member(gid, uid)
            meta = self.storage.get_group_meta(gid)
            self.storage.set_group_meta(gid, "member_count", max(0, int(meta.get("member_count") or 1) - 1))
            await self.storage.flush()
            notice = self.notifier.build_kick_notice_chain(uid, username, days, reason, decision_desc)
            await self.notifier.send_group_chain(platform_id, gid, notice)
            logger.info(f"[lurker_watcher] 已将 {username}({uid}) 移出群 {gid}｜{decision_desc}｜{reason}")
            return True

        # 3. 失败处理：通常是机器人没有群管理员权限
        fails = int(rec.get("kick_fails") or 0) + 1
        self.storage.set_member_fields(gid, uid, kick_fails=fails)
        logger.error(f"[lurker_watcher] 移出群 {gid} 成员 {uid} 失败（第 {fails} 次）：{err}")
        if fails >= KICK_MAX_FAILS:
            self.storage.remove_member(gid, uid)
            logger.warning(
                f"[lurker_watcher] 成员 {uid} 连续 {fails} 次移出失败，已停止跟踪；"
                f"请确认机器人在群 {gid} 中拥有管理员权限"
            )
        return False

    # ==================================================================
    # 每日报告
    # ==================================================================
    async def _daily_report_job(self):
        """每 60 秒检查一次：某群到达报告时间且今天未发过，则发送日报。

        判定规则：
        * 今天已发过（last_report_date == 今天）-> 跳过；
        * 今天已尝试过且失败（30 分钟内）-> 跳过，避免每分钟重试刷爆协议端；
        * 今天的目标时刻已过：
            - 有过历史报告（last_report_date 存在）：允许补发（覆盖机器人重启
              错过报告时间的场景）；
            - 全新安装（从未发过）：仅当处于 [目标时刻, 目标时刻+30分钟] 窗口内
              才发送，避免「刚装好插件就收到一份过时日报」的突兀体验。
        """
        now = datetime.now()
        today = now.strftime("%Y-%m-%d")
        now_ts = time.time()
        for gid, info in list(self.storage.list_groups().items()):
            if not self._is_monitored(gid):
                continue
            hhmm = self.cfg.get_group("daily_report_time", gid)
            target = datetime.strptime(f"{today} {hhmm}", "%Y-%m-%d %H:%M")
            meta = self.storage.get_group_meta(gid)
            if meta.get("last_report_date") == today:
                continue  # 今天已发过
            # 30 分钟内失败过则不再重试（last_report_attempt_ts 为时间戳）
            if now_ts - float(meta.get("last_report_attempt_ts") or 0) < 1800:
                continue
            if now < target:
                continue  # 还没到点
            if not meta.get("last_report_date") and now > target + timedelta(minutes=30):
                continue  # 全新安装且已错过窗口：等明天
            self.storage.set_group_meta(gid, "last_report_attempt_ts", now_ts)
            ok = await self._send_report(gid, info, title="群潜水监测日报")
            if ok:
                self.storage.set_group_meta(gid, "last_report_date", today)
            else:
                logger.error(f"[lurker_watcher] 群 {gid} 日报发送失败，30 分钟后自动重试")

    async def _send_report(self, gid, info: dict, title: str) -> bool:
        """构建并发送某群的监测报告。"""
        members = self.storage.get_members(gid)
        if not members:
            logger.info(f"[lurker_watcher] 群 {gid} 暂无成员数据，跳过报告")
            return False
        text = self.notifier.build_report(
            title=title,
            group_name=self.storage.get_group_name(gid),
            members=members,
            whitelist=self.cfg.get_whitelist(gid),
            threshold=int(self.cfg.get_group("threshold_days", gid)),
            warning_days=int(self.cfg.get_group("warning_days", gid)),
            top_n=int(self.cfg.get_group("report_top_n", gid)),
        )
        ok = await self.notifier.send_group_text(info.get("platform_id", ""), gid, text)
        if not ok:
            logger.error(f"[lurker_watcher] 群 {gid} 报告发送失败")
        return ok

    # ==================================================================
    # 工具函数
    # ==================================================================
    def _is_monitored(self, gid) -> bool:
        """群是否在 WebUI 配置的监控范围内（groups_to_monitor 为空表示全部）。"""
        monitor = self.cfg.get_monitor_groups()
        return (not monitor) or (str(gid) in monitor)

    def _resolve_target_group(self, event: AstrMessageEvent, arg: str):
        """解析指令的目标群号。

        返回 (群号, 错误提示)。arg 优先（私聊查群/跨群操作）；
        未携带 arg 时取当前群；两处都没有则返回错误提示。
        """
        arg = str(arg or "").strip()
        if arg:
            if not arg.isdigit():
                return "", "❌ 群号必须是纯数字，例如：/lurker list 123456789"
            current = str(event.get_group_id() or "").strip()
            if not event.is_admin() and arg != current:
                return "", "🚫 查询其他群需要 AstrBot 管理员权限"
            if not self.storage.has_group(arg):
                return "", f"❌ 群 {arg} 尚未被监控或未初始化，请先执行 /lurker init {arg}"
            return arg, ""
        gid = str(event.get_group_id() or "").strip()
        if gid:
            if not self.storage.has_group(gid):
                return gid, f"❌ 本群尚未初始化，请管理员执行 /lurker init"
            return gid, ""
        return "", "❌ 请携带群号使用，例如：/lurker list 123456789"

    @staticmethod
    def _extract_target_user_id(event: AstrMessageEvent, target: str) -> str:
        """从指令参数或消息的 At 组件中提取目标用户 QQ 号。

        支持三种写法：/lurker whitelist add 123456、
        /lurker whitelist add @某人（At 组件）、/lurker whitelist add @123456。
        """
        target = str(target or "").strip()
        if target.isdigit():
            return target
        digits = re.sub(r"\D", "", target)
        if digits:
            return digits
        # 回退：从消息组件里找 At
        try:
            for seg in event.message_obj.message:
                qq = getattr(seg, "qq", None)
                if qq is not None and str(qq).isdigit():
                    return str(qq)
        except Exception:
            pass
        return ""

    # ==================================================================
    # 指令注册（指令组：/lurker）
    # ==================================================================
    @filter.command_group("lurker")
    async def lurker(self, event: AstrMessageEvent):
        """潜水监测指令组（单独发送 /lurker 可查看全部子指令）"""
        # 注意：指令组的根函数不会被执行 —— 用户只输入 /lurker 时，
        # AstrBot 会自动抛出带指令树的参数不足提示（见 CommandGroupFilter）。

    @lurker.command("list")
    async def lurker_list(self, event: AstrMessageEvent, group_id: str = ""):
        """查看群潜水排行榜（可附带群号跨群查询，跨群需管理员）"""
        gid, err = self._resolve_target_group(event, group_id)
        if err:
            yield event.plain_result(err)
            return
        info = self.storage.list_groups().get(gid, {})
        members = self.storage.get_members(gid)
        if not members:
            yield event.plain_result(f"❌ 群 {gid} 暂无成员数据，请先初始化（/lurker init）")
            return
        text = self.notifier.build_report(
            title="潜水排行快照",
            group_name=self.storage.get_group_name(gid) or info.get("group_name", ""),
            members=members,
            whitelist=self.cfg.get_whitelist(gid),
            threshold=int(self.cfg.get_group("threshold_days", gid)),
            warning_days=int(self.cfg.get_group("warning_days", gid)),
            top_n=int(self.cfg.get_group("report_top_n", gid)),
        )
        yield event.plain_result(text)

    @lurker.command("set_threshold")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def lurker_set_threshold(self, event: AstrMessageEvent, days: str = "", group_id: str = ""):
        """设置潜水天数阈值（本群生效；可附带群号为指定群设置，写入群独立配置）"""
        # 纵深防御：除框架级 permission_type 过滤外，handler 内部再校验一次
        if not event.is_admin():
            yield event.plain_result("🚫 该指令需要 AstrBot 管理员权限")
            return
        days = days.strip()
        if not days.isdigit() or int(days) <= 0 or int(days) > 3650:
            yield event.plain_result("❌ 用法：/lurker set_threshold <天数>（1～3650 的整数）")
            return
        gid, err = self._resolve_target_group(event, group_id)
        if err:
            yield event.plain_result(err)
            return
        warning = int(self.cfg.get_group("warning_days", gid))
        if int(days) <= warning:
            yield event.plain_result(
                f"❌ 阈值必须大于预警天数（当前预警 {warning} 天），请先调整 set_warning"
            )
            return
        old = self.cfg.get_group("threshold_days", gid)
        await self.storage.set_group_config(gid, "threshold_days", int(days))
        yield event.plain_result(
            f"✅ 群 {gid} 潜水阈值：{old} 天 → {int(days)} 天"
            f"（预警线 {int(days) - warning} 天，已写入该群的独立配置）"
        )

    @lurker.command("set_warning")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def lurker_set_warning(self, event: AstrMessageEvent, days: str = "", group_id: str = ""):
        """设置提前预警天数（本群生效；可附带群号为指定群设置）"""
        if not event.is_admin():
            yield event.plain_result("🚫 该指令需要 AstrBot 管理员权限")
            return
        days = days.strip()
        if not days.isdigit() or int(days) > 365:
            yield event.plain_result("❌ 用法：/lurker set_warning <天数>（0～365 的整数）")
            return
        gid, err = self._resolve_target_group(event, group_id)
        if err:
            yield event.plain_result(err)
            return
        threshold = int(self.cfg.get_group("threshold_days", gid))
        if int(days) >= threshold:
            yield event.plain_result(
                f"❌ 预警天数必须小于阈值（当前阈值 {threshold} 天）"
            )
            return
        old = self.cfg.get_group("warning_days", gid)
        await self.storage.set_group_config(gid, "warning_days", int(days))
        yield event.plain_result(
            f"✅ 群 {gid} 提前预警：{old} 天 → {int(days)} 天"
            f"（阈值 {threshold} 天，潜水满 {threshold - int(days)} 天开始 @ 警告）"
        )

    @lurker.command("whitelist")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def lurker_whitelist(
        self, event: AstrMessageEvent, action: str = "", target: str = ""
    ):
        """群级白名单管理：add/remove <@用户|QQ号> 或 show 查看当前名单"""
        if not event.is_admin():
            yield event.plain_result("🚫 该指令需要 AstrBot 管理员权限")
            return
        gid = str(event.get_group_id() or "").strip()
        if not gid:
            yield event.plain_result("❌ 请在群聊中使用本指令（白名单按群独立维护）")
            return
        action = action.strip().lower()
        group_cfg = self.storage.get_group_config(gid)
        wl = [str(x) for x in group_cfg.get("whitelist", []) or []]

        if action == "show":
            if not wl:
                yield event.plain_result(
                    f"ℹ️ 群 {gid} 暂无群级白名单（全局白名单 {len(self.cfg.get_global('whitelist'))} 人见 WebUI 配置）"
                )
                return
            lines = [f"⭐ 群 {gid} 白名单（{len(wl)} 人）："]
            for i, uid in enumerate(wl, 1):
                rec = self.storage.get_member(gid, uid)
                name = rec.get("username", "") if rec else ""
                lines.append(f"{i}. {name}（{uid}）")
            yield event.plain_result("\n".join(lines))
            return

        if action in ("add", "remove", "del", "rm"):
            uid = self._extract_target_user_id(event, target)
            if not uid:
                yield event.plain_result("❌ 用法：/lurker whitelist add|remove <@用户 或 QQ号>")
                return
            if action == "add":
                if uid in wl:
                    yield event.plain_result(f"ℹ️ 用户 {uid} 已在群 {gid} 白名单中")
                    return
                wl.append(uid)
                await self.storage.set_group_config(gid, "whitelist", wl)
                yield event.plain_result(f"✅ 已将 {uid} 加入群 {gid} 白名单（不警告不踢，排行显示 ⭐）")
            else:
                if uid not in wl:
                    yield event.plain_result(f"ℹ️ 用户 {uid} 不在群 {gid} 白名单中")
                    return
                wl.remove(uid)
                await self.storage.set_group_config(gid, "whitelist", wl)
                yield event.plain_result(f"✅ 已将 {uid} 移出群 {gid} 白名单（恢复正常监控）")
            return

        yield event.plain_result(
            "❌ 用法：/lurker whitelist add|remove <@用户 或 QQ号>\n"
            "        /lurker whitelist show"
        )

    @lurker.command("report")
    async def lurker_report(self, event: AstrMessageEvent, group_id: str = ""):
        """手动发送当前群的潜水监测报告（可附带群号）"""
        gid, err = self._resolve_target_group(event, group_id)
        if err:
            yield event.plain_result(err)
            return
        info = self.storage.list_groups().get(gid, {})
        ok = await self._send_report(gid, info, title="潜水监测报告（手动）")
        if not ok:
            yield event.plain_result("❌ 报告发送失败，请检查机器人与协议端连接状态")
        else:
            if not event.get_group_id():  # 私聊触发时反馈一下
                yield event.plain_result(f"✅ 已向群 {gid} 发送监测报告")

    @lurker.command("init")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def lurker_init(self, event: AstrMessageEvent, group_id: str = ""):
        """重新拉取群成员并初始化（附带群号则只初始化该群，否则刷新所有受监控群）"""
        if not event.is_admin():
            yield event.plain_result("🚫 该指令需要 AstrBot 管理员权限")
            return
        arg = str(group_id or "").strip()
        if arg:
            if not arg.isdigit():
                yield event.plain_result("❌ 群号必须是纯数字")
                return
            groups = await self.fetcher.get_group_list()
            info = groups.get(arg)
            if info is None:
                yield event.plain_result(f"❌ 机器人未加入群 {arg}，无法初始化")
                return
            ok = await self._init_group(arg, info)
            yield event.plain_result(
                f"{'✅' if ok else '❌'} 群 {arg} 初始化{'完成，共 ' + str(len(self.storage.get_members(arg))) + ' 名成员' if ok else '失败，请检查协议端日志'}"
            )
            return

        gid = str(event.get_group_id() or "").strip()
        if gid:
            groups = await self.fetcher.get_group_list()
            info = groups.get(gid) or {"platform_id": self.storage.get_group_platform(gid), "group_name": self.storage.get_group_name(gid)}
            ok = await self._init_group(gid, info)
            yield event.plain_result(
                f"{'✅' if ok else '❌'} 本群重新初始化{'完成，共 ' + str(len(self.storage.get_members(gid))) + ' 名成员' if ok else '失败'}"
            )
            return

        # 无参数：刷新所有受监控群
        groups = await self.fetcher.get_group_list()
        if not groups:
            yield event.plain_result("❌ 未能获取群列表，请确认协议端已连接")
            return
        monitor = set(self.cfg.get_monitor_groups())
        done = 0
        total = 0
        for g, info in groups.items():
            if monitor and g not in monitor:
                continue
            if await self._init_group(g, info):
                done += 1
                total += len(self.storage.get_members(g))
        yield event.plain_result(f"✅ 已重新初始化 {done} 个群，共 {total} 名成员纳入监控")

    # ==================================================================
    # LLM 工具：decide_kick
    # ==================================================================
    @filter.llm_tool(name="decide_kick")
    async def decide_kick(
        self,
        event: AstrMessageEvent,
        group_id: str,
        user_id: str,
        action: str,
        reason: str,
    ):
        """潜水成员踢人裁决工具。当管理员在对话中要求评估/处置某位潜水成员时调用，执行最终裁决。

        Args:
            group_id(string): 目标群号（纯数字）
            user_id(string): 目标成员 QQ 号（纯数字）
            action(string): 裁决结果，只能填 "kick"（移出群聊）或 "keep"（保留）
            reason(string): 简短的裁决理由，会被公示到群里
        """
        if self.storage is None or self.cfg is None:
            return "插件尚未初始化完成，请稍后再试。"
        # 安全兜底：会话内触发的裁决必须来自 AstrBot 管理员，防止普通成员借 LLM 踢人
        if not event.is_admin():
            return "权限不足：只有 AstrBot 管理员可以执行踢人裁决。"
        gid = str(group_id or "").strip()
        uid = str(user_id or "").strip()
        act = str(action or "").strip().lower()
        why = str(reason or "").strip() or "管理员通过 LLM 裁决移出"
        if not gid.isdigit() or not uid.isdigit():
            return "参数错误：group_id 与 user_id 必须是纯数字群号/QQ号。"
        if act not in ("kick", "keep"):
            return '参数错误：action 只能是 "kick" 或 "keep"。'
        if not self.storage.has_group(gid):
            return f"群 {gid} 未被本插件监控，无法执行裁决。"
        rec = self.storage.get_member(gid, uid)
        if rec is None:
            return f"群 {gid} 中未找到成员 {uid} 的监控记录。"

        if act == "keep":
            self.storage.set_member_fields(gid, uid, evaluated_at=time.time())
            await self.storage.flush()
            logger.info(f"[lurker_watcher] 管理员 LLM 裁决保留群 {gid} 成员 {uid}：{why}")
            return f"已保留成员 {uid}，理由：{why}"

        days = max(0.0, (time.time() - float(rec.get("last_message_time") or time.time())) / DAY_SECONDS)
        info = self.storage.list_groups().get(gid, {})
        ok = await self._execute_kick(gid, info, uid, rec, days, why, "LLM 智能裁决")
        if ok:
            return f"已将 {rec.get('username', '')}({uid}) 移出群 {gid}，理由：{why}"
        return f"移出失败：请确认机器人在群 {gid} 中具有管理员权限，详见日志。"
