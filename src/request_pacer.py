"""
Request Pacer - 单账号请求节流与随机抖动（防封禁）

在每次向 Google API 发送聊天请求前，确保与上一次请求保持最小间隔，
并加上随机的抖动延迟，将请求节奏抹平滑，降低被识别为自动化突发流量的风险。

配置项可在控制面板设置并热更新。
"""

import asyncio
import random
import time

from config import (
    get_request_throttle_enabled,
    get_request_min_interval,
    get_request_jitter,
)
from log import log


class RequestPacer:
    """单账号请求节流器（最小间隔 + 随机抖动）"""

    def __init__(self):
        self._lock = asyncio.Lock()
        self._enabled = False
        self._min_interval = 0.0
        self._jitter = 0.0
        self._last_sent = None
        self._refresh_interval = 5.0
        self._last_refresh = 0.0

    async def refresh_config(self):
        """从配置系统读取最新配置（支持控制面板热更新）"""
        try:
            enabled = await get_request_throttle_enabled()
            min_interval = await get_request_min_interval()
            jitter = await get_request_jitter()
        except Exception as e:
            log.debug(f"[REQUEST_PACER] 读取配置失败: {e}")
            return

        self._enabled = bool(enabled)
        self._min_interval = max(0.0, float(min_interval))
        self._jitter = max(0.0, float(jitter))
        self._last_refresh = time.monotonic()

    async def _maybe_refresh(self):
        now = time.monotonic()
        if now - self._last_refresh >= self._refresh_interval:
            await self.refresh_config()

    async def pace(self):
        """在发送请求前调用，确保与上一次请求保持最小间隔并加入随机抖动。"""
        await self._maybe_refresh()

        if not self._enabled:
            return

        # 加锁让并发请求串行排队，保证请求间隔真正生效
        async with self._lock:
            now = time.monotonic()
            wait = 0.0
            if self._last_sent is not None:
                elapsed = now - self._last_sent
                if elapsed < self._min_interval:
                    wait = self._min_interval - elapsed
            wait += random.uniform(0.0, self._jitter)

            if wait > 0:
                last_delta = (now - self._last_sent) if self._last_sent else 0.0
                log.debug(
                    f"[REQUEST_PACER] 距上次发送 {last_delta:.2f}s，"
                    f"等待 {wait:.2f}s (最小间隔 {self._min_interval}s, 抖动 {self._jitter}s)"
                )
                await asyncio.sleep(wait)

            self._last_sent = time.monotonic()


# 全局节流器实例
request_pacer = RequestPacer()