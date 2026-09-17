"""Ingress bottleneck emulator for browser -> server traffic.

Models a single-queue link in front of the server: incoming UDP datagrams are
serialized at `rate_mbps`, wait in a FIFO while the link is busy, and are
dropped (tail drop) when their queueing delay would exceed `queue_ms`, plus
optional random loss. Used to reproduce "the browser's QUIC congestion control
is limiting us" locally, where loopback never congests.

The link is *shared by every connection*, like a real uplink: a second
connection from the same browser sees the same queue. That is what makes the
page's reference-probe comparison meaningful (a per-connection link would give
an idle second connection an uncongested path of its own).

Only incoming packets are shaped; server -> browser traffic is untouched.
"""

import asyncio
import random
from dataclasses import dataclass
from typing import Callable, Optional


@dataclass
class NetemConfig:
    rate_mbps: Optional[float] = None
    queue_ms: float = 50.0
    loss: float = 0.0

    @property
    def enabled(self) -> bool:
        return self.rate_mbps is not None or self.loss > 0

    def describe(self) -> str:
        if not self.enabled:
            return "off"
        rate = f"{self.rate_mbps} Mbps, queue {self.queue_ms} ms" if self.rate_mbps else "no rate limit"
        return f"{rate}, loss {self.loss:.2%}"


class SharedLink:
    """Serialization state of the emulated link, shared by all connections."""

    def __init__(self) -> None:
        self.free_at = 0.0
        self.passed = self.dropped_queue = self.dropped_random = 0


class IngressShaper:
    def __init__(
        self,
        config: NetemConfig,
        deliver: Callable[[bytes, object], None],
        link: Optional[SharedLink] = None,
    ) -> None:
        self.config = config
        self.deliver = deliver
        self.loop = asyncio.get_event_loop()
        self.link = link or SharedLink()

    def __call__(self, data: bytes, addr) -> None:
        cfg = self.config
        link = self.link
        if cfg.loss and random.random() < cfg.loss:
            link.dropped_random += 1
            return
        if not cfg.rate_mbps:
            link.passed += 1
            self.deliver(data, addr)
            return

        now = self.loop.time()
        start = max(now, link.free_at)
        if start - now > cfg.queue_ms / 1000:
            link.dropped_queue += 1
            return
        done = start + len(data) * 8 / (cfg.rate_mbps * 1e6)
        link.free_at = done
        link.passed += 1
        self.loop.call_at(done, self.deliver, data, addr)

    def stats(self) -> dict:
        return {
            "config": self.config.describe(),
            "shared_link": True,
            "passed": self.link.passed,
            "dropped_queue": self.link.dropped_queue,
            "dropped_random": self.link.dropped_random,
        }
