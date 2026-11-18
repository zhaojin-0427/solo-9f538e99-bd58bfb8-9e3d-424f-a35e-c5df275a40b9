"""拉普拉斯噪声采样（逆变换采样）。

差分隐私机制说明：
- 相邻数据集定义为“增删一名患者”。一名患者同日只计一次（取最高修订号），
  因此相邻数据集间，东/西/南三区计数向量的 L1 敏感度为 1。
- 对三区真实计数分别叠加独立的 Laplace(0, 1/ε) 噪声，即满足 ε-差分隐私。
- 取整、截断为非负整数、以及“全市 = 三区公布值之和”均为后处理，
  不额外消耗隐私预算。
"""
from __future__ import annotations

import math
import random


class LaplaceSampler:
    """Laplace(0, scale) 采样器。默认使用系统熵源，各次采样相互独立。"""

    def __init__(self, rng: random.Random | None = None):
        self._rng = rng if rng is not None else random.SystemRandom()

    def sample(self, scale: float) -> float:
        if scale <= 0:
            raise ValueError("scale 必须为正数")
        # 逆变换采样：u ~ U(-1/2, 1/2)，X = -scale * sign(u) * ln(1 - 2|u|)
        while True:
            u = self._rng.random() - 0.5
            if abs(u) < 0.5:  # 排除 |u| = 0.5，保证对数有定义
                break
        return scale * math.copysign(math.log1p(-2.0 * abs(u)), u)
