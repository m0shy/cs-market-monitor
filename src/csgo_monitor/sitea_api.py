# 站点适配器已在开源版中移除。
# 本文件为接口桩：保留原有可 import 的公开名字，任何实际调用/实例化都会抛 NotImplementedError。
"""站点适配器已在开源版中移除。

开源框架版仅保留模块边界，原站点抓取实现不再随仓库分发。
所有占位名在被调用/实例化时抛 :class:`NotImplementedError`。
"""


class _RemovedStub:
    """占位基类：实例化、调用或属性访问均抛 NotImplementedError。"""

    _NAME = "removed-stub"

    def __init__(self, *args, **kwargs):
        raise NotImplementedError(f"站点适配器已在开源版中移除: {self._NAME}")

    def __call__(self, *args, **kwargs):
        raise NotImplementedError(f"站点适配器已在开源版中移除: {self._NAME}")

    def __getattr__(self, item):
        raise NotImplementedError(f"站点适配器已在开源版中移除: {self._NAME}.{item}")

    def __repr__(self):
        return f"<站点适配器已在开源版中移除: {self._NAME}>"


def _stub_class(name):
    return type(name, (_RemovedStub,), {"_NAME": name})


def _stub_func(name):
    def _placeholder(*args, **kwargs):
        raise NotImplementedError(f"站点适配器已在开源版中移除: {name}")

    _placeholder.__name__ = name
    _placeholder.__qualname__ = name
    return _placeholder


def _stub_const(name):
    value = _RemovedStub.__new__(_RemovedStub)
    value._NAME = name
    return value

SiteAApiCollector = _stub_class('SiteAApiCollector')

__all__ = ['SiteAApiCollector']
