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

BrowserDiscovery = _stub_class('BrowserDiscovery')
DiscoveryResult = _stub_class('DiscoveryResult')
SiteABrowserHistory = _stub_class('SiteABrowserHistory')
SiteBBrowserHistory = _stub_class('SiteBBrowserHistory')
collect_siteb_catalog_from_sitemap = _stub_func('collect_siteb_catalog_from_sitemap')
collect_sitea_catalog = _stub_func('collect_sitea_catalog')
collect_siteb_catalog = _stub_func('collect_siteb_catalog')
recover_sitea_catalog = _stub_func('recover_sitea_catalog')
reconcile_siteb_quota_failures = _stub_func('reconcile_siteb_quota_failures')
siteb_block_reason = _stub_func('siteb_block_reason')
_allowed_page = _stub_func('_allowed_page')
_canonical_page_url = _stub_func('_canonical_page_url')
_fetch_sitemap_urls = _stub_func('_fetch_sitemap_urls')
_network_navigation_error = _stub_func('_network_navigation_error')
_stealth_init_scripts = _stub_func('_stealth_init_scripts')
_stealth_context = _stub_func('_stealth_context')
_read_body = _stub_func('_read_body')

__all__ = ['BrowserDiscovery', 'DiscoveryResult', 'SiteABrowserHistory', 'SiteBBrowserHistory', 'collect_siteb_catalog_from_sitemap', 'collect_sitea_catalog', 'collect_siteb_catalog', 'recover_sitea_catalog', 'reconcile_siteb_quota_failures', 'siteb_block_reason', '_allowed_page', '_canonical_page_url', '_fetch_sitemap_urls', '_network_navigation_error', '_stealth_init_scripts', '_stealth_context', '_read_body']
