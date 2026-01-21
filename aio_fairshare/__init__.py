"""
aio-fairshare: Async fair-share semaphore for multi-tenant resource management.

Distributes a fixed pool of resources (e.g., browser tabs, connections, workers)
fairly among active tenants, dynamically adjusting each tenant's share as
tenants join or leave.
"""

from .semaphore import (
    FairShareSemaphore,
    TenantContext,
    set_tenant_id,
    get_tenant_id,
    tenant_context,
)

__version__ = "0.1.0"
__all__ = [
    "FairShareSemaphore",
    "TenantContext",
    "set_tenant_id",
    "get_tenant_id",
    "tenant_context",
]
