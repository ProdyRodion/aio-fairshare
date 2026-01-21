"""
Fair-share semaphore implementation.
"""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import AsyncIterator, Callable

logger = logging.getLogger("aio_fairshare")

# Context variable for tenant identification
_tenant_id_var: ContextVar[str | None] = ContextVar("_tenant_id", default=None)


def set_tenant_id(tenant_id: str) -> None:
    """Set the current tenant ID in the context."""
    _tenant_id_var.set(tenant_id)


def get_tenant_id() -> str | None:
    """Get the current tenant ID from the context."""
    return _tenant_id_var.get()


@asynccontextmanager
async def tenant_context(tenant_id: str) -> AsyncIterator[None]:
    """Context manager to set tenant ID for a block of code."""
    token = _tenant_id_var.set(tenant_id)
    try:
        yield
    finally:
        _tenant_id_var.reset(token)


class TenantNotRegisteredError(RuntimeError):
    """Raised when trying to acquire without a registered tenant."""
    pass


@dataclass
class TenantStats:
    """Statistics for a single tenant."""
    tenant_id: str
    active_slots: int
    share: int
    waiting: int


@dataclass
class SemaphoreStats:
    """Overall semaphore statistics."""
    max_slots: int
    active_tenants: int
    total_active_slots: int
    available_slots: int
    tenants: dict[str, TenantStats] = field(default_factory=dict)


class TenantContext:
    """
    Manages a tenant's lifecycle within the semaphore.
    
    Use as async context manager:
        async with semaphore.tenant("request-123"):
            # tenant is registered
            async with semaphore.acquire():
                # slot acquired
                ...
    """
    
    def __init__(self, semaphore: FairShareSemaphore, tenant_id: str):
        self._semaphore = semaphore
        self._tenant_id = tenant_id
    
    async def __aenter__(self) -> TenantContext:
        await self._semaphore.register_tenant(self._tenant_id)
        set_tenant_id(self._tenant_id)
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self._semaphore.unregister_tenant(self._tenant_id)
        _tenant_id_var.set(None)
    
    @asynccontextmanager
    async def acquire(self) -> AsyncIterator[None]:
        """Acquire a slot for this tenant."""
        async with self._semaphore.acquire():
            yield


class _SlotAcquisition:
    """Internal context manager for slot acquisition."""
    
    def __init__(self, semaphore: FairShareSemaphore, tenant_id: str):
        self._semaphore = semaphore
        self._tenant_id = tenant_id
        self._acquired = False
    
    async def __aenter__(self) -> _SlotAcquisition:
        sem = self._semaphore
        
        # Increment waiting count
        async with sem._lock:
            if self._tenant_id not in sem._tenants:
                raise TenantNotRegisteredError(
                    f"Tenant '{self._tenant_id}' is not registered. "
                    "Use 'async with semaphore.tenant(id):' first."
                )
            sem._waiting[self._tenant_id] = sem._waiting.get(self._tenant_id, 0) + 1
        
        try:
            while True:
                # Check if we can proceed based on fair share
                async with sem._lock:
                    share = sem._calculate_share()
                    used = sem._tenants.get(self._tenant_id, 0)
                    
                    if used < share:
                        # Try to acquire global semaphore without blocking
                        if sem._available > 0:
                            sem._available -= 1
                            sem._tenants[self._tenant_id] = used + 1
                            self._acquired = True
                            return self
                
                # Wait a bit before retrying
                await asyncio.sleep(sem._poll_interval)
        finally:
            # Decrement waiting count
            async with sem._lock:
                sem._waiting[self._tenant_id] = max(
                    0, sem._waiting.get(self._tenant_id, 0) - 1
                )
    
    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        if self._acquired:
            sem = self._semaphore
            async with sem._lock:
                sem._tenants[self._tenant_id] = max(
                    0, sem._tenants.get(self._tenant_id, 0) - 1
                )
                sem._available += 1


class FairShareSemaphore:
    """
    Async semaphore that fairly distributes slots among active tenants.
    
    Args:
        max_slots: Maximum number of concurrent slots (e.g., browser tabs).
        min_share: Minimum slots guaranteed per tenant (default: 1).
        poll_interval: Seconds between acquisition attempts (default: 0.01).
        share_calculator: Optional custom function to calculate per-tenant share.
            Receives (max_slots, num_tenants) and returns share per tenant.
    
    Example:
        semaphore = FairShareSemaphore(max_slots=10)
        
        async with semaphore.tenant("request-123"):
            async with semaphore.acquire():
                # Do work with acquired slot
                await process_page()
    
    Fair sharing:
        - With 10 slots and 2 tenants: each gets up to 5 slots
        - With 10 slots and 5 tenants: each gets up to 2 slots
        - Shares are recalculated dynamically as tenants join/leave
    """
    
    def __init__(
        self,
        max_slots: int,
        *,
        min_share: int = 1,
        poll_interval: float = 0.01,
        share_calculator: Callable[[int, int], int] | None = None,
    ):
        if max_slots < 1:
            raise ValueError("max_slots must be at least 1")
        if min_share < 1:
            raise ValueError("min_share must be at least 1")
        
        self._max_slots = max_slots
        self._min_share = min_share
        self._poll_interval = poll_interval
        self._share_calculator = share_calculator
        
        self._lock = asyncio.Lock()
        self._available = max_slots
        self._tenants: dict[str, int] = {}  # tenant_id -> active slots
        self._waiting: dict[str, int] = {}  # tenant_id -> waiting count
    
    @property
    def max_slots(self) -> int:
        """Maximum number of slots."""
        return self._max_slots
    
    def _calculate_share(self) -> int:
        """Calculate fair share per tenant."""
        num_tenants = max(1, len(self._tenants))
        
        if self._share_calculator:
            return max(self._min_share, self._share_calculator(self._max_slots, num_tenants))
        
        return max(self._min_share, self._max_slots // num_tenants)
    
    async def register_tenant(self, tenant_id: str) -> None:
        """Register a tenant to participate in fair sharing."""
        async with self._lock:
            if tenant_id not in self._tenants:
                self._tenants[tenant_id] = 0
                self._waiting[tenant_id] = 0
                logger.debug(f"Registered tenant '{tenant_id}', total: {len(self._tenants)}")
    
    async def unregister_tenant(self, tenant_id: str) -> None:
        """
        Unregister a tenant.
        
        Note: Should only be called when tenant has released all slots.
        """
        async with self._lock:
            active = self._tenants.get(tenant_id, 0)
            if active > 0:
                logger.warning(
                    f"Unregistering tenant '{tenant_id}' with {active} active slots. "
                    "Slots will be released."
                )
                self._available += active
            
            self._tenants.pop(tenant_id, None)
            self._waiting.pop(tenant_id, None)
            logger.debug(f"Unregistered tenant '{tenant_id}', remaining: {len(self._tenants)}")
    
    def tenant(self, tenant_id: str) -> TenantContext:
        """
        Create a tenant context.
        
        Usage:
            async with semaphore.tenant("request-123"):
                async with semaphore.acquire():
                    ...
        """
        return TenantContext(self, tenant_id)
    
    @asynccontextmanager
    async def acquire(self, tenant_id: str | None = None) -> AsyncIterator[None]:
        """
        Acquire a slot, respecting fair share limits.
        
        Args:
            tenant_id: Optional tenant ID. If not provided, uses context variable.
        
        Raises:
            TenantNotRegisteredError: If tenant is not registered.
        """
        tid = tenant_id or get_tenant_id()
        if not tid:
            raise TenantNotRegisteredError(
                "No tenant ID provided. Either pass tenant_id or use tenant_context()."
            )
        
        async with _SlotAcquisition(self, tid):
            yield
    
    async def stats(self) -> SemaphoreStats:
        """Get current semaphore statistics."""
        async with self._lock:
            share = self._calculate_share()
            tenant_stats = {
                tid: TenantStats(
                    tenant_id=tid,
                    active_slots=slots,
                    share=share,
                    waiting=self._waiting.get(tid, 0),
                )
                for tid, slots in self._tenants.items()
            }
            
            return SemaphoreStats(
                max_slots=self._max_slots,
                active_tenants=len(self._tenants),
                total_active_slots=self._max_slots - self._available,
                available_slots=self._available,
                tenants=tenant_stats,
            )
    
    def stats_sync(self) -> dict:
        """
        Get current statistics synchronously (for logging/debugging).
        
        Note: May not be 100% accurate due to lack of locking.
        """
        share = self._calculate_share() if self._tenants else self._max_slots
        return {
            "max_slots": self._max_slots,
            "active_tenants": len(self._tenants),
            "available_slots": self._available,
            "share_per_tenant": share,
            "tenants": {
                tid: {"active": slots, "waiting": self._waiting.get(tid, 0)}
                for tid, slots in self._tenants.items()
            },
        }
