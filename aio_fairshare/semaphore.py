"""
Fair-share semaphore implementation.
"""

from __future__ import annotations

import asyncio
import logging
from collections import deque
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger("aio_fairshare")

# Context variable for tenant identification
_tenant_id_var: ContextVar[str | None] = ContextVar("_tenant_id", default=None)


def set_tenant_id(tenant_id: str | None) -> None:
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


class TenantHasActiveSlotsError(RuntimeError):
    """Raised when trying to unregister a tenant with active slots."""

    pass


@dataclass(slots=True)
class TenantStats:
    """Statistics for a single tenant."""

    tenant_id: str
    active_slots: int
    share: int
    waiting: int


@dataclass(slots=True)
class SemaphoreStats:
    """Overall semaphore statistics."""

    max_slots: int
    active_tenants: int
    demanding_tenants: int
    total_active_slots: int
    available_slots: int
    tenants: dict[str, TenantStats] = field(default_factory=dict)


@dataclass(slots=True)
class _TenantState:
    """Internal state for a tenant."""

    active_slots: int = 0
    waiting: int = 0
    closing: bool = False  # Marked for deferred unregistration


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

    __slots__ = ("_semaphore", "_tenant_id")

    def __init__(self, semaphore: FairShareSemaphore, tenant_id: str) -> None:
        self._semaphore = semaphore
        self._tenant_id = tenant_id

    async def __aenter__(self) -> TenantContext:
        await self._semaphore.register_tenant(self._tenant_id)
        set_tenant_id(self._tenant_id)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        await self._semaphore.unregister_tenant(self._tenant_id)
        _tenant_id_var.set(None)

    @asynccontextmanager
    async def acquire(self) -> AsyncIterator[None]:
        """Acquire a slot for this tenant."""
        async with self._semaphore.acquire():
            yield


class _SlotAcquisition:
    """Internal context manager for slot acquisition."""

    __slots__ = ("_semaphore", "_tenant_id", "_acquired", "_waiting_registered")

    def __init__(self, semaphore: FairShareSemaphore, tenant_id: str) -> None:
        self._semaphore = semaphore
        self._tenant_id = tenant_id
        self._acquired = False
        self._waiting_registered = False

    async def __aenter__(self) -> _SlotAcquisition:
        sem = self._semaphore

        # Register as waiting
        async with sem._lock:
            if self._tenant_id not in sem._tenants:
                raise TenantNotRegisteredError(
                    f"Tenant '{self._tenant_id}' is not registered. "
                    "Use 'async with semaphore.tenant(id):' first."
                )
            tenant = sem._tenants[self._tenant_id]
            if tenant.closing:
                raise TenantNotRegisteredError(f"Tenant '{self._tenant_id}' is closing.")
            tenant.waiting += 1
            self._waiting_registered = True

        try:
            while True:
                async with sem._lock:
                    tenant_state = sem._tenants.get(self._tenant_id)

                    if tenant_state is None or tenant_state.closing:
                        raise TenantNotRegisteredError(
                            f"Tenant '{self._tenant_id}' was unregistered."
                        )

                    share = sem._calculate_share()

                    # Check if we can acquire
                    if tenant_state.active_slots < share and sem._available > 0:
                        sem._available -= 1
                        tenant_state.active_slots += 1
                        tenant_state.waiting = max(0, tenant_state.waiting - 1)
                        self._acquired = True
                        self._waiting_registered = False
                        return self

                    # Need to wait - add to queue
                    waiter: asyncio.Future[None] = asyncio.get_event_loop().create_future()
                    sem._waiters.append((self._tenant_id, waiter))

                # Wait for signal (outside lock)
                try:
                    await waiter
                except asyncio.CancelledError:
                    # Remove from waiters if still there
                    async with sem._lock:
                        sem._waiters = deque(
                            (tid, w) for tid, w in sem._waiters if w is not waiter
                        )
                    raise

        except (asyncio.CancelledError, Exception):
            # Rollback on any error
            async with sem._lock:
                tenant_state = sem._tenants.get(self._tenant_id)

                # Rollback waiting count if we registered but didn't acquire
                if self._waiting_registered and tenant_state:
                    tenant_state.waiting = max(0, tenant_state.waiting - 1)
                    self._waiting_registered = False

                # Rollback acquisition if it happened
                if self._acquired:
                    if tenant_state:
                        tenant_state.active_slots = max(0, tenant_state.active_slots - 1)
                    sem._available += 1
                    self._acquired = False
                    sem._notify_waiters()
            raise

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        if self._acquired:
            sem = self._semaphore
            async with sem._lock:
                tenant_state = sem._tenants.get(self._tenant_id)
                if tenant_state:
                    tenant_state.active_slots = max(0, tenant_state.active_slots - 1)

                    # Check for deferred unregistration
                    if (
                        tenant_state.closing
                        and tenant_state.active_slots == 0
                        and tenant_state.waiting == 0
                    ):
                        del sem._tenants[self._tenant_id]
                        logger.debug(f"Deferred unregister of tenant '{self._tenant_id}'")

                sem._available += 1
                sem._notify_waiters()


class FairShareSemaphore:
    """
    Async semaphore that fairly distributes slots among active tenants.

    Args:
        max_slots: Maximum number of concurrent slots (e.g., browser tabs).
        min_share: Minimum slots guaranteed per tenant (default: 1).
        share_calculator: Optional custom function to calculate per-tenant share.
            Receives (max_slots, num_demanding_tenants) and returns share per tenant.

    Example:
        semaphore = FairShareSemaphore(max_slots=10)

        async with semaphore.tenant("request-123"):
            async with semaphore.acquire():
                # Do work with acquired slot
                await process_page()

    Fair sharing:
        - With 10 slots and 2 demanding tenants: each gets up to 5 slots
        - With 10 slots and 5 demanding tenants: each gets up to 2 slots
        - Shares are recalculated dynamically as tenants join/leave
        - Only "demanding" tenants (active_slots > 0 or waiting > 0) count
    """

    __slots__ = (
        "_max_slots",
        "_min_share",
        "_share_calculator",
        "_lock",
        "_available",
        "_tenants",
        "_waiters",
    )

    def __init__(
        self,
        max_slots: int,
        *,
        min_share: int = 1,
        share_calculator: Callable[[int, int], int] | None = None,
    ) -> None:
        if max_slots < 1:
            raise ValueError("max_slots must be at least 1")
        if min_share < 1:
            raise ValueError("min_share must be at least 1")

        self._max_slots = max_slots
        self._min_share = min_share
        self._share_calculator = share_calculator

        self._lock = asyncio.Lock()
        self._available = max_slots
        self._tenants: dict[str, _TenantState] = {}
        self._waiters: deque[tuple[str, asyncio.Future[None]]] = deque()

    @property
    def max_slots(self) -> int:
        """Maximum number of slots."""
        return self._max_slots

    def _get_demanding_tenants_count(self) -> int:
        """Count tenants that are actively using or waiting for slots."""
        return sum(
            1
            for t in self._tenants.values()
            if (t.active_slots > 0 or t.waiting > 0) and not t.closing
        )

    def _calculate_share(self) -> int:
        """Calculate fair share per tenant based on demanding tenants."""
        num_demanding = self._get_demanding_tenants_count()
        if num_demanding == 0:
            return self._max_slots

        if self._share_calculator:
            return max(
                self._min_share, self._share_calculator(self._max_slots, num_demanding)
            )

        return max(self._min_share, self._max_slots // num_demanding)

    def _notify_waiters(self) -> None:
        """Wake up waiters in FIFO order, respecting fair share."""
        if not self._waiters or self._available == 0:
            return

        share = self._calculate_share()
        to_remove: list[tuple[str, asyncio.Future[None]]] = []

        for tenant_id, waiter in self._waiters:
            if waiter.done():
                to_remove.append((tenant_id, waiter))
                continue

            tenant_state = self._tenants.get(tenant_id)
            if tenant_state is None or tenant_state.closing:
                waiter.cancel()
                to_remove.append((tenant_id, waiter))
                continue

            # Check if tenant can acquire based on fair share
            # Only wake up as many as we have available slots
            if tenant_state.active_slots < share and self._available > 0:
                waiter.set_result(None)
                to_remove.append((tenant_id, waiter))
                # Break after waking one waiter per available slot
                # This prevents thundering herd
                break

        # Remove processed waiters
        for item in to_remove:
            try:
                self._waiters.remove(item)
            except ValueError:
                pass

    async def register_tenant(self, tenant_id: str) -> None:
        """Register a tenant to participate in fair sharing."""
        async with self._lock:
            if tenant_id not in self._tenants:
                self._tenants[tenant_id] = _TenantState()
                logger.debug(
                    f"Registered tenant '{tenant_id}', total: {len(self._tenants)}"
                )
            elif self._tenants[tenant_id].closing:
                # Re-register a closing tenant
                self._tenants[tenant_id].closing = False
                logger.debug(f"Re-registered closing tenant '{tenant_id}'")

    async def unregister_tenant(self, tenant_id: str, force: bool = False) -> None:
        """
        Unregister a tenant.

        Args:
            tenant_id: The tenant to unregister.
            force: If True, immediately unregister even with active slots (leaks slots).
                   If False (default), uses deferred unregistration.

        Raises:
            TenantHasActiveSlotsError: If force=False and tenant has active slots,
                                       and deferred mode is not possible.
        """
        async with self._lock:
            tenant_state = self._tenants.get(tenant_id)
            if tenant_state is None:
                return

            if tenant_state.active_slots == 0 and tenant_state.waiting == 0:
                # Safe to unregister immediately
                del self._tenants[tenant_id]
                logger.debug(
                    f"Unregistered tenant '{tenant_id}', remaining: {len(self._tenants)}"
                )
            elif force:
                # Force unregister - WARNING: leaks slots
                logger.warning(
                    f"Force unregistering tenant '{tenant_id}' with "
                    f"{tenant_state.active_slots} active slots - slots will be leaked!"
                )
                del self._tenants[tenant_id]
            else:
                # Deferred unregistration
                tenant_state.closing = True
                logger.debug(
                    f"Marked tenant '{tenant_id}' for deferred unregistration "
                    f"(active: {tenant_state.active_slots}, waiting: {tenant_state.waiting})"
                )

            # Recalculate shares and notify waiters
            self._notify_waiters()

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
        """Get current semaphore statistics (thread-safe)."""
        async with self._lock:
            demanding = self._get_demanding_tenants_count()
            share = self._calculate_share()

            tenant_stats = {
                tid: TenantStats(
                    tenant_id=tid,
                    active_slots=state.active_slots,
                    share=share,
                    waiting=state.waiting,
                )
                for tid, state in self._tenants.items()
                if not state.closing
            }

            return SemaphoreStats(
                max_slots=self._max_slots,
                active_tenants=len(
                    [t for t in self._tenants.values() if not t.closing]
                ),
                demanding_tenants=demanding,
                total_active_slots=self._max_slots - self._available,
                available_slots=self._available,
                tenants=tenant_stats,
            )

    def _stats_sync_unsafe(self) -> dict[str, Any]:
        """
        Get current statistics synchronously (NOT thread-safe).

        WARNING: This method reads state without locking and may return
        inconsistent data. Use only for debugging/logging where approximate
        values are acceptable. Prefer `await stats()` for accurate data.
        """
        demanding = sum(
            1
            for t in self._tenants.values()
            if (t.active_slots > 0 or t.waiting > 0) and not t.closing
        )
        share = self._calculate_share() if demanding > 0 else self._max_slots

        return {
            "max_slots": self._max_slots,
            "active_tenants": len(
                [t for t in self._tenants.values() if not t.closing]
            ),
            "demanding_tenants": demanding,
            "available_slots": self._available,
            "share_per_tenant": share,
            "tenants": {
                tid: {
                    "active": state.active_slots,
                    "waiting": state.waiting,
                    "closing": state.closing,
                }
                for tid, state in self._tenants.items()
            },
        }
