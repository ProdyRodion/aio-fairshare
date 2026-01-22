"""
Tests for aio-fairshare v0.2.0.
"""

import asyncio

import pytest

from aio_fairshare import (
    FairShareSemaphore,
    TenantNotRegisteredError,
    get_tenant_id,
    set_tenant_id,
    tenant_context,
)


class TestBasicFunctionality:
    """Test basic semaphore operations."""

    @pytest.mark.asyncio
    async def test_single_tenant_can_use_all_slots(self):
        """Single tenant should be able to use all available slots."""
        sem = FairShareSemaphore(max_slots=5)
        acquired = []

        async with sem.tenant("tenant-1"):

            async def acquire_slot(slot_id: int):
                async with sem.acquire():
                    acquired.append(slot_id)
                    await asyncio.sleep(0.05)

            await asyncio.gather(*[acquire_slot(i) for i in range(5)])

        assert len(acquired) == 5

    @pytest.mark.asyncio
    async def test_acquire_and_release(self):
        """Test basic acquire and release cycle."""
        sem = FairShareSemaphore(max_slots=2)

        async with sem.tenant("tenant-1"):
            stats_before = await sem.stats()
            assert stats_before.available_slots == 2

            async with sem.acquire():
                stats_during = await sem.stats()
                assert stats_during.available_slots == 1

            stats_after = await sem.stats()
            assert stats_after.available_slots == 2

    @pytest.mark.asyncio
    async def test_tenant_registration(self):
        """Test tenant registration and unregistration."""
        sem = FairShareSemaphore(max_slots=10)

        stats = await sem.stats()
        assert stats.active_tenants == 0

        async with sem.tenant("tenant-1"):
            stats = await sem.stats()
            assert stats.active_tenants == 1
            assert "tenant-1" in stats.tenants

        stats = await sem.stats()
        assert stats.active_tenants == 0


class TestFairSharing:
    """Test fair sharing between tenants."""

    @pytest.mark.asyncio
    async def test_two_tenants_share_equally(self):
        """Two demanding tenants should each get half the slots."""
        sem = FairShareSemaphore(max_slots=10)

        async with sem.tenant("tenant-1"), sem.tenant("tenant-2"):
            # Both tenants acquire to become "demanding"
            async with sem.acquire("tenant-1"), sem.acquire("tenant-2"):
                stats = await sem.stats()
                assert stats.demanding_tenants == 2
                assert stats.tenants["tenant-1"].share == 5
                assert stats.tenants["tenant-2"].share == 5

    @pytest.mark.asyncio
    async def test_share_based_on_demanding_tenants(self):
        """Share should be based on demanding tenants, not all registered."""
        sem = FairShareSemaphore(max_slots=10)

        async with sem.tenant("tenant-1"), sem.tenant("tenant-2"), sem.tenant("tenant-3"):
            # Only tenant-1 acquires (becomes demanding)
            async with sem.acquire("tenant-1"):
                stats = await sem.stats()
                assert stats.active_tenants == 3
                assert stats.demanding_tenants == 1
                # Share should be based on 1 demanding tenant
                assert stats.tenants["tenant-1"].share == 10

    @pytest.mark.asyncio
    async def test_waiting_tenant_counts_as_demanding(self):
        """Waiting tenants should count as demanding for share calculation."""
        sem = FairShareSemaphore(max_slots=2)
        started = asyncio.Event()

        async with sem.tenant("tenant-1"), sem.tenant("tenant-2"):
            # tenant-1 takes both slots
            async with sem.acquire("tenant-1"), sem.acquire("tenant-1"):
                # tenant-2 tries to acquire (will wait)
                async def waiting_task():
                    started.set()
                    async with sem.acquire("tenant-2"):
                        pass

                task = asyncio.create_task(waiting_task())
                await started.wait()
                await asyncio.sleep(0.05)  # Let it register as waiting

                stats = await sem.stats()
                assert stats.demanding_tenants == 2  # Both are demanding
                assert stats.tenants["tenant-2"].waiting == 1

            # Now tenant-2 can acquire
            await task

    @pytest.mark.asyncio
    async def test_share_adjusts_when_tenant_leaves(self):
        """Share should increase when tenant leaves."""
        sem = FairShareSemaphore(max_slots=10)

        async with sem.tenant("tenant-1"):
            async with sem.acquire("tenant-1"):
                async with sem.tenant("tenant-2"):
                    async with sem.acquire("tenant-2"):
                        stats = await sem.stats()
                        assert stats.tenants["tenant-1"].share == 5

                # tenant-2 left
                stats = await sem.stats()
                assert stats.tenants["tenant-1"].share == 10


class TestCancellation:
    """Test cancellation handling (Bug #1 fix)."""

    @pytest.mark.asyncio
    async def test_cancellation_during_acquire_no_slot_leak(self):
        """Cancellation during acquire should not leak slots."""
        sem = FairShareSemaphore(max_slots=2)

        async with sem.tenant("tenant-1"):
            # Take all slots
            async with sem.acquire(), sem.acquire():
                stats = await sem.stats()
                assert stats.available_slots == 0

                # Start a task that will wait
                async def waiting_acquire():
                    async with sem.acquire():
                        await asyncio.sleep(10)

                task = asyncio.create_task(waiting_acquire())
                await asyncio.sleep(0.05)  # Let it start waiting

                # Cancel the waiting task
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

            # After releasing, slots should be back
            stats = await sem.stats()
            assert stats.available_slots == 2  # No leak!

    @pytest.mark.asyncio
    async def test_cancellation_rollback_waiting_count(self):
        """Cancellation should rollback waiting count."""
        sem = FairShareSemaphore(max_slots=1)

        async with sem.tenant("tenant-1"):
            async with sem.acquire():

                async def waiting_acquire():
                    async with sem.acquire():
                        pass

                task = asyncio.create_task(waiting_acquire())
                await asyncio.sleep(0.05)

                stats = await sem.stats()
                assert stats.tenants["tenant-1"].waiting == 1

                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

                await asyncio.sleep(0.05)
                stats = await sem.stats()
                assert stats.tenants["tenant-1"].waiting == 0


class TestFIFOOrdering:
    """Test FIFO ordering (Bug #5 fix)."""

    @pytest.mark.asyncio
    async def test_waiters_served_in_fifo_order(self):
        """Waiters should be served in FIFO order."""
        sem = FairShareSemaphore(max_slots=1)
        order = []

        async with sem.tenant("t1"), sem.tenant("t2"), sem.tenant("t3"):
            async with sem.acquire("t1"):
                # Create waiting tasks in order
                async def acquire_and_record(tid: str, order_num: int):
                    async with sem.acquire(tid):
                        order.append(order_num)
                        await asyncio.sleep(0.01)

                tasks = []
                for i, tid in enumerate(["t2", "t3", "t1"], 1):
                    task = asyncio.create_task(acquire_and_record(tid, i))
                    tasks.append(task)
                    await asyncio.sleep(0.02)  # Ensure ordering

            await asyncio.gather(*tasks)

        # Should be served in order they started waiting
        assert order == [1, 2, 3]


class TestDeferredUnregistration:
    """Test deferred unregistration (Bug #4 fix)."""

    @pytest.mark.asyncio
    async def test_unregister_with_active_slots_deferred(self):
        """Unregistering tenant with active slots should be deferred."""
        sem = FairShareSemaphore(max_slots=5)

        await sem.register_tenant("tenant-1")

        # Acquire a slot
        async with sem.acquire("tenant-1"):
            # Try to unregister - should be deferred
            await sem.unregister_tenant("tenant-1")

            stats = await sem.stats()
            # Tenant should still be there (but marked closing)
            assert "tenant-1" not in stats.tenants  # Not in public stats

            # Internal state should show closing
            assert sem._tenants["tenant-1"].closing is True

        # After release, tenant should be gone
        await asyncio.sleep(0.05)
        assert "tenant-1" not in sem._tenants

    @pytest.mark.asyncio
    async def test_force_unregister(self):
        """Force unregister should work but logs warning."""
        sem = FairShareSemaphore(max_slots=5)

        await sem.register_tenant("tenant-1")

        async with sem.acquire("tenant-1"):
            # Force unregister
            await sem.unregister_tenant("tenant-1", force=True)

            assert "tenant-1" not in sem._tenants


class TestContextVariables:
    """Test context variable functionality."""

    @pytest.mark.asyncio
    async def test_set_and_get_tenant_id(self):
        """Test setting and getting tenant ID."""
        assert get_tenant_id() is None

        set_tenant_id("test-tenant")
        assert get_tenant_id() == "test-tenant"

        set_tenant_id(None)

    @pytest.mark.asyncio
    async def test_tenant_context_manager(self):
        """Test tenant_context context manager."""
        assert get_tenant_id() is None

        async with tenant_context("ctx-tenant"):
            assert get_tenant_id() == "ctx-tenant"

        assert get_tenant_id() is None

    @pytest.mark.asyncio
    async def test_nested_tenant_contexts(self):
        """Test that nested contexts work correctly."""
        assert get_tenant_id() is None

        async with tenant_context("outer"):
            assert get_tenant_id() == "outer"

            async with tenant_context("inner"):
                assert get_tenant_id() == "inner"

            assert get_tenant_id() == "outer"

        assert get_tenant_id() is None


class TestErrorHandling:
    """Test error handling."""

    @pytest.mark.asyncio
    async def test_acquire_without_registration_raises(self):
        """Acquiring without registration should raise error."""
        sem = FairShareSemaphore(max_slots=5)

        set_tenant_id("unregistered")
        with pytest.raises(TenantNotRegisteredError):
            async with sem.acquire():
                pass
        set_tenant_id(None)

    @pytest.mark.asyncio
    async def test_acquire_without_tenant_id_raises(self):
        """Acquiring without tenant ID should raise error."""
        sem = FairShareSemaphore(max_slots=5)

        with pytest.raises(TenantNotRegisteredError):
            async with sem.acquire():
                pass

    @pytest.mark.asyncio
    async def test_invalid_max_slots_raises(self):
        """Invalid max_slots should raise ValueError."""
        with pytest.raises(ValueError):
            FairShareSemaphore(max_slots=0)

        with pytest.raises(ValueError):
            FairShareSemaphore(max_slots=-1)

    @pytest.mark.asyncio
    async def test_invalid_min_share_raises(self):
        """Invalid min_share should raise ValueError."""
        with pytest.raises(ValueError):
            FairShareSemaphore(max_slots=10, min_share=0)

    @pytest.mark.asyncio
    async def test_acquire_on_closing_tenant_raises(self):
        """Acquiring on a closing tenant should raise error."""
        sem = FairShareSemaphore(max_slots=5)

        await sem.register_tenant("tenant-1")

        async with sem.acquire("tenant-1"):
            await sem.unregister_tenant("tenant-1")  # Marks as closing

            # New acquire should fail
            with pytest.raises(TenantNotRegisteredError):
                async with sem.acquire("tenant-1"):
                    pass


class TestMinShare:
    """Test minimum share functionality."""

    @pytest.mark.asyncio
    async def test_min_share_guaranteed(self):
        """Each tenant should get at least min_share slots."""
        sem = FairShareSemaphore(max_slots=10, min_share=3)

        async with (
            sem.tenant("t1"),
            sem.tenant("t2"),
            sem.tenant("t3"),
            sem.tenant("t4"),
            sem.tenant("t5"),
        ):
            # All become demanding
            async with (
                sem.acquire("t1"),
                sem.acquire("t2"),
                sem.acquire("t3"),
                sem.acquire("t4"),
                sem.acquire("t5"),
            ):
                stats = await sem.stats()
                for tid in ["t1", "t2", "t3", "t4", "t5"]:
                    assert stats.tenants[tid].share >= 3


class TestCustomShareCalculator:
    """Test custom share calculator."""

    @pytest.mark.asyncio
    async def test_custom_calculator(self):
        """Test custom share calculation function."""

        def custom_calc(max_slots: int, num_demanding: int) -> int:
            # Give each demanding tenant more than equal share
            return max_slots // num_demanding + 1

        sem = FairShareSemaphore(max_slots=10, share_calculator=custom_calc)

        async with sem.tenant("t1"), sem.tenant("t2"):
            async with sem.acquire("t1"), sem.acquire("t2"):
                stats = await sem.stats()
                # Custom calc: 10 // 2 + 1 = 6
                assert stats.tenants["t1"].share == 6


class TestConcurrency:
    """Test concurrent operations."""

    @pytest.mark.asyncio
    async def test_many_concurrent_acquires(self):
        """Test many concurrent acquire operations."""
        sem = FairShareSemaphore(max_slots=5)
        results = []

        async with sem.tenant("tenant-1"):

            async def work(task_id: int):
                async with sem.acquire():
                    results.append(f"start-{task_id}")
                    await asyncio.sleep(0.01)
                    results.append(f"end-{task_id}")

            await asyncio.gather(*[work(i) for i in range(20)])

        # All 20 tasks should complete
        assert len([r for r in results if r.startswith("start")]) == 20
        assert len([r for r in results if r.startswith("end")]) == 20

    @pytest.mark.asyncio
    async def test_concurrent_tenant_registration(self):
        """Test concurrent tenant registrations."""
        sem = FairShareSemaphore(max_slots=10)

        async def tenant_work(tenant_id: str):
            async with sem.tenant(tenant_id):
                async with sem.acquire():
                    await asyncio.sleep(0.05)

        await asyncio.gather(*[tenant_work(f"t-{i}") for i in range(10)])

        stats = await sem.stats()
        assert stats.active_tenants == 0  # All should have unregistered


class TestStats:
    """Test statistics functionality."""

    @pytest.mark.asyncio
    async def test_stats_accuracy(self):
        """Test that stats are accurate."""
        sem = FairShareSemaphore(max_slots=10)

        async with sem.tenant("t1"):
            async with sem.acquire():
                async with sem.acquire():
                    stats = await sem.stats()

                    assert stats.max_slots == 10
                    assert stats.active_tenants == 1
                    assert stats.demanding_tenants == 1
                    assert stats.total_active_slots == 2
                    assert stats.available_slots == 8
                    assert stats.tenants["t1"].active_slots == 2

    @pytest.mark.asyncio
    async def test_stats_excludes_closing_tenants(self):
        """Stats should exclude closing tenants."""
        sem = FairShareSemaphore(max_slots=10)

        await sem.register_tenant("t1")

        async with sem.acquire("t1"):
            await sem.unregister_tenant("t1")  # Marks as closing

            stats = await sem.stats()
            assert "t1" not in stats.tenants


class TestEdgeCases:
    """Test edge cases."""

    @pytest.mark.asyncio
    async def test_single_slot_semaphore(self):
        """Test semaphore with single slot."""
        sem = FairShareSemaphore(max_slots=1)
        order = []

        async with sem.tenant("t1"), sem.tenant("t2"):

            async def work(tenant_id: str, task_id: int):
                async with sem.acquire(tenant_id):
                    order.append(f"{tenant_id}-{task_id}")
                    await asyncio.sleep(0.01)

            await asyncio.gather(
                work("t1", 1),
                work("t2", 1),
                work("t1", 2),
                work("t2", 2),
            )

        # All should complete, one at a time
        assert len(order) == 4

    @pytest.mark.asyncio
    async def test_reregister_tenant(self):
        """Test registering same tenant twice."""
        sem = FairShareSemaphore(max_slots=10)

        await sem.register_tenant("t1")
        await sem.register_tenant("t1")  # Should not create duplicate

        stats = await sem.stats()
        assert stats.active_tenants == 1

        await sem.unregister_tenant("t1")

    @pytest.mark.asyncio
    async def test_exception_in_acquired_block(self):
        """Test that slots are released even when exception occurs."""
        sem = FairShareSemaphore(max_slots=5)

        async with sem.tenant("t1"):
            try:
                async with sem.acquire():
                    stats = await sem.stats()
                    assert stats.available_slots == 4
                    raise ValueError("Test error")
            except ValueError:
                pass

            stats = await sem.stats()
            assert stats.available_slots == 5  # Slot should be released

    @pytest.mark.asyncio
    async def test_reregister_closing_tenant(self):
        """Test re-registering a tenant that is closing."""
        sem = FairShareSemaphore(max_slots=5)

        await sem.register_tenant("t1")

        async with sem.acquire("t1"):
            await sem.unregister_tenant("t1")  # Marks as closing
            assert sem._tenants["t1"].closing is True

            # Re-register
            await sem.register_tenant("t1")
            assert sem._tenants["t1"].closing is False
