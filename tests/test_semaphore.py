"""
Tests for aio-fairshare.
"""

import asyncio
import pytest
from aio_fairshare import (
    FairShareSemaphore,
    set_tenant_id,
    get_tenant_id,
    tenant_context,
)
from aio_fairshare.semaphore import TenantNotRegisteredError


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
                    await asyncio.sleep(0.1)
            
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
        """Two tenants should each get half the slots."""
        sem = FairShareSemaphore(max_slots=10)
        tenant1_max = 0
        tenant2_max = 0
        
        async def tenant_work(tenant_id: str, results: list):
            async with sem.tenant(tenant_id):
                tasks = []
                for _ in range(10):  # Try to acquire more than fair share
                    async def work():
                        async with sem.acquire():
                            await asyncio.sleep(0.05)
                    tasks.append(asyncio.create_task(work()))
                
                # Check max concurrent after a short delay
                await asyncio.sleep(0.02)
                stats = await sem.stats()
                results.append(stats.tenants[tenant_id].active_slots)
                
                await asyncio.gather(*tasks)
        
        results1, results2 = [], []
        await asyncio.gather(
            tenant_work("tenant-1", results1),
            tenant_work("tenant-2", results2),
        )
        
        # Each should have gotten at most 5 slots (half of 10)
        assert max(results1) <= 5
        assert max(results2) <= 5
    
    @pytest.mark.asyncio
    async def test_share_adjusts_when_tenant_joins(self):
        """Share should decrease when new tenant joins."""
        sem = FairShareSemaphore(max_slots=10)
        
        async with sem.tenant("tenant-1"):
            stats = await sem.stats()
            assert stats.tenants["tenant-1"].share == 10  # All slots
            
            async with sem.tenant("tenant-2"):
                stats = await sem.stats()
                assert stats.tenants["tenant-1"].share == 5  # Half
                assert stats.tenants["tenant-2"].share == 5
    
    @pytest.mark.asyncio
    async def test_share_adjusts_when_tenant_leaves(self):
        """Share should increase when tenant leaves."""
        sem = FairShareSemaphore(max_slots=10)
        
        async with sem.tenant("tenant-1"):
            async with sem.tenant("tenant-2"):
                stats = await sem.stats()
                assert stats.tenants["tenant-1"].share == 5
            
            # tenant-2 left
            stats = await sem.stats()
            assert stats.tenants["tenant-1"].share == 10
    
    @pytest.mark.asyncio
    async def test_three_tenants_fair_share(self):
        """Three tenants should each get 1/3 of slots."""
        sem = FairShareSemaphore(max_slots=9)
        
        async with sem.tenant("t1"), sem.tenant("t2"), sem.tenant("t3"):
            stats = await sem.stats()
            assert stats.tenants["t1"].share == 3
            assert stats.tenants["t2"].share == 3
            assert stats.tenants["t3"].share == 3


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


class TestMinShare:
    """Test minimum share functionality."""
    
    @pytest.mark.asyncio
    async def test_min_share_guaranteed(self):
        """Each tenant should get at least min_share slots."""
        sem = FairShareSemaphore(max_slots=10, min_share=3)
        
        # With 5 tenants, fair share would be 2, but min_share is 3
        async with sem.tenant("t1"), sem.tenant("t2"), sem.tenant("t3"), \
                   sem.tenant("t4"), sem.tenant("t5"):
            stats = await sem.stats()
            for tid in ["t1", "t2", "t3", "t4", "t5"]:
                assert stats.tenants[tid].share >= 3


class TestCustomShareCalculator:
    """Test custom share calculator."""
    
    @pytest.mark.asyncio
    async def test_custom_calculator(self):
        """Test custom share calculation function."""
        def custom_calc(max_slots: int, num_tenants: int) -> int:
            # Give first tenant more
            return max_slots // num_tenants + 1
        
        sem = FairShareSemaphore(max_slots=10, share_calculator=custom_calc)
        
        async with sem.tenant("t1"), sem.tenant("t2"):
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
                    assert stats.total_active_slots == 2
                    assert stats.available_slots == 8
                    assert stats.tenants["t1"].active_slots == 2
    
    @pytest.mark.asyncio
    async def test_stats_sync(self):
        """Test synchronous stats method."""
        sem = FairShareSemaphore(max_slots=10)
        
        async with sem.tenant("t1"):
            stats = sem.stats_sync()
            
            assert stats["max_slots"] == 10
            assert stats["active_tenants"] == 1
            assert "t1" in stats["tenants"]


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
