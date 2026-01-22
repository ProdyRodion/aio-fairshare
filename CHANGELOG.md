# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.1] - 2025-01-22

### Fixed
- **Double waiting decrement bug**: Fixed issue where `waiting` counter could be decremented twice on error if acquisition was successful. Added `_waiting_registered` flag to track state properly.
- **Thundering herd in `_notify_waiters()`**: Now wakes only one waiter per call instead of all eligible waiters, preventing inefficient mass wakeups.

### Changed
- Added `__slots__` to all classes (`TenantContext`, `_SlotAcquisition`, `FairShareSemaphore`) and dataclasses for better memory efficiency.
- Moved `waiting` decrement into the acquire success path for cleaner state management.

## [0.2.0] - 2025-01-22

### Fixed
- **Slot leak on cancellation**: Fixed slot leak when task is cancelled during `acquire().__aenter__`. Now properly catches `CancelledError` and rolls back slot/waiting counters before re-raising.
- **Busy-wait polling**: Replaced `asyncio.sleep()` polling loop with FIFO queue + `Future` signaling. Waiters are now notified deterministically on slot release.
- **Fair share calculation**: Now uses "demanding tenants" (those with `active_slots > 0` or `waiting > 0`) instead of all registered tenants for share calculation. Idle tenants no longer reduce others' shares.
- **Double-release risk**: Implemented deferred unregistration - tenants with active slots are marked as "closing" and only fully removed when all slots are released.
- **Starvation risk**: Added FIFO ordering for waiters. Tasks are now served in the order they started waiting.
- **Inconsistent stats_sync()**: Renamed to `_stats_sync_unsafe()` and marked as internal/debug-only. Added warning in docstring about potential inconsistency.

### Added
- `TenantHasActiveSlotsError` exception (not currently raised but available for strict mode)
- `_TenantState` dataclass for cleaner internal state management
- `demanding_tenants` field in `SemaphoreStats`
- `force` parameter to `unregister_tenant()` for emergency unregistration
- Support for re-registering a closing tenant

### Changed
- `share_calculator` callback now receives `num_demanding_tenants` instead of `num_tenants`
- Internal tenant state now uses `_TenantState` dataclass with `closing` flag
- `stats()` now excludes closing tenants from results

## [0.1.0] - 2025-01-21

### Added
- Initial release
- `FairShareSemaphore` class for fair resource distribution
- `TenantContext` for tenant lifecycle management
- Context variable support (`set_tenant_id`, `get_tenant_id`, `tenant_context`)
- Custom share calculator support
- Configurable `min_share` and `poll_interval`
- Statistics via `stats()` and `stats_sync()` methods
- Comprehensive test suite
