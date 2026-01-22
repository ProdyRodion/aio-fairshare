# aio-fairshare

Async fair-share semaphore for multi-tenant resource management in Python.

[![PyPI version](https://img.shields.io/pypi/v/aio-fairshare)](https://pypi.org/project/aio-fairshare/)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## What is it?

`aio-fairshare` distributes a fixed pool of resources **fairly** among active tenants. Unlike a regular semaphore, it dynamically adjusts each tenant's share as tenants join or leave.

**Best suited for:**
- Browser tabs / pages (Playwright, Puppeteer)
- Worker processes / threads
- Shared API clients with concurrent connection limits

**Not suited for:**
- Database connection pools (use asyncpg/SQLAlchemy built-in pools)
- Rate limiting (use `aiolimiter` instead)

### The Problem

Imagine you have 10 browser tabs and multiple concurrent requests:

```
Regular Semaphore:
- Request A arrives first, grabs all 10 tabs
- Request B waits... and waits... 
- Request A finishes after 60 seconds
- Request B finally starts

Fair-Share Semaphore:
- Request A arrives, can use up to 10 tabs
- Request B arrives, now each can use up to 5 tabs
- Both requests proceed concurrently!
- Request B finishes, Request A can now use all 10 tabs again
```

## Installation

```bash
pip install aio-fairshare
```

## Quick Start

```python
import asyncio
from aio_fairshare import FairShareSemaphore

async def main():
    # Create semaphore with 10 slots
    semaphore = FairShareSemaphore(max_slots=10)
    
    async def process_request(request_id: str):
        # Register as a tenant
        async with semaphore.tenant(request_id):
            # Acquire slots (respects fair share)
            async with semaphore.acquire():
                print(f"{request_id}: Working with a slot")
                await asyncio.sleep(1)
    
    # Run multiple requests concurrently
    await asyncio.gather(
        process_request("request-1"),
        process_request("request-2"),
        process_request("request-3"),
    )

asyncio.run(main())
```

## Real-World Example: Browser Automation

```python
from playwright.async_api import async_playwright
from aio_fairshare import FairShareSemaphore

# Global semaphore for browser tabs
tab_semaphore = FairShareSemaphore(max_slots=10)

async def scrape_urls(request_id: str, urls: list[str]):
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        
        async with tab_semaphore.tenant(request_id):
            async def scrape_url(url: str):
                async with tab_semaphore.acquire():
                    page = await browser.new_page()
                    try:
                        await page.goto(url)
                        return await page.title()
                    finally:
                        await page.close()
            
            # Process URLs concurrently, respecting fair share
            results = await asyncio.gather(*[scrape_url(u) for u in urls])
        
        await browser.close()
        return results

# Multiple requests can run concurrently with fair resource sharing
await asyncio.gather(
    scrape_urls("req-1", ["https://example1.com", "https://example2.com"]),
    scrape_urls("req-2", ["https://example3.com", "https://example4.com"]),
)
```

## Real-World Example: Shared API Client

Use when multiple services share one API client with a **concurrent connection limit** (not rate limit):

```python
import httpx
from aio_fairshare import FairShareSemaphore

# API allows max 10 concurrent requests
api_semaphore = FairShareSemaphore(max_slots=10)
client = httpx.AsyncClient()

async def call_api(service_name: str, endpoints: list[str]):
    """Multiple internal services share the same API client."""
    async with api_semaphore.tenant(service_name):
        async def fetch(endpoint: str):
            async with api_semaphore.acquire():
                return await client.get(f"https://api.example.com/{endpoint}")
        
        return await asyncio.gather(*[fetch(ep) for ep in endpoints])

# Service A and Service B fairly share the 10 concurrent slots
await asyncio.gather(
    call_api("service-a", ["users", "orders", "products"]),
    call_api("service-b", ["inventory", "shipping"]),
)
```

> **Note:** If you need rate limiting (e.g., 100 requests/minute), use [`aiolimiter`](https://github.com/mjpieters/aiolimiter) instead.

## API Reference

### FairShareSemaphore

```python
FairShareSemaphore(
    max_slots: int,
    *,
    min_share: int = 1,
    share_calculator: Callable[[int, int], int] | None = None,
)
```

**Parameters:**
- `max_slots`: Maximum number of concurrent slots
- `min_share`: Minimum slots guaranteed per tenant (default: 1)
- `share_calculator`: Custom function to calculate per-tenant share. Receives `(max_slots, num_demanding_tenants)` and returns share per tenant.

### Methods

#### `tenant(tenant_id: str) -> TenantContext`

Register a tenant and get a context for acquiring slots.

```python
async with semaphore.tenant("my-tenant"):
    # Tenant is registered
    async with semaphore.acquire():
        # Slot acquired
        ...
```

#### `acquire(tenant_id: str | None = None) -> AsyncContextManager`

Acquire a slot. Blocks until a slot is available within the tenant's fair share.

```python
async with semaphore.acquire():
    # Do work
    ...
```

#### `stats() -> SemaphoreStats`

Get current statistics (thread-safe).

```python
stats = await semaphore.stats()
print(f"Active tenants: {stats.active_tenants}")
print(f"Demanding tenants: {stats.demanding_tenants}")
print(f"Available slots: {stats.available_slots}")
```

### Context Variables

For advanced use cases, you can manage tenant ID via context variables:

```python
from aio_fairshare import set_tenant_id, get_tenant_id, tenant_context

# Option 1: Set directly
set_tenant_id("my-tenant")

# Option 2: Use context manager
async with tenant_context("my-tenant"):
    print(get_tenant_id())  # "my-tenant"
```

## How Fair Sharing Works

| Demanding Tenants | Max Slots | Share per Tenant |
|-------------------|-----------|------------------|
| 1                 | 10        | 10               |
| 2                 | 10        | 5                |
| 3                 | 10        | 3                |
| 5                 | 10        | 2                |
| 10                | 10        | 1                |

**Note:** Only "demanding" tenants (those with active slots or waiting for slots) are counted for share calculation. Idle registered tenants don't reduce others' shares.

The share is recalculated **dynamically** whenever a tenant joins, leaves, or becomes demanding.

## Custom Share Calculator

You can provide a custom function to calculate shares:

```python
def priority_calculator(max_slots: int, num_demanding_tenants: int) -> int:
    """Give each tenant a bit more than equal share."""
    return max_slots // num_demanding_tenants + 2

semaphore = FairShareSemaphore(
    max_slots=20,
    share_calculator=priority_calculator,
)
```

## Comparison with Alternatives

| Feature | `asyncio.Semaphore` | `aiolimiter` | `aio-fairshare` |
|---------|---------------------|--------------|-----------------|
| Global limit | ✅ | ✅ | ✅ |
| Per-tenant limit | ❌ | ✅ (fixed) | ✅ (dynamic) |
| Fair sharing | ❌ | ❌ | ✅ |
| Dynamic adjustment | ❌ | ❌ | ✅ |
| FIFO ordering | ❌ | ❌ | ✅ |
| Cancellation safe | ✅ | ✅ | ✅ |
| Zero dependencies | ✅ | ✅ | ✅ |

## Development

```bash
# Clone the repo
git clone https://github.com/ProdyRodion/aio-fairshare
cd aio-fairshare

# Install dependencies with Poetry
poetry install

# Run tests
poetry run pytest -v

# Run linter
poetry run ruff check .

# Run type checker
poetry run mypy aio_fairshare

# Build package
poetry build

# Publish to PyPI
poetry publish
```

## License

MIT License. See [LICENSE](LICENSE) for details.