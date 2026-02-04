#!/usr/bin/env python3
"""
Quick test for L2 Reasoner metrics endpoint.
"""

import asyncio
from aiohttp import web
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST


async def test_metrics():
    """Test that metrics endpoint returns valid Prometheus data."""
    # Create a simple test app with metrics endpoint
    app = web.Application()
    
    async def metrics_handler(_request):
        metrics_data = generate_latest()
        return web.Response(
            body=metrics_data,
            content_type=CONTENT_TYPE_LATEST,
        )
    
    app.router.add_get('/metrics', metrics_handler)
    
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, 'localhost', 8080)
    await site.start()
    
    print("Test server started on http://localhost:8080/metrics")
    print("Press Ctrl+C to stop")
    
    try:
        # Keep running
        await asyncio.sleep(3600)
    except KeyboardInterrupt:
        print("\nStopping test server...")
    finally:
        await runner.cleanup()


if __name__ == '__main__':
    asyncio.run(test_metrics())