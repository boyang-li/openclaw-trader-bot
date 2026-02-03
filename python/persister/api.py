import logging
from aiohttp import web

from .config import APIConfig
from .database import DatabaseManager

logger = logging.getLogger(__name__)


class QueryAPI:
    def __init__(self, config: APIConfig, db: DatabaseManager):
        self.config = config
        self.db = db
        self.app = web.Application()
        self._setup_routes()

    def _setup_routes(self):
        self.app.router.add_get("/health", self.health_handler)
        self.app.router.add_get("/ready", self.ready_handler)
        self.app.router.add_get("/stats", self.stats_handler)
        self.app.router.add_get("/signals", self.query_signals_handler)
        self.app.router.add_get("/signals/{signal_id}", self.get_signal_handler)

    async def health_handler(self, request: web.Request) -> web.Response:
        return web.json_response({"status": "healthy"})

    async def ready_handler(self, request: web.Request) -> web.Response:
        try:
            stats = self.db.get_stats()
            if stats.get("status") == "not_initialized":
                return web.json_response({"status": "not_ready"}, status=503)
            return web.json_response({"status": "ready"})
        except Exception as e:
            return web.json_response({"status": "error", "error": str(e)}, status=503)

    async def stats_handler(self, request: web.Request) -> web.Response:
        try:
            stats = self.db.get_stats()
            return web.json_response(stats)
        except Exception as e:
            logger.error(f"Stats query failed: {e}")
            return web.json_response({"error": str(e)}, status=500)

    async def query_signals_handler(self, request: web.Request) -> web.Response:
        try:
            source = request.query.get("source")
            category = request.query.get("category")
            urgency = request.query.get("urgency")
            subject = request.query.get("subject")
            start_time = request.query.get("start")
            end_time = request.query.get("end")
            
            limit = min(
                int(request.query.get("limit", 100)),
                self.config.max_query_results
            )
            offset = int(request.query.get("offset", 0))
            
            signals = self.db.query_signals(
                source=source,
                category=category,
                urgency=urgency,
                subject=subject,
                start_time=start_time,
                end_time=end_time,
                limit=limit,
                offset=offset,
            )
            
            return web.json_response({
                "count": len(signals),
                "limit": limit,
                "offset": offset,
                "signals": signals,
            })
            
        except ValueError as e:
            return web.json_response({"error": f"Invalid parameter: {e}"}, status=400)
        except Exception as e:
            logger.error(f"Query failed: {e}")
            return web.json_response({"error": str(e)}, status=500)

    async def get_signal_handler(self, request: web.Request) -> web.Response:
        signal_id = request.match_info["signal_id"]
        
        try:
            signal = self.db.get_signal_by_id(signal_id)
            if signal is None:
                return web.json_response({"error": "Signal not found"}, status=404)
            return web.json_response(signal)
        except Exception as e:
            logger.error(f"Get signal failed: {e}")
            return web.json_response({"error": str(e)}, status=500)

    async def start(self):
        runner = web.AppRunner(self.app)
        await runner.setup()
        site = web.TCPSite(runner, self.config.host, self.config.port)
        await site.start()
        logger.info(f"Query API started on {self.config.host}:{self.config.port}")
        return runner
