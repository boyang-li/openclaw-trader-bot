import asyncio
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Literal
from datetime import datetime
import aiohttp
from aiohttp import ClientTimeout

from .config import TelegramConfig, DiscordConfig

logger = logging.getLogger(__name__)


@dataclass
class AlertEnvelope:
    kind: Literal["l1_signal", "l2_insight"]
    data: dict[str, Any]
    reasons: list[str]


class Notifier(ABC):
    @abstractmethod
    async def send(self, alert: AlertEnvelope) -> bool:
        pass

    def build_signal_context(self, signal: dict[str, Any], reasons: list[str]) -> dict[str, Any]:
        enrichment = signal.get("enrichment", {}) or {}
        raw_data = signal.get("raw_data", {}) or {}
        
        return {
            "layer_title": "ACC L1 Signal",
            "subject": signal.get("subject", "Unknown"),
            "action": signal.get("action", "Unknown"),
            "source": signal.get("source", "Unknown"),
            "category": signal.get("category", "Unknown"),
            "urgency": enrichment.get("refined_urgency", signal.get("urgency", "Unknown")),
            "sentiment": enrichment.get("refined_sentiment", signal.get("sentiment", 0.0)),
            "market_impact": enrichment.get("market_impact", "Unknown"),
            "summary": enrichment.get("summary", ""),
            "confidence": signal.get("confidence", 0.0),
            "timestamp": signal.get("timestamp", ""),
            "reasons": reasons,
            "raw_data": raw_data,
        }

    def build_insight_context(self, insight: dict[str, Any], reasons: list[str]) -> dict[str, Any]:
        trigger_signals = insight.get("trigger_signals") or []
        metrics = insight.get("metrics") or {}
        context = insight.get("context") or {}
        metadata = insight.get("metadata") or {}
        
        return {
            "layer_title": "ACC L2 Insight",
            "id": insight.get("id", "unknown"),
            "title": insight.get("title", "Unknown insight"),
            "type": insight.get("type", "unknown"),
            "severity": insight.get("severity", "info"),
            "description": insight.get("description", ""),
            "entities": insight.get("entities", []) or [],
            "categories": insight.get("categories", []) or [],
            "metrics": metrics,
            "context": context,
            "metadata": metadata,
            "trigger_signals": trigger_signals,
            "reasons": reasons,
            "timestamp": insight.get("timestamp", metadata.get("created_at", "")),
        }


class TelegramNotifier(Notifier):
    URGENCY_EMOJI = {
        "low": "🟢",
        "medium": "🟡", 
        "high": "🟠",
        "critical": "🔴",
    }
    
    SENTIMENT_EMOJI = {
        "bullish": "📈",
        "bearish": "📉",
        "neutral": "➖",
    }
    
    IMPACT_EMOJI = {
        "highly_positive": "🚀",
        "positive": "✅",
        "neutral": "➖",
        "negative": "⚠️",
        "highly_negative": "🚨",
    }

    SEVERITY_EMOJI = {
        "info": "ℹ️",
        "warning": "⚠️",
        "alert": "🚨",
        "critical": "🛑",
    }

    def __init__(self, config: TelegramConfig):
        self.config = config
        self.api_url = f"https://api.telegram.org/bot{config.bot_token}/sendMessage"

    async def send(self, alert: AlertEnvelope) -> bool:
        if not self.config.is_configured():
            logger.warning("Telegram not configured, skipping notification")
            return False

        if alert.kind == "l1_signal":
            msg = self.build_signal_context(alert.data, alert.reasons)
            text = self._format_signal_message(msg)
        else:
            msg = self.build_insight_context(alert.data, alert.reasons)
            text = self._format_insight_message(msg)
        
        payload = {
            "chat_id": self.config.chat_id,
            "text": text,
            "parse_mode": self.config.parse_mode,
            "disable_notification": self.config.disable_notification,
        }

        try:
            timeout = ClientTimeout(total=10)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(self.api_url, json=payload) as resp:
                    if resp.status == 200:
                        logger.info("Telegram alert sent for %s", msg.get("subject", msg.get("title", "unknown")))
                        return True
                    body = await resp.text()
                    logger.error("Telegram API error %s: %s", resp.status, body)
                    return False
        except asyncio.TimeoutError:
            logger.error("Telegram API timeout")
            return False
        except Exception as exc:
            logger.error("Telegram send error: %s", exc)
            return False

    def _format_signal_message(self, msg: dict[str, Any]) -> str:
        urgency_emoji = self.URGENCY_EMOJI.get(msg["urgency"], "⚪")
        sentiment_val = msg["sentiment"]
        sentiment_emoji = self.SENTIMENT_EMOJI["bullish"] if sentiment_val > 0.1 else (
            self.SENTIMENT_EMOJI["bearish"] if sentiment_val < -0.1 else self.SENTIMENT_EMOJI["neutral"]
        )
        impact_emoji = self.IMPACT_EMOJI.get(msg["market_impact"], "➖")
        
        lines = [
            f"{urgency_emoji} <b>{msg['layer_title']}: {msg['subject'].upper()}</b>",
            "",
            f"<b>Action:</b> {msg['action']}",
            f"<b>Source:</b> {msg['source']} | <b>Category:</b> {msg['category']}",
            f"<b>Urgency:</b> {msg['urgency'].upper()} | <b>Confidence:</b> {msg['confidence']:.0%}",
            f"{sentiment_emoji} <b>Sentiment:</b> {sentiment_val:+.2f}",
            f"{impact_emoji} <b>Market Impact:</b> {msg['market_impact']}",
        ]
        
        raw_data = msg.get("raw_data", {})
        if raw_data and msg["source"].lower() == "binance":
            trade_lines = self._format_binance_trade_details(raw_data)
            if trade_lines:
                lines.extend(["", "<b>Trade Details:</b>"])
                lines.extend(trade_lines)
        
        if raw_data and msg["source"].lower() == "fred":
            econ_lines = self._format_fred_economic_details(raw_data)
            if econ_lines:
                lines.extend(["", "<b>Economic Data:</b>"])
                lines.extend(econ_lines)
        
        if raw_data and msg["source"].lower() == "telegram":
            tg_lines = self._format_telegram_message_details(raw_data)
            if tg_lines:
                lines.extend(["", "<b>Source Message:</b>"])
                lines.extend(tg_lines)
        
        if msg["summary"]:
            lines.extend(["", f"<b>Summary:</b> {msg['summary'][:500]}"])
        
        lines.extend([
            "",
            f"<b>Alert triggers:</b> {', '.join(msg['reasons'])}",
            "",
            f"<i>{msg['timestamp'][:19] if msg['timestamp'] else 'N/A'}</i>",
        ])
        
        return "\n".join(lines)

    def _format_insight_message(self, msg: dict[str, Any]) -> str:
        severity = msg["severity"].lower()
        severity_emoji = self.SEVERITY_EMOJI.get(severity, "🧠")
        lines = [
            f"{severity_emoji} <b>{msg['layer_title']}: {msg['title']}</b>",
            "",
            f"<b>Type:</b> {msg['type']} | <b>Severity:</b> {msg['severity'].upper()}",
        ]

        if msg["categories"]:
            lines.append(f"<b>Categories:</b> {', '.join(msg['categories'])}")
        if msg["entities"]:
            lines.append(f"<b>Entities:</b> {', '.join(msg['entities'])}")

        metrics = msg["metrics"]
        metric_bits: list[str] = []
        if metrics:
            if "correlation_score" in metrics:
                metric_bits.append(f"Correlation {metrics['correlation_score']:.2f}")
            if "confidence" in metrics:
                metric_bits.append(f"Confidence {metrics['confidence']:.2f}")
            if "signal_count" in metrics:
                metric_bits.append(f"Signals {metrics['signal_count']}")
            if "time_window_hours" in metrics:
                metric_bits.append(f"Window {metrics['time_window_hours']:.2f}h")
        if metric_bits:
            lines.append("<b>Metrics:</b> " + ", ".join(metric_bits))

        description = msg.get("description")
        if description:
            lines.extend(["", f"<b>Description:</b> {description[:600]}"])

        triggers = msg["trigger_signals"]
        if triggers:
            lines.extend(["", "<b>Trigger Signals:</b>"])
            for trigger in triggers[:5]:
                source = trigger.get("source", "?")
                weight = trigger.get("weight", 0.0)
                signal_id = trigger.get("id", "")
                lines.append(f"  • {source} ({weight:.0%}) {signal_id}")

        lines.extend([
            "",
            f"<b>Insight notes:</b> {', '.join(msg['reasons']) or 'n/a'}",
        ])

        timestamp = msg.get("timestamp")
        if timestamp:
            lines.extend(["", f"<i>{timestamp[:19]}</i>"])

        return "\n".join(lines)

    def _format_binance_trade_details(self, raw_data: dict[str, Any]) -> list[str]:
        lines = []
        
        if "value_usd" in raw_data:
            value = raw_data["value_usd"]
            lines.append(f"  💰 <b>Value:</b> ${value:,.0f} USD")
        
        if "quantity" in raw_data:
            qty = raw_data["quantity"]
            lines.append(f"  📦 <b>Quantity:</b> {qty:,.4f}")
        
        if "price" in raw_data:
            price = raw_data["price"]
            lines.append(f"  💵 <b>Price:</b> ${price:,.2f}")
        
        if "direction" in raw_data:
            direction = raw_data["direction"]
            direction_emoji = "🟢" if direction == "buy" else "🔴"
            lines.append(f"  {direction_emoji} <b>Direction:</b> {direction.upper()}")
        
        if "change_24h_pct" in raw_data:
            change = raw_data["change_24h_pct"]
            change_emoji = "📈" if change > 0 else "📉"
            lines.append(f"  {change_emoji} <b>24h Change:</b> {change:+.2f}%")
        
        return lines

    def _format_fred_economic_details(self, raw_data: dict[str, Any]) -> list[str]:
        lines = []
        
        if "series_title" in raw_data:
            lines.append(f"  📊 <b>Indicator:</b> {raw_data['series_title']}")
        
        if "value" in raw_data:
            value = raw_data["value"]
            if "previous" in raw_data and raw_data.get("has_previous"):
                prev = raw_data["previous"]
                if prev != 0:
                    change_pct = ((value - prev) / abs(prev)) * 100
                    change_emoji = "📈" if change_pct > 0 else "📉" if change_pct < 0 else "➖"
                    lines.append(f"  📈 <b>Current:</b> {value:,.2f}")
                    lines.append(f"  📉 <b>Previous:</b> {prev:,.2f}")
                    lines.append(f"  {change_emoji} <b>Change:</b> {change_pct:+.2f}%")
                else:
                    lines.append(f"  📈 <b>Current:</b> {value:,.2f}")
            else:
                lines.append(f"  📈 <b>Value:</b> {value:,.2f}")
        
        if "date" in raw_data:
            lines.append(f"  📅 <b>Release Date:</b> {raw_data['date']}")
        
        return lines

    def _format_telegram_message_details(self, raw_data: dict[str, Any]) -> list[str]:
        lines = []
        
        chat_title = raw_data.get("chat_title", "")
        chat_username = raw_data.get("chat_username", "")
        chat_type = raw_data.get("chat_type", "")
        
        if chat_title:
            channel_display = chat_title
            if chat_username:
                channel_display += f" (@{chat_username})"
            lines.append(f"  📢 <b>Channel:</b> {channel_display}")
        elif chat_username:
            lines.append(f"  📢 <b>Channel:</b> @{chat_username}")
        
        if chat_type:
            type_emoji = "📢" if chat_type == "channel" else "👥" if chat_type in ("group", "supergroup") else "💬"
            lines.append(f"  {type_emoji} <b>Type:</b> {chat_type}")
        
        from_username = raw_data.get("from_username", "")
        if from_username:
            lines.append(f"  👤 <b>From:</b> @{from_username}")
        
        text = raw_data.get("text", "")
        if text:
            preview = text[:300] + "..." if len(text) > 300 else text
            preview = preview.replace("<", "&lt;").replace(">", "&gt;")
            lines.append(f"  💬 <i>{preview}</i>")
        
        return lines


class DiscordNotifier(Notifier):
    URGENCY_COLOR = {
        "low": 0x00FF00,
        "medium": 0xFFFF00,
        "high": 0xFF8C00,
        "critical": 0xFF0000,
    }

    SEVERITY_COLOR = {
        "info": 0x3498DB,
        "warning": 0xF1C40F,
        "alert": 0xE67E22,
        "critical": 0xE74C3C,
    }

    def __init__(self, config: DiscordConfig):
        self.config = config

    async def send(self, alert: AlertEnvelope) -> bool:
        if not self.config.is_configured():
            logger.warning("Discord not configured, skipping notification")
            return False

        if alert.kind == "l1_signal":
            msg = self.build_signal_context(alert.data, alert.reasons)
            embed = self._build_signal_embed(msg)
        else:
            msg = self.build_insight_context(alert.data, alert.reasons)
            embed = self._build_insight_embed(msg)
        
        payload = {"embeds": [embed]}

        try:
            timeout = ClientTimeout(total=10)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(self.config.webhook_url, json=payload) as resp:
                    if resp.status in (200, 204):
                        label = msg.get("subject") or msg.get("title") or "unknown"
                        logger.info("Discord alert sent for %s", label)
                        return True
                    else:
                        body = await resp.text()
                        logger.error(f"Discord webhook error {resp.status}: {body}")
                        return False
        except asyncio.TimeoutError:
            logger.error("Discord webhook timeout")
            return False
        except Exception as e:
            logger.error(f"Discord send error: {e}")
            return False

    def _build_signal_embed(self, msg: dict[str, Any]) -> dict[str, Any]:
        color = self.URGENCY_COLOR.get(msg["urgency"], 0x808080)
        
        fields = [
            {"name": "Action", "value": msg["action"], "inline": True},
            {"name": "Source", "value": msg["source"], "inline": True},
            {"name": "Category", "value": msg["category"], "inline": True},
            {"name": "Urgency", "value": msg["urgency"].upper(), "inline": True},
            {"name": "Sentiment", "value": f"{msg['sentiment']:+.2f}", "inline": True},
            {"name": "Market Impact", "value": msg["market_impact"], "inline": True},
        ]
        
        raw_data = msg.get("raw_data", {})
        if raw_data and msg["source"].lower() == "binance":
            trade_details = self._format_binance_trade_for_discord(raw_data)
            if trade_details:
                fields.append({"name": "Trade Details", "value": trade_details, "inline": False})
        
        if raw_data and msg["source"].lower() == "fred":
            econ_details = self._format_fred_economic_for_discord(raw_data)
            if econ_details:
                fields.append({"name": "Economic Data", "value": econ_details, "inline": False})
        
        if raw_data and msg["source"].lower() == "telegram":
            tg_details = self._format_telegram_for_discord(raw_data)
            if tg_details:
                fields.append({"name": "Source Message", "value": tg_details, "inline": False})
        
        fields.append({"name": "Alert Triggers", "value": ", ".join(msg["reasons"]), "inline": False})
        
        if msg["summary"]:
            fields.append({"name": "Summary", "value": msg["summary"][:1024], "inline": False})
        
        return {
            "title": f"🔔 {msg['layer_title']}: {msg['subject'].upper()}",
            "color": color,
            "fields": fields,
            "timestamp": datetime.utcnow().isoformat(),
            "footer": {"text": f"Confidence: {msg['confidence']:.0%}"},
        }

    def _build_insight_embed(self, msg: dict[str, Any]) -> dict[str, Any]:
        color = self.SEVERITY_COLOR.get(msg["severity"].lower(), 0x95A5A6)
        fields = [
            {"name": "Type", "value": msg["type"], "inline": True},
            {"name": "Severity", "value": msg["severity"].upper(), "inline": True},
        ]

        if msg["entities"]:
            fields.append({"name": "Entities", "value": ", ".join(msg["entities"])[:1024], "inline": False})
        if msg["categories"]:
            fields.append({"name": "Categories", "value": ", ".join(msg["categories"])[:1024], "inline": False})

        metrics = msg["metrics"]
        if metrics:
            metric_lines = []
            if "correlation_score" in metrics:
                metric_lines.append(f"Correlation {metrics['correlation_score']:.2f}")
            if "confidence" in metrics:
                metric_lines.append(f"Confidence {metrics['confidence']:.2f}")
            if "signal_count" in metrics:
                metric_lines.append(f"Signals {metrics['signal_count']}")
            if "time_window_hours" in metrics:
                metric_lines.append(f"Window {metrics['time_window_hours']:.2f}h")
            if metric_lines:
                fields.append({"name": "Metrics", "value": " | ".join(metric_lines), "inline": False})

        triggers = msg["trigger_signals"]
        if triggers:
            trigger_lines = []
            for trigger in triggers[:5]:
                trigger_lines.append(
                    f"{trigger.get('source','?')} ({trigger.get('weight',0):.0%}) — {trigger.get('id','')}"
                )
            fields.append({"name": "Trigger Signals", "value": "\n".join(trigger_lines), "inline": False})

        if msg.get("description"):
            fields.append({"name": "Description", "value": msg["description"][:1024], "inline": False})

        if msg["reasons"]:
            fields.append({"name": "Insight Notes", "value": ", ".join(msg["reasons"]), "inline": False})

        timestamp = msg.get("timestamp") or datetime.utcnow().isoformat()

        return {
            "title": f"🧠 {msg['layer_title']}: {msg['title']}",
            "color": color,
            "fields": fields,
            "timestamp": timestamp,
            "footer": {"text": f"Insight ID: {msg['id']}"},
        }

    def _format_binance_trade_for_discord(self, raw_data: dict[str, Any]) -> str:
        parts = []
        
        if "value_usd" in raw_data:
            parts.append(f"💰 **Value:** ${raw_data['value_usd']:,.0f}")
        
        if "quantity" in raw_data:
            parts.append(f"📦 **Qty:** {raw_data['quantity']:,.4f}")
        
        if "price" in raw_data:
            parts.append(f"💵 **Price:** ${raw_data['price']:,.2f}")
        
        if "direction" in raw_data:
            direction = raw_data["direction"]
            emoji = "🟢" if direction == "buy" else "🔴"
            parts.append(f"{emoji} **{direction.upper()}**")
        
        if "change_24h_pct" in raw_data:
            change = raw_data["change_24h_pct"]
            emoji = "📈" if change > 0 else "📉"
            parts.append(f"{emoji} **24h:** {change:+.2f}%")
        
        return " | ".join(parts) if parts else ""

    def _format_fred_economic_for_discord(self, raw_data: dict[str, Any]) -> str:
        parts = []
        
        if "series_title" in raw_data:
            parts.append(f"📊 **{raw_data['series_title']}**")
        
        if "value" in raw_data:
            value = raw_data["value"]
            if "previous" in raw_data and raw_data.get("has_previous"):
                prev = raw_data["previous"]
                if prev != 0:
                    change_pct = ((value - prev) / abs(prev)) * 100
                    emoji = "📈" if change_pct > 0 else "📉" if change_pct < 0 else "➖"
                    parts.append(f"{emoji} {value:,.2f} (prev: {prev:,.2f}, {change_pct:+.2f}%)")
                else:
                    parts.append(f"📈 {value:,.2f}")
            else:
                parts.append(f"📈 **Value:** {value:,.2f}")
        
        if "date" in raw_data:
            parts.append(f"📅 {raw_data['date']}")
        
        return " | ".join(parts) if parts else ""

    def _format_telegram_for_discord(self, raw_data: dict[str, Any]) -> str:
        parts = []
        
        chat_title = raw_data.get("chat_title", "")
        chat_username = raw_data.get("chat_username", "")
        
        if chat_title:
            channel_display = f"📢 **{chat_title}**"
            if chat_username:
                channel_display += f" (@{chat_username})"
            parts.append(channel_display)
        elif chat_username:
            parts.append(f"📢 **@{chat_username}**")
        
        from_username = raw_data.get("from_username", "")
        if from_username:
            parts.append(f"👤 @{from_username}")
        
        text = raw_data.get("text", "")
        if text:
            preview = text[:200] + "..." if len(text) > 200 else text
            parts.append(f"*\"{preview}\"*")
        
        return "\n".join(parts) if parts else ""


class ConsoleNotifier(Notifier):
    async def send(self, alert: AlertEnvelope) -> bool:
        if alert.kind == "l1_signal":
            msg = self.build_signal_context(alert.data, alert.reasons)
            self._log_signal(msg)
        else:
            msg = self.build_insight_context(alert.data, alert.reasons)
            self._log_insight(msg)
        return True

    def _log_signal(self, msg: dict[str, Any]) -> None:
        logger.info("=" * 60)
        logger.info(f"{msg['layer_title']}: {msg['subject']} - {msg['action']}")
        logger.info(f"Source: {msg['source']} | Category: {msg['category']}")
        logger.info(f"Urgency: {msg['urgency']} | Sentiment: {msg['sentiment']:+.2f}")
        logger.info(f"Market Impact: {msg['market_impact']}")
        logger.info(f"Reasons: {', '.join(msg['reasons'])}")
        if msg["summary"]:
            logger.info(f"Summary: {msg['summary'][:200]}")
        logger.info("=" * 60)

    def _log_insight(self, msg: dict[str, Any]) -> None:
        logger.info("=" * 60)
        logger.info(f"{msg['layer_title']}: {msg['title']}")
        logger.info(f"Type: {msg['type']} | Severity: {msg['severity']}")
        if msg["entities"]:
            logger.info(f"Entities: {', '.join(msg['entities'])}")
        if msg["categories"]:
            logger.info(f"Categories: {', '.join(msg['categories'])}")
        if msg.get("description"):
            logger.info(f"Description: {msg['description'][:200]}")
        logger.info(f"Reasons: {', '.join(msg['reasons'])}")
        logger.info("=" * 60)


class CompositeNotifier(Notifier):
    def __init__(self, notifiers: list[Notifier]):
        self.notifiers = notifiers

    async def send(self, alert: AlertEnvelope) -> bool:
        if not self.notifiers:
            logger.warning("No notifiers configured")
            return False

        tasks = [notifier.send(alert) for notifier in self.notifiers]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        successes = sum(1 for r in results if r is True)
        logger.debug("Sent to %s/%s notification channels", successes, len(self.notifiers))
        
        return successes > 0
