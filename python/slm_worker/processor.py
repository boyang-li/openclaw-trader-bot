"""
SLM Processor - enriches signals using Qwen 2.5-1.5B
"""

import json
import logging
import time
from datetime import datetime
from typing import Any

import torch
from transformers import AutoModelForCausalLM, AutoTokenizer, BitsAndBytesConfig

from .config import SLMConfig
from .signal_schema import Signal

logger = logging.getLogger(__name__)

ENRICHMENT_PROMPT_TEMPLATE = """You are a financial signal analyzer. Analyze this signal and provide enrichment.

Signal:
- Source: {source}
- Category: {category}  
- Subject: {subject}
- Action: {action}
- Sentiment: {sentiment}
- Urgency: {urgency}

Respond with valid JSON only:
{{
  "refined_sentiment": <float -1.0 to 1.0>,
  "refined_urgency": "<low|medium|high|critical>",
  "entities": ["<entity1>", "<entity2>"],
  "summary": "<one sentence summary>",
  "market_impact": "<positive|negative|neutral>",
  "confidence_adjustment": <float -0.2 to 0.2>
}}"""


class SLMProcessor:
    def __init__(self, config: SLMConfig):
        self.config = config
        self.model: Any = None
        self.tokenizer: Any = None
        self._loaded = False

    def load_model(self):
        if self._loaded:
            return

        logger.info(f"Loading SLM model: {self.config.model_name}")
        start = time.time()

        device = self._detect_device()
        logger.info(f"Using device: {device}")

        quantization_config = None
        if self.config.use_4bit and device == "cuda":
            quantization_config = BitsAndBytesConfig(
                load_in_4bit=True,
                bnb_4bit_compute_dtype=torch.float16,
                bnb_4bit_use_double_quant=True,
                bnb_4bit_quant_type="nf4",
            )
            logger.info("Using 4-bit quantization")

        self.tokenizer = AutoTokenizer.from_pretrained(
            self.config.model_name,
            trust_remote_code=self.config.trust_remote_code,
        )

        model_kwargs: dict[str, Any] = {
            "trust_remote_code": self.config.trust_remote_code,
            "low_cpu_mem_usage": True,
        }

        if quantization_config:
            model_kwargs["quantization_config"] = quantization_config
            model_kwargs["device_map"] = "auto"
        elif device == "cuda":
            model_kwargs["torch_dtype"] = torch.float16
            model_kwargs["device_map"] = "auto"
        elif device == "mps":
            model_kwargs["torch_dtype"] = torch.float16

        self.model = AutoModelForCausalLM.from_pretrained(
            self.config.model_name,
            **model_kwargs,
        )

        if device == "mps":
            self.model = self.model.to("mps")
        elif device == "cpu" and not quantization_config:
            pass  # Keep on CPU

        self._loaded = True
        elapsed = time.time() - start
        logger.info(f"Model loaded in {elapsed:.2f}s")

    def _detect_device(self) -> str:
        if self.config.device != "auto":
            return self.config.device

        if torch.cuda.is_available():
            return "cuda"
        elif torch.backends.mps.is_available():
            return "mps"
        return "cpu"

    def enrich_signal(self, signal: Signal) -> Signal:
        if not self._loaded:
            self.load_model()

        start = time.time()

        prompt = ENRICHMENT_PROMPT_TEMPLATE.format(
            source=signal.source,
            category=signal.category,
            subject=signal.subject,
            action=signal.action,
            sentiment=signal.sentiment,
            urgency=signal.urgency,
        )

        try:
            enrichment = self._generate_enrichment(prompt)
            signal.enrichment = enrichment

            if "refined_sentiment" in enrichment:
                refined = float(enrichment["refined_sentiment"])
                signal.sentiment = max(-1.0, min(1.0, refined))

            if "refined_urgency" in enrichment:
                if enrichment["refined_urgency"] in ("low", "medium", "high", "critical"):
                    signal.urgency = enrichment["refined_urgency"]

            if "confidence_adjustment" in enrichment:
                adj = float(enrichment["confidence_adjustment"])
                signal.confidence = max(0.0, min(1.0, signal.confidence + adj))

            if "entities" in enrichment and isinstance(enrichment["entities"], list):
                existing_tags = set(signal.tags or [])
                for entity in enrichment["entities"]:
                    if entity and entity not in existing_tags:
                        signal.tags.append(entity)

        except Exception as e:
            logger.warning(f"Enrichment failed for signal {signal.id}: {e}")
            signal.enrichment = {"error": str(e)}

        elapsed_ms = (time.time() - start) * 1000
        signal.metadata.processed_at = datetime.utcnow().isoformat() + "Z"
        signal.metadata.slm_model = self.config.model_name
        signal.metadata.slm_version = "1.0.0"
        signal.metadata.enrichment_latency_ms = elapsed_ms

        return signal

    def _generate_enrichment(self, prompt: str) -> dict[str, Any]:
        messages = [
            {"role": "system", "content": "You are a JSON-only financial signal analyzer."},
            {"role": "user", "content": prompt},
        ]

        text = self.tokenizer.apply_chat_template(
            messages,
            tokenize=False,
            add_generation_prompt=True,
        )

        inputs = self.tokenizer(text, return_tensors="pt")

        device = self._detect_device()
        if device in ("cuda", "mps"):
            inputs = {k: v.to(device) for k, v in inputs.items()}

        with torch.no_grad():
            outputs = self.model.generate(
                **inputs,
                max_new_tokens=self.config.max_length,
                temperature=self.config.temperature,
                do_sample=self.config.temperature > 0,
                pad_token_id=self.tokenizer.eos_token_id,
            )

        response = self.tokenizer.decode(
            outputs[0][inputs["input_ids"].shape[1]:],
            skip_special_tokens=True,
        )

        return self._parse_json_response(response)

    def _parse_json_response(self, response: str) -> dict[str, Any]:
        response = response.strip()

        if "```json" in response:
            start = response.find("```json") + 7
            end = response.find("```", start)
            if end > start:
                response = response[start:end].strip()
        elif "```" in response:
            start = response.find("```") + 3
            end = response.find("```", start)
            if end > start:
                response = response[start:end].strip()

        start_idx = response.find("{")
        end_idx = response.rfind("}") + 1
        if start_idx != -1 and end_idx > start_idx:
            response = response[start_idx:end_idx]

        try:
            return json.loads(response)
        except json.JSONDecodeError as e:
            logger.warning(f"Failed to parse JSON response: {e}")
            logger.debug(f"Raw response: {response}")
            return {"parse_error": str(e), "raw_response": response[:200]}

    def enrich_batch(self, signals: list[Signal]) -> list[Signal]:
        return [self.enrich_signal(s) for s in signals]
