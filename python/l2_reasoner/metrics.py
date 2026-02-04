"""
Prometheus metrics for L2 Reasoner.
"""

from prometheus_client import Counter, Histogram, Gauge, REGISTRY


class L2ReasonerMetrics:
    """Metrics collector for L2 Reasoner."""
    
    def __init__(self):
        # Input metrics
        try:
            self.signals_received = Counter(
                'l2_reasoner_signals_received_total',
                'Total number of signals received from Kafka',
                ['source']
            )
        except ValueError:
            self.signals_received = REGISTRY._names_to_collectors['l2_reasoner_signals_received_total']
        
        try:
            self.signals_processed = Counter(
                'l2_reasoner_signals_processed_total',
                'Total number of signals successfully processed',
                ['source']
            )
        except ValueError:
            self.signals_processed = REGISTRY._names_to_collectors['l2_reasoner_signals_processed_total']
        
        try:
            self.signals_failed = Counter(
                'l2_reasoner_signals_failed_total',
                'Total number of signals that failed processing',
                ['error_type']
            )
        except ValueError:
            self.signals_failed = REGISTRY._names_to_collectors['l2_reasoner_signals_failed_total']
        
        # Entity tracking metrics
        try:
            self.entities_tracked = Gauge(
                'l2_reasoner_entities_tracked',
                'Number of entities currently being tracked'
            )
        except ValueError:
            self.entities_tracked = REGISTRY._names_to_collectors['l2_reasoner_entities_tracked']
        
        try:
            self.entity_signals_count = Counter(
                'l2_reasoner_entity_signals_total',
                'Total signals tracked per entity',
                ['entity']
            )
        except ValueError:
            self.entity_signals_count = REGISTRY._names_to_collectors['l2_reasoner_entity_signals_total']
        
        # Rule evaluation metrics
        try:
            self.rule_evaluations = Counter(
                'l2_reasoner_rule_evaluations_total',
                'Total rule evaluations performed',
                ['rule_name']
            )
        except ValueError:
            self.rule_evaluations = REGISTRY._names_to_collectors['l2_reasoner_rule_evaluations_total']
        
        try:
            self.rule_triggers = Counter(
                'l2_reasoner_rule_triggers_total',
                'Total rule triggers (insights generated)',
                ['rule_name', 'severity']
            )
        except ValueError:
            self.rule_triggers = REGISTRY._names_to_collectors['l2_reasoner_rule_triggers_total']
        
        # Insight metrics
        try:
            self.insights_generated = Counter(
                'l2_reasoner_insights_generated_total',
                'Total insights generated',
                ['type', 'severity']
            )
        except ValueError:
            self.insights_generated = REGISTRY._names_to_collectors['l2_reasoner_insights_generated_total']
        
        # Timing metrics
        try:
            self.processing_latency = Histogram(
                'l2_reasoner_processing_latency_seconds',
                'Latency of signal processing in seconds',
                buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0)
            )
        except ValueError:
            self.processing_latency = REGISTRY._names_to_collectors['l2_reasoner_processing_latency_seconds']
        
        try:
            self.rule_evaluation_latency = Histogram(
                'l2_reasoner_rule_evaluation_latency_seconds',
                'Latency of rule evaluation in seconds',
                ['rule_name'],
                buckets=(0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1)
            )
        except ValueError:
            self.rule_evaluation_latency = REGISTRY._names_to_collectors['l2_reasoner_rule_evaluation_latency_seconds']
        
        # Correlation metrics
        try:
            self.correlation_scores = Histogram(
                'l2_reasoner_correlation_score_distribution',
                'Distribution of correlation scores',
                buckets=(0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0)
            )
        except ValueError:
            self.correlation_scores = REGISTRY._names_to_collectors['l2_reasoner_correlation_score_distribution']
        
        # Signal source distribution
        try:
            self.source_distribution = Counter(
                'l2_reasoner_source_signals_total',
                'Total signals by source',
                ['source']
            )
        except ValueError:
            # Fallback to fetching from registry if already exists
            for collector in REGISTRY._collector_to_names:
                for name in REGISTRY._collector_to_names[collector]:
                    if name == 'l2_reasoner_source_signals_total':
                        self.source_distribution = collector
                        break
        
        # Category distribution
        try:
            self.category_distribution = Counter(
                'l2_reasoner_category_signals_total',
                'Total signals by category',
                ['category']
            )
        except ValueError:
            # Fallback to fetching from registry if already exists
            for collector in REGISTRY._collector_to_names:
                for name in REGISTRY._collector_to_names[collector]:
                    if name == 'l2_reasoner_category_signals_total':
                        self.category_distribution = collector
                        break
        
        # Window metrics
        try:
            self.window_signals_count = Histogram(
                'l2_reasoner_window_signals_count',
                'Number of signals in correlation windows',
                buckets=(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 15, 20, 30, 50)
            )
        except ValueError:
            self.window_signals_count = REGISTRY._names_to_collectors['l2_reasoner_window_signals_count']
        
        # Database metrics
        try:
            self.db_operations = Counter(
                'l2_reasoner_db_operations_total',
                'Total database operations',
                ['operation']
            )
        except ValueError:
            self.db_operations = REGISTRY._names_to_collectors['l2_reasoner_db_operations_total']
        
        try:
            self.db_latency = Histogram(
                'l2_reasoner_db_latency_seconds',
                'Database operation latency in seconds',
                ['operation'],
                buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5)
            )
        except ValueError:
            self.db_latency = REGISTRY._names_to_collectors['l2_reasoner_db_latency_seconds']
        
        # Kafka metrics
        try:
            self.kafka_messages_consumed = Counter(
                'l2_reasoner_kafka_messages_consumed_total',
                'Total messages consumed from Kafka'
            )
        except ValueError:
            self.kafka_messages_consumed = REGISTRY._names_to_collectors['l2_reasoner_kafka_messages_consumed_total']
        
        try:
            self.kafka_messages_produced = Counter(
                'l2_reasoner_kafka_messages_produced_total',
                'Total messages produced to Kafka',
                ['topic']
            )
        except ValueError:
            self.kafka_messages_produced = REGISTRY._names_to_collectors['l2_reasoner_kafka_messages_produced_total']
        
        # Health metrics
        try:
            self.health_status = Gauge(
                'l2_reasoner_health_status',
                'Health status of L2 Reasoner (1 = healthy, 0 = unhealthy)'
            )
        except ValueError:
            self.health_status = REGISTRY._names_to_collectors['l2_reasoner_health_status']
        
        try:
            self.consumer_lag = Gauge(
                'l2_reasoner_consumer_lag',
                'Consumer lag in messages'
            )
        except ValueError:
            self.consumer_lag = REGISTRY._names_to_collectors['l2_reasoner_consumer_lag']
        
        # Register all metrics
        self._register_metrics()
    
    def _register_metrics(self):
        """Register all metrics with Prometheus registry."""
        # Prometheus client automatically registers metrics on creation by default.
        # However, if we're re-initializing or something else is creating these metrics,
        # we might get duplicates. The safe way is to handle the registration explicitly
        # or rely on the default behavior but be resilient.
        
        # In this specific codebase/context, it seems we are hitting a case where
        # metrics are being created multiple times or conflicting with existing ones.
        # Since the Counters/Gauges register themselves by default with REGISTRY,
        # calling REGISTRY.register(attr) again in the loop below causes the "Duplicated" error.
        
        # The fix is to NOT manually register them again.
        # The previous attempt to just 'pass' was correct for avoiding the manual re-registration,
        # BUT the error log shows the error happens during __init__ when creating the Counter/Gauge objects themselves,
        # NOT in this method (although the traceback points here because __init__ calls this method, 
        # wait, no, the traceback says:
        # File "/app/l2_reasoner/metrics.py", line 228, in get_metrics
        #   _metrics_instance = L2ReasonerMetrics()
        # File "/app/l2_reasoner/metrics.py", line 143, in __init__
        #   self._register_metrics()
        # File "/app/l2_reasoner/metrics.py", line 150, in _register_metrics
        #   REGISTRY.register(attr)
        
        # So yes, the manual registration IS the problem.
        # My previous 'pass' fix was reverted because I edited the file back and forth.
        # I need to simply remove the manual registration loop.
        pass
    
    def record_signal_received(self, source: str):
        """Record a signal received from Kafka."""
        self.signals_received.labels(source=source).inc()
        self.source_distribution.labels(source=source).inc()
    
    def record_signal_processed(self, source: str):
        """Record a signal successfully processed."""
        self.signals_processed.labels(source=source).inc()
    
    def record_signal_failed(self, error_type: str):
        """Record a signal processing failure."""
        self.signals_failed.labels(error_type=error_type).inc()
    
    def record_entity_tracked(self, entity: str, signal_count: int):
        """Record entity tracking metrics."""
        self.entity_signals_count.labels(entity=entity).inc(signal_count)
    
    def record_rule_evaluation(self, rule_name: str, triggered: bool, severity: str | None = None, latency: float | None = None):
        """Record rule evaluation metrics."""
        self.rule_evaluations.labels(rule_name=rule_name).inc()
        if triggered and severity:
            self.rule_triggers.labels(rule_name=rule_name, severity=severity).inc()
        if latency is not None:
            self.rule_evaluation_latency.labels(rule_name=rule_name).observe(latency)
    
    def record_insight_generated(self, insight_type: str, severity: str):
        """Record insight generation."""
        self.insights_generated.labels(type=insight_type, severity=severity).inc()
    
    def record_processing_latency(self, latency: float):
        """Record total processing latency."""
        self.processing_latency.observe(latency)
    
    def record_correlation_score(self, score: float):
        """Record correlation score."""
        self.correlation_scores.observe(score)
    
    def record_category_signal(self, category: str):
        """Record signal by category."""
        self.category_distribution.labels(category=category).inc()
    
    def record_window_signals(self, count: int):
        """Record number of signals in correlation window."""
        self.window_signals_count.observe(count)
    
    def record_db_operation(self, operation: str, latency: float | None = None):
        """Record database operation."""
        self.db_operations.labels(operation=operation).inc()
        if latency is not None:
            self.db_latency.labels(operation=operation).observe(latency)
    
    def record_kafka_consumed(self):
        """Record Kafka message consumed."""
        self.kafka_messages_consumed.inc()
    
    def record_kafka_produced(self, topic: str):
        """Record Kafka message produced."""
        self.kafka_messages_produced.labels(topic=topic).inc()
    
    def set_health_status(self, healthy: bool):
        """Set health status."""
        self.health_status.set(1 if healthy else 0)
    
    def set_consumer_lag(self, lag: int):
        """Set consumer lag."""
        self.consumer_lag.set(lag)


# Global metrics instance
_metrics_instance = None


def get_metrics() -> L2ReasonerMetrics:
    """Get or create the global metrics instance."""
    global _metrics_instance
    if _metrics_instance is None:
        _metrics_instance = L2ReasonerMetrics()
    return _metrics_instance