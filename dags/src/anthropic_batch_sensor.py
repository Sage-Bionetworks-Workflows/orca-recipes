from airflow.sensors.base import BaseSensorOperator

from src.anthropic_hook import AnthropicHook


class AnthropicBatchSensor(BaseSensorOperator):
    """Waits for an Anthropic message batch to reach 'ended' status.

    Use mode='reschedule' so the worker slot is freed between pokes.
    """

    template_fields = ("batch_id",)

    def __init__(self, *, batch_id: str, anthropic_conn_id: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.batch_id = batch_id
        self.anthropic_conn_id = anthropic_conn_id

    def poke(self, context) -> bool:
        if not self.batch_id:
            return True

        hook = AnthropicHook(self.anthropic_conn_id)
        batch = hook.client.messages.batches.retrieve(self.batch_id)
        self.log.info(
            "Batch %s: %s (%d succeeded, %d errored, %d processing)",
            self.batch_id,
            batch.processing_status,
            batch.request_counts.succeeded,
            batch.request_counts.errored,
            batch.request_counts.processing,
        )
        return batch.processing_status == "ended"
