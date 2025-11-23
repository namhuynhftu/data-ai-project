from elt_pipeline.streaming.ops.sink.kafka_sink_manager import KafkaToMinIOSink
if __name__ == "__main__":
    sink = KafkaToMinIOSink(
        topics=[
            "postgres.streaming.users",
            "postgres.streaming.transactions",
            "postgres.streaming.detailed_transactions",
        ]
    )
    sink.run()
