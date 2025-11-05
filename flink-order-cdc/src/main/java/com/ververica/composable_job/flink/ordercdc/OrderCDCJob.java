package com.ververica.composable_job.flink.ordercdc;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.cdc.connectors.postgres.PostgreSQLSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.Objects;

/**
 * Flink CDC Job: PostgreSQL Order Items → Kafka
 *
 * This job demonstrates a real-time data pipeline using Change Data Capture (CDC).
 * It captures INSERT events from the 'order_items' table in PostgreSQL, transforms
 * the raw CDC event into a simple, clean JSON format, and publishes it to a Kafka topic
 * for downstream consumption by services like inventory management.
 *
 * ARCHITECTURE:
 * <pre>
 * PostgreSQL (orders + order_items tables)
 *   │
 *   ├─ INSERT/UPDATE/DELETE operations
 *   │
 *   ▼
 * Flink CDC Source (Debezium)
 *   │
 *   ├─ Captures changes via PostgreSQL Write-Ahead Log (WAL)
 *   ├─ Reads from a persistent replication slot
 *   │
 *   ▼
 * Filter & Transform
 *   │
 *   ├─ Filter for INSERT operations on the 'order_items' table
 *   ├─ Map the raw Debezium event to a simple JSON object
 *   │  (e.g., { "productId": ..., "quantity": ..., "orderId": ... })
 *   │
 *   ▼
 * Kafka Sink (order-events topic)
 *   │
 *   ├─ Downstream consumers: Inventory Job, Analytics, etc.
 *   │
 *   ▼
 * Inventory Deduction Service
 * </pre>
 *
 * KEY CONCEPTS:
 * - CDC (Change Data Capture): Capture database changes without polling.
 * - Debezium: Open-source CDC framework used by the Flink connector.
 * - WAL (Write-Ahead Log): PostgreSQL's transaction log, the source of changes.
 * - Replication Slot: A cursor that tracks the position in the WAL, ensuring no events are missed.
 *
 * SETUP REQUIREMENTS:
 * 1. PostgreSQL with `wal_level=logical` (configured in docker-compose.yml).
 * 2. A PostgreSQL Publication must be created. The name is configurable, e.g.:
 *    `CREATE PUBLICATION workshop_cdc FOR ALL TABLES;`
 * 3. A replication slot is automatically created by the Flink CDC source on first run.
 *
 * RUN THIS JOB:
 * <pre>
 * ./flink-2-order-cdc-job.sh
 * </pre>
 */
public class OrderCDCJob {

    private static final Logger LOG = LoggerFactory.getLogger(OrderCDCJob.class);

    public static void main(String[] args) throws Exception {

        // ========================================
        // STEP 1: Environment Setup
        // ========================================

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Load configuration from environment
        String postgresHost = System.getenv().getOrDefault("POSTGRES_HOST", "localhost");
        String postgresPort = System.getenv().getOrDefault("POSTGRES_PORT", "5432");
        String postgresDb = System.getenv().getOrDefault("POSTGRES_DB", "ecommerce");
        String postgresUser = System.getenv().getOrDefault("POSTGRES_USER", "postgres");
        String postgresPassword = System.getenv().getOrDefault("POSTGRES_PASSWORD", "postgres");
        String kafkaBootstrapServers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092");

        int parallelism = Integer.parseInt(System.getenv().getOrDefault("PARALLELISM", "1"));
        env.setParallelism(parallelism);

        LOG.info("🔧 Starting Order CDC Job");
        LOG.info("📊 Parallelism: {}", parallelism);
        LOG.info("💾 PostgreSQL: {}:{}/{}", postgresHost, postgresPort, postgresDb);
        LOG.info("🔧 Kafka: {}", kafkaBootstrapServers);

        // ========================================
        // STEP 2: Create PostgreSQL CDC Source
        // ========================================

        LOG.info("\n📥 Creating PostgreSQL CDC Source");
        LOG.info("   Database: {}", postgresDb);
        LOG.info("   Tables: orders, order_items");
        LOG.info("   Slot: flink_order_cdc_slot");

        Properties debeziumProps = new Properties();
        debeziumProps.setProperty("snapshot.mode", "initial");
        debeziumProps.setProperty("decimal.handling.mode", "double");

        SourceFunction<String> ordersCdcSource = PostgreSQLSource.<String>builder()
            .hostname(postgresHost)
            .port(Integer.parseInt(postgresPort))
            .database(postgresDb)
            .schemaList("public")
            .tableList("public.orders", "public.order_items")
            .username(postgresUser)
            .password(postgresPassword)
            .slotName("flink_order_cdc_slot")
            .decodingPluginName("pgoutput")
            .deserializer(new JsonDebeziumDeserializationSchema())
            .debeziumProperties(debeziumProps)
            .build();

        DataStream<String> cdcStream = env.addSource(
            ordersCdcSource,
            "PostgreSQL CDC Source (orders + order_items)"
        );

        // ========================================
        // STEP 3: Filter for INSERT Operations from "order_items"
        // ========================================

        LOG.info("\n🔍 Filtering CDC Events");
        LOG.info("   - Filter for 'op=c' (CREATE/INSERT) operations");
        LOG.info("   - Ignore UPDATE/DELETE for this demo");

        DataStream<String> newOrderItems = cdcStream
            .filter(new FilterFunction<String>() {
                @Override
                public boolean filter(String event) throws Exception {
                    // Filter for INSERT operations (op == 'c' in Debezium)
                    // and only from order_items table
                    return event.contains("\"op\":\"c\"") &&
                           event.contains("\"source\":{") &&
                           event.contains("\"table\":\"order_items\"");
                }
            })
            .name("Filter New Orders (INSERT only)");

        // =============================================================
        // STEP 4: Transform the event using the external Mapper class
        // Converts to eg)
        // {
        //     "quantity": 1,
        //     "productId": "PROD_0151",
        //     "orderId": "727237df-9fd6-44f8-8a46-6defe27c7585",
        //     "timestamp": 1762307641516
        // }
        // =============================================================
        LOG.info("\n✨ Transforming CDC events to custom format");

        DataStream<String> transformedStream = newOrderItems
                .map(new OrderItemEventMapper())
                .filter(Objects::nonNull);

        // ========================================
        // STEP 5: Kafka Sink for Transformed Events
        // ========================================

        LOG.info("\n📤 Configuring Kafka Sink");
        LOG.info("   Topic: order-events");
        LOG.info("   Bootstrap Servers: {}", kafkaBootstrapServers);

        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
            .setBootstrapServers(kafkaBootstrapServers)
            .setRecordSerializer(
                KafkaRecordSerializationSchema.builder()
                    .setTopic("order-events")
                    .setValueSerializationSchema(new SimpleStringSchema())
                    .build()
            )
            .build();

        transformedStream.sinkTo(kafkaSink)
            .name("Transformed Orders → Kafka (order-events)");

        // ========================================
        // STEP 6: Execute Job
        // ========================================

        LOG.info("\n✅ Job configured successfully!");
        LOG.info("🎯 CDC Flow:");
        LOG.info("   PostgreSQL orders → Flink CDC → Kafka order-events → Inventory Job");
        LOG.info("\n🚀 Executing job...\n");

        env.execute("Order CDC Job (PostgreSQL → Kafka)");
    }
}