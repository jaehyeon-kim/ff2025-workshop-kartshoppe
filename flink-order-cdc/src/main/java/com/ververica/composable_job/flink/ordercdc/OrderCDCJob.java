package com.ververica.composable_job.flink.ordercdc;

import com.ververica.cdc.connectors.postgres.PostgreSQLSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Flink CDC Job: PostgreSQL Orders → Kafka
 *
 * This job demonstrates PATTERN: Change Data Capture (CDC) for real-time order processing
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
 *   ├─ Reads from replication slot
 *   │
 *   ▼
 * JSON Deserialization
 *   │
 *   ├─ Parse CDC events (before/after values)
 *   ├─ Filter for INSERT operations (new orders)
 *   │
 *   ▼
 * Kafka Sink (order-events topic)
 *   │
 *   ├─ Downstream consumers: Inventory Job, Analytics, etc.
 *   │
 *   ▼
 * Inventory Deduction
 * </pre>
 *
 * KEY CONCEPTS:
 * - CDC (Change Data Capture): Capture database changes without polling
 * - Debezium: Open-source CDC framework
 * - WAL (Write-Ahead Log): PostgreSQL's transaction log
 * - Replication Slot: Persistent CDC position tracker
 *
 * SETUP REQUIREMENTS:
 * 1. PostgreSQL with wal_level=logical (already configured in docker-compose.yml)
 * 2. Publication created: CREATE PUBLICATION paimon_cdc FOR ALL TABLES;
 * 3. Replication slot (auto-created by Flink CDC)
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
        // STEP 3: Filter for INSERT Operations
        // ========================================

        LOG.info("\n🔍 Filtering CDC Events");
        LOG.info("   - Filter for 'op=c' (CREATE/INSERT) operations");
        LOG.info("   - Ignore UPDATE/DELETE for this demo");

        DataStream<String> newOrders = cdcStream
            .filter(new FilterFunction<String>() {
                @Override
                public boolean filter(String event) throws Exception {
                    // Filter for INSERT operations (op == 'c' in Debezium)
                    // and only from orders table
                    return event.contains("\"op\":\"c\"") &&
                           event.contains("\"source\":{") &&
                           event.contains("\"table\":\"orders\"");
                }
            })
            .name("Filter New Orders (INSERT only)");

        // ========================================
        // STEP 4: Log CDC Events (for debugging)
        // ========================================

        newOrders.map(event -> {
            LOG.info("📦 New Order CDC Event: {}", event.substring(0, Math.min(200, event.length())) + "...");
            return event;
        }).name("Log Order CDC Events");

        // ========================================
        // STEP 5: Kafka Sink for Order Events
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

        newOrders.sinkTo(kafkaSink)
            .name("Orders → Kafka (order-events)");

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
