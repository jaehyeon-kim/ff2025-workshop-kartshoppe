package com.ververica.composable_job.flink.inventory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ververica.composable_job.flink.inventory.patterns.hybrid_source.HybridSourceExample;
import com.ververica.composable_job.flink.inventory.shared.config.InventoryConfig;
import com.ververica.composable_job.flink.inventory.shared.model.AlertEvent;
import com.ververica.composable_job.flink.inventory.shared.model.InventoryEvent;
import com.ververica.composable_job.flink.inventory.shared.model.OrderItemDeduction;
import com.ververica.composable_job.flink.inventory.shared.processor.*;
import com.ververica.composable_job.model.ecommerce.Product;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.base.source.hybrid.HybridSource;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Inventory Management Job WITH ORDER DEDUCTION - Advanced Pattern Composition
 *
 * This enhanced version adds PATTERN 05: Multiple Sources + Order Processing
 *
 * PATTERNS DEMONSTRATED:
 *
 * 1. PATTERN 01: Hybrid Source (Bounded → Unbounded)
 *    - Bootstrap from file: data/initial-products.json
 *    - Then stream from Kafka: product-updates topic
 *
 * 2. PATTERN 02: Keyed State
 *    - Track inventory per product using ValueState
 *    - Shared state between product updates and order deductions
 *
 * 3. PATTERN 03: Timers
 *    - Detect stale inventory (no updates for 1 hour)
 *    - Processing time timers for each product
 *
 * 4. PATTERN 04: Side Outputs
 *    - Route alerts to different streams
 *    - LOW_STOCK, OUT_OF_STOCK, PRICE_DROP alerts
 *
 * 5. PATTERN 05: Multiple Sources (NEW!)
 *    - Product updates source (hybrid: file + Kafka)
 *    - Order events source (Kafka only)
 *    - Both update the same keyed state
 *
 * 6. PATTERN 06: Real-time Inventory Deduction (NEW!)
 *    - Orders placed → inventory deducted
 *    - Out-of-stock detection
 *    - Complete e-commerce flow
 *
 * ARCHITECTURE:
 * <pre>
 * Product File  ──┐
 *                 ├─→ Hybrid Source → Product Parser ──┐
 * Product Kafka ──┘                                     │
 *                                                       ▼
 *                                          Shared Keyed State (per product)
 *                                                       ▲
 * Order Kafka ───→ Order Parser → Deduction Function ──┘
 *                                          │
 *                    ┌─────────────────────┼─────────────────────┐
 *                    ▼                     ▼                     ▼
 *              Inventory Events    Low Stock Alerts    Out of Stock Alerts
 *                    │                     │                     │
 *                    ▼                     └─────────────────────┘
 *            Kafka: inventory-events         Kafka: inventory-alerts
 *                    │
 *                    ▼
 *            Kafka: websocket_fanout (Real-time UI updates)
 * </pre>
 *
 * LEARNING OUTCOMES:
 * - Understand how multiple sources can update the same keyed state
 * - See real-time inventory depletion in action
 * - Learn complete event-driven e-commerce architecture
 * - PostgreSQL → Quarkus → Kafka → Flink → UI (full stack)
 *
 * RUN THIS JOB:
 * <pre>
 * # 1. Start infrastructure
 * docker compose up -d
 *
 * # 2. Setup Kafka topics
 * ./0-setup-topics.sh
 *
 * # 3. Run this job
 * ./flink-1b-inventory-with-orders-job.sh
 *
 * # 4. Place orders in the UI
 * # Watch inventory decrease in real-time!
 * </pre>
 */
public class InventoryManagementJobWithOrders {

    private static final Logger LOG = LoggerFactory.getLogger(InventoryManagementJobWithOrders.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {

        // ========================================
        // STEP 1: Setup Environment & Configuration
        // ========================================

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        InventoryConfig config = InventoryConfig.fromEnvironment();

        env.setParallelism(config.getParallelism());
        env.enableCheckpointing(config.getCheckpointInterval());

        LOG.info("🚀 Starting Inventory Management Job WITH ORDER DEDUCTION");
        LOG.info("📊 Parallelism: {}", config.getParallelism());
        LOG.info("💾 Checkpoint interval: {}ms", config.getCheckpointInterval());
        LOG.info("🆕 NEW: This job processes orders and deducts inventory!");

        // ========================================
        // STEP 2: PATTERN 01 - Hybrid Source (Products)
        // ========================================

        LOG.info("\n📥 PATTERN 01: Creating Product Hybrid Source (File → Kafka)");

        HybridSource<String> productSource = HybridSourceExample.createHybridSource(
            config.getInitialProductsFile(),
            config.getKafkaBootstrapServers()
        );

        DataStream<String> rawProductStream = env.fromSource(
            productSource,
            WatermarkStrategy.noWatermarks(),
            "Product Hybrid Source",
            Types.STRING
        );

        DataStream<Product> productStream = rawProductStream
            .process(new ProductParser())
            .name("Parse Product JSON")
            .uid("product-parser");

        // =============================================================
        // NEW STEP: Sink Processed Products to 'products' topic
        // =============================================================
        LOG.info("\n📤 Sinking clean product data to 'products' topic for other services");

        KafkaSink<String> productsSink = KafkaSink.<String>builder()
            .setBootstrapServers(config.getKafkaBootstrapServers())
            .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                .setTopic("products")
                .setValueSerializationSchema(new SimpleStringSchema())
                .build()
            )
            .build();

        productStream
            .map(product -> MAPPER.writeValueAsString(product))
            .sinkTo(productsSink)
            .name("Sink to products topic")
            .uid("products-sink");

        // ========================================
        // STEP 3: PATTERN 05 - Order Events Source (NEW!)
        // ========================================

        LOG.info("\n📦 PATTERN 05: Creating Order Events Source (Kafka)");
        LOG.info("   Topic: order-events");
        LOG.info("   Purpose: Real-time inventory deduction from orders");

        KafkaSource<String> orderEventsSource = KafkaSource.<String>builder()
            .setBootstrapServers(config.getKafkaBootstrapServers())
            .setTopics("order-events")
            .setGroupId("inventory-order-consumer")
            .setStartingOffsets(OffsetsInitializer.latest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        DataStream<String> rawOrderStream = env.fromSource(
            orderEventsSource,
            WatermarkStrategy.noWatermarks(),
            "Order Events Source"
        );

        DataStream<OrderItemDeduction> orderStream = rawOrderStream
            .process(new OrderItemParser())
            .name("Parse Order Item JSON")
            .uid("order-parser");

        // ========================================
        // STEP 4, 5, 6, 7 COMBINED: Connect streams for shared state processing
        // ========================================
        LOG.info("\n🤝 Connecting product and order streams to operate on a single, shared state...");

        SingleOutputStreamOperator<InventoryEvent> allInventoryEvents = productStream
                .keyBy(product -> product.productId)
                .connect(orderStream.keyBy(order -> order.productId))
                .process(new SharedInventoryProcessor())
                .name("Shared Inventory State Processor")
                .uid("shared-inventory-state");

        // Extract the side outputs for alerts directly from the new, single operator.
        DataStream<AlertEvent> allAlerts = allInventoryEvents.getSideOutput(SharedInventoryProcessor.LOW_STOCK_TAG)
                .union(allInventoryEvents.getSideOutput(SharedInventoryProcessor.OUT_OF_STOCK_TAG),
                    allInventoryEvents.getSideOutput(SharedInventoryProcessor.PRICE_DROP_TAG));

        ////////////////////////////////////////////////////////////////////
        ////////////////////////////////////////////////////////////////////
        // // ========================================
        // // STEP 4: PATTERNS 02, 03, 04 - Product Inventory Tracking
        // // ========================================

        // LOG.info("\n🔧 PATTERNS 02, 03, 04: Product Inventory State Management");

        // SingleOutputStreamOperator<InventoryEvent> productInventoryEvents = productStream
        //     .keyBy(product -> product.productId)
        //     .process(new InventoryStateFunction())
        //     .name("Product Inventory State (Patterns 02+03+04)")
        //     .uid("product-inventory-state");

        // // ========================================
        // // STEP 5: PATTERN 06 - Inventory Deduction from Orders (NEW!)
        // // ========================================

        // LOG.info("\n📉 PATTERN 06: Inventory Deduction from Orders");
        // LOG.info("   - Orders placed → inventory deducted");
        // LOG.info("   - Shares same keyed state as product updates");
        // LOG.info("   - Detects out-of-stock scenarios");

        // SingleOutputStreamOperator<InventoryEvent> orderInventoryEvents = orderStream
        //     .keyBy(order -> order.productId)
        //     .process(new InventoryDeductionFunction())
        //     .name("Order Inventory Deduction (Pattern 06)")
        //     .uid("order-inventory-deduction");

        // // ========================================
        // // STEP 6: Union Both Event Streams
        // // ========================================

        // LOG.info("\n🔀 Combining Product Updates + Order Deductions");

        // DataStream<InventoryEvent> allInventoryEvents = productInventoryEvents
        //     .union(orderInventoryEvents);

        // // ========================================
        // // STEP 7: PATTERN 04 - Extract Side Outputs
        // // ========================================

        // LOG.info("\n🎯 PATTERN 04: Extracting Alert Side Outputs");

        // DataStream<AlertEvent> lowStockAlerts = productInventoryEvents
        //     .getSideOutput(InventoryStateFunction.LOW_STOCK_TAG);

        // DataStream<AlertEvent> outOfStockAlerts = productInventoryEvents
        //     .getSideOutput(InventoryStateFunction.OUT_OF_STOCK_TAG);

        // DataStream<AlertEvent> priceDropAlerts = productInventoryEvents
        //     .getSideOutput(InventoryStateFunction.PRICE_DROP_TAG);

        // DataStream<AlertEvent> allAlerts = lowStockAlerts
        //     .union(outOfStockAlerts, priceDropAlerts);
        ////////////////////////////////////////////////////////////////////
        ////////////////////////////////////////////////////////////////////

        // ========================================
        // STEP 8: Sinks - Output to Kafka
        // ========================================

        LOG.info("\n📤 Configuring Kafka Sinks");

        // All inventory events (product updates + order deductions)
        SinkFactory.sinkInventoryEvents(allInventoryEvents, config);

        // Alerts
        SinkFactory.sinkAlerts(allAlerts, config);

        // WebSocket for real-time UI updates
        SinkFactory.sinkToWebSocket(allInventoryEvents, config);

        // ========================================
        // STEP 9: Execute Job
        // ========================================

        LOG.info("\n✅ Job configured with COMPLETE inventory depletion!");
        LOG.info("🎯 Pattern Summary:");
        LOG.info("   01. Hybrid Source: ✓ (File → Kafka products)");
        LOG.info("   02. Keyed State: ✓ (Shared inventory state)");
        LOG.info("   03. Timers: ✓ (Stale detection)");
        LOG.info("   04. Side Outputs: ✓ (Alert routing)");
        LOG.info("   05. Multiple Sources: ✓ (Products + Orders)");
        LOG.info("   06. Order Deduction: ✓ (Real-time inventory)");
        LOG.info("\n💡 TRY IT:");
        LOG.info("   1. Place an order in the UI");
        LOG.info("   2. Watch inventory decrease in real-time!");
        LOG.info("   3. See out-of-stock alerts when inventory = 0");
        LOG.info("\n🚀 Executing job...\n");

        env.execute("Inventory Management Job WITH ORDER DEDUCTION");
    }
}
