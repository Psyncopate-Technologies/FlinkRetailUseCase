package org.psyncopate.flink;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cdc.connectors.mongodb.MongoDBSource;
import org.apache.flink.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.table.PartitionWriter.Context;
import org.apache.flink.connector.mongodb.source.MongoSource;
import org.apache.flink.connector.mongodb.source.enumerator.splitter.PartitionStrategy;
import org.apache.flink.connector.mongodb.source.reader.deserializer.MongoDeserializationSchema;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.bson.BsonDocument;
import org.psyncopate.flink.model.EnrichedOrderNotPlacedAlerts;
import org.psyncopate.flink.model.Event;
import org.psyncopate.flink.model.PortalViewAudit;
import org.psyncopate.flink.model.Shoe;
import org.psyncopate.flink.model.ShoeCustomer;
import org.psyncopate.flink.model.ShoeOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.delta.flink.sink.DeltaSink;

public class TriggerEventsForNullCase {
    @SuppressWarnings("deprecation")
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        final Logger logger = LoggerFactory.getLogger(TriggerEventsForNullCase.class);

        Properties mongoProperties = PropertyFilesLoader.loadProperties("mongodb.properties");
        Properties appconfigProperties = PropertyFilesLoader.loadProperties("appconfig.properties");
        Properties deltaLakeProperties = PropertyFilesLoader.loadProperties("delta-lake.properties");

        // Enable checkpointing for fault tolerance
        env.enableCheckpointing(30000, CheckpointingMode.EXACTLY_ONCE);
        org.apache.flink.configuration.Configuration config = new
        org.apache.flink.configuration.Configuration();
        config.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
        config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY,
        appconfigProperties.getProperty("faulttolerance.filesystem.scheme") +
        appconfigProperties.getProperty("checkpoint.location"));
        config.set(CheckpointingOptions.SAVEPOINT_DIRECTORY,
        appconfigProperties.getProperty("faulttolerance.filesystem.scheme") +
        appconfigProperties.getProperty("savepoint.location"));
        env.configure(config);

        ObjectMapper objectMapper = new ObjectMapper();

        // Create MongoSource for ShoeOrders Collection
        /* MongoSource<ShoeOrder> shoeorders = MongoSource.<ShoeOrder>builder()
                .setUri(mongoProperties.getProperty("mongodb.uri"))
                .setDatabase(mongoProperties.getProperty("mongodb.database"))
                .setCollection(appconfigProperties.getProperty("retail.orderplacement.collection.name"))
                .setFetchSize(2048)
                .setLimit(-1)
                .setNoCursorTimeout(true)
                .setPartitionStrategy(PartitionStrategy.SINGLE)
                .setPartitionSize(MemorySize.ofMebiBytes(64))
                .setDeserializationSchema(new MongoDeserializationSchema<ShoeOrder>() {
                    @Override
                    public ShoeOrder deserialize(BsonDocument document) {
                        ShoeOrder order = new ShoeOrder();
                        order.setProduct_id(document.getString("product_id").getValue());
                        order.setCustomer_id(document.getString("customer_id").getValue());
                        order.setTimestamp(new Date(document.getDateTime("timestamp").getValue()));
                        return order;
                    }

                    @Override
                    public TypeInformation<ShoeOrder> getProducedType() {
                        return TypeInformation.of(ShoeOrder.class);
                    }
                })
                .build(); */

        DebeziumSourceFunction<String> shoeOrdersString = MongoDBSource.<String>builder()
            .hosts(mongoProperties.getProperty("mongodb.host"))
            .username(mongoProperties.getProperty("mongodb.user"))
            .password(mongoProperties.getProperty("mongodb.password"))
            .databaseList(mongoProperties.getProperty("mongodb.database")) // set captured database, support regexs
            .collectionList(mongoProperties.getProperty("mongodb.database") + "." + appconfigProperties.getProperty("retail.orderplacement.collection.name")) //set captured collections, support regex
            .deserializer(new JsonDebeziumDeserializationSchema())
            .build();

        
        // Create MongoSource for ShoeOrders Collection
        /* MongoSource<PortalViewAudit> portalviews = MongoSource.<PortalViewAudit>builder()
                .setUri(mongoProperties.getProperty("mongodb.uri"))
                .setDatabase(mongoProperties.getProperty("mongodb.database"))
                .setCollection(appconfigProperties.getProperty("retail.portal.views.collection.name"))
                .setFetchSize(2048)
                .setLimit(-1)
                .setNoCursorTimeout(true)
                .setPartitionStrategy(PartitionStrategy.SINGLE)
                .setPartitionSize(MemorySize.ofMebiBytes(64))
                .setDeserializationSchema(new MongoDeserializationSchema<PortalViewAudit>() {
                    @Override
                    public PortalViewAudit deserialize(BsonDocument document) {
                        PortalViewAudit portalViews = new PortalViewAudit();
                        portalViews.setProduct_id(document.getString("product_id").getValue());
                        portalViews.setUser_id(document.getString("user_id").getValue());
                        portalViews.setTimestamp(new Date(document.getDateTime("timestamp").getValue()));
                        return portalViews;
                    }

                    @Override
                    public TypeInformation<PortalViewAudit> getProducedType() {
                        return TypeInformation.of(PortalViewAudit.class);
                    }
                })
                .build(); */

        DebeziumSourceFunction<String> portalviewsString = MongoDBSource.<String>builder()
            .hosts(mongoProperties.getProperty("mongodb.host"))
            .username(mongoProperties.getProperty("mongodb.user"))
            .password(mongoProperties.getProperty("mongodb.password"))
            .databaseList(mongoProperties.getProperty("mongodb.database")) // set captured database, support regexs
            .collectionList(mongoProperties.getProperty("mongodb.database") + "." + appconfigProperties.getProperty("retail.portal.views.collection.name")) //set captured collections, support regex
            .deserializer(new JsonDebeziumDeserializationSchema())
            .build();

        /* MongoSource<Shoe> shoeinventory = MongoSource.<Shoe>builder()
                .setUri(mongoProperties.getProperty("mongodb.uri"))
                .setDatabase(mongoProperties.getProperty("mongodb.database"))
                .setCollection(appconfigProperties.getProperty("retail.inventory.collection.name"))
                .setFetchSize(2048)
                .setLimit(-1)
                .setNoCursorTimeout(true)
                .setPartitionStrategy(PartitionStrategy.SINGLE)
                .setPartitionSize(MemorySize.ofMebiBytes(64))
                .setDeserializationSchema(new MongoDeserializationSchema<Shoe>() {
                    @Override
                    public Shoe deserialize(BsonDocument document) {
                        Shoe shoe = new Shoe();
                        shoe.setId(document.getString("id").getValue());
                        shoe.setBrand(document.getString("brand").getValue());
                        shoe.setName(document.getString("name").getValue());
                        shoe.setSale_price(document.getInt32("sale_price").getValue());
                        shoe.setRating(document.getDouble("rating").getValue());
                        shoe.setQuantity(document.getString("quantity").getValue());
                        shoe.setTimestamp(new Date(document.getDateTime("timestamp").getValue()));
                        return shoe;
                    }

                    @Override
                    public TypeInformation<Shoe> getProducedType() {
                        return TypeInformation.of(Shoe.class);
                    }
                })
                .build(); */

        DebeziumSourceFunction<String> shoeinventoryString = MongoDBSource.<String>builder()
            .hosts(mongoProperties.getProperty("mongodb.host"))
            .username(mongoProperties.getProperty("mongodb.user"))
            .password(mongoProperties.getProperty("mongodb.password"))
            .databaseList(mongoProperties.getProperty("mongodb.database")) // set captured database, support regexs
            .collectionList(mongoProperties.getProperty("mongodb.database") + "." + appconfigProperties.getProperty("retail.inventory.collection.name")) //set captured collections, support regex
            .deserializer(new JsonDebeziumDeserializationSchema())
            .build();

        // Create MongoSource for ShoeOrders Collection
        /* MongoSource<ShoeCustomer> shoecustomers = MongoSource.<ShoeCustomer>builder()
                .setUri(mongoProperties.getProperty("mongodb.uri"))
                .setDatabase(mongoProperties.getProperty("mongodb.database"))
                .setCollection(appconfigProperties.getProperty("retail.customer.collection.name"))
                .setFetchSize(2048)
                .setLimit(-1)
                .setNoCursorTimeout(true)
                .setPartitionStrategy(PartitionStrategy.SINGLE)
                .setPartitionSize(MemorySize.ofMebiBytes(64))
                .setDeserializationSchema(new MongoDeserializationSchema<ShoeCustomer>() {
                    @Override
                    public ShoeCustomer deserialize(BsonDocument document) {
                        ShoeCustomer customer = new ShoeCustomer();
                        customer.setId((document.getString("id").getValue()));
                        customer.setFirst_name(document.getString("first_name").getValue());
                        customer.setLast_name(document.getString("last_name").getValue());
                        customer.setEmail(document.getString("email").getValue());
                        customer.setPhone(document.getString("phone").getValue());
                        customer.setZip_code(document.getString("zip_code").getValue());
                        customer.setCountry(document.getString("country_code").getValue());
                        customer.setState(document.getString("state").getValue());
                        customer.setTimestamp(new Date(document.getDateTime("timestamp").getValue()));
                        return customer;
                    }

                    @Override
                    public TypeInformation<ShoeCustomer> getProducedType() {
                        return TypeInformation.of(ShoeCustomer.class);
                    }
                })
                .build(); */

        DebeziumSourceFunction<String> shoecustomersString = MongoDBSource.<String>builder()
            .hosts(mongoProperties.getProperty("mongodb.host"))
            .username(mongoProperties.getProperty("mongodb.user"))
            .password(mongoProperties.getProperty("mongodb.password"))
            .databaseList(mongoProperties.getProperty("mongodb.database")) // set captured database, support regexs
            .collectionList(mongoProperties.getProperty("mongodb.database") + "." + appconfigProperties.getProperty("retail.customer.collection.name")) //set captured collections, support regex
            .deserializer(new JsonDebeziumDeserializationSchema())
            .build();

        // Define watchlist datastream
        /* DataStream<PortalViewAudit> portalviews_ds = env
        .fromSource(portalviews, WatermarkStrategy
                .<PortalViewAudit>forBoundedOutOfOrderness(Duration.ofMinutes(10))
                .withTimestampAssigner((portalView, recordTimestamp) -> portalView.getTimestamp().getTime()), 
                "Read Portal Views from MongoDB")
        .setParallelism(1)
        .name("Read Portal Views"); */

        DataStream<PortalViewAudit> portalviews_ds = env.addSource(portalviewsString).map(data -> {
    
            JsonNode rootNode = objectMapper.readTree(data);
            String fullDocument = rootNode.get("fullDocument").asText();
            PortalViewAudit portalView = objectMapper.readValue(fullDocument, PortalViewAudit.class);
            return portalView;
        })
        .assignTimestampsAndWatermarks(WatermarkStrategy.<PortalViewAudit>forBoundedOutOfOrderness(Duration.ofMinutes(10))
            .withTimestampAssigner((portalView, recordTimestamp) -> portalView.getTimestamp().getTime()))
        .setParallelism(1)
        .name("Read Portal Views");

         // Define shoeorders datastream
        /* DataStream<ShoeOrder> shoeorders_ds = env
                .fromSource(shoeorders, WatermarkStrategy
                        .<ShoeOrder>forBoundedOutOfOrderness(Duration.ofMinutes(10))
                        .withTimestampAssigner((order, recordTimestamp) -> order.getTimestamp().getTime()), 
                        "Read Shoe Orders from MongoDB")
                .setParallelism(1)
                .name("Read Shoe Orders Placed"); */

        DataStream<ShoeOrder> shoeorders_ds = env.addSource(shoeOrdersString).map(data -> {
    
            JsonNode rootNode = objectMapper.readTree(data);
            String fullDocument = rootNode.get("fullDocument").asText();
            ShoeOrder shoeOrder = objectMapper.readValue(fullDocument, ShoeOrder.class);
            return shoeOrder;
        })
        .assignTimestampsAndWatermarks(WatermarkStrategy.<ShoeOrder>forBoundedOutOfOrderness(Duration.ofMinutes(10))
            .withTimestampAssigner((shoeOrder, recordTimestamp) -> shoeOrder.getTimestamp().getTime()))
        .setParallelism(1)
        .name("Read Shoe Orders Placed");

        /* DataStream<ShoeCustomer> shoecustomers_ds = env
                .fromSource(shoecustomers, WatermarkStrategy
                .<ShoeCustomer>forMonotonousTimestamps()
                .withTimestampAssigner((shoecustomer, recordTimestamp) -> shoecustomer.getTimestamp().getTime()),  "Read Customers from MongoDB")
                .setParallelism(1).name("Read Customers"); */

        DataStream<ShoeCustomer> shoecustomers_ds = env.addSource(shoecustomersString).map(data -> {

            JsonNode rootNode = objectMapper.readTree(data);
            String fullDocument = rootNode.get("fullDocument").asText();
            ShoeCustomer shoeCust = objectMapper.readValue(fullDocument, ShoeCustomer.class);
            return shoeCust;
        })
        .assignTimestampsAndWatermarks(WatermarkStrategy.<ShoeCustomer>forBoundedOutOfOrderness(Duration.ofMinutes(10))
            .withTimestampAssigner((shoeCust, recordTimestamp) -> shoeCust.getTimestamp().getTime()))
        .setParallelism(1)
        .name("Read Customers");

        // Map each stream to the base Event type
        DataStream<Event> portalviewsEventStream = portalviews_ds.map(view -> (Event) view);
        DataStream<Event> shoeordersEventStream = shoeorders_ds.map(order -> (Event) order);

        // Union both streams into a single stream of Event
        DataStream<Event> unifiedStream = portalviewsEventStream.union(shoeordersEventStream);

        //unifiedStream.print("Unified Stream");
        
        // Define the CEP pattern
        Pattern<Event, ?> pattern = Pattern.<Event>begin("view")
                .subtype(PortalViewAudit.class)
                .where(new SimpleCondition<PortalViewAudit>() {
                    @Override
                    public boolean filter(PortalViewAudit view) {
                        //System.out.println("Portal View Event===>"+ view);
                        return view.getProduct_id() != null && view.getCustomer_id() != null;
                    }
                })
                .next("order")
                .subtype(ShoeOrder.class)
                .where(new IterativeCondition<ShoeOrder>() {
                    @Override
                    public boolean filter(ShoeOrder order, Context<ShoeOrder> ctx) throws Exception {
                        //System.out.println("Order Event===>"+ order);
                        for (Event prev : ctx.getEventsForPattern("view")) {
                            PortalViewAudit prevEvent = (PortalViewAudit)prev;
                            //System.out.println("Prev posrtal visit Event===>"+ prevEvent);
                            return order.getProduct_id().equals(prevEvent.getProduct_id())
                                    && order.getCustomer_id().equals(prevEvent.getCustomer_id());
                        }
                        return false;
                    }
                })
                .within(Duration.ofDays(1));
                

        // Apply pattern to the unifiedStream
        PatternStream<Event> patternStream = CEP.pattern(unifiedStream.keyBy(Event::getCustomer_id), pattern);

        

        // Timeout tag for unmatched events
        OutputTag<AlertEvent> timeoutTag = new OutputTag<AlertEvent>("order-not-placed") {};

        // Select matching and timeout events
        SingleOutputStreamOperator<AlertEvent> alerts = patternStream.select(
                timeoutTag,
                new PatternTimeoutFunction<Event, AlertEvent>() {
                    @Override
                    public AlertEvent timeout(Map<String, List<Event>> pattern, long timeoutTimestamp) {
                        PortalViewAudit view = (PortalViewAudit) pattern.get("view").get(0);
                        return new AlertEvent(view.getProduct_id(), view.getCustomer_id(), "Order not placed");
                    }
                },
                new PatternSelectFunction<Event, AlertEvent>() {
                    @Override
                    public AlertEvent select(Map<String, List<Event>> pattern) {
                        PortalViewAudit view = (PortalViewAudit) pattern.get("view").get(0);
                        return new AlertEvent(view.getProduct_id(), view.getCustomer_id(), "Order placed");
                    }
                }
        );

        // Retrieve only the timeout events
        DataStream<AlertEvent> orderNotPlacedAlerts = alerts.getSideOutput(timeoutTag);
        //alerts.print("Matched Events");
        
        /* DataStream<Shoe> shoes_ds = env
            .fromSource(shoeinventory, WatermarkStrategy
            .<Shoe>forMonotonousTimestamps()
            .withTimestampAssigner((shoe, recordTimestamp) -> shoe.getTimestamp().getTime()),  "Read Shoes Inventory from MongoDB")
            .setParallelism(1)
            .name("Retrieve Shoes Inventory"); */

        DataStream<Shoe> shoes_ds = env.addSource(shoeinventoryString).map(data -> {

            JsonNode rootNode = objectMapper.readTree(data);
            String fullDocument = rootNode.get("fullDocument").asText();
            Shoe shoe = objectMapper.readValue(fullDocument, Shoe.class);
            return shoe;
        })
        .assignTimestampsAndWatermarks(WatermarkStrategy.<Shoe>forBoundedOutOfOrderness(Duration.ofMinutes(10))
            .withTimestampAssigner((shoe, recordTimestamp) -> shoe.getTimestamp().getTime()))
        .setParallelism(1)
        .name("Retrieve Shoes Inventory");
        // Key the shoes stream by product ID
        KeyedStream<Shoe, String> keyedShoes = shoes_ds.keyBy(Shoe::getId);

        // Key the shoes stream by product ID
        KeyedStream<ShoeCustomer, String> keyedCustomers = shoecustomers_ds.keyBy(ShoeCustomer::getId);

        // Connect and process the two streams
        DataStream<EnrichedOrderNotPlacedAlerts> productUserEnrichedOrderNotPlacedAlerts = orderNotPlacedAlerts.keyBy(AlertEvent :: getProductId)
        .connect(keyedShoes)
        .process(new ProductMetadataEnricher()).name("Enrich Product Details").keyBy(EnrichedOrderNotPlacedAlerts :: getUserId).connect(keyedCustomers).process(new UserMetadataEnricher()).name("Enrich User Details");




        String deltaTablePath = deltaLakeProperties.getProperty("storage.filesystem.scheme")
                + deltaLakeProperties.getProperty("storage.filesystem.s3.bucket.name") + "/"
                + deltaLakeProperties.getProperty("deltalake.watch.list.unmatched.events");

        // Define the schema for the RowData
        RowType rowType = new RowType(Arrays.asList(
                new RowType.RowField("productId", new VarCharType()),
                new RowType.RowField("productName", new VarCharType()),
                new RowType.RowField("userId", new VarCharType()),
                new RowType.RowField("userName", new VarCharType()),
                new RowType.RowField("reason", new VarCharType()),
                new RowType.RowField("state", new VarCharType()),
                new RowType.RowField("zip", new VarCharType())));

        // Create and add the Delta sink
        DataStream<RowData> product_count_map = productUserEnrichedOrderNotPlacedAlerts.map( order_not_placed -> {
            logger.info("Latest Product added to cart but not purchased by the user--->: "+ order_not_placed );
            //System.out.println("Latest Product added to cart but not purchased by the user--->: "+ order_not_placed );
            GenericRowData rowData = new GenericRowData(RowKind.INSERT, 7); 
            rowData.setField(0, StringData.fromString(order_not_placed.getProductId()));
            rowData.setField(1, StringData.fromString(order_not_placed.getProductName()));
            rowData.setField(2, StringData.fromString(order_not_placed.getUserId()));
            rowData.setField(3, StringData.fromString(order_not_placed.getUserName()));
            rowData.setField(4, StringData.fromString(order_not_placed.getReason()));
            rowData.setField(5, StringData.fromString(order_not_placed.getState()));
            rowData.setField(6, StringData.fromString(order_not_placed.getZip()));
            return rowData;
        });

        createDeltaSink(product_count_map, deltaTablePath, rowType);

        //alerts.print("Matched Events");

        env.execute("CEP Pattern for Order not placed alerts");

        


    }
    // Define the AlertEvent class
    public static class AlertEvent {
        private String productId;
        private String userId;
        private String reason;

        public AlertEvent(String productId, String userId, String reason) {
            this.productId = productId;
            this.userId = userId;
            this.reason = reason;
        }

        // Getters and toString() method for logging or debugging
        public String getProductId() { return productId; }
        public String getUserId() { return userId; }
        public String getReason() { return reason; }

        @Override
        public String toString() {
            return "AlertEvent{" +
                    "productId='" + productId + '\'' +
                    ", userId='" + userId + '\'' +
                    ", reason='" + reason + '\'' +
                    '}';
        }
    }

    // Function to create Delta sink for RowData
    public static DataStream<RowData> createDeltaSink(
            DataStream<RowData> stream,
            String deltaTablePath,
            RowType rowType) {

        // Hadoop configuration for AWS S3 access
        org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();

        String[] partitionCols = { "userId" };
        DeltaSink<RowData> deltaSink = DeltaSink
                .forRowData(
                        new Path(deltaTablePath), // Path to Delta Lake
                        hadoopConf, // Delta Lake configuration
                        rowType) // RowType defining the schema
                .withPartitionColumns(partitionCols)
                .build();

        stream.sinkTo(deltaSink).name("Write as Delta lake table");
        return stream;
    }

    // KeyedCoProcessFunction to enrich ProductViewCount with Shoe metadata
    public static class ProductMetadataEnricher extends KeyedCoProcessFunction<String, AlertEvent, Shoe, EnrichedOrderNotPlacedAlerts> {
        // State to store Shoe metadata
        private MapState<String, Shoe> shoeState;
        private ListState<AlertEvent> bufferedProducts;

        @Override
        public void open(Configuration parameters) throws Exception {
            MapStateDescriptor<String, Shoe> descriptor = new MapStateDescriptor<>(
                "shoeState", // State name
                String.class, // Key type
                Shoe.class // Value type (Shoe class)
            );
            shoeState = getRuntimeContext().getMapState(descriptor);

            ListStateDescriptor<AlertEvent> viewCountDescriptor = new ListStateDescriptor<>(
                "bufferedViewCounts",
                AlertEvent.class
            );
            bufferedProducts = getRuntimeContext().getListState(viewCountDescriptor);
        }

        @Override
        public void processElement1(AlertEvent orderNotPlacedAlert, Context ctx, Collector<EnrichedOrderNotPlacedAlerts> out) throws Exception {
            // Enrich ProductViewCount with Shoe metadata if available
            Shoe shoe = shoeState.get(orderNotPlacedAlert.getProductId());
            if (shoe != null) {
                out.collect(new EnrichedOrderNotPlacedAlerts(orderNotPlacedAlert.getProductId(), shoe.getName(), orderNotPlacedAlert.getUserId(), "", orderNotPlacedAlert.getReason(), "", ""));
            }else {
                // Buffer the event if shoe metadata is not available
                bufferedProducts.add(orderNotPlacedAlert);
            }
        }

        @Override
        public void processElement2(Shoe shoe, Context ctx, Collector<EnrichedOrderNotPlacedAlerts> out) throws Exception {
            // Update state with new Shoe metadata
            shoeState.put(shoe.getId(), shoe);

            List<AlertEvent> nonMatchingBufferedProducts = new ArrayList<>();

            // Emit buffered ProductViewCounts if available
            for (AlertEvent bufferedProd : bufferedProducts.get()) {
                if (bufferedProd.getProductId().equals(shoe.getId())) {
                    out.collect(new EnrichedOrderNotPlacedAlerts(bufferedProd.getProductId(), shoe.getName(), bufferedProd.getUserId(), "", bufferedProd.getReason(), "", ""));
                }else {
                    // Store non-matching counts for later
                    nonMatchingBufferedProducts.add(bufferedProd);
                }
                
            }
            // Clear buffered state
            bufferedProducts.clear();
            for (AlertEvent product : nonMatchingBufferedProducts) {
                bufferedProducts.add(product);
            }
        }
    }

    // KeyedCoProcessFunction to enrich ProductViewCount with Shoe metadata
    public static class UserMetadataEnricher extends KeyedCoProcessFunction<String, EnrichedOrderNotPlacedAlerts, ShoeCustomer, EnrichedOrderNotPlacedAlerts> {
        // State to store Shoe metadata
        private MapState<String, ShoeCustomer> customerState;
        private ListState<EnrichedOrderNotPlacedAlerts> bufferedProducts;

        @Override
        public void open(Configuration parameters) throws Exception {
            MapStateDescriptor<String, ShoeCustomer> descriptor = new MapStateDescriptor<>(
                "customerState", // State name
                String.class, // Key type
                ShoeCustomer.class // Value type (ShoeCustomer class)
            );
            customerState = getRuntimeContext().getMapState(descriptor);

            ListStateDescriptor<EnrichedOrderNotPlacedAlerts> viewCountDescriptor = new ListStateDescriptor<>(
                "bufferedViewCounts",
                EnrichedOrderNotPlacedAlerts.class
            );
            bufferedProducts = getRuntimeContext().getListState(viewCountDescriptor);
        }

        @Override
        public void processElement1(EnrichedOrderNotPlacedAlerts partial_enrichedOrderNotPlacedAlert, Context ctx, Collector<EnrichedOrderNotPlacedAlerts> out) throws Exception {
            // Enrich ProductViewCount with Shoe metadata if available
            ShoeCustomer cust = customerState.get(partial_enrichedOrderNotPlacedAlert.getUserId());
            if (cust != null) {
                out.collect(new EnrichedOrderNotPlacedAlerts(partial_enrichedOrderNotPlacedAlert.getProductId(), partial_enrichedOrderNotPlacedAlert.getProductName(), partial_enrichedOrderNotPlacedAlert.getUserId(), cust.getFirst_name()+" "+cust.getLast_name(), partial_enrichedOrderNotPlacedAlert.getReason(), cust.getState(), cust.getZip_code()));
            }else {
                // Buffer the event if shoe metadata is not available
                bufferedProducts.add(partial_enrichedOrderNotPlacedAlert);
            }
        }

        @Override
        public void processElement2(ShoeCustomer cust, Context ctx, Collector<EnrichedOrderNotPlacedAlerts> out) throws Exception {
            // Update state with new Shoe metadata
            customerState.put(cust.getId(), cust);

            List<EnrichedOrderNotPlacedAlerts> nonMatchingBufferedProducts = new ArrayList<>();

            // Emit buffered ProductViewCounts if available
            for (EnrichedOrderNotPlacedAlerts bufferedProd : bufferedProducts.get()) {
                if (bufferedProd.getUserId().equals(cust.getId())) {
                    out.collect(new EnrichedOrderNotPlacedAlerts(bufferedProd.getProductId(), bufferedProd.getProductName(), bufferedProd.getUserId(), cust.getFirst_name()+" "+cust.getLast_name(), bufferedProd.getReason(), cust.getState(), cust.getZip_code()));
                }else {
                    // Store non-matching counts for later
                    nonMatchingBufferedProducts.add(bufferedProd);
                }
                
            }
            // Clear buffered state
            bufferedProducts.clear();
            for (EnrichedOrderNotPlacedAlerts product : nonMatchingBufferedProducts) {
                bufferedProducts.add(product);
            }
        }
    }
}