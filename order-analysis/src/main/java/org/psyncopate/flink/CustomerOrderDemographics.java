package org.psyncopate.flink;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cdc.connectors.mongodb.MongoDBSource;
import org.apache.flink.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.mongodb.source.MongoSource;
import org.apache.flink.connector.mongodb.source.enumerator.splitter.PartitionStrategy;
import org.apache.flink.connector.mongodb.source.reader.deserializer.MongoDeserializationSchema;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.bson.BsonDocument;
import org.bson.Document;
import org.psyncopate.flink.model.CustomerCount;
import org.psyncopate.flink.model.Shoe;
import org.psyncopate.flink.model.ShoeCustomer;
import org.psyncopate.flink.model.ShoeOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.delta.flink.sink.DeltaSink;

public class CustomerOrderDemographics {
    @SuppressWarnings("deprecation")
    public static void main(String[] args) throws Exception {
        // Initialize Logger
        final Logger logger = LoggerFactory.getLogger(CustomerOrderDemographics.class);

        //LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

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
                        order.setOrder_id(document.getInt32("order_id").getValue());
                        order.setProduct_id(document.getString("product_id").getValue());
                        order.setCustomer_id(document.getString("customer_id").getValue());
                        order.setTs(new Date(document.getDateTime("ts").getValue()));
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
                        customer.setFirstName(document.getString("first_name").getValue());
                        customer.setLastName(document.getString("last_name").getValue());
                        customer.setEmail(document.getString("email").getValue());
                        customer.setPhone(document.getString("phone").getValue());
                        customer.setZipCode(document.getString("zip_code").getValue());
                        customer.setCountry(document.getString("country_code").getValue());
                        customer.setState(document.getString("state").getValue());
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

        /* DataStream<Shoe> shoes_ds = env
                .fromSource(shoeinventory, WatermarkStrategy.noWatermarks(), "Read Shoes Inventory from MongoDB")
                .setParallelism(1).name("Retrieve Shoes Inventory"); */

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

        /* DataStream<ShoeOrder> shoeorders_ds = env
                .fromSource(shoeorders, WatermarkStrategy.noWatermarks(), "Read Shoe Orders from MongoDB")
                .setParallelism(1).name("Read Shoe Orders"); */

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


       /*  DataStream<ShoeCustomer> shoecustomers_ds = env
                .fromSource(shoecustomers, WatermarkStrategy.noWatermarks(), "Read Shoe Customers from MongoDB")
                .setParallelism(1).name("Read Shoe Customer"); */

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
        
        // Key the streams by customerId for joining
        KeyedStream<ShoeOrder, String> keyedOrders = shoeorders_ds.keyBy(ShoeOrder :: getCustomer_id);
        KeyedStream<ShoeCustomer, String> keyedCustomers = shoecustomers_ds.keyBy(ShoeCustomer :: getId);

        DataStream<CustomerCount> customerCountStream = keyedOrders
        .connect(keyedCustomers)
        .process(new OrderCustomerJoinFunction()).name("Compute Orders per customer in every state");

        
        String deltaTablePath = deltaLakeProperties.getProperty("storage.filesystem.scheme")
                + deltaLakeProperties.getProperty("storage.filesystem.s3.bucket.name") + "/"
                + deltaLakeProperties.getProperty("deltalake.cutomer.order.demographics.table.name");

        // Define the schema for the RowData
        RowType rowType = new RowType(Arrays.asList(
                new RowType.RowField("state", new VarCharType()),
                new RowType.RowField("customer_name", new VarCharType()),
                new RowType.RowField("order_count", new IntType()),
                new RowType.RowField("zip", new VarCharType())));

        // Create and add the Delta sink
        DataStream<RowData> orderdemographics_customer_count = customerCountStream.map( customercount -> {
            logger.info("Customer Order Demographics"+ customercount );
            GenericRowData rowData = new GenericRowData(RowKind.INSERT, 4); 
            rowData.setField(0, StringData.fromString(customercount.getState()));
            rowData.setField(1, StringData.fromString(customercount.getCustomerName()));
            rowData.setField(2, customercount.getCustomerCount());
            rowData.setField(3, StringData.fromString(customercount.getZipCode()));
            return rowData;
        });
        createDeltaSink(orderdemographics_customer_count, deltaTablePath, rowType);


        env.execute("OrderCount per customer from every State");   

    }

    // Function to create Delta sink for RowData
    public static DataStream<RowData> createDeltaSink(
            DataStream<RowData> stream,
            String deltaTablePath,
            RowType rowType) {

        // Hadoop configuration for AWS S3 access
        org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();

        String[] partitionCols = { "zip" };
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

    // KeyedCoProcessFunction to join shoe_orders and shoe_customers streams using POJOs
    public static class OrderCustomerJoinFunction extends KeyedCoProcessFunction<String, ShoeOrder, ShoeCustomer, CustomerCount> {

        // State to store customer data (customer_id -> customer details)
        private MapState<String, ShoeCustomer> customerState;

        // State to buffer orders when customer details are not available
        private ListState<ShoeOrder> bufferedOrders;

        // State to keep track of order counts by state
        private MapState<String, Integer> stateOrderCount;

        @Override
        public void open(Configuration parameters) throws Exception {
            // State descriptor to store customer information keyed by customer_id
            MapStateDescriptor<String, ShoeCustomer> customerStateDescriptor = 
                new MapStateDescriptor<>("customerState", String.class, ShoeCustomer.class);
            customerState = getRuntimeContext().getMapState(customerStateDescriptor);

            // State descriptor to buffer orders for customers not yet seen
            ListStateDescriptor<ShoeOrder> bufferedOrdersDescriptor = 
                new ListStateDescriptor<>("bufferedOrders", ShoeOrder.class);
            bufferedOrders = getRuntimeContext().getListState(bufferedOrdersDescriptor);

            // State descriptor to store order count by state
            MapStateDescriptor<String, Integer> stateOrderCountDescriptor = 
                new MapStateDescriptor<>("stateOrderCount", String.class, Integer.class);
            stateOrderCount = getRuntimeContext().getMapState(stateOrderCountDescriptor);
        }

        @Override
        public void processElement1(ShoeOrder order, Context ctx, Collector<CustomerCount> out) throws Exception {
            // This method processes elements from the shoe_orders stream

            // Get the customer_id from the order
            String customerId = order.getCustomer_id();

            // Check if customer details are available in state
            ShoeCustomer customer = customerState.get(customerId);
            if (customer != null) {
                // If customer details exist, process the order immediately
                processOrderWithCustomer(order, customer, out);
            } else {
                // If customer details are not available, buffer the order
                bufferedOrders.add(order);
            }
        }

        @Override
        public void processElement2(ShoeCustomer customer, Context ctx, Collector<CustomerCount> out) throws Exception {
            // This method processes elements from the shoe_customers stream

            // Get the customer_id from the customer record
            String customerId = customer.getId();

            // Store the customer details in state, keyed by customer_id
            customerState.put(customerId, customer);

            // Check if there are any buffered orders for this customer
            List<ShoeOrder> remainingOrders = new ArrayList<>();
            for (ShoeOrder order : bufferedOrders.get()) {
                if (order.getCustomer_id().equals(customerId)) {
                    // Process the buffered order with the now-available customer details
                    processOrderWithCustomer(order, customer, out);
                } else {
                    // If the order is for a different customer, add it to the remainingOrders list
                    remainingOrders.add(order);
                }
            }

            // Clear the bufferedOrders state
            bufferedOrders.clear();

            // Re-add the remaining orders back to the buffer
            for (ShoeOrder remainingOrder : remainingOrders) {
                bufferedOrders.add(remainingOrder);
            }
        }

        private void processOrderWithCustomer(ShoeOrder order, ShoeCustomer customer, Collector<CustomerCount> out) throws Exception {
            // Extract the state from the customer record
            String state = customer.getState();

            // Increment the order count for the state
            Integer currentCount = stateOrderCount.get(state);
            if (currentCount == null) {
                currentCount = 0;
            }
            stateOrderCount.put(state, currentCount + 1);

            // Emit the updated count for the state
            out.collect(new CustomerCount(state, customer.getId(), customer.getFirstName()+" "+customer.getLastName(), currentCount + 1, customer.getZipCode()));
        }
    }
}