package org.psyncopate.flink;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.slf4j.LoggerFactory;

import io.delta.flink.sink.DeltaSink;

import org.slf4j.Logger;
import org.bson.BsonDocument;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import org.psyncopate.flink.model.Shoe;
import org.psyncopate.flink.model.ShoeOrder;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cdc.connectors.mongodb.source.MongoDBSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.mongodb.source.MongoSource;
import org.apache.flink.connector.mongodb.source.enumerator.splitter.PartitionStrategy;
import org.apache.flink.connector.mongodb.source.reader.deserializer.MongoDeserializationSchema;
import org.apache.flink.core.fs.Path;

public class ShoesInventoryAnalysis {
    public static void main(String[] args) throws Exception {

        // Initialize Logger
        final Logger logger = LoggerFactory.getLogger(ShoesInventoryAnalysis.class);

        // LocalStreamEnvironment env =
        // StreamExecutionEnvironment.createLocalEnvironment();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        Properties mongoProperties = PropertyFilesLoader.loadProperties("mongodb.properties");
        Properties appconfigProperties = PropertyFilesLoader.loadProperties("appconfig.properties");
        Properties deltaLakeProperties = PropertyFilesLoader.loadProperties("delta-lake.properties");

        // Enable checkpointing for fault tolerance
        /*
         * env.enableCheckpointing(30000, CheckpointingMode.EXACTLY_ONCE);
         * org.apache.flink.configuration.Configuration config = new
         * org.apache.flink.configuration.Configuration();
         * config.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
         * config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY,
         * appconfigProperties.getProperty("faulttolerance.filesystem.scheme") +
         * appconfigProperties.getProperty("checkpoint.location"));
         * config.set(CheckpointingOptions.SAVEPOINT_DIRECTORY,
         * appconfigProperties.getProperty("faulttolerance.filesystem.scheme") +
         * appconfigProperties.getProperty("savepoint.location"));
         * env.configure(config);
         */
        MongoSource<Shoe> shoeinventory = MongoSource.<Shoe>builder()
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
                .build();

        // Create MongoSource for ShoeOrders Collection
        MongoSource<ShoeOrder> shoeorders = MongoSource.<ShoeOrder>builder()
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
                        return order;
                    }

                    @Override
                    public TypeInformation<ShoeOrder> getProducedType() {
                        return TypeInformation.of(ShoeOrder.class);
                    }
                })
                .build();

        DataStream<Shoe> shoes_ds = env
                .fromSource(shoeinventory, WatermarkStrategy.forMonotonousTimestamps(), "Read Shoes Inventory from MongoDB")
                .setParallelism(1).name("Retrieve Shoes Inventory");
        DataStream<ShoeOrder> shoeorders_ds = env
                .fromSource(shoeorders, WatermarkStrategy.forMonotonousTimestamps(), "Read Shoe Orders from MongoDB")
                .setParallelism(1).name("Read Shoe Orders");

        // Key both streams by product ID for joining
        KeyedStream<Shoe, String> keyedShoesStream = shoes_ds.keyBy(Shoe::getId);
        KeyedStream<ShoeOrder, String> keyedOrdersStream = shoeorders_ds.keyBy(ShoeOrder::getProduct_id);

        // Process function to handle orders and update quantity
        DataStream<Shoe> updatedShoesInventory_ds = keyedShoesStream
                .connect(keyedOrdersStream)
                .process(new KeyedCoProcessFunction<String, Shoe, ShoeOrder, Shoe>() {

                    private MapState<String, Shoe> inventoryState;
                    private ListState<ShoeOrder> orderBufferState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        MapStateDescriptor<String, Shoe> inventoryDescriptor = new MapStateDescriptor<>(
                                "inventoryState", String.class, Shoe.class);
                        inventoryState = getRuntimeContext().getMapState(inventoryDescriptor);

                        // Initialize ListState to buffer orders
                        ListStateDescriptor<ShoeOrder> orderBufferDescriptor = new ListStateDescriptor<>(
                                "orderBufferState", ShoeOrder.class);
                        orderBufferState = getRuntimeContext().getListState(orderBufferDescriptor);
                    }

                    @Override
                    public void processElement1(Shoe shoe, Context ctx, Collector<Shoe> out) throws Exception {
                        // Store initial inventory
                        //inventoryState.put(shoe.getId(), Integer.parseInt(shoe.getQuantity()));
                        inventoryState.put(shoe.getId(), shoe);

                        List<ShoeOrder> nonMatchingBufferedShoeOrders = new ArrayList<>();

                        // Process any buffered orders
                        Iterable<ShoeOrder> bufferedOrders = orderBufferState.get();
                        for (ShoeOrder order : bufferedOrders) {
                            logger.info("into the process1 function of shoe. About to process the buffered order: "+ order);
                            if(order.getProduct_id().equals(shoe.getId())){
                                processOrder(order, shoe.getId(), out);
                            }else{
                                nonMatchingBufferedShoeOrders.add(order);
                            }
                            
                        }
                        // Clear the buffer after processing
                        orderBufferState.clear();
                        for (ShoeOrder order : nonMatchingBufferedShoeOrders) {
                            orderBufferState.add(order);
                        }
                    }

                    @Override
                    public void processElement2(ShoeOrder order, Context ctx, Collector<Shoe> out) throws Exception {
                        // Update shoe inventory when an order comes in
                        Shoe shoeFromState = inventoryState.get(order.getProduct_id());
                        
                        //Integer currentQuantity = inventoryState.get(order.getProduct_id());
                        if (shoeFromState != null ) {
                            /*
                             * currentQuantity -= 1;
                             * inventoryState.put(order.getProduct_id(), currentQuantity);
                             * 
                             * // Emit updated shoe with new quantity
                             * Shoe updatedShoe = new Shoe();
                             * updatedShoe.setId(order.getProduct_id());
                             * updatedShoe.setQuantity(currentQuantity.toString());
                             * out.collect(updatedShoe);
                             */
                            //Integer currentQuantity = Integer.parseInt(shoeFromState.getQuantity());
                            logger.info("into the process2 function of shoeorder. About to process the new order: "+ order);
                            processOrder(order, order.getProduct_id(), out);
                        } else {
                            // If shoe not found in inventory, skip processing
                            /*
                             * System.out.println("Order for product_id " + order.getProduct_id() +
                             * " does not exist in inventory.");
                             * logger.info("Order for product_id " + order.getProduct_id() +
                             * " does not exist in inventory.");
                             */
                            // Buffer the order until inventory is available
                            logger.info("No Inventory found for order. Adding to buffered: "+ order);
                            orderBufferState.add(order);
                        }
                    }

                    // Helper method to process the order and collect the result
                    private void processOrder(ShoeOrder order, String productId, Collector<Shoe> out) throws Exception {
                        Shoe shoeFromState = inventoryState.get(productId);
                        Integer currentInventory = Integer.parseInt(shoeFromState.getQuantity());
                        
                        //Integer currentInventory = inventoryState.get(productId);
                        logger.info("processing order: "+ order);
                        if (currentInventory != null && currentInventory > 0) {
                            // Deduct the order quantity from inventory
                            Integer updatedInventory = currentInventory - 1;
                            shoeFromState.setQuantity(String.valueOf(updatedInventory));
                            inventoryState.put(productId, shoeFromState);
                            logger.info("order inventory id: "+ productId + "updatedqty : " + updatedInventory);
                            // Emit updated shoe with new quantity
                            Shoe updatedShoe = new Shoe();
                            updatedShoe.setId(order.getProduct_id());
                            updatedShoe.setQuantity(updatedInventory.toString());
                            if(updatedInventory < Integer.parseInt(appconfigProperties.getProperty("inventory.low.quantity.alert.thershold"))){
                                out.collect(shoeFromState);
                            }else{
                                logger.info("No alerts for this order");
                            }
                        }
                    }
                }).name("Low Stock Alerts");
        

        String deltaTablePath = deltaLakeProperties.getProperty("storage.filesystem.scheme")
                + deltaLakeProperties.getProperty("storage.filesystem.s3.bucket.name") + "/"
                + deltaLakeProperties.getProperty("deltalake.table.name");

        // Define the schema for the RowData
        RowType rowType = new RowType(Arrays.asList(
                new RowType.RowField("product_id", new VarCharType()),
                new RowType.RowField("quantity", new VarCharType())));

        // Create and add the Delta sink
        DataStream<RowData> lowStock_rowDatastream = updatedShoesInventory_ds.map(shoe -> {
            logger.info("Low Stock Alert---->"+ shoe);
            GenericRowData rowData = new GenericRowData(RowKind.INSERT, 2); 
            rowData.setField(0, StringData.fromString(shoe.getId()));
            rowData.setField(1, StringData.fromString(shoe.getQuantity()));
            return rowData;
        });

        //createDeltaSink(lowStock_rowDatastream, deltaTablePath, rowType);

        env.execute("Shoes Inventory Analysis");

    }

    // Function to create Delta sink for RowData
    public static DataStream<RowData> createDeltaSink(
            DataStream<RowData> stream,
            String deltaTablePath,
            RowType rowType) {

        // Hadoop configuration for AWS S3 access
        org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();

        String[] partitionCols = { "product_id" };
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

    /* public static class LowStockAlertFunction extends KeyedProcessFunction<String, Shoe, Shoe> {
        private final int alertThreshold;
        private final Logger logger;
        private ValueState<Integer> latestQuantityState;

        public LowStockAlertFunction(int alertThreshold, Logger logger) {
            this.alertThreshold = alertThreshold;
            this.logger = logger;
        }

       @Override
        public void open(org.apache.flink.configuration.Configuration parameters) {
            ValueStateDescriptor<Integer> descriptor = new ValueStateDescriptor<>(
                "latestQuantity", // state name
                Integer.class // state type
            );
            latestQuantityState = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(Shoe shoe, Context context, Collector<Shoe> collector) throws Exception {
            Integer latestQuantity = latestQuantityState.value();

            // Update the latest quantity
            latestQuantityState.update(Integer.parseInt(shoe.getQuantity()));

            logger.info("Shoe Qunatity :"+ Integer.parseInt(shoe.getQuantity()));
            logger.info("Threshold :"+ alertThreshold);
            // Emit an alert if the latest quantity is less than or equal to the threshold
            if (latestQuantity != null && Integer.parseInt(shoe.getQuantity()) < alertThreshold) {
                logger.info("Filtered element collected");
                collector.collect(shoe); // Emit the shoe for low stock alert
            }else{
                logger.info("Inventory more than the threshold set for product"+ shoe.getId());
            }
        }
    } */
}