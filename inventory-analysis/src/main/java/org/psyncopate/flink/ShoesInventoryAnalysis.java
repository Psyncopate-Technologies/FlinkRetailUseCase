package org.psyncopate.flink;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
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
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;
import org.psyncopate.flink.model.Shoe;
import org.psyncopate.flink.model.ShoeOrder;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
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
                .setLimit(10000)
                .setNoCursorTimeout(true)
                .setPartitionStrategy(PartitionStrategy.SAMPLE)
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
                .setLimit(10000)
                .setNoCursorTimeout(true)
                .setPartitionStrategy(PartitionStrategy.SAMPLE)
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
                .fromSource(shoeinventory, WatermarkStrategy.noWatermarks(), "Read Shoes Inventory from MongoDB")
                .setParallelism(1);
        DataStream<ShoeOrder> shoeorders_ds = env
                .fromSource(shoeorders, WatermarkStrategy.noWatermarks(), "Read Shoe Orders from MongoDB")
                .setParallelism(1);

        // Key both streams by product ID for joining
        KeyedStream<Shoe, String> keyedShoesStream = shoes_ds.keyBy(Shoe::getId);
        KeyedStream<ShoeOrder, String> keyedOrdersStream = shoeorders_ds.keyBy(ShoeOrder::getProduct_id);

        // Process function to handle orders and update quantity
        DataStream<Shoe> updatedShoesInventory_ds = keyedShoesStream
                .connect(keyedOrdersStream)
                .process(new KeyedCoProcessFunction<String, Shoe, ShoeOrder, Shoe>() {

                    private MapState<String, Integer> inventoryState;
                    private ListState<ShoeOrder> orderBufferState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        MapStateDescriptor<String, Integer> inventoryDescriptor = new MapStateDescriptor<>(
                                "inventoryState", String.class, Integer.class);
                        inventoryState = getRuntimeContext().getMapState(inventoryDescriptor);

                        // Initialize ListState to buffer orders
                        ListStateDescriptor<ShoeOrder> orderBufferDescriptor = new ListStateDescriptor<>(
                                "orderBufferState", ShoeOrder.class);
                        orderBufferState = getRuntimeContext().getListState(orderBufferDescriptor);
                    }

                    @Override
                    public void processElement1(Shoe shoe, Context ctx, Collector<Shoe> out) throws Exception {
                        // Store initial inventory
                        inventoryState.put(shoe.getId(), Integer.parseInt(shoe.getQuantity()));

                        // Process any buffered orders
                        Iterable<ShoeOrder> bufferedOrders = orderBufferState.get();
                        for (ShoeOrder order : bufferedOrders) {
                            processOrder(order, shoe.getId(), out);
                        }
                        // Clear the buffer after processing
                        orderBufferState.clear();
                    }

                    @Override
                    public void processElement2(ShoeOrder order, Context ctx, Collector<Shoe> out) throws Exception {
                        // Update shoe inventory when an order comes in
                        Integer currentQuantity = inventoryState.get(order.getProduct_id());
                        if (currentQuantity != null && currentQuantity > 0) {
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
                            orderBufferState.add(order);
                        }
                    }

                    // Helper method to process the order and collect the result
                    private void processOrder(ShoeOrder order, String productId, Collector<Shoe> out) throws Exception {
                        Integer currentInventory = inventoryState.get(productId);

                        if (currentInventory != null && currentInventory > 0) {
                            // Deduct the order quantity from inventory
                            Integer updatedInventory = currentInventory - 1;
                            inventoryState.put(productId, updatedInventory);

                            // Emit updated shoe with new quantity
                            Shoe updatedShoe = new Shoe();
                            updatedShoe.setId(order.getProduct_id());
                            updatedShoe.setQuantity(updatedInventory.toString());
                            out.collect(updatedShoe);

                            /*
                             * // Trigger alert if inventory goes below the threshold
                             * if (updatedInventory < 50) {
                             * out.collect(Tuple2.of(shoeInventory, "ALERT: Low inventory for product " +
                             * productId + ". Remaining: " + updatedInventory));
                             * }
                             */
                        }
                    }
                });

        // Stream for low stock alerts (quantity < 50)
        int alertThershold = Integer
                .parseInt(appconfigProperties.getProperty("inventory.low.quantity.alert.thershold"));
        DataStream<Shoe> lowStockAlert_ds = updatedShoesInventory_ds
                .filter(shoe -> Integer.parseInt(shoe.getQuantity()) < alertThershold);

        // String deltaTablePath =
        // deltaLakeProperties.getProperty("storage.filesystem.scheme")+"/opt/flink/delta-lake/"
        // + deltaLakeProperties.getProperty("deltalake.table.name");
        String deltaTablePath = deltaLakeProperties.getProperty("storage.filesystem.scheme")
                + deltaLakeProperties.getProperty("storage.filesystem.s3.bucket.name") + "/"
                + deltaLakeProperties.getProperty("deltalake.table.name");

        // Define the schema for the RowData
        RowType rowType = new RowType(Arrays.asList(
                new RowType.RowField("product_id", new VarCharType()),
                new RowType.RowField("quantity", new VarCharType())));

        // Create and add the Delta sink
        DataStream<RowData> lowStock_rowDatastream = lowStockAlert_ds.map(shoe -> {
            GenericRowData rowData = new GenericRowData(RowKind.INSERT, 2); // 5 fields in the Shoe POJO
            rowData.setField(0, StringData.fromString(shoe.getId()));
            rowData.setField(1, StringData.fromString(shoe.getQuantity()));
            return rowData;
        });

        createDeltaSink(lowStock_rowDatastream, deltaTablePath, rowType);

        // Print Alerts and updated inventory for local testing
        /*
         * shoes_ds.print("Initial Shoes Inventory");
         * shoeorders_ds.print("Shoe Order placement");
         * updatedShoesInventory_ds.print("Updated Shoe Inventory");
         * lowStockAlert_ds.print("Low Stock Alerts");
         */

        /*
         * lowStockAlert_ds.map( record -> {
         * logger.info("Low Stock alert"+record);
         * return record;
         * });
         */

        /*
         * shoes_ds.map( record -> {
         * logger.info("Retrieved Inventory record", record);
         * return record;
         * });
         * 
         * shoeorders_ds.map( record -> {
         * logger.info("Retrieved Order Placement record", record);
         * return record;
         * });
         */

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

        stream.sinkTo(deltaSink);
        return stream;
    }
}