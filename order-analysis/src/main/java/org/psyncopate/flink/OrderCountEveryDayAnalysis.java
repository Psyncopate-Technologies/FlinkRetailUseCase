package org.psyncopate.flink;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
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
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.mongodb.source.MongoSource;
import org.apache.flink.connector.mongodb.source.enumerator.splitter.PartitionStrategy;
import org.apache.flink.connector.mongodb.source.reader.deserializer.MongoDeserializationSchema;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.bson.BsonDocument;
import org.bson.Document;
import org.psyncopate.flink.model.CustomerCount;
import org.psyncopate.flink.model.EnrichedOrder;
import org.psyncopate.flink.model.ProductOrderCount;
import org.psyncopate.flink.model.Shoe;
import org.psyncopate.flink.model.ShoeCustomer;
import org.psyncopate.flink.model.ShoeOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.delta.flink.sink.DeltaSink;


public class OrderCountEveryDayAnalysis {
    public static void main(String[] args) throws Exception {
        // Initialize Logger
        final Logger logger = LoggerFactory.getLogger(OrderCountEveryDayAnalysis.class);

        //LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
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
                        order.setTimestamp(new Date(document.getDateTime("timestamp").getValue()));
                        return order;
                    }

                    @Override
                    public TypeInformation<ShoeOrder> getProducedType() {
                        return TypeInformation.of(ShoeOrder.class);
                    }
                })
                .build();

        // Create MongoSource for ShoeOrders Collection
        MongoSource<ShoeCustomer> shoecustomers = MongoSource.<ShoeCustomer>builder()
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
                .build();

        DataStream<Shoe> shoes_ds = env
                .fromSource(shoeinventory, WatermarkStrategy.forMonotonousTimestamps(), "Read Shoes Inventory from MongoDB")
                .setParallelism(1).name("Retrieve Shoes Inventory");

        /* DataStream<ShoeOrder> shoeorders_ds = env
                .fromSource(shoeorders, WatermarkStrategy.noWatermarks(), "Read Shoe Orders from MongoDB")
                .setParallelism(1).name("Read Shoe Orders"); */

        // Data stream for shoe orders with proper watermark strategy and timestamp assigner
        DataStream<ShoeOrder> shoeorders_ds = env
            .fromSource(shoeorders, WatermarkStrategy
                .<ShoeOrder>forBoundedOutOfOrderness(Duration.ofSeconds(5)) // Out-of-orderness of 5 seconds
                .withTimestampAssigner((order, recordTimestamp) -> order.getTimestamp().getTime()), // Extract timestamp from ShoeOrder
                "Read Shoe Orders from MongoDB")
            .setParallelism(1)
            .name("Read Shoe Orders");

        // Key the stream by product_id
        KeyedStream<ShoeOrder, String> keyedStream = shoeorders_ds
            .keyBy(ShoeOrder::getProduct_id);

        // Apply a tumbling window of 1 day
        DataStream<ProductOrderCount> productOrderCount = keyedStream
        .window(TumblingEventTimeWindows.of(Time.days(1))) // 1-day window
        .process(new CountAggFunction()); // Use a custom aggregation function


        // Print the counts
       /*  productOrderCount.addSink(new SinkFunction<Tuple3<String, Long, Tuple2<String, String>>>() {
            @Override
            public void invoke(Tuple3<String, Long, Tuple2<String, String>> value, Context context) throws Exception {
                // Log the result
                logger.info("Product ID: {}, Count: {}, Window Start: {}, Window End: {}", 
                            value.f0, value.f1, value.f2.f0, value.f2.f1);
            }
        }); */

        productOrderCount.map(prodCount -> {
            logger.info("Number of Order for a product per day: "+ prodCount);
            return prodCount;
        }).print();
        

        


        String deltaTablePath = deltaLakeProperties.getProperty("storage.filesystem.scheme")
                + deltaLakeProperties.getProperty("storage.filesystem.s3.bucket.name") + "/"
                + deltaLakeProperties.getProperty("deltalake.order.count.table.name");

        // Define the schema for the RowData
        RowType rowType = new RowType(Arrays.asList(
                new RowType.RowField("product_id", new VarCharType()),
                new RowType.RowField("order_count", new BigIntType()),
                new RowType.RowField("starttime", new VarCharType()),
                new RowType.RowField("endtime", new VarCharType())));

        // Create and add the Delta sink
        DataStream<RowData> product_count_map = productOrderCount.map( prodCount -> {
            logger.info("Number of Order for a product per day: "+ prodCount );
            GenericRowData rowData = new GenericRowData(RowKind.INSERT, 4); 
            rowData.setField(0, StringData.fromString(prodCount.getProductId()));
            rowData.setField(1, prodCount.getOrderCount());
            rowData.setField(2, StringData.fromString(prodCount.getWindowStart()));
            rowData.setField(3, StringData.fromString(prodCount.getWindowEnd()));
            return rowData;
        });
        createDeltaSink(product_count_map, deltaTablePath, rowType); 


        env.execute("Order Count per product for every 1 day");   

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

    public static class CountAggFunction extends ProcessWindowFunction<ShoeOrder, ProductOrderCount , String, TimeWindow> {
        @Override
        public void process(String productId, Context context, Iterable<ShoeOrder> orders, Collector<ProductOrderCount> out) {
            long count = 0;
            DateTimeFormatter formatter = DateTimeFormatter
            .ofPattern("yyyy-MM-dd HH:mm:ss") // Format as desired
            .withZone(ZoneId.systemDefault()); // Use the system default time zone

            for (ShoeOrder order : orders) {
                count++;
            }

            // Capture the window start and end times
            long windowStartMillis = context.window().getStart();
            long windowEndMillis = context.window().getEnd();

            String windowStart = formatter.format(Instant.ofEpochMilli(windowStartMillis));
            String windowEnd = formatter.format(Instant.ofEpochMilli(windowEndMillis));

            // Emit product_id, count, and a tuple containing window start and end times
            ProductOrderCount productOrderCount = new ProductOrderCount(productId, count, windowStart, windowEnd);
            out.collect(productOrderCount);
        }
    }
}