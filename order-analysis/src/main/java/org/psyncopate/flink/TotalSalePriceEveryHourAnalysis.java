package org.psyncopate.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.cdc.connectors.mongodb.MongoDBSource;
import org.apache.flink.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.configuration.CheckpointingOptions;
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
import org.apache.flink.streaming.api.functions.aggregation.AggregationFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.bson.BsonDocument;
import org.psyncopate.flink.TotalSalePriceEveryHourAnalysis.SaleRecord;
import org.psyncopate.flink.model.TotalSaleRecord;
import org.psyncopate.flink.model.Shoe;
import org.psyncopate.flink.model.ShoeOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.delta.flink.sink.DeltaSink;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;

public class TotalSalePriceEveryHourAnalysis {

    @SuppressWarnings("deprecation")
    public static void main(String[] args) throws Exception {
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        final Logger logger = LoggerFactory.getLogger(TotalSalePriceEveryHourAnalysis.class);

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
        
        // Define your sources
        /* DataStream<ShoeOrder> shoeorders_ds = env
                .fromSource(shoeorders, WatermarkStrategy
                        .<ShoeOrder>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((order, recordTimestamp) -> order.getTimestamp().getTime()), 
                        "Read Shoe Orders from MongoDB")
                .setParallelism(1)
                .name("Read Shoe Orders"); */
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

        /* DataStream<Shoe> shoes_ds = env
                .fromSource(shoeinventory, WatermarkStrategy
                .<Shoe>forBoundedOutOfOrderness(Duration.ofSeconds(5))
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

        // Join the streams
        DataStream<SaleRecord> sales_ds = shoeorders_ds
                .join(shoes_ds)
                .where(order -> order.getProduct_id())  // Assuming productId is in ShoeOrder
                .equalTo(shoe -> shoe.getId())         // Assuming id is in Shoe
                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(15))) // 1 hour sliding window every 15 minutes
                .apply((order, shoe) -> new SaleRecord(shoe.getSale_price(), order.getTimestamp().getTime())); // Create SaleRecord with sale price and timestamp
 
        
        // Assign timestamps and watermarks to the sales_ds stream
       DataStream<SaleRecord> salesWithTimestamps_ds = sales_ds
        .assignTimestampsAndWatermarks(
            WatermarkStrategy.<SaleRecord>forBoundedOutOfOrderness(Duration.ofMinutes(1))  // Allow 1 minute out-of-orderness
                .withTimestampAssigner((sale, timestamp) -> sale.getTimestamp()) // Extract timestamp from SaleRecord
        );

        
        KeyedStream<SaleRecord, Boolean> keyedStream = salesWithTimestamps_ds
                            .keyBy(sale -> true);
        
        
       // Assuming SaleRecord class has salePrice and timestamp fields
        DataStream<TotalSaleRecord> avgSalePrice_ds = keyedStream
            .keyBy(sale -> true)  // Key all records into a single group
            .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(15)))  // 1-hour window sliding every 15 minutes
            .aggregate(new TotalSumAggregate(), new TotalSaleWindowFunction());
            
            
        String deltaTablePath = deltaLakeProperties.getProperty("storage.filesystem.scheme")
                + deltaLakeProperties.getProperty("storage.filesystem.s3.bucket.name") + "/"
                + deltaLakeProperties.getProperty("deltalake.avg.sales.table.name");

        // Define the schema for the RowData
        RowType rowType = new RowType(Arrays.asList(
                new RowType.RowField("total_sale_pice", new DoubleType() ),
                new RowType.RowField("starttime", new VarCharType()),
                new RowType.RowField("endtime", new VarCharType())));

        // Create and add the Delta sink
        DataStream<RowData> product_count_map = avgSalePrice_ds.map( avgSalePriceRec -> {
            logger.info("Total Sales value of products sold for past hour--->: "+ avgSalePriceRec );
            GenericRowData rowData = new GenericRowData(RowKind.INSERT, 3); 
            rowData.setField(0, avgSalePriceRec.getTotalPrice());
            rowData.setField(1, StringData.fromString(avgSalePriceRec.getWindowStart()));
            rowData.setField(2, StringData.fromString(avgSalePriceRec.getWindowEnd()));
            return rowData;
        });

        createDeltaSink(product_count_map, deltaTablePath, rowType); 
        

        // Execute the job
        env.execute("Total Sale values Calculation for past hour in an interval of 15 mins");
    }

    // Define a SaleRecord class for joining results
    public static class SaleRecord {
        private double salePrice;
        private long timestamp;

        public SaleRecord(double salePrice, long timestamp) {
            this.salePrice = salePrice;
            this.timestamp = timestamp;
        }

        public double getSalePrice() {
            return salePrice;
        }

        public long getTimestamp() {
            return timestamp;
        }
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
                //.withPartitionColumns(partitionCols)
                .build();

        stream.sinkTo(deltaSink).name("Write as Delta lake table");
        return stream;
    }

    public static class TotalSumAccumulator {
        double sum = 0.0;
    }

    public static class TotalSumAggregate implements AggregateFunction<SaleRecord, TotalSumAccumulator, TotalSaleRecord> {

        @Override
        public TotalSumAccumulator createAccumulator() {
            return new TotalSumAccumulator();  // Initialize the accumulator
        }

        @Override
        public TotalSumAccumulator add(SaleRecord saleRecord, TotalSumAccumulator accumulator) {
            accumulator.sum += saleRecord.getSalePrice();  // Add the sale price to the sum
            return accumulator;
        }

        @Override
        public TotalSaleRecord getResult(TotalSumAccumulator accumulator) {
            // Return null as we don't handle the result in the aggregate function itself
            TotalSaleRecord result = new TotalSaleRecord();
            //double avgPrice = accumulator.count == 0 ? 0.0 : accumulator.sum / accumulator.count;
            result.setTotalPrice(accumulator.sum);  // Set the average sale price
            return result;
        }

        @Override
        public TotalSumAccumulator merge(TotalSumAccumulator a, TotalSumAccumulator b) {
            a.sum += b.sum;
            //a.count += b.count;
            return a;
        }
    }

    // The Window Function to calculate average and return AverageSaleRecord with window times
    public static class TotalSaleWindowFunction extends ProcessWindowFunction<TotalSaleRecord, TotalSaleRecord, Boolean, TimeWindow> {

        private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.of("UTC"));

        @Override
        public void process(Boolean key, Context context, Iterable<TotalSaleRecord> elements, Collector<TotalSaleRecord> out) {
            TotalSaleRecord avgSaleRecord = elements.iterator().next();  // Get the average sale record from the aggregator

            String windowStart = new Timestamp(context.window().getStart()).toString();
            String windowEnd = new Timestamp(context.window().getEnd()).toString();

            avgSaleRecord.setWindowStart(windowStart);  // Set window start time
            avgSaleRecord.setWindowEnd(windowEnd);      // Set window end time

            out.collect(avgSaleRecord);  // Emit the final result
        }
    }

    

    
}

