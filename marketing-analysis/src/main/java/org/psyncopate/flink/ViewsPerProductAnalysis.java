package org.psyncopate.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
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
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.aggregation.AggregationFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
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
import org.psyncopate.flink.model.EnrichedProductViewCount;
import org.psyncopate.flink.model.PortalViewAudit;
import org.psyncopate.flink.model.ProductViewCount;
import org.psyncopate.flink.model.Shoe;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import java.util.Properties;

public class ViewsPerProductAnalysis {
    @SuppressWarnings("deprecation")
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final Logger logger = LoggerFactory.getLogger(ViewsPerProductAnalysis.class);

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
                        portalViews.setProductId(document.getString("product_id").getValue());
                        portalViews.setUserId(document.getString("user_id").getValue());
                        portalViews.setViewTime(document.getInt32("view_time").longValue());
                        portalViews.setIp(document.getString("ip").getValue());
                        portalViews.setPageUrl(document.getString("page_url").getValue());
                        portalViews.setTs(new Date(document.getDateTime("ts").getValue()));
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

            // Define your sources
        /* DataStream<PortalViewAudit> portalviews_ds = env
        .fromSource(portalviews, WatermarkStrategy
                .<PortalViewAudit>forMonotonousTimestamps()
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

            // Key by productId and apply a sliding window of 1 hour with a slide interval of 15 minutes
            DataStream<ProductViewCount> viewCounts = portalviews_ds
                    .keyBy(PortalViewAudit::getProduct_id)  // Key by productId
                    .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(15)))
                    .process(new ProductViewCountProcessFunction()).name("View counts for products");

            // Key the shoes stream by product ID
            KeyedStream<Shoe, String> keyedShoes = shoes_ds.keyBy(Shoe::getId);

            // Connect and process the two streams
            DataStream<EnrichedProductViewCount> enrichedViewCounts = viewCounts.keyBy(ProductViewCount :: getProductId)
            .connect(keyedShoes)
            .process(new ShoeMetadataEnricher()).name("Enrich Product details");

            String deltaTablePath = deltaLakeProperties.getProperty("storage.filesystem.scheme")
            + deltaLakeProperties.getProperty("storage.filesystem.s3.bucket.name") + "/"
            + deltaLakeProperties.getProperty("deltalake.productviews.table.name");
    
            // Define the schema for the RowData
            RowType rowType = new RowType(Arrays.asList(
                    new RowType.RowField("product_id", new VarCharType() ),
                    new RowType.RowField("product_name", new VarCharType() ),
                    new RowType.RowField("brand", new VarCharType() ),
                    new RowType.RowField("view_count", new BigIntType()),
                    new RowType.RowField("starttime", new VarCharType()),
                    new RowType.RowField("endtime", new VarCharType())));
    
            // Create and add the Delta sink
            DataStream<RowData> product_count_map = enrichedViewCounts.map( viewCount -> {
                logger.info("Total views per product for past hour--->: "+ viewCount );
                GenericRowData rowData = new GenericRowData(RowKind.INSERT, 6); 
                rowData.setField(0, StringData.fromString(viewCount.getProductId()));
                rowData.setField(1, StringData.fromString(viewCount.getProductName()));
                rowData.setField(2, StringData.fromString(viewCount.getBrand()));
                rowData.setField(3, viewCount.getViewCount());
                rowData.setField(4, StringData.fromString(viewCount.getStarttime()));
                rowData.setField(5, StringData.fromString(viewCount.getEndtime()));
                return rowData;
            });
    
            createDeltaSink(product_count_map, deltaTablePath, rowType); 

        // Execute the job
        env.execute("Product Views for past hour in an interval of 15 mins");

    }
    // ProcessWindowFunction to count views per product
    public static class ProductViewCountProcessFunction
            extends ProcessWindowFunction<PortalViewAudit, ProductViewCount, String, TimeWindow> {

        @Override
        public void process(String productId, Context context, Iterable<PortalViewAudit> elements, Collector<ProductViewCount> out) {
            long totalViewTime = 0; 
            for (PortalViewAudit event : elements) {
                totalViewTime += event.getView_time(); 
            }
            String windowStart = new Timestamp(context.window().getStart()).toString();
            String windowEnd = new Timestamp(context.window().getEnd()).toString();

            out.collect(new ProductViewCount(productId, totalViewTime, windowStart, windowEnd));
        }
    }

    // KeyedCoProcessFunction to enrich ProductViewCount with Shoe metadata
    public static class ShoeMetadataEnricher extends KeyedCoProcessFunction<String, ProductViewCount, Shoe, EnrichedProductViewCount> {
        // State to store Shoe metadata
        private MapState<String, Shoe> shoeState;
        private ListState<ProductViewCount> bufferedViewCounts;

        @Override
        public void open(Configuration parameters) throws Exception {
            MapStateDescriptor<String, Shoe> descriptor = new MapStateDescriptor<>(
                "shoeState", // State name
                String.class, // Key type
                Shoe.class // Value type (Shoe class)
            );
            shoeState = getRuntimeContext().getMapState(descriptor);

            ListStateDescriptor<ProductViewCount> viewCountDescriptor = new ListStateDescriptor<>(
                "bufferedViewCounts",
                Types.POJO(ProductViewCount.class)
            );
            bufferedViewCounts = getRuntimeContext().getListState(viewCountDescriptor);
        }

        @Override
        public void processElement1(ProductViewCount viewCount, Context ctx, Collector<EnrichedProductViewCount> out) throws Exception {
            // Enrich ProductViewCount with Shoe metadata if available
            Shoe shoe = shoeState.get(viewCount.getProductId());
            if (shoe != null) {
                out.collect(new EnrichedProductViewCount(viewCount.getProductId(), shoe.getName(), shoe.getBrand(), viewCount.getViewCount(), viewCount.getStarttime(), viewCount.getEndtime()));
            }else {
                // Buffer the event if shoe metadata is not available
                bufferedViewCounts.add(viewCount);
            }
        }

        @Override
        public void processElement2(Shoe shoe, Context ctx, Collector<EnrichedProductViewCount> out) throws Exception {
            // Update state with new Shoe metadata
            shoeState.put(shoe.getId(), shoe);

            List<ProductViewCount> nonMatchingBufferedCounts = new ArrayList<>();

            // Emit buffered ProductViewCounts if available
            for (ProductViewCount bufferedViewCount : bufferedViewCounts.get()) {
                if (bufferedViewCount.getProductId().equals(shoe.getId())) {
                    out.collect(new EnrichedProductViewCount(bufferedViewCount.getProductId(), shoe.getName(), shoe.getBrand(), bufferedViewCount.getViewCount(), bufferedViewCount.getStarttime(), bufferedViewCount.getEndtime()));
                }else {
                    // Store non-matching counts for later
                    nonMatchingBufferedCounts.add(bufferedViewCount);
                }
                
            }
            // Clear buffered state
            bufferedViewCounts.clear();
            for (ProductViewCount count : nonMatchingBufferedCounts) {
                bufferedViewCounts.add(count);
            }
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
                .withPartitionColumns(partitionCols)
                .build();

        stream.sinkTo(deltaSink).name("Write as Delta lake table");
        return stream;
    }
}