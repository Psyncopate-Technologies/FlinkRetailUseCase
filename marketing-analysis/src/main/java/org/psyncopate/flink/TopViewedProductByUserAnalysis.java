package org.psyncopate.flink;

import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.mongodb.source.MongoSource;
import org.apache.flink.connector.mongodb.source.enumerator.splitter.PartitionStrategy;
import org.apache.flink.connector.mongodb.source.reader.deserializer.MongoDeserializationSchema;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.bson.BsonDocument;
import org.psyncopate.flink.ViewsPerProductAnalysis.ShoeMetadataEnricher;
import org.psyncopate.flink.model.EnrichedProductViewCount;
import org.psyncopate.flink.model.EnrichedTopViewedProduct;
import org.psyncopate.flink.model.PortalViewAudit;
import org.psyncopate.flink.model.ProductViewCount;
import org.psyncopate.flink.model.Shoe;
import org.psyncopate.flink.model.ShoeCustomer;
import org.psyncopate.flink.model.TopViewedProduct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.delta.flink.sink.DeltaSink;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.util.Collector;

public class TopViewedProductByUserAnalysis {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final Logger logger = LoggerFactory.getLogger(TopViewedProductByUserAnalysis.class);

        Properties mongoProperties = PropertyFilesLoader.loadProperties("mongodb.properties");
        Properties appconfigProperties = PropertyFilesLoader.loadProperties("appconfig.properties");
        Properties deltaLakeProperties = PropertyFilesLoader.loadProperties("delta-lake.properties");

        // Create MongoSource for ShoeOrders Collection
        MongoSource<PortalViewAudit> portalviews = MongoSource.<PortalViewAudit>builder()
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
                .build();

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
                        shoe.setTimestamp(new Date(document.getDateTime("timestamp").getValue()));
                        return shoe;
                    }

                    @Override
                    public TypeInformation<Shoe> getProducedType() {
                        return TypeInformation.of(Shoe.class);
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
                        customer.setTimestamp(new Date(document.getDateTime("timestamp").getValue()));
                        return customer;
                    }

                    @Override
                    public TypeInformation<ShoeCustomer> getProducedType() {
                        return TypeInformation.of(ShoeCustomer.class);
                    }
                })
                .build();

        // Define your sources
        DataStream<PortalViewAudit> portalviews_ds = env
        .fromSource(portalviews, WatermarkStrategy
                .<PortalViewAudit>forMonotonousTimestamps()
                .withTimestampAssigner((portalView, recordTimestamp) -> portalView.getTimestamp().getTime()), 
                "Read Portal Views from MongoDB")
        .setParallelism(1)
        .name("Read Portal Views");

        DataStream<Shoe> shoes_ds = env
            .fromSource(shoeinventory, WatermarkStrategy
            .<Shoe>forMonotonousTimestamps()
            .withTimestampAssigner((shoe, recordTimestamp) -> shoe.getTimestamp().getTime()),  "Read Shoes Inventory from MongoDB")
            .setParallelism(1)
            .name("Retrieve Shoes Inventory");

        DataStream<ShoeCustomer> shoecustomers_ds = env
        .fromSource(shoecustomers, WatermarkStrategy
        .<ShoeCustomer>forMonotonousTimestamps()
        .withTimestampAssigner((shoecustomer, recordTimestamp) -> shoecustomer.getTimestamp().getTime()),  "Read Customers from MongoDB")
        .setParallelism(1).name("Read Customers");


        DataStream<TopViewedProduct> topViewedProducts = portalviews_ds.keyBy(PortalViewAudit :: getUserId)
                                    .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(15)))
                                    .process(new TopViewedProductProcessFunction()).name("Top Viewed Products by every user");

        
        // Key the shoes stream by product ID
        KeyedStream<Shoe, String> keyedShoes = shoes_ds.keyBy(Shoe::getId);
        // Key the shoes stream by product ID
        KeyedStream<ShoeCustomer, String> keyedCustomers = shoecustomers_ds.keyBy(ShoeCustomer::getId);

        // Connect and process the two streams
        DataStream<EnrichedTopViewedProduct> productEnrichedTopViewedProducts = topViewedProducts.keyBy(TopViewedProduct :: getProductId)
        .connect(keyedShoes)
        .process(new ProductMetadataEnricher()).name("Enrich Product Details");

        // Connect and process the two streams
        DataStream<EnrichedTopViewedProduct> userEnrichedTopViewedProducts = productEnrichedTopViewedProducts.keyBy(EnrichedTopViewedProduct :: getUserId)
        .connect(keyedCustomers)
        .process(new UserMetadataEnricher()).name("Enrich User Details");

        String deltaTablePath = deltaLakeProperties.getProperty("storage.filesystem.scheme")
            + deltaLakeProperties.getProperty("storage.filesystem.s3.bucket.name") + "/"
            + deltaLakeProperties.getProperty("deltalake.topviewedproducts.table.name");
    
        // Define the schema for the RowData
        RowType rowType = new RowType(Arrays.asList(
                new RowType.RowField("product_id", new VarCharType() ),
                new RowType.RowField("product_name", new VarCharType() ),
                new RowType.RowField("brand", new VarCharType() ),
                new RowType.RowField("user_id", new VarCharType() ),
                new RowType.RowField("user_name", new VarCharType() ),
                new RowType.RowField("state", new VarCharType() ),
                new RowType.RowField("country", new VarCharType() ),
                new RowType.RowField("view_count", new BigIntType()),
                new RowType.RowField("starttime", new VarCharType()),
                new RowType.RowField("endtime", new VarCharType())));

        // Create and add the Delta sink
        DataStream<RowData> product_count_map = userEnrichedTopViewedProducts.map( enrichedTopViewdProduct -> {
            logger.info("Top Viewed Product by Users--->"+ enrichedTopViewdProduct );
            GenericRowData rowData = new GenericRowData(RowKind.INSERT, 10); 
            rowData.setField(0, StringData.fromString(enrichedTopViewdProduct.getProductId()));
            rowData.setField(1, StringData.fromString(enrichedTopViewdProduct.getProductName()));
            rowData.setField(2, StringData.fromString(enrichedTopViewdProduct.getBrand()));
            rowData.setField(3, StringData.fromString(enrichedTopViewdProduct.getUserId()));
            rowData.setField(4, StringData.fromString(enrichedTopViewdProduct.getUserName()));
            rowData.setField(5, StringData.fromString(enrichedTopViewdProduct.getState()));
            rowData.setField(6, StringData.fromString(enrichedTopViewdProduct.getCountry()));
            rowData.setField(7, enrichedTopViewdProduct.getViewCount());
            rowData.setField(8, StringData.fromString(enrichedTopViewdProduct.getWindowStart()));
            rowData.setField(9, StringData.fromString(enrichedTopViewdProduct.getWindowEnd()));
            return rowData;
        });

        createDeltaSink(product_count_map, deltaTablePath, rowType); 

       /*  userEnrichedTopViewedProducts.map(topProd -> {
            logger.info("Top Viewed Product by Users--->"+ topProd);
            return topProd;
        }).print(); */

        // Execute the job
        env.execute("Top Viewed products by User for past hour in an interval of 15 mins");
    }

    public static class TopViewedProductProcessFunction extends ProcessWindowFunction<PortalViewAudit, TopViewedProduct, String, TimeWindow> {

        @Override
        public void process(String userId, Context context, Iterable<PortalViewAudit> elements, Collector<TopViewedProduct> out) {
            // Map to hold the total view time for each product
            Map<String, Long> productViewTime = new HashMap<>();

            // Sum viewTime for each product for the given user
            for (PortalViewAudit audit : elements) {
                productViewTime.put(audit.getProductId(), productViewTime.getOrDefault(audit.getProductId(), 0L) + audit.getViewTime());
            }

            // Determine the product with the highest view time
            String topProductId = null;
            long maxViewTime = 0;
            for (Map.Entry<String, Long> entry : productViewTime.entrySet()) {
                if (entry.getValue() > maxViewTime) {
                    maxViewTime = entry.getValue();
                    topProductId = entry.getKey();
                }
            }

            // Emit the result with window start and end times if a top product exists
            if (topProductId != null) {
                String windowStart = new Timestamp(context.window().getStart()).toString();
                String windowEnd = new Timestamp(context.window().getEnd()).toString();
                out.collect(new TopViewedProduct(topProductId, userId, maxViewTime, windowStart, windowEnd));
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

        String[] partitionCols = { "user_id" };
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
    public static class ProductMetadataEnricher extends KeyedCoProcessFunction<String, TopViewedProduct, Shoe, EnrichedTopViewedProduct> {
        // State to store Shoe metadata
        private MapState<String, Shoe> shoeState;
        private ListState<TopViewedProduct> bufferedProducts;

        @Override
        public void open(Configuration parameters) throws Exception {
            MapStateDescriptor<String, Shoe> descriptor = new MapStateDescriptor<>(
                "shoeState", // State name
                String.class, // Key type
                Shoe.class // Value type (Shoe class)
            );
            shoeState = getRuntimeContext().getMapState(descriptor);

            ListStateDescriptor<TopViewedProduct> viewCountDescriptor = new ListStateDescriptor<>(
                "bufferedViewCounts",
                TopViewedProduct.class
            );
            bufferedProducts = getRuntimeContext().getListState(viewCountDescriptor);
        }

        @Override
        public void processElement1(TopViewedProduct topViewProduct, Context ctx, Collector<EnrichedTopViewedProduct> out) throws Exception {
            // Enrich ProductViewCount with Shoe metadata if available
            Shoe shoe = shoeState.get(topViewProduct.getProductId());
            if (shoe != null) {
                out.collect(new EnrichedTopViewedProduct(topViewProduct.getProductId(), shoe.getName(), shoe.getBrand(), topViewProduct.getUserId(), "", "", "", topViewProduct.getViewCount(), topViewProduct.getWindowStart(), topViewProduct.getWindowEnd()));
            }else {
                // Buffer the event if shoe metadata is not available
                bufferedProducts.add(topViewProduct);
            }
        }

        @Override
        public void processElement2(Shoe shoe, Context ctx, Collector<EnrichedTopViewedProduct> out) throws Exception {
            // Update state with new Shoe metadata
            shoeState.put(shoe.getId(), shoe);

            List<TopViewedProduct> nonMatchingBufferedProducts = new ArrayList<>();

            // Emit buffered ProductViewCounts if available
            for (TopViewedProduct bufferedProd : bufferedProducts.get()) {
                if (bufferedProd.getProductId().equals(shoe.getId())) {
                    out.collect(new EnrichedTopViewedProduct(bufferedProd.getProductId(), shoe.getName(), shoe.getBrand(), bufferedProd.getUserId(), "", "", "", bufferedProd.getViewCount(), bufferedProd.getWindowStart(), bufferedProd.getWindowEnd()));
                }else {
                    // Store non-matching counts for later
                    nonMatchingBufferedProducts.add(bufferedProd);
                }
                
            }
            // Clear buffered state
            bufferedProducts.clear();
            for (TopViewedProduct product : nonMatchingBufferedProducts) {
                bufferedProducts.add(product);
            }
        }
    }

    // KeyedCoProcessFunction to enrich ProductViewCount with Shoe metadata
    public static class UserMetadataEnricher extends KeyedCoProcessFunction<String, EnrichedTopViewedProduct, ShoeCustomer, EnrichedTopViewedProduct> {
        // State to store Shoe metadata
        private MapState<String, ShoeCustomer> customerState;
        private ListState<EnrichedTopViewedProduct> bufferedProducts;

        @Override
        public void open(Configuration parameters) throws Exception {
            MapStateDescriptor<String, ShoeCustomer> descriptor = new MapStateDescriptor<>(
                "customerState", // State name
                String.class, // Key type
                ShoeCustomer.class // Value type (ShoeCustomer class)
            );
            customerState = getRuntimeContext().getMapState(descriptor);

            ListStateDescriptor<EnrichedTopViewedProduct> viewCountDescriptor = new ListStateDescriptor<>(
                "bufferedViewCounts",
                EnrichedTopViewedProduct.class
            );
            bufferedProducts = getRuntimeContext().getListState(viewCountDescriptor);
        }

        @Override
        public void processElement1(EnrichedTopViewedProduct topViewProduct, Context ctx, Collector<EnrichedTopViewedProduct> out) throws Exception {
            // Enrich ProductViewCount with Shoe metadata if available
            ShoeCustomer cust = customerState.get(topViewProduct.getUserId());
            if (cust != null) {
                out.collect(new EnrichedTopViewedProduct(topViewProduct.getProductId(), topViewProduct.getProductName(), topViewProduct.getBrand(), topViewProduct.getUserId(), cust.getFirstName()+" "+cust.getLastName(), cust.getState(), cust.getCountry(), topViewProduct.getViewCount(), topViewProduct.getWindowStart(), topViewProduct.getWindowEnd()));
            }else {
                // Buffer the event if shoe metadata is not available
                bufferedProducts.add(topViewProduct);
            }
        }

        @Override
        public void processElement2(ShoeCustomer cust, Context ctx, Collector<EnrichedTopViewedProduct> out) throws Exception {
            // Update state with new Shoe metadata
            customerState.put(cust.getId(), cust);

            List<EnrichedTopViewedProduct> nonMatchingBufferedProducts = new ArrayList<>();

            // Emit buffered ProductViewCounts if available
            for (EnrichedTopViewedProduct bufferedProd : bufferedProducts.get()) {
                if (bufferedProd.getUserId().equals(cust.getId())) {
                    out.collect(new EnrichedTopViewedProduct(bufferedProd.getProductId(), bufferedProd.getProductName(), bufferedProd.getBrand(), bufferedProd.getUserId(), cust.getFirstName()+" "+cust.getLastName(), cust.getState(), cust.getCountry(), bufferedProd.getViewCount(), bufferedProd.getWindowStart(), bufferedProd.getWindowEnd()));
                }else {
                    // Store non-matching counts for later
                    nonMatchingBufferedProducts.add(bufferedProd);
                }
                
            }
            // Clear buffered state
            bufferedProducts.clear();
            for (EnrichedTopViewedProduct product : nonMatchingBufferedProducts) {
                bufferedProducts.add(product);
            }
        }
    }
}
