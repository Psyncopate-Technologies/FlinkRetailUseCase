package org.psyncopate.flink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;
import org.bson.BsonDocument;
import org.psyncopate.flink.model.ShoeOrder;
import org.psyncopate.flink.model.PortalViewAudit;
import org.psyncopate.flink.model.Shoe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.core.fs.Path;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.delta.flink.sink.DeltaSink;

import io.delta.flink.sink.DeltaSink;

import io.delta.flink.sink.DeltaSink;

import org.apache.flink.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.configuration.MemorySize;

import org.apache.flink.connector.mongodb.source.enumerator.splitter.PartitionStrategy;
import org.apache.flink.connector.mongodb.source.reader.deserializer.MongoDeserializationSchema;

import java.time.Duration;
import java.util.Arrays;
import java.util.Date;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cdc.connectors.mongodb.MongoDBSource;

@Deprecated
public class CDCMongoSource {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        final Logger logger = LoggerFactory.getLogger(CDCMongoSource.class);

        // Enable checkpointing for fault tolerance
        
         env.enableCheckpointing(30000, CheckpointingMode.EXACTLY_ONCE);
         org.apache.flink.configuration.Configuration config = new
         org.apache.flink.configuration.Configuration();
         config.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
         config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY,"file:///opt/flink/checkpoints");
         config.set(CheckpointingOptions.SAVEPOINT_DIRECTORY,"file:///opt/flink/savepoints");
         env.configure(config);
        

        SourceFunction<String> sourceFunction = MongoDBSource.<String>builder()
        .hosts("container-mongodb:27017/api_prod_db")
        .username("api_user")
        .password("api1234")
        .databaseList("api_prod_db") // set captured database, support regexs
        .collectionList("api_prod_db.shoes") //set captured collections, support regex
        .deserializer(new JsonDebeziumDeserializationSchema())
        .build();

        ObjectMapper objectMapper = new ObjectMapper();
        
        /* DataStream<Shoe> ds = env.addSource(sourceFunction).map(data -> {
            
             JsonNode rootNode = objectMapper.readTree(data);
             String fullDocument = rootNode.get("fullDocument").asText();
             Shoe shoes = objectMapper.readValue(fullDocument, Shoe.class);
             return shoes;
        })
        .setParallelism(1); */ // use parallelism 1 for sink to keep message ordering

        /* ds.map(shoe -> {
            logger.info("Data from Mongo"+ shoe);
            System.out.println("Data from Mongo"+ shoe);
            return shoe;
        }); */

        DebeziumSourceFunction<String> sourceFunction1 = MongoDBSource.<String>builder()
        .hosts("container-mongodb:27017/api_prod_db")
        .username("api_user")
        .password("api1234")
        .databaseList("api_prod_db") // set captured database, support regexs
        .collectionList("api_prod_db.shoes") //set captured collections, support regex
        .deserializer(new JsonDebeziumDeserializationSchema())
        .build();

        
        DataStream<Shoe> ds1 = env.addSource(sourceFunction1).map(data -> {
            
            JsonNode rootNode = objectMapper.readTree(data);
            String fullDocument = rootNode.get("fullDocument").asText();
            Shoe shoes = objectMapper.readValue(fullDocument, Shoe.class);
            return shoes;
       })
       .assignTimestampsAndWatermarks(WatermarkStrategy.<Shoe>forBoundedOutOfOrderness(Duration.ofMinutes(10))
            .withTimestampAssigner((portalView, recordTimestamp) -> portalView.getTimestamp().getTime()))
       .setParallelism(1);
                                                
        String deltaTablePath = "file:///opt/flink/delta-lake/test-delta-table";

                

        // Define the schema for the RowData
        RowType rowType = new RowType(Arrays.asList(
                new RowType.RowField("productId", new VarCharType()),
                new RowType.RowField("productName", new VarCharType())));

        // Create and add the Delta sink
        DataStream<RowData> product_count_map = ds1.map( shoe -> {
            logger.info("Shoe from Mongo--->: "+ shoe );
            System.out.println("Shoe from Mongo--->: "+ shoe );
            GenericRowData rowData = new GenericRowData(RowKind.INSERT, 2); 
            rowData.setField(0, StringData.fromString(shoe.getId()));
            rowData.setField(1, StringData.fromString(shoe.getName()));
            return rowData;
        });


        createDeltaSink(product_count_map, deltaTablePath, rowType);
        

        env.execute();
    }

    // Function to create Delta sink for RowData
    public static DataStream<RowData> createDeltaSink(
            DataStream<RowData> stream,
            String deltaTablePath,
            RowType rowType) {

        // Hadoop configuration for AWS S3 access
        org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();

        String[] partitionCols = { "productId" };
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
