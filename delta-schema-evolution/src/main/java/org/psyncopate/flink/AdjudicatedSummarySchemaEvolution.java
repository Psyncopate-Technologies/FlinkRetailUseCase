package org.psyncopate.flink;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.types.RowKind;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.delta.flink.sink.DeltaSink;

import org.slf4j.Logger;
import org.psyncopate.flink.model.AdjudicatedClaimsSummary;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.csv.CsvReaderFormat;

public class AdjudicatedSummarySchemaEvolution {
    public static void main(String[] args) throws Exception {

        // Initialize Logger
        final Logger logger = LoggerFactory.getLogger(DeltaSchemaEvolution.class);

        //LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        //Properties mongoProperties = PropertyFilesLoader.loadProperties("mongodb.properties");
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

         // Define the path to the CSV file
        //Path csvFilePath = new Path("abfss://molina@molinahealthcareusecase.dfs.core.windows.net/inputFiles/claim_diagnosis");
        //Path csvFilePath = Path.fromLocalFile(new File("/Users/sasidarendinakaran/Documents/Demos/FlinkRetailUseCase/mounts/claim_diagnosis.csv"));
        //Path csvFilePath = new Path("file:///Users/sasidarendinakaran/Documents/Demos/FlinkRetailUseCase/mounts/claim_diagnosis.csv");
        Path csvFilePath = new Path(deltaLakeProperties.getProperty("storage.filesystem.scheme") + deltaLakeProperties.getProperty("storage.filesystem.s3.bucket.name") + "/" + deltaLakeProperties.getProperty("input.csv.dir"));
         CsvReaderFormat<AdjudicatedClaimsSummary> csvFormat = CsvReaderFormat.forPojo(AdjudicatedClaimsSummary.class);
         FileSource<AdjudicatedClaimsSummary> source = 
            FileSource.forRecordStreamFormat(csvFormat, csvFilePath)
            .monitorContinuously(Duration.ofSeconds(10))
            .build();

        DataStream<AdjudicatedClaimsSummary> adjudicated_summary_ds = env
            .fromSource(source, WatermarkStrategy.noWatermarks(), "Read Adjudicated Summary CSV")
            .setParallelism(1)
            .name("Read Adjudicated Summary CSV");

        
        String deltaTablePath = deltaLakeProperties.getProperty("storage.filesystem.scheme")
                + deltaLakeProperties.getProperty("storage.filesystem.s3.bucket.name") + "/"
                + deltaLakeProperties.getProperty("deltalake.table.name");

        // Define the schema for the RowData
        RowType rowType = new RowType(Arrays.asList(
                new RowType.RowField("claim_id", new VarCharType()),
                new RowType.RowField("total_diagnoses", new BigIntType()),
                new RowType.RowField("total_procedures", new BigIntType()),
                new RowType.RowField("total_cost", new DecimalType(10,2))));

        // Create and add the Delta sink
        DataStream<RowData> cld_Datastream = adjudicated_summary_ds.map(adjsummary -> {
            logger.info("Adjudicated Summary data---->"+ adjsummary);
            System.out.println("Adjudicated Summary data---->"+ adjsummary);
            GenericRowData rowData = new GenericRowData(RowKind.INSERT, 4); 
            rowData.setField(0, StringData.fromString(adjsummary.getClaim_id()));
            rowData.setField(1, adjsummary.getTotal_diagnoses());
            rowData.setField(2, adjsummary.getTotal_procedures());
            rowData.setField(3, DecimalData.fromBigDecimal(adjsummary.getTotal_cost(), 10, 2));
            return rowData;
        });

        createDeltaSink(cld_Datastream, deltaTablePath, rowType);

        env.execute("Schema Evolution for Adjuducated Claims Summary");

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
                .withMergeSchema(true)
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