package com.lyg6772.batch;

import com.lyg6772.util.InputValueException;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Properties;

public class BehaviorETLClass {
    private static final Logger logger = Logger.getLogger(BehaviorETLClass.class);
    static final String appName = "EcommerceEventETL";
    static Properties prop;
    static String etlDate = null;
    static SparkSession sparkSession;

    public static void main(String[] args){
        Dataset<Row> dataSet;
        logger.info(appName+" batch start");
        try {
            initBatch();
        }
        catch (Exception e){
            logger.error("error occured in initBatch", e);
            return;
        }

        try {
            dataSet = extractData(etlDate);
        }
        catch (IOException ioe){
            logger.error("file not exists", ioe);
            return;
        }
        catch (Exception e){
            logger.error("error occured in extractData", e);
            return;
        }

        try{
            dataSet = transferData(dataSet);
        }catch (Exception e){
            logger.error("error occured in extractData", e);
            return;
        }
        dataSet.checkpoint();
        try {
            loadData(dataSet);
        }
        catch (Exception e){
            logger.error("error occured in loadData", e);
            return;
        }
        logger.info("Batch finished");
    }

    private static void initBatch() throws IOException, InputValueException {
        prop = new Properties();
        InputStream is = BehaviorETLClass.class.getResourceAsStream("/config.properties");
        prop.load(is);
        etlDate = prop.getProperty("data.ETLDate");
        if (StringUtils.isNotEmpty(etlDate) && etlDate.length() < 6){
            throw new InputValueException("date.etlDate must be like YYYYMM");
        }
        sparkSession = SparkSession.builder()
                .appName(appName)
                .master(prop.getProperty("spark.master"))
                .config("spark.hadoop.fs.default.name", prop.getProperty("spark.hadoop.fs.default.name"))
                .config("spark.hadoop.fs.defaultFS", prop.getProperty("spark.hadoop.fs.defaultFS"))
                .config("spark.hadoop.fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName())
                .config("spark.hadoop.fs.hdfs.server", org.apache.hadoop.hdfs.server.namenode.NameNode.class.getName())
                .config("spark.hadoop.conf", org.apache.hadoop.hdfs.HdfsConfiguration.class.getName())
                .config("spark.sql.warehouse.dir", prop.getProperty("spark.sql.warehouse.dir"))
                .config("spark.sql.hive.metastore.uris", "thrift://localhost:9083") // Hive Metastore URI 설정
                .config("spark.sql.streaming.checkpointLocation", prop.getProperty("hdfs.checkPointFolderPath"))
                .enableHiveSupport()
                .getOrCreate();


    }



    private static Dataset<Row> extractData(String etlDate) throws IOException{
        String rawFilePath;
        LocalDate baseDate;
        if (StringUtils.isNotEmpty(etlDate)) {
            baseDate = LocalDate.parse(etlDate+"01", DateTimeFormatter.ofPattern("yyyyMMdd"));
        }
        else{
            baseDate = LocalDate.now(ZoneId.of("UTC"));
        }
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MMM", Locale.US);
        rawFilePath = prop.getProperty("hdfs.rawFolderPath","hdfs:///tmp/raw") + baseDate.format(formatter) + ".csv";
        return sparkSession.read()
                .format("csv")
                .option("header","true")
                .option("inferSchema", "true")
                .load(rawFilePath);
    }

    private static Dataset<Row> transferData(Dataset<Row> dataSet){
        dataSet = dataSet.withColumn("event_time",
                functions.from_utc_timestamp(dataSet.col("event_time"), "Asia/Seoul"));

        dataSet = dataSet.withColumn("year", functions.date_format(dataSet.col("event_time"), "yyyy"))
                .withColumn("month", functions.date_format(dataSet.col("event_time"), "MM"))
                .withColumn("day", functions.date_format(dataSet.col("event_time"), "dd"));

        return dataSet;
    }
    public static void loadData(Dataset<Row> dataSet){

        dataSet.write()
                .format("parquet")
                .mode("overwrite") // 기존 파티션 덮어쓰기 설정
                .partitionBy("year", "month", "day")
                .option("compression", "snappy")
                .save(prop.getProperty("spark.sql.warehouse.dir") +
                        prop.getProperty("hive.table.external.log.name"));

        createHiveExternalTable();

        sparkSession.sql("MSCK REPAIR TABLE " + prop.getProperty("hive.table.external.log.name"));

    }

    private static void createHiveExternalTable(){
        String tableName = prop.getProperty("hive.table.external.log.name", "ecommerce_behavior_log");
        sparkSession.sql("CREATE EXTERNAL TABLE IF NOT EXISTS " +
                tableName +
                " (" +
                "event_time TIMESTAMP, " +
                "event_type STRING, " +
                "product_id BIGINT, " +
                "category_id STRING, " +
                "category_code STRING, " +
                "brand STRING, " +
                "price DOUBLE, " +
                "user_id BIGINT, " +
                "user_session STRING" +
                ") PARTITIONED BY (year STRING, month STRING, day STRING) " +
                "STORED AS PARQUET " +
                "LOCATION '" +
                prop.getProperty("spark.hadoop.fs.defaultFS") +
                prop.getProperty("hdfs.parquetFolderPath") +
                tableName +
                "' " +
                "TBLPROPERTIES ('parquet.compression'='SNAPPY' ); ")
        ;
        sparkSession.sql("SHOW TABLES;").show();

    }
}
