package com.lyg6772.batch;

import com.lyg6772.util.InputValueException;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Row;
import java.io.InputStream;
import java.time.LocalDate;
import java.time.ZoneId;

import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Properties;

public class BehaviorETLClass {
    private static Logger logger = Logger.getLogger(BehaviorETLClass.class);
    static final String appName = "EcommerceEventETL";
    static Properties prop;
    static String etlDate = null;
    public static void main(String[] args){
        logger.info(appName+" batch start");
        try {
            prop = new Properties();
            InputStream is = BehaviorETLClass.class.getResourceAsStream("/config.properties");
            prop.load(is);
            etlDate = prop.getProperty("data.ETLDate");
            if (etlDate.length() < 6){
                throw new InputValueException("date.etlDate must be like YYYYMM");
            }
        }
        catch (Exception ioe){
            logger.error("properties file loading error",ioe);
            return;
        }

        SparkContext context = new SparkContext(
                new SparkConf().setAppName(appName).setMaster(prop.getProperty("spark.master"))
                .set("spark.hadoop.fs.default.name", prop.getProperty("spark.hadoop.fs.default.name"))
                .set("spark.hadoop.fs.defaultFS", prop.getProperty("spark.hadoop.fs.defaultFS"))
                .set("spark.hadoop.fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName())
                .set("spark.hadoop.fs.hdfs.server", org.apache.hadoop.hdfs.server.namenode.NameNode.class.getName())
                .set("spark.hadoop.conf", org.apache.hadoop.hdfs.HdfsConfiguration.class.getName())
                .set("spark.sql.warehouse.dir", prop.getProperty("spark.sql.warehouse.dir"))
        );
        SparkSession sparkSession = SparkSession.builder()
                .sparkContext(context)
                .enableHiveSupport()
                .getOrCreate();


        Dataset<Row> dataSet = extractData(etlDate, sparkSession);
        logger.info("Batch finished");
    }

    private static Dataset<Row> extractData(String etlDate, SparkSession sparkSession){
        String rawFilePath;
        LocalDate baseDate;
        if (!etlDate.isEmpty()) {
            baseDate = LocalDate.parse(etlDate+"01", DateTimeFormatter.ofPattern("yyyyMMdd", Locale.US));
        }
        else{
            baseDate = LocalDate.now(ZoneId.of("UTC"));
        }
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MMM", Locale.US);
        rawFilePath = prop.getProperty("hdfs.rawFolderPath") + baseDate.format(formatter) + ".csv";
        Dataset<Row> csvData = sparkSession.read()
                .format("csv")
                .option("header","true")
                .option("inferSchema", "true")
                .load(rawFilePath);
        csvData.show();
        return csvData;
    }

    private static Boolean isRawFileExists(String fileName){
        return Boolean.TRUE;
    }

    private static void createHiveExternalTable(){}
}
