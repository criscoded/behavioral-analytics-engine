//********************************************************************
//
//  Author:        Cristofer Rosa
//  Description:   Analytics engine using Spark 4.1.1 and Java 21.
//                 Processes behavioral metrics with a multi-sink
//                 strategy; parquet for systems, txt for analysts
//
//********************************************************************

package com.criscoded.analytics.engine;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.apache.spark.sql.functions.*;

import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

public class AnalyticsEngine {

    // Instantiate the logger for this specific class
    private static final Logger logger = LoggerFactory.getLogger(AnalyticsEngine.class);

    public void process(SparkSession spark, String inputPath, String parquetPath, String txtPath) throws IOException {

        logger.info("Starting behavioral analytics pipeline reading from: {}", inputPath);

        Dataset<Row> rawData = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(inputPath);

        Dataset<Row> processedData = rawData
                .withColumn("successRate",
                        col("conversions").divide(col("engagement_score")).multiply(100))
                .groupBy("region", "service_tier")
                .agg(round(avg("successRate"), 2).alias("Average_Success_Rate"))
                .orderBy(desc("Average_Success_Rate"));

        logger.info("Exporting optimized Parquet partitions to: {}", parquetPath);
        processedData.write()
                .mode("overwrite")
                .parquet(parquetPath);

        logger.info("Generating formatted analyst report at: {}", txtPath);
        List<Row> results = processedData.collectAsList();

        try (FileWriter writer = new FileWriter(txtPath)) {
            writer.write(String.format("%-15s %-15s %-20s%n", "Region", "Service Tier", "Avg Success Rate (%)"));
            writer.write("------------------------------------------------------------\n");

            for (Row row : results) {
                writer.write(String.format("%-15s %-15s %.2f%n",
                        row.getAs("region"),
                        row.getAs("service_tier"),
                        (Double) row.getAs("Average_Success_Rate")));
            }
        }

        logger.info("Pipeline execution completed successfully.");
    }
}