//********************************************************************
//
//  Author:        Cristofer Rosa
//  Description:   Driver class to initialize Spark Session and
//                 execute the multi-sink analytics pipeline.
//
//********************************************************************

package com.criscoded.analytics;

import com.criscoded.analytics.engine.AnalyticsEngine;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        String projectRoot = System.getProperty("user.dir");
        String inputCsv = projectRoot + "/src/main/resources/customer_behavior_log.csv";
        String outputParquet = projectRoot + "/output/behavioral_report";
        String outputTxt = projectRoot + "/output/behavioral_report/behavioral-output.txt";

        try (SparkSession spark = SparkSession.builder()
                .appName("EnterpriseBehavioralAnalytics")
                .config("spark.master", "local[*]")
                .getOrCreate()) {

            spark.sparkContext().setLogLevel("WARN");

            logger.info("Spark Session initialized successfully for Java 21.");

            AnalyticsEngine engine = new AnalyticsEngine();

            engine.process(spark, inputCsv, outputParquet, outputTxt);

            logger.info("Analytics complete. Parquet saved to /output and Text report generated.");

        } catch (Exception e) {
            logger.error("Critical failure in Spark analytics pipeline", e);
            System.exit(1);
        }
    }
}