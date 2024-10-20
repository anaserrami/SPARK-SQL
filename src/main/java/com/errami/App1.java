package com.errami;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class App1 {
    public static void main(String[] args) {

        // Initialize Spark session
        SparkSession spark = SparkSession.builder()
                .appName("IncidentAnalysis")
                .master("local")
                .getOrCreate();

        // Set the time parser policy to LEGACY
        spark.conf().set("spark.sql.legacy.timeParserPolicy", "LEGACY");

        // Load the CSV file
        Dataset<Row> incidentsDF = spark.read()
                .option("header", "true")  // CSV has a header
                .option("inferSchema", "true")  // Infer the schema from data
                .csv("./incidents.csv");

        // Show the data
        incidentsDF.show();

        // 1. Display the number of incidents per service
        Dataset<Row> incidentsPerService = incidentsDF.groupBy("service")
                .count()
                .orderBy(functions.desc("count"));

        System.out.println("1- Number of incidents per service:");
        incidentsPerService.show();

        // 2. Display the two years with the most incidents
        Dataset<Row> incidentsPerYear = incidentsDF
                .withColumn("parsed_date", functions.to_date(incidentsDF.col("date"), "MM/dd/yyyy"))  // Parse the date with specified format
                .withColumn("year", functions.year(functions.col("parsed_date")))  // Extract year from parsed date
                .groupBy("year")
                .count()
                .orderBy(functions.desc("count"));

        System.out.println("2- Years with the most incidents:");
        incidentsPerYear.show(2);

        // Stop the Spark session
        spark.stop();
    }
}
