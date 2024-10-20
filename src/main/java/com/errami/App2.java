package com.errami;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class App2 {
    public static void main(String[] args) {
        // Initialize SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("Hospital Data Analysis")
                .master("local[*]")
                .getOrCreate();

        // MySQL JDBC connection properties
        String jdbcUrl = "jdbc:mysql://localhost:3306/DB_HOPITAL";
        String user = "root";
        String password = "";

        // Load Patients, Medecins, and Consultations tables into DataFrames
        Dataset<Row> patientsDf = spark.read()
                .format("jdbc")
                .option("url", jdbcUrl)
                .option("dbtable", "PATIENTS")
                .option("user", user)
                .option("password", password)
                .load();

        Dataset<Row> medecinsDf = spark.read()
                .format("jdbc")
                .option("url", jdbcUrl)
                .option("dbtable", "MEDECINS")
                .option("user", user)
                .option("password", password)
                .load();

        Dataset<Row> consultationsDf = spark.read()
                .format("jdbc")
                .option("url", jdbcUrl)
                .option("dbtable", "CONSULTATIONS")
                .option("user", user)
                .option("password", password)
                .load();

        // 1. Display the number of consultations per day
        System.out.println("1- Number of consultations per day:");
        consultationsDf.groupBy("date_consultation")
                .count()
                .orderBy("date_consultation")
                .show();

        // 2. Display the number of consultations per doctor (NOM | PRENOM | NOMBRE DE CONSULTATION)
        System.out.println("2- Number of consultations per doctor:");
        Dataset<Row> medecinConsultationCount = consultationsDf
                .join(medecinsDf, consultationsDf.col("id_medecin").equalTo(medecinsDf.col("id")))
                .groupBy("nom", "prenom")
                .count()
                .withColumnRenamed("count", "nombre_de_consultations");

        medecinConsultationCount
                .select("nom", "prenom", "nombre_de_consultations")
                .orderBy("nombre_de_consultations")
                .show();

        // 3. Display for each doctor, the number of distinct patients he has assisted
        System.out.println("3- Number of distinct patients per doctor:");
        Dataset<Row> medecinPatientsCount = consultationsDf
                .join(medecinsDf, consultationsDf.col("id_medecin").equalTo(medecinsDf.col("id")))
                .groupBy("nom", "prenom")
                .agg(functions.countDistinct("id_patient").alias("nombre_de_patients"));

        medecinPatientsCount
                .select("nom", "prenom", "nombre_de_patients")
                .orderBy("nombre_de_patients")
                .show();

        // Stop the Spark session
        spark.stop();
    }
}
