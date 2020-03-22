package it.polimi.middleware.spark.car.accidents;

import org.apache.spark.SparkFiles;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import it.polimi.middleware.spark.utils.LogUtils;
import it.polimi.middleware.spark.utils.Init;

import java.util.Date;

import static it.polimi.middleware.spark.utils.Init.*;

public class CarAccidents {
	public static void main(String[] args) {
		LogUtils.setLogLevel();

		final String master = args.length > 0 ? args[0] : "local[4]";
		final String filePath = args.length > 1 ? args[1] : "./";

		final SparkSession spark = SparkSession
				.builder() 
				.master("spark://Lim1.homenet.telecomitalia.it:7077")
				.appName("Car Accidents in New York")
				.getOrCreate();

		final StructType mySchema = Init.getCarAccidentsSchema();

		spark.sparkContext().addFile("file:/Users/simonestaffa/Desktop/MW/Car-Accidents-in-NY-Spark/files/NYPD_Motor_Vehicle_Collisions.csv");

		final long startLoadingDataFromFile = new Date().getTime();
		final Dataset<Row> ds = spark
				.read()
				.option("header", "true")
				.option("delimiter", ",").option("inferSchema", "false").schema(mySchema)
				.csv("file:/Users/simonestaffa/Desktop/MW/Car-Accidents-in-NY-Spark/files/NYPD_Motor_Vehicle_Collisions.csv");
		final long endLoadingDataFromFile = new Date().getTime();
		final long loadingDataFromFileTime = endLoadingDataFromFile - startLoadingDataFromFile;
		//Filtering casualties and injuries mismatch
		Dataset<Row> corrected_ds = Init.clearIncorrectValues(ds);
		corrected_ds.printSchema();

		//Q1 Number of lethal accidents per week throughout the entire dataset
		final long startQuery1 = new Date().getTime();
		first_query(corrected_ds);
		final long endQuery1 = new Date().getTime();
		final long query1 = endQuery1 - startQuery1;

		//Q2 Number of accidents and percentage of number of deaths per contributing factor in the dataset.
		final long startQuery2 = new Date().getTime();
		second_query(corrected_ds);
		final long endQuery2 = new Date().getTime();
		final long query2 = endQuery2 - startQuery2;

		//Q3 Number of accidents and average number of lethal accidents per week per borough.
		final long startQuery3 = new Date().getTime();
		third_query(corrected_ds);
		final long endQuery3 = new Date().getTime();
		final long query3 = endQuery3 - startQuery3;

		spark.close();

		System.out.println("---\nLOADING DATA FROM FILE: " + loadingDataFromFileTime);
		System.out.println("---\nQUERY 1: " + query1);
		System.out.println("---\nQUERY 2: " + query2);
		System.out.println("---\nQUERY 3: " + query3);

	}
}
