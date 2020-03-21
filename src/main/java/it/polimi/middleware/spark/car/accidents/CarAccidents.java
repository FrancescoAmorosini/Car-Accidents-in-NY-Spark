package it.polimi.middleware.spark.car.accidents;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import it.polimi.middleware.spark.utils.LogUtils;
import it.polimi.middleware.spark.utils.Init;

import static it.polimi.middleware.spark.utils.Init.*;

public class CarAccidents {
	public static void main(String[] args) {
		LogUtils.setLogLevel();

		final String master = args.length > 0 ? args[0] : "local[4]";
		final String filePath = args.length > 1 ? args[1] : "./";

		final SparkSession spark = SparkSession
				.builder() 
				.master(master) 
				.appName("Car Accidents in New York")
				.getOrCreate();

		final StructType mySchema = Init.getCarAccidentsSchema();

		final Dataset<Row> ds = spark
				.read()
				.option("header", "true")
				.option("delimiter", ",").option("inferSchema", "false").schema(mySchema)
				.csv(filePath + "files/sample.csv");

		//Filtering casualties and injuries mismatch
		Dataset<Row> corrected_ds = Init.clearIncorrectValues(ds);
		corrected_ds.printSchema();

		//Q1 Number of lethal accidents per week throughout the entire dataset
		first_query(corrected_ds);
		//Q2 Number of accidents and percentage of number of deaths per contributing factor in the dataset.
		second_query(corrected_ds);
		//Q3 Number of accidents and average number of lethal accidents per week per borough.
		third_query(corrected_ds);
		spark.close();

	}
}
