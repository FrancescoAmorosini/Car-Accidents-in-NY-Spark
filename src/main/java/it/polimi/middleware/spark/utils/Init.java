package it.polimi.middleware.spark.utils;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.expressions.*;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;



public class Init {

	public static StructType getCarAccidentsSchema(){

		final List<StructField> mySchemaFields = new ArrayList<>();
		mySchemaFields.add(DataTypes.createStructField("DATE", DataTypes.StringType, false));
		mySchemaFields.add(DataTypes.createStructField("TIME", DataTypes.StringType, false));
		mySchemaFields.add(DataTypes.createStructField("BOROUGH", DataTypes.StringType, true));
		mySchemaFields.add(DataTypes.createStructField("ZIP CODE", DataTypes.StringType, true));
		mySchemaFields.add(DataTypes.createStructField("LATITUDE", DataTypes.DoubleType, true));
		mySchemaFields.add(DataTypes.createStructField("LONGITUDE", DataTypes.DoubleType, true));
		mySchemaFields.add(DataTypes.createStructField("LOCATION", DataTypes.StringType, true));
		mySchemaFields.add(DataTypes.createStructField("ON STREET NAME", DataTypes.StringType, true));
		mySchemaFields.add(DataTypes.createStructField("CROSS STREET NAME", DataTypes.StringType, true));
		mySchemaFields.add(DataTypes.createStructField("OFF STREET NAME", DataTypes.StringType, true));
		mySchemaFields.add(DataTypes.createStructField("NUMBER OF PERSONS INJURED", DataTypes.IntegerType, true));
		mySchemaFields.add(DataTypes.createStructField("NUMBER OF PERSONS KILLED", DataTypes.IntegerType, true));
		mySchemaFields.add(DataTypes.createStructField("NUMBER OF PEDESTRIANS INJURED", DataTypes.IntegerType, true));
		mySchemaFields.add(DataTypes.createStructField("NUMBER OF PEDESTRIANS KILLED", DataTypes.IntegerType, true));
		mySchemaFields.add(DataTypes.createStructField("NUMBER OF CYCLIST INJURED", DataTypes.IntegerType, true));
		mySchemaFields.add(DataTypes.createStructField("NUMBER OF CYCLIST KILLED", DataTypes.IntegerType, true));
		mySchemaFields.add(DataTypes.createStructField("NUMBER OF MOTORIST INJURED", DataTypes.IntegerType, true));
		mySchemaFields.add(DataTypes.createStructField("NUMBER OF MOTORIST KILLED", DataTypes.IntegerType, true));
		mySchemaFields.add(DataTypes.createStructField("CONTRIBUTING FACTOR VEHICLE 1", DataTypes.StringType, true));
		mySchemaFields.add(DataTypes.createStructField("CONTRIBUTING FACTOR VEHICLE 2", DataTypes.StringType, true));
		mySchemaFields.add(DataTypes.createStructField("CONTRIBUTING FACTOR VEHICLE 3", DataTypes.StringType, true));
		mySchemaFields.add(DataTypes.createStructField("CONTRIBUTING FACTOR VEHICLE 4", DataTypes.StringType, true));
		mySchemaFields.add(DataTypes.createStructField("CONTRIBUTING FACTOR VEHICLE 5", DataTypes.StringType, true));
		mySchemaFields.add(DataTypes.createStructField("UNIQUE KEY", DataTypes.StringType, false));
		mySchemaFields.add(DataTypes.createStructField("VEHICLE TYPE CODE 1", DataTypes.StringType, true));
		mySchemaFields.add(DataTypes.createStructField("VEHICLE TYPE CODE 2", DataTypes.StringType, true));
		mySchemaFields.add(DataTypes.createStructField("VEHICLE TYPE CODE 3", DataTypes.StringType, true));
		mySchemaFields.add(DataTypes.createStructField("VEHICLE TYPE CODE 4", DataTypes.StringType, true));
		mySchemaFields.add(DataTypes.createStructField("VEHICLE TYPE CODE 5", DataTypes.StringType, true));

		final StructType mySchema = DataTypes.createStructType(mySchemaFields);

		return mySchema;
	}

	public static Dataset<Row> clearIncorrectValues(Dataset<Row> ds){
		// Create 2 new columns with the correct number of causalities and injured
		/*final Dataset<Row> ds_with_correct_nums = ds
				.withColumn("TOTAL_INJURED",
						ds.col("NUMBER OF PEDESTRIANS INJURED")
								.plus(ds.col("NUMBER OF CYCLIST INJURED")
										.plus(ds.col("NUMBER OF MOTORIST INJURED"))))
				.withColumnRenamed("NUMBER OF PERSONS INJURED", "FAKETOTAL_I")

				.withColumn("TOTAL_KILLED",
						ds.col("NUMBER OF PEDESTRIANS KILLED")
								.plus(ds.col("NUMBER OF CYCLIST KILLED")
										.plus(ds.col("NUMBER OF MOTORIST KILLED"))))
				.withColumnRenamed("NUMBER OF PERSONS KILLED", "FAKETOTAL_K");*/

		// Filter those rows that do not match the number of causalities or injured
		final Dataset<Row> ds_corrected = ds
				.drop(ds.col("TIME"))
				.drop(ds.col("ZIP CODE"))
				.drop(ds.col("LATITUDE"))
				.drop(ds.col("LONGITUDE"))
				.drop(ds.col("LOCATION"))
				.drop(ds.col("ON STREET NAME"))
				.drop(ds.col("CROSS STREET NAME"))
				.drop(ds.col("OFF STREET NAME"))
				.drop(ds.col("VEHICLE TYPE CODE 1"))
				.drop(ds.col("VEHICLE TYPE CODE 2"))
				.drop(ds.col("VEHICLE TYPE CODE 3"))
				.drop(ds.col("VEHICLE TYPE CODE 4"))
				.drop(ds.col("VEHICLE TYPE CODE 5"));


		return ds_corrected;
	}

	//Q1 Number of lethal accidents per week throughout the entire dataset
	public static Dataset<Row> first_query(Dataset<Row> ds){
		final Dataset<Row> ds_lethal_per_week = ds
				.withColumn("WEEK", weekofyear(to_date(ds.col("DATE"), "MM/dd/yyyy")))
				.withColumn("YEAR", year(to_date(ds.col("DATE"), "MM/dd/yyyy")))
				.withColumn("IS_LETHAL", col("NUMBER OF PERSONS KILLED").gt(0).cast(DataTypes.IntegerType))
				.groupBy("YEAR", "WEEK").sum("IS_LETHAL")
				.withColumnRenamed("sum(IS_LETHAL)", "LETHAL_ACCIDENTS");

		ds_lethal_per_week.show(30, true);
		return ds_lethal_per_week;
	}

	//Q2 Number of accidents and percentage of number of deaths per contributing factor in the dataset.
	public static Dataset<Row> second_query(Dataset<Row> ds){
		final Dataset<Row> ds_cause1 = ds
				.groupBy(ds.col("CONTRIBUTING FACTOR VEHICLE 1"),
						ds.col("UNIQUE KEY"))
				.sum("NUMBER OF PERSONS KILLED")
				.filter(ds.col("CONTRIBUTING FACTOR VEHICLE 1").isNotNull())
				.withColumnRenamed("CONTRIBUTING FACTOR VEHICLE 1", "CONTRIBUTING FACTOR");
		final Dataset<Row> ds_cause2 = ds
				.groupBy(ds.col("CONTRIBUTING FACTOR VEHICLE 2"),
						ds.col("UNIQUE KEY"))
				.sum("NUMBER OF PERSONS KILLED")
				.filter(ds.col("CONTRIBUTING FACTOR VEHICLE 2").isNotNull())
				.withColumnRenamed("CONTRIBUTING FACTOR VEHICLE 2", "CONTRIBUTING FACTOR");
		final Dataset<Row> ds_cause3 = ds
				.groupBy(ds.col("CONTRIBUTING FACTOR VEHICLE 3"),
						ds.col("UNIQUE KEY"))
				.sum("NUMBER OF PERSONS KILLED")
				.filter(ds.col("CONTRIBUTING FACTOR VEHICLE 3").isNotNull())
				.withColumnRenamed("CONTRIBUTING FACTOR VEHICLE 3", "CONTRIBUTING FACTOR");
		final Dataset<Row> ds_cause4 = ds
				.groupBy(ds.col("CONTRIBUTING FACTOR VEHICLE 4"),
						ds.col("UNIQUE KEY"))
				.sum("NUMBER OF PERSONS KILLED")
				.filter(ds.col("CONTRIBUTING FACTOR VEHICLE 4").isNotNull())
				.withColumnRenamed("CONTRIBUTING FACTOR VEHICLE 4", "CONTRIBUTING FACTOR");
		final Dataset<Row> ds_cause5 = ds
				.groupBy(ds.col("CONTRIBUTING FACTOR VEHICLE 5"),
						ds.col("UNIQUE KEY"))
				.sum("NUMBER OF PERSONS KILLED")
				.filter(ds.col("CONTRIBUTING FACTOR VEHICLE 5").isNotNull())
				.withColumnRenamed("CONTRIBUTING FACTOR VEHICLE 5", "CONTRIBUTING FACTOR");

		Dataset<Row> ds_all_causes = ds_cause1.union(ds_cause2).union(ds_cause3).union(ds_cause4).union(ds_cause5).dropDuplicates()
				.withColumn("IS_LETHAL", col("sum(NUMBER OF PERSONS KILLED)").gt(0).cast(DataTypes.IntegerType))
				.groupBy(col("CONTRIBUTING FACTOR"))
				.agg(sum("sum(NUMBER OF PERSONS KILLED)"), count("UNIQUE KEY"),
						sum("IS_LETHAL"))
				.withColumnRenamed("sum(sum(NUMBER OF PERSONS KILLED))", "NUMBER OF PERSONS KILLED")
				.withColumnRenamed("count(UNIQUE KEY)", "TOTAL_ACCIDENTS")
				.withColumnRenamed("sum(IS_LETHAL)", "LETHAL_ACCIDENTS")
				.withColumn("%LETHAL", format_number(expr("(LETHAL_ACCIDENTS / TOTAL_ACCIDENTS) * 100"),2));

		ds_all_causes.show(30, true);
		return ds;
	}

	public static Dataset<Row> third_query(Dataset<Row> ds) {
		final Dataset<Row> ds_lethal = ds
				.filter(ds.col("BOROUGH").isNotNull());
		//ds_lethal.select("UNIQUE KEY","TOTAL_KILLED").show(30,false);
		final Dataset<Row> ds_lethal_per_week = ds_lethal
				.withColumn("BOROUGH", ds_lethal.col("BOROUGH"))
				.withColumn("WEEK", weekofyear(to_date(ds_lethal.col("DATE"), "MM/dd/yyyy")))
				.withColumn("YEAR", year(to_date(ds_lethal.col("DATE"), "MM/dd/yyyy")))
				.withColumn("IS_LETHAL", col("NUMBER OF PERSONS KILLED").cast(DataTypes.IntegerType))
				.groupBy("BOROUGH","YEAR", "WEEK")
				.agg(sum("NUMBER OF PERSONS KILLED"), count("UNIQUE KEY"), sum("IS_LETHAL"), avg("IS_LETHAL"))
				.withColumnRenamed("sum(NUMBER OF PERSONS KILLED)", "TOTAL_KILLED")
				.withColumnRenamed("count(UNIQUE KEY)", "TOTAL_ACCIDENTS")
				.withColumnRenamed("sum(IS_LETHAL)", "LETHAL_ACCIDENTS")
				.withColumnRenamed("avg(IS_LETHAL)", "AVG_LETHAL_ACCIDENTS");

		ds_lethal_per_week.orderBy(ds_lethal_per_week.col("YEAR").asc(), ds_lethal_per_week.col("WEEK").asc()).show(50, true);
		return ds;
	}
}