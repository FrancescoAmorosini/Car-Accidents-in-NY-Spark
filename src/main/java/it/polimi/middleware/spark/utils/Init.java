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
		final Dataset<Row> ds_with_correct_nums = ds
            .withColumn("TOTAL_INJURED",
                ds.col("NUMBER OF PEDESTRIANS INJURED")
					.plus(ds.col("NUMBER OF CYCLIST INJURED")
					.plus(ds.col("NUMBER OF MOTORIST INJURED"))))
			.withColumnRenamed("NUMBER OF PERSONS INJURED", "FAKETOTAL_I")
			
            .withColumn("TOTAL_KILLED",
                ds.col("NUMBER OF PEDESTRIANS KILLED")
						.plus(ds.col("NUMBER OF CYCLIST KILLED")
						.plus(ds.col("NUMBER OF MOTORIST KILLED"))))
            .withColumnRenamed("NUMBER OF PERSONS KILLED", "FAKETOTAL_K");

        // Filter those rows that do not match the number of causalities or injured
		final Dataset<Row> ds_corrected = ds_with_correct_nums
        .filter(ds_with_correct_nums.col("FAKETOTAL_I").equalTo(ds_with_correct_nums.col("TOTAL_INJURED"))
				.and(ds_with_correct_nums.col("FAKETOTAL_K").equalTo(ds_with_correct_nums.col("TOTAL_KILLED"))))
		.drop(ds_with_correct_nums.col("FAKETOTAL_K"))
		.drop(ds_with_correct_nums.col("FAKETOTAL_I"));
        
        return ds_corrected;
    }

	//Q1 Number of lethal accidents per week throughout the entire dataset
	public static Dataset<Row> first_query(Dataset<Row> ds){
		final Dataset<Row> ds_lethal = ds
			.filter(ds.col("TOTAL_KILLED").gt(0));

		final Dataset<Row> ds_lethal_per_week = ds_lethal
				.withColumn("WEEK", weekofyear(to_date(ds_lethal.col("DATE"), "MM/dd/yyyy")))
				.withColumn("YEAR", year(to_date(ds_lethal.col("DATE"), "MM/dd/yyyy")))
				.groupBy("YEAR", "WEEK").sum("TOTAL_KILLED");

		ds_lethal_per_week.show();
		return ds_lethal_per_week;
	}

	//Q2 Number of accidents and percentage of number of deaths per contributing factor in the dataset.
	public static Dataset<Row> second_query(Dataset<Row> ds){
		final Dataset<Row> ds_cause1 = ds
		.groupBy(ds.col("CONTRIBUTING FACTOR VEHICLE 1"), 
				ds.col("UNIQUE KEY"))
		.sum("TOTAL_KILLED")
		.filter(ds.col("CONTRIBUTING FACTOR VEHICLE 1").isNotNull())
		.withColumnRenamed("CONTRIBUTING FACTOR VEHICLE 1", "CONTRIBUTING FACTOR");
		final Dataset<Row> ds_cause2 = ds
		.groupBy(ds.col("CONTRIBUTING FACTOR VEHICLE 2"), 
				ds.col("UNIQUE KEY"))
		.sum("TOTAL_KILLED")
		.filter(ds.col("CONTRIBUTING FACTOR VEHICLE 2").isNotNull())
		.withColumnRenamed("CONTRIBUTING FACTOR VEHICLE 2", "CONTRIBUTING FACTOR");
		final Dataset<Row> ds_cause3 = ds
		.groupBy(ds.col("CONTRIBUTING FACTOR VEHICLE 3"), 
				ds.col("UNIQUE KEY"))
		.sum("TOTAL_KILLED")
		.filter(ds.col("CONTRIBUTING FACTOR VEHICLE 3").isNotNull())
		.withColumnRenamed("CONTRIBUTING FACTOR VEHICLE 3", "CONTRIBUTING FACTOR");
		final Dataset<Row> ds_cause4 = ds
		.groupBy(ds.col("CONTRIBUTING FACTOR VEHICLE 4"), 
				ds.col("UNIQUE KEY"))
		.sum("TOTAL_KILLED")
		.filter(ds.col("CONTRIBUTING FACTOR VEHICLE 4").isNotNull())
		.withColumnRenamed("CONTRIBUTING FACTOR VEHICLE 4", "CONTRIBUTING FACTOR");
		final Dataset<Row> ds_cause5 = ds
		.groupBy(ds.col("CONTRIBUTING FACTOR VEHICLE 5"), 
				ds.col("UNIQUE KEY"))
		.sum("TOTAL_KILLED")
		.filter(ds.col("CONTRIBUTING FACTOR VEHICLE 5").isNotNull())
		.withColumnRenamed("CONTRIBUTING FACTOR VEHICLE 5", "CONTRIBUTING FACTOR");

		Dataset<Row> ds_all_causes = ds_cause1.union(ds_cause2).union(ds_cause3).union(ds_cause4).union(ds_cause5).dropDuplicates()
		.withColumn("IS_LETHAL", col("sum(TOTAL_KILLED)").gt(0).cast(DataTypes.IntegerType));

		Dataset<Row> ds_count_causes = ds_all_causes
		.groupBy(ds_all_causes.col("CONTRIBUTING FACTOR"))
		.agg(sum("sum(TOTAL_KILLED)"), count("UNIQUE KEY"),
		sum("IS_LETHAL"))
		.withColumnRenamed("sum(sum(TOTAL_KILLED))", "TOTAL_KILLED")
		.withColumnRenamed("count(UNIQUE KEY)", "TOTAL_ACCIDENTS")
		.withColumnRenamed("sum(IS_LETHAL)", "LETHAL_ACCIDENTS")
		.withColumn("%LETHAL", format_number(expr("LETHAL_ACCIDENTS / TOTAL_ACCIDENTS"),5));

		ds_count_causes.show(30, false);
		return ds;
	}
}