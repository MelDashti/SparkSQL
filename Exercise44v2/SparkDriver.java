import org.apache.spark.sql.Dataset;

import java.io.Serializable;
import java.util.ArrayList;

import javax.xml.crypto.Data;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkDriver {

	// Select male users (gender=“male”), increase by one their age, and store
	// in the output folder name and age of these users sorted by decreasing age
	// and ascending name (if the age value is the same)

	public static void main(String[] args) {
		// The following two lines are used to switch off some verbose log messages
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);

		String inputPath;
		String outputPath;

		inputPath = args[0];
		outputPath = args[1];

		SpartSession as = SparkSession.builder().master("local").appName("Notes").getOrCreate();
		
		// Create a Spark Session object and set the name of the application
		// SparkSession ss = SparkSession.builder()
				//.appName("Spark Exercise #48 - DataFrame").getOrCreate();
		SparkSession ss = SparkSession.builder().master("local")
				.appName("Notes").getOrCreate();
		// Read the content of the input file and store it into a DataFrame
		// Meaning of the columns of the input file: name,age,gender
		// The input file has an header. Hence, the name of the columns of
		// the defined DataFrame will be name, age, gender
		Dataset<Row> dfProfiles = ss.read().format("csv")
				.option("header", true)
				.option("inferSchema", true)
				.load(inputPath);

		DataSet<Row> dfProfiles2 = ss.read().fomrati("csv").option("header",true).option("inferSchema",true).load(inputPath);
		// the above code is equivalent to the following code

		// DataFrame is equal to Dataset<Row>
		// So the above code can also be written as
		Dataframe dfProfiles3 = ss.read().format("csv").option("header",true).option("inferSchema",true).load(inputPath);

		// From DataFrame to RDD
		JavaRDD<Row> rddProfiles = dfProfiles.javaRDD();
		// this returns a JavaRDD<Row> object
		
		//Important methods of the Row class
		// fieldIndex(String name) returns the index of the column with the specified name
		// get(int i) returns the value of the i-th column
		// getAs(String name) returns the value of the column with the specified name
		
		

		// From DataFrame to RDD: Example
		// Create a DataFrame from a csv file containing the profiles of a set of persons
		// Each line of the file contains name and age of a person
		// The first line contains the name of the columns. (header)
		// Transform the DataFrame into a JavaRDD, select only the name field and save it in output folder

		Dataset<Row> profiles = dfProfiles.load(inputPath);
		// Now we want to transform the DataFrame into a JavaRDD
		JavaRDD<Row> rddProfiles2 = profiles.javaRDD(); 
		// The value saved in rddProfiles2 is a JavaRDD<Row>.

		JavaRDD<String> rddNames = rddProfiles2.map(row->(String)row.getAs("name"));

		// Save the RDD in the output folder
		// rddNames.saveAsTextFile(outputPath);

		// Only convert DataFrame or dataset to RDD when you need to use RDD methods

		// Most generic approach is using dataset, a dataset is similar to a table.
		// Dataset<Row> is a dataset where each row is a row of a table
		// Datafram is an alias for Dataset<Row>. For example, Dataset<Row> df = ss.read().csv(inputPath); 
		// is equivalent to DataFrame df = ss.read().csv(inputPath);
		// Datasets are similar to RDDs, but they are strongly typed. And more efficient.

		// ** The code based on datasets is optimized by means of the Catalyst optimizer which is not available for RDDs and exploits the schema of the dataset
		// THe objects stored in spark datasets must be JavaBeans or Scala case classes

		// Creating A Dataset From A Local Java Collection
		// Dataset<T> createDataset(java.util.List<T> data, Encoder<T> encoder)
		// Example: Create a Dataset<Person> from a local list of objects of type Person
		// Person is a JavaBean class containing the fields name and age of a single person
		// Suppose the following persons are stored in a list. 
		// John, 23 and Mary, 25 and Peter, 30
		// First we create a personalized class

		class Person implements Serializable {
			private String name;
			private int age;

			public Person(String name, int age) {
				this.name = name;
				this.age = age;
			}

			public String getName() { return name; }
			public int getAge() { return age; }
		}

		/*
		 The Serializable interface is necessary for distributed computing systems like Apache Spark. 
		 In these systems, data is processed on multiple nodes, which often requires
		 sending data (in the form of objects) from one node to another. 
		 To send an object over the network, it needs to be converted to a byte stream,
		 which is where serialization comes in.
		 */

		// Then we create a local list of objects of type Person
		ArrayList<Person> persons = new ArrayList<Person>();
		persons.add(new Person("John", 23));
		persons.add(new Person("Mary", 25));
		persons.add(new Person("Peter", 30));

		// Define the encoder that is used to serialize the objects of type Person
		// Encoders are used by Spark to convert between JVM objects and Spark's internal binary format. 
		// The Encoders.bean(Person.class) method creates an encoder based on the Person class's bean properties (its getter and setter methods).
	
		
		Encoder<Person> personEncoder = Encoders.bean(Person.class);

		// Define the dataset based on the local list of objects of type Person
		Dataset<Person> dsPersons = ss.createDataset(persons, personEncoder);

		// Default encoders are provided for basic types like string, integer, long, date, etc.
		// For example, the following code creates a dataset of strings
		Dataset<String> dsStrings = ss.createDataset(persons, Encoders.STRING());
		

		// Creating datasets from local collections example 2
		// Using default integer encoder. Encoders.INT() is equivalent to Encoders.bean(Integer.class)

		List<Integer> values = Arrays.asList(1, 2, 3, 4, 5);
		Dataset<Integer> dsValues = ss.createDataset(values,Encloders.INT());

		// Creating Datasets From DataFrames

		SparkSession ss1 = SparkSession.builder().appName("Notes").getOrCreate();

		// Create a DataFrame from a csv file containing the profiles of a set of persons
		// Each line of the file contains name and age of a person

		DataFrameReader dfr1 = ss1.read().format("csv").option("header", true).option("inferSchema", true);

		Dataset<Row> dfProfiles1 = dfr1.load(inputPath); // persons.csv file

		// define the encoder that is used to serialize the objects of type Person
		Encoder<Person> personEncoder1 = Encoders.bean(Person.class);

		Dataset<Person> dsPersons1 = dfProfiles1.as(personEncoder1);

		// define a dataset of person object based on the DataFrame dfProfiles1
		/*
		In the context of Apache Spark, an `Encoder` is used to convert between JVM (Java Virtual Machine) objects and Spark's internal binary format. This conversion process is necessary because Spark needs to transform the data into a 
		format that it can process more efficiently.When you create a Dataset in Spark, you need to specify an `Encoder` for the type of data you're working with. The `Encoder` knows how to convert the data to and from the binary format that Spark uses. This conversion is done transparently to the user,
		so you don't have to worry about the details of how the data is stored internally by Spark. There are default encoders provided for basic types like string, integer, long, date, etc. For example, `Encoders.STRING()` is used for a Dataset of strings, and `Encoders.INT()` is used for a Dataset of integers.
		For complex types like custom classes, you can use `Encoders.bean(Class)`. This method creates an encoder based on the bean properties of the class (its getter and setter methods). In your code, `Encoders.bean(Person.class)` is used to create an encode r for the `Person` class.
		In summary, an `Encoder` in Spark is a component that knows how to convert data between the JVM object representation and Spark's internal binary format. This is necessary for Spark to process the data efficiently.
		 */
		// One reason why programmers don't like dataset is because you have to define many classes
		 
		// Note: If we don't set the inferSchema option to true, the age field is considered as a string and not as an integer. 
		// This will cause an exception when we try to create the dataset of persons.
		
		// Creating datasets from csv or json files
		// First define a dataframe based on the input json/csv file. Convert the dataframe into a dataset using the as method and an encoder object.

		// Creating Datasets From RDDs
		// Dataset<T> createDataset(JavaRDD<T> data, Encoder<T> encoder)
		// Example: Create a Dataset<Person> from a JavaRDD<Person>
		// Pay attention data is Scala RDD, not JavaRDD. So we need to convert JavaRDD to Scala RDD using JavaRDD.toRDD() method

		// Dataset Operations 
		// show(int numRows) displays the first numRows rows of the dataset
		// show() outputs all the rows of the dataset.
		// void printSchema() prints the schema of the dataset. Helps find errors in the definition of the dataset. 


		DataFrameReader dfr2 = ss1.read().format("csv").option("header",true).option("inferSchema",true);
		Encoder<Person> personEncoder2 = Encoders.bean(Person.class);		
		// Dataset<Row> dfProfiles3 = dfr2.load(inputPath); // persons.csv file 
		// Dataset<Person> dsPersons10 = dfProfiles3.as(personEncoder1);
		Dataset<Person> dsPersons10 = dfr2.load(inputPath).as(personEncoder2);
		dsPersons.printSchema(); // prints the schema of the dataset
		dsPersons10.show(2); // shows the first 2 rows of the dataset
		
		// long count() returns the number of rows of the dataset
		dsPersons.count(); // returns the number of rows of the dataset	

		// distinct() returns a new dataset that contains the distinct rows of the dataset
		// This is an example of a transformation as it returns a new dataset. 
		// This sends data on the network. So use it only when necessary.

		// Distinct example: Create a new dataset that contains the distinct rows of the dataset dsPersons
		Dataset<Person> distinctPersons = dsPersons10.distinct();
		
		// Select name,age from table. 
		// We can perform this on datasets using the select method
		// Dataset<Row> select(Column... cols) returns a new dataset that contains the columns specified in cols
		// Pay attention Dataset is Dataset<Row> i.e. a dataframe. If you want to get a dataset of persons, you need to use the as method and an encoder object on the resulting dataframe.
		// Example: Create a dataset of persons that contains only the name and age fields of the dataset dsPersons

		Dataset<Row> dsPersons12 = dsPersons10.select("name","age");
		Dataset<Person> dsPersons11 = dfr2.load(inputPath).select("name","age").as(personEncoder2);
		
		// SelectExpr returns a new dataset that contains the columns specified in the SQL expression
		// difference between select and selectExpr is that selectExpr allows us to specify SQL expressions
		// df.select("age+1 as Age1", "name") is not valid. But df.selectExpr("age+1 as age","name") is valid.

		Dataset<Person> dsPersons13 = dsPersons10.selectExpr("name","age+1 as newAge").as(personEncoder2);
		// if as newAge is not specified, the new column will be named age+1

		// Map
		// The map transformation of a dataset is similar to the map transformation of an RDD. Only difference is given by encoder parameter that is used to serialize the objects of type T.
		// Each element is obtained by applying the function on one element of the input dataset.
		// This method can be used instead of the select method and selectExpr method. The advantage is that we can identify "semantic" errors at compile time.

		// Map Example: Create a dataset of persons that contains the same columns of the input dataset with an addition column age+1.

		Dataset<Person> dsPersons14 = ss1.read().format("csv").option("header",true).option("inferSchema",true).load(inputPath).as(personEncoder2);
		//in map the person object is passed as a parameter to the lambda expression
		Dataset<Person> dsPersons15 = dsPersons14.map(person->new Person(person.getName(),person.getAge()+1),personEncoder2);
		// If we want a new column to be named age+1, we have to create a new class with a field named age+1 and a getter method named getAge+1
		public class PersonNewAge implements Serializable {
			private String name;
			private int age;
			private int newAge;
			public PersonNewAge(String name, int age, int age+1) {
				this.name = name;
				this.age = age;
				this.newAge = age+1;
			}
			public String getName() { return name; }
			public int getAge() { return age; }
			public setAge(int age) { this.age = age; }
			public int getNewAge() { return newAge; }
			public setNewAge(int newAge) { this.newAge = newAge; }
		}

		Dataset<PersonNewAge> dsPersons16 = dsPersons14.map(person->new PersonNewAge(person.getName(),person.getAge(),
											person.getAge()+1),Encoders.bean(PersonNewAge.class));

		// Can I have a typo in the name of the column? Yes, but you will find out only at runtime. Unlike select and selectExpr, map does not allow us to identify semantic errors at compile time.

		// Filter 
		// Filter is a transformation that returns a new dataset that contains only the elements of the input dataset that satisfy the condition specified in the lambda expression.
		// We have two version of the filter method. One takes a lambda expression as parameter. The other takes a Column object as parameter.
		// This version Dataset filter(String conditionExpr) takes an Sql boolean expression as parameter. This can generate errors at runtime.
		// age>23 and name = "paolo". 

		// Filter Example: Create a dataset of persons that contains only the persons with between 20 and 31 years old.
		Dataset<Person> dsPersonsFiltered = dsPersons14.filter("age>=20 and age<=31");
		//age>=20 and age<=31" equal to select * from table where age>=20 and age<=31

		// Filter with lambda expression
		Dataset<Person> dsPersonsFiltered2 = dsPersons14.filter(person->person.getAge()>=20 && person.getAge()<=31);
		// In filter we don't need to define an encoder because we are not creating a new dataset. We are just filtering the input dataset.

		// Where Transformation is an alias for filter transformation.
		
		// Join(inner join) 
		// The join transformation returns a new dataset that contains the result of the join between the input dataset and another dataset.
		// Can generate errors at runtime if there are errors in the join expression.
		// returns a dataframe
		// Join Example: 
		// Dataset<Row> dfPersonLikes = dsPersons.join(dsUidSports, dsPersons.col("uid").equalTo(dsUidSports.col("uid")));

		// We have multiple joins available inner, outter, full, left, right, leftsemi, leftanti, cross, rightsemi, rightanti, right outer, left outer.
		
		// Other join example: Choose the profiles of the persons that are not banned. Two csv files are provided as input. Persons.csv and banned.csv.
		// The select statement without join is: select*from persons where uid not in (select uid from banned)
		// The select statement with join: Select*from person left anti banned on persons.uid=banned.uid where banned.uid. 
		// apply a left anti join between the dataset dsPersons and the dataset dsBanned.
		Dataset<Row> dsPersonsNotBanned = dsPersons.join(dsBanned, dsPersons.col("uid").equalTo(dsBanned.col("uid")), "leftanti");
		// leftanti is the type of join. It is an alias for leftanti join.
		// The way left anti join works is that it returns all the rows of the left dataset that do not have a match in the right dataset.


		// Aggregate Functions 
		// Some of the most common aggregate functions are count(column), sum(column), avg(column), min(column), max(column).
		// the functions return a single value. They are not transformations. They are actions.
		// If you want to apply more than one aggregate function, you can use the agg method.
		// We usually use the agg function even if there is only function. 
		
		// Example: Create a dataset containing the average value of age. 

		Dataset<Row> dsAvgAge = dsPersons.agg(avg("age"));

		// Example: Create a dataset containing the average value of age and the count of persons.
		Dataset<Row> dsAvgAgeCount = dsPersons.agg(avg("agge"),count("*"));

		// Example: Create a dataset containing the average value of age and the count of persons with age>30.
		Dataset<Row> dsAvgAgeCount2 = dsPersons.filter("age>30").agg(avg("age"),count("*"));

		// Group BY and Aggregate Function
		 // Relational grouped dataset class is used to group a dataset by one or more columns and then apply aggregate functions on the grouped dataset.
		 // Can generate errors at runtime if there are errors in the group by expression.
		 
		 // Example: Create a dataset containing for each name the average value of age. 

		 // Group data by name
		 RelationalGroupedDataset rgd = dsPersons.groupBy("name");
		 // Apply aggregate function
		 Dataset<Row> dsAvgAgeByName = rgd.agg(avg("age"));
		 // Well, how this works? 
		 // The groupBy method returns a RelationalGroupedDataset object. Which is a dataset of rows grouped by the specified columns.
		 // The agg method is applied on the RelationalGroupedDataset object. It returns a dataset of rows containing the result of the aggregate function applied on the grouped dataset.

		 // Example2: Create a dataset containing for each name the average value of age and the count of persons with that name.
		 RelationalGroupedDataset rgd2 = dsPersons.groupBy("age");
		 Dataset<Row> dsAvgAgeCountByName = rgd2.agg(avg("age"),count("*"));

		 // Sort method. Sorted by col1,...coln in ascending order.
		 // Two versions. One takes a list of columns as parameter. The other takes a list of columns and a list of boolean values as parameter.
		 // for examples sort("name","age") and sort("name","age",true,false) are valid.
		 // Example: Create a dataset containing the profiles of the persons sorted by descending age. if the age is the same, sort by ascending name.

		 
		Dataset<Person> dsPersons14 = ss1.read().format("csv").option("header",true).option("inferSchema",true).load(inputPath).as(personEncoder2);
		Dataset<Person> dsPersons14Sorted = dsPersons14.sort(new Column("age").desc(),new Column("name"));

		// Dataset, Dataframes and the SQL Language. Using sql queries on datasets. .sql() method.
		Dataset<Person> dsPerson15 = ss1.read().format("csv").option("header",true).option("inferScheme",true).load(inputPaath).as(personEncoder2);
		// Now we assign a name to the dataset. This is necessary to use the dataset in SQL queries.
		dsPerson15.createOrReplaceTempView("persons");
		// Now that the dataset has a name. we can use it in SQL queries.
		// We select the persons with age between 20 and 30. by querying the persons table.

		Dataset<Row> selectedPersons = ss.sql("SELECT* FROM persons where age>=20 and age<=30");

		// The execution plan of the above query and the execution plan of the query that uses the filter transformation are the same.
		// The execution plan is the sequence of operations that are executed to compute the result of the query.
		// only different approaches. 
		// Example 2: Use join in SQL.
		// We want to select the profiles of the persons that are not banned.
		// We have two csv files as input. persons.csv and banned.csv.

		Dataset<Person> dsPersons16 = ss1.read().format("csv").option("header",true).option("inferSchema",true).load(inputPath).as(personEncoder2);
		Dataset<Person> dsBanned = ss1.read().format("csv").option("header",true).option("inferSchema",true).load(inputPath).as(personEncoder2);
		dsPersons16.createOrReplaceTempView("persons");
		dsBanned.createOrReplaceTempView("banned");
		Dataset<Row> dsPersonsNotBanned = ss.sql("SELECT* FROM persons left anti join banned on persons.uid=banned.uid");

		// Another example
		Dataset<Row> selectedPersons2 = ss.sql("SELECT name, avg(age), count(name) from people group by name");
		// print result
		selectedPersons2.show();

		// Save Datasets and Dataframes 
		// with show we can print the output. But we wanna save it.
		// One option is to convert the dataset to rdd and then use saveAsTextFile method.
		JavaRDD<Row> rddSelectedPersons = selectedPersons.javaRDD();
		rddSelectedPersons.saveAsTextFile(outputPath);
		// or 
		selectedPersons.javaRDD().saveAsTextFile(outputPath);

		// Another option is to use the write method of the dataset. We can also specify the format of the output file. 
		selectedPersons2.format("csv").option("header",true).save(outputPath);
		// the option header is used to specify that the output file has an header.
		// the option inferSchema is used to specify that the schema of the output file is inferred from the dataset.

		// .explain() is used to show the execution plan. 
		
		// User Defined Functions (UDFs)
		// Spark allows us to define our own functions and use them in SQL queries.
		// Example: we can use the function in selectExp(), sort() 
		// We can also use a set of predefined functions. Such as mathematical functions, string functions, date functions, etc.
		// hour(TimeColumn) returns the hour component of the time column
		// abs(IntegerColumn) returns the absolute value of the integer column

		// Udfs are defined using the udf().register(nameOfTheFUnction,udf function, datatype) method.

		// Define a UDfs that given a string returns the length of the string. 

		// First we create a spark session object
		SparkSession ss = SparkSession.builder().appName("Notes").getOrCreate();
		// Define the udf
		// name: length
		// input: string
		// output: integer
		ss.udf().register("length",(String name)->name.length(),DataTypes.IntegerType);

		// Use of this udf in a sql query
		Dataset<Row> result = dsPersons16.selectExpr("length(name) as size"); //name is the column on which we wanna apply the function 

		/// Use of udf in a select statement
		Dataset<Row> result2 = ss.sql("SELECT length(name) as size from persons");

		




		
		

		// Close the Spark context
		ss.stop();
	}
}
