package it.polito.bigdata.spark.exercise44;

import org.apache.spark.api.java.*;

import scala.Tuple2;

import org.apache.spark.SparkConf;

public class SparkDriver {

	public static void main(String[] args) {

		String inputPathWatched;
		String inputPathPreferences;
		String inputPathMovies;
		String outputPath;
		double threshold;

		inputPathWatched = args[0];
		inputPathPreferences = args[1];
		inputPathMovies = args[2];
		outputPath = args[3];
		threshold = Double.parseDouble(args[4]);

		// Create a configuration object and set the name of the application
		SparkConf conf = new SparkConf().setAppName("Spark Exercise #44")
		.setMaster("local");

		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Read the content of the watched movies file
		JavaRDD<String> watchedRDD = sc.textFile(inputPathWatched);

		// Select only the userid and the movieid
		// Define a JavaPairRDD with movieid as key and userid as value
		JavaPairRDD<String, String> movieUserPairRDD = watchedRDD.mapToPair(line -> {

			String[] fields = line.split(",");
			Tuple2<String, String> movieUser = new Tuple2<String, String>(fields[1], fields[0]);

			return movieUser;
		});

		// Read the content of the movies file
		JavaRDD<String> moviesRDD = sc.textFile(inputPathMovies);

		// Select only the movieid and genre
		// Define a JavaPairRDD with movieid as key and genre as value
		JavaPairRDD<String, String> movieGenrePairRDD = moviesRDD.mapToPair(line -> {

			String[] fields = line.split(",");
			Tuple2<String, String> movieGenre = new Tuple2<String, String>(fields[0], fields[2]);

			return movieGenre;
		});

		// Join watched movie with movies
		JavaPairRDD<String, Tuple2<String, String>> joinWatchedGenreRDD = 
				movieUserPairRDD.join(movieGenrePairRDD);

		// Select only userid (as key) and genre (as value)
		// Map to pairs
		// Key = userid+watchedMovieGenre
		// Value = +1
		JavaPairRDD<Tuple2<String, String>, Integer> usersWatchedMovieGenreOneRDD 
		= joinWatchedGenreRDD.mapToPair(p -> 
		new Tuple2<Tuple2<String, String>, Integer>(
			new Tuple2<String, String>(p._2()._1(), p._2()._2()), 1));

		// Read the content of the preferences
		JavaRDD<String> preferencesRDD = sc.textFile(inputPathPreferences);

		// Define a JavaPairRDD with 
		// Key = userid+likedMovieGenre
		// Value = +1
		JavaPairRDD<Tuple2<String, String>, Integer> usersLikedMovieGenreOneRDD 
		= preferencesRDD.mapToPair(line -> {
					String[] fields = line.split(",");

					return new Tuple2<Tuple2<String, String>, Integer>
					(new Tuple2<String, String>(fields[0], fields[1]), 1);
				});

		// userWatchedLikedGenresRDD left outer join usersLikedMovieGenreOneRDD
		JavaPairRDD<Tuple2<String, String>, Tuple2<Integer, Optional<Integer>>> userWatchedLikedGenresRDD =
		usersWatchedMovieGenreOneRDD.leftOuterJoin(usersLikedMovieGenreOneRDD);


		// For debug
		// userWatchedLikedGenresRDD.foreach(p-> System.out.println(p));

		// Map each pair to a new pair
		// Key = UserId
		// Value = (1, 0/1) - The second part is 1 if the Optional integer is empty. 0 otherwise.
		// If Optional is empty it means that the user watched a movie genre 
		// that is not in his/her list of liked movie genres.
		JavaPairRDD<String,Tuple2<Integer, Integer>> userIDVisNotLikedVisRDD =
		userWatchedLikedGenresRDD
		.mapToPair(p -> {
			if (p._2()._2().isPresent()==false)
				return new Tuple2<String,Tuple2<Integer, Integer>>(p._1()._1(), new Tuple2<Integer, Integer>(1, 1));
			else 
				return new Tuple2<String,Tuple2<Integer, Integer>>(p._1()._1(), new Tuple2<Integer, Integer>(1, 0));
		});

		// Compute how many visualizations for each user and how many visualizations 
		// of movies associated with not liked movie genres.
		JavaPairRDD<String,Tuple2<Integer, Integer>> userIDNumVisNumNotLikedVisRDD =
		userIDVisNotLikedVisRDD.reduceByKey((v1,v2) ->
		 new Tuple2<Integer, Integer>(v1._1()+v2._1(), v1._2()+v2._2()));

		// Select the users with a "misleading profile"
		JavaRDD<String> misleadingUsersRDD = userIDNumVisNumNotLikedVisRDD
		.filter(p -> ((double)p._2()._2()/(double)p._2()._1())>threshold)
		.keys();

		misleadingUsersRDD.saveAsTextFile(outputPath);

		// Close the Spark context
		sc.close();
	}
}
