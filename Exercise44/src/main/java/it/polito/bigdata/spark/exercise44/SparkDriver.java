package it.polito.bigdata.spark.exercise44;

import org.apache.spark.api.java.*;

import scala.Tuple2;

import java.lang.reflect.Array;
import java.util.ArrayList;

import org.apache.spark.SparkConf;

public class SparkDriver {

	@SuppressWarnings("resource")
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
		SparkConf conf = new SparkConf().setAppName("Spark Exercise #44");

		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Read the content of the watched movies file
		JavaRDD<String> watchedRDD = sc.textFile(inputPathWatched);

		// First map: movieid - userid

		JavaPairRDD<String, String> movieUserPairRDD = watchedRDD.mapToPair(line->
				 new Tuple2<String,String>(line.split(",")[1],line.split(",")[0]));
		
		JavaRDD<String> moviesRDD = sc.textFile(inputPathMovies);
		// Second map: movieid - genre	
		JavaPairRDD<String, String> movieGenrePairRDD = moviesRDD.mapToPair(line->
			new Tuple2<String,String>(line.split(",")[0],line.split(",")[2]));	

		// Join
		// the result is a JavaPairRDD with movieid as key and a tuple as value.
		// The tuple contains userid and genre
		JavaPairRDD<String, Tuple2<String,String>> joinRDD = movieUserPairRDD.join(movieGenrePairRDD);

		// Select only userid (as key) and genre (as value)
		JavaPairRDD<String, String> usersWatchedGenresRDD = joinRDD
				.mapToPair((Tuple2<String, Tuple2<String, String>> userMovie) -> {
					// movieid - userid - genre
					Tuple2<String, String> movieGenre = new Tuple2<String, String>(userMovie._2()._1(),
							userMovie._2()._2());

					return movieGenre;
				});
		// The result is a JavaPairRDD with userid as key and genre as value
		

		// Read the content of the preferences
		JavaRDD<String> preferencesRDD = sc.textFile(inputPathPreferences);

		// Define a JavaPairRDD with userid as key and genre as value
		JavaPairRDD<String,String> userPrefGenreRDD = preferencesRDD.mapToPair(line ->
				new Tuple2<String,String>(line.split(",")[0],line.split(",")[1]));	 
				
		// Now we use cogroup to join the two RDDs. So that we can compare the genres
		// of the watched movies with the preferred genres of the user
		// The result is a JavaPairRDD with userid as key and a tuple as value.
		JavaPairRDD<String, Tuple2<Iterable<String>, Iterable<String>>> userWatchedLikedGenres = usersWatchedGenresRDD
				.cogroup(userPrefGenreRDD);

		// The result will look like this: (userid, ([genre1,genre2,genre3],[genre1,genre2]))
		// We can now filter the users with a misleading profile

		JavaPairRDD<String, Tuple2<Iterable<String>, Iterable<String>>> misleadingUsersListsRDD = userWatchedLikedGenres.
				filter((Tuple2<String, Tuple2<Iterable<String>, Iterable<String>>> listWatchedLikedGenres) -> {
					ArrayList<String> likedGenres = new ArrayList<String>();
					for (String likedGenre : listWatchedLikedGenres._2()._2()) {
						likedGenres.add(likedGenre);
					}
					ArrayList<String> watchedGenres = new ArrayList<String>();
					for(String watchedGenre : listWatchedLikedGenres._2()._1()) {
						watchedGenres.add(watchedGenre);
					}
					int numWatchedMovies = watchedGenres.size();
					int notLiked = 0;
					for (String watchedGenre : watchedGenres) {
						if (likedGenres.contains(watchedGenre) == false) {
							notLiked++;
						}
					}
					if ((double) notLiked > threshold * (double) numWatchedMovies) {
						return true;
					} else
						return false;
				});
					
				// Select only the userid of the users with a misleading profile
				JavaRDD<String> misleadingUsersRDD = misleadingUsersListsRDD.keys();


				

		// Close the Spark context
		sc.close();
	}
}

