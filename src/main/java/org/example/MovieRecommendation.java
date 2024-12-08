package org.example;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.Serializable;
import java.util.List;

public class MovieRecommendation implements Serializable {

    private transient JavaSparkContext sc;

    public MovieRecommendation(JavaSparkContext sc) {
        this.sc = sc;
    }

    public JavaRDD<Movie> loadMovies(String filePath) {
        JavaRDD<String> lines = sc.textFile(filePath);
        return lines.map(line -> {
            String[] columns = line.split(",");
            int movieId = Integer.parseInt(columns[0]);
            String title = columns[1];
            String genres = columns[2];
            return new Movie(movieId, title, genres);
        });
    }

    public JavaRDD<Rating> loadRatings(String filePath) {
        JavaRDD<String> lines = sc.textFile(filePath);
        return lines.map(line -> {
            String[] columns = line.split(",");
            int userId = Integer.parseInt(columns[0]);
            int movieId = Integer.parseInt(columns[1]);
            float rating = Float.parseFloat(columns[2]);
            return new Rating(userId, movieId, rating);
        });
    }

    public JavaRDD<Tag> loadTags(String filePath) {
        JavaRDD<String> lines = sc.textFile(filePath);
        return lines.map(line -> {
            String[] columns = line.split(",");
            int userId = Integer.parseInt(columns[0]);
            int movieId = Integer.parseInt(columns[1]);
            String tag = columns[2];
            return new Tag(userId, movieId, tag);
        });
    }

    public void recommendMovies(JavaRDD<Movie> movies, JavaRDD<Rating> ratings) {
        JavaPairRDD<Integer, Tuple2<Float, Integer>> movieRatings = ratings.mapToPair(rating ->
                new Tuple2<>(rating.getMovieId(), new Tuple2<>(rating.getRating(), 1))
        );

        JavaPairRDD<Integer, Tuple2<Float, Integer>> aggregatedRatings = movieRatings.reduceByKey((a, b) ->
                new Tuple2<>(a._1 + b._1, a._2 + b._2)
        );

        JavaPairRDD<Integer, Float> averageRatings = aggregatedRatings.mapToPair(entry ->
                new Tuple2<>(entry._1, entry._2._1 / entry._2._2)
        );

        JavaPairRDD<Float, Integer> swappedRatings = averageRatings.mapToPair(entry ->
                new Tuple2<>(entry._2, entry._1)
        );

        JavaPairRDD<Float, Integer> sortedRatings = swappedRatings.sortByKey(false);

        List<Tuple2<Float, Integer>> top10Movies = sortedRatings.take(500);

        JavaPairRDD<Integer, String> movieNames = movies.mapToPair(movie ->
                new Tuple2<>(movie.getMovieId(), movie.getTitle())
        );

        JavaPairRDD<Integer, Tuple2<Float, String>> joinedRDD = averageRatings.join(movieNames);

        for (Tuple2<Float, Integer> result : top10Movies) {
            Integer movieId = result._2;
            Float averageRating = result._1;

            List<Tuple2<Integer, Tuple2<Float, String>>> matchingMovies = joinedRDD.filter(entry -> entry._1.equals(movieId)).collect();

            if (!matchingMovies.isEmpty()) {
                String movieName = matchingMovies.get(0)._2._2;
                System.out.println("Movie: " + movieName + " (ID: " + movieId + ") Average Rating: " + averageRating);
            }
        }
    }

    public void recommendMoviesByTag(JavaRDD<Tag> tags, JavaRDD<Movie> movies) {
        JavaPairRDD<String, Integer> tagMoviePairs = tags.mapToPair(tag ->
                new Tuple2<>(tag.getTag(), tag.getMovieId())
        );

        JavaPairRDD<String, Iterable<Integer>> tagGroupedMovies = tagMoviePairs.groupByKey();

        JavaPairRDD<String, String> tagMovieList = tagGroupedMovies.mapToPair(entry -> {
            StringBuilder movieList = new StringBuilder();
            for (Integer movieId : entry._2) {
                movieList.append(movieId).append(", ");
            }
            return new Tuple2<>(entry._1, movieList.toString());
        });

        List<Tuple2<String, String>> results = tagMovieList.collect();
        //results.forEach(result ->
                //System.out.println("Tag: " + result._1 + " Movies: " + result._2)
        //);
    }
}
