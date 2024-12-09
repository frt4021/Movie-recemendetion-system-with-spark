package org.example;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.logging.Level;
import java.util.logging.Logger;

public class Main {

    public static void main(String[] args) {

        SparkSession sparkSession = SparkSession.builder()
                .appName("MovieRecommendation")
                .master("local[*]")
                .getOrCreate();

        JavaSparkContext sc = new JavaSparkContext(sparkSession.sparkContext());

        // MovieRecommendation nesnesini oluşturuyoruz
        MovieRecommendation movieRecommendation = new MovieRecommendation(sc);

        // CSV dosyalarını okuyarak Movies ve Ratings veri kümesini elde ediyoruz
        JavaRDD<Movie> movies = movieRecommendation.loadMovies("movies.csv");
        JavaRDD<Rating> ratings = movieRecommendation.loadRatings("ratings.csv");
        JavaRDD<Tag> tags = movieRecommendation.loadTags("tags.csv");

        // Film önerilerini başlatıyoruz
        movieRecommendation.recommendMovies(movies, ratings);
        movieRecommendation.recommendMoviesByTag(tags,movies);


        // SparkContext'i kapatıyoruz
        sc.close();
    }
}