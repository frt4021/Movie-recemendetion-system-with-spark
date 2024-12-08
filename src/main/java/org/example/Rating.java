package org.example;

public class Rating {
    private int userId;
    private int movieId;
    private float rating;

    public Rating(int userId, int movieId, float rating) {
        this.userId = userId;
        this.movieId = movieId;
        this.rating = rating;
    }

    public int getUserId() {
        return userId;
    }

    public int getMovieId() {
        return movieId;
    }

    public float getRating() {
        return rating;
    }

    @Override
    public String toString() {
        return "Rating [userId=" + userId + ", movieId=" + movieId + ", rating=" + rating + "]";
    }


}
