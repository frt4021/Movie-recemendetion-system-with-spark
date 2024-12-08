package org.example;

public class Tag {
    private int userId;
    private int movieId;
    private String tag;

    // Constructor
    public Tag(int userId, int movieId, String tag) {
        this.userId = userId;
        this.movieId = movieId;
        this.tag = tag;
    }

    // Getter ve Setter metotları
    public int getUserId() {
        return userId;
    }

    public void setUserId(int userId) {
        this.userId = userId;
    }

    public int getMovieId() {
        return movieId;
    }

    public void setMovieId(int movieId) {
        this.movieId = movieId;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    // toString metodu
    @Override
    public String toString() {
        return "Tag{userId=" + userId + ", movieId=" + movieId + ", tag='" + tag + "'}";
    }
}
