package com.mentory.pricealert.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;

/**
 * Created by anatolytikhonov on 26/09/2017.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class PriceRequest extends Randomized {

    private String user;
    private City src;
    private City dest;

    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd")
    private Date date;

    public PriceRequest() {
    }

    public PriceRequest(int userNumber, int date) {
        this.user = "User-" + userNumber + "@mentory.io";
        this.src = randomEnum(City.class);
        this.dest = randomEnum(City.class);
        this.date = Date.from(Instant.now().plus(date, ChronoUnit.DAYS));
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public City getSrc() {
        return src;
    }

    public void setSrc(City src) {
        this.src = src;
    }

    public City getDest() {
        return dest;
    }

    public void setDest(City dest) {
        this.dest = dest;
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    @Override
    public String toString() {
        return "PriceRequest{" +
                "user='" + user + '\'' +
                ", src=" + src +
                ", dest=" + dest +
                ", date=" + date +
                "} " + super.toString();
    }
}
