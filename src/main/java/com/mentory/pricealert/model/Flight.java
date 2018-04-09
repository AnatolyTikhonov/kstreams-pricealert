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
public class Flight extends Randomized {
    private Airline airline;
    private City src;
    private City dest;
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd")
    private Date date;
    private Integer price;

    public Flight() {
    }

    public Flight(int date, int price) {
        this.airline = randomEnum(Airline.class);
        this.src = randomEnum(City.class);
        this.dest = randomEnum(City.class);
        this.date = Date.from(Instant.now().plus(date, ChronoUnit.DAYS));
        this.price = price;
    }

    public Airline getAirline() {
        return airline;
    }

    public void setAirline(Airline airline) {
        this.airline = airline;
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

    public Integer getPrice() {
        return price;
    }

    public void setPrice(Integer price) {
        this.price = price;
    }

    @Override
    public String toString() {
        return "Flight{" +
                "airline=" + airline +
                ", src=" + src +
                ", dest=" + dest +
                ", date=" + date +
                ", price=" + price +
                "} " + super.toString();
    }
}
