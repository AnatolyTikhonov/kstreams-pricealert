package com.mentory.pricealert.model;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by anatolytikhonov on 26/09/2017.
 */
public class PriceAlert {
    private Set<String> users;
    private Set<Flight> flights;

    public PriceAlert() {
    }

    public PriceAlert(HashSet<Flight> flights, HashSet<String> users) {
        this.users = users;
        this.flights = flights;
    }



    public Set<String> getUsers() {
        return users;
    }

    public void setUsers(Set<String> users) {
        this.users = users;
    }

    public Set<Flight> getFlights() {
        return flights;
    }

    public void setFlights(Set<Flight> flights) {
        this.flights = flights;
    }
}
