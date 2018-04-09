package com.mentory.pricealert.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mentory.pricealert.model.City;
import com.mentory.pricealert.model.Flight;
import io.reactivex.Observable;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Created by anatolytikhonov on 27/09/2017.
 */
@Service
public class FlightProducer extends BaseProducer{

    @Value("${bootstrap.servers}")
    private String bootstrapServers;

    @Autowired
    private ObjectMapper om;

    public void produce() throws InterruptedException {
        init(bootstrapServers);
        CountDownLatch latch = new CountDownLatch(1);
        Observable.interval(3000, 100, TimeUnit.MILLISECONDS)
                .flatMap(i -> generateOperations())
                .map(flight -> new ProducerRecord("flights", flight.getAirline().name(), om.writeValueAsString(flight)))
                .subscribe(
                        producer::send,
                        System.out::println,
                        () -> latch.countDown());
        latch.await();
        producer.close();
    }

    private Observable<Flight> generateOperations() {
        int price = new Random().nextInt(100) + 100;
        int date = new Random().nextInt(10);
        Flight flight = new Flight(date, price);
        return Observable.just(flight);
    }
}
