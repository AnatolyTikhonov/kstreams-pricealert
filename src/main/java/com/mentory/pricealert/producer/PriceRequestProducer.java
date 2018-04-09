package com.mentory.pricealert.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mentory.pricealert.model.City;
import com.mentory.pricealert.model.PriceRequest;
import io.reactivex.Observable;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Created by anatolytikhonov on 14/08/2017.
 */
@Service
public class PriceRequestProducer extends BaseProducer{

    private Logger logger = LoggerFactory.getLogger(getClass());

    @Value("${bootstrap.servers}")
    private String bootstrapServers;

    @Autowired
    private ObjectMapper om;

    public void produce() throws InterruptedException {
        init(bootstrapServers);
        CountDownLatch latch = new CountDownLatch(1);
        Observable.interval(3000, 1000, TimeUnit.MILLISECONDS)
                .flatMap(i -> generateOperations())
                .map(request -> new ProducerRecord("requests", request.getUser(), om.writeValueAsString(request)))
                .subscribe(
                        producer::send,
                        System.out::println,
                        () -> latch.countDown());
        latch.await();
        producer.close();
    }

    private Observable<PriceRequest> generateOperations() {
        int userNumber = new Random().nextInt(10);
        int date = new Random().nextInt(10) + 1;
        PriceRequest priceRequest = new PriceRequest(userNumber, date);
        return Observable.just(priceRequest);
    }
}
