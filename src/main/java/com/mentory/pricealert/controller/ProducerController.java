package com.mentory.pricealert.controller;

import com.mentory.pricealert.producer.FlightProducer;
import com.mentory.pricealert.producer.PriceRequestProducer;
import com.mentory.pricealert.topolgy.KsTopologyBuilder;
import io.reactivex.Observable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Created by anatolytikhonov on 14/08/2017.
 */
@RestController
public class ProducerController {

    @Autowired
    private FlightProducer flightProducer;

    @Autowired
    private PriceRequestProducer priceRequestProducer;

    @Autowired
    private KsTopologyBuilder ksTopologyBuilder;

    @RequestMapping(value = "/produce", method = RequestMethod.POST)
    public void produce() throws InterruptedException {
        Observable.interval(1, TimeUnit.MILLISECONDS)
                .take(1)
                .subscribe(i -> flightProducer.produce());
        Observable.interval(1, TimeUnit.MILLISECONDS)
                .take(1)
                .subscribe(i -> priceRequestProducer.produce());
    }

    @RequestMapping(value = "/activate", method = RequestMethod.POST)
    public void activate() {
        ksTopologyBuilder.buildAndDeploy();
    }
}
