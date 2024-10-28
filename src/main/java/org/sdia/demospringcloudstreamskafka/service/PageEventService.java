package org.sdia.demospringcloudstreamskafka.service;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.sdia.demospringcloudstreamskafka.entities.PageEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Date;
import java.util.Random;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

@Service
public class PageEventService {
    // on definit un consumer qui va consommer les messages envoyés par le producer
    @Bean
    public Consumer<PageEvent> pageEventConsumer(){
        return (input) ->{
            System.out.println("*************************");
            System.out.println("PageEvent received: "+input.toString());
            System.out.println("*************************");

        };
    }
    // on definit un supplier qui va produire des messages
    @Bean
    public Supplier<PageEvent> pageEventSupplier(){
        return () -> new PageEvent(
                Math.random()>0.5?"P1":"P2",
                Math.random()>0.5?"U1":"U2",
                new Date(),
                new Random().nextInt(9000));
    }
    // cette fonction va definir les deux : le consumer et le supplier
    @Bean
    public Function<PageEvent,PageEvent> pageEventFunction(){
        return (input) ->{
            input.setName("L:"+input.getName().length());
            input.setUser("User");
            return input;
        };
    }
    @Bean
    // String: le nom de la page => la clé et PageEvent la valeur
    // Long : Le nombre de visit
    public Function<KStream<String,PageEvent>,KStream<String,Long>> KStreamFunction(){
        return (input) ->{
            // la duréé de page visitée est supérieure à 100s
            return input
                    // on filtre les pages visitées pendant plus de 100s
                    .filter((k,v) ->v.getDuration()>100)
                    // groupe par le nom de la page
                    .map((k,v) -> new KeyValue<>(v.getName(),0L))
                    .groupBy((k,v) -> k, Grouped.with(Serdes.String(),Serdes.Long()))
                    .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMillis(5000)))
                    // on compte le nombre de visite
                    .count(Materialized.as("page-count"))
                    .toStream()
                    .map((k,v) -> new KeyValue<>("=>" + k.window().startTime()+k.window().endTime()+k.key(),v));
        };
    }
}
