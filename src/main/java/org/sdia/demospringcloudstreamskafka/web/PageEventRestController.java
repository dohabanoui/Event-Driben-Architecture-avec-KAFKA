package org.sdia.demospringcloudstreamskafka.web;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.sdia.demospringcloudstreamskafka.entities.PageEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.binder.kafka.streams.InteractiveQueryService;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

@RestController
public class PageEventRestController {
    @Autowired
    private StreamBridge streamBridge;
    @Autowired
    private InteractiveQueryService interactiveQueryService;


    @GetMapping("/publish/{topic}/{name}")
    public PageEvent publish(@PathVariable String topic, @PathVariable String name){
        PageEvent pageEvent = new PageEvent(name, Math.random() > 0.5 ? "user1" : "user2",
                new Date(),
                new Random().nextInt(9000));
        // on specifie vers quel topic on veut envoyer le message
        streamBridge.send(topic, pageEvent);
        return pageEvent;
    }

    // Flux est un stream de données
    @GetMapping(value = "/analytics", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<Map<String,Long>> analytics(){
        return Flux.interval(Duration.ofSeconds(1))
                .map(sequence -> {
                    Map<String,Long> stringLongMap=new HashMap<>();
                    try {
                        ReadOnlyWindowStore<String, Long> windowStore = interactiveQueryService.getQueryableStore("page-count", QueryableStoreTypes.windowStore());
                        Instant now = Instant.now();
                        Instant from = now.minusMillis(5000);
                        KeyValueIterator<Windowed<String>, Long> fetchAll = windowStore.fetchAll(from, now);
                        // windowStoreIterator<Long> fetch = windowStore.fetch("P1", from, now);
                        while (fetchAll.hasNext()) {
                            KeyValue<Windowed<String>, Long> next = fetchAll.next();
                            stringLongMap.put(next.key.key(), next.value);
                        }
                    }catch (Exception e){
                        e.printStackTrace();
                    }
                    return stringLongMap;
                }).share();

    }
}
