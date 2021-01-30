package com.mydeveloperplanet.myrsocketplanet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.messaging.rsocket.RSocketRequester;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;

@RestController
public class RsocketClientController {

    private static final String CLIENT = "Client";
    private static final String SERVER = "Server";

    private final RSocketRequester rSocketRequester;

    Logger logger = LoggerFactory.getLogger(RsocketClientController.class);

    public RsocketClientController(@Autowired RSocketRequester.Builder builder) {
        this.rSocketRequester = builder.tcp("localhost", 7000);
    }

    @GetMapping("/request-response")
    public Mono<Notification> requestResponse() {
        Notification notification = new Notification(CLIENT, SERVER, "Test the Request-Response interaction model");
        logger.info("Send notification for my-request-response: " + notification);
        return rSocketRequester
                .route("my-request-response")
                .data(notification)
                .retrieveMono(Notification.class);
    }

    @GetMapping("/fire-and-forget")
    public Mono<Void> fireAndForget() {
        Notification notification = new Notification(CLIENT, SERVER, "Test the Fire-And-Forget interaction model");
        logger.info("Send notification for my-fire-and-forget: " + notification);
        return rSocketRequester
                .route("my-fire-and-forget")
                .data(notification)
                .retrieveMono(Void.class);
    }

    @GetMapping("/request-stream")
    public ResponseEntity<Flux<Notification>> requestStream() {
        Notification notification = new Notification(CLIENT, SERVER, "Test the Request-Stream interaction model");
        logger.info("Send notification for my-request-stream: " + notification);
        Flux<Notification> notificationFlux = rSocketRequester
                .route("my-request-stream")
                .data(notification)
                .retrieveFlux(Notification.class);
        return ResponseEntity.ok().contentType(MediaType.TEXT_EVENT_STREAM).body(notificationFlux);
    }

    @GetMapping("/channel")
    public ResponseEntity<Flux<Long>> channel() {
        Mono<Notification> notification0 = Mono.just(new Notification(CLIENT, SERVER, "Test the Channel interaction model"));
        Mono<Notification> notification2 = Mono.just(new Notification(CLIENT, SERVER, "Test the Channel interaction model")).delayElement(Duration.ofSeconds(2));
        Mono<Notification> notification5 = Mono.just(new Notification(CLIENT, SERVER, "Test the Channel interaction model")).delayElement(Duration.ofSeconds(5));

        Flux<Notification> notifications = Flux.concat(notification0, notification5, notification0, notification2, notification2, notification2)
                .doOnNext(d -> logger.info("Send notification for my-channel"));

        Flux<Long> numberOfNotifications = this.rSocketRequester
                .route("my-channel")
                .data(notifications)
                .retrieveFlux(Long.class);

        return ResponseEntity.ok().contentType(MediaType.TEXT_EVENT_STREAM).body(numberOfNotifications);
    }

}
