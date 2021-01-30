package com.mydeveloperplanet.myrsocketplanet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.stereotype.Controller;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

@Controller
public class RsocketServerController {

    Logger logger = LoggerFactory.getLogger(RsocketServerController.class);

    @MessageMapping("my-request-response")
    public Notification requestResponse(Notification notification) {
        logger.info("Received notification for my-request-response: " + notification);
        return new Notification(notification.getDestination(), notification.getSource(), "In response to: " + notification.getText());
    }

    @MessageMapping("my-fire-and-forget")
    public void fireAndForget(Notification notification) {
        logger.info("Received notification: " + notification);
    }

    @MessageMapping("my-request-stream")
    Flux<Notification> requestStream(Notification notification) {
        logger.info("Received notification for my-request-stream: " + notification);
        return Flux
                .interval(Duration.ofSeconds(3))
                .map(i -> new Notification(notification.getDestination(), notification.getSource(), "In response to: " + notification.getText()));
    }

    @MessageMapping("my-channel")
    public Flux<Long> channel(Flux<Notification> notifications) {
        final AtomicLong notificationCount = new AtomicLong(0);
        return notifications.doOnNext(notification -> {
            logger.info("Received notification for channel: " + notification);
            notificationCount.incrementAndGet();
        })
                .switchMap(notification -> Flux.interval(Duration.ofSeconds(1)).map(new Object() {
                    private Function<Long, Long> numberOfMessages(AtomicLong notificationCount) {
                        long count = notificationCount.get();
                        logger.info("Return flux with count: " + count);
                        return i -> count;
                    }
                }.numberOfMessages(notificationCount))).log();
    }
}
