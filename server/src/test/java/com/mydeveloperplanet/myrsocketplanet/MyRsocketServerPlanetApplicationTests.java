package com.mydeveloperplanet.myrsocketplanet;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.messaging.rsocket.RSocketRequester;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
class MyRsocketServerPlanetApplicationTests {

    private static final String CLIENT = "Client";
    private static final String SERVER = "Server";

    private static RSocketRequester rSocketRequester;

    @BeforeAll
    public static void setupOnce(@Autowired RSocketRequester.Builder builder, @Value("${spring.rsocket.server.port}") Integer port) {
        rSocketRequester = builder.tcp("localhost", port);
    }

    @Test
    void testRequestResponse() {
        // Send a request message
        Mono<Notification> result = rSocketRequester
                .route("my-request-response")
                .data(new Notification(CLIENT, SERVER, "Test the Request-Response interaction model"))
                .retrieveMono(Notification.class);

        // Verify that the response message contains the expected data
        StepVerifier
                .create(result)
                .consumeNextWith(notification -> {
                    assertThat(notification.getSource()).isEqualTo(SERVER);
                    assertThat(notification.getDestination()).isEqualTo(CLIENT);
                    assertThat(notification.getText()).isEqualTo("In response to: Test the Request-Response interaction model");
                })
                .verifyComplete();
    }

    @Test
    void testFireAndForget() {
        // Send a fire-and-forget message
        Mono<Void> result = rSocketRequester
                .route("my-fire-and-forget")
                .data(new Notification(CLIENT, SERVER, "Test the Fire-And-Forget interaction model"))
                .retrieveMono(Void.class);

        // Assert that the result is a completed Mono.
        StepVerifier
                .create(result)
                .verifyComplete();
    }

    @Test
    void testRequestStream() {
        // Send a request message
        Flux<Notification> result = rSocketRequester
                .route("my-request-stream")
                .data(new Notification(CLIENT, SERVER, "Test the Request-Stream interaction model"))
                .retrieveFlux(Notification.class);

        // Verify that the response messages contain the expected data
        StepVerifier
                .create(result)
                .consumeNextWith(notification -> {
                    assertThat(notification.getSource()).isEqualTo(SERVER);
                    assertThat(notification.getDestination()).isEqualTo(CLIENT);
                    assertThat(notification.getText()).isEqualTo("In response to: Test the Request-Stream interaction model");
                })
                .expectNextCount(5)
                .consumeNextWith(notification -> {
                    assertThat(notification.getSource()).isEqualTo(SERVER);
                    assertThat(notification.getDestination()).isEqualTo(CLIENT);
                    assertThat(notification.getText()).isEqualTo("In response to: Test the Request-Stream interaction model");
                })
                .thenCancel()
                .verify();
    }

    @Test
    void testChannel() {
        Mono<Notification> notification0 = Mono.just(new Notification(CLIENT, SERVER, "Test the Channel interaction model"));
        Mono<Notification> notification2 = Mono.just(new Notification(CLIENT, SERVER, "Test the Channel interaction model")).delayElement(Duration.ofSeconds(2));
        Mono<Notification> notification5 = Mono.just(new Notification(CLIENT, SERVER, "Test the Channel interaction model")).delayElement(Duration.ofSeconds(5));

        Flux<Notification> notifications = Flux.concat(notification0, notification5, notification0, notification2, notification2, notification2);
        // Send a request message
        Flux<Long> result = rSocketRequester
                .route("my-channel")
                .data(notifications)
                .retrieveFlux(Long.class);

        // Verify that the response messages contain the expected data
        StepVerifier
                .create(result)
                .consumeNextWith(count -> {
                    assertThat(count).isEqualTo(1);
                })
                .consumeNextWith(count -> {
                    assertThat(count).isEqualTo(1);
                })
                .consumeNextWith(count -> {
                    assertThat(count).isEqualTo(1);
                })
                .consumeNextWith(count -> {
                    assertThat(count).isEqualTo(1);
                })
                .consumeNextWith(count -> {
                    assertThat(count).isEqualTo(3);
                })
                .consumeNextWith(count -> {
                    assertThat(count).isEqualTo(3);
                })
                .consumeNextWith(count -> {
                    assertThat(count).isEqualTo(4);
                })
                .consumeNextWith(count -> {
                    assertThat(count).isEqualTo(4);
                })
                .consumeNextWith(count -> {
                    assertThat(count).isEqualTo(5);
                })
                .consumeNextWith(count -> {
                    assertThat(count).isEqualTo(5);
                })
                .consumeNextWith(count -> {
                    assertThat(count).isEqualTo(6);
                })
                .thenCancel()
                .verify();
    }

}
