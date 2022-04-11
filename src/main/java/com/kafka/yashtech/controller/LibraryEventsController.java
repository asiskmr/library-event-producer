package com.kafka.yashtech.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.kafka.yashtech.domain.LibraryEvent;
import com.kafka.yashtech.domain.LibraryEventType;
import com.kafka.yashtech.producer.LibraryEventProducer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

@RestController
@Slf4j
public class LibraryEventsController {

    @Autowired
    private LibraryEventProducer libraryEventProducer;

    @PostMapping("/v1/libraryevent")
    public ResponseEntity<LibraryEvent> postLibraryEvent(@RequestBody LibraryEvent libraryEvent) throws JsonProcessingException, ExecutionException, InterruptedException, TimeoutException {

        log.info("before send library events");
        libraryEvent.setLibraryEventType(LibraryEventType.NEW);
        //libraryEventProducer.sendEvent(libraryEvent);
       // SendResult<Integer, String> sendResult = libraryEventProducer.sendLibraryEventSync(libraryEvent);

        libraryEventProducer.sendEvent_Approch2(libraryEvent);
       // log.info("SendResult is {} ", sendResult.toString());
        log.info("after send library events");
        return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
    }
}