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
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;


import javax.validation.Valid;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

@RestController
@Slf4j
public class LibraryEventsController {

    @Autowired
    private LibraryEventProducer libraryEventProducer;

    @PostMapping("/v1/libraryevent")
    public ResponseEntity<LibraryEvent> postLibraryEvent( @Valid @RequestBody LibraryEvent libraryEvent) throws JsonProcessingException, ExecutionException, InterruptedException, TimeoutException {

        log.info("before send library events");
        libraryEvent.setLibraryEventType(LibraryEventType.NEW);
        //libraryEventProducer.sendEvent(libraryEvent);
        //SendResult<Integer, String> sendResult = libraryEventProducer.sendLibraryEventSync(libraryEvent);

        libraryEventProducer.sendEvent_Approch2(libraryEvent);
       // log.info("SendResult is {} ", sendResult.toString());
        log.info("after send library events");
        return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
    }

    @PutMapping("/v1/libraryevent")
    public ResponseEntity<?> putLibraryEvent( @Valid @RequestBody LibraryEvent libraryEvent) throws JsonProcessingException, ExecutionException, InterruptedException, TimeoutException {

        log.info("before send library events");

        if(libraryEvent.getLibraryEventId() == null){
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Please pass the library id");
        }
        libraryEvent.setLibraryEventType(LibraryEventType.UPDATE);

        libraryEventProducer.sendEvent_Approch2(libraryEvent);
        // log.info("SendResult is {} ", sendResult.toString());
        log.info("after send library events");
        return ResponseEntity.status(HttpStatus.OK).body(libraryEvent);
    }
}
