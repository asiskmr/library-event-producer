package com.kafka.yashtech.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.yashtech.domain.LibraryEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


@Component
@Slf4j
public class LibraryEventProducer {

    String topic = "library-events";
    @Autowired
    private KafkaTemplate<Integer, String> kafkaTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    public LibraryEventProducer(KafkaTemplate<Integer, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    public void sendEvent(LibraryEvent libraryEvent) throws JsonProcessingException {

        Integer key = libraryEvent.getLibraryEventId();
        String value;
        value = objectMapper.writeValueAsString(libraryEvent);
        ListenableFuture<SendResult<Integer, String>> listenableFuture =kafkaTemplate.sendDefault(key, value);

        listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {
            @Override
            public void onFailure(Throwable ex) {
                handleFailure(key, value, ex);
            }


            @Override
            public void onSuccess(SendResult<Integer, String> result) {
                    handleSuccess(key, value, result);
            }
        });
    }

    public void sendEvent_Approch2(LibraryEvent libraryEvent) throws JsonProcessingException {

        Integer key = libraryEvent.getLibraryEventId();
        String value;
        value = objectMapper.writeValueAsString(libraryEvent);

        ProducerRecord<Integer, String> producerRecord = buildProducerRecord(key, value, topic);

        ListenableFuture<SendResult<Integer, String>> listenableFuture =kafkaTemplate.send(producerRecord);

        listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {
            @Override
            public void onFailure(Throwable ex) {
                handleFailure(key, value, ex);
            }


            @Override
            public void onSuccess(SendResult<Integer, String> result) {
                handleSuccess(key, value, result);
            }
        });

    }

    private ProducerRecord<Integer, String> buildProducerRecord(Integer key, String value, String topic) {

        List<Header> recordHeaders = List.of(new RecordHeader("event-source", "scanner".getBytes()),
                new RecordHeader("event-source", "manual".getBytes()));
        return new ProducerRecord<>(topic, null, key, value, recordHeaders);
    }

    public SendResult<Integer, String> sendLibraryEventSync(LibraryEvent libraryEvent) throws JsonProcessingException, ExecutionException, InterruptedException, TimeoutException {

        Integer key = libraryEvent.getLibraryEventId();
        String value = objectMapper.writeValueAsString(libraryEvent);
        SendResult<Integer, String> sendResult = null;
        try {
            sendResult = kafkaTemplate.sendDefault(key, value).get(1, TimeUnit.SECONDS);
        } catch (ExecutionException | InterruptedException e) {
            log.error("ExecutionException|InterruptedException in sending the message and the exception : {} ",e);
            throw e;
        }catch (Exception e){
            log.error("Exception in sending the message and the exception : {} ",e);
            throw e;
        }
        return sendResult;
    }

    private void handleSuccess(Integer key, String value, SendResult<Integer, String> result) {

        log.info(" message send success fully  for the key : {} and the value : {} and partition {} ",key, value, result.getRecordMetadata().partition());
    }

    private void handleFailure(Integer key, String value, Throwable ex) {

        log.error("Error in sending the message and the exception : {} ",ex);

        try{
            throw ex;
        }catch (Throwable throwable){
            log.error(" Error on failure : {} ", throwable.getMessage());
        }
    }
}
