package com.kafka.yashtech.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.yashtech.domain.Book;
import com.kafka.yashtech.domain.LibraryEvent;
import kafka.cluster.Partition;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.SettableListenableFuture;

import org.springframework.kafka.support.SendResult;
import java.util.concurrent.ExecutionException;

import static org.mockito.ArgumentMatchers.isA;


@ExtendWith(MockitoExtension.class)
@EmbeddedKafka(topics = {"library-events"}, partitions = 3)
public class LibraryEventProducerUnitTest {

    @Mock
    private KafkaTemplate<Integer, String> kafkaTemplate;

    @Spy
    ObjectMapper mapper = new ObjectMapper();
    @InjectMocks
    LibraryEventProducer eventProducer;

    @Test
    void sendEvent_Approch2_failure(){

        Book book = Book.builder()
                .bookId(123)
                .bookAuthor("Ashish Kumar")
                .bookName("Kafka Test")
                .build();

        LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(null)
                .book(book)
                .build();

        SettableListenableFuture future = new SettableListenableFuture();
        future.setException(new RuntimeException("Exception calling kafka"));
        Mockito.when(kafkaTemplate.send(isA(ProducerRecord.class))).thenReturn(future);

        Assertions.assertThrows(Exception.class, () -> eventProducer.sendEvent_Approch2(libraryEvent).get());
    }

    @Test
    void sendEvent_Approch2_success() throws JsonProcessingException, ExecutionException, InterruptedException {

        Book book = Book.builder()
                .bookId(123)
                .bookAuthor("Ashish Kumar")
                .bookName("Kafka Test")
                .build();

        LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(null)
                .book(book)
                .build();

        String value = mapper.writeValueAsString(libraryEvent);

        SettableListenableFuture future = new SettableListenableFuture();

        ProducerRecord<Integer, String> producerRecord = new ProducerRecord<>("library-events", libraryEvent.getLibraryEventId(), value);
        RecordMetadata recordMetadata = new RecordMetadata(new TopicPartition("library-events", 1), 1, 1, 242, 1 , 2);

        SendResult<Integer, String> sendResult = new SendResult<>(producerRecord, recordMetadata);
        future.set(sendResult);
        Mockito.when(kafkaTemplate.send(isA(ProducerRecord.class))).thenReturn(future);

        ListenableFuture<SendResult<Integer, String>> listenableFuture =  eventProducer.sendEvent_Approch2(libraryEvent);

        SendResult<Integer, String> sendResult1 =  listenableFuture.get();
        assert sendResult1.getRecordMetadata().partition() == 1;
    }

}
