package com.kafka.yashtech.controller;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.yashtech.domain.Book;
import com.kafka.yashtech.domain.LibraryEvent;
import com.kafka.yashtech.producer.LibraryEventProducer;

import org.hamcrest.core.Is;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;

import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doNothing;


@WebMvcTest(LibraryEventsController.class)
@AutoConfigureMockMvc
public class LibraryEventsControllerUnitTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private LibraryEventProducer libraryEventProducer;

    private ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void postLibraryEvent() throws Exception {

        //Given

        Book book = Book.builder()
                .bookId(123)
                .bookAuthor("Ashish Kumar")
                .bookName("Kafka Test")
                .build();

        LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(null)
                .book(book)
                .build();

        String json = objectMapper.writeValueAsString(libraryEvent);
        doNothing().when(libraryEventProducer).sendEvent_Approch2(isA(LibraryEvent.class));
        when(libraryEventProducer.sendEvent_Approch2(isA(LibraryEvent.class))).thenReturn(null);
        //When
        mockMvc.perform(MockMvcRequestBuilders.post("/v1/libraryevent")
                .content(json)
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isCreated());
    }


    @Test
    void postLibraryEvent_4xx() throws Exception {

        //Given

        Book book = Book.builder()
                .bookId(123)
                .bookAuthor("Ashish Kumar")
                .bookName("Kafka Test")
                .build();

        LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(null)
                //.book(null)
                .build();

        String json = objectMapper.writeValueAsString(libraryEvent);
       // doNothing().when(libraryEventProducer).sendEvent_Approch2(isA(LibraryEvent.class));
        when(libraryEventProducer.sendEvent_Approch2(isA(LibraryEvent.class))).thenReturn(null);
        //When

        String expectedErrorValue = "";
        mockMvc.perform(MockMvcRequestBuilders.post("/v1/libraryevent")
                        .content(json)
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().is4xxClientError())
                .andExpect(content().string(expectedErrorValue));
    }

}
