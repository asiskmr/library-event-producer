package com.kafka.yashtech.controller;


import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

import java.util.List;
import java.util.stream.Collectors;

@ControllerAdvice
@Slf4j
public class LibraryEventsControllerAdvice {

    @ExceptionHandler({MethodArgumentNotValidException.class})
    public ResponseEntity<?> handleRequestBody(MethodArgumentNotValidException ex){

       List<FieldError> fieldErrors = ex.getBindingResult().getFieldErrors();

       fieldErrors.stream()
               .map(fieldError -> fieldError.getField() + " - " + fieldError.getDefaultMessage())
               .sorted()
               .collect(Collectors.joining(", "));

       log.info(" error message ", fieldErrors);
       return new ResponseEntity<>(fieldErrors, HttpStatus.BAD_REQUEST);
    }
}
