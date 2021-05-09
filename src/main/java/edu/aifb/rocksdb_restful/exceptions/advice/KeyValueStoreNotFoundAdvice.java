package edu.aifb.rocksdb_restful.exceptions.advice;

import edu.aifb.rocksdb_restful.exceptions.KeyValueStoreNotFoundException;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;

@ControllerAdvice
public class KeyValueStoreNotFoundAdvice {

    @ResponseBody
    @ExceptionHandler(KeyValueStoreNotFoundException.class)
    @ResponseStatus(HttpStatus.NOT_FOUND)
    String keyValueStoreNotFoundExceptionHandler(KeyValueStoreNotFoundException e){
        return e.getMessage();
    }

}
