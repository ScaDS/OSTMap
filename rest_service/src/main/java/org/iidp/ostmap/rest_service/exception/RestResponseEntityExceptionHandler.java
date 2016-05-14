package org.iidp.ostmap.rest_service.exception;


import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

@ControllerAdvice
public class RestResponseEntityExceptionHandler extends ResponseEntityExceptionHandler{

    @ExceptionHandler(value = {RuntimeException.class})
    protected ResponseEntity<Object> handleInternalServerError(RuntimeException ex, WebRequest request) {
        String bodyOfResponse = "There is a server problem. Details: " + ex.getMessage();
        return handleExceptionInternal(ex, bodyOfResponse,
                new HttpHeaders(), HttpStatus.INTERNAL_SERVER_ERROR, request);
    }

    @ExceptionHandler(value = {IllegalArgumentException.class})
    protected ResponseEntity<Object> handleIllegalArgument(IllegalArgumentException ex, WebRequest request) {
        String bodyOfResponse = "The given parameters are not valid. Please check api documentation for further information. Details: " + ex.getMessage();
        return handleExceptionInternal(ex, bodyOfResponse,
                new HttpHeaders(), HttpStatus.BAD_REQUEST, request);
    }
}
