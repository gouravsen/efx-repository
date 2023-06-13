package com.market.efx.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.market.efx.service.EFXService;
import com.market.efx.service.MessageSubscriber;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;


@RestController
@RequestMapping(value = "/efx")
public class EFXController {

    @Autowired
    EFXService efxService;

    @Autowired
    MessageSubscriber messageSubscriber;

    @PostMapping(value= "/publish")
    public void publishEFX() throws IOException {
        this.efxService.sendMessage();
        messageSubscriber.getALlTopics();
    }

    @PostMapping(value= "/get-efx-market-price")
    public ResponseEntity<?> getEFXMarketRates() throws JsonProcessingException {
        messageSubscriber.consumeUpdatedEFX();
        return ResponseEntity.status(HttpStatus.OK).body(null);
    }
}
