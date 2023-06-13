package com.market.efx.model;

import lombok.*;

@Data
@Getter
@Setter
@ToString
@EqualsAndHashCode
public class MarketPrice {

    private String uniqueId;
    private String instrumentName;
    private Double bid;
    private Double ask;
    private String timestamp;
}
