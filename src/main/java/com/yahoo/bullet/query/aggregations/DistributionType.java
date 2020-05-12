package com.yahoo.bullet.query.aggregations;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter @AllArgsConstructor
public enum DistributionType {
    QUANTILE("QUANTILE"),
    PMF("FREQ"),
    CDF("CUMFREQ");

    private String name;
}
