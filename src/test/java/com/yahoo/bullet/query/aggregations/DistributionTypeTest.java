/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.query.aggregations;

import org.testng.Assert;
import org.testng.annotations.Test;

public class DistributionTypeTest {
    @Test
    public void testGetName() {
        Assert.assertEquals(DistributionType.QUANTILE.getName(), "QUANTILE");
        Assert.assertEquals(DistributionType.PMF.getName(), "FREQ");
        Assert.assertEquals(DistributionType.CDF.getName(), "CUMFREQ");
    }
}
