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
