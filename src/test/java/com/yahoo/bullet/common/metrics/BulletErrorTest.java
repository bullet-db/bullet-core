package com.yahoo.bullet.common.metrics;

import com.yahoo.bullet.common.BulletError;
import org.testng.Assert;
import org.testng.annotations.Test;

public class BulletErrorTest {
    @Test
    public void testToString() {
        BulletError error = BulletError.makeError("foo", "bar");
        Assert.assertEquals(error.toString(), "{error: foo, resolutions: [bar]}");
    }
}
