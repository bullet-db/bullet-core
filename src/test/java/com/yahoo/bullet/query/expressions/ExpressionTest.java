package com.yahoo.bullet.query.expressions;

import com.yahoo.bullet.common.SerializerDeserializer;
import com.yahoo.bullet.typesystem.Type;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ExpressionTest {
    @Test
    public void testTransientType() {
        ValueExpression expression = new ValueExpression(5);
        Assert.assertEquals(expression.getType(), Type.INTEGER);

        expression = SerializerDeserializer.fromBytes(SerializerDeserializer.toBytes(expression));
        Assert.assertNotNull(expression);
        Assert.assertNull(expression.getType());
    }
}
