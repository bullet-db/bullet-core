package com.yahoo.bullet.query.aggregations;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.querying.aggregations.RawStrategy;
import com.yahoo.bullet.querying.aggregations.Strategy;

public class Raw extends Aggregation {
    private static final long serialVersionUID = -589592577885076012L;

    /**
     * Constructor that creates a RAW aggregation with a specified max size.
     *
     * @param size The max size of the RAW aggregation. Can be null.
     */
    public Raw(Integer size) {
        super(size, AggregationType.RAW);
    }

    @Override
    public Strategy getStrategy(BulletConfig config) {
        return new RawStrategy(this, config);
    }
}
