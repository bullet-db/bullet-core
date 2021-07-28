/*
 *  Copyright 2021, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying.tablefunctors;

import com.yahoo.bullet.query.tablefunctions.LateralView;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.record.BulletRecordProvider;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A table functor that joins the generated records of the nested table functor with the original input record.
 */
public class LateralViewFunctor extends TableFunctor {
    private static final long serialVersionUID = 1017033253024183470L;

    final List<TableFunctor> tableFunctors;

    /**
     * Constructor that creates a lateral view table functor from a {@link LateralView}.
     *
     * @param lateralView The lateral view table function to construct the table functor from.
     */
    public LateralViewFunctor(LateralView lateralView) {
        tableFunctor = lateralView.getTableFunction().getTableFunctor();
    }

    @Override
    public List<BulletRecord> apply(BulletRecord record, BulletRecordProvider provider) {
        if (tableFunctors.size() == 1) {
            return apply(record, provider, tableFunctors.get(0));
        }
        List<BulletRecord> records = Collections.singletonList(record);
        for (TableFunctor tableFunctor : tableFunctors) {
            records = records.stream().flatMap(r -> apply(r, provider, tableFunctor).stream()).collect(Collectors.toList());
        }
        return records;
        //List<BulletRecord> records = tableFunctor.apply(record, provider);
        //return records.stream().map(generated -> new LateralViewBulletRecord(record, generated)).collect(Collectors.toList());
    }

    private List<BulletRecord> apply(BulletRecord record, BulletRecordProvider provider, TableFunctor tableFunctor) {
        List<BulletRecord> records = tableFunctor.apply(record, provider);
        return records.stream().map(generated -> new LateralViewBulletRecord(record, generated)).collect(Collectors.toList());
    }
}
