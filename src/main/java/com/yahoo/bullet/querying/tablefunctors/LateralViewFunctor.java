/*
 *  Copyright 2021, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying.tablefunctors;

import com.yahoo.bullet.query.tablefunctions.LateralView;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.record.BulletRecordProvider;
import com.yahoo.bullet.record.LateralBulletRecord;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class LateralViewFunctor extends TableFunctor {
    private static final long serialVersionUID = 1017033253024183470L;

    final TableFunctor tableFunctor;

    public LateralViewFunctor(LateralView lateralView) {
        super(lateralView.isOuter());
        tableFunctor = lateralView.getTableFunction().getTableFunctor();
    }

    @Override
    public List<BulletRecord> apply(BulletRecord record, BulletRecordProvider provider) {
        List<BulletRecord> records = tableFunctor.apply(record, provider);
        if (records.isEmpty()) {
            return outer ? Collections.singletonList(record) : Collections.emptyList();
        }
        return records.stream().map(generated -> new LateralBulletRecord(record, generated)).collect(Collectors.toList());
    }
}
