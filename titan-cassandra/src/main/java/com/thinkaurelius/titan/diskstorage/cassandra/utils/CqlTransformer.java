package com.thinkaurelius.titan.diskstorage.cassandra.utils;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.EdgeSliceQuery;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * Created by Great on 2017/4/11.
 */
public class CqlTransformer {

    private static final Map<String, String> cqlMap = new HashMap<>();

    private static final String CREATE_CF_SYSTEM_PROP = "CREATE TABLE system_properties (" +
            "key text," +
            "PRIMARY KEY(key)" +
            ");";


    static {
        cqlMap.put("system_properties", CREATE_CF_SYSTEM_PROP);
    }


    public static Statement getKeysQuery(String columnFamilyName, ByteBuffer start, ByteBuffer end, int limit) {
        return QueryBuilder.select().from(columnFamilyName)
                .where(QueryBuilder.gte("column", start))
                .and(QueryBuilder.lt("column", end))
                .limit(limit)
                .allowFiltering();
    }

    public static Statement getKeysQuery(String columnFamilyName, int limit) {
        return QueryBuilder.select().from(columnFamilyName).limit(limit);
    }

    public static Statement getKeysQuery(String columnFamilyName,
                                         EdgeSliceQuery.Slice start,
                                         EdgeSliceQuery.Slice end, int limit) {
        return QueryBuilder.select().from(columnFamilyName)
                .where(QueryBuilder.gte("column", start.getId()))
                .and(QueryBuilder.lt("column", end.getId()))
                .limit(limit)
                .allowFiltering();
    }

    public static List<Statement> getKeysQuery(String columnFamilyName,
                                               StaticBuffer keyStart, StaticBuffer keyEnd,
                                               ByteBuffer columnStart, ByteBuffer columnEnd,
                                               int limit, ConsistencyLevel cLevel) {
        StaticBuffer[] keys = new StaticBuffer[]{keyStart, keyEnd};
        return getNamesSliceQuery(columnFamilyName, Arrays.asList(keys), columnStart, columnEnd, limit, cLevel);
    }

    public static List<Statement> getKeysQuery(String columnFamilyName,
                                                   long keyStart, long keyEnd,
                                                   EdgeSliceQuery.Slice sliceStart,
                                                   EdgeSliceQuery.Slice sliceEnd,
                                                   int limit, ConsistencyLevel cLevel) {
        Long[] keys = new Long[]{keyStart, keyEnd};
        return getNamesSliceQuery(columnFamilyName, Arrays.asList(keys), sliceStart, sliceEnd, limit, cLevel);
    }

    public static List<Statement> getNamesSliceQuery(String columnFamilyName,
                                                     List<StaticBuffer> keys,
                                                     ByteBuffer columnStart, ByteBuffer columnEnd,
                                                     int limit, ConsistencyLevel cLevel) {
        List<Statement> statements = new ArrayList<>();
        for(StaticBuffer key : keys) {
            Statement statement = QueryBuilder.select().from(columnFamilyName)
                    .where(QueryBuilder.eq("key", key.asByteBuffer()))
                    .and(QueryBuilder.gte("column", columnStart))
                    .and(QueryBuilder.lt("column", columnEnd))
                    .limit(limit)
                    .allowFiltering()
                    .setConsistencyLevel(cLevel);
            statements.add(statement);
        }
        return statements;
    }

    public static List<Statement> getNamesSliceQuery(String columnFamilyName,
                                                     List<Long> keys,
                                                     EdgeSliceQuery.Slice sliceStart,
                                                     EdgeSliceQuery.Slice sliceEnd,
                                                     int limit, ConsistencyLevel cLevel) {
        List<Statement> statements = new ArrayList<>();
        for(Long key : keys) {
            List<Statement> tmp = new ArrayList<>();
            if(sliceStart.getId() == -1) {
                tmp.add(QueryBuilder.select().from(columnFamilyName)
                        .where(QueryBuilder.eq("key", key))
                        .and(QueryBuilder.gte("direction", sliceStart.getDir()))
                        .and(QueryBuilder.lt("direction", sliceEnd.getDir()))
                        .limit(limit)
                        .allowFiltering()
                        .setConsistencyLevel(cLevel));
            }
            else if(sliceStart.getDir() == -1) {
                tmp.add(QueryBuilder.select().from(columnFamilyName)
                        .where(QueryBuilder.eq("key", key))
                        .and(QueryBuilder.gte("column", sliceStart.getId()))
                        .and(QueryBuilder.lt("column", sliceEnd.getId()))
                        .limit(limit)
                        .allowFiltering()
                        .setConsistencyLevel(cLevel));
            }
            else {
                int dir = sliceStart.getDir();
                while(dir < sliceEnd.getDir()) {
                    tmp.add(QueryBuilder.select().from(columnFamilyName)
                            .where(QueryBuilder.eq("key", key))
                            .and(QueryBuilder.gte("column", sliceStart.getId()))
                            .and(QueryBuilder.lt("column", sliceEnd.getId()))
                            .and(QueryBuilder.eq("direction", dir))
                            .limit(limit)
                            .allowFiltering()
                            .setConsistencyLevel(cLevel));
                    if(dir == 0) dir = 2;
                    else if(dir == 2) dir = 3;
                    else dir++;

                }
            }
            statements.addAll(tmp);
        }
        return statements;
    }

    public static String createCF(String name) {
        return cqlMap.get(name);
    }

    public static String insertSystemProperty(String columnFamilyName,
                                              String rowKey,
                                              String columnKey,
                                              Object value) {
        StringBuffer sb = new StringBuffer();
        sb.append("INSERT INTO ")
                .append(columnFamilyName)
                .append("(key,").append(columnKey).append(") ")
                .append("VALUES(").append(rowKey).append(",")
                .append(value).append(");");
        return sb.toString();
    }
}
