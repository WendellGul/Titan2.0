package com.thinkaurelius.titan.graphdb.database;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.EdgeSliceQuery;
import com.thinkaurelius.titan.graphdb.database.idhandling.IDHandler;
import com.thinkaurelius.titan.graphdb.internal.InternalRelationType;
import com.thinkaurelius.titan.graphdb.internal.RelationCategory;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.EnumMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

/**
 * Created by Great on 2017/5/15.
 */
public class EdgeRelationQueryCache implements AutoCloseable {

    private final Cache<Long, CacheEntry> cache;
    private final EdgeSerializer edgeSerializer;

    private final EnumMap<RelationCategory, EdgeSliceQuery> relationTypes;

    public EdgeRelationQueryCache(EdgeSerializer edgeSerializer) {
        this(edgeSerializer, 256);
    }

    public EdgeRelationQueryCache(EdgeSerializer edgeSerializer, int capacity) {
        this.edgeSerializer = edgeSerializer;
        this.cache = CacheBuilder.newBuilder().maximumSize(capacity*3/2).initialCapacity(capacity)
                .concurrencyLevel(2).build();
        relationTypes = new EnumMap<RelationCategory, EdgeSliceQuery>(RelationCategory.class);
        for (RelationCategory rt : RelationCategory.values()) {
            relationTypes.put(rt,edgeSerializer.getEdgeQuery(rt,false));
        }
    }

    public EdgeSliceQuery getQuery(RelationCategory type) {
        return relationTypes.get(type);
    }

    public EdgeSliceQuery getQuery(final InternalRelationType type, Direction dir) {
        EdgeRelationQueryCache.CacheEntry ce;
        try {
            ce = cache.get(type.longId(),new Callable<EdgeRelationQueryCache.CacheEntry>() {
                @Override
                public EdgeRelationQueryCache.CacheEntry call() throws Exception {
                    return new EdgeRelationQueryCache.CacheEntry(edgeSerializer,type);
                }
            });
        } catch (ExecutionException e) {
            throw new AssertionError("Should not happen: " + e.getMessage());
        }
        assert ce!=null;
        return ce.get(dir);
    }

    @Override
    public void close() throws Exception {
        cache.invalidateAll();
        cache.cleanUp();
    }

    private static final class CacheEntry {

        private final EdgeSliceQuery in;
        private final EdgeSliceQuery out;
        private final EdgeSliceQuery both;
        private final EdgeSliceQuery none;

        public CacheEntry(EdgeSerializer edgeSerializer, InternalRelationType t) {
            if (t.isPropertyKey()) {
                out = edgeSerializer.getEdgeQuery(t, Direction.OUT);
                in = out;
                both = out;
            } else {
                out = edgeSerializer.getEdgeQuery(t, Direction.OUT);
                in = edgeSerializer.getEdgeQuery(t, Direction.IN);
                both = edgeSerializer.getEdgeQuery(t, Direction.BOTH);
            }
            none = new EdgeSliceQuery(new EdgeSliceQuery.Slice(t.longId(), -1),
                    new EdgeSliceQuery.Slice(t.longId() + 1, -1));
        }

        public EdgeSliceQuery get(Direction dir) {
            if(dir == null)
                return none;
            switch (dir) {
                case IN: return in;
                case OUT: return out;
                case BOTH: return both;
                default: throw new AssertionError("Unknown direction: " + dir);
            }
        }
    }
}
