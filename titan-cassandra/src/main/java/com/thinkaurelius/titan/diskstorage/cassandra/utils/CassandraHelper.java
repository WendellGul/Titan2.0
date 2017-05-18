package com.thinkaurelius.titan.diskstorage.cassandra.utils;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.TimeUnit;

import com.datastax.driver.core.DataType;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.thinkaurelius.titan.core.Cardinality;
import com.thinkaurelius.titan.core.Multiplicity;
import com.thinkaurelius.titan.core.schema.ConsistencyModifier;
import com.thinkaurelius.titan.core.schema.Mapping;
import com.thinkaurelius.titan.core.schema.SchemaStatus;
import com.thinkaurelius.titan.diskstorage.*;
import com.thinkaurelius.titan.diskstorage.idmanagement.ConflictAvoidanceMode;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyRange;
import com.thinkaurelius.titan.diskstorage.util.BufferUtil;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayBuffer;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayEntry;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayEntryList;
import com.thinkaurelius.titan.diskstorage.util.time.TimestampProviders;
import com.thinkaurelius.titan.graphdb.database.log.LogTxStatus;
import com.thinkaurelius.titan.graphdb.database.management.MgmtLogType;
import com.thinkaurelius.titan.graphdb.internal.ElementCategory;
import com.thinkaurelius.titan.graphdb.internal.Order;
import com.thinkaurelius.titan.graphdb.internal.RelationCategory;
import com.thinkaurelius.titan.graphdb.internal.TitanSchemaCategory;
import com.thinkaurelius.titan.graphdb.types.ParameterType;
import com.thinkaurelius.titan.graphdb.types.TypeDefinitionCategory;
import org.apache.cassandra.dht.BytesToken;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.tinkerpop.gremlin.structure.Direction;

import javax.annotation.Nullable;

public class CassandraHelper {

    private static final Map<Class, DataType> handlers = new HashMap<>();
    public static final Set<Class> ENUM_SETS = new HashSet<>();

    static {
        // Primitive data types
        handlers.put(String.class, DataType.text());
        handlers.put(Integer.class, DataType.varint());
        handlers.put(Boolean.class, DataType.cboolean());
        handlers.put(Long.class, DataType.bigint());
        handlers.put(Short.class, DataType.smallint());
        handlers.put(Date.class, DataType.date());
        handlers.put(Byte.class, DataType.blob());
        handlers.put(Character.class, DataType.ascii());
        handlers.put(Float.class, DataType.cfloat());
        handlers.put(Double.class, DataType.cdouble());
        handlers.put(Class.class, DataType.text());

        //Arrays
        handlers.put(String[].class, DataType.list(DataType.text()));
        handlers.put(long[].class, DataType.list(DataType.bigint()));

        // Needed by Titan
        handlers.put(TypeDefinitionCategory.class, DataType.cint());
        handlers.put(TitanSchemaCategory.class, DataType.cint());
        handlers.put(ParameterType.class, DataType.cint());
        handlers.put(RelationCategory.class, DataType.cint());
        handlers.put(Order.class, DataType.cint());
        handlers.put(Multiplicity.class, DataType.cint());
        handlers.put(Cardinality.class, DataType.cint());
        handlers.put(Direction.class, DataType.cint());
        handlers.put(ElementCategory.class, DataType.cint());
        handlers.put(ConsistencyModifier.class, DataType.cint());
        handlers.put(SchemaStatus.class, DataType.cint());
        handlers.put(LogTxStatus.class, DataType.cint());
        handlers.put(MgmtLogType.class, DataType.cint());
        handlers.put(TimestampProviders.class, DataType.cint());
        handlers.put(TimeUnit.class, DataType.cint());
        handlers.put(Mapping.class, DataType.cint());
        handlers.put(ConflictAvoidanceMode.class, DataType.cint());

        // enum set
        ENUM_SETS.add(TypeDefinitionCategory.class);
        ENUM_SETS.add(TitanSchemaCategory.class);
        ENUM_SETS.add(ParameterType.class);
        ENUM_SETS.add(RelationCategory.class);
        ENUM_SETS.add(Order.class);
        ENUM_SETS.add(Multiplicity.class);
        ENUM_SETS.add(Cardinality.class);
        ENUM_SETS.add(Direction.class);
        ENUM_SETS.add(ElementCategory.class);
        ENUM_SETS.add(ConsistencyModifier.class);
        ENUM_SETS.add(SchemaStatus.class);
        ENUM_SETS.add(LogTxStatus.class);
        ENUM_SETS.add(MgmtLogType.class);
        ENUM_SETS.add(TimestampProviders.class);
        ENUM_SETS.add(TimeUnit.class);
        ENUM_SETS.add(Mapping.class);
        ENUM_SETS.add(ConflictAvoidanceMode.class);

    }

    public static List<ByteBuffer> convert(List<StaticBuffer> keys) {
        List<ByteBuffer> requestKeys = new ArrayList<ByteBuffer>(keys.size());
        for (int i = 0; i < keys.size(); i++) {
            requestKeys.add(keys.get(i).asByteBuffer());
        }
        return requestKeys;
    }

    /**
     * Constructs an {@link EntryList} from the Iterable of entries while excluding the end slice
     * (since the method contract states that the end slice is exclusive, yet Cassandra treats it as
     * inclusive) and respecting the limit.
     *
     * @param entries
     * @param getter
     * @param lastColumn TODO: make this StaticBuffer so we can avoid the conversion and provide equals method
     * @param limit
     * @param <E>
     * @return
     */
    public static<E> EntryList makeEntryList(final Iterable<E> entries,
                                             final StaticArrayEntry.GetColVal<E,ByteBuffer> getter,
                                             final StaticBuffer lastColumn, final int limit) {
        return StaticArrayEntryList.ofByteBuffer(new Iterable<E>() {
            @Override
            public Iterator<E> iterator() {
                return Iterators.filter(entries.iterator(),new FilterResultColumns<E>(lastColumn,limit,getter));
            }
        },getter);
    }

    public static<E> MyEntryList makeMyEntryList(final Iterable<E> entries,
                                                 final MyEntry.GetEntry<E> getter,
                                                 final long lastColumn, final int limit) {
        return MyArrayEntryList.of(new Iterable<E>() {
            @Override
            public Iterator<E> iterator() {
                return Iterators.filter(entries.iterator(), new MyFilterResultColumns<E>(lastColumn, limit, getter));
            }
        }, getter);
    }

    private static class MyFilterResultColumns<E> implements Predicate<E> {

        private int count = 0;

        private final int limit;
        private final long lastColumn;
        private final MyEntry.GetEntry<E> getter;

        private MyFilterResultColumns(long lastColumn, int limit, MyEntry.GetEntry<E> getter) {
            this.limit = limit;
            this.lastColumn = lastColumn;
            this.getter = getter;
        }

        @Override
        public boolean apply(@Nullable E e) {
            assert e!=null;
            if (count >= limit || lastColumn == getter.getColumn(e)) return false;
            count++;
            return true;
        }
    }

    private static class FilterResultColumns<E> implements Predicate<E> {

        private int count = 0;

        private final int limit;
        private final StaticBuffer lastColumn;
        private final StaticArrayEntry.GetColVal<E,ByteBuffer> getter;

        private FilterResultColumns(StaticBuffer lastColumn, int limit, StaticArrayEntry.GetColVal<E, ByteBuffer> getter) {
            this.limit = limit;
            this.lastColumn = lastColumn;
            this.getter = getter;
        }

        @Override
        public boolean apply(@Nullable E e) {
            assert e!=null;
            if (count>=limit || BufferUtil.equals(lastColumn, getter.getColumn(e))) return false;
            count++;
            return true;
        }

    }

    public static<E> Iterator<Entry> makeEntryIterator(final Iterable<E> entries,
                                             final StaticArrayEntry.GetColVal<E,ByteBuffer> getter,
                                             final StaticBuffer lastColumn, final int limit) {
        return Iterators.transform(Iterators.filter(entries.iterator(),
                new FilterResultColumns<E>(lastColumn, limit, getter)), new Function<E, Entry>() {
            @Nullable
            @Override
            public Entry apply(@Nullable E e) {
                return StaticArrayEntry.ofByteBuffer(e,getter);
            }
        });
    }

    public static<E> Iterator<MyEntry> makeEntryIterator(final Iterable<E> entries,
                                                       final MyEntry.GetEntry<E> getter,
                                                       final long lastColumn, final int limit) {
        return Iterators.transform(Iterators.filter(entries.iterator(),
                new MyFilterResultColumns<E>(lastColumn, limit, getter)), new Function<E, MyEntry>() {
            @Nullable
            @Override
            public MyEntry apply(@Nullable E e) {
                return MyEntry.of(e, getter);
            }
        });
    }


    public static KeyRange transformRange(Range<Token> range) {
        return transformRange(range.left, range.right);
    }

    public static KeyRange transformRange(Token leftKeyExclusive, Token rightKeyInclusive) {
        if (!(leftKeyExclusive instanceof BytesToken))
            throw new UnsupportedOperationException();

        // if left part is BytesToken, right part should be too, otherwise there is no sense in the ring
        assert rightKeyInclusive instanceof BytesToken;

        // l is exclusive, r is inclusive
        BytesToken l = (BytesToken) leftKeyExclusive;
        BytesToken r = (BytesToken) rightKeyInclusive;

        byte[] leftTokenValue = l.getTokenValue();
        byte[] rightTokenValue = r.getTokenValue();

        Preconditions.checkArgument(leftTokenValue.length == rightTokenValue.length, "Tokens have unequal length");
        int tokenLength = leftTokenValue.length;

        byte[][] tokens = new byte[][]{leftTokenValue, rightTokenValue};
        byte[][] plusOne = new byte[2][tokenLength];

        for (int j = 0; j < 2; j++) {
            boolean carry = true;
            for (int i = tokenLength - 1; i >= 0; i--) {
                byte b = tokens[j][i];
                if (carry) {
                    b++;
                    carry = false;
                }
                if (b == 0) carry = true;
                plusOne[j][i] = b;
            }
        }

        StaticBuffer lb = StaticArrayBuffer.of(plusOne[0]);
        StaticBuffer rb = StaticArrayBuffer.of(plusOne[1]);
        Preconditions.checkArgument(lb.length() == tokenLength, lb.length());
        Preconditions.checkArgument(rb.length() == tokenLength, rb.length());

        return new KeyRange(lb, rb);
    }

    public static DataType getDataType(Class<?> clazz) {
        if(handlers.containsKey(clazz))
            return handlers.get(clazz);
        else if(handlers.containsKey(clazz.getSuperclass()))
            return handlers.get(clazz.getSuperclass());
        else {
            throw new IllegalStateException("Unsupported data type!");
        }
    }

    public static Object checkValue(Object value) {
        if(value.getClass() == Integer.class
                || value.getClass() == Boolean.class
                || value.getClass() == Long.class
                || value.getClass() == String.class)
            return value;
        else if(value.getClass() == Class.class)
            return ((Class) value).getName();
        else if(value.getClass() == long[].class) {
            List<Long> re = new ArrayList<>(((long[]) value).length);
            for(long l : (long[]) value)
                re.add(l);
            return re;
        }
        else if(value.getClass() == String[].class)
            return Arrays.asList(value);
        else
            return ((Enum) value).ordinal();
    }
}
