/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.mapper.xcontent;

import org.apache.lucene.analysis.NumericTokenStream;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.common.thread.ThreadLocals;
import org.elasticsearch.common.trove.TIntObjectHashMap;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.field.data.FieldDataType;
import org.elasticsearch.index.mapper.MergeMappingException;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;

/**
 * @author kimchy (shay.banon)
 */
public abstract class NumberFieldMapper<T extends Number> extends AbstractFieldMapper<T> implements IncludeInAllMapper {

    public static class Defaults extends AbstractFieldMapper.Defaults {
        public static final int PRECISION_STEP = NumericUtils.PRECISION_STEP_DEFAULT;
        public static final Field.Index INDEX = Field.Index.NOT_ANALYZED;
        public static final boolean OMIT_NORMS = true;
        public static final boolean OMIT_TERM_FREQ_AND_POSITIONS = true;
    }

    public abstract static class Builder<T extends Builder, Y extends NumberFieldMapper> extends AbstractFieldMapper.Builder<T, Y> {

        protected int precisionStep = Defaults.PRECISION_STEP;

        public Builder(String name) {
            super(name);
            this.index = Defaults.INDEX;
            this.omitNorms = Defaults.OMIT_NORMS;
            this.omitTermFreqAndPositions = Defaults.OMIT_TERM_FREQ_AND_POSITIONS;
        }

        @Override public T store(Field.Store store) {
            return super.store(store);
        }

        @Override public T boost(float boost) {
            return super.boost(boost);
        }

        @Override public T indexName(String indexName) {
            return super.indexName(indexName);
        }

        @Override public T includeInAll(Boolean includeInAll) {
            return super.includeInAll(includeInAll);
        }

        public T precisionStep(int precisionStep) {
            this.precisionStep = precisionStep;
            return builder;
        }
    }

    private static final ThreadLocal<ThreadLocals.CleanableValue<TIntObjectHashMap<Deque<CachedNumericTokenStream>>>> cachedStreams = new ThreadLocal<ThreadLocals.CleanableValue<TIntObjectHashMap<Deque<CachedNumericTokenStream>>>>() {
        @Override protected ThreadLocals.CleanableValue<TIntObjectHashMap<Deque<CachedNumericTokenStream>>> initialValue() {
            return new ThreadLocals.CleanableValue<TIntObjectHashMap<Deque<CachedNumericTokenStream>>>(new TIntObjectHashMap<Deque<CachedNumericTokenStream>>());
        }
    };

    protected int precisionStep;

    protected Boolean includeInAll;

    protected NumberFieldMapper(Names names, int precisionStep,
                                Field.Index index, Field.Store store,
                                float boost, boolean omitNorms, boolean omitTermFreqAndPositions,
                                NamedAnalyzer indexAnalyzer, NamedAnalyzer searchAnalyzer) {
        super(names, index, store, Field.TermVector.NO, boost, omitNorms, omitTermFreqAndPositions, indexAnalyzer, searchAnalyzer);
        if (precisionStep <= 0 || precisionStep >= maxPrecisionStep()) {
            this.precisionStep = Integer.MAX_VALUE;
        } else {
            this.precisionStep = precisionStep;
        }
    }

    @Override public void includeInAll(Boolean includeInAll) {
        if (includeInAll != null) {
            this.includeInAll = includeInAll;
        }
    }

    protected abstract int maxPrecisionStep();

    public int precisionStep() {
        return this.precisionStep;
    }

    /**
     * Use the field query created here when matching on numbers.
     */
    @Override public boolean useFieldQueryWithQueryString() {
        return true;
    }

    /**
     * Numeric field level query are basically range queries with same value and included. That's the recommended
     * way to execute it.
     */
    @Override public Query fieldQuery(String value) {
        return rangeQuery(value, value, true, true);
    }

    /**
     * Numeric field level filter are basically range queries with same value and included. That's the recommended
     * way to execute it.
     */
    @Override public Filter fieldFilter(String value) {
        return rangeFilter(value, value, true, true);
    }

    @Override public abstract Query rangeQuery(String lowerTerm, String upperTerm, boolean includeLower, boolean includeUpper);

    @Override public abstract Filter rangeFilter(String lowerTerm, String upperTerm, boolean includeLower, boolean includeUpper);

    /**
     * Override the default behavior (to return the string, and return the actual Number instance).
     */
    @Override public Object valueForSearch(Fieldable field) {
        return value(field);
    }

    @Override public String valueAsString(Fieldable field) {
        Number num = value(field);
        return num == null ? null : num.toString();
    }

    @Override public void merge(XContentMapper mergeWith, MergeContext mergeContext) throws MergeMappingException {
        super.merge(mergeWith, mergeContext);
        if (!this.getClass().equals(mergeWith.getClass())) {
            return;
        }
        if (!mergeContext.mergeFlags().simulate()) {
            this.precisionStep = ((NumberFieldMapper) mergeWith).precisionStep;
            this.includeInAll = ((NumberFieldMapper) mergeWith).includeInAll;
        }
    }

    @Override public abstract FieldDataType fieldDataType();

    /**
     * Removes a cached numeric token stream. The stream will be returned to the cached once it is used
     * since it implements the end method.
     */
    protected CachedNumericTokenStream popCachedStream(int precisionStep) {
        Deque<CachedNumericTokenStream> deque = cachedStreams.get().get().get(precisionStep);
        if (deque == null) {
            deque = new ArrayDeque<CachedNumericTokenStream>();
            cachedStreams.get().get().put(precisionStep, deque);
            deque.add(new CachedNumericTokenStream(new NumericTokenStream(precisionStep), precisionStep));
        }
        if (deque.isEmpty()) {
            deque.add(new CachedNumericTokenStream(new NumericTokenStream(precisionStep), precisionStep));
        }
        return deque.pollFirst();
    }

    /**
     * A wrapper around a numeric stream allowing to reuse it by implementing the end method which returns
     * this stream back to the thread local cache.
     */
    protected static final class CachedNumericTokenStream extends TokenStream {

        private final int precisionStep;

        private final NumericTokenStream numericTokenStream;

        public CachedNumericTokenStream(NumericTokenStream numericTokenStream, int precisionStep) {
            super(numericTokenStream);
            this.numericTokenStream = numericTokenStream;
            this.precisionStep = precisionStep;
        }

        public void end() throws IOException {
            numericTokenStream.end();
        }

        /**
         * Close the input TokenStream.
         */
        public void close() throws IOException {
            numericTokenStream.close();
            TIntObjectHashMap<Deque<CachedNumericTokenStream>> cached = cachedStreams.get().get();
            if (cached != null) {
                Deque<CachedNumericTokenStream> cachedDeque = cached.get(precisionStep);
                if (cachedDeque != null) {
                    cachedDeque.add(this);
                }
            }
        }

        /**
         * Reset the filter as well as the input TokenStream.
         */
        public void reset() throws IOException {
            numericTokenStream.reset();
        }

        @Override public boolean incrementToken() throws IOException {
            return numericTokenStream.incrementToken();
        }

        public CachedNumericTokenStream setIntValue(int value) {
            numericTokenStream.setIntValue(value);
            return this;
        }

        public CachedNumericTokenStream setLongValue(long value) {
            numericTokenStream.setLongValue(value);
            return this;
        }

        public CachedNumericTokenStream setFloatValue(float value) {
            numericTokenStream.setFloatValue(value);
            return this;
        }

        public CachedNumericTokenStream setDoubleValue(double value) {
            numericTokenStream.setDoubleValue(value);
            return this;
        }
    }
}
