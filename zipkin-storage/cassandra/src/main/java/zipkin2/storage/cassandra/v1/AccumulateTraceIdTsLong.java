package zipkin2.storage.cassandra.v1;

import com.datastax.driver.core.Row;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import zipkin2.storage.cassandra.internal.call.AccumulateAllResults;

final class AccumulateTraceIdTsLong extends AccumulateAllResults<Set<Pair>> {
  final TimestampCodec timestampCodec;

  AccumulateTraceIdTsLong(TimestampCodec timestampCodec) {
    this.timestampCodec = timestampCodec;
  }

  @Override
  protected Supplier<Set<Pair>> supplier() {
    return LinkedHashSet::new; // because results are not distinct
  }

  @Override
  protected BiConsumer<Row, Set<Pair>> accumulator() {
    return (row, result) ->
        result.add(new Pair(row.getLong("trace_id"), timestampCodec.deserialize(row, "ts")));
  }

  @Override
  public String toString() {
    return "AccumulateTraceIdTsLong{}";
  }
}
