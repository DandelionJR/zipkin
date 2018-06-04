package zipkin2.storage.cassandra.v1;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import zipkin2.Call;
import zipkin2.Span;

// TODO(adriancole): at some later point we can refactor this out
final class GroupByTraceId implements Call.Mapper<List<Span>, List<List<Span>>> {
  final boolean strictTraceId;

  GroupByTraceId(boolean strictTraceId) {
    this.strictTraceId = strictTraceId;
  }

  @Override
  public List<List<Span>> map(List<Span> input) {
    if (input.isEmpty()) return Collections.emptyList();

    Map<String, List<Span>> groupedByTraceId = new LinkedHashMap<>();
    for (Span span : input) {
      String traceId =
          strictTraceId || span.traceId().length() == 16
              ? span.traceId()
              : span.traceId().substring(16);
      if (!groupedByTraceId.containsKey(traceId)) {
        groupedByTraceId.put(traceId, new ArrayList<>());
      }
      groupedByTraceId.get(traceId).add(span);
    }
    return new ArrayList<>(groupedByTraceId.values());
  }

  @Override
  public String toString() {
    return "GroupByTraceId{strictTraceId=" + strictTraceId + "}";
  }
}
