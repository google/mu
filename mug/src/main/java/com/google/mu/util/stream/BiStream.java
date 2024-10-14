/*****************************************************************************
 * ------------------------------------------------------------------------- *
 * Licensed under the Apache License, Version 2.0 (the "License");           *
 * you may not use this file except in compliance with the License.          *
 * You may obtain a copy of the License at                                   *
 *                                                                           *
 * http://www.apache.org/licenses/LICENSE-2.0                                *
 *                                                                           *
 * Unless required by applicable law or agreed to in writing, software       *
 * distributed under the License is distributed on an "AS IS" BASIS,         *
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  *
 * See the License for the specific language governing permissions and       *
 * limitations under the License.                                            *
 *****************************************************************************/
package com.google.mu.util.stream;

import static com.google.mu.util.stream.MoreStreams.collectingAndThen;
import static java.util.Map.Entry.comparingByKey;
import static java.util.Map.Entry.comparingByValue;
import static java.util.Objects.requireNonNull;
import static java.util.Spliterator.ORDERED;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toList;
import static java.util.stream.StreamSupport.doubleStream;
import static java.util.stream.StreamSupport.intStream;
import static java.util.stream.StreamSupport.longStream;
import static java.util.stream.StreamSupport.stream;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators.AbstractDoubleSpliterator;
import java.util.Spliterators.AbstractIntSpliterator;
import java.util.Spliterators.AbstractLongSpliterator;
import java.util.Spliterators.AbstractSpliterator;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.DoubleConsumer;
import java.util.function.Function;
import java.util.function.IntConsumer;
import java.util.function.LongConsumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.ToDoubleBiFunction;
import java.util.function.ToIntBiFunction;
import java.util.function.ToLongBiFunction;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.google.mu.util.BiOptional;
import com.google.mu.util.Both;

/**
 * A class similar to {@link Stream}, but operating over a sequence of pairs of objects.
 *
 * <p>Note: For ease of reference, this class uses 'key' and 'value' to refer to each of the two
 * parts of each pair in the sequence. However, both 'key' and 'value' can be any object of the
 * appropriate type, or null. There is no implication that keys or key-value pairs are unique, or
 * that keys can be compared for equality. You may equivalently read them as 'left' and 'right', or
 * 'first' and 'second'.
 *
 * <p>If the contents of the stream aren't identifiably 'keys' or 'values', and the methods with
 * 'key' or 'value' in their name are distracting, consider using the pair-wise operations. For
 * instance, instead of:
 *
 * <pre>{@code
 * BiStream.from(cities.stream(), City::population, City::latitude)
 *     .filterKeys(p -> p > 10000);
 * }</pre>
 *
 * you might use:
 *
 * <pre>{@code
 * BiStream.from(cities.stream(), City::population, City::latitude)
 *     .filter((population, lat) -> population > 10000);
 * }</pre>
 *
 * <p>Keys and values are allowed to be null by default unless explicitly documented otherwise.
 *
 * <p>Some methods (e.g. {@code mapKeys()}) come in two versions, one taking {@link BiFunction}
 * (which will receive both key and value) and another taking {@link Function} (which in this case
 * will receive only the key) . They operate equivalently otherwise.
 */
public abstract class BiStream<K, V> implements AutoCloseable {
  static final boolean NOT_PARALLEL = false;

  /**
   * Builder for {@link BiStream}. Similar to {@link Stream.Builder}, entries may not be added after
   * {@link #build} is called.
   *
   * @since 3.2
   */
  public static final class Builder<K, V> {
    private final Stream.Builder<Map.Entry<K, V>> entries = Stream.builder();

    Builder() {}

    public Builder<K, V> add(K key, V value) {
      entries.add(kv(key, value));
      return this;
    }

    public BiStream<K, V> build() {
      return fromEntries(entries.build());
    }
  }

  /**
   * Returns a new {@link Builder}.
   *
   * @since 3.2
   */
  public static <K, V> Builder<K, V> builder() {
    return new Builder<>();
  }

  /**
   * Returns a {@code Collector} that groups the input elements by {@code classifier} and reduces
   * the values mapping to the same key using {@code reducer}.
   *
   * <pre>{@code
   * ImmutableMap<CurrencyCode, Money> expenseByCurrency = expenses.stream()
   *     .collect(groupingBy(Money::currencyCode, Money::add))
   *     .collect(ImmutableMap::toImmutableMap);
   * }</pre>
   *
   * <p>Entries are collected in encounter order.
   *
   * @since 3.3
   */
  public static <K, V> Collector<V, ?, BiStream<K, V>> groupingBy(
          Function<? super V, ? extends K> classifier, BinaryOperator<V> reducer) {
    return groupingBy(classifier, reducingGroupMembers(reducer));
  }

  /**
   * Returns a {@code Collector} that groups the input elements by {@code classifier} and reduces
   * the values mapping to the same key using {@code mapper} then {@code reducer}.
   *
   * <pre>{@code
   * ImmutableMap<State, Money> householdIncomeByState = households.stream()
   *     .collect(groupingBy(Household::state, Household::income, Money::add))
   *     .collect(ImmutableMap::toImmutableMap);
   * }</pre>
   *
   * <p>Entries are collected in encounter order.
   *
   * @since 3.3
   */
  public static <T, K, V> Collector<T, ?, BiStream<K, V>> groupingBy(
          Function<? super T, ? extends K> classifier,
          Function<? super T, ? extends V> mapper,
          BinaryOperator<V> reducer) {
    return groupingBy(classifier, Collectors.mapping(mapper, reducingGroupMembers(reducer)));
  }

  /**
   * Returns a {@code Collector} that groups the input elements by {@code classifier} and collects
   * the values mapping to the same key into a {@link List}. Similar but different from
   * {@link Collectors#groupingBy(Function)}, this method collects the groups into {@link #BiStream}
   * instead, allowing fluent method chaining. For example:
   *
   * <pre>{@code
   * Map<EmployeeId, List<Task>> employeesWithMultipleTasks = tasks.stream()
   *     .collect(BiStream.groupingBy(Task::assignedTo))
   *     .filterValues(tasks -> tasks.size() > 1)
   *     .toMap();
   * }</pre>
   *
   * Even if you don't need to chain more methods, using this collector allows you to fluently
   * collect the results into the desired container type. For example {@link #toMap} collects to an
   * immutable {@code Map}; or {@code collect(Collectors::toConcurrentMap)} if concurrency is needed.
   *
   * <p>Entries are collected in encounter order.
   *
   * @since 3.0
   */
  public static <T, K> Collector<T, ?, BiStream<K, List<T>>> groupingBy(
          Function<? super T, ? extends K> classifier) {
    return groupingBy(classifier, toList());
  }

  /**
   * Returns a {@code Collector} that groups the input elements by {@code classifier} and collects
   * the values mapping to the same key using {@code valueCollector}. Similar but different from
   * {@link Collectors#groupingBy(Function, Collector)}, this method collects the groups into {@link
   * #BiStream} instead, allowing fluent method chaining. For example:
   *
   * <pre>{@code
   * Map<EmployeeId, Integer> topTenEmployeesByWorkHour = projects.stream()
   *     .flatMap(project -> project.getMembers().stream())  // Stream<TeamMember>
   *     .collect(BiStream.groupingBy(TeamMember::employeeId, summingInt(TeamMember::hours)))
   *     .sortedByValues(Comparator.reverseOrder())
   *     .limit(10)
   *     .toMap();
   * }</pre>
   *
   * Even if you don't need to chain more methods, using this collector allows you to fluently
   * collect the results into the desired container type. For example {@link #toMap} collects to an
   * immutable {@code Map}; or you could supply {@code collect(ImmutableBiMap::toImmutableBiMap)}
   * if {@code BiMap} is needed.
   *
   * <p>Entries are collected in encounter order.
   *
   * @since 3.0
   */
  public static <T, K, V> Collector<T, ?, BiStream<K, V>> groupingBy(
          Function<? super T, ? extends K> classifier, Collector<? super T, ?, V> valueCollector) {
    requireNonNull(classifier);
    Collector<T, ?, Map<K, V>> grouping =
            Collectors.groupingBy(classifier, LinkedHashMap::new, valueCollector);
    return collectingAndThen(grouping, BiStream::from);
  }

  /**
   * Returns a {@link Collector} grouping input elements by each of the multiple keys returned by
   * the {@code keysFunction}. It's similar to {@link #groupingBy(Function, Collector)} except each
   * element can belong to multiple groups. For example:
   *
   * <pre>{@code
   * ImmutableMap<Person, ImmutableList<Club>> clubMemberships =
   *     clubs.stream()
   *         .collect(
   *             groupingByEach(club -> club.getMembers().stream(), toImmutableList()))
   *         .toMap();
   * }</pre>
   *
   * <p>Entries are collected in encounter order.
   *
   * @since 6.5
   */
  public static <T, K, V> Collector<T, ?, BiStream<K, V>> groupingByEach(
          Function<? super T, ? extends Stream<? extends K>> keysFunction,
          Collector<T, ?, V> groupCollector) {
    requireNonNull(keysFunction);
    return Java9Collectors.flatMapping(
            (T e) -> keysFunction.apply(e).map(k -> kv(k, e)),
            groupingBy(Map.Entry::getKey, Collectors.mapping(Map.Entry::getValue, groupCollector)));
  }

  /**
   * Returns a {@link Collector} grouping input elements by each of the multiple keys returned by
   * the {@code keysFunction}. The input element is passed to the {@code valueFunction} and the
   * return value is added to every group it belongs to. And finally each group is collected using
   * {@code groupCollector}. For example:
   *
   * <pre>{@code
   * ImmutableMap<EmployeeId, ImmutableList<ProjectId>> projectIdsPerEmployee =
   *     projects.stream()
   *         .collect(
   *             groupingByEach(
   *                 project -> project.getOwnersList().stream(),
   *                 Project::id,
   *                 toImmutableList()))
   *         .toMap();
   * }</pre>
   *
   * <p>Entries are collected in encounter order.
   *
   * @since 6.5
   */
  public static <T, K, V, G> Collector<T, ?, BiStream<K, G>> groupingByEach(
          Function<? super T, ? extends Stream<? extends K>> keysFunction,
          Function<? super T, ? extends V> valueFunction,
          Collector<V, ?, G> groupCollector) {
    requireNonNull(keysFunction);
    requireNonNull(valueFunction);
    requireNonNull(groupCollector);
    return Java9Collectors.flatMapping(
            (T e) -> {
              V v = valueFunction.apply(e);
              return keysFunction.apply(e).map(k -> kv(k, v));
            },
            groupingBy(Map.Entry::getKey, Collectors.mapping(Map.Entry::getValue, groupCollector)));
  }

  /**
   * Returns a {@link Collector} grouping input elements by each of the multiple keys returned by
   * the {@code keysFunction}. It's similar to {@link #groupingBy(Function, Function,
   * BinaryOperator)} except each element can belong to multiple groups. For example:
   *
   * <pre>{@code
   * ImmutableMap<Person, Money> clubMembershipFees =
   *     clubs.stream()
   *         .collect(
   *             groupingByEach(
   *                 club -> club.getMembers().stream(),
   *                 Club::getMembershipFee,
   *                 Money::add))
   *         .toMap();
   * }</pre>
   *
   * <p>Entries are collected in encounter order.
   *
   * @since 6.5
   */
  public static <T, K, V> Collector<T, ?, BiStream<K, V>> groupingByEach(
          Function<? super T, ? extends Stream<? extends K>> keysFunction,
          Function<? super T, ? extends V> valueFunction,
          BinaryOperator<V> groupReducer) {
    return groupingByEach(keysFunction, valueFunction, reducingGroupMembers(groupReducer));
  }

  /**
   * Returns a {@code Collector} that concatenates {@code BiStream} objects derived from the input
   * elements using the given {@code toBiStream} function.
   *
   * <p>For example:
   *
   * <pre>{@code
   * Map<EmployeeId, Task> billableTaskAssignments = projects.stream()
   *     .collect(concatenating(p -> BiStream.from(p.getTaskAssignments())))
   *     .filterValues(Task::billable)
   *     .toMap();
   * }</pre>
   *
   * @since 3.0
   */
  public static <T, K, V> Collector<T, ?, BiStream<K, V>> concatenating(
          Function<? super T, ? extends BiStream<? extends K, ? extends V>> toBiStream) {
    return collectingAndThen(stream -> concat(stream.map(toBiStream)));
  }

  /**
   * Returns a {@code Collector} that will pair each input element with each element from {@code
   * right} into a new {@code BiStream}. For example:
   *
   * <pre>{@code
   * ImmutableList<QuarterlyReport> allQuarterlyReports = quarters.stream()
   *     .collect(crossJoining(departments))
   *     .mapToObj(QuarterlyReport::new)
   *     .collect(toImmutableList());
   * }</pre>
   *
   * <p>The input elements are repeated once per element from {@code right}. For example: {@code [1,
   * 2, 3].collect(crossJoining([a, b]))} will generate {@code [{1, a}, {2, a}, {3, a}, {1, b}, {2,
   * b}, {3, b}]}.
   *
   * <p>The returned {@code BiStream} takes {@code O(n)} space where {@code n} is the size of the
   * input elements. The "cross-joining" with the {@code right} stream is computed on-the-fly with
   * {@code O(1)} memory cost.
   *
   * @since 3.0
   */
  public static <L, R> Collector<L, ?, BiStream<L, R>> crossJoining(Stream<R> right) {
    requireNonNull(right);
    return collectingAndThen(
            toList(),
            (List<L> left) ->
                    // If `right` is infinite, even limit(1) will result in infinite loop otherwise.
                    left.isEmpty() ? empty() : concat(right.map(r -> from(left, identity(), l -> r))));
  }

  /**
   * Returns a {@code Collector} that accumulates every neighboring pair of elements into a new
   * {@code BiStream}. For example {@code Stream.of(1, 2, 3, 4).collect(toAdjacentPairs())} will
   * return {@code [{1, 2}, {2, 3}, {3, 4}]}.
   *
   * <p>If the input has 0 or 1 elements then the output is an empty {@code BiStream}. Otherwise the
   * length of the output {@code BiStream} is one less than the length of the input.
   *
   * @since 3.2
   */
  public static <T> Collector<T, ?, BiStream<T, T>> toAdjacentPairs() {
    return collectingAndThen(toList(), list -> zip(list.stream(), list.stream().skip(1)));
  }

  /**
   * Returns a {@code Collector} that splits each input element as a pair and collects them into a
   * {@link BiStream}.
   *
   * <p>Note that it's more efficient to use {@code BiStream.from(stream, toKey, toValue)} than
   * {@code stream.collect(toBiStream(toKey, toValue))}. The latter is intended to be used in the
   * middle of a long stream pipeline, when performance isn't critical.
   *
   * @since 3.2
   */
  public static <E, K, V> Collector<E, ?, BiStream<K, V>> toBiStream(
          Function<? super E, ? extends K> toKey, Function<? super E, ? extends V> toValue) {
    requireNonNull(toKey);
    requireNonNull(toValue);
    return collectingAndThen(stream -> from(stream, toKey, toValue));
  }

  /**
   * Returns a {@code Collector} that splits each input element as a pair and collects them into a
   * {@link BiStream}.
   *
   * <p>Note that it's more efficient to use {@code BiStream.from(stream, toPair)} than
   * {@code stream.collect(toBiStream(toPair))}. The latter is intended to be used in the
   * middle of a long stream pipeline, when performance isn't critical.
   *
   * @since 5.1
   */
  public static <E, K, V> Collector<E, ?, BiStream<K, V>> toBiStream(
          Function<? super E, ? extends Both<? extends K, ? extends V>> toPair) {
    requireNonNull(toPair);;
    return collectingAndThen(stream -> from(stream.map(toPair)));
  }

  /**
   * Returns a {@code Collector} that copies each input element as a pair of itself into an equivalent
   * {@code BiStream}.
   *
   * <p>Note that it's more efficient to use {@code biStream(stream)} than
   * {@code stream.collect(toBiStream())}. The latter is intended to be used in the
   * middle of a long stream pipeline, when performance isn't critical.
   *
   * @since 3.6
   */
  public static <T> Collector<T, ?, BiStream<T, T>> toBiStream() {
    return toBiStream(identity(), identity());
  }

  /** Returns an empty {@code BiStream}. */
  public static <K, V> BiStream<K, V> empty() {
    return fromEntries(Stream.empty());
  }

  /** Returns a {@code BiStream} of a single pair containing {@code key} and {@code value}. */
  public static <K, V> BiStream<K, V> of(K key, V value) {
    return fromEntries(Stream.of(kv(key, value)));
  }

  /** Returns a {@code BiStream} of two pairs, containing the supplied keys and values. */
  public static <K, V> BiStream<K, V> of(
          K key1, V value1, K key2, V value2) {
    return fromEntries(Stream.of(kv(key1, value1), kv(key2, value2)));
  }

  /** Returns a {@code BiStream} of three pairs, containing the supplied keys and values. */
  public static <K, V> BiStream<K, V> of(
          K key1,
          V value1,
          K key2,
          V value2,
          K key3,
          V value3) {
    return fromEntries(Stream.of(kv(key1, value1), kv(key2, value2), kv(key3, value3)));
  }

  /**
   * Returns a {@code BiStream} of 4 pairs, containing the supplied keys and values.
   *
   * @since 5.6
   */
  public static <K, V> BiStream<K, V> of(
          K k1,
          V v1,
          K k2,
          V v2,
          K k3,
          V v3,
          K k4,
          V v4) {
    return fromEntries(Stream.of(kv(k1, v1), kv(k2, v2), kv(k3, v3), kv(k4, v4)));
  }

  /**
   * Returns a {@code BiStream} of 5 pairs, containing the supplied keys and values.
   *
   * @since 5.6
   */
  public static <K, V> BiStream<K, V> of(
          K k1,
          V v1,
          K k2,
          V v2,
          K k3,
          V v3,
          K k4,
          V v4,
          K k5,
          V v5) {
    return fromEntries(Stream.of(kv(k1, v1), kv(k2, v2), kv(k3, v3), kv(k4, v4), kv(k5, v5)));
  }

  /**
   * Returns a {@code BiStream} of 6 pairs, containing the supplied keys and values.
   *
   * @since 5.6
   */
  public static <K, V> BiStream<K, V> of(
          K k1,
          V v1,
          K k2,
          V v2,
          K k3,
          V v3,
          K k4,
          V v4,
          K k5,
          V v5,
          K k6,
          V v6) {
    return fromEntries(
            Stream.of(kv(k1, v1), kv(k2, v2), kv(k3, v3), kv(k4, v4), kv(k5, v5), kv(k6, v6)));
  }

  /**
   * Returns a {@code BiStream} of 7 pairs, containing the supplied keys and values.
   *
   * @since 5.6
   */
  public static <K, V> BiStream<K, V> of(
          K k1,
          V v1,
          K k2,
          V v2,
          K k3,
          V v3,
          K k4,
          V v4,
          K k5,
          V v5,
          K k6,
          V v6,
          K k7,
          V v7) {
    return fromEntries(
            Stream.of(
                    kv(k1, v1), kv(k2, v2), kv(k3, v3), kv(k4, v4), kv(k5, v5), kv(k6, v6), kv(k7, v7)));
  }

  /**
   * Returns a {@code BiStream} of 8 pairs, containing the supplied keys and values.
   *
   * @since 5.6
   */
  public static <K, V> BiStream<K, V> of(
          K k1,
          V v1,
          K k2,
          V v2,
          K k3,
          V v3,
          K k4,
          V v4,
          K k5,
          V v5,
          K k6,
          V v6,
          K k7,
          V v7,
          K k8,
          V v8) {
    return fromEntries(
            Stream.of(
                    kv(k1, v1),
                    kv(k2, v2),
                    kv(k3, v3),
                    kv(k4, v4),
                    kv(k5, v5),
                    kv(k6, v6),
                    kv(k7, v7),
                    kv(k8, v8)));
  }

  /**
   * Returns a {@code BiStream} of 9 pairs, containing the supplied keys and values.
   *
   * @since 5.6
   */
  public static <K, V> BiStream<K, V> of(
          K k1,
          V v1,
          K k2,
          V v2,
          K k3,
          V v3,
          K k4,
          V v4,
          K k5,
          V v5,
          K k6,
          V v6,
          K k7,
          V v7,
          K k8,
          V v8,
          K k9,
          V v9) {
    return fromEntries(
            Stream.of(
                    kv(k1, v1),
                    kv(k2, v2),
                    kv(k3, v3),
                    kv(k4, v4),
                    kv(k5, v5),
                    kv(k6, v6),
                    kv(k7, v7),
                    kv(k8, v8),
                    kv(k9, v9)));
  }

  /**
   * Returns a {@code BiStream} of 10 pairs, containing the supplied keys and values.
   *
   * @since 5.6
   */
  public static <K, V> BiStream<K, V> of(
          K k1,
          V v1,
          K k2,
          V v2,
          K k3,
          V v3,
          K k4,
          V v4,
          K k5,
          V v5,
          K k6,
          V v6,
          K k7,
          V v7,
          K k8,
          V v8,
          K k9,
          V v9,
          K k10,
          V v10) {
    return fromEntries(
            Stream.of(
                    kv(k1, v1),
                    kv(k2, v2),
                    kv(k3, v3),
                    kv(k4, v4),
                    kv(k5, v5),
                    kv(k6, v6),
                    kv(k7, v7),
                    kv(k8, v8),
                    kv(k9, v9),
                    kv(k10, v10)));
  }

  /**
   * Returns a {@code BiStream} of the entries from {@code m1}, {@code m2} then {@code rest} in
   * encounter order. For example:
   *
   * <pre>{@code
   * Map<AccountId, Account> allAccounts = concat(primaryAccounts, secondaryAccounts).toMap();
   * }</pre>
   *
   * @since 3.0
   */
  @SafeVarargs
  public static <K, V> BiStream<K, V> concat(
          Map<? extends K, ? extends V> m1,
          Map<? extends K, ? extends V> m2,
          Map<? extends K, ? extends V>... rest) {
    Stream.Builder<Map<? extends K, ? extends V>> builder = Stream.builder();
    builder.add(requireNonNull(m1)).add(requireNonNull(m2));
    for (Map<? extends K, ? extends V> m : rest) {
      builder.add(requireNonNull(m));
    }
    return concat(builder.build().map(BiStream::from));
  }

  /**
   * Returns a {@code BiStream} of the entries from {@code s1}, {@code s2} then {@code rest} in
   * encounter order. For example:
   *
   * <pre>{@code
   * Map<AccountId, Account> allAccounts = concat(primaryAccounts, secondaryAccounts).toMap();
   * }</pre>
   *
   * @since 4.7
   */
  @SafeVarargs
  public static <K, V> BiStream<K, V> concat(
          BiStream<? extends K, ? extends V> s1,
          BiStream<? extends K, ? extends V> s2,
          BiStream<? extends K, ? extends V>... rest) {
    Stream.Builder<BiStream<? extends K, ? extends V>> builder = Stream.builder();
    builder.add(requireNonNull(s1)).add(requireNonNull(s2));
    for (BiStream<? extends K, ? extends V> s : rest) {
      builder.add(requireNonNull(s));
    }
    return concat(builder.build());
  }

  /**
   * Returns a {@code BiStream} of pairs from {@code biStreams} concatenated in encounter order.
   *
   * @since 3.0
   */
  public static <K, V> BiStream<K, V> concat(
          Stream<? extends BiStream<? extends K, ? extends V>> biStreams) {
    return fromEntries(biStreams.flatMap(BiStream::mapToEntry));
  }

  /**
   * Returns a {@code BiStream} in which the first element in {@code left} is paired with the first
   * element in {@code right}; the second paired with the corresponding second and the third with
   * the corresponding third etc. For example: {@code BiStream.zip(asList(1, 2, 3), asList("one",
   * "two"))} will return {@code BiStream.of(1, "one", 2, "two")}.
   *
   * <p>The resulting stream will only be as long as the shorter of the two iterables; if one is
   * longer, its extra elements will be ignored.
   *
   * @since 3.0
   */
  public static <L, R> BiStream<L, R> zip(Collection<L> left, Collection<R> right) {
    return zip(left.stream(), right.stream());
  }

  /**
   * Returns a {@code BiStream} in which the first element in {@code left} is paired with the first
   * element in {@code right}; the second paired with the corresponding second and the third with
   * the corresponding third etc. For example: {@code BiStream.zip(Stream.of(1, 2, 3),
   * Stream.of("one", "two"))} will return {@code BiStream.of(1, "one", 2, "two")}.
   *
   * <p>The resulting stream will only be as long as the shorter of the two input streams; if one
   * stream is longer, its extra elements will be ignored.
   *
   * <p>The resulting stream by default runs sequentially regardless of the input streams. This is
   * because the implementation is not <a
   * href="http://gee.cs.oswego.edu/dl/html/StreamParallelGuidance.html">efficiently splittable</a>.
   * and may not perform well if run in parallel.
   */
  public static <L, R> BiStream<L, R> zip(Stream<L> left, Stream<R> right) {
    return new ZippingStream<>(left, right);
  }

  /**
   * Short-hand for {@code from(elements, identity(), identity())}. Typically followed by {@link
   * #mapKeys} or {@link #mapValues}. For example:
   *
   * <pre>{@code
   * static import com.google.common.labs.collect.BiStream.biStream;
   *
   * Map<EmployeeId, Employee> employeesById = biStream(employees)
   *     .mapKeys(Employee::id)
   *     .toMap();
   * }</pre>
   *
   * @since 3.0
   */
  public static <T> BiStream<T, T> biStream(Collection<T> elements) {
    return from(elements, identity(), identity());
  }

  /**
   * Short-hand for {@code from(elements, identity(), identity())}. Typically followed by {@link
   * #mapKeys} or {@link #mapValues}. For example:
   *
   * <pre>{@code
   * static import com.google.common.labs.collect.BiStream.biStream;
   *
   * Map<EmployeeId, Employee> employeesById = biStream(employees)
   *     .mapKeys(Employee::id)
   *     .toMap();
   * }</pre>
   *
   * @since 3.6
   */
  public static <T> BiStream<T, T> biStream(Stream<T> elements) {
    return biStream(elements, identity());
  }

  /**
   * Returns a {@code BiStream} of mappings between {@code keys} and the corresponding return values
   * of the {@code toValue} function. For example:
   *
   * <pre>{@code
   * BiStream<Request, ListenableFuture<Response>> requestsAndResponses =
   *     biStream(requests, service::sendRequest);
   * }</pre>
   *
   * @since 5.6
   */
  public static <K, V> BiStream<K, V> biStream(
          Collection<K> keys, Function<? super K, ? extends V> toValue) {
    return new GenericEntryStream<>(keys.stream(), identity(), toValue);
  }

  /**
   * Returns a {@code BiStream} of mappings between {@code keys} and the corresponding return values
   * of the {@code toValue} function. For example:
   *
   * <pre>{@code
   * BiStream<Request, ListenableFuture<Response>> requestsAndResponses =
   *     biStream(requests, service::sendRequest);
   * }</pre>
   *
   * @since 5.6
   */
  public static <K, V> BiStream<K, V> biStream(
          Stream<K> keys, Function<? super K, ? extends V> toValue) {
    return new GenericEntryStream<>(keys, identity(), toValue);
  }

  /**
   * Returns a {@code BiStream} of mappings between the key returned by the {@code toKey} function
   * (when applied to each element of {@code values}), and the element itself.
   *
   * <pre>{@code
   * ImmutableListMultimap<UserId, Account> userAccounts =
   *     biStream(User::id, users)
   *         .flatMapValues(User::accounts)
   *         .collect(toImmutableListMultimap());
   * }</pre>
   *
   * @since 5.6
   */
  public static <K, V> BiStream<K, V> biStream(
          Function<? super V, ? extends K> toKey, Collection<V> values) {
    return new GenericEntryStream<>(values.stream(), toKey, identity());
  }

  /**
   * Returns a {@code BiStream} of mappings between the key returned by the {@code toKey} function
   * (when applied to each element of {@code values}), and the element itself.
   *
   * <pre>{@code
   * ImmutableListMultimap<UserId, Account> userAccounts =
   *     biStream(User::id, users)
   *         .flatMapValues(User::accounts)
   *         .collect(toImmutableListMultimap());
   * }</pre>
   *
   * @since 5.6
   */
  public static <K, V> BiStream<K, V> biStream(
          Function<? super V, ? extends K> toKey, Stream<V> values) {
    return new GenericEntryStream<>(values, toKey, identity());
  }

  /** Returns a {@code BiStream} of the entries in {@code map}. */
  public static <K, V> BiStream<K, V> from(Map<K, V> map) {
    return from(map.entrySet());
  }

  /**
   * Returns a {@code BiStream} of the key value pairs from {@code entries}.
   * For example {@code BiStream.from(multimap.entries())}.
   *
   * @since 4.7
   */
  public static <K, V> BiStream<K, V> from(Collection<? extends Map.Entry<? extends K, ? extends V>> entries) {
    return fromEntries(entries.stream());
  }

  /**
   * Returns a {@code BiStream} of {@code elements}, each transformed to a pair of values with
   * {@code toKey} and {@code toValue}.
   *
   * @deprecated Use {@code biStream(User::id, users)} to create {@code BiStream<UserId, User>},
   *     or, use {@code biStream(users, User::getAccount)} to create {@code BiStream<User, Account>}.
   *     Then use {@link #mapKeys} or {@link #mapValues} to apply further mappings.
   */
  @Deprecated
  public static <T, K, V> BiStream<K, V> from(
          Collection<T> elements,
          Function<? super T, ? extends K> toKey,
          Function<? super T, ? extends V> toValue) {
    return from(elements.stream(), toKey, toValue);
  }

  /**
   * Returns a {@code BiStream} of the elements from {@code stream}, each transformed to a pair of
   * values with {@code toKey} and {@code toValue}.
   *
   * @deprecated Use {@code biStream(User::id, users)} to create {@code BiStream<UserId, User>},
   *     or, use {@code biStream(users, User::getAccount)} to create {@code BiStream<User, Account>}.
   *     Then use {@link #mapKeys} or {@link #mapValues} to apply further mappings.
   */
  @Deprecated
  public static <T, K, V> BiStream<K, V> from(
          Stream<T> stream,
          Function<? super T, ? extends K> toKey,
          Function<? super T, ? extends V> toValue) {
    return new GenericEntryStream<>(stream, toKey, toValue);
  }

  /**
   * Returns a {@code BiStream} of the pairs from {@code stream}.
   *
   * @since 5.1
   */
  public static <K, V> BiStream<K, V> from(
          Stream<? extends Both<? extends K, ? extends V>> pairs) {
    return from(pairs, BiStream::left, BiStream::right);
  }

  /**
   * Returns a stream of the inputs and outputs from repeated applications of the {@code work}
   * function. The {@code initial} input is passed to {@code work} for the first round, after
   * which the {@code increment} function is called to determine the input for the next round. This
   * process repeats until the {@code increment} function returns {@code Optional.empty()}.
   *
   * <p>A common use case is pagination. For example, if you have a list API with pagination
   * support, the following code retrieves all pages eagerly:
   *
   * <pre>{@code
   * ImmutableList<Foo> listAllFoos() {
   *   ImmutableList.Builder<Foo> builder = ImmutableList.builder();
   *   ListFooRequest.Builder request = ListFooRequest.newBuilder()...;
   *     do {
   *       ListFooResponse response = service.listFoos(request.build());
   *       builder.addAll(response.getFoos());
   *       request.setPageToken(response.getNextPageToken());
   *     } while (!request.getPageToken().isEmpty());
   *   return builder.build();
   * }
   * }</pre>
   *
   * You can turn the above code to a lazy stream so that callers can short-circuit when they need
   * to without having to exhaust all pages:
   *
   * <pre>{@code
   * Stream<Foo> listAllFoos() {
   *   return BiStream.repeat(
   *           service::listFoos,
   *           initialRequest,
   *           (request, response) ->
   *               optional(
   *                   response.hasNextPageToken(),
   *                   request.toBuilder().setPageToken(response.getNextPageToken()).build()))
   *       .flatMapToObj((request, response) -> response.getAllFoos().stream());
   * }
   * }</pre>
   * @param work the function to repeat. Null outputs are passed through as is.
   * @param initial the initial input to pass to the {@code work} function. Cannot be null.
   * @param increment the function to get the next input given the current input and output.
   * @param <I> the input type
   * @param <O> the output type
   * @return A BiStream of the inputs and outputs of the {@code work} function. The stream is
   *     lazy in that {@code work} won't be called until the stream is being consumed; and it
   *     won't be called again until the second pair of input and output are being consumed, etc.
   * @since 5.5
   */
  public static <I, O> BiStream<I, O> repeat(
          Function<? super I, ? extends O> work,
          I initial,
          BiFunction<? super I, ? super O, ? extends Optional<? extends I>> increment) {
    requireNonNull(work);
    requireNonNull(increment);
    return fromEntries(
            MoreStreams.whileNotNull(
                    new Supplier<Map.Entry<I, O>>() {
                      I nextInput = requireNonNull(initial);
                      @Override public Map.Entry<I, O> get() {
                        I in = nextInput;
                        if (in == null) {
                          return null;
                        }
                        O out = work.apply(in);
                        nextInput = increment.apply(in, out).orElse(null);
                        return kv(in, out);
                      }
                    }));
  }

  /**
   * A predicate used to partition a {@code BiStream} into sub-groups of consecutive pairs.
   *
   * <p>Aside from that it operates on pairs, logically a "Partitioner" is unlike {@code
   * com.google.common.base.Equivalence} in that it doesn't need to be reflexive. For example, one
   * may use {@code (v1, v2) -> v1 < v2} to partition ascending sub-sequences into groups, such that
   * {@code [1, 2, 3]} results in a single group while {@code [3, 2, 1]} will be 3 groups: {@code
   * [[3], [2], [1]]}.
   */
  @FunctionalInterface
  public interface Partitioner<A, B> {
    /**
     * Returns true if consecutive pair {@code (a1, b1)} and {@code (a2, b2)} belong to the same
     * partition.
     */
    boolean belong(A a1, B b1, A a2, B b2);
  }

  /** @since 7.1 */
  public static <K, V, E extends Map.Entry<? extends K, ? extends V>> BiStream<K, V> fromEntries(
          Stream<E> entryStream) {
    return new GenericEntryStream<E, K, V>(entryStream, Map.Entry::getKey, Map.Entry::getValue) {
      @Override public <K2, V2> BiStream<K2, V2> map(
              BiFunction<? super K, ? super V, ? extends K2> keyMapper,
              BiFunction<? super K, ? super V, ? extends V2> valueMapper) {
        return from(entryStream, forEntry(keyMapper), forEntry(valueMapper));
      }

      @Override public <K2> BiStream<K2, V> mapKeys(
              BiFunction<? super K, ? super V, ? extends K2> keyMapper) {
        return from(entryStream, forEntry(keyMapper), Map.Entry::getValue);
      }

      @Override public <V2> BiStream<K, V2> mapValues(
              BiFunction<? super K, ? super V, ? extends V2> valueMapper) {
        return from(entryStream, Map.Entry::getKey, forEntry(valueMapper));
      }

      @Override public BiStream<K, V> limit(int maxSize) { // Stick to this impl where mapToEntry() is cheap
        return fromEntries(entryStream.limit(maxSize));
      }

      @Override public BiStream<K, V> skip(int n) { // Stick to this impl where mapToEntry() is cheap
        return fromEntries(entryStream.skip(n));
      }

      @Override Stream<E> mapToEntry() { // Reuse the same Entry objects. Don't allocate new ones
        return entryStream;
      }
    };
  }

  Stream<? extends Map.Entry<? extends K, ? extends V>> mapToEntry() {
    return mapToObj(BiStream::kv);
  }

  /**
   * Returns a {@code Stream} consisting of the results of applying {@code mapper} to each pair in
   * this {@code BiStream}.
   */
  public abstract <T> Stream<T> mapToObj(BiFunction<? super K, ? super V, ? extends T> mapper);

  /**
   * Returns a {@code Stream} consisting of the results of applying {@code mapper} to each pair in
   * this {@code BiStream}. If {@code mapper} function returns empty, the pair is discarded.
   *
   * @since 4.7
   */
  public final <T> Stream<T> mapToObjIfPresent(
          BiFunction<? super K, ? super V, ? extends Optional<? extends T>> mapper) {
    return mapToObj(mapper).<T>map(BiStream::orElseNull).filter(Objects::nonNull);
  }

  /**
   * Returns a {@code BiStream} consisting of the results of applying {@code keyMapper} and {@code
   * valueMapper} to the pairs in this {@code BiStream}.
   */
  public <K2, V2> BiStream<K2, V2> map(
          BiFunction<? super K, ? super V, ? extends K2> keyMapper,
          BiFunction<? super K, ? super V, ? extends V2> valueMapper) {
    requireNonNull(keyMapper);
    requireNonNull(valueMapper);
    return fromEntries(mapToObj((k, v) -> kv(keyMapper.apply(k, v), valueMapper.apply(k, v))));
  }

  /**
   * Returns a {@code BiStream} consisting of the result pairs of applying {@code mapper} to the
   * pairs in this {@code BiStream}.
   *
   * <p>For example, the following code parses each line read from a file in the format of "key:
   * value", and upon expected format, reports the line number:
   *
   * <pre>{@code
   * import static com.google.mu.util.Substring.first;
   *
   * BiStream.zip(MoreStreams.indexesFrom(1), readLines().stream())
   *     .map((lineNumber, line) ->
   *         first(':')
   *             .splitThenTrim(line)
   *             .orElseThrow(() -> new IllegalArgumentException("line: " + lineNumber)))
   *     ...;
   * }</pre>
   *
   * @since 5.2
   */
  public final <K2, V2> BiStream<K2, V2> map(
          BiFunction<? super K, ? super V, ? extends Both<? extends K2, ? extends V2>> mapper) {
    return from(mapToObj(mapper));
  }

  /**
   * Returns a {@code BiStream} consisting of the results of applying {@code mapper} function to
   * the pairs in this {@code BiStream}. If {@code mapper} returns empty, the pair is discarded.
   *
   * @since 5.0
   */
  public final <K2, V2> BiStream<K2, V2> mapIfPresent(
          BiFunction<? super K, ? super V, ? extends BiOptional<? extends K2, ? extends V2>> mapper) {
    return fromEntries(
            mapToObj(mapper)
                    .filter(BiOptional::isPresent)
                    .map(optional -> optional.map(BiStream::kv).get()));
  }

  /**
   * Returns a {@link DoubleStream} consisting of the results of applying {@code mapper} to the
   * pairs in this {@code BiStream}.
   */
  public abstract DoubleStream mapToDouble(ToDoubleBiFunction<? super K, ? super V> mapper);

  /**
   * Returns an {@link IntStream} consisting of the results of applying {@code mapper} to the pairs
   * in this {@code BiStream}.
   */
  public abstract IntStream mapToInt(ToIntBiFunction<? super K, ? super V> mapper);

  /**
   * Returns a {@link LongStream} consisting of the results of applying {@code mapper} to the pairs
   * in this {@code BiStream}.
   */
  public abstract LongStream mapToLong(ToLongBiFunction<? super K, ? super V> mapper);

  /**
   * Returns a {@code BiStream} of pairs whose keys are the result of applying {@code keyMapper} to
   * the key of each pair in this {@code BiStream}, and whose values are unchanged.
   */
  public <K2> BiStream<K2, V> mapKeys(BiFunction<? super K, ? super V, ? extends K2> keyMapper) {
    return map(keyMapper, (k, v) -> v);
  }

  /** Maps each key to another key of type {@code K2}. */
  public abstract <K2> BiStream<K2, V> mapKeys(Function<? super K, ? extends K2> keyMapper);

  /** Maps each value to another value of type {@code V2}. */
  public <V2> BiStream<K, V2> mapValues(
          BiFunction<? super K, ? super V, ? extends V2> valueMapper) {
    return map((k, v) -> k, valueMapper);
  }

  /** Maps each value to another value of type {@code V2}. */
  public abstract <V2> BiStream<K, V2> mapValues(Function<? super V, ? extends V2> valueMapper);

  /**
   * Maps a single pair to zero or more objects of type {@code T}.
   *
   * <p>If a mapped stream is null, an empty stream is used instead.
   */
  public final <T> Stream<T> flatMapToObj(
          BiFunction<? super K, ? super V, ? extends Stream<? extends T>> mapper) {
    return mapToObj(mapper).flatMap(identity());
  }

  /**
   * Maps a single pair to zero or more {@code double}s.
   *
   * <p>If a mapped stream is null, an empty stream is used instead.
   */
  public final DoubleStream flatMapToDouble(
          BiFunction<? super K, ? super V, ? extends DoubleStream> mapper) {
    return mapToObj(mapper).flatMapToDouble(identity());
  }

  /**
   * Maps a single pair to zero or more {@code int}s.
   *
   * <p>If a mapped stream is null, an empty stream is used instead.
   */
  public final IntStream flatMapToInt(
          BiFunction<? super K, ? super V, ? extends IntStream> mapper) {
    return mapToObj(mapper).flatMapToInt(identity());
  }

  /**
   * Maps a single pair to zero or more {@code long}s.
   *
   * <p>If a mapped stream is null, an empty stream is used instead.
   */
  public final LongStream flatMapToLong(
          BiFunction<? super K, ? super V, ? extends LongStream> mapper) {
    return mapToObj(mapper).flatMapToLong(identity());
  }

  /**
   * Maps each pair in this stream to zero or more pairs in another {@code BiStream}. For example
   * the following code snippet repeats each pair in a {@code BiStream} for 3 times:
   *
   * <pre>{@code
   * BiStream<K, V> repeated = stream.flatMap((k, v) -> BiStream.of(k, v, k, v, k, v));
   * }</pre>
   *
   * <p>If a mapped stream is null, an empty stream is used instead.
   */
  public final <K2, V2> BiStream<K2, V2> flatMap(
          BiFunction<? super K, ? super V, ? extends BiStream<? extends K2, ? extends V2>> mapper) {
    return fromEntries(mapToObj(mapper).filter(Objects::nonNull).flatMap(BiStream::mapToEntry));
  }

  /**
   * Maps each key to zero or more keys of type {@code K2}.
   *
   * <p>If a mapped stream is null, an empty stream is used instead.
   */
  public final <K2> BiStream<K2, V> flatMapKeys(
          BiFunction<? super K, ? super V, ? extends Stream<? extends K2>> keyMapper) {
    requireNonNull(keyMapper);
    return fromEntries(
            this.<Map.Entry<K2, V>>flatMapToObj( // j2cl compiler needs help with type inference
                    (k, v) -> nullToEmpty(keyMapper.apply(k, v)).map(k2 -> kv(k2, v))));
  }

  /**
   * Maps each key to zero or more keys of type {@code K2}.
   *
   * <p>If a mapped stream is null, an empty stream is used instead.
   */
  public final <K2> BiStream<K2, V> flatMapKeys(
          Function<? super K, ? extends Stream<? extends K2>> keyMapper) {
    requireNonNull(keyMapper);
    return flatMapKeys((k, v) -> keyMapper.apply(k));
  }


  /**
   * Given {@code keyMapping} that maps the keys of type {@code K} to elements of type {@code K2},
   * returns a {@code BiStream} of type {@code <K2, V>}.
   *
   * <p>Keys not found in {@code keyMap} (or mapped to null) are discarded.
   *
   * <p>For example, if you need to turn a {@code BiStream<StudentId, Score>} to {@code
   * BiStream<Student, Score>} by looking up the student id in a {@code Map<StudentId, Student>},
   * you can do:
   *
   * <pre>{@code
   * Map<StudentId, Score> scores = ...;
   * BiStream.from(scores)
   *     .mapKeysIfPresent(studentsMap)
   *     ...;
   * }</pre>
   *
   * <p>The above code is equivalent to the following variants:
   *
   * <pre>{@code
   * Map<StudentId, Score> scores = ...;
   * BiStream.from(scores)
   *     .mapKeys(studentsMap::get)
   *     .mapKeys(Optional::ofNullable)
   *     .flatMapKeys(Streams::stream)
   *     ...;
   * }</pre>
   *
   * or:
   *
   * <pre>{@code
   * Map<StudentId, Score> scores = ...;
   * BiStream.from(scores)
   *     .mapKeys(studentsMap::get)
   *     .filterKeys(Objects::nonNull)
   *     ...;
   * }</pre>
   *
   * @since 4.7
   */
  public final <K2> BiStream<K2, V> mapKeysIfPresent(Map<? super K, ? extends K2> keyMapping) {
    return this.<K2>mapKeys(keyMapping::get).filterKeys(Objects::nonNull);
  }

  /**
   * Returns a {@code BiStream} of pairs whose keys are the result of applying {@code keyMapper} to
   * the key of each pair in this {@code BiStream}, and whose values are unchanged. If {@code
   * keyMapper} function returns empty, the pair is discarded.
   *
   * <p>For example the following code counts the total number of unique patients per hospital, from
   * doctors' affiliated hospitals:
   *
   * <pre>{@code
   * Map<Doctor, Patient> doctorAndPatients = ...;
   * Map<Hospital, Long> hospitalPatientCounts =
   *    BiStream.from(doctorAndPatients)
   *        .mapKeysIfPresent(Doctor::optionalAffliatedHospital)
   *        .collect(toImmutableMap(counting()));
   * }</pre>
   *
   * @since 4.7
   */
  public final <K2> BiStream<K2, V> mapKeysIfPresent(
          Function<? super K, ? extends Optional<? extends K2>> keyMapper) {
    return mapKeys(keyMapper).<K2>mapKeys(BiStream::orElseNull).filterKeys(Objects::nonNull);
  }

  /**
   * Returns a {@code BiStream} of pairs whose keys are the result of applying {@code keyMapper} to
   * each pair in this {@code BiStream}, and whose values are unchanged. If {@code keyMapper}
   * function returns empty, the pair is discarded.
   *
   * @since 4.7
   */
  public final <K2> BiStream<K2, V> mapKeysIfPresent(
          BiFunction<? super K, ? super V, ? extends Optional<? extends K2>> keyMapper) {
    return mapKeys(keyMapper).<K2>mapKeys(BiStream::orElseNull).filterKeys(Objects::nonNull);
  }

  /**
   * Maps each value to zero or more values of type {@code V2}.
   *
   * <p>If a mapped stream is null, an empty stream is used instead.
   */
  public final <V2> BiStream<K, V2> flatMapValues(
          BiFunction<? super K, ? super V, ? extends Stream<? extends V2>> valueMapper) {
    requireNonNull(valueMapper);
    return fromEntries(
            this.<Map.Entry<K, V2>>flatMapToObj( // j2cl compiler needs help with type inference
                    (k, v) -> nullToEmpty(valueMapper.apply(k, v)).map(v2 -> kv(k, v2))));
  }

  /**
   * Maps each value to zero or more values of type {@code V2}.
   *
   * <p>If a mapped stream is null, an empty stream is used instead.
   */
  public final <V2> BiStream<K, V2> flatMapValues(
          Function<? super V, ? extends Stream<? extends V2>> valueMapper) {
    requireNonNull(valueMapper);
    return flatMapValues((k, v) -> valueMapper.apply(v));
  }

  /**
   * Given {@code valueMapping} that maps values of type {@code V} to result values of type {@code
   * V2}, returns a {@code BiStream} of type {@code <K, V2>}.
   *
   * <p>Values not found in {@code valueMap} (or mapped to null) are discarded.
   *
   * <p>For example, if you need to turn a {@code Multimap<ClassId, StudentId>} to {@code
   * Multimap<ClassId, Student>} by looking up the student id in a {@code Map<StudentId, Student>},
   * you can do:
   *
   * <pre>{@code
   * Multimap<ClassId, StudentId> registration = ...;
   * ImmutableSetMultimap<ClassId, Student> roster = BiStream.from(registration)
   *     .mapValuesIfPresent(studentsMap)
   *     .collect(toImmutableSetMultimap());
   * }</pre>
   *
   * <p>The above code is equivalent to the following variants:
   *
   * <pre>{@code
   * Multimap<ClassId, StudentId> registration = ...;
   * ImmutableSetMultimap<ClassId, Student> roster = BiStream.from(registration)
   *     .mapValues(studentsMap::get)
   *     .mapValues(Optional::ofNullable)
   *     .flatMapValues(Streams::stream)
   *     .collect(toImmutableSetMultimap());
   * }</pre>
   *
   * or:
   *
   * <pre>{@code
   * Multimap<ClassId, StudentId> registration = ...;
   * ImmutableSetMultimap<ClassId, Student> roster = BiStream.from(registration)
   *     .mapValues(studentsMap::get)
   *     .filterValues(Objects::nonNull)
   *     .collect(toImmutableSetMultimap());
   * }</pre>
   *
   * @since 4.7
   */
  public final <V2> BiStream<K, V2> mapValuesIfPresent(Map<? super V, ? extends V2> valueMapping) {
    return this.<V2>mapValues(valueMapping::get).filterValues(Objects::nonNull);
  }

  /**
   * Returns a {@code BiStream} of pairs whose values are the result of applying {@code valueMapper}
   * to the value of each pair in this {@code BiStream}, and whose keys are unchanged. If {@code
   * valueMapper} function returns empty, the pair is discarded.
   *
   * <p>For example the following code collects all unique insurance companies per doctor:
   *
   * <pre>{@code
   * Map<Doctor, Patient> doctorAndPatients = ...;
   * ImmutableSetMultimap<Doctor, InsuranceCompany> insurancesPerDoctor =
   *    BiStream.from(doctorAndPatients)
   *        .mapValuesIfPresent(Partient::optionalInsurarnce)
   *        .collect(toImmutableSetMultimap());
   * }</pre>
   *
   * @since 4.7
   */
  public final <V2> BiStream<K, V2> mapValuesIfPresent(
          Function<? super V, ? extends Optional<? extends V2>> valueMapper) {
    return mapValues(valueMapper).<V2>mapValues(BiStream::orElseNull).filterValues(Objects::nonNull);
  }

  /**
   * Returns a {@code BiStream} of pairs whose values are the result of applying {@code valueMapper}
   * to each pair in this {@code BiStream}, and whose keys are unchanged. If {@code valueMapper}
   * function returns empty, the pair is discarded.
   *
   * @since 4.7
   */
  public final <V2> BiStream<K, V2> mapValuesIfPresent(
          BiFunction<? super K, ? super V, ? extends Optional<? extends V2>> valueMapper) {
    return mapValues(valueMapper).<V2>mapValues(BiStream::orElseNull).filterValues(Objects::nonNull);
  }

  /**
   * Returns a {@code BiStream} consisting of the pairs of this stream, additionally invoking {@code
   * action} on each pair as pairs are consumed from the resulting stream.
   */
  public final BiStream<K, V> peek(BiConsumer<? super K, ? super V> action) {
    requireNonNull(action);
    return fromEntries(mapToEntry().peek(e -> action.accept(e.getKey(), e.getValue())));
  }

  /**
   * Filter this stream to only pairs matching {@code predicate}.
   *
   * <p>Note that if you are passing in a lambda with the {@code !} operator, consider using {@link
   * #skipIf(BiPredicate)} instead that might even allow you to use method reference.
   */
  public final BiStream<K, V> filter(BiPredicate<? super K, ? super V> predicate) {
    requireNonNull(predicate);
    return fromEntries(mapToEntry().filter(kv -> predicate.test(kv.getKey(), kv.getValue())));
  }

  /**
   * Filter this stream to only entries matching {@code predicate}.
   *
   * <p>Note that if you are passing in a lambda with the {@code !} operator, consider using {@link
   * #skipIf(Predicate)} instead that might even allow you to use method reference.
   */
  public final BiStream<K, V> filter(Predicate<? super Map.Entry<? extends K, ? extends V>> predicate) {
    requireNonNull(predicate);
    return fromEntries(mapToEntry().filter(predicate));
  }

  /**
   * Filter this stream to only pairs whose key matches {@code predicate}.
   *
   * <p>Note that if you are passing in a lambda with the {@code !} operator, consider using {@link
   * #skipKeysIf} instead that might even allow you to use method reference.
   */
  public final BiStream<K, V> filterKeys(Predicate<? super K> predicate) {
    requireNonNull(predicate);
    return filter((k, v) -> predicate.test(k));
  }

  /**
   * Filter this stream to only pairs whose value matches {@code predicate}.
   *
   * <p>Note that if you are passing in a lambda with the {@code !} operator, consider using {@link
   * #skipValuesIf} instead that might even allow you to use method reference.
   */
  public final BiStream<K, V> filterValues(Predicate<? super V> predicate) {
    requireNonNull(predicate);
    return filter((k, v) -> predicate.test(v));
  }

  /**
   * Filter this stream to exclude pairs matching {@code predicate}.
   *
   * <p>Useful especially when it allows you to use method reference. For example:
   *
   * <pre>{@code
   * tasks.stream()
   *      .collect(crossJoining(Arrays.stream(MachineType.values()))
   *      .skipIf(Worker::blacklistsMachine)
   *      ...
   * }</pre>
   *
   * @since 5.1
   */
  public final BiStream<K, V> skipIf(BiPredicate<? super K, ? super V> predicate) {
    return filter(predicate.negate());
  }

  /**
   * Filter this stream to exclude entries matching {@code predicate}.
   *
   * <p>Useful especially when it allows you to use method reference. For example:
   *
   * <pre>{@code
   * tasks.stream()
   *      .collect(crossJoining(Arrays.stream(MachineType.values()))
   *      .skipIf(Worker::blacklistsMachine)
   *      ...
   * }</pre>
   *
   * @since 5.1
   */
  public final BiStream<K, V> skipIf(Predicate<? super Map.Entry<? extends K, ? extends V>> predicate) {
    return filter(predicate.negate());
  }

  /**
   * Filter this stream to exclude pairs whose key matches {@code predicate}.
   *
   * <p>Useful especially when it allows you to use method reference. For example:
   *
   * <pre>{@code
   * BiStream.from(rosters)
   *      .skipKeysIf(inactiveUserIds::contains)
   *      ...
   * }</pre>
   *
   * @since 5.1
   */
  public final BiStream<K, V> skipKeysIf(Predicate<? super K> predicate) {
    return filterKeys(predicate.negate());
  }

  /**
   * Filter this stream to exclude pairs whose value matches {@code predicate}.
   *
   * <p>Useful especially when it allows you to use method reference. For example:
   *
   * <pre>{@code
   * BiStream.zip(userIds, userNames)
   *      .skipValuesIf(String::isEmpty)
   *      ...
   * }</pre>
   *
   * @since 5.1
   */
  public final BiStream<K, V> skipValuesIf(Predicate<? super V> predicate) {
    return filterValues(predicate.negate());
  }

  /**
   * Returns a {@code BiStream} consisting of the pairs in this stream, followed by the pairs in
   * {@code other}.
   *
   * <p>NOTE: This method is implemented using {@link Stream#concat}; therefore, the same warnings
   *     about deeply-nested combined streams also apply to this method. In particular, avoid
   *     calling this method in a loop to combine many streams together.
   */
  public final BiStream<K, V> append(BiStream<? extends K, ? extends V> other) {
    return fromEntries(Stream.concat(mapToEntry(), other.mapToEntry()));
  }

  /**
   * Returns a {@code BiStream} consisting of the pairs in this stream, followed by the pair of
   * {@code key} and {@code value}.
   *
   * <p>NOTE: This method is implemented using {@link Stream#concat}; therefore, the same warnings
   *     about deeply-nested combined streams also apply to this method. In particular, avoid
   *     calling this method in a loop to combine many streams together.
   */
  public final BiStream<K, V> append(K key, V value) {
    return append(of(key, value));
  }

  /** Returns a {@code Stream} consisting of only the keys from each pair in this stream. */
  public final Stream<K> keys() {
    return mapToObj((k, v) -> k);
  }

  /** Returns a {@code Stream} consisting of only the values from each pair in this stream. */
  public final Stream<V> values() {
    return mapToObj((k, v) -> v);
  }

  /**
   * Returns a {@code BiStream} where each pair is a pair from this stream with the key and value
   * swapped.
   */
  public abstract BiStream<V, K> inverse();

  /** Performs {@code action} for each pair in this stream. */
  public abstract void forEach(BiConsumer<? super K, ? super V> action);

  /** Performs {@code action} for each pair in this stream, in order. */
  public abstract void forEachOrdered(BiConsumer<? super K, ? super V> consumer);

  /** Returns true if all pairs in this stream match {@code predicate}. */
  public abstract boolean allMatch(BiPredicate<? super K, ? super V> predicate);

  /** Returns true if any pair in this stream matches {@code predicate}. */
  public abstract boolean anyMatch(BiPredicate<? super K, ? super V> predicate);

  /** Returns true if no pairs in this stream match {@code predicate}. */
  public final boolean noneMatch(BiPredicate<? super K, ? super V> predicate) {
    return !anyMatch(predicate);
  }

  /**
   * Returns the first pair from this stream, or {@code BiOptional.empty()} if the stream is empty.
   *
   * @throws NullPointerException if the chosen pair includes null
   * @since 5.0
   */
  public final BiOptional<K, V> findFirst() {
    return BiOptional.flatMap(mapToObj(BiOptional::of).findFirst(), identity());
  }

  /**
   * Returns any pair from this stream, or {@code BiOptional.empty()} if the stream is empty.
   *
   * @throws NullPointerException if the chosen pair includes null
   * @since 5.0
   */
  public final BiOptional<K, V> findAny() {
    return BiOptional.flatMap(mapToObj(BiOptional::of).findAny(), identity());
  }

  /**
   * Returns a {@code BiStream} consisting of the only the first {@code maxSize} pairs of this
   * stream.
   */
  public abstract BiStream<K, V> limit(int maxSize);

  /**
   * Returns a {@code BiStream} consisting of the remaining pairs from this stream, after discarding
   * the first {@code n} pairs.
   */
  public abstract BiStream<K, V> skip(int n);

  /**
   * Returns a {@code BiStream} consisting of only the distinct pairs (according to {@code
   * Object.equals(Object)} for both key and value).
   */
  public final BiStream<K, V> distinct() {
    return fromEntries(mapToEntry().distinct());
  }

  /**
   * Returns a {@code BiStream} consisting of the pairs in this stream, in the order produced by
   * applying {@code comparator} on the keys of each pair.
   */
  public final BiStream<K, V> sortedByKeys(Comparator<? super K> comparator) {
    return sorted(comparingByKey(comparator));
  }

  /**
   * Returns a {@code BiStream} consisting of the pairs in this stream, in the order produced by
   * applying {@code comparator} on the values of each pair.
   */
  public final BiStream<K, V> sortedByValues(Comparator<? super V> comparator) {
    return sorted(comparingByValue(comparator));
  }

  /**
   * Returns a {@code BiStream} consisting of the pairs in this stream, in the order produced by
   * applying {@code comparator} on the result of applying the {@code sortKeyFunction}.
   *
   * @since 6.6
   */
  public final <T> BiStream<K, V> sortedBy(
          BiFunction<? super K, ? super V, T> sortKeyFunction, Comparator<? super T> comparator) {
    requireNonNull(sortKeyFunction);
    return sorted(Comparator.comparing((Map.Entry<K, V> e) -> sortKeyFunction.apply(e.getKey(), e.getValue()), comparator));
  }

  /**
   * Returns a {@code BiStream} consisting of the pairs in this stream, in the order produced by
   * applying the {@code byKey} comparator on the keys of each pair, and then the {@code byValue}
   * comparator on the values of pairs with equal keys.
   *
   * <p>To sort by value then by key, consider using {@link #inverse} first.
   */
  public final BiStream<K, V> sorted(Comparator<? super K> byKey, Comparator<? super V> byValue) {
    return sorted(
            Comparator.<Map.Entry<? extends K, ? extends V>, K>comparing(Map.Entry::getKey, byKey)
                    .thenComparing(Map.Entry::getValue, byValue));
  }

  /**
   * Returns a {@code BiStream} consisting of the pairs in this stream, in the order produced by
   * applying the {@code entryComparator} comparator between key-value pairs.
   */
  @SuppressWarnings("unchecked") // Immutable Map.Entry<> is covariant.
  private BiStream<K, V> sorted(Comparator<? super Map.Entry< K, V>> entryComparator) {
    return fromEntries(((Stream<Map.Entry<K, V>>) mapToEntry()).sorted(entryComparator));
  }

  /** Returns the count of pairs in this stream. */
  public final long count() {
    return keys().count();
  }

  /**
   * Returns an immutable {@link Map} that is the result of collecting the pairs in this stream. If a
   * duplicate key is encountered, throws an {@link IllegalStateException}.
   *
   * <p>While this is a convenient shortcut of {@code collect(Collectors::toMap)}, if you have a
   * {@code BiStream<SubFoo, SubBar>}, the return type of {@code toMap()} will be
   * {@code Map<SubFoo, SubBar>}. To collect to {@code Map<Foo, Bar>}, use the equivalent
   * {@code collect(Collectors::toMap)} or {@code collect(BiCollectors.toMap())}.
   */
  public final Map<K, V> toMap() {
    return collect(BiCollectors.toMap());
  }

  /**
   * Returns an object of type {@code R} that is the result of collecting the pairs in this stream
   * using {@code collector}.
   *
   * <p>Please note that any {@code Collector}-returning factory method can be directly
   * "method referenced" as {@link BiCollector} if it accepts two {@code Function} parameters
   * corresponding to the "key" and the "value" parts respectively. For example: {@code
   * collect(Collectors::toConcurrentMap)}, {@code
   * collect(ImmutableSetMultimap::toImmutableSetMultimap)}, {@code
   * collect(Maps::toImmutableEnumMap)}, {@code collect(ImmutableBiMap::toImmutableBiMap)}.
   *
   * <p>In addition, check out {@link BiCollectors} for some other useful {@link BiCollector}
   * implementations.
   */
  public abstract <R> R collect(BiCollector<? super K, ? super V, R> collector);

  /**
   * Equivalent to {@code collect(collectingAndThen(collector, finisher))} but helps to save
   * syntactic noise.
   *
   * <p>This is mainly used for "return" statements where you have a long BiStream chain, only the
   * last* step needs to pass the return value of {@code collect()} to a final method call, for
   * example: <pre>{@code
   *   return new Ledger(
   *       BiStream.from(...)
   *           .mapKeys(...)
   *           .flatMapValues(...)
   *           ...
   *           .collect(toImmutableMap()));
   * }</pre>
   *
   * This syntax breaks the first-thing-first order of the BiStream pipeline by showing the last
   * step at the top-most line. Alternatively, one can declare a local variable to hold the
   * return value of {@code collect()}. But sometimes it's undesirable if the intermediary
   * object's type is implementation-detail-ish or just too verbose.
   *
   * <p>Using this method, the above example can be changed to pipeline-friendly syntax with less
   * indentation:
   * <pre>{@code
   *   return BiStream.from(...)
   *       .mapKeys(...)
   *       .flatMapValues(...)
   *       ...
   *       .collect(toImmutableMap(), Ledger::new);
   * }</pre>
   *
   * @since 5.6
   */
  public final <T, R> R collect(
          BiCollector<? super K, ? super V, T> collector, Function<? super T, R> finisher) {
    return finisher.apply(collect(collector));
  }

  /**
   * Performs mutable reduction, as in {@code
   * collect(ImmutableMap.builder(), ImmutableMap.Builder::put)}.
   *
   * <p>More realistically (since you'd likely use {@code collect(toImmutableMap())} instead for
   * ImmutableMap), you could collect pairs into two repeated proto fields:
   *
   * <pre>{@code
   *   BiStream.zip(shardRequests, shardResponses)
   *       .filter(...)
   *       .collect(
   *           BatchResponse.newBuilder(),
   *           (builder, req, resp) -> builder.addShardRequest(req).addShardResponse(resp))
   *       .build();
   * }</pre>
   *
   * <p>While {@link #collect(BiCollector)} may perform parallel reduction if the underlying
   * stream is parallel, this reduction is guaranteed to be sequential and single-threaded.
   *
   * <p>Returns the populated {@code container} instance.
   *
   * @since 5.0
   */
  public abstract <A> A collect(
          A container, BiAccumulator<? super A, ? super K, ? super V> accumulator);

  /**
   * Closes any resources associated with this stream, typically used in a try-with-resources
   * statement.
   */
  @Override public abstract void close();

  /** Returns a BiIterator for callers to iterate through the pairs in this stream in encounter order. */
  abstract BiIterator<K, V> iterator();

  /**
   * Returns a {@code BiStream} consisting of consecutive groupings from this stream. Consecutive
   * pairs whose key maps to the same group according to {@code classifier} will have their values
   * grouped together using {@code groupCollector}.
   *
   * <p>For example to lazily summarize a large, pre-sorted stock price data stream per day:
   *
   * <pre>{@code
   * biStream(stockPriceData)
   *     .groupConsecutiveBy(PriceDatum::day, summarizingDouble(PriceDatum::price))
   *     .toMap();
   * }</pre>
   *
   * <p>Unlike JDK {@link Collectors#groupingBy groupingBy()} collectors, the returned BiStream
   * consumes the input elements lazily and only requires {@code O(groupCollector)} space for the
   * current consecutive elements group. For instance the {@code groupConsecutiveBy(Event::type,
   * counting())} stream takes O(1) space. While this makes it more efficient to process large
   * streams, the input data often need to be pre-sorted for the grouping to be useful.
   *
   * <p>To apply grouping beyond consecutive elements, use {@link BiCollectors#groupingBy(Function,
   * Collector) collect(BiCollectors.groupingBy(classifier, groupCollector))} instead.
   *
   * <p>Consecutive keys mapped to null by {@code classifier} will be grouped together.
   *
   * @param classifier The function to determine the group key. Because it's guaranteed to be
   *     invoked once and only once per entry, and that the returned BiStream is sequential and
   *     respects encounter order, this function is allowed to have side effects.
   *
   * @since 5.4
   */
  public final <G, A, R> BiStream<G, R> groupConsecutiveBy(
          Function<? super K, ? extends G> classifier,
          Collector<? super V, A, ? extends R> groupCollector) {
    return this
            .<G>mapKeys(classifier)
            .groupConsecutiveByKeys(groupCollector.supplier(), groupCollector.accumulator())
            .mapValues(groupCollector.finisher());
  }

  /**
   * Returns a lazy {@code BiStream} of the consecutive groups of pairs from this stream.
   * Consecutive pairs whose key maps to the same group according to {@code classifier} will have
   * their values reduced using the {@code groupReducer} function.
   *
   * <p>For example to lazily find the daily opening stock price from a large, pre-sorted stock data
   * stream:
   *
   * <pre>{@code
   * biStream(stockPriceDataSortedByTime)
   *     .groupConsecutiveBy(PriceDatum::day, (a, b) -> a)
   *     ,toMap();
   * }</pre>
   *
   * <p>Unlike JDK {@link Collectors#groupingBy groupingBy()} collectors, the returned BiStream
   * consumes the input elements lazily and only requires {@code O(1)} space. While this makes it
   * more efficient to process large streams, the input data often need to be pre-sorted for the
   * grouping to be useful.
   *
   * <p>To apply grouping beyond consecutive elements, use {@link BiCollectors#groupingBy(Function,
   * BinaryOperator) collect(BiCollectors.groupingBy(classifier, groupReducer))} instead.
   *
   * <p>Consecutive null keys are grouped together.
   *
   * @param classifier The function to determine the group key. Because it's guaranteed to be
   *     invoked once and only once per entry, and that the returned BiStream is sequential and
   *     respects encounter order, this function is allowed to have side effects.
   *
   * @since 5.4
   */
  public final <G> BiStream<G, V> groupConsecutiveBy(
          Function<? super K, ? extends G> classifier, BinaryOperator<V> groupReducer) {
    return groupConsecutiveBy(classifier, reducingGroupMembers(groupReducer));
  }

  /**
   * Returns a {@code BiStream} consisting of consecutive groupings from this stream. Consecutive
   * pairs mapping to the same group according to {@code classifier} will be grouped together using
   * {@code groupCollector}.
   *
   * <p>This can be useful when you need to apply nested groupings, for example, to first group
   * consecutive events by year, then by continuity (happened within 24 hours):
   *
   * <pre>{@code
   * import static com.google.mu.util.stream.BiiCollectors.collectingAndThen;
   *
   * ImmutableListMultimap<Integer, List<Event>> continuousEventsByYear =
   *     biStream(events)
   *         .mapKeys(Event::date)
   *         .groupConsecutiveBy(
   *             (date, event) -> date.year(),
   *             collectingAndThen(
   *                 annual -> annual.groupConsecutiveIf(
   *                     (d1, d2) -> Duration.between(d1, d2).compareTo(Duration.ofHours(24)) < 0,
   *                     toList())))
   *         .collect(flatteningToImmutableListMultimap(subgroups -> subgroups));
   * }</pre>
   *
   * <p>Unlike JDK {@link Collectors#groupingBy groupingBy()} collectors, the returned BiStream
   * consumes the input elements lazily and only requires {@code O(groupCollector)} space for the
   * current consecutive elements group. For instance the {@code groupConsecutiveBy(Event::type,
   * counting())} stream takes O(1) space. While this makes it more efficient to process large
   * streams, the input data often need to be pre-sorted for the grouping to be useful.
   *
   * <p>To apply grouping beyond consecutive elements, use {@link
   * BiCollectors#groupingBy(BiFunction, BiCollector) collect(BiCollectors.groupingBy(classifier,
   * groupCollector))} instead.
   *
   * <p>Consecutive pairs mapped to null by {@code classifier} will be grouped together.
   *
   * @param classifier The function to determine the group key. Because it's guaranteed to be
   *     invoked once and only once per entry, and that the returned BiStream is sequential and
   *     respects encounter order, this function is allowed to have side effects.
   *
   * @since 5.5
   */
  public final <G, A, R> BiStream<G, R> groupConsecutiveBy(
          BiFunction<? super K, ? super V, ? extends G> classifier,
          BiCollector<? super K, ? super V, R> groupCollector) {
    return this
            .<G, Map.Entry<K, V>>map(classifier, BiStream::kv)
            .groupConsecutiveByKeys(groupCollector.collectorOf(Map.Entry::getKey, Map.Entry::getValue));
  }

  /**
   * Returns a lazy {@code Stream} of the consecutive groups of values from this stream. Two
   * consecutive entries belong to the same group if {@code sameGroup.belong(key1, value1, key2,
   * value2)} is true. Pairs belonging to the same group are grouped together using {@code
   * groupCollector}.
   *
   * <p>The {@code sameGroup} predicate is always evaluated with two consecutive pairs in encounter
   * order.
   *
   * <p>The following example identifies price changes above a gap threshold from a stock's
   * historical price:
   *
   * <pre>{@code
   * Map<DateTime, Double> historicalPrices = ...;
   * ImmutableList<ImmutableMap<DateTime, Double>> priceClusters =
   *     biStream(historicalPrices)
   *         .groupConsecutiveIf((d1, p1, d2, p2) -> abs(p1 - p2) < gap, toImmutableMap())
   *         .collect(toImmutableList());
   * }</pre>
   *
   * <p>Unlike JDK {@link Collectors#groupingBy groupingBy()} collectors, the returned Stream
   * consumes the input elements lazily and only requires {@code O(groupCollector)} space for the
   * current consecutive elements group. While this makes it more efficient to process large
   * streams, the input data often need to be pre-sorted for the grouping to be useful.
   *
   * <p>Null elements are allowed as long as the {@code sameGroup} predicate and {@code
   * groupCollector} allow nulls.
   *
   * @since 5.6
   */
  public final <R> Stream<R> groupConsecutiveIf(
          Partitioner<? super K, ? super V> sameGroup, BiCollector<? super K, ? super V, R> groupCollector) {
    requireNonNull(sameGroup);
    return biStream(mapToEntry())
            .groupConsecutiveIf(
                    (p1, p2) -> sameGroup.belong(p1.getKey(), p1.getValue(), p2.getKey(), p2.getValue()),
                    groupCollector.collectorOf(Map.Entry::getKey, Map.Entry::getValue));
  }

  /**
   * Returns a lazy {@code Stream} of the consecutive groups of values from this stream. Two
   * consecutive entries belong to the same group if {@code sameGroup.test(key1, key2)} is true.
   * Values belonging to the same group are grouped together using {@code groupCollector}.
   *
   * <p>The {@code sameGroup} predicate is always evaluated with two consecutive keys in encounter
   * order.
   *
   * <p>The following example identifies price changes above a gap threshold from a stock's
   * historical price:
   *
   * <pre>{@code
   * Stream<DoubleSummaryStatistics> priceClusters =
   *     biStream(stockPriceData)
   *         .groupConsecutiveIf((a, b) -> abs(a - b) < gap, summarizingDouble());
   * }</pre>
   *
   * <p>Unlike JDK {@link Collectors#groupingBy groupingBy()} collectors, the returned Stream
   * consumes the input elements lazily and only requires {@code O(groupCollector)} space for the
   * current consecutive elements group. While this makes it more efficient to process large
   * streams, the input data often need to be pre-sorted for the grouping to be useful.
   *
   * <p>Null elements are allowed as long as the {@code sameGroup} predicate and {@code
   * groupCollector} allow nulls.
   *
   * @since 5.4
   */
  public final <A, R> Stream<R> groupConsecutiveIf(
          BiPredicate<? super K, ? super K> sameGroup, Collector<? super V, A, R> groupCollector) {
    BiConsumer<A, ? super V> groupAccumulator = groupCollector.accumulator();
    return groupConsecutiveIf(
            sameGroup,
            groupCollector.supplier(),
            (a, k, v) -> groupAccumulator.accept(a, v),
            groupCollector.finisher());
  }

  /**
   * Returns a lazy {@code Stream} of the consecutive groups of values from this stream. Two
   * consecutive entries belong to the same group if {@code sameGroup.test(key1, key2)} is true.
   * Values belonging to the same group are reduced using {@code groupReducer}.
   *
   * <p>The {@code sameGroup} predicate is always evaluated with two consecutive keys in encounter
   * order.
   *
   * <p>Unlike JDK {@link Collectors#groupingBy groupingBy()} collectors, the returned Stream
   * consumes the input elements lazily and only requires {@code O(groupCollector)} space for the
   * current consecutive elements group. While this makes it more efficient to process large
   * streams, the input data often need to be pre-sorted for the grouping to be useful.
   *
   * <p>Null elements are allowed as long as the {@code sameGroup} predicate and {@code
   * groupCollector} allow nulls.
   *
   * @since 5.4
   */
  public final Stream<V> groupConsecutiveIf(
          BiPredicate<? super K, ? super K> sameGroup, BinaryOperator<V> groupReducer) {
    return groupConsecutiveIf(sameGroup, reducingGroupMembers(groupReducer));
  }

  private <A, R> BiStream<K, R> groupConsecutiveByKeys(
          Collector<? super V, A, R> groupCollector) {
    return groupConsecutiveByKeys(groupCollector.supplier(), groupCollector.accumulator())
            .mapValues(groupCollector.finisher());
  }

  private <A> BiStream<K, A> groupConsecutiveByKeys(
          Supplier<A> newGroup, BiConsumer<? super A, ? super V> groupAccumulator) {
    requireNonNull(newGroup);
    requireNonNull(groupAccumulator);
    Temp<K> currentGroup = new Temp<>();
    return fromEntries(
            groupConsecutiveIf(
                    Objects::equals,
                    newGroup,
                    (a, k, v) -> {
                      currentGroup.value = k;
                      groupAccumulator.accept(a, v);
                    },
                    a -> kv(currentGroup.value, a)));
  }

  private <A, R> Stream<R> groupConsecutiveIf(
          BiPredicate<? super K, ? super K> sameGroup,
          Supplier<? extends A> newRun,
          BiAccumulator<? super A, ? super K, ? super V> groupAccumulator,
          Function<? super A, ? extends R> groupFinisher) {
    requireNonNull(sameGroup);
    requireNonNull(newRun);
    requireNonNull(groupAccumulator);
    requireNonNull(groupFinisher);
    final int characteristics = Spliterator.ORDERED | Spliterator.DISTINCT;

    class Runner extends AbstractSpliterator<R> implements BiConsumer<K, V> {
      private final BiIterator<K, V> iterator = iterator();
      private K previousKey;
      private A currentRun = null;
      private boolean hasRunResult = false;
      private R runResult;

      Runner() {
        super(Long.MAX_VALUE, characteristics);
      }

      @Override
      public boolean tryAdvance(Consumer<? super R> action) {
        while (iterator.tryAdvance(this)) {
          if (hasRunResult) {
            action.accept(runResult);
            hasRunResult = false;
            return true;
          }
        }
        if (currentRun == null) {
          return false;
        }
        // The last run.
        stop();
        currentRun = null; // Be idempotent
        action.accept(runResult);
        return true;
      }

      @Override
      public void accept(K key, V value) {
        if (currentRun == null) {
          start();
        } else if (!sameGroup.test(previousKey, key)) {
          stop();
          start();
        }
        groupAccumulator.accumulate(currentRun, key, value);
        previousKey = key;
      }

      private void start() {
        currentRun = requireNonNull(newRun.get());
      }

      private void stop() {
        runResult = groupFinisher.apply(currentRun);
        hasRunResult = true;
      }
    }
    return StreamSupport.stream(Runner::new, characteristics, NOT_PARALLEL);
  }

  static <K, V> Map.Entry<K, V> kv(K key, V value) {
    return new AbstractMap.SimpleImmutableEntry<>(key, value);
  }

  /** A group has at least 1 member, with 2nd+ members incrementally reduced by {@code reducer}. */
  static <T> Collector<T, ?, T> reducingGroupMembers(BinaryOperator<T> reducer) {
    return collectingAndThen(Collectors.reducing(requireNonNull(reducer)), Optional::get);
  }

  private static <T> Stream<T> nullToEmpty(Stream<T> stream) {
    return stream == null ? Stream.empty() : stream;
  }

  private static <T> T orElseNull(Optional<T> optional) {
    return optional.orElse(null);
  }

  /**
   * An implementation that operates on a generic entry type {@code <E>} using two functions to
   * extract the 'key' and 'value' from each entry.
   *
   * <p>Because the {@code toKey} and {@code toValue} functions could be arbitrary custom functions
   * that are expensive or even with side-effects, it is strictly guaranteed that for any single
   * entry in the stream, each function is invoked exactly once.
   *
   * <p>Common methods like {@link #mapKeys(Function)}, {@link #mapValues(Function)}, {@link #keys},
   * {@link #values}, {@link #forEach} etc. can avoid allocating intermediary {@link Map.Entry}
   * instances by either invoking {@code toKey} and {@code toValue} then using the results directly,
   * or composing the functions to be invoked later.
   *
   * <p>Doing so isn't always feasible. For example {@link #filter(BiPredicate)} and {@link #peek} both need to
   * evaluate the entry by invoking {@code toKey} and {@code toValue} and the return values need to
   * be stored to avoid invoking the functions again. For these cases, the stream will be
   * degeneralized into {@code Stream<Map.Entry<K, V>>} so as to guarantee the "at-most-once"
   * semantic.
   */
  private static class GenericEntryStream<E, K, V> extends BiStream<K, V> {
    private final Stream<E> underlying;
    private final Function<? super E, ? extends K> toKey;
    private final Function<? super E, ? extends V> toValue;

    GenericEntryStream(
            Stream<E> underlying,
            Function<? super E, ? extends K> toKey,
            Function<? super E, ? extends V> toValue) {
      this.underlying = requireNonNull(underlying);
      this.toKey = requireNonNull(toKey);
      this.toValue = requireNonNull(toValue);
    }

    @Override public final <T> Stream<T> mapToObj(BiFunction<? super K, ? super V, ? extends T> mapper) {
      return underlying.map(forEntry(mapper));
    }

    @Override public final DoubleStream mapToDouble(ToDoubleBiFunction<? super K, ? super V> mapper) {
      requireNonNull(mapper);
      return underlying.mapToDouble(e -> mapper.applyAsDouble(toKey.apply(e), toValue.apply(e)));
    }

    @Override public final IntStream mapToInt(ToIntBiFunction<? super K, ? super V> mapper) {
      requireNonNull(mapper);
      return underlying.mapToInt(e -> mapper.applyAsInt(toKey.apply(e), toValue.apply(e)));
    }

    @Override public final LongStream mapToLong(ToLongBiFunction<? super K, ? super V> mapper) {
      requireNonNull(mapper);
      return underlying.mapToLong(e -> mapper.applyAsLong(toKey.apply(e), toValue.apply(e)));
    }

    @Override public final <K2> BiStream<K2, V> mapKeys(Function<? super K, ? extends K2> keyMapper) {
      return from(underlying, toKey.andThen(keyMapper), toValue);
    }

    @Override public final <V2> BiStream<K, V2> mapValues(Function<? super V, ? extends V2> valueMapper) {
      return from(underlying, toKey, toValue.andThen(valueMapper));
    }

    @Override public final BiStream<V, K> inverse() {
      return from(underlying, toValue, toKey);
    }

    @Override public final void forEach(BiConsumer<? super K, ? super V> action) {
      requireNonNull(action);
      underlying.forEach(e -> action.accept(toKey.apply(e), toValue.apply(e)));
    }

    @Override public final void forEachOrdered(BiConsumer<? super K, ? super V> action) {
      requireNonNull(action);
      underlying.forEachOrdered(e -> action.accept(toKey.apply(e), toValue.apply(e)));
    }

    @Override public final boolean allMatch(BiPredicate<? super K, ? super V> predicate) {
      requireNonNull(predicate);
      return underlying.allMatch(e -> predicate.test(toKey.apply(e), toValue.apply(e)));
    }

    @Override public final boolean anyMatch(BiPredicate<? super K, ? super V> predicate) {
      requireNonNull(predicate);
      return underlying.anyMatch(e -> predicate.test(toKey.apply(e), toValue.apply(e)));
    }

    @Override public BiStream<K, V> limit(int maxSize) {
      return from(underlying.limit(maxSize), toKey, toValue);
    }

    @Override public BiStream<K, V> skip(int n) {
      return from(underlying.skip(n), toKey, toValue);
    }

    @Override public final <R> R collect(BiCollector<? super K, ? super V, R> collector) {
      return underlying.collect(collector.collectorOf(toKey::apply, toValue::apply));
    }

    @Override public final <A> A collect(A container, BiAccumulator<? super A, ? super K, ? super V> accumulator) {
      requireNonNull(accumulator);
      underlying
              .sequential()
              .forEachOrdered(e -> accumulator.accumulate(container, toKey.apply(e), toValue.apply(e)));
      return container;
    }

    @Override public final void close() {
      underlying.close();
    }

    @Override public final BiIterator<K, V> iterator() {
      Spliterator<E> spliterator = underlying.spliterator();
      Temp<E> temp = new Temp<>();
      return consumer -> {
        boolean advanced = spliterator.tryAdvance(temp);
        if (advanced) {
          E entry = temp.value;
          consumer.accept(toKey.apply(entry), toValue.apply(entry));
        }
        return advanced;
      };
    }

    final <T> Function<E, T> forEntry(BiFunction<? super K, ? super V, T> function) {
      requireNonNull(function);
      return e -> function.apply(toKey.apply(e), toValue.apply(e));
    }
  }

  private static final class ZippingStream<K, V> extends BiStream<K, V> {
    private final Stream<K> left;
    private final Stream<V> right;

    ZippingStream(Stream<K> left, Stream<V> right) {
      this.left = requireNonNull(left);
      this.right = requireNonNull(right);
    }

    @Override public <T> Stream<T> mapToObj(BiFunction<? super K, ? super V, ? extends T> mapper) {
      requireNonNull(mapper);
      return stream(() -> new Spliteration().<T>ofObj(mapper), ORDERED, NOT_PARALLEL)
              .onClose(left::close)
              .onClose(right::close);
    }

    @Override public DoubleStream mapToDouble(ToDoubleBiFunction<? super K, ? super V> mapper) {
      requireNonNull(mapper);
      return doubleStream(() -> new Spliteration().ofDouble(mapper), ORDERED, NOT_PARALLEL)
              .onClose(left::close)
              .onClose(right::close);
    }

    @Override public IntStream mapToInt(ToIntBiFunction<? super K, ? super V> mapper) {
      requireNonNull(mapper);
      return intStream(() -> new Spliteration().ofInt(mapper), ORDERED, NOT_PARALLEL)
              .onClose(left::close)
              .onClose(right::close);
    }

    @Override public LongStream mapToLong(ToLongBiFunction<? super K, ? super V> mapper) {
      requireNonNull(mapper);
      return longStream(() -> new Spliteration().ofLong(mapper), ORDERED, NOT_PARALLEL)
              .onClose(left::close)
              .onClose(right::close);
    }

    @Override public <K2> BiStream<K2, V> mapKeys(Function<? super K, ? extends K2> keyMapper) {
      return zip(left.map(keyMapper), right);
    }

    @Override public <V2> BiStream<K, V2> mapValues(Function<? super V, ? extends V2> valueMapper) {
      return zip(left, right.map(valueMapper));
    }

    @Override public BiStream<V, K> inverse() {
      return zip(right, left);
    }

    @Override public void forEach(BiConsumer<? super K, ? super V> action) {
      forEachOrdered(action);
    }

    @Override public void forEachOrdered(BiConsumer<? super K, ? super V> action) {
      requireNonNull(action);
      new Spliteration().forEach(action);
    }

    @Override public boolean allMatch(BiPredicate<? super K, ? super V> predicate) {
      requireNonNull(predicate);
      return new Spliteration().any(false, predicate); // any false means false
    }

    @Override public boolean anyMatch(BiPredicate<? super K, ? super V> predicate) {
      requireNonNull(predicate);
      return new Spliteration().any(true, predicate); // any true means true
    }

    @Override public BiStream<K, V> limit(int maxSize) {
      return zip(left.limit(maxSize), right.limit(maxSize));
    }

    @Override public BiStream<K, V> skip(int n) {
      return zip(left.skip(n), right.skip(n));
    }

    @Override public <R> R collect(BiCollector<? super K, ? super V, R> collector) {
      requireNonNull(collector);
      return new Spliteration().collectWith(collector);
    }

    @Override public final <A> A collect(A container, BiAccumulator<? super A, ? super K, ? super V> accumulator) {
      forEach(accumulator.into(container));
      return container;
    }

    @Override public final void close() {
      try (Stream<K> closeLeft = left) {
        right.close();
      }
    }

    @Override public final BiIterator<K, V> iterator() {
      Spliterator<K> leftSpliterator = left.spliterator();
      Spliterator<V> rightSpliterator = right.spliterator();
      Temp<K> tempLeft = new Temp<>();
      Temp<V> tempRight = new Temp<>();
      return consumer -> {
        boolean advanced = leftSpliterator.tryAdvance(tempLeft) && rightSpliterator.tryAdvance(tempRight);
        if (advanced) {
          consumer.accept(tempLeft.value, tempRight.value);
        }
        return advanced;
      };
    }

    private final class Spliteration {
      private final Temp<K> currentLeft = new Temp<>();
      private final Temp<V> currentRight = new Temp<>();
      private final Spliterator<K> leftIt = left.spliterator();
      private final Spliterator<V> rightIt = right.spliterator();

      /**
       * Returns {@code dominatingResult} if {@code predicate} evaluates to {@code dominatingResult}
       * for any pair, or else returns {@code !dominatingResult}.
       */
      boolean any(boolean dominatingResult, BiPredicate<? super K, ? super V> predicate) {
        while (advance()) {
          if (predicate.test(currentLeft.value, currentRight.value) == dominatingResult) {
            return dominatingResult;
          }
        }
        return !dominatingResult;
      }

      void forEach(BiConsumer<? super K, ? super V> consumer) {
        while (advance()) {
          consumer.accept(currentLeft.value, currentRight.value);
        }
      }

      <T> Spliterator<T> ofObj(BiFunction<? super K, ? super V, ? extends T> mapper) {
        return new AbstractSpliterator<T>(estimateSize(), ORDERED) {
          @Override public boolean tryAdvance(Consumer<? super T> consumer) {
            return advance() && emit(mapper.apply(currentLeft.value, currentRight.value), consumer);
          }
        };
      }

      Spliterator.OfInt ofInt(ToIntBiFunction<? super K, ? super V> mapper) {
        return new AbstractIntSpliterator(estimateSize(), ORDERED) {
          @Override public boolean tryAdvance(IntConsumer consumer) {
            return advance()
                    && emit(mapper.applyAsInt(currentLeft.value, currentRight.value), consumer);
          }
        };
      }

      Spliterator.OfLong ofLong(ToLongBiFunction<? super K, ? super V> mapper) {
        return new AbstractLongSpliterator(estimateSize(), ORDERED) {
          @Override public boolean tryAdvance(LongConsumer consumer) {
            return advance()
                    && emit(mapper.applyAsLong(currentLeft.value, currentRight.value), consumer);
          }
        };
      }

      Spliterator.OfDouble ofDouble(ToDoubleBiFunction<? super K, ? super V> mapper) {
        return new AbstractDoubleSpliterator(estimateSize(), ORDERED) {
          @Override public boolean tryAdvance(DoubleConsumer consumer) {
            return advance()
                    && emit(mapper.applyAsDouble(currentLeft.value, currentRight.value), consumer);
          }
        };
      }

      <R> R collectWith(BiCollector<? super K, ? super V, R> collector) {
        return collectWith(collector.collectorOf(x -> currentLeft.value, x -> currentRight.value));
      }

      /** {@code collector} internally reads from {@link #currentLeft} and {@link #currentRight}. */
      private <A, R> R collectWith(Collector<Void, A, R> collector) {
        A container = collector.supplier().get();
        BiConsumer<A, Void> accumulator = collector.accumulator();
        while (advance()) {
          accumulator.accept(container, null);
        }
        return collector.finisher().apply(container);
      }

      private boolean advance() {
        return leftIt.tryAdvance(currentLeft) && rightIt.tryAdvance(currentRight);
      }

      private long estimateSize() {
        return Math.min(leftIt.estimateSize(), rightIt.estimateSize());
      }
    }

    private static boolean emit(int result, IntConsumer consumer) {
      consumer.accept(result);
      return true;
    }

    private static boolean emit(long result, LongConsumer consumer) {
      consumer.accept(result);
      return true;
    }

    private static boolean emit(double result, DoubleConsumer consumer) {
      consumer.accept(result);
      return true;
    }

    private static <T> boolean emit(T result, Consumer<? super T> consumer) {
      consumer.accept(result);
      return true;
    }
  }

  static final class Temp<T> implements Consumer<T> {
    T value;

    @Override public void accept(T value) {
      this.value = value;
    }
  }

  static <T> T left(Both<T, ?> both) {
    return both.andThen((l, r) -> l);
  }

  static <T> T right(Both<?, T> both) {
    return both.andThen((l, r) -> r);
  }

  private static <K, V> BiOptional<K, V> fromOptionalEntry(
          Optional<? extends Map.Entry<? extends K, ? extends V>> optional) {
    return optional.isPresent()
            ? BiOptional.of(optional.get().getKey(), optional.get().getValue())
            : BiOptional.empty();
  }

  private BiStream() {}
}
