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

import static com.google.mu.util.stream.BiCollectors.toMap;
import static java.util.Objects.requireNonNull;

import java.util.AbstractMap;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Spliterator;
import java.util.Spliterators.AbstractSpliterator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.google.mu.function.CheckedConsumer;
import com.google.mu.function.DualValuedFunction;
import com.google.mu.util.Both;

/**
 * Static utilities pertaining to {@link Stream} and {@link Collector} in addition to relevant
 * utilities in Jdk and Guava.
 *
 * @since 1.1
 */
public final class MoreStreams {
  /**
   * Returns a Stream produced by iterative application of {@code step} to the initial
   * {@code seed}, producing a Stream consisting of seed, elements of step(seed),
   * elements of step(x) for each x in step(seed), etc.
   * (If the result stream returned by the {@code step} function is null an empty stream is used,
   * instead.)
   *
   * <p>While {@code Stream.generate(supplier)} can be used to generate infinite streams,
   * it's not as easy to generate a <em>finite</em> stream unless the size can be pre-determined.
   * This method can be used to generate finite streams: just return an empty stream when the
   * {@code step} determines that there's no more elements to be generated.
   *
   * <p>A typical group of use cases are BFS traversal algorithms.
   * For example, to stream the tree nodes in BFS order: <pre>{@code
   *   Stream<Node> bfs(Node root) {
   *     return generate(root, node -> node.children().stream());
   *   }
   * }</pre>
   *
   * It's functionally equivalent to the following common imperative code: <pre>{@code
   *   List<Node> bfs(Node root) {
   *     List<Node> result = new ArrayList<>();
   *     Queue<Node> queue = new ArrayDeque<>();
   *     queue.add(root);
   *     while (!queue.isEmpty()) {
   *       Node node = queue.remove();
   *       result.add(node);
   *       queue.addAll(node.children());
   *     }
   *     return result;
   *   }
   * }</pre>
   *
   * A BFS 2-D grid traversal algorithm: <pre>{@code
   *   Stream<Cell> bfs(Cell startingCell) {
   *     Set<Cell> visited = new HashSet<>();
   *     visited.add(startingCell);
   *     return generate(startingCell, c -> c.neighbors().filter(visited::add));
   *   }
   * }</pre>
   *
   * <p>At every step, 0, 1 or more elements can be generated into the resulting stream.
   * As discussed above, returning an empty stream leads to eventual termination of the stream;
   * returning 1-element stream is equivalent to {@code Stream.generate(supplier)};
   * while returning more than one elements allows a single element to fan out to multiple
   * elements.
   *
   * @since 1.9
   */
  public static <T> Stream<T> generate(
      T seed, Function<? super T, ? extends Stream<? extends T>> step) {
    requireNonNull(step);
    Queue<Stream<? extends T>> queue = new ArrayDeque<>();
    queue.add(Stream.of(seed));
    return whileNotNull(queue::poll)
        .flatMap(seeds -> withSideEffect(
            seeds,
            v -> {
              Stream<? extends T> fanout = step.apply(v);
              if (fanout != null) {
                queue.add(fanout);
              }
            }));
  }

  /**
   * Flattens {@code streamOfStream} and returns an unordered sequential stream of the nested
   * elements.
   *
   * <p>Logically, {@code stream.flatMap(fanOut)} is equivalent to
   * {@code MoreStreams.flatten(stream.map(fanOut))}.
   * Due to this <a href="https://bugs.openjdk.java.net/browse/JDK-8075939">JDK bug</a>,
   * {@code flatMap()} uses {@code forEach()} internally and doesn't support short-circuiting for
   * the passed-in stream. {@code flatten()} supports short-circuiting and can be used to
   * flatten infinite streams.
   *
   * @since 1.9
   */
  public static <T> Stream<T> flatten(Stream<? extends Stream<? extends T>> streamOfStream) {
    return mapBySpliterator(streamOfStream.sequential(), 0, FlattenedSpliterator<T>::new);
  }

  /**
   * Iterates through {@code stream} <em>only once</em>. It's strongly recommended
   * to avoid assigning the return value to a variable or passing it to any other method because
   * the returned {@code Iterable}'s {@link Iterable#iterator iterator()} method can only be called
   * once. Instead, always use it together with a for-each loop, as in:
   *
   * <pre>{@code
   *   for (Foo foo : iterateOnce(stream)) {
   *     ...
   *     if (...) continue;
   *     if (...) break;
   *     ...
   *   }
   * }</pre>
   *
   * The above is equivalent to manually doing:
   *
   * <pre>{@code
   *   Iterable<Foo> foos = stream::iterator;
   *   for (Foo foo : foos) {
   *     ...
   *   }
   * }</pre>
   * except using this API eliminates the need for a named variable that escapes the scope of the
   * for-each loop. And code is more readable too.
   *
   * <p>Note that {@link #iterateThrough iterateThrough()} should be preferred whenever possible
   * due to the caveats mentioned above. This method is still useful when the loop body needs to
   * use control flows such as {@code break} or {@code return}.
   */
  public static <T> Iterable<T> iterateOnce(Stream<T> stream) {
    return stream::iterator;
  }

  /**
   * Iterates through {@code stream} sequentially and passes each element to {@code consumer}
   * with exceptions propagated. For example:
   *
   * <pre>{@code
   *   void writeAll(Stream<?> stream, ObjectOutput out) throws IOException {
   *     iterateThrough(stream, out::writeObject);
   *   }
   * }</pre>
   */
  public static <T, E extends Throwable> void iterateThrough(
      Stream<? extends T> stream, CheckedConsumer<? super T, E> consumer) throws E {
    requireNonNull(consumer);
    for (T element : iterateOnce(stream)) {
      consumer.accept(element);
    }
  }

  /**
   * Dices {@code stream} into smaller chunks each with up to {@code maxSize} elements.
   *
   * <p>For a sequential stream, the first N-1 chunk's will contain exactly {@code maxSize}
   * elements and the last chunk may contain less (but never 0).
   * However for parallel streams, it's possible that the stream is split in roughly equal-sized
   * sub streams before being diced into smaller chunks, which then will result in more than one
   * chunks with less than {@code maxSize} elements.
   *
   * <p>This is an <a href="https://docs.oracle.com/javase/8/docs/api/java/util/stream/package-summary.html#StreamOps">
   * intermediary operation</a>.
   *
   * @param stream the source stream to be diced
   * @param maxSize the maximum size for each chunk
   * @return Stream of diced chunks each being a list of size up to {@code maxSize}
   * @throws IllegalStateException if {@code maxSize <= 0}
   */
  public static <T> Stream<List<T>> dice(Stream<? extends T> stream, int maxSize) {
    requireNonNull(stream);
    if (maxSize <= 0) throw new IllegalArgumentException();
    return mapBySpliterator(stream, Spliterator.NONNULL, it -> dice(it, maxSize));
  }

  /**
   * Dices {@code spliterator} into smaller chunks each with up to {@code maxSize} elements.
   *
   * @param spliterator the source spliterator to be diced
   * @param maxSize the maximum size for each chunk
   * @return Spliterator of diced chunks each being a list of size up to {@code maxSize}
   * @throws IllegalStateException if {@code maxSize <= 0}
   */
  public static <T> Spliterator<List<T>> dice(Spliterator<? extends T> spliterator, int maxSize) {
    requireNonNull(spliterator);
    if (maxSize <= 0) throw new IllegalArgumentException();
    return new DicedSpliterator<T>(spliterator, maxSize);
  }

  /** @deprecated Use {@code maps.collect(flatteningMaps(toMap())} instead. */
  @Deprecated
  public static <K, V> Collector<Map<K, V>, ?, Map<K, V>> uniqueKeys() {
    return flattening(Map::entrySet, toMap());
  }

  /**
   * Analogous to {@link Collectors#mapping Collectors.mapping()}, applies a mapping function to
   * each input element before accumulation, except that the {@code mapper} function returns a
   * <em><b>pair of elements</b></em>, which are then accumulated by a <em>BiCollector</em>.
   *
   * <p>For example, you can parse key-value pairs in the form of "k1=v1,k2=v2" with:
   *
   * <pre>{@code
   * Substring.first(',')
   *     .delimit("k1=v2,k2=v2")
   *     .collect(
   *         mapping(
   *             s -> first('=').split(s).orElseThrow(...),
   *             toImmutableSetMultimap()));
   * }</pre>
   *
   * @since 5.1
   */
  public static <T, A, B, R> Collector<T, ?, R> mapping(
      Function<? super T, ? extends Both<? extends A, ? extends B>> mapper,
      BiCollector<A, B, R> downstream) {
    Function<? super T, Map.Entry<A, B>> toEntry =
        mapper.andThen(b -> b.mapToObj(AbstractMap.SimpleImmutableEntry::new));
    return Collectors.mapping(
        toEntry, downstream.splitting(Map.Entry::getKey, Map.Entry::getValue));
  }

  /**
   * Collects the pairs of input elements using the {@code downstream} BiCollector.
   *
   * <p>For example, you can parse key-value pairs in the form of "k1=v1,k2=v2" with:
   *
   * <pre>{@code
   * Substring.first(',')
   *     .delimit("k1=v2,k2=v2")
   *     .map(s -> first('=').split(s).orElseThrow(...))
   *     .collect(fromPairs(toImmutableSetMultimap()));
   * }</pre>
   *
   * @since 5.1
   */
  public static <A, B, R> Collector<Both<A, B>, ?, R> fromPairs(
      BiCollector<? super A, ? super B, R> downstream) {
    return mapping(Both::mapToObj, downstream);
  }

  /**
   * Similar but slightly different than {@link Collectors#flatMapping}, returns a {@link Collector}
   * that first flattens the input stream of <em>pairs</em> (as opposed to single elements) and then
   * collects the flattened pairs with the {@code downstream} BiCollector.
   *
   * @since 4.8
   */
  public static <T, K, V, R> Collector<T, ?, R> flatMapping(
      Function<? super T, ? extends BiStream<? extends K, ? extends V>> flattener,
      BiCollector<K, V, R> downstream) {
    return BiStream.flatMapping(
        flattener.andThen(BiStream::mapToEntry),
        downstream.<Map.Entry<? extends K, ? extends V>>splitting(
            Map.Entry::getKey, Map.Entry::getValue));
  }

  /**
   * @since 3.6
   * @deprecated If you need to flatten a stream of Multimap, use something like {@code
   * flatMapping(m -> BiStream.from(m.asMap()), flatteningToImmutableSetMultimap())}.
   */
  @Deprecated
  public static <T, K, V, R> Collector<T, ?, R> flattening(
      Function<? super T, ? extends Collection<? extends Map.Entry<? extends K, ? extends V>>> flattener,
      BiCollector<K, V, R> downstream) {
    return flatMapping(flattener.andThen(BiStream::from), downstream);
  }

  /**
   * Returns a {@code Collector} that flattens the input {@link Map} entries and collects them using
   * the {@code downstream} BiCollector.
   *
   * <p>For example, you can flatten a list of multimaps:
   *
   * <pre>{@code
   * ImmutableMap<EmployeeId, Task> billableTaskAssignments = projects.stream()
   *     .map(Project::getTaskAssignments)
   *     .collect(flatteningMaps(ImmutableMap::toImmutableMap)));
   * }</pre>
   *
   * <p>To flatten a stream of multimaps, use {@link #flattening}.
   *
   * @since 4.6
   */
  public static <K, V, R> Collector<Map<K, V>, ?, R> flatteningMaps(
      BiCollector<K, V, R> downstream) {
    return flatMapping(BiStream::from, downstream);
  }

  /**
   * Returns a collector that collects input elements into a list, which is then arranged by the
   * {@code arranger} function before being wrapped as <em>immutable</em> list result.
   * List elements are not allowed to be null.
   *
   * <p>Example usages: <ul>
   * <li>{@code stream.collect(toListAndThen(Collections::reverse))} to collect to reverse order.
   * <li>{@code stream.collect(toListAndThen(Collections::shuffle))} to collect and shuffle.
   * <li>{@code stream.collect(toListAndThen(Collections::sort))} to collect and sort.
   * </ul>
   *
   * @since 4.2
   */
  public static <T> Collector<T, ?, List<T>> toListAndThen(Consumer<? super List<T>> arranger) {
    requireNonNull(arranger);
    Collector<T, ?, List<T>> rejectingNulls =
        Collectors.mapping(Objects::requireNonNull, Collectors.toCollection(ArrayList::new));
    return Collectors.collectingAndThen(rejectingNulls, list -> {
      arranger.accept(list);
      return Collections.unmodifiableList(list);
    });
  }

  /**
   * Returns an infinite {@link Stream} starting from {@code firstIndex}.
   * Can be used together with {@link BiStream#zip} to iterate over a stream with index.
   * For example: {@code zip(indexesFrom(0), values)}.
   *
   * <p>To get a finite stream, use {@code indexesFrom(...).limit(size)}.
   *
   * <p>Note that while {@code indexesFrom(0)} will eventually incur boxing cost for every integer,
   * the JVM typically pre-caches small {@code Integer} instances (by default up to 127).
   *
   * @since 3.7
   */
  public static Stream<Integer> indexesFrom(int firstIndex) {
    return IntStream.iterate(firstIndex, i -> i + 1).boxed();
  }

  /**
   * Similar to {@link Stream#generate}, returns an infinite, sequential, unordered, and non-null
   * stream where each element is generated by the provided Supplier. The stream however will
   * terminate as soon as the Supplier returns null, in which case the null is treated as the
   * terminal condition and doesn't constitute a stream element.
   *
   * <p>For sequential iterations, {@code whileNotNll()} is usually more concise than implementing
   * {@link AbstractSpliterator} directly. The latter requires boilerplate that looks like this:
   *
   * <pre>{@code
   * return StreamSupport.stream(
   *     new AbstractSpliterator<T>(MAX_VALUE, NONNULL) {
   *       public boolean tryAdvance(Consumer<? super T> action) {
   *         if (hasData) {
   *           action.accept(data);
   *           return true;
   *         }
   *         return false;
   *       }
   *     }, false);
   * }</pre>
   *
   * Which is equivalent to the following one-liner using {@code whileNotNull()}:
   *
   * <pre>{@code
   * return whileNotNull(() -> hasData ? data : null);
   * }</pre>
   *
   * <p>Why null? Why not {@code Optional}? Wrapping every generated element of a stream in an
   * {@link Optional} carries considerable allocation cost. Also, while nulls are in general
   * discouraged, they are mainly a problem for users who have to remember to deal with them.
   * The stream returned by {@code whileNotNull()} on the other hand is guaranteed to never include
   * nulls that users have to worry about.
   *
   * <p>If you already have an {@code Optional} from a method return value, you can use {@code
   * whileNotNull(() -> optionalReturningMethod().orElse(null))}.
   *
   * <p>One may still need to implement {@code AbstractSpliterator} or {@link java.util.Iterator}
   * directly if null is a valid element (usually discouraged though).
   *
   * <p>If you have an imperative loop over a mutable queue or stack:
   *
   * <pre>{@code
   * while (!queue.isEmpty()) {
   *   int num = queue.poll();
   *   if (someCondition) {
   *     ...
   *   }
   * }
   * }</pre>
   *
   * it can be turned into a stream using {@code whileNotNull()}:
   *
   * <pre>{@code
   * whileNotNull(queue::poll).filter(someCondition)...
   * }</pre>
   *
   * @since 4.1
   */
  public static <T> Stream<T> whileNotNull(Supplier<? extends T> supplier) {
    requireNonNull(supplier);
    return StreamSupport.stream(
        new AbstractSpliterator<T>(Long.MAX_VALUE, Spliterator.NONNULL) {
          @Override public boolean tryAdvance(Consumer<? super T> action) {
            T element = supplier.get();
            if (element == null) return false;
            action.accept(element);
            return true;
          }
        }, false);
  }

  /**
   * Returns a sequential stream with {@code sideEfect} attached on every element.
   *
   * <p>Unlike {@link Stream#peek}, which should only be used for debugging purpose,
   * the side effect is allowed to interfere with the source of the stream, and is
   * guaranteed to be applied in encounter order.
   *
   * <p>If you have to resort to side effects, use this dedicated method instead of {@code peek()}
   * or any other stream method. From the API specification, all methods defined by {@link Stream}
   * are expected to be stateless, and should not cause or depend on side effects, because even for
   * ordered, sequential streams, only the order of output is defined, not the order of evaluation.
   *
   * @since 4.9
   */
  public static <T> Stream<T> withSideEffect(Stream<T> stream, Consumer<? super T> sideEffect) {
    requireNonNull(stream);
    requireNonNull(sideEffect);
    return StreamSupport.stream(() -> withSideEffect(stream.spliterator(), sideEffect), 0, false);
  }

  private static <T> Spliterator<T> withSideEffect(
      Spliterator<T> spliterator, Consumer<? super T> sideEffect) {
    return new AbstractSpliterator<T>(spliterator.estimateSize(), 0) {
      @Override public boolean tryAdvance(Consumer<? super T> action) {
        return spliterator.tryAdvance(e -> {
          sideEffect.accept(e);
          action.accept(e);
        });
      }
    };
  }

  /**
   * Analogous to {@link Collectors#mapping Collectors.mapping()}, applies a mapping function to
   * each input element before accumulation, except that the {@code mapper} function returns a
   * <em><b>pair of elements</b></em>, which are then accumulated by a <em>BiCollector</em>.
   *
   * @since 4.6
   */
  public static <T, K, V, R> Collector<T, ?, R> mapping(
      DualValuedFunction<? super T, ? extends K, ? extends V> mapper,
      BiCollector<K, V, R> downstream) {
    Function<? super T, Map.Entry<K, V>> toEntry =
        mapper.andThen(AbstractMap.SimpleImmutableEntry::new);
    return Collectors.mapping(
        toEntry, downstream.splitting(Map.Entry::getKey, Map.Entry::getValue));
  }

  /**
   * Returns a collector that first copies all input elements into a new {@code Stream} and then
   * passes the stream to the {@code finisher} function, which translates it to the final result.
   */
  static <T, R> Collector<T, ?, R> streaming(Function<Stream<T>, R> finisher) {
    return Collectors.collectingAndThen(toStream(), finisher);
  }

  static <F, T> Stream<T> mapBySpliterator(
      Stream<F> stream, int characteristics,
      Function<? super Spliterator<F>, ? extends Spliterator<T>> mapper) {
    requireNonNull(mapper);
    Stream<T> mapped = StreamSupport.stream(
        () -> mapper.apply(stream.spliterator()), characteristics, stream.isParallel());
    mapped.onClose(stream::close);
    return mapped;
  }

  /** Copying input elements into another stream. */
  private static <T> Collector<T, ?, Stream<T>> toStream() {
    return Collector.of(
        Stream::<T>builder,
        Stream.Builder::add,
        (b1, b2) -> {
          b2.build().forEachOrdered(b1::add);
          return b1;
        },
        Stream.Builder::build);
  }

  private static <F, T> T splitThenWrap(
      Spliterator<F> from, Function<? super Spliterator<F>, ? extends T> wrapper) {
    Spliterator<F> it = from.trySplit();
    return it == null ? null : wrapper.apply(it);
  }

  private static final class DicedSpliterator<T> implements Spliterator<List<T>> {
    private final Spliterator<? extends T> underlying;
    private final int maxSize;

    DicedSpliterator(Spliterator<? extends T> underlying, int maxSize) {
      this.underlying = requireNonNull(underlying);
      this.maxSize = maxSize;
    }

    @Override public boolean tryAdvance(Consumer<? super List<T>> action) {
      requireNonNull(action);
      List<T> chunk = new ArrayList<>(chunkSize());
      for (int i = 0; i < maxSize && underlying.tryAdvance(chunk::add); i++) {}
      if (chunk.isEmpty()) return false;
      action.accept(chunk);
      return true;
    }

    @Override public Spliterator<List<T>> trySplit() {
      return splitThenWrap(underlying, it -> new DicedSpliterator<>(it, maxSize));
    }

    @Override public long estimateSize() {
      long size = underlying.estimateSize();
      return size == Long.MAX_VALUE ? Long.MAX_VALUE : estimateChunks(size);
    }

    @Override public long getExactSizeIfKnown() {
      return -1;
    }

    @Override public int characteristics() {
      return Spliterator.NONNULL;
    }

    private int chunkSize() {
      long estimate = underlying.estimateSize();
      if (estimate <= maxSize) return (int) estimate;
      // The user could set a large chunk size for an unknown-size stream, don't blow up memory.
      return estimate == Long.MAX_VALUE ? Math.min(maxSize, 8192) : maxSize;
    }

    private long estimateChunks(long size) {
      long lower = size / maxSize;
      return lower + ((size % maxSize == 0) ? 0 : 1);
    }
  }

  private static final class FlattenedSpliterator<T> implements Spliterator<T> {
    private final Spliterator<? extends Stream<? extends T>> blocks;
    private Spliterator<? extends T> currentBlock;
    private final Consumer<Stream<? extends T>> nextBlock = block -> {
      currentBlock = block.spliterator();
    };

    FlattenedSpliterator(Spliterator<? extends Stream<? extends T>> blocks) {
      this.blocks = requireNonNull(blocks);
    }

    private FlattenedSpliterator(
        Spliterator<? extends T> currentBlock, Spliterator<? extends Stream<? extends T>> blocks) {
      this.blocks = requireNonNull(blocks);
      this.currentBlock = currentBlock;
    }

    @Override public boolean tryAdvance(Consumer<? super T> action) {
      requireNonNull(action);
      if (currentBlock == null && !tryAdvanceBlock()) {
        return false;
      }
      boolean advanced = false;
      while ((!(advanced = currentBlock.tryAdvance(action))) && tryAdvanceBlock()) {}
      return advanced;
    }

    @Override public Spliterator<T> trySplit() {
      return splitThenWrap(blocks, it -> {
        Spliterator<T> result = new FlattenedSpliterator<>(currentBlock, it);
        currentBlock = null;
        return result;
      });
    }

    @Override public long estimateSize() {
      return Long.MAX_VALUE;
    }

    @Override public long getExactSizeIfKnown() {
      return -1;
    }

    @Override public int characteristics() {
      // While we maintain encounter order as long as 'blocks' does, returning an ordered stream
      // (which can be infinite) could surprise users when the user does things like
      // "parallel().limit(n)". It's sufficient for normal use cases to respect encounter order
      // without reporting order-ness.
      return 0;
    }

    private boolean tryAdvanceBlock() {
      return blocks.tryAdvance(nextBlock);
    }
  }

  private MoreStreams() {}
}
