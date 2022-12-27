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
package com.google.mu.function;

import static com.google.common.truth.Truth.assertThat;
import static com.google.mu.function.BiComparator.comparing;
import static com.google.mu.function.BiComparator.comparingDouble;
import static com.google.mu.function.BiComparator.comparingInt;
import static com.google.mu.function.BiComparator.comparingKey;
import static com.google.mu.function.BiComparator.comparingLong;
import static com.google.mu.function.BiComparator.comparingValue;
import static java.util.Comparator.naturalOrder;
import static java.util.Comparator.reverseOrder;
import static java.util.Objects.requireNonNull;

import java.util.Comparator;
import java.util.function.Function;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.common.testing.NullPointerTester;

@RunWith(JUnit4.class)
public class BiComparatorTest {
  @Test public void comparingByFunction() {
    assertThat(comparing(Integer::sum).compare(1, 3, 2, 2)).isEqualTo(0);
    assertThat(comparing(Integer::sum).compare(1, 3, 0, 5)).isLessThan(0);
    assertThat(comparing(Integer::sum).compare(1, 4, 2, 2)).isGreaterThan(0);
  }

  @Test public void comparingIntFunction() {
    assertThat(comparingInt(Integer::sum).compare(1, 3, 2, 2)).isEqualTo(0);
    assertThat(comparingInt(Integer::sum).compare(1, 3, 0, 5)).isLessThan(0);
    assertThat(comparingInt(Integer::sum).compare(1, 4, 2, 2)).isGreaterThan(0);
  }

  @Test public void comparingLongFunction() {
    assertThat(comparingLong(Long::sum).compare(1L, 3L, 2L, 2L)).isEqualTo(0);
    assertThat(comparingLong(Long::sum).compare(1L, 3L, 0L, 5L)).isLessThan(0);
    assertThat(comparingLong(Long::sum).compare(1L, 4L, 2L, 2L)).isGreaterThan(0);
  }

  @Test public void comparingDoubleFunction() {
    assertThat(comparingDouble(Double::sum).compare(1D, 3D, 2D, 2D)).isEqualTo(0);
    assertThat(comparingDouble(Double::sum).compare(1D, 3D, 0D, 5D)).isLessThan(0);
    assertThat(comparingDouble(Double::sum).compare(1D, 4D, 2D, 2D)).isGreaterThan(0);
  }

  @Test public void comparingKeyByFunction() {
    assertThat(comparingKey(Object::toString).compare(1, 3, 2, 2)).isLessThan(0);
    assertThat(comparingKey(Object::toString).compare(2, 3, 2, 2)).isEqualTo(0);
    assertThat(comparingKey(Object::toString).compare(3, 3, 2, 2)).isGreaterThan(0);
  }

  @Test public void comparingKeyByComparator() {
    assertThat(BiComparator.<Integer>comparingKey(reverseOrder()).compare(1, "", 2, null))
        .isGreaterThan(0);
    assertThat(BiComparator.<Integer>comparingKey(reverseOrder()).compare(2, "", 2, null))
        .isEqualTo(0);
    assertThat(BiComparator.<Integer>comparingKey(reverseOrder()).compare(2, "", 1, null))
        .isLessThan(0);
  }

  @Test public void comparingValueByFunction() {
    assertThat(comparingValue(Object::toString).compare(1, 3, 2, 2)).isGreaterThan(0);
    assertThat(comparingValue(Object::toString).compare(1, 2, 2, 2)).isEqualTo(0);
    assertThat(comparingValue(Object::toString).compare(5, 3, 2, 4)).isLessThan(0);
  }

  @Test public void comparingValueByComparator() {
    assertThat(BiComparator.<Integer>comparingValue(reverseOrder()).compare("", 1, null, 2))
        .isGreaterThan(0);
    assertThat(BiComparator.<Integer>comparingValue(reverseOrder()).compare("", 1, null, 1))
        .isEqualTo(0);
    assertThat(BiComparator.<Integer>comparingValue(reverseOrder()).compare("", 1, null, 0))
        .isLessThan(0);
  }

  @Test public void thenComparingByFunction_subtype() {
    BiComparator<Integer, Integer> ordering =
        comparingKey(Object::toString).then(comparing(Integer::sum));
    assertThat(ordering.compare(1, 2, 3, 0)).isLessThan(0);
    assertThat(ordering.compare(3, 2, 1, 0)).isGreaterThan(0);
    assertThat(ordering.compare(1, 2, 1, 0)).isGreaterThan(0);
    assertThat(ordering.compare(1, 2, 1, 3)).isLessThan(0);
    assertThat(ordering.compare(1, 2, 1, 2)).isEqualTo(0);
  }

  @Test public void thenComparingByFunction_supertype() {
    BiComparator<Integer, Integer> ordering =
        BiComparator.<Integer>comparingKey(naturalOrder()).then(comparing((a, b) -> 0));
    assertThat(ordering.compare(1, 2, 3, 0)).isLessThan(0);
    assertThat(ordering.compare(3, 2, 1, 0)).isGreaterThan(0);
    assertThat(ordering.compare(1, 2, 1, 0)).isEqualTo(0);
    assertThat(ordering.compare(1, 2, 1, 3)).isEqualTo(0);
    assertThat(ordering.compare(1, 2, 1, 2)).isEqualTo(0);
  }

  @Test public void thenComparingKey() {
    BiComparator<Integer, Object> ordering =
        comparingValue(Object::toString).then(comparingKey(naturalOrder()));
    assertThat(ordering.compare(1, "a", 1, "b")).isLessThan(0);
    assertThat(ordering.compare(1, "a", 1, "a")).isEqualTo(0);
    assertThat(ordering.compare(2, "a", 1, "a")).isGreaterThan(0);
  }

  @Test public void thenComparingKey_byFunction() {
    BiComparator<Integer, Object> ordering =
        comparingValue(Object::toString).then(comparingKey(Integer::highestOneBit));
    assertThat(ordering.compare(1, "a", 1, "b")).isLessThan(0);
    assertThat(ordering.compare(2, "a", 3, "a")).isEqualTo(0);
    assertThat(ordering.compare(4, "a", 3, "a")).isGreaterThan(0);
  }

  @Test public void thenComparingValue() {
    BiComparator<Object, Integer> ordering =
        comparingKey(Object::toString).then(BiComparator.comparingValue(naturalOrder()));
    assertThat(ordering.compare("a", 1, "b", 1)).isLessThan(0);
    assertThat(ordering.compare("a", 1, "a", 1)).isEqualTo(0);
    assertThat(ordering.compare("a", 2, "a", 1)).isGreaterThan(0);
  }

  @Test public void thenComparingValue_byFunction() {
    BiComparator<Object, Integer> ordering =
        comparingKey(Object::toString).then(comparingValue(Integer::highestOneBit));
    assertThat(ordering.compare("a", 1, "b", 1)).isLessThan(0);
    assertThat(ordering.compare("a", 3, "a", 2)).isEqualTo(0);
    assertThat(ordering.compare("a", 4, "a", 3)).isGreaterThan(0);
  }

  @Test public void testComparingInOrder_primaryOnly() {
    BiComparator<?, ?> primary = comparingKey(Object::toString);
    assertThat(BiComparator.comparingInOrder(primary)).isSameAs(primary);
  }

  @Test public void testComparingInOrder_byValueThenByKey() {
    BiComparator<Integer, Object> ordering =
        BiComparator.comparingInOrder(comparingValue(Object::toString), comparingKey(Function.identity()));
    assertThat(ordering.compare(2, "a", 1, "b")).isLessThan(0);
    assertThat(ordering.compare(2, "a", 1, "a")).isGreaterThan(0);
    assertThat(ordering.compare(1, "a", 1, "a")).isEqualTo(0);
  }

  @Test public void reversed_comparing() {
    BiComparator<Object, Object> ordering = comparingKey(Object::toString).reversed();
    assertThat(ordering.compare("a", 1, "b", 1)).isGreaterThan(0);
    assertThat(ordering.compare("a", 3, "a", 2)).isEqualTo(0);
    assertThat(ordering.compare("b", 4, "a", 1)).isLessThan(0);
  }

  @Test public void reversed_reversed() {
    BiComparator<Object, Object> ordering = comparingKey(Object::toString);
    assertThat(ordering.reversed().reversed()).isSameAs(ordering);
  }

  @Test public void testNuls() {
    new NullPointerTester().testAllPublicStaticMethods(BiComparator.class);
    new NullPointerTester()
        .setDefault(Comparator.class, (a, b) -> {
          requireNonNull(a);
          requireNonNull(b);
          return -1;
        })
        .testAllPublicInstanceMethods(BiComparator.comparing((String x, String y) -> x.toString() + y.toString()));
  }
}
