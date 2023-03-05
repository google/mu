package com.google.mu.util;

import static com.google.mu.util.InternalCollectors.toImmutableList;
import static com.google.mu.util.Optionals.optional;
import static com.google.mu.util.Substring.before;
import static com.google.mu.util.Substring.first;
import static com.google.mu.util.Substring.suffix;
import static com.google.mu.util.stream.MoreCollectors.combining;
import static com.google.mu.util.stream.MoreCollectors.onlyElement;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Stream;

import com.google.mu.function.Quarternary;
import com.google.mu.function.Quinary;
import com.google.mu.function.Senary;
import com.google.mu.function.Ternary;
import com.google.mu.util.stream.MoreStreams;

/**
 * A (lossy) reverse operation of {@link String#format} to extract placeholder values from input
 * strings according to a format string. For example:
 *
 * <pre>{@code
 * return new StringFormat("{address}+{subaddress}@{domain}")
 *     .parse("my-account+test@gmail.com", (address, subaddress, domain) -> ...);
 * }</pre>
 *
 * <p>Note that except the placeholders, characters in the format string are treated as literals.
 * This works better if your pattern is close to free-form text with characters like '.', '?', '(',
 * '|' and whatnot because you don't need to escape them. On the other hand, the literal characters
 * won't offer regex functionalities you get from {@code (\w+)}, {@code (foo|bar)} etc.
 *
 * <p>In the face of ambiguity, the {@code parse()} methods can be lossy. Consider the format string
 * of {@code String.format("I bought %s and %s", "apples and oranges", "chips")}, it returns {@code
 * "I bought apples and oranges and chips"}; but the following parsing code will incorrectly parse
 * "apples" as "{fruits}" and "oranges and chips" as "{snacks}":
 *
 * <pre>{@code
 * new StringFormat("I bought {fruits} and {snacks}")
 *     .parse("I bought apples and oranges and chips", (fruits, snacks) -> ...);
 * }</pre>
 *
 * As such, only use this class on trusted input strings (i.e. not user inputs). And use regex
 * instead to better deal with ambiguity.
 *
 * <p>All the {@code parse()} methods attempt to match the entire input string from beginning to
 * end. If you need to find the string format as a substring anywhere inside the input string, or
 * need to find repeated occurrences from the input string, use the {@code scan()} methods instead.
 * Tack on {@code .findFirst()} on the returned lazy stream if you only care to find a single
 * occurrence.
 *
 * <p>This class is immutable and pre-compiles the format string at constructor time so that the
 * {@code parse()} and {@code scan()} methods will be more efficient.
 *
 * @since 6.6
 */
public final class StringFormat {
  private final String format;
  private final List<String> literals; // The string literals between placeholders

  /**
   * Constructs a StringFormat with placeholders in the syntax of {@code "{foo}"}. For example:
   *
   * <pre>{@code
   * new StringFormat("Dear {customer}, your confirmation number is {conf#}");
   * }</pre>
   *
   * <p>Nesting "{placeholder}" syntax inside literal curly braces is supported. For example, you
   * could use a format like: {@code "{name: {name}, age: {age}}"}, and it will be able to parse
   * record-like strings such as "{name: Joe, age: 25}".
   *
   * @param format the template format with placeholders
   * @throws IllegalArgumentException if {@code format} is invalid
   *     (e.g. a placeholder immediately followed by another placeholder)
   */
  public StringFormat(String format) {
    this.format = format;
    this.literals =
        Substring.consecutive(c -> c != '{' && c != '}') // Find the inner-most pairs of curly braces.
            .immediatelyBetween("{", "}")
            .repeatedly()
            .split(format)
            .map(
                literal ->
                    // Format "{key:{k}, value:{v}}" will split into ["{key:{", "}, value:{", "}}"].
                    // Remove the leading "}" for all except the first split results, then remove
                    // the trailing '{' for all except the last split results. The result is the
                    // exact literals around {k} and {v}: ["{key:", ", value:", "}"].
                    literal.skip(
                        literal.index() == 0 ? 0 : 1,
                        literal.index() + literal.length() == format.length() ? 0 : 1))
            .map(Substring.Match::toString)
            .collect(toImmutableList());
    for (int i = 1; i < numPlaceholders(); i++) {
      if (literals.get(i).isEmpty()) {
        throw new IllegalArgumentException("Placeholders cannot be next to each other: " + format);
      }
    }
  }

  /**
   * Parses {@code input} and applies the {@code mapper} function with the single placeholder value
   * in this string format.
   *
   * <p>For example: <pre>{@code
   * new StringFormat("Job failed (job id: {job_id})").parse(input, jobId -> ...);
   * }</pre>
   *
   * @return the return value of the {@code mapper} function if not null. Returns empty if
   *     {@code input} doesn't match the format, or {@code mapper} returns null.
   * @throws IllegalArgumentException if or the format string doesn't have exactly one placeholder.
   */
  public <R> Optional<R> parse(String input, Function<? super String, ? extends R> mapper) {
    requireNonNull(input);
    requireNonNull(mapper);
    checkPlaceholderCount(1);
    return parseAndCollect(input, onlyElement(mapper));
  }

  /**
   * Parses {@code input} and applies {@code mapper} with the two placeholder values
   * in this string format.
   *
   * <p>For example: <pre>{@code
   * new StringFormat("Job failed (job id: '{id}', error code: {code})")
   *     .parse(input, (jobId, errorCode) -> ...);
   * }</pre>
   *
   * @return the return value of the {@code mapper} function if not null. Returns empty if
   *     {@code input} doesn't match the format, or {@code mapper} returns null.
   * @throws IllegalArgumentException if or the format string doesn't have exactly two placeholders.
   */
  public <R> Optional<R> parse(
      String input, BiFunction<? super String, ? super String, ? extends R> mapper) {
    requireNonNull(input);
    requireNonNull(mapper);
    checkPlaceholderCount(2);
    return parseAndCollect(input, combining(mapper));
  }

  /**
   * Similar to {@link #parse(String, BiFunction)}, but parses {@code input} and applies {@code
   * mapper} with the <em>3</em> placeholder values in this string format.
   *
   * <p>For example: <pre>{@code
   * new StringFormat("Job failed (job id: '{job_id}', error code: {code}, error details: {details})")
   *     .parse(input, (jobId, errorCode, errorDetails) -> ...);
   * }</pre>
   *
   * @return the return value of the {@code mapper} function if not null. Returns empty if
   *     {@code input} doesn't match the format, or {@code mapper} returns null.
   * @throws IllegalArgumentException if or the format string doesn't have exactly 3 placeholders.
   */
  public <R> Optional<R> parse(String input, Ternary<? super String, ? extends R> mapper) {
    requireNonNull(input);
    requireNonNull(mapper);
    checkPlaceholderCount(3);
    return parseAndCollect(input, combining(mapper));
  }

  /**
   * Similar to {@link #parse(String, BiFunction)}, but parses {@code input} and applies {@code
   * mapper} with the <em>4</em> placeholder values in this string format.
   *
   * @return the return value of the {@code mapper} function if not null. Returns empty if
   *     {@code input} doesn't match the format, or {@code mapper} returns null.
   * @throws IllegalArgumentException if or the format string doesn't have exactly 4 placeholders.
   */
  public <R> Optional<R> parse(String input, Quarternary<? super String, ? extends R> mapper) {
    requireNonNull(input);
    requireNonNull(mapper);
    checkPlaceholderCount(4);
    return parseAndCollect(input, combining(mapper));
  }

  /**
   * Similar to {@link #parse(String, BiFunction)}, but parses {@code input} and applies {@code
   * mapper} with the <em>5</em> placeholder values in this string format.
   *
   * @return the return value of the {@code mapper} function if not null. Returns empty if
   *     {@code input} doesn't match the format, or {@code mapper} returns null.
   * @throws IllegalArgumentException if or the format string doesn't have exactly 5 placeholders.
   */
  public <R> Optional<R> parse(String input, Quinary<? super String, ? extends R> mapper) {
    requireNonNull(input);
    requireNonNull(mapper);
    checkPlaceholderCount(5);
    return parseAndCollect(input, combining(mapper));
  }

  /**
   * Similar to {@link #parse(String, BiFunction)}, but parses {@code input} and applies {@code
   * mapper} with the <em>6</em> placeholder values in this string format.
   *
   * @return the return value of the {@code mapper} function if not null. Returns empty if
   *     {@code input} doesn't match the format, or {@code mapper} returns null.
   * @throws IllegalArgumentException if or the format string doesn't have exactly 6 placeholders.
   */
  public <R> Optional<R> parse(String input, Senary<? super String, ? extends R> mapper) {
    requireNonNull(input);
    requireNonNull(mapper);
    checkPlaceholderCount(6);
    return parseAndCollect(input, combining(mapper));
  }

  /**
   * Parses {@code input} against the pattern.
   *
   * <p>Returns an immutable list of placeholder values in the same order as {@link #placeholders},
   * upon success; otherwise returns empty.
   *
   * <p>The {@link Substring.Match} result type allows caller to inspect the characters around each
   * match, or to access the raw index in the input string.
   */
  public Optional<List<Substring.Match>> parse(String input) {
    if (!input.startsWith(literals.get(0))) {  // first literal is the prefix
      return Optional.empty();
    }
    final int numPlaceholders = numPlaceholders();
    List<Substring.Match> builder = new ArrayList<>(numPlaceholders);
    int inputIndex = literals.get(0).length();
    for (int i = 1; i <= numPlaceholders; i++) {
      // subsequent literals are searched left-to-right; last literal is the suffix.
      Substring.Pattern trailingLiteral =
          i < numPlaceholders ? first(literals.get(i)) : suffix(literals.get(i));
      Substring.Match placeholder = before(trailingLiteral).match(input, inputIndex);
      if (placeholder == null) {
        return Optional.empty();
      }
      builder.add(placeholder);
      inputIndex = placeholder.index() + placeholder.length() + literals.get(i).length();
    }
    return optional(inputIndex == input.length(), unmodifiableList(builder));
  }

  /**
   * Scans the {@code input} string and extracts all matched placeholders in this string format.
   *
   * <p>unlike {@link #parse(String)}, the input string isn't matched entirely:
   * the pattern doesn't have to start from the beginning, and if there are some remaining
   * characters that don't match the pattern any more, the stream stops. In particular, if there
   * is no match, empty stream is returned.
   */
  public Stream<List<Substring.Match>> scan(String input) {
    requireNonNull(input);
    int numPlaceholders = numPlaceholders();
    return MoreStreams.whileNotNull(
        new Supplier<List<Substring.Match>>() {
          private int inputIndex = 0;
          private boolean done = false;

          @Override public List<Substring.Match> get() {
            if (done) {
              return null;
            }
            inputIndex = input.indexOf(literals.get(0), inputIndex);
            if (inputIndex < 0) {
              return null;
            }
            inputIndex += literals.get(0).length();
            List<Substring.Match> builder = new ArrayList<>(numPlaceholders);
            for (int i = 1; i <= numPlaceholders; i++) {
              String literal = literals.get(i);
              // Always search left-to-right. The last placeholder at the end of format is suffix.
              Substring.Pattern literalLocator =
                  i == numPlaceholders && literals.get(i).isEmpty()
                      ? Substring.END
                      : first(literals.get(i));
              Substring.Match placeholder = before(literalLocator).match(input, inputIndex);
              if (placeholder == null) {
                return null;
              }
              builder.add(placeholder);
              inputIndex = placeholder.index() + placeholder.length() + literal.length();
            }
            if (inputIndex == input.length()) {
              done = true;
            }
            return unmodifiableList(builder);
          }
        });
  }

  /**
   * Scans the {@code input} string and extracts all matches of this string format.
   * Returns the lazy stream of non-null results from passing the single placeholder values to
   * the {@code mapper} function for each iteration, with null results skipped.
   *
   * <p>For example: <pre>{@code
   * new StringFormat("/home/usr/myname/{file_name}\n")
   *     .scan(multiLineInput, fileName -> ...);
   * }</pre>
   *
   * <p>unlike {@link #parse(String, Function)}, the input string isn't matched
   * entirely: the pattern doesn't have to start from the beginning, and if there are some remaining
   * characters that don't match the pattern any more, the stream stops. In particular, if there
   * is no match, empty stream is returned.
   *
   * <p>By default, placeholders are allowed to be matched against an empty string. If the
   * placeholder isn't expected to be empty, consider filtering it out by returning null from
   * the {@code mapper} function, which will then be ignored in the result stream.
   */
  public <R> Stream<R> scan(String input, Function<? super String, ? extends R> mapper) {
    requireNonNull(input);
    requireNonNull(mapper);
    checkPlaceholderCount(1);
    return scanAndCollect(input, onlyElement(mapper));
  }

  /**
   * Scans the {@code input} string and extracts all matches of this string format.
   * Returns the lazy stream of non-null results from passing the two placeholder values to
   * the {@code mapper} function for each iteration, with null results skipped.
   *
   * <p>For example: <pre>{@code
   * new StringFormat("[key={key}, value={value}]")
   *     .repeatedly()
   *     .parse(input, (key, value) -> ...);
   * }</pre>
   *
   * <p>unlike {@link #parse(String, BiFunction)}, the input string isn't matched
   * entirely: the pattern doesn't have to start from the beginning, and if there are some remaining
   * characters that don't match the pattern any more, the stream stops. In particular, if there
   * is no match, empty stream is returned.
   *
   * <p>By default, placeholders are allowed to be matched against an empty string. If a certain
   * placeholder isn't expected to be empty, consider filtering it out by returning null from
   * the {@code mapper} function, which will then be ignored in the result stream.
   */
  public <R> Stream<R> scan(
      String input, BiFunction<? super String, ? super String, ? extends R> mapper) {
    requireNonNull(input);
    requireNonNull(mapper);
    checkPlaceholderCount(2);
    return scanAndCollect(input, combining(mapper));
  }

  /**
   * Scans the {@code input} string and extracts all matches of this string format.
   * Returns the lazy stream of non-null results from passing the 3 placeholder values to
   * the {@code mapper} function for each iteration, with null results skipped.
   *
   * <p>For example: <pre>{@code
   * new StringFormat("[{lhs} + {rhs} = {result}]")
   *     .repeatedly()
   *     .parse(input, (lhs, rhs, result) -> ...);
   * }</pre>
   *
   * <p>unlike {@link #parse(String, Ternary)}, the input string isn't matched
   * entirely: the pattern doesn't have to start from the beginning, and if there are some remaining
   * characters that don't match the pattern any more, the stream stops. In particular, if there
   * is no match, empty stream is returned.
   *
   * <p>By default, placeholders are allowed to be matched against an empty string. If a certain
   * placeholder isn't expected to be empty, consider filtering it out by returning null from
   * the {@code mapper} function, which will then be ignored in the result stream.
   */
  public <R> Stream<R> scan(String input, Ternary<? super String, ? extends R> mapper) {
    requireNonNull(input);
    requireNonNull(mapper);
    checkPlaceholderCount(3);
    return scanAndCollect(input, combining(mapper));
  }

  /**
   * Scans the {@code input} string and extracts all matches of this string format.
   * Returns the lazy stream of non-null results from passing the 4 placeholder values to
   * the {@code mapper} function for each iteration, with null results skipped.
   *
   * <p>unlike {@link #parse(String, Quarternary)}, the input string isn't matched
   * entirely: the pattern doesn't have to start from the beginning, and if there are some remaining
   * characters that don't match the pattern any more, the stream stops. In particular, if there
   * is no match, empty stream is returned.
   *
   * <p>By default, placeholders are allowed to be matched against an empty string. If a certain
   * placeholder isn't expected to be empty, consider filtering it out by returning null from
   * the {@code mapper} function, which will then be ignored in the result stream.
   */
  public <R> Stream<R> scan(String input, Quarternary<? super String, ? extends R> mapper) {
    requireNonNull(input);
    requireNonNull(mapper);
    checkPlaceholderCount(4);
    return scanAndCollect(input, combining(mapper));
  }

  /**
   * Scans the {@code input} string and extracts all matches of this string format.
   * Returns the lazy stream of non-null results from passing the 5 placeholder values to
   * the {@code mapper} function for each iteration, with null results skipped.
   *
   * <p>unlike {@link #parse(String, Quinary)}, the input string isn't matched
   * entirely: the pattern doesn't have to start from the beginning, and if there are some remaining
   * characters that don't match the pattern any more, the stream stops. In particular, if there
   * is no match, empty stream is returned.
   *
   * <p>By default, placeholders are allowed to be matched against an empty string. If a certain
   * placeholder isn't expected to be empty, consider filtering it out by returning null from
   * the {@code mapper} function, which will then be ignored in the result stream.
   */
  public <R> Stream<R> scan(String input, Quinary<? super String, ? extends R> mapper) {
    requireNonNull(input);
    requireNonNull(mapper);
    checkPlaceholderCount(5);
    return scanAndCollect(input, combining(mapper));
  }

  /**
   * Scans the {@code input} string and extracts all matches of this string format.
   * Returns the lazy stream of non-null results from passing the 6 placeholder values to
   * the {@code mapper} function for each iteration, with null results skipped.
   *
   * <p>unlike {@link #parse(String, Senary)}, the input string isn't matched
   * entirely: the pattern doesn't have to start from the beginning, and if there are some remaining
   * characters that don't match the pattern any more, the stream stops. In particular, if there
   * is no match, empty stream is returned.
   *
   * <p>By default, placeholders are allowed to be matched against an empty string. If a certain
   * placeholder isn't expected to be empty, consider filtering it out by returning null from
   * the {@code mapper} function, which will then be ignored in the result stream.
   */
  public <R> Stream<R> scan(String input, Senary<? super String, ? extends R> mapper) {
    requireNonNull(input);
    requireNonNull(mapper);
    checkPlaceholderCount(6);
    return scanAndCollect(input, combining(mapper));
  }

  /** Returns the string format. */
  @Override public String toString() {
    return format;
  }

  private <R> Optional<R> parseAndCollect(String input, Collector<? super String, ?, R> collector) {
    return parse(input).map(values -> values.stream().map(Substring.Match::toString).collect(collector));
  }

  private <R> Stream<R> scanAndCollect(String input, Collector<? super String, ?, R> collector) {
    return scan(input)
        .map(values -> values.stream().map(Substring.Match::toString).collect(collector))
        .filter(v -> v != null);
  }

  private int numPlaceholders() {
    return literals.size() - 1;
  }

  private void checkPlaceholderCount(int expected) {
    if (numPlaceholders() != expected) {
      throw new IllegalArgumentException(
          String.format(
              "format string has %s placeholders; %s expected.",
              numPlaceholders(),
              expected));
    }
  }
}
