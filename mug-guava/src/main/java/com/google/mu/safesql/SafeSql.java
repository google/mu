package com.google.mu.safesql;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;
import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.mapping;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import java.util.stream.Collector;
import java.util.stream.Collector.Characteristics;

import com.google.errorprone.annotations.CompileTimeConstant;
import com.google.mu.annotations.TemplateFormatMethod;
import com.google.mu.annotations.TemplateString;
import com.google.mu.util.StringFormat;
import com.google.mu.util.StringFormat.Template;
import com.google.mu.util.stream.BiStream;
import com.google.mu.util.stream.MoreStreams;

/**
 * An injection-safe parameterized SQL, constructed using compile-time enforced templates and can be
 * used to create {@link java.sql.PreparedStatement}.
 *
 * <p>This class is intended to work with JDBC {@link Connection} and {@link PreparedStatement} API
 * with parameters set through the {@link PreparedStatement#setObject(int, Object) setObject()} method.
 *
 * <p>In contrast, {@link SafeQuery} directly escapes string parameters and is intended for SQL engines
 * that don't provide native safe parameterized queries support.
 *
 * @since 8.2
 */
public final class SafeSql {
  private final String sql;
  private final List<?> paramValues;

  private SafeSql(String sql) {
    this(sql, emptyList());
  }

  private SafeSql(String sql, List<?> paramValues) {
    this.sql = sql;
    this.paramValues = paramValues;
  }

  /** An empty SQL */
  public static SafeSql EMPTY = new SafeSql("");

  @TemplateFormatMethod
  public static SafeSql of(@CompileTimeConstant @TemplateString String sql) {
    return new SafeSql(validate(sql));
  }

  /**
   * Convenience method when you need to create the {@link SafeSql} inline, with both the
   * query template and the arguments.
   *
   * <p>For example:
   *
   * <pre>{@code
   * PreparedStatement statement = SafeSql.of("select * from JOBS where id = {id}", jobId).prepare(connection);
   * }</pre>
   */
  @SuppressWarnings("StringFormatArgsCheck") // protected by @TemplateFormatMethod
  @TemplateFormatMethod
  public static SafeSql of(@CompileTimeConstant @TemplateString String query, Object... args) {
    return template(query).with(args);
  }

  /**
   * An optional query that's only rendered if {@code condition} is true; otherwise returns {@link
   * #EMPTY}. It's for use cases where a subquery is only conditionally added, for example the
   * following query will only include the userEmail column under super user mode:
   *
   * <pre>{@code
   * SafeSql query = SafeSql.of(
   *     "SELECT job_id, start_timestamp {user_email} FROM jobs",
   *     SafeSql.when(isSuperUser, ", user_email"));
   * }</pre>
   */
  @TemplateFormatMethod
  @SuppressWarnings("StringFormatArgsCheck") // protected by @TemplateFormatMethod
  public static SafeSql when(
      boolean condition, @TemplateString @CompileTimeConstant String query, Object... args) {
    checkNotNull(query);
    checkNotNull(args);
    return condition ? of(query, args) : EMPTY;
  }

  /**
   * An optional query that's only rendered if {@code arg} is present; otherwise returns {@link
   * #EMPTY}. It's for use cases where a subquery is only added when present, for example the
   * following query will add the WHERE clause if the filter is present:
   *
   * <pre>{@code
   * SafeSql query = SafeSql.of(
   *     "SELECT * FROM jobs {where}",
   *     SafeSql.optionally("WHERE {filter}", getOptionalFilter()));
   * }</pre>
   */
  @TemplateFormatMethod
  @SuppressWarnings("StringFormatArgsCheck") // protected by @TemplateFormatMethod
  public static SafeSql optionally(
      @TemplateString @CompileTimeConstant String query, Optional<?> arg) {
    checkNotNull(query);
    return arg.map(v -> of(query, v)).orElse(EMPTY);
  }

  /**
   * Returns a template of {@link SafeSql} based on the {@code template} string.
   *
   * <p>For example:
   *
   * <pre>{@code
   * private static final Template<SafeSql> GET_JOB_IDS_BY_QUERY =
   *     SafeSql.template(
   *         """
   *         SELECT job_id from Jobs
   *         WHERE query LIKE '%{keyword}%'
   *         """);
   *
   * PreparedStatement stmt = GET_JOB_IDS_BY_QUERY.with("sensitive word").prepareStatement(conn);
   * }</pre>
   *
   * <p>Except {@link SafeSql} itself, which are directly substituted into the query, all
   * other placeholder arguments are passed into the PreparedStatement as query parameters.
   */
  public static Template<SafeSql> template(@CompileTimeConstant String template) {
    return StringFormat.template(
        template,
        (fragments, placeholders) -> {
          Iterator<String> it = fragments.iterator();
          return placeholders
              .collect(
                  new Builder(),
                  (builder, placeholder, value) -> {
                    builder.appendSql(it.next());
                    String paramName = placeholder.skip(1, 1).toString().trim();
                    if (value instanceof SafeSql) {
                      validate(paramName);
                      builder.addSubQuery((SafeSql) value);
                    } else {
                      checkArgument(!(value instanceof SafeQuery), "Don't mix SafeQuery with SafeSql.");
                      builder.appendPlaceholder(paramName);
                      builder.addParameter(value);
                    }
                  })
              .appendSql(it.next())
              .build();
        });
  }

  /**
   * A collector that joins boolean query snippets using {@code AND} operator. The
   * AND'ed sub-queries will be enclosed in pairs of parenthesis to avoid
   * ambiguity. If the input is empty, the result will be "TRUE".
   *
   * <p>Empty SafeSql elements are ignored and not joined.
   */
  public static Collector<SafeSql, ?, SafeSql> and() {
    return collectingAndThen(
        nonEmptyQueries(mapping(SafeSql::parenthesized, joining(" AND "))),
        query -> query.sql.isEmpty() ? of("1 = 1") : query);
  }

  /**
   * A collector that joins boolean query snippets using {@code OR} operator. The
   * OR'ed sub-queries will be enclosed in pairs of parenthesis to avoid
   * ambiguity. If the input is empty, the result will be "FALSE".
   *
   * <p>Empty SafeSql elements are ignored and not joined.
   */
  public static Collector<SafeSql, ?, SafeSql> or() {
    return collectingAndThen(
        nonEmptyQueries(mapping(SafeSql::parenthesized, joining(" OR "))),
        query -> query.sql.isEmpty() ? of("1 = 0") : query);
  }

  /**
   * Returns a collector that joins SafeSql elements using {@code delimiter}.
   *
   * <p>Useful if you need to parameterize by a set of columns to select. Say, you might need to
   * query the table names only, or read the project, dataset and table names:
   *
   * <pre>{@code
   * private static final Template<SafeSql> QUERY_TABLES =
   *     SafeSql.template("SELECT {columns} FROM {dataset}.INFORMATION_SCHEMA.TABLES");
   *
   * SafeSql getTableNames = QUERY_TABLES.with(SafeSql.of("table_name"));
   * SafeSql getFullyQualified = QUERY_TABLES.with(
   *     Stream.of("table_catalog", "table_schema", "table_name")
   *         .map(SafeSql::of)
   *         .collect(SafeSql.joining(", ")),
   *     SafeSql.of("my-dataset"));
   * }</pre>
   *
   * <p>Empty SafeSql elements are ignored and not joined.
   */
  public static Collector<SafeSql, ?, SafeSql> joining(@CompileTimeConstant String delimiter) {
    validate(delimiter);
    return Collector.of(
        Builder::new,
        (b, q) -> {
          if (!q.sql.isEmpty()) {  // ignore empty
            b.appendDelimiter(delimiter).addSubQuery(q);
          }
        },
        (b1, b2) -> b1.appendDelimiter(delimiter).addSubQuery(b2.build()),
        Builder::build);
  }

  /**
   * Returns a {@link PreparedStatement} with the encapsulated sql and parameters.
   *
   * @throws UncheckedSqlException wraps {@link SQLException} if failed
   */
  public PreparedStatement prepareStatement(Connection connection) {
    try {
      PreparedStatement statement = connection.prepareStatement(sql);
      setArgs(statement);
      return statement;
    } catch (SQLException e) {
      throw new UncheckedSqlException(e);
    }
  }

  /**
   * Returns a {@link CallableStatement} with the encapsulated sql and parameters.
   *
   * @throws UncheckedSqlException wraps {@link SQLException} if failed
   */
  public CallableStatement prepareCall(Connection connection) {
    try {
      CallableStatement statement = connection.prepareCall(sql);
      setArgs(statement);
      return statement;
    } catch (SQLException e) {
      throw new UncheckedSqlException(e);
    }
  }

  /** Returns the parameter values in the order they occur in the sql. */
  public List<?> getParameters() {
    return paramValues;
  }

  /** Returns the SQL text that can be used to create {@link PreparedStatement}. */
  @Override
  public String toString() {
    return sql;
  }

  @Override
  public int hashCode() {
    return Objects.hash(sql, paramValues);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof SafeSql) {
      SafeSql that = (SafeSql) obj;
      return sql.equals(that.sql)
          && paramValues.equals(that.paramValues);
    }
    return false;
  }

  private void setArgs(PreparedStatement statement) {
    BiStream.zip(MoreStreams.indexesFrom(1), paramValues.stream())
        .forEach((index, value) -> {
          try {
            statement.setObject(index, value);
          } catch (SQLException e) {
            throw new UncheckedSqlException(e);
          }
        });
  }

  private static String validate(String sql) {
    checkArgument(sql.indexOf('?') < 0, "please use named {placeholder} instead of '?'");
    return sql;
  }

  private SafeSql parenthesized() {
    return new SafeSql("(" + sql + ")", paramValues);
  }

  private static <R> Collector<SafeSql, ?, R> nonEmptyQueries(
      Collector<SafeSql, ?, R> downstream) {
    return filtering(q -> !q.sql.isEmpty(), downstream);
  }

  // Not in Java 8
  private static <T, A, R> Collector<T, A, R> filtering(
      Predicate<? super T> filter, Collector<? super T, A, R> collector) {
    BiConsumer<A, ? super T> accumulator = collector.accumulator();
    return Collector.of(
        collector.supplier(),
        (a, input) -> {if (filter.test(input)) {accumulator.accept(a, input);}},
        collector.combiner(),
        collector.finisher(),
        collector.characteristics().toArray(new Characteristics[0]));
  }

  private static final class Builder {
    private final StringBuilder queryText = new StringBuilder();
    private final List<Object> paramValues = new ArrayList<>();

    Builder appendSql(String snippet) {
      queryText.append(validate(snippet));
      return this;
    }

    Builder appendPlaceholder(String name) {
      validate(name);
      queryText.append("?");
      return this;
    }

    Builder appendDelimiter(String delim) {
      if (queryText.length() > 0) {
        queryText.append(delim);
      }
      return this;
    }

    Builder addSubQuery(SafeSql subQuery) {
      queryText.append(subQuery.sql);
      subQuery.getParameters().forEach(this::addParameter);
      return this;
    }

    Builder addParameter(Object value) {
      paramValues.add(value);
      return this;
    }

    SafeSql build() {
      return new SafeSql(queryText.toString(), unmodifiableList(new ArrayList<>(paramValues)));
    }
  }
}
