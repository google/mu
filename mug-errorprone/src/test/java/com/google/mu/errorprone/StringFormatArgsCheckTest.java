package com.google.mu.errorprone;

import com.google.errorprone.CompilationTestHelper;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class StringFormatArgsCheckTest {
  private final CompilationTestHelper helper =
      CompilationTestHelper.newInstance(StringFormatArgsCheck.class, getClass());

  @Test
  public void goodFormat() {
    helper
        .addSourceLines(
            "Test.java",
            "import com.google.mu.util.StringFormat;",
            "class Test {",
            "  private static final StringFormat FORMAT =",
            "      new StringFormat(\"{foo}-{bar_id}-{camelCase}\");",
            "  void test(String foo, String barId, String camelCase) {",
            "    FORMAT.format(foo, barId, camelCase);",
            "  }",
            "}")
        .doTest();
  }

  @Test
  public void format_goodInlinedFormat() {
    helper
        .addSourceLines(
            "Test.java",
            "import com.google.mu.util.StringFormat;",
            "class Test {",
            "  void test(String foo, String barId, String camelCase) {",
            "    new StringFormat(\"{foo}-{bar_id}-{CamelCase}\").format(foo, barId, camelCase);",
            "  }",
            "}")
        .doTest();
  }

  @Test
  public void format_concatenatedInlinedFormat() {
    helper
        .addSourceLines(
            "Test.java",
            "import com.google.mu.util.StringFormat;",
            "class Test {",
            "  void test(String foo, String barId, String camelCase) {",
            "    new StringFormat(\"{foo}-{bar_id}\" + \"-{CamelCase}\")",
            "    .format(foo, barId, camelCase);",
            "  }",
            "}")
        .doTest();
  }

  @Test
  public void format_argsOutOfOrder() {
    helper
        .addSourceLines(
            "Test.java",
            "import com.google.mu.util.StringFormat;",
            "class Test {",
            "  private static final StringFormat FORMAT = new StringFormat(\"{foo}-{bar_id}\");",
            "  void test(String foo, String barId) {",
            "    // BUG: Diagnostic contains:",
            "    FORMAT.format(barId, foo);",
            "  }",
            "}")
        .doTest();
  }

  @Test
  public void format_i18nArgsOutOfOrder() {
    helper
        .addSourceLines(
            "Test.java",
            "import com.google.mu.util.StringFormat;",
            "class Test {",
            "  private static final StringFormat FORMAT = new StringFormat(\"{用户}-{地址}\");",
            "  void test() {",
            "    // BUG: Diagnostic contains:",
            "    FORMAT.format(\"地址\", \"用户\");",
            "  }",
            "}")
        .doTest();
  }

  @Test
  public void format_i18nArgsMatched() {
    helper
        .addSourceLines(
            "Test.java",
            "import com.google.mu.util.StringFormat;",
            "class Test {",
            "  private static final StringFormat FORMAT = new StringFormat(\"{用户}-{地址}\");",
            "  void test() {",
            "    FORMAT.format(\"用户\", \"地址\");",
            "  }",
            "}")
        .doTest();
  }

  @Test
  public void format_namedArgsCommented() {
    helper
        .addSourceLines(
            "Test.java",
            "import com.google.mu.util.StringFormat;",
            "class Test {",
            "  private static final StringFormat FORMAT =",
            "      new StringFormat(\"{foo}-{bar_id}-{camelCase}\");",
            "  void test(String foo, String barId, String camelCase) {",
            "    FORMAT.format(/*foo=*/ barId, /*barId=*/ camelCase, /*camelCase=*/ foo);",
            "  }",
            "}")
        .doTest();
  }

  @Test
  public void format_literalArgsCommented() {
    helper
        .addSourceLines(
            "Test.java",
            "import com.google.mu.util.StringFormat;",
            "class Test {",
            "  private static final StringFormat FORMAT =",
            "      new StringFormat(\"{foo}-{bar_id}-{CamelCase}\");",
            "  void test() {",
            "    FORMAT.format(/*foo=*/ 1, /*bar_id=*/ 2, /*camelCase=*/ 3);",
            "  }",
            "}")
        .doTest();
  }

  @Test
  public void format_argIsMethodInvocation() {
    helper
        .addSourceLines(
            "Test.java",
            "import com.google.mu.util.StringFormat;",
            "class Test {",
            "  private static final StringFormat FORMAT =",
            "      new StringFormat(\"{foo}-{bar_id}-{camelCased}\");",
            "  interface Bar {",
            "    String id();",
            "  }",
            "  interface Camel {",
            "    String cased();",
            "  }",
            "  void test(String foo, Bar bar, Camel camel) {",
            "    FORMAT.format(foo, bar.id(), camel.cased());",
            "  }",
            "}")
        .doTest();
  }

  @Test
  public void format_usingStringConstant_argsMatchPlaceholders() {
    helper
        .addSourceLines(
            "Test.java",
            "import com.google.mu.util.StringFormat;",
            "class Test {",
            "  private static final String FORMAT_STR =",
            "      \"{foo}-{bar_id}-{camelCased}\";",
            "  private static final StringFormat FORMAT =",
            "      new StringFormat(FORMAT_STR);",
            "  interface Bar {",
            "    String id();",
            "  }",
            "  interface Camel {",
            "    String cased();",
            "  }",
            "  void test(String foo, Bar bar, Camel camel) {",
            "    FORMAT.format(foo, bar.id(), camel.cased());",
            "  }",
            "}")
        .doTest();
  }

  @Test
  public void format_usingStringConstant_argsDoNotMatchPlaceholders() {
    helper
        .addSourceLines(
            "Test.java",
            "import com.google.mu.util.StringFormat;",
            "class Test {",
            "  private static final String FORMAT_STR =",
            "      \"{a}-{b}-{c}\";",
            "  void test() {",
            "    // BUG: Diagnostic contains:",
            "    new StringFormat(FORMAT_STR).format(1, 2, 3);",
            "  }",
            "}")
        .doTest();
  }

  @Test
  public void format_usingStringConstantConcatenated_argsDoNotMatchPlaceholders() {
    helper
        .addSourceLines(
            "Test.java",
            "import com.google.mu.util.StringFormat;",
            "class Test {",
            "  private static final String FORMAT_STR =",
            "      \"{a}-{b}-{c}\";",
            "  void test() {",
            "    // BUG: Diagnostic contains:",
            "    new StringFormat(FORMAT_STR + \"-{d}\").format(1, 2, 3, 4);",
            "  }",
            "}")
        .doTest();
  }

  @Test
  public void format_usingStringConstantInStringFormatConstant_argsDoNotMatchPlaceholders() {
    helper
        .addSourceLines(
            "Test.java",
            "import com.google.mu.util.StringFormat;",
            "class Test {",
            "  private static final String FORMAT_STR =",
            "      \"{a}-{b}\" + \"-{c}\";",
            "  private static final StringFormat FORMAT =",
            "      new StringFormat(FORMAT_STR);",
            "  void test() {",
            "    // BUG: Diagnostic contains:",
            "    FORMAT.format(1, 2, 3);",
            "  }",
            "}")
        .doTest();
  }

  @Test
  public void format_usingStringConstantInStringFormatConstant_argsCommented() {
    helper
        .addSourceLines(
            "Test.java",
            "import com.google.mu.util.StringFormat;",
            "class Test {",
            "  private static final String FORMAT_STR =",
            "      \"{a}-{b}\" + \"-{c}\";",
            "  private static final StringFormat FORMAT =",
            "      new StringFormat(FORMAT_STR);",
            "  void test() {",
            "    FORMAT.format(/* a */ 1, /* b */ 2, /* c */ 3);",
            "  }",
            "}")
        .doTest();
  }

  @Test
  public void format_getOrIsprefixIgnorableInArgs() {
    helper
        .addSourceLines(
            "Test.java",
            "import com.google.mu.util.StringFormat;",
            "class Test {",
            "  private static final StringFormat FORMAT =",
            "      new StringFormat(\"{foo}-{bar_id}-{camelCase}\");",
            "  interface Bar {",
            "    String getId();",
            "  }",
            "  interface Camel {",
            "    String isCase();",
            "  }",
            "  void test(String foo, Bar bar, Camel camel) {",
            "    FORMAT.format(foo, bar.getId(), camel.isCase());",
            "  }",
            "}")
        .doTest();
  }

  @Test
  public void format_argIsLiteral() {
    helper
        .addSourceLines(
            "Test.java",
            "import com.google.mu.util.StringFormat;",
            "class Test {",
            "  private static final StringFormat FORMAT = new StringFormat(\"{foo}-{bar_id}\");",
            "  void test() {",
            "    FORMAT.format(\"foo\", \"bar id\");",
            "  }",
            "}")
        .doTest();
  }

  @Test
  public void format_inlinedFormatDoesNotRequireArgNameMatchingForLiterals() {
    helper
        .addSourceLines(
            "Test.java",
            "import com.google.mu.util.StringFormat;",
            "class Test {",
            "  void test() {",
            "    new StringFormat(\"{foo}-{bar_id}\").format(1, 2);",
            "  }",
            "}")
        .doTest();
  }

  @Test
  public void format_3args_inlinedFormatRequiresArgNameMatchingForExpressions() {
    helper
        .addSourceLines(
            "Test.java",
            "import com.google.mu.util.StringFormat;",
            "class Test {",
            "  void test(String x, String y, String z) {",
            "    // BUG: Diagnostic contains:",
            "    new StringFormat(\"{foo}-{bar_id}-{z}\").format(x, y, z);",
            "  }",
            "}")
        .doTest();
  }

  @Test
  public void format_3args_inlinedFormatAllowsLiteralArgs() {
    helper
        .addSourceLines(
            "Test.java",
            "import com.google.mu.util.StringFormat;",
            "class Test {",
            "  void test() {",
            "    new StringFormat(\"{foo}-{bar_id}-{z}\").format(1, 2, \"a\" + \"3\");",
            "  }",
            "}")
        .doTest();
  }

  @Test
  public void format_2args_inlinedFormatRequiresArgNameMatch() {
    helper
        .addSourceLines(
            "Test.java",
            "import com.google.mu.util.StringFormat;",
            "class Test {",
            "  void test(String x, String y) {",
            "    // BUG: Diagnostic contains:",
            "    new StringFormat(\"{foo}-{bar_id}\").format(x, y);",
            "  }",
            "}")
        .doTest();
  }

  @Test
  public void format_2args_inlinedFormatWithMiscommentedLiteralArgs() {
    helper
        .addSourceLines(
            "Test.java",
            "import com.google.mu.util.StringFormat;",
            "class Test {",
            "  void test() {",
            "    // BUG: Diagnostic contains:",
            "    new StringFormat(\"{foo}-{bar_id}\").format(/* bar */ 1, 2);",
            "  }",
            "}")
        .doTest();
  }

  @Test
  public void format_2args_inlinedFormatWithLiteralArgs() {
    helper
        .addSourceLines(
            "Test.java",
            "import com.google.mu.util.StringFormat;",
            "class Test {",
            "  void test() {",
            "    new StringFormat(\"{foo}-{bar_id}\").format(1, 2);",
            "  }",
            "}")
        .doTest();
  }

  @Test
  public void format_2args_inlinedFormatWithCommentedLiteralArgs() {
    helper
        .addSourceLines(
            "Test.java",
            "import com.google.mu.util.StringFormat;",
            "class Test {",
            "  void test() {",
            "    new StringFormat(\"{foo}-{bar_id}\").format(/* foo */ 1, /* bar id */ 2);",
            "  }",
            "}")
        .doTest();
  }

  @Test
  public void format_parenthesizedInlinedFormatDoesNotRequireArgNameMatching() {
    helper
        .addSourceLines(
            "Test.java",
            "import com.google.mu.util.StringFormat;",
            "class Test {",
            "  void test() {",
            "    new StringFormat((\"{foo}-{bar_id}\")).format(1, 2);",
            "  }",
            "}")
        .doTest();
  }

  @Test
  public void format_inlinedFormatWithOutOfOrderLiteralArgs() {
    helper
        .addSourceLines(
            "Test.java",
            "import com.google.mu.util.StringFormat;",
            "class Test {",
            "  void test() {",
            "    // BUG: Diagnostic contains:",
            "    new StringFormat(\"{foo}-{bar_id}\").format(/*bar*/ 1, /*foo*/ 2);",
            "  }",
            "}")
        .doTest();
  }

  @Test
  public void format_inlinedFormatWithOutOfOrderNamedArgs() {
    helper
        .addSourceLines(
            "Test.java",
            "import com.google.mu.util.StringFormat;",
            "class Test {",
            "  void test(String bar, String foo) {",
            "    // BUG: Diagnostic contains:",
            "    new StringFormat(\"{foo}-{bar_id}\").format(bar, foo);",
            "  }",
            "}")
        .doTest();
  }

  @Test
  public void format_inlinedFormatWithMoreThan3Args_requiresNameMatch() {
    helper
        .addSourceLines(
            "Test.java",
            "import com.google.mu.util.StringFormat;",
            "class Test {",
            "  void test() {",
            "    // BUG: Diagnostic contains:",
            "    new StringFormat(\"{a}-{b}-{c}-{d}\").format(1, 2, 3, 4);",
            "  }",
            "}")
        .doTest();
  }

  @Test
  public void format_inlinedFormatWithMoreThan3Args_argsMatchPlaceholderNames() {
    helper
        .addSourceLines(
            "Test.java",
            "import com.google.mu.util.StringFormat;",
            "class Test {",
            "  void test(int a, int b, int c, int d) {",
            "    new StringFormat(\"{a}-{b}-{c}-{d}\").format(a, b, c, d);",
            "  }",
            "}")
        .doTest();
  }

  @Test
  public void format_argLiteralDoesNotMatchPlaceholderName() {
    helper
        .addSourceLines(
            "Test.java",
            "import com.google.mu.util.StringFormat;",
            "class Test {",
            "  private static final StringFormat FORMAT = new StringFormat(\"{foo}-{bar_id}\");",
            "  void test() {",
            "    // BUG: Diagnostic contains:",
            "    FORMAT.format(1, 2);",
            "  }",
            "}")
        .doTest();
  }

  @Test
  public void format_inlinedFormatChecksNumberOfArgs() {
    helper
        .addSourceLines(
            "Test.java",
            "import com.google.mu.util.StringFormat;",
            "class Test {",
            "  void test(String foo, int barId, String baz) {",
            "    // BUG: Diagnostic contains:",
            "    new StringFormat(\"{foo}-{bar_id}\").format(foo, barId, baz);",
            "  }",
            "}")
        .doTest();
  }

  @Test
  public void format_tooManyArgs() {
    helper
        .addSourceLines(
            "Test.java",
            "import com.google.mu.util.StringFormat;",
            "class Test {",
            "  void test(String foo, int barId, String baz) {",
            "    // BUG: Diagnostic contains:",
            "    new StringFormat(\"{foo}-{bar_id}\").format(foo, barId, baz);",
            "  }",
            "}")
        .doTest();
  }

  @Test
  public void format_tooFewArgs() {
    helper
        .addSourceLines(
            "Test.java",
            "import com.google.mu.util.StringFormat;",
            "class Test {",
            "  void test(String foo) {",
            "    // BUG: Diagnostic contains:",
            "    new StringFormat(\"{foo}-{bar_id}\").format(foo);",
            "  }",
            "}")
        .doTest();
  }

  @Test
  public void to_argLiteralDoesNotMatchPlaceholderName() {
    helper
        .addSourceLines(
            "Test.java",
            "import com.google.mu.util.StringFormat;",
            "class Test {",
            "  private static final StringFormat.To<IllegalArgumentException> TEMPLATE =",
            "      StringFormat.to(IllegalArgumentException::new, \"{foo}-{bar_id}\");",
            "  void test() {",
            "    // BUG: Diagnostic contains:",
            "    TEMPLATE.with(1, 2);",
            "  }",
            "}")
        .doTest();
  }

  @Test
  public void to_tooManyArgs() {
    helper
        .addSourceLines(
            "Test.java",
            "import com.google.mu.util.StringFormat;",
            "class Test {",
            "  void test(String foo, int barId, String baz) {",
            "    StringFormat.to(IllegalArgumentException::new, \"{foo}-{bar_id}\")",
            "        // BUG: Diagnostic contains:",
            "        .with(foo, barId, baz);",
            "  }",
            "}")
        .doTest();
  }

  @Test
  public void to_tooFewArgs() {
    helper
        .addSourceLines(
            "Test.java",
            "import com.google.mu.util.StringFormat;",
            "class Test {",
            "  void test(String foo, int barId, String baz) {",
            "    StringFormat.to(IllegalArgumentException::new, \"{foo}-{bar_id}\")",
            "        // BUG: Diagnostic contains:",
            "        .with(foo);",
            "  }",
            "}")
        .doTest();
  }

  @Test
  public void nestingPlaceholderIsOk() {
    helper
        .addSourceLines(
            "Test.java",
            "import com.google.mu.util.StringFormat;",
            "class Test {",
            "  void test() {",
            "    new StringFormat(\"{{foo}}\").format(1);",
            "  }",
            "}")
        .doTest();
  }

  @Test
  public void optionalArgDisallowed() {
    helper
        .addSourceLines(
            "Test.java",
            "import com.google.mu.util.StringFormat;",
            "import java.util.Optional;",
            "class Test {",
            "  void test() {",
            "    new StringFormat(\"{foo}\")",
            "        // BUG: Diagnostic contains:",
            "        .format(Optional.empty());",
            "  }",
            "}")
        .doTest();
  }

  @Test
  public void optionalIntArgDisallowed() {
    helper
        .addSourceLines(
            "Test.java",
            "import com.google.mu.util.StringFormat;",
            "import java.util.OptionalInt;",
            "class Test {",
            "  void test() {",
            "    new StringFormat(\"{foo}\")",
            "        // BUG: Diagnostic contains:",
            "        .format(OptionalInt.empty());",
            "  }",
            "}")
        .doTest();
  }

  @Test
  public void optionalLongArgDisallowed() {
    helper
        .addSourceLines(
            "Test.java",
            "import com.google.mu.util.StringFormat;",
            "import java.util.OptionalLong;",
            "class Test {",
            "  void test() {",
            "    new StringFormat(\"{foo}\")",
            "        // BUG: Diagnostic contains:",
            "        .format(OptionalLong.empty());",
            "  }",
            "}")
        .doTest();
  }

  @Test
  public void optionalDoubleArgDisallowed() {
    helper
        .addSourceLines(
            "Test.java",
            "import com.google.mu.util.StringFormat;",
            "import java.util.OptionalDouble;",
            "class Test {",
            "  void test() {",
            "    new StringFormat(\"{foo}\")",
            "        // BUG: Diagnostic contains:",
            "        .format(OptionalDouble.empty());",
            "  }",
            "}")
        .doTest();
  }

  @Test
  public void streamArgDisallowed() {
    helper
        .addSourceLines(
            "Test.java",
            "import com.google.mu.util.StringFormat;",
            "import java.util.stream.Stream;",
            "class Test {",
            "  void test() {",
            "    new StringFormat(\"{foo}\")",
            "        // BUG: Diagnostic contains:",
            "        .format(Stream.empty());",
            "  }",
            "}")
        .doTest();
  }

  @Test
  public void intStreamArgDisallowed() {
    helper
        .addSourceLines(
            "Test.java",
            "import com.google.mu.util.StringFormat;",
            "import java.util.stream.IntStream;",
            "class Test {",
            "  void test() {",
            "    new StringFormat(\"{foo}\")",
            "        // BUG: Diagnostic contains:",
            "        .format(IntStream.empty());",
            "  }",
            "}")
        .doTest();
  }

  @Test
  public void longStreamArgDisallowed() {
    helper
        .addSourceLines(
            "Test.java",
            "import com.google.mu.util.StringFormat;",
            "import java.util.stream.LongStream;",
            "class Test {",
            "  void test() {",
            "    new StringFormat(\"{foo}\")",
            "        // BUG: Diagnostic contains:",
            "        .format(LongStream.empty());",
            "  }",
            "}")
        .doTest();
  }

  @Test
  public void doubleStreamArgDisallowed() {
    helper
        .addSourceLines(
            "Test.java",
            "import com.google.mu.util.StringFormat;",
            "import java.util.stream.DoubleStream;",
            "class Test {",
            "  void test() {",
            "    new StringFormat(\"{foo}\")",
            "        // BUG: Diagnostic contains:",
            "        .format(DoubleStream.empty());",
            "  }",
            "}")
        .doTest();
  }

  @Test
  public void arrayArgDisallowed() {
    helper
        .addSourceLines(
            "Test.java",
            "import com.google.mu.util.StringFormat;",
            "class Test {",
            "  void test() {",
            "    new StringFormat(\"{foo}\")",
            "        // BUG: Diagnostic contains:",
            "        .format(new int[] {1});",
            "  }",
            "}")
        .doTest();
  }

  @Test
  public void cannotPassStringFormatAsParameter() {
    helper
        .addSourceLines(
            "Test.java",
            "import com.google.mu.util.StringFormat;",
            "class Test {",
            "  void test(StringFormat format) {",
            "    // BUG: Diagnostic contains:",
            "    format.format(1);",
            "  }",
            "}")
        .doTest();
  }

  @Test
  public void usingSquareBrackets_correctMethodReference() {
    helper
        .addSourceLines(
            "Test.java",
            "import com.google.mu.util.StringFormat;",
            "import java.util.stream.Stream;",
            "class Test {",
            "  private static final StringFormat.WithSquareBracketedPlaceholders FORMAT =",
            "      new StringFormat.WithSquareBracketedPlaceholders(\"[foo]-[bar_id]\");",
            "  String test() {",
            "    return Stream.of(\"x\").reduce(\"\", FORMAT::format);",
            "  }",
            "}")
        .doTest();
  }

  @Test
  public void usingSquareBrackets_incorrectMethodReference() {
    helper
        .addSourceLines(
            "Test.java",
            "import com.google.mu.util.StringFormat;",
            "import java.util.stream.Stream;",
            "class Test {",
            "  private static final StringFormat.WithSquareBracketedPlaceholders FORMAT =",
            "      new StringFormat.WithSquareBracketedPlaceholders(\"[foo]\");",
            "  String test() {",
            "    // BUG: Diagnostic contains: (2) will be provided",
            "    return Stream.of(\"\").reduce(\"\", FORMAT::format);",
            "  }",
            "}")
        .doTest();
  }

  @Test
  public void formatMethodReferenceUsedAsFunctionCorrectly() {
    helper
        .addSourceLines(
            "Test.java",
            "import com.google.mu.util.StringFormat;",
            "import java.util.stream.Stream;",
            "class Test {",
            "  private static final StringFormat FORMAT = new StringFormat(\"{foo}\");",
            "  private static final long COUNT = Stream.of(1).map(FORMAT::format).count();",
            "}")
        .doTest();
  }

  @Test
  public void formatMethodReferenceUsedAsIntFunctionCorrectly() {
    helper
        .addSourceLines(
            "Test.java",
            "import com.google.mu.util.StringFormat;",
            "import java.util.stream.IntStream;",
            "class Test {",
            "  private static final StringFormat FORMAT = new StringFormat(\"{foo}\");",
            "  private static final long COUNT = IntStream.of(1).mapToObj(FORMAT::format).count();",
            "}")
        .doTest();
  }

  @Test
  public void formatMethodReferenceUsedAsLongFunctionCorrectly() {
    helper
        .addSourceLines(
            "Test.java",
            "import com.google.mu.util.StringFormat;",
            "import java.util.stream.LongStream;",
            "class Test {",
            "  private static final StringFormat FORMAT = new StringFormat(\"{foo}\");",
            "  private static final long COUNT =",
            "      LongStream.of(1).mapToObj(FORMAT::format).count();",
            "}")
        .doTest();
  }

  @Test
  public void formatMethodReferenceUsedAsDoubleFunctionCorrectly() {
    helper
        .addSourceLines(
            "Test.java",
            "import com.google.mu.util.StringFormat;",
            "import java.util.stream.DoubleStream;",
            "class Test {",
            "  private static final StringFormat FORMAT = new StringFormat(\"{foo}\");",
            "  private static final long COUNT =",
            "      DoubleStream.of(1).mapToObj(FORMAT::format).count();",
            "}")
        .doTest();
  }

  @Test
  public void formatMethodReferenceUsedAsFunctionIncorrectly() {
    helper
        .addSourceLines(
            "Test.java",
            "import com.google.mu.util.StringFormat;",
            "import java.util.stream.Stream;",
            "class Test {",
            "  long test() {",
            "    // BUG: Diagnostic contains: (1) will be provided from ",
            "    return Stream.of(1).map(new StringFormat(\"{foo}.{bar}\")::format).count();",
            "  }",
            "}")
        .doTest();
  }

  @Test
  public void formatMethodReferenceUsedAsBiFunctionCorrectly() {
    helper
        .addSourceLines(
            "Test.java",
            "import com.google.mu.util.StringFormat;",
            "import java.util.stream.Stream;",
            "class Test {",
            "  String test() {",
            "    return Stream.of(\"x\").reduce(\"\", new StringFormat(\"{a}:{b}\")::format);",
            "  }",
            "}")
        .doTest();
  }

  @Test
  public void formatMethodReferenceUsedAsBiFunctionIncorrectly() {
    helper
        .addSourceLines(
            "Test.java",
            "import com.google.mu.util.StringFormat;",
            "import java.util.stream.Stream;",
            "class Test {",
            "  String test() {",
            "    // BUG: Diagnostic contains: (2) will be provided from ",
            "    return Stream.of(\"x\").reduce(\"\", new StringFormat(\"{foo}\")::format);",
            "  }",
            "}")
        .doTest();
  }

  @Test
  public void formatMethodReferenceUsedAsUnknownInterface() {
    helper
        .addSourceLines(
            "MyFunction.java", "interface MyFunction {", "  String test(String s);", "}")
        .addSourceLines(
            "Test.java",
            "import com.google.mu.util.StringFormat;",
            "import java.util.stream.Stream;",
            "class Test {",
            "  MyFunction test() {",
            "    // BUG: Diagnostic contains: format() is used as a MyFunction",
            "    return new StringFormat(\"{foo}\")::format;",
            "  }",
            "}")
        .doTest();
  }

  @Test
  public void lenientFormatUsedAsMethodReferenceCorrectly() {
    helper
        .addSourceLines(
            "Test.java",
            "import com.google.mu.util.StringFormat;",
            "import java.util.stream.Stream;",
            "class Test {",
            "  long test() {",
            "    return Stream.of(1).map(new StringFormat(\"{foo}\")::lenientFormat).count();",
            "  }",
            "}")
        .doTest();
  }

  @Test
  public void lenientFormatUsedAsMethodReferenceIncorrectly() {
    helper
        .addSourceLines(
            "Test.java",
            "import com.google.mu.util.StringFormat;",
            "import java.util.stream.Stream;",
            "class Test {",
            "  long test() {",
            "    // BUG: Diagnostic contains: (1) will be provided",
            "    return Stream.of(1).map(new StringFormat(\"{foo}{bar}\")::lenientFormat).count();",
            "  }",
            "}")
        .doTest();
  }

  @Test
  public void toWithMethodUsedAsMethodReferenceCorrectly() {
    helper
        .addSourceLines(
            "Test.java",
            "import com.google.mu.util.StringFormat;",
            "import java.util.stream.Stream;",
            "class Test {",
            "  private static final StringFormat.To<Exception> FAIL =",
            "      StringFormat.to(Exception::new, \"error: {foo}\");",
            "  long test() {",
            "    return Stream.of(1).map(FAIL::with).count();",
            "  }",
            "}")
        .doTest();
  }

  @Test
  public void toWithMethodUsedAsMethodReferenceIncorrectly() {
    helper
        .addSourceLines(
            "Test.java",
            "import com.google.mu.util.StringFormat;",
            "import java.util.stream.Stream;",
            "class Test {",
            "  private static final StringFormat.To<Exception> FAIL =",
            "      StringFormat.to(Exception::new, \"{foo}:{bar}\");",
            "  long test() {",
            "    // BUG: Diagnostic contains: (1) will be provided",
            "    return Stream.of(1).map(FAIL::with).count();",
            "  }",
            "}")
        .doTest();
  }

  @Test
  public void formatMethodReferenceUsedAsFunctionButFormatStringCannotBeDetermined() {
    helper
        .addSourceLines(
            "Test.java",
            "import com.google.mu.util.StringFormat;",
            "import java.util.stream.Stream;",
            "class Test {",
            "  long test(StringFormat format) {",
            "    // BUG: Diagnostic contains: definition not found",
            "    return Stream.of(1).map(format::format).count();",
            "  }",
            "}")
        .doTest();
  }

  @Test
  public void formatMethodReferenceUsedAsFunctionWithIndex() {
    helper
        .addSourceLines(
            "Test.java",
            "import com.google.mu.util.StringFormat;",
            "import com.google.common.collect.Streams;",
            "import java.util.stream.Stream;",
            "class Test {",
            "  private static final StringFormat FORMAT = new StringFormat(\"{index}:{foo}\");",
            "  private static final long COUNT =",
            "      // BUG: Diagnostic contains: FunctionWithIndex",
            "      Streams.mapWithIndex(Stream.of(1), FORMAT::format).count();",
            "}")
        .doTest();
  }

  @Test
  public void formatMethodReferenceUsedAsIntFunctionWithIndex() {
    helper
        .addSourceLines(
            "Test.java",
            "import com.google.mu.util.StringFormat;",
            "import com.google.common.collect.Streams;",
            "import java.util.stream.IntStream;",
            "class Test {",
            "  private static final StringFormat FORMAT = new StringFormat(\"{index}:{foo}\");",
            "  private static final long COUNT =",
            "      // BUG: Diagnostic contains: IntFunctionWithIndex",
            "      Streams.mapWithIndex(IntStream.of(1), FORMAT::format).count();",
            "}")
        .doTest();
  }

  @Test
  public void formatMethodReferenceUsedAsLongFunctionWithIndex() {
    helper
        .addSourceLines(
            "Test.java",
            "import com.google.mu.util.StringFormat;",
            "import com.google.common.collect.Streams;",
            "import java.util.stream.LongStream;",
            "class Test {",
            "  private static final StringFormat FORMAT = new StringFormat(\"{index}:{foo}\");",
            "  private static final long COUNT =",
            "      // BUG: Diagnostic contains: LongFunctionWithIndex",
            "      Streams.mapWithIndex(LongStream.of(1), FORMAT::format).count();",
            "}")
        .doTest();
  }

  @Test
  public void formatMethodReferenceUsedAsDoubleFunctionWithIndex() {
    helper
        .addSourceLines(
            "Test.java",
            "import com.google.mu.util.StringFormat;",
            "import com.google.common.collect.Streams;",
            "import java.util.stream.DoubleStream;",
            "class Test {",
            "  private static final StringFormat FORMAT = new StringFormat(\"{index}:{foo}\");",
            "  private static final long COUNT =",
            "      // BUG: Diagnostic contains: DoubleFunctionWithIndex",
            "      Streams.mapWithIndex(DoubleStream.of(1), FORMAT::format).count();",
            "}")
        .doTest();
  }
}
