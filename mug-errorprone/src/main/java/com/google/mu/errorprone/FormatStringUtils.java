package com.google.mu.errorprone;


import static com.google.common.base.CharMatcher.whitespace;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.mu.util.Optionals.optionally;
import static com.google.mu.util.Substring.consecutive;
import static com.google.mu.util.Substring.first;
import static com.google.mu.util.Substring.firstOccurrence;
import static com.google.mu.util.Substring.BoundStyle.INCLUSIVE;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import com.google.common.base.Ascii;
import com.google.common.base.CharMatcher;
import com.google.common.collect.ImmutableList;
import com.google.errorprone.VisitorState;
import com.google.errorprone.util.ASTHelpers;
import com.google.mu.util.Substring;
import com.sun.source.tree.ExpressionTree;
import com.sun.source.tree.IdentifierTree;
import com.sun.source.tree.MethodInvocationTree;
import com.sun.source.tree.NewClassTree;
import com.sun.source.tree.Tree;
import com.sun.source.tree.VariableTree;
import com.sun.tools.javac.api.JavacTrees;
import com.sun.tools.javac.code.Symbol;
import com.sun.tools.javac.code.Symbol.VarSymbol;

/** Some common utils for format and unformat checks. */
final class FormatStringUtils {
  static final Substring.Pattern PLACEHOLDER_PATTERN =
      consecutive(CharMatcher.noneOf("{}")::matches).immediatelyBetween("{", INCLUSIVE, "}", INCLUSIVE);
  private static final Substring.Pattern PLACEHOLDER_SEPARATOR =
      Stream.of("=", "->").map(Substring::first).collect(firstOccurrence());
  static final Substring.RepeatingPattern PLACEHOLDER_NAMES_PATTERN =
      consecutive(CharMatcher.noneOf("{}")::matches).immediatelyBetween("{", "}").repeatedly();

  static ImmutableList<String> placeholderVariableNames(String formatString) {
    Substring.Pattern beforeSeparator = Substring.before(PLACEHOLDER_SEPARATOR);
    return PLACEHOLDER_NAMES_PATTERN
        .from(formatString)
        // for Cloud resource name syntax
        .map(n -> beforeSeparator.from(n).map(whitespace()::trimTrailingFrom).orElse(n))
        .collect(toImmutableList());
  }

  static Optional<ExpressionTree> getInlineStringArg(Tree expression, VisitorState state) {
    ImmutableList<? extends ExpressionTree> args =
        invocationArgs(expression).stream()
            .map(ASTHelpers::stripParentheses)
            .filter(arg -> isStringType(arg, state))
            .collect(toImmutableList());
    return optionally(args.size() == 1, () -> args.get(0));
  }

  static Optional<String> findFormatString(Tree unformatter, VisitorState state) {
    if (unformatter instanceof IdentifierTree) {
      Symbol symbol = ASTHelpers.getSymbol(unformatter);
      if (symbol instanceof VarSymbol) {
        Tree def = JavacTrees.instance(state.context).getTree(symbol);
        if (def instanceof VariableTree) {
          return findFormatString(((VariableTree) def).getInitializer(), state);
        }
      }
      return Optional.empty();
    }
    return getInlineStringArg(unformatter, state)
        .map(tree -> ASTHelpers.constValue(tree, String.class));
  }

  static boolean looksLikeSql(String template) {
    return looksLikeQuery().or(looksLikeInsert()).in(Ascii.toLowerCase(template)).isPresent();
  }

  private static Substring.Pattern looksLikeQuery() {
    return Stream.of("select", "update", "delete")
        .map(w -> keyword(w))
        .collect(firstOccurrence())
        .peek(keyword("from").or(keyword("where")))
        .peek(PLACEHOLDER_PATTERN);
  }

  private static Substring.Pattern looksLikeInsert() {
    return keyword("insert into")
        .peek(keyword("values").or(keyword("select")))
        .peek(PLACEHOLDER_PATTERN);
  }

  private static Substring.Pattern keyword(String word) {
    return first(word).separatedBy(CharMatcher.whitespace().or(CharMatcher.anyOf("()"))::matches);
  }

  private static List<? extends ExpressionTree> invocationArgs(Tree tree) {
    if (tree instanceof NewClassTree) {
      return ((NewClassTree) tree).getArguments();
    }
    if (tree instanceof MethodInvocationTree) {
      return ((MethodInvocationTree) tree).getArguments();
    }
    return ImmutableList.of();
  }

  private static boolean isStringType(ExpressionTree arg, VisitorState state) {
    return ASTHelpers.isSameType(ASTHelpers.getType(arg), state.getSymtab().stringType, state);
  }
}
