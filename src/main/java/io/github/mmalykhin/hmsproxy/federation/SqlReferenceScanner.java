package io.github.mmalykhin.hmsproxy.federation;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Lexical scanner that locates qualified table references inside Hive SQL text.
 *
 * <p>It is intentionally not a SQL parser: it only tokenizes enough of the grammar to know whether
 * a dotted identifier chain stands in a table position. String literals, comments and quoted
 * identifiers are skipped, so their content is never mistaken for a table reference, and chains
 * outside table positions (column qualifiers, aliases) are never reported.
 */
final class SqlReferenceScanner {
  /** Keywords that put the following identifier chain into a table position. */
  private static final Set<String> TABLE_POSITION_KEYWORDS = Set.of("FROM", "JOIN", "INTO", "TABLE", "UPDATE");

  /**
   * Value functions whose argument list uses {@code FROM} as a separator
   * ({@code extract(year from ts)}). Table-position keywords are ignored inside them.
   */
  private static final Set<String> VALUE_FUNCTIONS_WITH_FROM =
      Set.of("EXTRACT", "TRIM", "SUBSTRING", "SUBSTR", "POSITION", "OVERLAY");

  private static final Set<String> KEYWORDS = Set.of(
      "ALL", "AND", "ANTI", "ANY", "AS", "ASC", "BETWEEN", "BY", "CASE", "CLUSTER", "CROSS", "DELETE",
      "DESC", "DISTINCT", "DISTRIBUTE", "ELSE", "END", "EXCEPT", "EXISTS", "FALSE", "FOR", "FROM", "FULL",
      "GROUP", "HAVING", "IN", "INNER", "INSERT", "INTERSECT", "INTO", "IS", "JOIN", "LATERAL", "LEFT",
      "LIKE", "LIMIT", "MERGE", "MINUS", "NOT", "NULL", "OFFSET", "ON", "OR", "ORDER", "OUTER", "OVER",
      "OVERWRITE", "PARTITION", "PIVOT", "QUALIFY", "REGEXP", "RIGHT", "RLIKE", "SELECT", "SEMI", "SET",
      "SORT", "STRAIGHT_JOIN", "TABLE", "TABLESAMPLE", "THEN", "TRUE", "UNION", "UNNEST", "UPDATE",
      "USING", "VALUES", "VIEW", "WHEN", "WHERE", "WINDOW", "WITH");

  private SqlReferenceScanner() {
  }

  /** One dot-separated part of a reference, with its position in the original SQL text. */
  record Part(String text, int start, int end) {
    boolean quoted() {
      return text.length() >= 2 && text.charAt(0) == '`' && text.charAt(text.length() - 1) == '`';
    }

    String unquoted() {
      return quoted() ? text.substring(1, text.length() - 1).replace("``", "`") : text;
    }
  }

  /** A dotted identifier chain that stands in a table position, for example {@code db.table}. */
  record TableReference(List<Part> parts) {
    /** Qualifier parts, that is everything but the trailing table name. */
    List<Part> qualifier() {
      return parts.subList(0, parts.size() - 1);
    }
  }

  /**
   * Returns qualified ({@code >= 2} parts) table references in table positions, in source order.
   */
  static List<TableReference> scan(String sql) {
    List<Token> tokens = tokenize(sql);
    List<TableReference> references = new ArrayList<>();
    State state = State.NONE;
    List<Frame> frames = new ArrayList<>();
    boolean suppressed = false;
    boolean afterAs = false;
    int index = 0;
    while (index < tokens.size()) {
      Token token = tokens.get(index);
      switch (token.kind) {
        case IDENTIFIER, QUOTED_IDENTIFIER -> {
          String keyword = token.kind == Kind.IDENTIFIER
              ? token.text.toUpperCase(Locale.ROOT)
              : null;
          if (keyword != null && KEYWORDS.contains(keyword)) {
            if ("AS".equals(keyword)) {
              afterAs = true;
            } else {
              afterAs = false;
              if (!suppressed && TABLE_POSITION_KEYWORDS.contains(keyword)) {
                state = State.EXPECT_TABLE_REFERENCE;
              } else {
                state = State.NONE;
              }
            }
            index++;
            continue;
          }
          afterAs = false;
          List<Part> chain = new ArrayList<>();
          index = collectChain(tokens, index, chain);
          if (state == State.EXPECT_TABLE_REFERENCE) {
            if (chain.size() >= 2) {
              references.add(new TableReference(List.copyOf(chain)));
            }
            state = State.AFTER_TABLE_REFERENCE;
          }
        }
        case COMMA -> {
          state = state == State.AFTER_TABLE_REFERENCE ? State.EXPECT_TABLE_REFERENCE : State.NONE;
          afterAs = false;
          index++;
        }
        case OPEN_PAREN -> {
          String callee = index > 0 && tokens.get(index - 1).kind == Kind.IDENTIFIER
              ? tokens.get(index - 1).text.toUpperCase(Locale.ROOT)
              : null;
          frames.add(new Frame(state, suppressed));
          suppressed = suppressed || (callee != null && VALUE_FUNCTIONS_WITH_FROM.contains(callee));
          state = State.NONE;
          afterAs = false;
          index++;
        }
        case CLOSE_PAREN -> {
          if (frames.isEmpty()) {
            state = State.NONE;
            suppressed = false;
          } else {
            Frame frame = frames.remove(frames.size() - 1);
            // A closed subquery behaves like a table reference: an alias may follow it.
            state = frame.state == State.EXPECT_TABLE_REFERENCE ? State.AFTER_TABLE_REFERENCE : frame.state;
            suppressed = frame.suppressed;
          }
          afterAs = false;
          index++;
        }
        default -> {
          state = State.NONE;
          afterAs = false;
          index++;
        }
      }
    }
    return references;
  }

  private static int collectChain(List<Token> tokens, int index, List<Part> chain) {
    int cursor = index;
    chain.add(new Part(tokens.get(cursor).text, tokens.get(cursor).start, tokens.get(cursor).end));
    cursor++;
    while (cursor + 1 < tokens.size()
        && tokens.get(cursor).kind == Kind.DOT
        && isIdentifier(tokens.get(cursor + 1))) {
      Token part = tokens.get(cursor + 1);
      chain.add(new Part(part.text, part.start, part.end));
      cursor += 2;
    }
    return cursor;
  }

  private static boolean isIdentifier(Token token) {
    return token.kind == Kind.IDENTIFIER || token.kind == Kind.QUOTED_IDENTIFIER;
  }

  private static List<Token> tokenize(String sql) {
    List<Token> tokens = new ArrayList<>();
    int index = 0;
    int length = sql.length();
    while (index < length) {
      char current = sql.charAt(index);
      if (Character.isWhitespace(current)) {
        index++;
      } else if (current == '-' && index + 1 < length && sql.charAt(index + 1) == '-') {
        index = skipLineComment(sql, index);
      } else if (current == '/' && index + 1 < length && sql.charAt(index + 1) == '*') {
        index = skipBlockComment(sql, index);
      } else if (current == '\'' || current == '"') {
        index = skipStringLiteral(sql, index, current);
      } else if (current == '`') {
        int end = scanQuotedIdentifier(sql, index);
        tokens.add(new Token(Kind.QUOTED_IDENTIFIER, sql.substring(index, end), index, end));
        index = end;
      } else if (isIdentifierStart(current)) {
        int end = index + 1;
        while (end < length && isIdentifierPart(sql.charAt(end))) {
          end++;
        }
        tokens.add(new Token(Kind.IDENTIFIER, sql.substring(index, end), index, end));
        index = end;
      } else if (Character.isDigit(current)) {
        int end = index + 1;
        while (end < length && (Character.isLetterOrDigit(sql.charAt(end)) || sql.charAt(end) == '.')) {
          end++;
        }
        tokens.add(new Token(Kind.NUMBER, sql.substring(index, end), index, end));
        index = end;
      } else {
        Kind kind = switch (current) {
          case '.' -> Kind.DOT;
          case ',' -> Kind.COMMA;
          case '(' -> Kind.OPEN_PAREN;
          case ')' -> Kind.CLOSE_PAREN;
          default -> Kind.OTHER;
        };
        tokens.add(new Token(kind, sql.substring(index, index + 1), index, index + 1));
        index++;
      }
    }
    return tokens;
  }

  private static int skipLineComment(String sql, int index) {
    int end = sql.indexOf('\n', index);
    return end < 0 ? sql.length() : end + 1;
  }

  private static int skipBlockComment(String sql, int index) {
    int end = sql.indexOf("*/", index + 2);
    return end < 0 ? sql.length() : end + 2;
  }

  private static int skipStringLiteral(String sql, int index, char quote) {
    int cursor = index + 1;
    while (cursor < sql.length()) {
      char current = sql.charAt(cursor);
      if (current == '\\' && cursor + 1 < sql.length()) {
        cursor += 2;
      } else if (current == quote) {
        // Doubled quotes escape the quote character itself.
        if (cursor + 1 < sql.length() && sql.charAt(cursor + 1) == quote) {
          cursor += 2;
        } else {
          return cursor + 1;
        }
      } else {
        cursor++;
      }
    }
    return sql.length();
  }

  private static int scanQuotedIdentifier(String sql, int index) {
    int cursor = index + 1;
    while (cursor < sql.length()) {
      if (sql.charAt(cursor) == '`') {
        if (cursor + 1 < sql.length() && sql.charAt(cursor + 1) == '`') {
          cursor += 2;
        } else {
          return cursor + 1;
        }
      } else {
        cursor++;
      }
    }
    return sql.length();
  }

  private static boolean isIdentifierStart(char value) {
    return Character.isLetter(value) || value == '_' || value == '@' || value == '#' || value == '$';
  }

  private static boolean isIdentifierPart(char value) {
    return Character.isLetterOrDigit(value)
        || value == '_'
        || value == '@'
        || value == '#'
        || value == '$';
  }

  private record Token(Kind kind, String text, int start, int end) {
  }

  private record Frame(State state, boolean suppressed) {
  }

  private enum Kind {
    IDENTIFIER,
    QUOTED_IDENTIFIER,
    NUMBER,
    DOT,
    COMMA,
    OPEN_PAREN,
    CLOSE_PAREN,
    OTHER
  }

  private enum State {
    NONE,
    EXPECT_TABLE_REFERENCE,
    AFTER_TABLE_REFERENCE
  }
}
