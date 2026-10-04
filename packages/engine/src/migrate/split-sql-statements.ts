/**
 * Split a .sql file into individual statements.
 *
 * Neon's HTTP driver (`@neondatabase/serverless`) sends each `sql(...)` call as a
 * prepared statement, and Postgres refuses to put more than one command into a
 * prepared statement ("cannot insert multiple commands into a prepared
 * statement", SQLSTATE 42601). So every migration file has to be executed one
 * statement at a time.
 *
 * A naive `split(";")` is wrong: our migrations contain `DEFAULT '{}'::jsonb`
 * and `--` comment lines, and future ones may contain dollar-quoted function
 * bodies. This scanner tracks string literals, quoted identifiers,
 * dollar-quoted blocks, and both comment styles, so a `;` only terminates a
 * statement when it appears in actual code.
 */

const IDENTIFIER_DOLLAR_TAG = /^\$(?:[A-Za-z_][A-Za-z0-9_]*)?\$/;

export function splitSqlStatements(source: string): string[] {
  const statements: string[] = [];

  // `raw` is what we execute (comments preserved — Postgres ignores them).
  // `code` is the same statement with comments stripped, used only to decide
  // whether the statement contains anything executable.
  let raw = "";
  let code = "";

  const push = () => {
    if (code.trim().length > 0) {
      statements.push(raw.trim());
    }
    raw = "";
    code = "";
  };

  let index = 0;
  const length = source.length;

  while (index < length) {
    const char = source[index];
    const nextChar = source[index + 1];

    // -- line comment
    if (char === "-" && nextChar === "-") {
      const newline = source.indexOf("\n", index);
      const stop = newline === -1 ? length : newline;
      raw += source.slice(index, stop);
      index = stop;
      continue;
    }

    // /* block comment */ (Postgres nests these)
    if (char === "/" && nextChar === "*") {
      let depth = 1;
      let cursor = index + 2;
      while (cursor < length && depth > 0) {
        if (source[cursor] === "/" && source[cursor + 1] === "*") {
          depth += 1;
          cursor += 2;
        } else if (source[cursor] === "*" && source[cursor + 1] === "/") {
          depth -= 1;
          cursor += 2;
        } else {
          cursor += 1;
        }
      }
      raw += source.slice(index, cursor);
      index = cursor;
      continue;
    }

    // 'string literal' or "quoted identifier" — doubled quote escapes itself
    if (char === "'" || char === '"') {
      let cursor = index + 1;
      while (cursor < length) {
        if (source[cursor] === char) {
          if (source[cursor + 1] === char) {
            cursor += 2;
            continue;
          }
          cursor += 1;
          break;
        }
        cursor += 1;
      }
      const chunk = source.slice(index, cursor);
      raw += chunk;
      code += chunk;
      index = cursor;
      continue;
    }

    // $tag$ dollar-quoted block $tag$ (function bodies, DO blocks)
    if (char === "$") {
      const tagMatch = IDENTIFIER_DOLLAR_TAG.exec(source.slice(index));
      if (tagMatch) {
        const tag = tagMatch[0];
        const closing = source.indexOf(tag, index + tag.length);
        const stop = closing === -1 ? length : closing + tag.length;
        const chunk = source.slice(index, stop);
        raw += chunk;
        code += chunk;
        index = stop;
        continue;
      }
    }

    if (char === ";") {
      push();
      index += 1;
      continue;
    }

    raw += char;
    code += char;
    index += 1;
  }

  // Trailing statement without a terminating semicolon.
  push();

  return statements;
}
