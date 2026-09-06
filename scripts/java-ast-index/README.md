# Java AST Index

Build a compact JSON index for the project Java sources without adding Maven dependencies:

```bash
scripts/build-java-ast-index.sh
```

The default output is `target/java-ast-index/java-symbols.json`. Pass a path as the first argument
to write somewhere else. The tool also writes greppable TSV slices into the same directory:
`symbols.tsv`, `classes.tsv`, `methods.tsv`, `calls.tsv`, `news.tsv`, and `affected-tests.tsv`.
The call/constructor tables are capped summaries with counts and examples; use `methods.tsv` when
you need the full per-method lists. `affected-tests.tsv` is heuristic, based on names, imports,
constructor usages, and signatures.

The index is intentionally lossy and token-friendly: it stores packages, imports, classes, fields,
methods, line numbers, method-call names, and constructed type names. Use it to find the small
source regions worth reading before opening full files.

For day-to-day lookup, use the query wrapper. It rebuilds the index automatically when Java sources
are newer than the generated JSON:

```bash
scripts/java-index-query.sh class WriteRouteGate
scripts/java-index-query.sh method check
scripts/java-index-query.sh call invokeDirect
scripts/java-index-query.sh tests WriteRouteGate
```

Useful queries:

```bash
rg 'WriteRouteGate' target/java-ast-index/symbols.tsv
rg 'WriteRouteGate' target/java-ast-index/classes.tsv
rg $'\tcheck\t' target/java-ast-index/methods.tsv
rg '^invokeDirect\t' target/java-ast-index/calls.tsv
rg 'WriteRouteGate' target/java-ast-index/affected-tests.tsv
```

This is a parse-level index, not full semantic resolution: overloaded methods and calls with the
same simple name are intentionally grouped together.
