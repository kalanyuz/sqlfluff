"""The FlinkSQL dialect.

Inherits from ANSI SQL.
FlinkSQL is based on ANSI SQL standard but includes additional features
for stream processing and table operations.

Based on:
https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/sql/
"""

from sqlfluff.core.dialects import load_raw_dialect
from sqlfluff.core.parser import (
    AnyNumberOf,
    BaseSegment,
    Bracketed,
    CodeSegment,
    CommentSegment,
    Dedent,
    Delimited,
    IdentifierSegment,
    Indent,
    OneOf,
    OptionallyBracketed,
    Ref,
    RegexLexer,
    Sequence,
    StringParser,
    SymbolSegment,
    TypedParser,
    WordSegment,
)
from sqlfluff.dialects import dialect_ansi as ansi
from sqlfluff.dialects.dialect_flink_keywords import (
    RESERVED_KEYWORDS,
    UNRESERVED_KEYWORDS,
)

ansi_dialect = load_raw_dialect("ansi")
flink_dialect = ansi_dialect.copy_as(
    "flink",
    formatted_name="Apache Flink SQL",
    docstring="""**Default Casing**: FlinkSQL is case insensitive with
both quoted and unquoted identifiers.

**Quotes**: String Literals: ``'`` (single quotes), Identifiers: |back_quotes|.

The dialect for Apache `Flink SQL`_. This dialect includes FlinkSQL-specific
syntax for stream processing, table operations, and connector configurations.

FlinkSQL supports advanced features like:
- ROW data types for complex nested structures
- Table connectors (Kafka, BigQuery, etc.)
- Watermark definitions for event time processing
- Computed columns and metadata columns
- Temporal table functions

.. _`Flink SQL`: https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/sql/
""",
)

# Update keywords - extend ANSI keywords instead of replacing them
flink_dialect.sets("reserved_keywords").update(RESERVED_KEYWORDS)
flink_dialect.sets("unreserved_keywords").update(UNRESERVED_KEYWORDS)

# Add FlinkSQL-specific lexer patterns
flink_dialect.patch_lexer_matchers(
    [
        # FlinkSQL uses -- for single-line comments
        RegexLexer(
            "inline_comment",
            r"(--)[^\n]*",
            CommentSegment,
            segment_kwargs={"trim_start": ("--",)},
        ),
        # Support for backtick-quoted identifiers
        RegexLexer(
            "back_quote",
            r"`([^`]|``)*`",
            CodeSegment,
            segment_kwargs={
                "quoted_value": (r"`((?:[^`]|``)*)`", 1),
                "escape_replacements": [(r"``", "`")],
            },
        ),
        # Support for numeric literals with precision/scale
        RegexLexer(
            "numeric_literal",
            (
                r"(?>(?>\d+\.\d+|\d+\.|\.\d+)([eE][+-]?\d+)?([dDfF]|BD|bd)?"
                r"|\d+[eE][+-]?\d+([dDfF]|BD|bd)?"
                r"|\d+([dDfFlLsSyY]|BD|bd)?)"
                r"((?<=\.)|(?=\b))"
            ),
            CodeSegment,
        ),
        # Support for string literals with single quotes, including
        # doubled quotes used to escape a quote.
        RegexLexer(
            "single_quote",
            r"'([^'\\]|\\.|'')*'",
            CodeSegment,
            segment_kwargs={
                "quoted_value": (r"'((?:[^'\\]|\\.|'')*)'", 1),
                "escape_replacements": [(r"\\'|''", "'")],
            },
        ),
        # Support for == equality operator
        RegexLexer("equals", r"==|=", CodeSegment),
    ]
)

# Add FlinkSQL-specific datetime units
flink_dialect.sets("datetime_units").clear()
flink_dialect.sets("datetime_units").update(
    [
        "YEAR",
        "MONTH",
        "DAY",
        "HOUR",
        "MINUTE",
        "SECOND",
        "MICROSECOND",
        "MILLISECOND",
        "NANOSECOND",
        "EPOCH",
        "DECADE",
        "CENTURY",
        "MILLENNIUM",
        "QUARTER",
        "WEEK",
        "DOW",
        "ISODOW",
        "DOY",
        "ISOYEAR",
    ]
)

# Add FlinkSQL-specific bare functions
flink_dialect.sets("bare_functions").clear()
flink_dialect.sets("bare_functions").update(
    [
        "CURRENT_DATE",
        "CURRENT_TIME",
        "CURRENT_TIMESTAMP",
        "CURRENT_USER",
        "LOCALTIME",
        "LOCALTIMESTAMP",
        "NOW",
        "CURRENT_WATERMARK",
        "PROCTIME",
    ]
)

# Add FlinkSQL-specific date part functions
flink_dialect.sets("date_part_function_name").update(["TIMESTAMPDIFF"])

# Add angle brackets for generic types
flink_dialect.bracket_sets("angle_bracket_pairs").update(
    [
        ("angle", "StartAngleBracketSegment", "EndAngleBracketSegment", False),
    ]
)

# Add FlinkSQL-specific segments
flink_dialect.add(
    # Angle bracket segments
    StartAngleBracketSegment=StringParser(
        "<",
        SymbolSegment,
        type="start_angle_bracket",
    ),
    EndAngleBracketSegment=StringParser(
        ">",
        SymbolSegment,
        type="end_angle_bracket",
    ),
    # Backtick identifier support
    BackQuotedIdentifierSegment=TypedParser(
        "back_quote",
        IdentifierSegment,
        type="quoted_identifier",
        trim_chars=("`",),
        casefold=str.upper,
    ),
    # Double equals operator for connector options
    DoubleEqualsSegment=StringParser("==", SymbolSegment, type="double_equals"),
    # Row pattern quantifier used by MATCH_RECOGNIZE
    QuestionMarkSegment=StringParser("?", SymbolSegment, type="question_mark"),
    FlinkPropertyWordSegment=OneOf(
        Ref("SingleIdentifierGrammar"),
        TypedParser("word", WordSegment, type="naked_identifier"),
    ),
    FlinkPropertyNamePartSegment=Sequence(
        Ref("FlinkPropertyWordSegment"),
        AnyNumberOf(
            Sequence(
                Ref("MinusSegment"),
                Ref("FlinkPropertyWordSegment"),
                allow_gaps=False,
            ),
        ),
    ),
    FlinkPropertyNameSegment=OneOf(
        Delimited(
            Ref("FlinkPropertyNamePartSegment"),
            delimiter=Ref("DotSegment"),
            allow_gaps=False,
        ),
        Ref("QuotedLiteralSegment"),
    ),
    FlinkPropertyValueSegment=OneOf(
        Sequence(
            Ref("NumericLiteralSegment"),
            Ref("FlinkPropertyWordSegment"),
        ),
        Ref("LiteralGrammar"),
        Delimited(
            Ref("FlinkPropertyNamePartSegment"),
            delimiter=Ref("DotSegment"),
            allow_gaps=False,
        ),
    ),
    # LIKE / NOT LIKE / ILIKE / NOT ILIKE pattern clause for SHOW statements
    FlinkShowLikeClauseGrammar=Sequence(
        Ref.keyword("NOT", optional=True),
        OneOf("LIKE", "ILIKE"),
        Ref("QuotedLiteralSegment"),
    ),
    # (FROM | IN) [catalog.]database clause for SHOW statements
    FlinkShowFromClauseGrammar=Sequence(
        OneOf("FROM", "IN"),
        Ref("ObjectReferenceSegment"),
    ),
    # Connector options for CREATE TABLE
    CreateTableConnectorOptionsSegment=Sequence(
        "WITH",
        Bracketed(
            Delimited(
                Sequence(
                    OneOf(
                        Ref("QuotedLiteralSegment"),  # 'key' format
                        Ref("NakedIdentifierSegment"),  # key format
                    ),
                    OneOf(
                        Ref("EqualsSegment"),  # single =
                        Ref("DoubleEqualsSegment"),  # double == format
                    ),
                    OneOf(
                        Ref("QuotedLiteralSegment"),  # 'value' format
                        Ref("NakedIdentifierSegment"),  # value format
                    ),
                ),
            ),
        ),
    ),
    # Watermark definition
    WatermarkDefinitionSegment=Sequence(
        "WATERMARK",
        "FOR",
        Ref("ColumnReferenceSegment"),
        "AS",
        Ref("ExpressionSegment"),
    ),
    # Computed column definition
    ComputedColumnDefinitionSegment=Sequence(
        Ref("SingleIdentifierGrammar"),  # column name
        "AS",
        Ref("ExpressionSegment"),  # computed expression
        Sequence("COMMENT", Ref("QuotedLiteralSegment"), optional=True),
    ),
    # Metadata column definition
    MetadataColumnDefinitionSegment=Sequence(
        Ref("SingleIdentifierGrammar"),  # column name
        Ref("DatatypeSegment"),  # column type
        "METADATA",
        Sequence("FROM", Ref("QuotedLiteralSegment"), optional=True),
        Sequence("VIRTUAL", optional=True),
        Sequence("COMMENT", Ref("QuotedLiteralSegment"), optional=True),
    ),
    # Table constraint
    FlinkTableConstraintSegment=Sequence(
        Sequence("CONSTRAINT", Ref("NakedIdentifierSegment"), optional=True),
        "PRIMARY",
        "KEY",
        Bracketed(
            Delimited(
                Ref("ColumnReferenceSegment"),
            ),
        ),
        "NOT",
        "ENFORCED",
    ),
    # Partition definition
    PartitionDefinitionSegment=Sequence(
        "PARTITIONED",
        "BY",
        Bracketed(
            Delimited(
                Ref("ColumnReferenceSegment"),
            ),
        ),
    ),
    # Distribution definition
    DistributionDefinitionSegment=OneOf(
        # DISTRIBUTED BY [HASH|RANGE] (columns) [INTO n BUCKETS]
        Sequence(
            "DISTRIBUTED",
            "BY",
            OneOf("HASH", "RANGE", optional=True),
            Bracketed(
                Delimited(
                    Ref("ColumnReferenceSegment"),
                ),
            ),
            Sequence("INTO", Ref("NumericLiteralSegment"), "BUCKETS", optional=True),
        ),
        # DISTRIBUTED INTO n BUCKETS
        Sequence(
            "DISTRIBUTED",
            "INTO",
            Ref("NumericLiteralSegment"),
            "BUCKETS",
        ),
    ),
    # LIKE options
    LikeOptionsSegment=Delimited(
        OneOf(
            # INCLUDING/EXCLUDING with ALL/CONSTRAINTS/DISTRIBUTION/PARTITIONS
            Sequence(
                OneOf("INCLUDING", "EXCLUDING"),
                OneOf("ALL", "CONSTRAINTS", "DISTRIBUTION", "PARTITIONS"),
            ),
            # INCLUDING/EXCLUDING/OVERWRITING with GENERATED/OPTIONS/WATERMARKS
            Sequence(
                OneOf("INCLUDING", "EXCLUDING", "OVERWRITING"),
                OneOf("GENERATED", "OPTIONS", "WATERMARKS"),
            ),
        ),
    ),
    # LIKE clause
    LikeClauseSegment=Sequence(
        "LIKE",
        Ref("TableReferenceSegment"),
        Bracketed(
            Ref("LikeOptionsSegment"),
            optional=True,
        ),
    ),
    # DESCRIPTOR(col) - used in windowing TVFs
    DescriptorSegment=Sequence(
        "DESCRIPTOR",
        Bracketed(
            Ref("ColumnReferenceSegment"),
        ),
    ),
    # TABLE t - table argument for TVFs like TUMBLE, HOP
    TableArgSegment=Sequence(
        "TABLE",
        Ref("TableReferenceSegment"),
    ),
    # Row pattern quantifiers for MATCH_RECOGNIZE.
    FlinkPatternQuantifierGrammar=Sequence(
        OneOf(
            Ref("PositiveSegment"),
            Ref("StarSegment"),
            Ref("QuestionMarkSegment"),
            Bracketed(
                OneOf(
                    Ref("NumericLiteralSegment"),
                    Sequence(
                        Ref("NumericLiteralSegment"),
                        Ref("CommaSegment"),
                    ),
                    Sequence(
                        Ref("CommaSegment"),
                        Ref("NumericLiteralSegment"),
                    ),
                    Sequence(
                        Ref("NumericLiteralSegment"),
                        Ref("CommaSegment"),
                        Ref("NumericLiteralSegment"),
                    ),
                ),
                bracket_type="curly",
                bracket_pairs_set="bracket_pairs",
            ),
        ),
        allow_gaps=False,
    ),
    FlinkPatternTermGrammar=OneOf(
        Sequence(
            Ref("SingleIdentifierGrammar"),
            Ref("FlinkPatternQuantifierGrammar", optional=True),
            allow_gaps=False,
        ),
        Bracketed(
            Ref("FlinkPatternSegment"),
        ),
    ),
)


class FlinkArrayExpressionSegment(BaseSegment):
    """An `ARRAY[...]` constructor for FlinkSQL."""

    type = "array_expression"
    match_grammar = Sequence(
        "ARRAY",
        Bracketed(
            Delimited(
                Ref("BaseExpressionElementGrammar"),
                optional=True,
            ),
            bracket_type="square",
        ),
    )


class FunctionNameSegment(ansi.FunctionNameSegment):
    """A function name that supports Flink backtick-quoted identifiers."""

    match_grammar = Sequence(
        AnyNumberOf(
            Sequence(
                Ref("SingleIdentifierGrammar"),
                Ref("DotSegment"),
            ),
            terminators=[Ref("BracketedSegment")],
        ),
        OneOf(
            Ref("FunctionNameIdentifierSegment"),
            Ref("QuotedIdentifierSegment"),
            Ref("BackQuotedIdentifierSegment"),
            terminators=[Ref("BracketedSegment")],
        ),
        allow_gaps=False,
    )


flink_dialect.replace(
    SingleIdentifierGrammar=OneOf(
        Ref("NakedIdentifierSegment"),
        Ref("QuotedIdentifierSegment"),
        Ref("BackQuotedIdentifierSegment"),
    ),
    FunctionNameSegment=FunctionNameSegment,
    # Extended to support TABLE and DESCRIPTOR arguments in TVFs
    FunctionContentsExpressionGrammar=OneOf(
        Ref("TableArgSegment"),
        Ref("DescriptorSegment"),
        Ref("ExpressionSegment"),
    ),
    # Flink supports ARRAY[...] and MAP[...] constructors.
    ArrayExpressionSegment=FlinkArrayExpressionSegment,
    Expression_D_Potential_Select_Statement_Without_Brackets=flink_dialect.get_grammar(
        "Expression_D_Potential_Select_Statement_Without_Brackets"
    ).copy(
        insert=[Ref("FlinkMapExpressionSegment")],
        before=Ref("ColumnReferenceSegment"),
    ),
    # Flink supports LATERAL TABLE(...) in a FROM clause.
    PreTableFunctionKeywordsGrammar=OneOf("LATERAL"),
    # Flink supports MATCH_RECOGNIZE as a join-like clause.
    JoinLikeClauseGrammar=Sequence(
        Ref("MatchRecognizeClauseSegment"),
        Ref("AliasExpressionSegment", optional=True),
    ),
)


class TemporalQuerySegment(BaseSegment):
    """A segment for `FOR SYSTEM_TIME AS OF`."""

    type = "temporal_query"
    match_grammar = Sequence(
        "FOR",
        "SYSTEM_TIME",
        "AS",
        "OF",
        Ref("ExpressionSegment"),
    )


class CreateViewStatementSegment(ansi.CreateViewStatementSegment):
    """A `CREATE VIEW` statement for FlinkSQL.

    https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/sql/create/#create-view
    """

    match_grammar = Sequence(
        "CREATE",
        Sequence("OR", "REPLACE", optional=True),
        Ref("TemporaryGrammar", optional=True),
        "VIEW",
        Ref("IfNotExistsGrammar", optional=True),
        Ref("TableReferenceSegment"),
        # Column list (optional)
        Bracketed(
            Delimited(
                Ref("ColumnReferenceSegment"),
            ),
            optional=True,
        ),
        Sequence("COMMENT", Ref("QuotedLiteralSegment"), optional=True),
        "AS",
        Ref("SelectableGrammar"),
    )


class IntervalExpressionSegment(ansi.IntervalExpressionSegment):
    """An interval expression segment for FlinkSQL.

    Flink supports precision for datetime units, e.g., SECOND(3).
    https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/sql/queries/window-tvf/
    """

    type = "interval_expression"

    # Datetime unit with optional precision, e.g., SECOND or SECOND(3)
    _datetime_unit_with_precision = Sequence(
        Ref("DatetimeUnitSegment"),
        Bracketed(
            Ref("NumericLiteralSegment"),
            optional=True,
        ),
    )

    match_grammar = Sequence(
        "INTERVAL",
        OneOf(
            # The Numeric Version
            Sequence(
                Ref("NumericLiteralSegment"),
                OneOf(
                    Ref("QuotedLiteralSegment"),
                    _datetime_unit_with_precision,
                ),
            ),
            # The String version
            Ref("QuotedLiteralSegment"),
            # Combine version (most common in Flink)
            Sequence(
                Ref("QuotedLiteralSegment"),
                OneOf(
                    Ref("QuotedLiteralSegment"),
                    _datetime_unit_with_precision,
                ),
            ),
        ),
    )


class CreateTableStatementSegment(ansi.CreateTableStatementSegment):
    """A `CREATE TABLE` statement for FlinkSQL."""

    match_grammar = Sequence(
        "CREATE",
        Sequence("OR", "REPLACE", optional=True),
        Ref("TemporaryGrammar", optional=True),
        "TABLE",
        Ref("IfNotExistsGrammar", optional=True),
        Ref("TableReferenceSegment"),
        OneOf(
            # Columns definition with FlinkSQL-specific elements
            Sequence(
                Bracketed(
                    Delimited(
                        OneOf(
                            # Physical column definition
                            Ref("ColumnDefinitionSegment"),
                            # Metadata column definition
                            Ref("MetadataColumnDefinitionSegment"),
                            # Computed column definition
                            Ref("ComputedColumnDefinitionSegment"),
                            # Watermark definition
                            Ref("WatermarkDefinitionSegment"),
                            # Table constraint
                            Ref("FlinkTableConstraintSegment"),
                        ),
                    ),
                ),
                # Table comment
                Sequence("COMMENT", Ref("QuotedLiteralSegment"), optional=True),
                # Partition definition
                Ref("PartitionDefinitionSegment", optional=True),
                # Distribution definition
                Ref("DistributionDefinitionSegment", optional=True),
                # Connector options
                Ref("CreateTableConnectorOptionsSegment", optional=True),
                # LIKE clause
                Ref("LikeClauseSegment", optional=True),
            ),
            # Create AS syntax:
            Sequence(
                "AS",
                OptionallyBracketed(Ref("SelectableGrammar")),
            ),
            # Create like syntax
            Ref("LikeClauseSegment"),
        ),
    )


class CreateCatalogStatementSegment(BaseSegment):
    """A `CREATE CATALOG` statement."""

    type = "create_catalog_statement"
    match_grammar = Sequence(
        "CREATE",
        "CATALOG",
        Ref("NakedIdentifierSegment"),
        Ref("CreateTableConnectorOptionsSegment"),
    )


class CreateDatabaseStatementSegment(BaseSegment):
    """A `CREATE DATABASE` statement for FlinkSQL."""

    type = "create_database_statement"
    match_grammar = Sequence(
        "CREATE",
        "DATABASE",
        Ref("IfNotExistsGrammar", optional=True),
        Ref("ObjectReferenceSegment"),
        Sequence("COMMENT", Ref("QuotedLiteralSegment"), optional=True),
        Ref("CreateTableConnectorOptionsSegment", optional=True),
    )


class DescribeStatementSegment(BaseSegment):
    """A `DESCRIBE` / `DESC` statement for FlinkSQL.

    https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/sql/describe/
    """

    type = "describe_statement"
    match_grammar = Sequence(
        OneOf("DESCRIBE", "DESC"),
        OneOf(
            Sequence(
                "CATALOG",
                Ref.keyword("EXTENDED", optional=True),
                Ref("SingleIdentifierGrammar"),
            ),
            Sequence(
                "FUNCTION",
                Ref.keyword("EXTENDED", optional=True),
                Ref("FunctionNameSegment"),
            ),
            Sequence(
                "JOB",
                Ref("QuotedLiteralSegment"),
            ),
            Sequence(
                "MODEL",
                Ref.keyword("EXTENDED", optional=True),
                Ref("ObjectReferenceSegment"),
            ),
            Ref("TableReferenceSegment"),
        ),
    )


class ExplainStatementSegment(ansi.ExplainStatementSegment):
    """An `EXPLAIN` statement for FlinkSQL."""

    type = "explain_statement"
    match_grammar = Sequence(
        "EXPLAIN",
        Sequence("PLAN", "FOR", optional=True),
        Ref("SelectableGrammar"),
    )


class ShowStatementsSegment(BaseSegment):
    """A `SHOW` statement for FlinkSQL.

    https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/sql/show/
    """

    type = "show_statement"
    match_grammar = Sequence(
        "SHOW",
        OneOf(
            # SHOW CURRENT CATALOG / DATABASE
            Sequence("CURRENT", OneOf("CATALOG", "DATABASE")),
            # SHOW CREATE CATALOG / TABLE / VIEW / MATERIALIZED TABLE / MODEL
            Sequence(
                "CREATE",
                OneOf(
                    Sequence("MATERIALIZED", "TABLE", Ref("TableReferenceSegment")),
                    Sequence("CATALOG", Ref("SingleIdentifierGrammar")),
                    Sequence("TABLE", Ref("TableReferenceSegment")),
                    Sequence("VIEW", Ref("TableReferenceSegment")),
                    Sequence("MODEL", Ref("ObjectReferenceSegment")),
                ),
            ),
            # SHOW CATALOGS [[NOT] (LIKE | ILIKE) 'pattern']
            Sequence(
                "CATALOGS",
                Ref("FlinkShowLikeClauseGrammar", optional=True),
            ),
            # SHOW DATABASES [(FROM | IN) catalog] [[NOT] (LIKE | ILIKE) 'pattern']
            Sequence(
                "DATABASES",
                Ref("FlinkShowFromClauseGrammar", optional=True),
                Ref("FlinkShowLikeClauseGrammar", optional=True),
            ),
            # SHOW MATERIALIZED TABLES [(FROM | IN) db] [[NOT] (LIKE | ILIKE) 'pattern']
            Sequence(
                "MATERIALIZED",
                "TABLES",
                Ref("FlinkShowFromClauseGrammar", optional=True),
                Ref("FlinkShowLikeClauseGrammar", optional=True),
            ),
            # SHOW TABLES / VIEWS / PROCEDURES / MODELS
            Sequence(
                OneOf("TABLES", "VIEWS", "PROCEDURES", "MODELS"),
                Ref("FlinkShowFromClauseGrammar", optional=True),
                Ref("FlinkShowLikeClauseGrammar", optional=True),
            ),
            # SHOW COLUMNS (FROM | IN) table [[NOT] (LIKE | ILIKE) 'pattern']
            Sequence(
                "COLUMNS",
                OneOf("FROM", "IN"),
                Ref("TableReferenceSegment"),
                Ref("FlinkShowLikeClauseGrammar", optional=True),
            ),
            # SHOW PARTITIONS table [PARTITION (k = v, ...)]
            Sequence(
                "PARTITIONS",
                Ref("TableReferenceSegment"),
                Sequence(
                    "PARTITION",
                    Bracketed(
                        Delimited(
                            Sequence(
                                Ref("SingleIdentifierGrammar"),
                                Ref("EqualsSegment"),
                                Ref("LiteralGrammar"),
                            ),
                        ),
                    ),
                    optional=True,
                ),
            ),
            # SHOW [USER] FUNCTIONS [(FROM | IN) db] [[NOT] (LIKE | ILIKE) 'pattern']
            Sequence(
                Ref.keyword("USER", optional=True),
                "FUNCTIONS",
                Ref("FlinkShowFromClauseGrammar", optional=True),
                Ref("FlinkShowLikeClauseGrammar", optional=True),
            ),
            # SHOW [FULL] MODULES
            Sequence(
                Ref.keyword("FULL", optional=True),
                "MODULES",
            ),
            "JARS",
            "JOBS",
        ),
    )


class LoadModuleStatementSegment(BaseSegment):
    """A `LOAD MODULE` statement for FlinkSQL.

    https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/sql/load/
    """

    type = "load_module_statement"
    match_grammar = Sequence(
        "LOAD",
        "MODULE",
        Ref("SingleIdentifierGrammar"),
        Ref("CreateTableConnectorOptionsSegment", optional=True),
    )


class UnloadModuleStatementSegment(BaseSegment):
    """An `UNLOAD MODULE` statement for FlinkSQL.

    https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/sql/unload/
    """

    type = "unload_module_statement"
    match_grammar = Sequence(
        "UNLOAD",
        "MODULE",
        Ref("SingleIdentifierGrammar"),
    )


class ResetStatementSegment(BaseSegment):
    """A `RESET` statement for FlinkSQL.

    https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/sql/reset/
    """

    type = "reset_statement"
    match_grammar = Sequence(
        "RESET",
        Ref("FlinkPropertyNameSegment", optional=True),
    )


class AddJarStatementSegment(BaseSegment):
    """An `ADD JAR` statement for FlinkSQL.

    https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/sql/jar/#add-jar
    """

    type = "add_jar_statement"
    match_grammar = Sequence(
        "ADD",
        "JAR",
        Ref("QuotedLiteralSegment"),
    )


class RemoveJarStatementSegment(BaseSegment):
    """A `REMOVE JAR` statement for FlinkSQL.

    https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/sql/jar/#remove-jar
    """

    type = "remove_jar_statement"
    match_grammar = Sequence(
        "REMOVE",
        "JAR",
        Ref("QuotedLiteralSegment"),
    )


class StopJobStatementSegment(BaseSegment):
    """A `STOP JOB` statement for FlinkSQL.

    https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/sql/job/#stop-job
    """

    type = "stop_job_statement"
    match_grammar = Sequence(
        "STOP",
        "JOB",
        Ref("QuotedLiteralSegment"),
        Sequence("WITH", "SAVEPOINT", optional=True),
        Sequence("WITH", "DRAIN", optional=True),
    )


class CallStatementSegment(BaseSegment):
    """A `CALL` procedure statement for FlinkSQL.

    https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/sql/call/
    """

    type = "call_statement"
    match_grammar = Sequence(
        "CALL",
        Ref("FunctionSegment"),
    )


class AnalyzeTableStatementSegment(BaseSegment):
    """An `ANALYZE TABLE` statement for FlinkSQL.

    https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/sql/analyze/
    """

    type = "analyze_table_statement"
    match_grammar = Sequence(
        "ANALYZE",
        "TABLE",
        Ref("TableReferenceSegment"),
        Sequence(
            "PARTITION",
            Bracketed(
                Delimited(
                    Sequence(
                        Ref("SingleIdentifierGrammar"),
                        Sequence(
                            Ref("EqualsSegment"),
                            Ref("LiteralGrammar"),
                            optional=True,
                        ),
                    ),
                ),
            ),
            optional=True,
        ),
        "COMPUTE",
        "STATISTICS",
        Sequence(
            "FOR",
            OneOf(
                Sequence("ALL", "COLUMNS"),
                Sequence(
                    "COLUMNS",
                    Delimited(Ref("ColumnReferenceSegment")),
                ),
            ),
            optional=True,
        ),
    )


class DropCatalogStatementSegment(BaseSegment):
    """A `DROP CATALOG` statement for FlinkSQL.

    https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/sql/drop/#drop-catalog
    """

    type = "drop_catalog_statement"
    match_grammar = Sequence(
        "DROP",
        "CATALOG",
        Ref("IfExistsGrammar", optional=True),
        Ref("SingleIdentifierGrammar"),
    )


class DropViewStatementSegment(ansi.DropViewStatementSegment):
    """A `DROP VIEW` statement for FlinkSQL.

    https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/sql/drop/#drop-view
    """

    match_grammar = Sequence(
        "DROP",
        Ref("TemporaryGrammar", optional=True),
        "VIEW",
        Ref("IfExistsGrammar", optional=True),
        Ref("TableReferenceSegment"),
    )


class DropFunctionStatementSegment(ansi.DropFunctionStatementSegment):
    """A `DROP FUNCTION` statement for FlinkSQL.

    https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/sql/drop/#drop-function
    """

    match_grammar = Sequence(
        "DROP",
        OneOf(
            Sequence("TEMPORARY", "SYSTEM"),
            "TEMPORARY",
            optional=True,
        ),
        "FUNCTION",
        Ref("IfExistsGrammar", optional=True),
        Ref("FunctionNameSegment"),
    )


class SetStatementSegment(BaseSegment):
    """A FlinkSQL SET statement."""

    type = "set_statement"
    match_grammar = Sequence(
        "SET",
        OneOf(
            Sequence(
                Ref("FlinkPropertyNameSegment"),
                Ref("EqualsSegment"),
                Ref("FlinkPropertyValueSegment"),
            ),
            Sequence(
                Ref("QuotedLiteralSegment"),
                Ref("EqualsSegment"),
                Ref("QuotedLiteralSegment"),
            ),
            optional=True,
        ),
    )


class StatementSegment(ansi.StatementSegment):
    """A generic segment, to any of its child subsegments."""

    match_grammar = ansi.StatementSegment.match_grammar.copy(
        insert=[
            # FlinkSQL-specific statements
            Ref("AddJarStatementSegment"),
            Ref("AnalyzeTableStatementSegment"),
            Ref("CallStatementSegment"),
            Ref("CreateCatalogStatementSegment"),
            Ref("CreateDatabaseStatementSegment"),
            Ref("CreateFunctionStatementSegment"),
            Ref("DescribeStatementSegment"),
            Ref("DropCatalogStatementSegment"),
            Ref("ExecuteStatementSetSegment"),
            Ref("LoadModuleStatementSegment"),
            Ref("RemoveJarStatementSegment"),
            Ref("ResetStatementSegment"),
            Ref("ShowStatementsSegment"),
            Ref("SetStatementSegment"),
            Ref("StopJobStatementSegment"),
            Ref("UnloadModuleStatementSegment"),
        ],
    )


class UseStatementSegment(ansi.UseStatementSegment):
    """A FlinkSQL USE statement.

    https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/sql/use/
    """

    type = "use_statement"
    match_grammar = OneOf(
        # USE CATALOG catalog_name (needs to be first to prevent CATALOG being
        # parsed as database name)
        Sequence(
            "USE",
            "CATALOG",
            Ref("ObjectReferenceSegment"),
        ),
        # USE MODULES module1 [, module2, ...]
        Sequence(
            "USE",
            "MODULES",
            Delimited(Ref("SingleIdentifierGrammar")),
        ),
        # USE [catalog_name.]database_name
        Sequence(
            "USE",
            Ref("ObjectReferenceSegment"),
        ),
    )


class CreateFunctionStatementSegment(BaseSegment):
    """A `CREATE FUNCTION` statement for FlinkSQL.

    https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/sql/create/#create-function
    """

    type = "create_function_statement"
    match_grammar = Sequence(
        "CREATE",
        OneOf(
            Sequence("TEMPORARY", "SYSTEM"),
            "TEMPORARY",
            optional=True,
        ),
        "FUNCTION",
        Ref("IfNotExistsGrammar", optional=True),
        Ref("FunctionNameSegment"),
        "AS",
        OneOf(
            Ref("QuotedLiteralSegment"),
            Ref("ObjectReferenceSegment"),
        ),
        Sequence(
            "LANGUAGE",
            OneOf("JAVA", "SCALA", "PYTHON"),
            optional=True,
        ),
        Sequence(
            "USING",
            Delimited(
                Sequence(
                    OneOf("JAR", "ARTIFACT"),
                    Ref("QuotedLiteralSegment"),
                ),
            ),
            optional=True,
        ),
        Ref("CreateTableConnectorOptionsSegment", optional=True),
    )


class ExecuteStatementSetSegment(BaseSegment):
    """An `EXECUTE STATEMENT SET` block for FlinkSQL.

    https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/sql/insert/#insert-into-multiple-tables
    """

    type = "execute_statement_set"
    match_grammar = OneOf(
        Sequence(
            "EXECUTE",
            "STATEMENT",
            "SET",
            "BEGIN",
            Indent,
            AnyNumberOf(
                Sequence(
                    Ref("InsertStatementSegment"),
                    Ref("DelimiterGrammar"),
                ),
                min_times=1,
            ),
            Dedent,
            "END",
        ),
        Sequence(
            "BEGIN",
            "STATEMENT",
            "SET",
            Ref("DelimiterGrammar"),
            Indent,
            AnyNumberOf(
                Sequence(
                    Ref("InsertStatementSegment"),
                    Ref("DelimiterGrammar"),
                ),
                min_times=1,
            ),
            Dedent,
            "END",
        ),
    )


class FlinkMapExpressionSegment(BaseSegment):
    """A `MAP[...]` constructor for FlinkSQL."""

    type = "map_expression"
    match_grammar = Sequence(
        "MAP",
        Bracketed(
            Delimited(
                Ref("BaseExpressionElementGrammar"),
                optional=True,
            ),
            bracket_type="square",
        ),
    )


class FlinkPatternSegment(BaseSegment):
    """A row pattern for FlinkSQL `MATCH_RECOGNIZE`."""

    type = "pattern_expression"
    match_grammar = Delimited(
        AnyNumberOf(
            Ref("FlinkPatternTermGrammar"),
            min_times=1,
        ),
        delimiter=Ref("BitwiseOrSegment"),
    )


class MatchRecognizeClauseSegment(BaseSegment):
    """A `MATCH_RECOGNIZE` clause for FlinkSQL.

    https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/sql/queries/match_recognize/
    """

    type = "match_recognize_clause"
    match_grammar = Sequence(
        "MATCH_RECOGNIZE",
        Bracketed(
            Ref("PartitionClauseSegment", optional=True),
            Ref("OrderByClauseSegment", optional=True),
            Sequence(
                "MEASURES",
                Delimited(
                    Sequence(
                        Ref("ExpressionSegment"),
                        Ref("AliasExpressionSegment"),
                    ),
                ),
                optional=True,
            ),
            Sequence(
                "ONE",
                "ROW",
                "PER",
                "MATCH",
                optional=True,
            ),
            Sequence(
                "AFTER",
                "MATCH",
                "SKIP",
                OneOf(
                    Sequence("PAST", "LAST", "ROW"),
                    Sequence("TO", "NEXT", "ROW"),
                    Sequence("TO", "LAST", Ref("SingleIdentifierGrammar")),
                    Sequence("TO", "FIRST", Ref("SingleIdentifierGrammar")),
                ),
                optional=True,
            ),
            "PATTERN",
            Bracketed(
                Ref("FlinkPatternSegment"),
            ),
            Sequence(
                "WITHIN",
                Ref("IntervalExpressionSegment"),
                optional=True,
            ),
            "DEFINE",
            Delimited(
                Sequence(
                    Ref("SingleIdentifierGrammar"),
                    "AS",
                    Ref("ExpressionSegment"),
                ),
            ),
        ),
    )


class RowDataTypeSegment(BaseSegment):
    """A ROW data type for FlinkSQL."""

    type = "row_data_type"
    match_grammar = Sequence(
        "ROW",
        OneOf(
            # ROW<...> syntax
            Bracketed(
                Delimited(
                    Sequence(
                        Ref("SingleIdentifierGrammar"),  # field name
                        Ref("DatatypeSegment"),  # field type
                        Sequence("NOT", "NULL", optional=True),
                        Sequence("COMMENT", Ref("QuotedLiteralSegment"), optional=True),
                    ),
                ),
                bracket_type="angle",
                bracket_pairs_set="angle_bracket_pairs",
            ),
            # ROW(...) syntax
            Bracketed(
                Delimited(
                    Sequence(
                        Ref("SingleIdentifierGrammar"),  # field name
                        Ref("DatatypeSegment"),  # field type
                        Sequence("NOT", "NULL", optional=True),
                        Sequence("COMMENT", Ref("QuotedLiteralSegment"), optional=True),
                    ),
                ),
                bracket_type="round",
            ),
        ),
    )


class DatatypeSegment(ansi.DatatypeSegment):
    """Enhanced data type segment for FlinkSQL."""

    type = "data_type"
    match_grammar = OneOf(
        # FlinkSQL ROW data type
        Ref("RowDataTypeSegment"),
        # Array data types
        Sequence(
            "ARRAY",
            Bracketed(
                Sequence(
                    Ref("DatatypeSegment"),
                    Sequence("NOT", "NULL", optional=True),
                ),
                bracket_type="angle",
                bracket_pairs_set="angle_bracket_pairs",
            ),
        ),
        # Map data types
        Sequence(
            "MAP",
            Bracketed(
                Sequence(
                    Ref("DatatypeSegment"),  # key type
                    Ref("CommaSegment"),
                    Ref("DatatypeSegment"),  # value type
                    Sequence("NOT", "NULL", optional=True),
                ),
                bracket_type="angle",
                bracket_pairs_set="angle_bracket_pairs",
            ),
        ),
        # Multiset data types
        Sequence(
            "MULTISET",
            Bracketed(
                Sequence(
                    Ref("DatatypeSegment"),
                    Sequence("NOT", "NULL", optional=True),
                ),
                bracket_type="angle",
                bracket_pairs_set="angle_bracket_pairs",
            ),
        ),
        # Include standard ANSI data types by inheriting parent match_grammar
        ansi.DatatypeSegment.match_grammar,
    )
