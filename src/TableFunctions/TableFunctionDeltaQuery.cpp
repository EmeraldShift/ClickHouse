#include <Core/Streaming/CursorTree.h>
#include <DataTypes/DataTypeFactory.h>
#include <Interpreters/convertFieldToType.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Storages/MergeTree/Streaming/DeltaQuery.h>
#include <Storages/StorageValues.h>
#include <Storages/StorageView.h>
#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/registerTableFunctions.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

/// A cursor of `DeltaQuery` as its flattened `CursorTree`, the form that `REFRESH ... INCREMENTAL` views store.
constexpr auto cursor_type_name = "Map(String, Int64)";

DataTypePtr getCursorType()
{
    return DataTypeFactory::instance().get(cursor_type_name);
}

CursorTreeNodePtr parseCursor(const ASTPtr & argument, const ContextPtr & context)
{
    auto [value, type] = evaluateConstantExpression(argument, context);
    return buildCursorTree(convertFieldToTypeOrThrow(value, *getCursorType(), type.get()).safeGet<Map>());
}

/// Shared by `deltaQuery` and `deltaQueryFrontier`, which both take Q as their first argument.
class TableFunctionDeltaQueryBase : public ITableFunction
{
protected:
    VectorWithMemoryTracking<size_t> skipAnalysisForArguments(const QueryTreeNodePtr &, ContextPtr) const override { return {0}; }

    void parseArguments(const ASTPtr & ast_function, ContextPtr /*context*/) override
    {
        const auto & function = ast_function->as<ASTFunction &>();
        size_t num_arguments = function.arguments ? function.arguments->children.size() : 0;
        const IAST * query = num_arguments ? function.arguments->children[0].get() : nullptr;
        if (const auto * subquery = query ? query->as<ASTSubquery>() : nullptr)
            query = subquery->children.at(0).get();
        if (!query || !query->as<ASTSelectWithUnionQuery>())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table function '{}' requires a query as its first argument", getName());
        if (const auto [min, max] = numCursorArguments(); num_arguments < 1 + min || num_arguments > 1 + max)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Table function '{}' takes a query and {} cursors",
                getName(), min == max ? fmt::format("{}", min) : fmt::format("{} or {}", min, max));
        delta_query = query->clone();
        cursor_arguments.assign(function.arguments->children.begin() + 1, function.arguments->children.end());
    }

    /// The least and the most cursors that the function takes.
    virtual std::pair<size_t, size_t> numCursorArguments() const = 0;

    ASTPtr delta_query;
    ASTs cursor_arguments;
};

class TableFunctionDeltaQuery : public TableFunctionDeltaQueryBase
{
public:
    static constexpr auto name = "deltaQuery";
    std::string getName() const override { return name; }

private:
    std::pair<size_t, size_t> numCursorArguments() const override { return {2, 2}; }
    const char * getStorageEngineName() const override { return "View"; }

    ColumnsDescription getActualTableStructure(ContextPtr context, bool /*is_insert_query*/) const override
    {
        DeltaQuery query(delta_query, context);
        return ColumnsDescription(query.getResultColumns());
    }

    StoragePtr executeImpl(const ASTPtr &, ContextPtr context, const String & table_name, ColumnsDescription cached_columns, bool) const override
    {
        DeltaQuery query(delta_query, context);
        ASTCreateQuery create;
        create.set(create.select, query.buildDelta(parseCursor(cursor_arguments[0], context), parseCursor(cursor_arguments[1], context)));
        /// Each term of the delta has the columns of the query. Analyzing the query once is much cheaper than analyzing every
        /// term, which is most of the planning of a query with many aggregates.
        ColumnsDescription columns = !cached_columns.empty() ? std::move(cached_columns) : ColumnsDescription(query.getResultColumns());
        auto storage = std::make_shared<StorageView>(StorageID(getDatabaseName(), table_name), create, columns, "");
        storage->startup();
        return storage;
    }
};

class TableFunctionDeltaQueryFrontier : public TableFunctionDeltaQueryBase
{
public:
    static constexpr auto name = "deltaQueryFrontier";
    std::string getName() const override { return name; }

private:
    std::pair<size_t, size_t> numCursorArguments() const override { return {0, 1}; }
    /// `StorageValues` is not a registered table engine, as in `TableFunctionValues`.
    const char * getStorageEngineName() const override { return ""; }

    ColumnsDescription getActualTableStructure(ContextPtr, bool) const override
    {
        return ColumnsDescription(NamesAndTypesList{{"cursor", getCursorType()}});
    }

    StoragePtr executeImpl(const ASTPtr &, ContextPtr context, const String & table_name, ColumnsDescription, bool) const override
    {
        auto type = getCursorType();
        auto column = type->createColumn();
        const auto from = cursor_arguments.empty() ? nullptr : parseCursor(cursor_arguments[0], context);
        column->insert(cursorTreeToMap(DeltaQuery(delta_query, context).getFrontier(from)));
        Block block{{std::move(column), type, "cursor"}};
        auto storage = std::make_shared<StorageValues>(
            StorageID(getDatabaseName(), table_name), getActualTableStructure(context, false), std::move(block));
        storage->startup();
        return storage;
    }
};

}

void registerTableFunctionDeltaQuery(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionDeltaQuery>({.description = R"DOCS_MD(
Returns the rows that a query gains from the blocks inserted between two cursors.

`deltaQuery(query, old_cursor, new_cursor)` runs `query` over the rows with `_block_number` from `old_cursor` up to `new_cursor`. The
query runs once per table occurrence, over the new rows of that occurrence joined with the rows of the occurrences before it as of
`old_cursor` and of the occurrences after it as of `new_cursor`. Merging the results over a chain of cursors gives the query evaluated
at the last cursor. Its aggregates are partial and merge like `SummingMergeTree` or `AggregatingMergeTree`.

A cursor has type `Map(String, Int64)`. Each key is a path `table_uuid.partition_id.block_number`, and its value is the first block number
of the partition that the cursor excludes. An empty map is the cursor before any insert. Get the current cursor with `deltaQueryFrontier`.

The tables must be `MergeTree` or `ReplicatedMergeTree` tables of Atomic databases with `enable_block_number_column = 1`, without
mutations or lightweight updates. A query may use filters, `INNER` and `CROSS` joins, and `GROUP BY`.

`target UNION ALL deltaQuery(query, cursor, frontier)` reads the result of a `REFRESH ... INCREMENTAL` view as of
now, where `cursor` is the column of `system.view_refreshes` and `frontier` is the cursor that `deltaQueryFrontier` returns. Each of its result columns is
either free of aggregates or one call of `sum`, `count`, their `-If` forms, `min`, `max`, `any`, `groupBitOr`, `groupBitAnd`, `groupBitXor`,
or any `-State` aggregate.

With the setting `delta_query_sign_column`, each table that has a column of that name is signed. A row of sign 1 inserts the row, and a row
of sign -1 retracts an earlier row with the same values. A query without aggregates returns the product of the signs of the signed tables
as a column of that name, for a `CollapsingMergeTree` target. A query with aggregates over signed tables supports only `count`, `sum`, their
`-If` forms and `exponentialMovingAverageState`. The moving average of a group is as of the latest time of any of its rows,
including retracted rows. The deltas sum to the result over the rows that remain, for a `SummingMergeTree`
target. `deltaQuery` doesn't support `HAVING`, `ORDER BY`, `LIMIT`, `DISTINCT`, windows or expressions over aggregates.

Besides the grants that `query` needs, `deltaQuery` needs `SELECT` on the virtual columns `_block_number`, `_partition_id` and `_part` of
the tables of `query`.
)DOCS_MD", .category = FunctionDocumentation::Category::TableFunction}, SecretArgumentsSpec{}, {.allow_readonly = true});

    factory.registerFunction<TableFunctionDeltaQueryFrontier>({.description = R"DOCS_MD(
Returns one row with column `cursor`, the cursor of the blocks that the server has committed to the tables of a query.
Pass it to `deltaQuery` as a cursor. `deltaQueryFrontier(query, cursor)` returns `cursor` extended by the blocks committed
since, so it is never behind `cursor`.
)DOCS_MD", .category = FunctionDocumentation::Category::TableFunction}, SecretArgumentsSpec{}, {.allow_readonly = true});
}

}
