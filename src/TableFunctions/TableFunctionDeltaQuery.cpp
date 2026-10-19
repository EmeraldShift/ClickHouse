#include <Access/Common/AccessFlags.h>
#include <Common/assert_cast.h>
#include <Core/Settings.h>
#include <Core/Streaming/CursorTree.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/convertFieldToType.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/MergeTree/AlterConversions.h>
#include <Storages/MergeTree/MergeTreeSequentialSource.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/MergeTree/Streaming/DeltaQuery.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/StorageProxy.h>
#include <Storages/StorageValues.h>
#include <Storages/StorageView.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/registerTableFunctions.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NOT_IMPLEMENTED;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace Setting
{
    extern const SettingsSeconds lock_acquire_timeout;
}

namespace
{

/// A cursor of `DeltaQuery` as its flattened `CursorTree`, the form that incremental refreshable materialized views store.
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
        if (num_arguments != 1 + numCursorArguments())
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Table function '{}' takes a query and {} cursors",
                getName(), numCursorArguments());
        delta_query = query->clone();
        cursor_arguments.assign(function.arguments->children.begin() + 1, function.arguments->children.end());
    }

    virtual size_t numCursorArguments() const = 0;

    ASTPtr delta_query;
    ASTs cursor_arguments;
};

/// The residual of a query runs over the integral of its core, which only a view keeps.
void checkNoResidual(const DeltaQuery & query)
{
    if (query.hasResidual())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "Table function deltaQuery does not support HAVING, ORDER BY, LIMIT, DISTINCT, windows or expressions over aggregates "
            "with delta_query_sign_column. Incremental refreshable materialized views support them");
}

class TableFunctionDeltaQuery : public TableFunctionDeltaQueryBase
{
public:
    static constexpr auto name = "deltaQuery";
    std::string getName() const override { return name; }

private:
    size_t numCursorArguments() const override { return 2; }
    const char * getStorageEngineName() const override { return "View"; }

    ColumnsDescription getActualTableStructure(ContextPtr context, bool /*is_insert_query*/) const override
    {
        DeltaQuery query(delta_query, context);
        checkNoResidual(query);
        return ColumnsDescription(InterpreterSelectQueryAnalyzer::getSampleBlock(query.getQuery()->clone(), context)->getNamesAndTypesList());
    }

    StoragePtr executeImpl(const ASTPtr &, ContextPtr context, const String & table_name, ColumnsDescription cached_columns, bool) const override
    {
        DeltaQuery query(delta_query, context);
        checkNoResidual(query);
        ASTCreateQuery create;
        create.set(create.select, query.buildDelta(parseCursor(cursor_arguments[0], context), parseCursor(cursor_arguments[1], context)));
        /// Each term of the delta has the columns of the query. Analyzing the query once is much cheaper than analyzing every
        /// term, which is most of the planning of a query with many aggregates.
        ColumnsDescription columns = !cached_columns.empty() ? std::move(cached_columns)
            : ColumnsDescription(InterpreterSelectQueryAnalyzer::getSampleBlock(query.getQuery()->clone(), context)->getNamesAndTypesList());
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
    size_t numCursorArguments() const override { return 0; }
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
        column->insert(cursorTreeToMap(DeltaQuery(delta_query, context).getFrontier(nullptr)));
        Block block{{std::move(column), type, "cursor"}};
        auto storage = std::make_shared<StorageValues>(
            StorageID(getDatabaseName(), table_name), getActualTableStructure(context, false), std::move(block));
        storage->startup();
        return storage;
    }
};

/// The bounds of each partition of a table, the first block number that the partition excludes, from a cursor-typed map.
std::map<String, Int64> parseBounds(const ASTPtr & argument, const ContextPtr & context)
{
    auto [value, type] = evaluateConstantExpression(argument, context);
    std::map<String, Int64> bounds;
    for (const auto & entry : convertFieldToTypeOrThrow(value, *getCursorType(), type.get()).safeGet<Map>())
    {
        const auto & pair = entry.safeGet<Tuple>();
        bounds.emplace(pair.at(0).safeGet<String>(), pair.at(1).safeGet<Int64>());
    }
    return bounds;
}

/// Shared by `deltaQuerySnapshot` and `deltaQueryPatches`, which both take a database, a `MergeTree` table and bounds.
class TableFunctionDeltaQueryTable : public ITableFunction
{
protected:
    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override
    {
        const auto & function = ast_function->as<ASTFunction &>();
        if (!function.arguments || function.arguments->children.size() != 2 + numBoundsArguments())
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Table function '{}' takes a database, a table and {} maps of bounds",
                getName(), numBoundsArguments());
        const auto & arguments = function.arguments->children;
        table_id = StorageID(
            checkAndGetLiteralArgument<String>(evaluateConstantExpressionForDatabaseName(arguments[0], context), "database"),
            checkAndGetLiteralArgument<String>(evaluateConstantExpressionOrIdentifierAsLiteral(arguments[1], context), "table"));
        for (size_t i = 2; i < arguments.size(); ++i)
            bounds.push_back(parseBounds(arguments[i], context));
    }

    virtual size_t numBoundsArguments() const = 0;

    /// `StorageDeltaQuerySnapshot` and `StorageDeltaQueryPatches` are not registered table engines.
    const char * getStorageEngineName() const override { return ""; }

    std::shared_ptr<StorageMergeTree> getTable(const ContextPtr & context) const
    {
        auto storage = castStorage<StorageMergeTree>(
            DatabaseCatalog::instance().getTable(context->resolveStorageID(table_id), context), DeferredTable::Load);
        if (!storage)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table function '{}' reads only MergeTree tables", getName());
        return storage;
    }

    StorageID table_id = StorageID::createEmpty();
    std::vector<std::map<String, Int64>> bounds;
};

/// A `MergeTree` table with the lightweight updates below `bounds`. It reads all the rows of the table, and the filters
/// of the delta query keep the rows below the bounds.
class StorageDeltaQuerySnapshot final : public IStorage
{
public:
    StorageDeltaQuerySnapshot(const StorageID & table_id_, std::shared_ptr<StorageMergeTree> table_, std::map<String, Int64> bounds_)
        : IStorage(table_id_), table(std::move(table_)), bounds(std::move(bounds_))
    {
    }

    std::string getName() const override { return "DeltaQuerySnapshot"; }
    bool readsFromOtherTables() const override { return true; }
    /// The row policies of the table apply.
    std::vector<StoragePtr> getUnderlyingStorages() const override { return {table}; }

    StorageMetadataHandle getInMemoryMetadataPtr(ContextPtr context, bool bypass_metadata_cache) const override
    {
        return table->getInMemoryMetadataPtr(context, bypass_metadata_cache);
    }

    bool supportsPrewhere() const override { return table->supportsPrewhere(); }
    std::optional<NameSet> supportedPrewhereColumns() const override { return table->supportedPrewhereColumns(); }
    bool supportedPrewhereColumnsIncludeSubcolumns() const override { return table->supportedPrewhereColumnsIncludeSubcolumns(); }
    bool supportsSubcolumns() const override { return table->supportsSubcolumns(); }
    bool supportsColumnsWithDynamicStructure() const override { return table->supportsColumnsWithDynamicStructure(); }

    /// The snapshot of the table that skips the patch parts at or above the bounds. A patch part that spans a bound would
    /// fail the snapshot, so they are checked first.
    StorageSnapshotPtr getStorageSnapshot(const StorageMetadataPtr & metadata_snapshot, ContextPtr query_context) const override
    {
        checkReadableAsOf(*table, table->getPatchPartsVectorForInternalUsage(), bounds);
        auto max_blocks = std::make_shared<PartitionIdToMaxBlock>();
        for (const auto & [partition_id, bound] : bounds)
            max_blocks->emplace(partition_id, bound - 1);
        auto context = Context::createCopy(query_context);
        context->setPartitionIdToMaxBlock(table->getStorageID().uuid, std::move(max_blocks));
        /// A snapshot shared by the query would have the updates of another bound.
        context->setSetting("enable_shared_storage_snapshot_in_query", false);
        auto snapshot = table->getStorageSnapshot(metadata_snapshot, context);

        DataPartsVector parts;
        for (const auto & part : *assert_cast<const MergeTreeData::SnapshotData &>(*snapshot->data).parts)
            parts.push_back(part.data_part);
        checkReadableAsOf(*table, parts, bounds);
        return snapshot->withStorageHolder(table);
    }

    /// Reads with the context of the query, whose maximum block numbers would skip the parts that merged across a bound.
    void read(QueryPlan & query_plan, const Names & column_names, const StorageSnapshotPtr & storage_snapshot, SelectQueryInfo & query_info,
        ContextPtr context, QueryProcessingStage::Enum processed_stage, size_t max_block_size, size_t num_streams) override
    {
        context->checkAccess(AccessType::SELECT, table->getStorageID(), column_names);
        auto lock = table->lockForShare(context->getCurrentQueryId(), context->getSettingsRef()[Setting::lock_acquire_timeout]);
        table->read(query_plan, column_names, storage_snapshot, query_info, context, processed_stage, max_block_size, num_streams);
        query_plan.addStorageHolder(table);
        query_plan.addTableLock(std::move(lock));
    }

private:
    std::shared_ptr<StorageMergeTree> table;
    std::map<String, Int64> bounds;
};

class TableFunctionDeltaQuerySnapshot : public TableFunctionDeltaQueryTable
{
public:
    static constexpr auto name = "deltaQuerySnapshot";
    std::string getName() const override { return name; }

private:
    size_t numBoundsArguments() const override { return 1; }

    ColumnsDescription getActualTableStructure(ContextPtr context, bool /*is_insert_query*/) const override
    {
        context->checkAccess(AccessType::SHOW_COLUMNS, context->resolveStorageID(table_id));
        auto metadata = getTable(context)->getInMemoryMetadataPtr(context, false);
        return metadata->getColumns();
    }

    StoragePtr executeImpl(const ASTPtr &, ContextPtr context, const String & table_name, ColumnsDescription, bool) const override
    {
        auto storage = std::make_shared<StorageDeltaQuerySnapshot>(StorageID(getDatabaseName(), table_name), getTable(context), bounds.at(0));
        storage->startup();
        return storage;
    }
};

/// The `_block_number` and `_block_offset` of the rows of a `MergeTree` table that its lightweight updates with versions in
/// [`lower`, `upper`) change. It holds the patch parts of those updates.
class StorageDeltaQueryPatches final : public IStorage
{
public:
    StorageDeltaQueryPatches(const StorageID & table_id_, const ColumnsDescription & columns, std::shared_ptr<StorageMergeTree> table_,
        const std::map<String, Int64> & lower, const std::map<String, Int64> & upper)
        : IStorage(table_id_), table(std::move(table_)), patches(getPatchParts(*table, lower, upper))
    {
        StorageInMemoryMetadata metadata;
        metadata.setColumns(columns);
        setInMemoryMetadata(metadata);
    }

    std::string getName() const override { return "DeltaQueryPatches"; }

    Pipe read(const Names & column_names, const StorageSnapshotPtr &, SelectQueryInfo &, ContextPtr context, QueryProcessingStage::Enum,
        size_t, size_t) override
    {
        context->checkAccess(AccessType::SELECT, table->getStorageID(), column_names);
        Pipes pipes;
        for (const auto & patch : patches)
            pipes.push_back(createMergeTreeSequentialSource(MergeTreeSequentialSourceType::Merge, *table,
                std::make_shared<StorageSnapshot>(*table, patch->getMetadataSnapshot()), RangesInDataPart(patch),
                std::make_shared<AlterConversions>(), /*merged_part_offsets=*/ nullptr, column_names, /*mark_ranges=*/ std::nullopt,
                /*filtered_rows_count=*/ nullptr, /*apply_deleted_mask=*/ false, /*read_with_direct_io=*/ false, /*prefetch=*/ false));
        return Pipe::unitePipes(std::move(pipes));
    }

private:
    std::shared_ptr<StorageMergeTree> table;
    DataPartsVector patches;
};

class TableFunctionDeltaQueryPatches : public TableFunctionDeltaQueryTable
{
public:
    static constexpr auto name = "deltaQueryPatches";
    std::string getName() const override { return name; }

private:
    size_t numBoundsArguments() const override { return 2; }
    bool hasStaticStructure() const override { return true; }

    ColumnsDescription getActualTableStructure(ContextPtr, bool) const override
    {
        return ColumnsDescription(NamesAndTypesList{
            {"_block_number", std::make_shared<DataTypeUInt64>()}, {"_block_offset", std::make_shared<DataTypeUInt64>()}});
    }

    StoragePtr executeImpl(const ASTPtr &, ContextPtr context, const String & table_name, ColumnsDescription, bool) const override
    {
        auto storage = std::make_shared<StorageDeltaQueryPatches>(StorageID(getDatabaseName(), table_name),
            getActualTableStructure(context, false), getTable(context), bounds.at(0), bounds.at(1));
        storage->startup();
        return storage;
    }
};

}

void registerTableFunctionDeltaQuery(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionDeltaQuery>({.description = R"DOCS_MD(
Returns the rows that a query gains from the blocks inserted between two cursors.

`deltaQuery(query, old_cursor, new_cursor)` runs `query` over the rows with `_block_number` from `old_cursor` up to `new_cursor`. A query
over one table runs over the new rows of the table. A query that joins tables runs once per table occurrence, over the new rows of that
occurrence joined with the rows of the occurrences before it as of `old_cursor` and of the occurrences after it as of `new_cursor`.
Merging the results of a join query over a chain of cursors gives the query evaluated at the last cursor. Its aggregates are partial and
merge like `SummingMergeTree` or `AggregatingMergeTree`.

A cursor has type `Map(String, Int64)`. Each key is a path `table_uuid.partition_id.block_number`, and its value is the first block number
of the partition that the cursor excludes. An empty map is the cursor before any insert. Get the current cursor with `deltaQueryFrontier`.

The tables must be `MergeTree` or `ReplicatedMergeTree` tables of Atomic databases with `enable_block_number_column = 1`. A query that joins tables may use filters, `INNER` and `CROSS` joins, and `GROUP BY`. With `delta_query_sign_column`
it may also use `LEFT`, `SEMI`, `ANTI` and `ASOF` joins of a table by `ON` with an equality of columns, and filters by `now()` in `WHERE`
over the columns of one table. Their deltas retract rows. The cursors of a query with `now()` have the key `now`, the time of the
snapshot.

The delta of a join reads each input other than the new rows by its join key, with `key IN (subquery)`. That read takes a range of
the primary key if the sorting key of the input starts with the join key, or if the input has a commit-order projection like
`PROJECTION p (SELECT *, _block_number ORDER BY key)` with `allow_commit_order_projection = 1`. Otherwise it reads the whole input.
Inserts don't write commit-order projections, so `OPTIMIZE TABLE ... FINAL` writes the projection for the existing parts, and the
deltas read the parts that haven't merged yet in full.

`target UNION ALL deltaQuery(query, cursor, frontier)` reads the result of an incremental refreshable materialized view without a residual
as of now, where `cursor` is the column of `system.view_refreshes` and `frontier` is the cursor that `deltaQueryFrontier` returns. Each of its result columns is
either free of aggregates or one call of `sum`, `count`, their `-If` forms, `min`, `max`, `any`, `groupBitOr`, `groupBitAnd`, `groupBitXor`,
or any `-State` aggregate.

With the setting `delta_query_sign_column`, each table that has a column of that name is signed. A row of sign 1 inserts the row, and a row
of sign -1 retracts an earlier row with the same values. A query without aggregates returns the product of the signs of the signed tables
as a column of that name, for a `CollapsingMergeTree` target. A query with aggregates over signed tables supports only `count`, `sum`, their
`-If` forms and `exponentialMovingAverageState`. The moving average of a group is as of the latest time of any of its rows,
including retracted rows. The deltas sum to the result over the rows that remain, for a `SummingMergeTree`
target. A query
over one table then follows the rules of a query that joins tables.
`deltaQuery` doesn't support `HAVING`, `ORDER BY`, `LIMIT`, `DISTINCT`, windows, expressions over aggregates or other aggregates with the
setting. Incremental refreshable materialized views support them.

A lightweight update of a `MergeTree` table takes a block number, and the table as of a cursor has the updates below the cursor. A query
with `delta_query_sign_column` reads the lightweight updates of the tables with `enable_block_offset_column = 1`, and of lightweight
deletes with `lightweight_delete_mode = 'lightweight_update'`. The delta retracts each row that the updates between the cursors change,
as of `old_cursor`, and inserts it as of `new_cursor`. A query without the setting throws `NOT_IMPLEMENTED` for such updates. The
tables of `ReplicatedMergeTree` don't support them. A merge that writes later updates into the rows below a cursor, or a mutation above
the cursor, loses the table as of the cursor, and the delta throws `DELTA_QUERY_HISTORY_LOST`. The delta throws `UNFINISHED` until
the mutations below its cursors are done.

A query can be `WITH RECURSIVE r AS (base UNION ALL step ...) SELECT * FROM r` without aggregates, where each step reads `r` once in
`FROM` through inner joins. The setting `delta_query_recursion_table` names the table that holds `r` as of `old_cursor`. The delta joins
the new rows of the inputs with the rows of that table, and then repeats the steps over the rows it found until a step returns no rows.
An incremental refreshable materialized view over such a query keeps `r` in its target, so the target must be a `MergeTree` with
`enable_block_number_column = 1`, with the sign column if the query sets `delta_query_sign_column`. A second view can read the target to aggregate `r`.

Besides the grants that `query` needs, `deltaQuery` needs `SELECT` on the virtual columns `_block_number`, `_partition_id` and `_part` of
the tables of `query`, and on `_block_offset` of the tables with lightweight updates.
)DOCS_MD", .category = FunctionDocumentation::Category::TableFunction}, SecretArgumentsSpec{}, {.allow_readonly = true});

    factory.registerFunction<TableFunctionDeltaQuerySnapshot>({.description = R"DOCS_MD(
Returns the rows of a `MergeTree` table with its lightweight updates below the bounds.
`deltaQuerySnapshot(database, table, bounds)` takes bounds of type `Map(String, Int64)` from a partition ID to the first block number
that the partition excludes. It returns every row of the table, also the rows at or above the bounds. The delta of `deltaQuery` reads
a table as of a cursor with it.
)DOCS_MD", .category = FunctionDocumentation::Category::TableFunction}, SecretArgumentsSpec{}, {.allow_readonly = true});

    factory.registerFunction<TableFunctionDeltaQueryPatches>({.description = R"DOCS_MD(
Returns the `_block_number` and `_block_offset` of the rows of a `MergeTree` table that its lightweight updates from the lower bounds
up to the upper bounds change. `deltaQueryPatches(database, table, lower, upper)` takes bounds like `deltaQuerySnapshot`. The delta of
`deltaQuery` reads the rows that change between its cursors with it.
)DOCS_MD", .category = FunctionDocumentation::Category::TableFunction}, SecretArgumentsSpec{}, {.allow_readonly = true});

    factory.registerFunction<TableFunctionDeltaQueryFrontier>({.description = R"DOCS_MD(
Returns one row with column `cursor`, the cursor of the blocks that the server has committed to the tables of a query.
Pass it to `deltaQuery` as a cursor.
)DOCS_MD", .category = FunctionDocumentation::Category::TableFunction}, SecretArgumentsSpec{}, {.allow_readonly = true});
}

}
