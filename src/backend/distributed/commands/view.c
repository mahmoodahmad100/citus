/*-------------------------------------------------------------------------
 *
 * view.c
 *    Commands for distributing CREATE OR REPLACE VIEW statements.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "fmgr.h"

#include "access/genam.h"
#include "catalog/objectaddress.h"
#include "commands/extension.h"
#include "distributed/commands.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/deparser.h"
#include "distributed/errormessage.h"
#include "distributed/listutils.h"
#include "distributed/metadata_sync.h"
#include "distributed/metadata/dependency.h"
#include "distributed/metadata/distobject.h"
#include "distributed/multi_executor.h"
#include "distributed/namespace_utils.h"
#include "distributed/worker_transaction.h"
#include "executor/spi.h"
#include "nodes/nodes.h"
#include "nodes/pg_list.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

static List * FilterNameListForDistributedViews(List *viewNamesList, bool missing_ok);
static void AppendQualifiedViewNameToCreateViewCommand(StringInfo buf, Oid viewOid);
static void AppendViewDefinitionToCreateViewCommand(StringInfo buf, Oid viewOid);
static void AppendAliasesToCreateViewCommand(StringInfo createViewCommand, Oid viewOid);
static void AppendOptionsToCreateViewCommand(StringInfo createViewCommand, Oid viewOid);
static StringInfo CreateAlterViewOwnerCommand(Oid viewOid);

/*
 * PreprocessViewStmt is called during the planning phase for CREATE OR REPLACE VIEW
 * before it is created on the local node internally.
 */
List *
PreprocessViewStmt(Node *node, const char *queryString,
				   ProcessUtilityContext processUtilityContext)
{
	if (!ShouldPropagate())
	{
		return NIL;
	}

	/* check creation against multi-statement transaction policy */
	if (!ShouldPropagateCreateInCoordinatedTransction())
	{
		return NIL;
	}

	EnsureCoordinator();

	return NIL;
}


/*
 * PostprocessViewStmt actually creates the commmands we need to run on workers to
 * propagate views.
 *
 * If view depends on any undistributable object, Citus can not distribute it. In order to
 * not to prevent users from creating local views on the coordinator WARNING message will
 * be sent to the customer about the case instead of erroring out. If no worker nodes exist
 * at all, view will be created locally without any WARNING message.
 *
 * Besides creating the plan we also make sure all (new) dependencies of the view are
 * created on all nodes.
 */
List *
PostprocessViewStmt(Node *node, const char *queryString)
{
	ViewStmt *stmt = castNode(ViewStmt, node);

	if (!ShouldPropagate())
	{
		return NIL;
	}

	/* check creation against multi-statement transaction policy */
	if (!ShouldPropagateCreateInCoordinatedTransction())
	{
		return NIL;
	}

	ObjectAddress viewAddress = GetObjectAddressFromParseTree((Node *) stmt, false);

	if (IsObjectAddressOwnedByExtension(&viewAddress, NULL))
	{
		return NIL;
	}

	/* If the view has any unsupported dependency, create it locally */
	DeferredErrorMessage *errMsg = DeferErrorIfHasUnsupportedDependency(&viewAddress);

	if (errMsg != NULL)
	{
		/*
		 * Don't need to give any warning/error messages if there is no worker nodes in
		 * the cluster as user's experience won't be affected on the single node even
		 * if the view won't be distributed.
		 */
		if (!HasAnyNodes())
		{
			return NIL;
		}

		/*
		 * If the view is already distributed, we should provide an error to not have
		 * different definition of view on coordinator and worker nodes. If the view
		 * is not distributed yet, we can create it locally to not affect user's local
		 * usage experience.
		 */
		if (IsObjectDistributed(&viewAddress))
		{
			RaiseDeferredError(errMsg, ERROR);
		}
		else
		{
			RaiseDeferredError(errMsg, WARNING);
			return NIL;
		}
	}

	EnsureDependenciesExistOnAllNodes(&viewAddress);

	List *commands = list_make1(DISABLE_DDL_PROPAGATION);
	commands = list_concat(commands,
						   CreateViewDDLCommandsIdempotent(viewAddress.objectId));
	commands = lappend(commands, ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(NON_COORDINATOR_NODES, commands);
}


/*
 * ViewStmtObjectAddress returns the ObjectAddress for the subject of the
 * CREATE [OR REPLACE] VIEW statement.
 */
ObjectAddress
ViewStmtObjectAddress(Node *node, bool missing_ok)
{
	ViewStmt *stmt = castNode(ViewStmt, node);

	Oid viewOid = RangeVarGetRelid(stmt->view, NoLock, missing_ok);

	ObjectAddress viewAddress = { 0 };
	ObjectAddressSet(viewAddress, RelationRelationId, viewOid);

	return viewAddress;
}


/*
 * PreprocessDropViewStmt gets called during the planning phase of a DROP VIEW statement
 * and returns a list of DDLJob's that will drop any distributed view from the
 * workers.
 *
 * The DropStmt could have multiple objects to drop, the list of objects will be filtered
 * to only keep the distributed views for deletion on the workers. Non-distributed
 * views will still be dropped locally but not on the workers.
 */
List *
PreprocessDropViewStmt(Node *node, const char *queryString, ProcessUtilityContext
					   processUtilityContext)
{
	DropStmt *stmt = castNode(DropStmt, node);

	if (!ShouldPropagate())
	{
		return NIL;
	}

	/*
	 * Our statements need to be fully qualified so we can drop them from the right schema
	 * on the workers
	 */
	QualifyTreeNode((Node *) stmt);

	List *distributedViews = FilterNameListForDistributedViews(stmt->objects,
															   stmt->missing_ok);

	if (list_length(distributedViews) < 1)
	{
		/* no distributed view to drop */
		return NIL;
	}

	EnsureCoordinator();

	/*
	 * Swap the list of objects before deparsing and restore the old list after. This
	 * ensures we only have distributed views in the deparsed drop statement.
	 */
	DropStmt *stmtCopy = copyObject(stmt);
	stmtCopy->objects = distributedViews;
	const char *dropStmtSql = DeparseTreeNode((Node *) stmtCopy);

	List *commands = list_make3(DISABLE_DDL_PROPAGATION,
								(void *) dropStmtSql,
								ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(NON_COORDINATOR_NODES, commands);
}


/*
 * FilterNameListForDistributedViews takes a list of view names and filters against the
 * views that are distributed. Given list of view names must be qualified before.
 *
 * The original list will not be touched, a new list will be created with only the objects
 * in there.
 */
static List *
FilterNameListForDistributedViews(List *viewNamesList, bool missing_ok)
{
	List *distributedViewNames = NIL;

	List *qualifiedViewName = NULL;
	foreach_ptr(qualifiedViewName, viewNamesList)
	{
		char *schemaName = strVal(linitial(qualifiedViewName));
		char *viewName = strVal(lsecond(qualifiedViewName));

		Oid schemaId = get_namespace_oid(schemaName, missing_ok);
		Oid viewOid = get_relname_relid(viewName, schemaId);

		if (!OidIsValid(viewOid))
		{
			continue;
		}

		ObjectAddress viewAddress = { 0 };
		ObjectAddressSet(viewAddress, RelationRelationId, viewOid);

		if (IsObjectDistributed(&viewAddress))
		{
			distributedViewNames = lappend(distributedViewNames, qualifiedViewName);
		}
	}
	return distributedViewNames;
}


/*
 * CreateViewDDLCommandsIdempotent returns a list of DDL statements (const char *) to be
 * executed on a node to recreate the view addressed by the viewAddress.
 */
List *
CreateViewDDLCommandsIdempotent(Oid viewOid)
{
	StringInfo createViewCommand = makeStringInfo();

	appendStringInfoString(createViewCommand, "CREATE OR REPLACE VIEW ");

	AppendQualifiedViewNameToCreateViewCommand(createViewCommand, viewOid);
	AppendAliasesToCreateViewCommand(createViewCommand, viewOid);
	AppendOptionsToCreateViewCommand(createViewCommand, viewOid);
	AppendViewDefinitionToCreateViewCommand(createViewCommand, viewOid);

	StringInfo alterOwnerCommand = CreateAlterViewOwnerCommand(viewOid);

	return list_make2(createViewCommand->data,
					  alterOwnerCommand->data);
}


/*
 * AppendQualifiedViewNameToCreateViewCommand adds the qualified view of the given view
 * oid to the given create view command.
 */
static void
AppendQualifiedViewNameToCreateViewCommand(StringInfo buf, Oid viewOid)
{
	char *viewName = get_rel_name(viewOid);
	char *schemaName = get_namespace_name(get_rel_namespace(viewOid));
	char *qualifiedViewName = quote_qualified_identifier(schemaName, viewName);

	appendStringInfo(buf, "%s ", qualifiedViewName);
}


/*
 * AppendAliasesToCreateViewCommand appends aliases to the create view
 * command for the existing view.
 */
static void
AppendAliasesToCreateViewCommand(StringInfo createViewCommand, Oid viewOid)
{
	/* Get column name aliases from pg_attribute */
	ScanKeyData key[1];
	ScanKeyInit(&key[0],
				Anum_pg_attribute_attrelid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(viewOid));

	Relation maprel = table_open(AttributeRelationId, AccessShareLock);
	Relation mapidx = index_open(AttributeRelidNumIndexId, AccessShareLock);
	SysScanDesc pgAttributeScan = systable_beginscan_ordered(maprel, mapidx, NULL, 1,
															 key);

	bool isInitialAlias = true;
	bool hasAlias = false;
	HeapTuple attributeTuple;
	while (HeapTupleIsValid(attributeTuple = systable_getnext_ordered(pgAttributeScan,
																	  ForwardScanDirection)))
	{
		Form_pg_attribute att = (Form_pg_attribute) GETSTRUCT(attributeTuple);
		const char *aliasName = quote_identifier(NameStr(att->attname));

		if (isInitialAlias)
		{
			appendStringInfoString(createViewCommand, "(");
		}
		else
		{
			appendStringInfoString(createViewCommand, ",");
		}

		appendStringInfoString(createViewCommand, aliasName);

		hasAlias = true;
		isInitialAlias = false;
	}

	if (hasAlias)
	{
		appendStringInfoString(createViewCommand, ") ");
	}

	systable_endscan_ordered(pgAttributeScan);
	index_close(mapidx, AccessShareLock);
	table_close(maprel, AccessShareLock);
}


/*
 * AppendOptionsToCreateViewCommand add relation options to create view command
 * for an existing view
 */
static void
AppendOptionsToCreateViewCommand(StringInfo createViewCommand, Oid viewOid)
{
	/* Add rel options to create view command */
	char *relOptions = flatten_reloptions(viewOid);
	if (relOptions != NULL)
	{
		appendStringInfo(createViewCommand, "WITH (%s) ", relOptions);
	}
}


/*
 * AppendViewDefinitionToCreateViewCommand adds the definition of the given view to the
 * given create view command.
 */
static void
AppendViewDefinitionToCreateViewCommand(StringInfo buf, Oid viewOid)
{
	/*
	 * Set search_path to NIL so that all objects outside of pg_catalog will be
	 * schema-prefixed.
	 */
	OverrideSearchPath *overridePath = GetOverrideSearchPath(CurrentMemoryContext);
	overridePath->schemas = NIL;
	overridePath->addCatalog = true;
	PushOverrideSearchPath(overridePath);

	/*
	 * Push the transaction snapshot to be able to get vief definition with pg_get_viewdef
	 */
	PushActiveSnapshot(GetTransactionSnapshot());

	Datum viewDefinitionDatum = DirectFunctionCall1(pg_get_viewdef,
													ObjectIdGetDatum(viewOid));
	char *viewDefinition = TextDatumGetCString(viewDefinitionDatum);

	PopActiveSnapshot();
	PopOverrideSearchPath();

	appendStringInfo(buf, "AS %s ", viewDefinition);
}


/*
 * CreateAlterViewOwnerCommand creates the command to alter view owner command for the
 * given view oid.
 */
static StringInfo
CreateAlterViewOwnerCommand(Oid viewOid)
{
	/* Add alter owner commmand */
	StringInfo alterOwnerCommand = makeStringInfo();

	char *viewName = get_rel_name(viewOid);
	Oid schemaOid = get_rel_namespace(viewOid);
	char *schemaName = get_namespace_name(schemaOid);

	char *viewOwnerName = TableOwner(viewOid);
	char *qualifiedViewName = NameListToQuotedString(list_make2(makeString(schemaName),
																makeString(viewName)));
	appendStringInfo(alterOwnerCommand,
					 "ALTER VIEW %s OWNER TO %s", qualifiedViewName,
					 quote_identifier(viewOwnerName));

	return alterOwnerCommand;
}


/*
 * PreprocessAlterViewStmt is invoked for alter view statements.
 */
List *
PreprocessAlterViewStmt(Node *node, const char *queryString, ProcessUtilityContext
						processUtilityContext)
{
	AlterTableStmt *stmt = castNode(AlterTableStmt, node);

	ObjectAddress viewAddress = GetObjectAddressFromParseTree((Node *) stmt, false);
	if (!ShouldPropagateObject(&viewAddress))
	{
		return NIL;
	}

	QualifyTreeNode((Node *) stmt);

	EnsureCoordinator();

	/* reconstruct alter statement in a portable fashion */
	const char *alterViewStmtSql = DeparseTreeNode((Node *) stmt);

	EnsureSequentialMode(OBJECT_VIEW);

	List *commands = list_make3(DISABLE_DDL_PROPAGATION,
								(void *) alterViewStmtSql,
								ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(NON_COORDINATOR_NODES, commands);
}


/*
 * PostprocessAlterViewStmt is invoked for alter view statements.
 */
List *
PostprocessAlterViewStmt(Node *node, const char *queryString)
{
	AlterTableStmt *stmt = castNode(AlterTableStmt, node);
	Assert(AlterTableStmtObjType_compat(stmt) == OBJECT_VIEW);

	ObjectAddress viewAddress = GetObjectAddressFromParseTree((Node *) stmt, false);
	if (!ShouldPropagateObject(&viewAddress))
	{
		return NIL;
	}

	if (IsObjectAddressOwnedByExtension(&viewAddress, NULL))
	{
		return NIL;
	}

	/* If the view has any unsupported dependency, create it locally */
	DeferredErrorMessage *errMsg = DeferErrorIfHasUnsupportedDependency(&viewAddress);

	if (errMsg != NULL)
	{
		/*
		 * Don't need to give any warning/error messages if there is no worker nodes in
		 * the cluster as user's experience won't be affected on the single node even
		 * if the view won't be distributed.
		 */
		if (!HasAnyNodes())
		{
			return NIL;
		}

		/*
		 * If the view is already distributed, we should provide an error to not have
		 * different definition of view on coordinator and worker nodes. If the view
		 * is not distributed yet, we can create it locally to not affect user's local
		 * usage experience.
		 */
		if (IsObjectDistributed(&viewAddress))
		{
			RaiseDeferredError(errMsg, ERROR);
		}
		else
		{
			RaiseDeferredError(errMsg, WARNING);
			return NIL;
		}
	}

	EnsureDependenciesExistOnAllNodes(&viewAddress);

	return NIL;
}


/*
 * AlterViewStmtObjectAddress returns the ObjectAddress for the subject of the
 * ALTER VIEW statement.
 */
ObjectAddress
AlterViewStmtObjectAddress(Node *node, bool missing_ok)
{
	AlterTableStmt *stmt = castNode(AlterTableStmt, node);
	Oid viewOid = RangeVarGetRelid(stmt->relation, NoLock, missing_ok);

	ObjectAddress viewAddress = { 0 };
	ObjectAddressSet(viewAddress, RelationRelationId, viewOid);

	return viewAddress;
}


/*
 * PreprocessRenameViewStmt is called when the user is renaming the view or the column of
 * the view.
 */
List *
PreprocessRenameViewStmt(Node *node, const char *queryString,
						 ProcessUtilityContext processUtilityContext)
{
	ObjectAddress typeAddress = GetObjectAddressFromParseTree(node, false);
	if (!ShouldPropagateObject(&typeAddress))
	{
		return NIL;
	}

	EnsureCoordinator();

	/* fully qualify */
	QualifyTreeNode(node);

	/* deparse sql*/
	const char *renameStmtSql = DeparseTreeNode(node);

	EnsureSequentialMode(OBJECT_VIEW);

	/* to prevent recursion with mx we disable ddl propagation */
	List *commands = list_make3(DISABLE_DDL_PROPAGATION,
								(void *) renameStmtSql,
								ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(NON_COORDINATOR_NODES, commands);
}


/*
 * RenameViewStmtObjectAddress returns the ObjectAddress of the view that is the object
 * of the RenameStmt. Errors if missing_ok is false.
 */
ObjectAddress
RenameViewStmtObjectAddress(Node *node, bool missing_ok)
{
	RenameStmt *stmt = castNode(RenameStmt, node);

	Oid viewOid = RangeVarGetRelid(stmt->relation, NoLock, missing_ok);

	ObjectAddress viewAddress = { 0 };
	ObjectAddressSet(viewAddress, RelationRelationId, viewOid);

	return viewAddress;
}


/*
 * PreprocessAlterViewSchemaStmt is executed before the statement is applied to the local
 * postgres instance.
 */
List *
PreprocessAlterViewSchemaStmt(Node *node, const char *queryString,
							  ProcessUtilityContext processUtilityContext)
{
	AlterObjectSchemaStmt *stmt = castNode(AlterObjectSchemaStmt, node);

	ObjectAddress typeAddress = GetObjectAddressFromParseTree((Node *) stmt, false);
	if (!ShouldPropagateObject(&typeAddress))
	{
		return NIL;
	}

	QualifyTreeNode((Node *) stmt);

	return NIL;
}


/*
 * PostprocessAlterViewSchemaStmt is executed after the change has been applied locally, we
 * can now use the new dependencies of the view to ensure all its dependencies exist on
 * the workers before we apply the commands remotely.
 */
List *
PostprocessAlterViewSchemaStmt(Node *node, const char *queryString)
{
	AlterObjectSchemaStmt *stmt = castNode(AlterObjectSchemaStmt, node);

	ObjectAddress viewAddress = GetObjectAddressFromParseTree((Node *) stmt, false);
	if (!ShouldPropagateObject(&viewAddress))
	{
		return NIL;
	}

	EnsureCoordinator();

	EnsureSequentialMode(OBJECT_VIEW);

	/* dependencies have changed (schema) let's ensure they exist */
	EnsureDependenciesExistOnAllNodes(&viewAddress);

	const char *sql = DeparseTreeNode((Node *) stmt);

	List *commands = list_make3(DISABLE_DDL_PROPAGATION,
								(void *) sql,
								ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(NON_COORDINATOR_NODES, commands);
}


/*
 * AlterViewSchemaStmtObjectAddress returns the ObjectAddress of the view that is the object
 * of the alter schema statement.
 */
ObjectAddress
AlterViewSchemaStmtObjectAddress(Node *node, bool missing_ok)
{
	AlterObjectSchemaStmt *stmt = castNode(AlterObjectSchemaStmt, node);

	Oid viewOid = RangeVarGetRelid(stmt->relation, NoLock, true);

	/*
	 * Since it can be called both before and after local execution, we need to check
	 * both old and new schemas
	 */
	if (viewOid == InvalidOid)
	{
		Oid schemaId = get_namespace_oid(stmt->newschema, missing_ok);
		viewOid = get_relname_relid(stmt->relation->relname, schemaId);

		/*
		 * if the view is still invalid we couldn't find the view, error with the same
		 * message postgres would error with it missing_ok is false (not ok to miss)
		 */
		if (!missing_ok && viewOid == InvalidOid)
		{
			ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
							errmsg("view \"%s\" does not exist",
								   stmt->relation->relname)));
		}
	}

	ObjectAddress viewAddress = { 0 };
	ObjectAddressSet(viewAddress, RelationRelationId, viewOid);

	return viewAddress;
}
