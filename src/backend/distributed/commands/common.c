/*-------------------------------------------------------------------------
 *
 * common.c
 *
 *    Most of the object propagation code consists of mostly the same
 *    operations, varying slightly in parameters passed around. This
 *    file contains most of the reusable logic in object propagation.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/objectaddress.h"
#include "nodes/parsenodes.h"
#include "tcop/utility.h"

#include "distributed/commands.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/deparser.h"
#include "distributed/metadata_sync.h"
#include "distributed/multi_executor.h"
#include "distributed/worker_transaction.h"


List *
PreprocessCreateDistributedObjectStmt(Node *stmt, const char *queryString,
									  ProcessUtilityContext processUtilityContext)
{
	const DistributeObjectOps *ops = GetDistributeObjectOps(stmt);
	Assert(ops != NULL);

	if (!ShouldPropagate())
	{
		return NIL;
	}

	/* check creation against multi-statement transaction policy */
	if (!ShouldPropagateCreateInCoordinatedTransction())
	{
		return NIL;
	}

	if (ops->featureFlag && *ops->featureFlag == false)
	{
		/* not propagating when a configured feature flag is turned off by the user */
		return NIL;
	}

	EnsureCoordinator();
	EnsureSequentialMode(ops->objectType);

	QualifyTreeNode(stmt);
	char *sql = DeparseTreeNode(stmt);

	/* to prevent recursion with mx we disable ddl propagation */
	List *commands = list_make3(DISABLE_DDL_PROPAGATION,
								(void *) sql,
								ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(NON_COORDINATOR_NODES, commands);
}


List *
PostprocessCreateDistributedObjectStmt(Node *stmt, const char *queryString)
{
	const DistributeObjectOps *ops = GetDistributeObjectOps(stmt);
	Assert(ops != NULL);

	if (!ShouldPropagate())
	{
		return NIL;
	}

	/* check creation against multi-statement transaction policy */
	if (!ShouldPropagateCreateInCoordinatedTransction())
	{
		return NIL;
	}

	if (ops->featureFlag && *ops->featureFlag == false)
	{
		/* not propagating when a configured feature flag is turned off by the user */
		return NIL;
	}

	ObjectAddress address = GetObjectAddressFromParseTree(stmt, false);
	EnsureDependenciesExistOnAllNodes(&address);

	return NIL;
}


List *
PreprocessCreateDistributedObjectFromCatalogStmt(Node *node, const char *queryString,
												 ProcessUtilityContext processUtilityContext)
{
	QualifyTreeNode((Node *) node);

	return NIL;
}


List *
PostprocessCreateDistributedObjectFromCatalogStmt(Node *stmt, const char *queryString)
{
	const DistributeObjectOps *ops = GetDistributeObjectOps(stmt);
	Assert(ops != NULL);

	if (!ShouldPropagate())
	{
		return NIL;
	}

	/* check creation against multi-statement transaction policy */
	if (!ShouldPropagateCreateInCoordinatedTransction())
	{
		return NIL;
	}

	if (ops->featureFlag && *ops->featureFlag == false)
	{
		/* not propagating when a configured feature flag is turned off by the user */
		return NIL;
	}

	ObjectAddress address = GetObjectAddressFromParseTree(stmt, false);

	EnsureCoordinator();
	EnsureSequentialMode(ops->objectType);

	/* If the object has any unsupported dependency warn, and only create locally */
	DeferredErrorMessage *depError = DeferErrorIfHasUnsupportedDependency(&address);
	if (depError != NULL)
	{
		RaiseDeferredError(depError, WARNING);
		return NIL;
	}

	EnsureDependenciesExistOnAllNodes(&address);

	List *commands = GetDependencyCreateDDLCommands(&address);

	commands = lcons(DISABLE_DDL_PROPAGATION, commands);
	commands = lappend(commands, ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(NON_COORDINATOR_NODES, commands);
}


List *
PreprocessAlterDistributedObjectStmt(Node *stmt, const char *queryString,
									 ProcessUtilityContext processUtilityContext)
{
	const DistributeObjectOps *ops = GetDistributeObjectOps(stmt);
	Assert(ops != NULL);

	ObjectAddress address = GetObjectAddressFromParseTree(stmt, false);
	if (!ShouldPropagateObject(&address))
	{
		return NIL;
	}

	if (ops->featureFlag && *ops->featureFlag == false)
	{
		/* not propagating when a configured feature flag is turned off by the user */
		return NIL;
	}

	EnsureCoordinator();
	EnsureSequentialMode(ops->objectType);

	QualifyTreeNode(stmt);
	const char *sql = DeparseTreeNode((Node *) stmt);

	List *commands = list_make3(DISABLE_DDL_PROPAGATION,
								(void *) sql,
								ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(NON_COORDINATOR_NODES, commands);
}


List *
PostprocessAlterDistributedObjectStmt(Node *stmt, const char *queryString)
{
	const DistributeObjectOps *ops = GetDistributeObjectOps(stmt);
	Assert(ops != NULL);

	ObjectAddress address = GetObjectAddressFromParseTree(stmt, false);
	if (!ShouldPropagateObject(&address))
	{
		return NIL;
	}

	if (ops->featureFlag && *ops->featureFlag == false)
	{
		/* not propagating when a configured feature flag is turned off by the user */
		return NIL;
	}

	EnsureDependenciesExistOnAllNodes(&address);

	return NIL;
}
