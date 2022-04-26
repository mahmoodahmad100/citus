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
PostprocessCreateDistributedObjectStmt(Node *node, const char *queryString)
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

	ObjectAddress address = GetObjectAddressFromParseTree(node, false);
	EnsureDependenciesExistOnAllNodes(&address);

	return NIL;
}


List *
PreprocessAlterDistributedObjectStmt(Node *stmt, const char *queryString,
									 ProcessUtilityContext processUtilityContext)
{
	const DistributeObjectOps *ops = GetDistributeObjectOps(stmt);
	Assert(ops != NULL);

	/* Alter statements should always propagate and thus _not_ have a feature flag set */
	Assert(ops->featureFlag == NULL);

	ObjectAddress address = GetObjectAddressFromParseTree(stmt, false);
	if (!ShouldPropagateObject(&address))
	{
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
	/* Alter statements should always propagate and thus _not_ have a feature flag set */
	Assert(GetDistributeObjectOps(stmt)->featureFlag == NULL);

	ObjectAddress address = GetObjectAddressFromParseTree(stmt, false);
	if (!ShouldPropagateObject(&address))
	{
		return NIL;
	}

	EnsureDependenciesExistOnAllNodes(&address);

	return NIL;
}
