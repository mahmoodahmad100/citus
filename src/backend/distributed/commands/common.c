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
#include "distributed/listutils.h"
#include "distributed/metadata_sync.h"
#include "distributed/metadata/dependency.h"
#include "distributed/metadata/distobject.h"
#include "distributed/multi_executor.h"
#include "distributed/worker_transaction.h"


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


List *
PreprocessDropDistributedObjectStmt(Node *node, const char *queryString,
									ProcessUtilityContext processUtilityContext)
{
	DropStmt *stmt = castNode(DropStmt, node);

	/*
	 * We swap the list of objects to remove during deparse so we need a reference back to
	 * the old list to put back
	 */
	List *originalObjects = stmt->objects;

	if (!ShouldPropagate())
	{
		return NIL;
	}

	QualifyTreeNode(node);

	List *distributedObjects = NIL;
	List *distributedObjectAddresses = NIL;
	Node *object = NULL;
	foreach_ptr(object, stmt->objects)
	{
		/* TODO understand if the lock should be sth else */
		Relation rel = NULL; /* not used, but required to pass to get_object_address */
		ObjectAddress address = get_object_address(stmt->removeType, object, &rel,
												   AccessShareLock, stmt->missing_ok);
		if (IsObjectDistributed(&address))
		{
			ObjectAddress *addressPtr = palloc0(sizeof(ObjectAddress));
			*addressPtr = address;

			distributedObjects = lappend(distributedObjects, object);
			distributedObjectAddresses = lappend(distributedObjectAddresses, addressPtr);
		}
	}

	if (list_length(distributedObjects) <= 0)
	{
		/* no distributed objects to drop */
		return NIL;
	}

	/*
	 * managing objects can only be done on the coordinator if ddl propagation is on. when
	 * it is off we will never get here. MX workers don't have a notion of distributed
	 * types, so we block the call.
	 */
	EnsureCoordinator();

	/*
	 * remove the entries for the distributed objects on dropping
	 */
	ObjectAddress *address = NULL;
	foreach_ptr(address, distributedObjectAddresses)
	{
		UnmarkObjectDistributed(address);
	}

	/*
	 * temporary swap the lists of objects to delete with the distributed objects and
	 * deparse to an executable sql statement for the workers
	 */
	stmt->objects = distributedObjects;
	char *dropStmtSql = DeparseTreeNode((Node *) stmt);
	stmt->objects = originalObjects;

	EnsureSequentialMode(stmt->removeType);

	/* to prevent recursion with mx we disable ddl propagation */
	List *commands = list_make3(DISABLE_DDL_PROPAGATION,
								dropStmtSql,
								ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(NON_COORDINATOR_NODES, commands);
}
