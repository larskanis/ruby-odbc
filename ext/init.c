/*
 * Part of ODBC-Ruby binding
 * Copyright (c) 2006-2007 Christian Werner <chw@ch-werner.de>
 *
 * See the file "COPYING" for information on usage
 * and redistribution of this file and for a
 * DISCLAIMER OF ALL WARRANTIES.
 *
 * $Id: init.c,v 1.6 2007/04/07 09:39:08 chw Exp chw $
 */

#include "ruby.h"

#ifdef USE_DLOPEN_FOR_ODBC_LIBS

/*
 * This module acts as a drop-in replacement for linking with
 * "-lodbc -lodbcinst" or "-liodbc -liodbcinst" when dlopen()
 * is supported.
 *
 * Setting the environment variable RUBY_ODBC_DM can be used
 * to force loading a specific driver manager shared library.
 * Same logic is used with RUBY_ODBC_INST for the ODBC installer
 * shared library.
 */

#include <dlfcn.h>

/* Create weak alias and function declarations. */

#define WEAKFUNC(name) \
    int __attribute__((weak, alias("__"#name))) name (void); \
    static int __attribute__((unused)) __ ## name (void) \
	{ return -1; /* == SQL_ERROR */ }

#define WEAKFUNC_BOOL(name) \
    int __attribute__((weak, alias("__"#name))) name (void); \
    static int __attribute__((unused)) __ ## name (void) \
	{ return 0; /* == BOOL/FALSE */ }

WEAKFUNC(SQLAllocConnect)
WEAKFUNC(SQLAllocEnv)
WEAKFUNC(SQLAllocStmt)
WEAKFUNC(SQLBindParameter)
WEAKFUNC(SQLCancel)
WEAKFUNC(SQLDescribeParam)
WEAKFUNC(SQLDisconnect)
WEAKFUNC(SQLExecute)
WEAKFUNC(SQLFetch)
WEAKFUNC(SQLFetchScroll)
WEAKFUNC(SQLFreeConnect)
WEAKFUNC(SQLFreeEnv)
WEAKFUNC(SQLFreeStmt)
WEAKFUNC(SQLGetData)
WEAKFUNC(SQLGetEnvAttr)
WEAKFUNC(SQLGetStmtOption)
WEAKFUNC(SQLMoreResults)
WEAKFUNC(SQLNumParams)
WEAKFUNC(SQLNumResultCols)
WEAKFUNC(SQLRowCount)
WEAKFUNC(SQLSetEnvAttr)
WEAKFUNC(SQLSetStmtOption)
WEAKFUNC(SQLTransact)
WEAKFUNC(SQLEndTran)

WEAKFUNC(SQLColAttributes)
WEAKFUNC(SQLColAttributesW)
WEAKFUNC(SQLColumns)
WEAKFUNC(SQLColumnsW)
WEAKFUNC(SQLConnect)
WEAKFUNC(SQLConnectW)
WEAKFUNC(SQLDataSources)
WEAKFUNC(SQLDataSourcesW)
WEAKFUNC(SQLDriverConnect)
WEAKFUNC(SQLDriverConnectW)
WEAKFUNC(SQLDrivers)
WEAKFUNC(SQLDriversW)
WEAKFUNC(SQLError)
WEAKFUNC(SQLErrorW)
WEAKFUNC(SQLExecDirect)
WEAKFUNC(SQLExecDirectW)
WEAKFUNC(SQLForeignKeys)
WEAKFUNC(SQLForeignKeysW)
WEAKFUNC(SQLGetConnectOption)
WEAKFUNC(SQLGetConnectOptionW)
WEAKFUNC(SQLGetCursorName)
WEAKFUNC(SQLGetCursorNameW)
WEAKFUNC(SQLGetInfo)
WEAKFUNC(SQLGetInfoW)
WEAKFUNC(SQLGetTypeInfo)
WEAKFUNC(SQLGetTypeInfoW)
WEAKFUNC(SQLPrepare)
WEAKFUNC(SQLPrepareW)
WEAKFUNC(SQLPrimaryKeys)
WEAKFUNC(SQLPrimaryKeysW)
WEAKFUNC(SQLProcedureColumns)
WEAKFUNC(SQLProcedureColumnsW)
WEAKFUNC(SQLProcedures)
WEAKFUNC(SQLProceduresW)
WEAKFUNC(SQLSetConnectOption)
WEAKFUNC(SQLSetConnectOptionW)
WEAKFUNC(SQLSetCursorName)
WEAKFUNC(SQLSetCursorNameW)
WEAKFUNC(SQLSpecialColumns)
WEAKFUNC(SQLSpecialColumnsW)
WEAKFUNC(SQLStatistics)
WEAKFUNC(SQLStatisticsW)
WEAKFUNC(SQLTablePrivileges)
WEAKFUNC(SQLTablePrivilegesW)
WEAKFUNC(SQLTables)
WEAKFUNC(SQLTablesW)
WEAKFUNC(SQLInstallerError)
WEAKFUNC(SQLInstallerErrorW)

WEAKFUNC_BOOL(SQLConfigDataSource)
WEAKFUNC_BOOL(SQLConfigDataSourceW)
WEAKFUNC_BOOL(SQLReadFileDSN)
WEAKFUNC_BOOL(SQLReadFileDSNW)
WEAKFUNC_BOOL(SQLWriteFileDSN)
WEAKFUNC_BOOL(SQLWriteFileDSNW)

/* Library initializer and finalizer. */

static void *lib_odbc = 0;
static void *lib_odbcinst = 0;

#define warn(msg) fputs(msg, stderr)

void
ruby_odbc_init()
{
    int useiodbc = 0;
    char *dm_name = getenv("RUBY_ODBC_DM");
    char *inst_name = getenv("RUBY_ODBC_INST");

    if (dm_name) {
	lib_odbc = dlopen(dm_name, RTLD_NOW | RTLD_GLOBAL);
	if (!lib_odbc) {
	    warn("WARNING: $RUBY_ODBC_DM not loaded.\n");
	} else {
	    if (inst_name) {
		lib_odbcinst = dlopen(inst_name, RTLD_NOW | RTLD_GLOBAL);
	    }
	    if (!lib_odbcinst) {
		warn("WARNING: $RUBY_ODBC_INST not loaded.\n");
	    }
	    return;
	}
    }
    lib_odbc = dlopen("libodbc" DLEXT ".1", RTLD_NOW | RTLD_GLOBAL);
    if (!lib_odbc) {
	lib_odbc = dlopen("libodbc" DLEXT, RTLD_NOW | RTLD_GLOBAL);
    }
    if (!lib_odbc) {
	lib_odbc = dlopen("libiodbc" DLEXT ".2", RTLD_NOW | RTLD_GLOBAL);
	if (!lib_odbc) {
	    lib_odbc = dlopen("libiodbc" DLEXT, RTLD_NOW | RTLD_GLOBAL);
	}
	if (!lib_odbc) {
	    warn("WARNING: no ODBC driver manager found.\n");
	    return;
	}
	useiodbc = 1;
    }
    lib_odbcinst = dlopen(useiodbc ?
			  "libiodbcinst" DLEXT ".2" : "libodbcinst" DLEXT ".1",
			  RTLD_NOW | RTLD_GLOBAL);
    if (!lib_odbcinst) {
	lib_odbcinst = dlopen(useiodbc ?
			      "libiodbcinst" DLEXT : "libodbcinst" DLEXT,
			      RTLD_NOW | RTLD_GLOBAL);
    }
    if (!lib_odbcinst) {
	warn("WARNING: no ODBC installer library found.\n");
    }
}

void
ruby_odbc_fini()
{
    if (lib_odbcinst) {
	dlclose(lib_odbcinst);
	lib_odbcinst = 0;
    }
    if (lib_odbc) {
	dlclose(lib_odbc);
	lib_odbc = 0;
    }
}

int
ruby_odbc_have_func(char *name, void *addr)
{
    return name && addr && (dlsym(NULL, name) != addr);
}

#endif
