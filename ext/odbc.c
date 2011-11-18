/*
 * ODBC-Ruby binding
 * Copyright (c) 2001-2011 Christian Werner <chw@ch-werner.de>
 * Portions copyright (c) 2004 Ryszard Niewisiewicz <micz@fibernet.pl>
 * Portions copyright (c) 2006 Carl Blakeley <cblakeley@openlinksw.co.uk>
 *
 * See the file "COPYING" for information on usage
 * and redistribution of this file and for a
 * DISCLAIMER OF ALL WARRANTIES.
 *
 * $Id: odbc.c,v 1.72 2011/01/15 08:02:55 chw Exp chw $
 */

#undef ODBCVER

#if defined(_WIN32) || defined(__CYGWIN32__) || defined(__MINGW32__)
#include <windows.h>
#endif
#include <stdarg.h>
#include <ctype.h>
#include "ruby.h"
#ifdef HAVE_VERSION_H
#include "version.h"
#endif
#ifdef HAVE_SQL_H
#include <sql.h>
#else
#error Missing include: sql.h
#endif
#ifdef HAVE_SQLEXT_H
#include <sqlext.h>
#else
#error Missing include: sqlext.h
#endif
#ifdef HAVE_ODBCINST_H
#include <odbcinst.h>
#endif

#ifdef UNICODE
#include <sqlucode.h>
#endif

#ifndef HAVE_TYPE_SQLTCHAR
#ifdef UNICODE
typedef SQLWCHAR SQLTCHAR;
#else
typedef SQLCHAR SQLTCHAR;
#endif
#endif

#ifndef HAVE_TYPE_SQLLEN
#define SQLLEN  SQLINTEGER
#endif
#ifndef HAVE_TYPE_SQLULEN
#define SQLULEN SQLUINTEGER
#endif

#if (RUBY_VERSION_MAJOR <= 1) && (RUBY_VERSION_MINOR < 9)
#define TIME_USE_USEC 1
#endif

/*
 * Conditionally undefine aliases of ODBC installer UNICODE functions.
 */

#if defined(UNICODE) && defined(HAVE_SQLINSTALLERERRORW)
#undef SQLInstallerError
#endif
#if defined(UNICODE) && defined(HAVE_SQLCONFIGDATASOURCEW)
#undef SQLConfigDataSource
#endif
#if defined(UNICODE) && defined(HAVE_SQLREADFILEDSNW)
#undef SQLReadFileDSN
#endif
#if defined(UNICODE) && defined(HAVE_SQLWRITEFILEDSNW)
#undef SQLWriteFileDSN
#endif

#if defined(UNICODE) && defined(USE_DLOPEN_FOR_ODBC_LIBS)
extern int ruby_odbc_have_func(const char *name, void *addr);
#endif

#ifdef UNICODE
/*
 * Declarations of required installer APIs in case
 * header files don't provide them (unixODBC?).
 */

#ifndef HAVE_SQLINSTALLERERRORW
SQLRETURN INSTAPI SQLInstallerErrorW(WORD, DWORD *, LPWSTR, WORD, WORD *);
#endif
#ifndef HAVE_SQLCONFIGDATASOURCEW
BOOL INSTAPI SQLConfigDataSourceW(HWND, WORD, LPWSTR, LPWSTR);
#endif
#ifndef HAVE_SQLREADFILEDSNW
BOOL INSTAPI SQLReadFileDSNW(LPWSTR, LPWSTR, LPWSTR, LPWSTR, WORD, WORD *);
#endif
#ifndef HAVE_SQLWRITEFILEDSNW
BOOL INSTAPI SQLWriteFileDSNW(LPWSTR, LPWSTR, LPWSTR, LPWSTR);
#endif

#if defined(HAVE_RUBY_ENCODING_H) && HAVE_RUBY_ENCODING_H
#define USE_RB_ENC 1
#include "ruby/encoding.h"
static rb_encoding *rb_enc = NULL;
static VALUE rb_encv = Qnil;
#endif

#endif /* UNICODE */

#ifndef HAVE_RB_DEFINE_ALLOC_FUNC
#define rb_define_alloc_func(cls, func) \
    rb_define_singleton_method(cls, "new", func, -1)
#define rb_undefine_alloc_func(cls) \
    rb_undef_method(CLASS_OF(cls), "new")
#endif

#ifdef RB_CVAR_SET_4ARGS
#define CVAR_SET(x, y, z) rb_cvar_set(x, y, z, 0)
#else
#define CVAR_SET(x, y, z) rb_cvar_set(x, y, z)
#endif

#ifndef STR2CSTR
#define STR2CSTR(x) StringValueCStr(x)
#define NO_RB_STR2CSTR 1
#endif

#ifdef TRACING
static int tracing = 0;
#define tracemsg(t, x) {if (tracing & t) { x }}
static SQLRETURN tracesql(SQLHENV henv, SQLHDBC hdbc, SQLHSTMT hstmt,
			  SQLRETURN ret, const char *m);
#else
#define tracemsg(t, x)
#define tracesql(a, b, c, d, e) d
#endif

#ifndef SQL_SUCCEEDED
#define SQL_SUCCEEDED(x) \
    (((x) == SQL_SUCCESS) || ((x) == SQL_SUCCESS_WITH_INFO))
#endif

#ifndef SQL_NO_DATA
#define SQL_NO_DATA SQL_NO_DATA_FOUND
#endif

typedef struct link {
    struct link *succ;
    struct link *pred;
    struct link *head;
    int offs;
} LINK;

typedef struct env {
    VALUE self;
    LINK dbcs;
    SQLHENV henv;
} ENV;

typedef struct dbc {
    LINK link;
    VALUE self;
    VALUE env;
    struct env *envp;
    LINK stmts; 
    SQLHDBC hdbc;
    VALUE rbtime;
    VALUE gmtime;
    int upc;
} DBC;

typedef struct {
    SQLSMALLINT type;
    SQLULEN coldef;
    SQLULEN coldef_max;
    SQLSMALLINT scale;
    SQLLEN rlen;
    SQLSMALLINT nullable;
    SQLSMALLINT iotype;
    int override;
#ifdef UNICODE
    SQLWCHAR *tofree;
#endif
    char buffer[sizeof (double) * 4 + sizeof (TIMESTAMP_STRUCT)];
    SQLSMALLINT ctype;
    SQLSMALLINT outtype;
    int outsize;
    char *outbuf;
} PARAMINFO;

typedef struct {
    int type;
    int size;
} COLTYPE;

typedef struct stmt {
    LINK link;
    VALUE self;
    VALUE dbc;
    struct dbc *dbcp;
    SQLHSTMT hstmt;
    int nump;
    PARAMINFO *paraminfo;
    int ncols;
    COLTYPE *coltypes;
    char **colnames;
    VALUE *colvals;
    char **dbufs;
    int fetchc;
    int upc;
    int usef;
} STMT;

static VALUE Modbc;
static VALUE Cobj;
static VALUE Cenv;
static VALUE Cdbc;
static VALUE Cstmt;
static VALUE Ccolumn;
static VALUE Cparam;
static VALUE Cerror;
static VALUE Cdsn;
static VALUE Cdrv;
static VALUE Cdate;
static VALUE Ctime;
static VALUE Ctimestamp;
static VALUE Cproc;
static VALUE rb_cDate;

static ID IDstart;
static ID IDatatinfo;
static ID IDataterror;
static ID IDkeys;
static ID IDatattrs;
static ID IDday;
static ID IDmonth;
static ID IDyear;
static ID IDmday;
static ID IDnsec;
static ID IDusec;
static ID IDsec;
static ID IDmin;
static ID IDhour;
static ID IDusec;
static ID IDkeyp;
static ID IDkey;
static ID IDSymbol;
static ID IDString;
static ID IDFixnum;
static ID IDtable_names;
static ID IDnew;
static ID IDnow;
static ID IDname;
static ID IDtable;
static ID IDtype;
static ID IDlength;
static ID IDnullable;
static ID IDscale;
static ID IDprecision;
static ID IDsearchable;
static ID IDunsigned;
static ID IDiotype;
static ID IDoutput_size;
static ID IDoutput_type;
static ID IDdescr;
static ID IDstatement;
static ID IDreturn_output_param;
static ID IDattrs;
static ID IDNULL;
static ID IDdefault;
#ifdef USE_RB_ENC
static ID IDencode;
#endif
static ID IDparse;
static ID IDutc;
static ID IDlocal;
static ID IDto_s;

/*
 * Modes for dbc_info
 */

#define INFO_TABLES   0
#define INFO_COLUMNS  1
#define INFO_PRIMKEYS 2
#define INFO_INDEXES  3
#define INFO_TYPES    4
#define INFO_FORKEYS  5
#define INFO_TPRIV    6
#define INFO_PROCS    7
#define INFO_PROCCOLS 8
#define INFO_SPECCOLS 9

/*
 * Modes for make_result/stmt_exec_int
 */

#define MAKERES_BLOCK   1
#define MAKERES_NOCLOSE 2
#define MAKERES_PREPARE 4
#define MAKERES_EXECD   8
#define EXEC_PARMXNULL(x) (16 | ((x) << 5))
#define EXEC_PARMXOUT(x)  (((x) & 16) ? ((x) >> 5) : -1)

/*
 * Modes for do_fetch
 */

#define DOFETCH_ARY    0
#define DOFETCH_HASH   1
#define DOFETCH_HASH2  2
#define DOFETCH_HASHK  3
#define DOFETCH_HASHK2 4
#define DOFETCH_HASHN  5
#define DOFETCH_MODES  7
#define DOFETCH_BANG   8

/*
 * Size of segment when SQL_NO_TOTAL
 */

#define SEGSIZE 65536

/*
 * Forward declarations.
 */

static SQLRETURN callsql(SQLHENV henv, SQLHDBC hdbc, SQLHSTMT hstmt,
			 SQLRETURN ret, const char *m);

static VALUE stmt_exec(int argc, VALUE *argv, VALUE self);
static VALUE stmt_each(VALUE self);
static VALUE stmt_each_hash(int argc, VALUE *argv, VALUE self);
static VALUE stmt_close(VALUE self);
static VALUE stmt_drop(VALUE self);

/*
 * Column name buffers on statement.
 */

static const char *colnamebuf[] = {
    "@_c0", "@_c1", "@_c2", "@_c3"
};

/*
 * Macro to align buffers.
 */

#define LEN_ALIGN(x) \
    ((x) + sizeof (double) - (((x) + sizeof (double)) % sizeof (double)))


/*
 *----------------------------------------------------------------------
 *
 *      UNICODE converters et.al.
 *
 *----------------------------------------------------------------------
 */

#ifdef UNICODE

static int
uc_strlen(SQLWCHAR *str)
{
    int len = 0;

    if (str != NULL) {
	while (*str != '\0') {
	    ++len;
	    ++str;
	}
    }
    return len;
}

static SQLWCHAR *
uc_strchr(SQLWCHAR *str, SQLWCHAR c)
{
    if (str != NULL) {
	while ((*str != '\0') && (*str != c)) {
	    ++str;
	}
	str = (*str == c) ? str : NULL;
    }
    return str;
}

static int
mkutf(char *dest, SQLWCHAR *src, int len)
{      
    int i;
    char *cp = dest;

    for (i = 0; i < len; i++) {
	unsigned long c = src[i];

	if (sizeof (SQLWCHAR) == (2 * sizeof (char))) {
	    c &= 0xffff;
	}
	if (c < 0x80) {
	    *cp++ = c;
	} else if (c < 0x800) {
	    *cp++ = 0xc0 | ((c >> 6) & 0x1f);
	    *cp++ = 0x80 | (c & 0x3f);
	} else if (c < 0x10000) {
	    if ((sizeof (SQLWCHAR) == (2 * sizeof (char))) &&
		(c >= 0xd800) && (c <= 0xdbff) && ((i + 1) < len)) {
		unsigned long c2 = src[i + 1] & 0xffff;

		if ((c2 >= 0xdc00) && (c <= 0xdfff)) {
		    c = ((c & 0x3ff) | ((c2 & 0x3ff) << 10)) + 0x10000;
		    *cp++ = 0xf0 | ((c >> 18) & 0x07);
		    *cp++ = 0x80 | ((c >> 12) & 0x3f);
		    *cp++ = 0x80 | ((c >> 6) & 0x3f);
		    *cp++ = 0x80 | (c & 0x3f);
		    ++i;
		    continue;
		}
	    }
	    *cp++ = 0xe0 | ((c >> 12) & 0x0f);
	    *cp++ = 0x80 | ((c >> 6) & 0x3f);
	    *cp++ = 0x80 | (c & 0x3f);
	} else if (c < 0x200000) {
	    *cp++ = 0xf0 | ((c >> 18) & 0x07);
	    *cp++ = 0x80 | ((c >> 12) & 0x3f);
	    *cp++ = 0x80 | ((c >> 6) & 0x3f);
	    *cp++ = 0x80 | (c & 0x3f);
	} else if (c < 0x4000000) {
	    *cp++ = 0xf8 | ((c >> 24) & 0x03);
	    *cp++ = 0x80 | ((c >> 18) & 0x3f);
	    *cp++ = 0x80 | ((c >> 12) & 0x3f);
	    *cp++ = 0x80 | ((c >> 6) & 0x3f);
	    *cp++ = 0x80 | (c & 0x3f);
	} else if (c < 0x80000000) {
	    *cp++ = 0xfc | ((c >> 31) & 0x01);
	    *cp++ = 0x80 | ((c >> 24) & 0x3f);
	    *cp++ = 0x80 | ((c >> 18) & 0x3f);
	    *cp++ = 0x80 | ((c >> 12) & 0x3f);
	    *cp++ = 0x80 | ((c >> 6) & 0x3f);
	    *cp++ = 0x80 | (c & 0x3f);
	}
    }
    *cp = '\0';
    return cp - dest;
}

static VALUE
uc_tainted_str_new(SQLWCHAR *str, int len)
{
    VALUE v;
    char *cp = xmalloc(len * 6 + 1);
    int ulen = 0;

    if ((cp != NULL) && (str != NULL)) {
	ulen = mkutf(cp, str, len);
    }
    v = rb_tainted_str_new((cp != NULL) ? cp : "", ulen);
#ifdef USE_RB_ENC
    rb_enc_associate(v, rb_enc);
#endif
    if (cp != NULL) {
	xfree(cp);
    }
    return v;
}

static VALUE
uc_tainted_str_new2(SQLWCHAR *str)
{
    return uc_tainted_str_new(str, uc_strlen(str));
}

static VALUE
uc_str_new(SQLWCHAR *str, int len)
{
    VALUE v;
    char *cp = xmalloc(len * 6 + 1);
    int ulen = 0;

    if ((cp != NULL) && (str != NULL)) {
	ulen = mkutf(cp, str, len);
    }
#ifdef USE_RB_ENC
    v = rb_enc_str_new((cp != NULL) ? cp : "", ulen, rb_enc);
#else
    v = rb_str_new((cp != NULL) ? cp : "", ulen);
#endif
    if (cp != NULL) {
	xfree(cp);
    }
    return v;
}

static VALUE
uc_str_new2(SQLWCHAR *str)
{
    return uc_str_new(str, uc_strlen(str));
}

static VALUE
uc_str_cat(VALUE v, SQLWCHAR *str, int len)
{
    VALUE vv = v;
    char *cp = xmalloc(len * 6 + 1);
    int ulen = 0;

    if ((cp != NULL) && (str != NULL)) {
	ulen = mkutf(cp, str, len);
    }
    if (cp != NULL) {
	vv = rb_str_cat(v, cp, ulen);
	xfree(cp);
    }
    return vv;
}

static SQLWCHAR *
uc_from_utf(unsigned char *str, int len)
{
    SQLWCHAR *uc = NULL;

    if (str != NULL) {
	int i = 0;
	unsigned char *strend;

	if (len < 0) {
	    len = strlen((char *) str);
	}
	strend = str + len;
	uc = ALLOC_N(SQLWCHAR, len + 1);
	if (uc != NULL) {
	    while (str < strend) {
		unsigned char c = str[0];

		if (c < 0x80) {
		    uc[i++] = c;
		    ++str;
		} else if ((c <= 0xc1) || (c >= 0xf5)) {
		    /* illegal, ignored */
		    ++str;
		} else if (c < 0xe0) {
		    if ((str[1] & 0xc0) == 0x80) {
			unsigned long t = ((c & 0x1f) << 6) | (str[1] & 0x3f);

			uc[i++] = t;
			str += 2;
		    } else {
			uc[i++] = c;
			++str;
		    }
		} else if (c < 0xf0) {
		    if (((str[1] & 0xc0) == 0x80) &&
			((str[2] & 0xc0) == 0x80)) {
			unsigned long t = ((c & 0x0f) << 12) |
			    ((str[1] & 0x3f) << 6) | (str[2] & 0x3f);

			uc[i++] = t;
			str += 3;
		    } else {
			uc[i++] = c;
			++str;
		    }
		} else if (c < 0xf8) {
		    if (((str[1] & 0xc0) == 0x80) &&
			((str[2] & 0xc0) == 0x80) &&
			((str[3] & 0xc0) == 0x80)) {
			unsigned long t = ((c & 0x03) << 18) |
			    ((str[1] & 0x3f) << 12) | ((str[2] & 0x3f) << 6) |
			    (str[4] & 0x3f);

			if ((sizeof (SQLWCHAR) == (2 * sizeof (char))) &&
			    (t >= 0x10000)) {
			    t -= 0x10000;
			    uc[i++] = 0xd800 | (t & 0x3ff);
			    t = 0xdc00 | ((t >> 10) & 0x3ff);
			}
			uc[i++] = t;
			str += 4;
		    } else {
			uc[i++] = c;
			++str;
		    }
		} else if (c < 0xfc) {
		    if (((str[1] & 0xc0) == 0x80) &&
			((str[2] & 0xc0) == 0x80) &&
			((str[3] & 0xc0) == 0x80) &&
			((str[4] & 0xc0) == 0x80)) {
			unsigned long t = ((c & 0x01) << 24) |
			    ((str[1] & 0x3f) << 18) | ((str[2] & 0x3f) << 12) |
			    ((str[4] & 0x3f) << 6) | (str[5] & 0x3f);

			if ((sizeof (SQLWCHAR) == (2 * sizeof (char))) &&
			    (t >= 0x10000)) {
			    t -= 0x10000;
			    uc[i++] = 0xd800 | (t & 0x3ff);
			    t = 0xdc00 | ((t >> 10) & 0x3ff);
			}
			uc[i++] = t;
			str += 5;
		    } else {
			uc[i++] = c;
			++str;
		    }
		} else {
		    /* ignore */
		    ++str;
		}
	    }
	    uc[i] = 0;
	}
    }
    return uc;
}

static void
uc_free(SQLWCHAR *str)
{
    if (str != NULL) {
	xfree(str);
    }
}

#endif


/*
 *----------------------------------------------------------------------
 *
 *      Things for ODBC::DSN
 *
 *----------------------------------------------------------------------
 */

#ifndef HAVE_RB_DEFINE_ALLOC_FUNC
static VALUE
dsn_new(VALUE self)
{
    VALUE obj = rb_obj_alloc(Cdsn);

    rb_obj_call_init(obj, 0, NULL);
    return obj;
}
#endif

static VALUE
dsn_init(VALUE self)
{
    rb_iv_set(self, "@name", Qnil);
    rb_iv_set(self, "@descr", Qnil);
    return self;
}

/*
 *----------------------------------------------------------------------
 *
 *      Things for ODBC::Driver
 *
 *----------------------------------------------------------------------
 */

#ifndef HAVE_RB_DEFINE_ALLOC_FUNC
static VALUE
drv_new(VALUE self)
{
    VALUE obj = rb_obj_alloc(Cdrv);

    rb_obj_call_init(obj, 0, NULL);
    return obj;
}
#endif

static VALUE
drv_init(VALUE self)
{
    rb_iv_set(self, "@name", Qnil);
    rb_iv_set(self, "@attrs", rb_hash_new());
    return self;
}

/*
 *----------------------------------------------------------------------
 *
 *      Cleanup routines and GC mark/free callbacks.
 *
 *----------------------------------------------------------------------
 */

static void
list_init(LINK *link, int offs)
{
    link->succ = link->pred = link->head = NULL;
    link->offs = offs;
}

static void
list_add(LINK *link, LINK *head)
{
    if (link->head != NULL) {
	rb_fatal("RubyODBC: already in list");
    }
    if (head == NULL) {
	rb_fatal("RubyODBC: invalid list head");
    }
    link->head = head;
    link->pred = NULL;
    link->succ = head->succ;
    head->succ = link;
    if (link->succ != NULL) {
	link->succ->pred = link;
    }
}

static void
list_del(LINK *link)
{
    if (link == NULL) {
	rb_fatal("RubyODBC: invalid list item");
    }
    if (link->head == NULL) {
	rb_fatal("RubyODBC: item not in list");
    }
    if (link->succ != NULL) {
	link->succ->pred = link->pred;
    }
    if (link->pred != NULL) {
	link->pred->succ = link->succ;
    } else {
	link->head->succ = link->succ;
    }
    link->succ = link->pred = link->head = NULL;
}

static void *
list_first(LINK *head)
{
    if (head->succ == NULL) {
	return NULL;
    }
    return (void *) ((char *) head->succ - head->offs);
}

static int
list_empty(LINK *head)
{
    return head->succ == NULL; 
}

static void
free_env(ENV *e)
{
    e->self = Qnil;
    if (!list_empty(&e->dbcs)) {
	return;
    }
    tracemsg(2, fprintf(stderr, "ObjFree: ENV %p\n", e););
    if (e->henv != SQL_NULL_HENV) {
	callsql(SQL_NULL_HENV, e->henv, SQL_NULL_HSTMT,
		SQLFreeEnv(e->henv), "SQLFreeEnv");
	e->henv = SQL_NULL_HENV;
    }
    xfree(e);
}

static void
link_dbc(DBC *p, ENV *e)
{
    p->envp = e;
    list_add(&p->link, &e->dbcs);
}

static void
unlink_dbc(DBC *p)
{
    if (p == NULL) {
	return;
    }
    p->env = Qnil;
    if (p->envp != NULL) {
	ENV *e = p->envp;

	list_del(&p->link);
	if (e->self == Qnil) {
	    free_env(e);
	}
	p->envp = NULL;
    }
}

static void
free_dbc(DBC *p)
{
    p->self = p->env = Qnil;
    if (!list_empty(&p->stmts)) {
	return;
    }
    tracemsg(2, fprintf(stderr, "ObjFree: DBC %p\n", p););
    if (p->hdbc != SQL_NULL_HDBC) {
	callsql(SQL_NULL_HENV, p->hdbc, SQL_NULL_HSTMT,
		SQLDisconnect(p->hdbc), "SQLDisconnect");
	callsql(SQL_NULL_HENV, p->hdbc, SQL_NULL_HSTMT,
		SQLFreeConnect(p->hdbc), "SQLFreeConnect");
	p->hdbc = SQL_NULL_HDBC;
    }
    unlink_dbc(p);
    xfree(p);
}

static void
free_stmt_sub(STMT *q, int withp)
{
    int i;

    if (withp) {
	if (q->paraminfo != NULL) {
	    for (i = 0; i < q->nump; i++) {
		if (q->paraminfo[i].outbuf != NULL) {
		    xfree(q->paraminfo[i].outbuf);
		}
	    }
	    xfree(q->paraminfo);
	    q->paraminfo = NULL;
	}
	q->nump = 0;
    }
    q->ncols = 0;
    if (q->coltypes != NULL) {
	xfree(q->coltypes);
	q->coltypes = NULL;
    }
    if (q->colnames != NULL) {
	xfree(q->colnames);
	q->colnames = NULL;
    }
    if (q->colvals != NULL) {
	xfree(q->colvals);
	q->colvals = NULL;
    }
    if (q->dbufs != NULL) {
	xfree(q->dbufs);
	q->dbufs = NULL;
    }
    if (q->self != Qnil) {
	VALUE v;

	v = rb_iv_get(q->self, "@_a");
	if (v != Qnil) {
	    rb_ary_clear(v);
	}
	v = rb_iv_get(q->self, "@_h");
	if (v != Qnil) {
	    rb_iv_set(q->self, "@_h", rb_hash_new());
	}
	for (i = 0; i < 4; i++) {
	    v = rb_iv_get(q->self, colnamebuf[i]);
	    if (v != Qnil) {
		rb_iv_set(q->self, colnamebuf[i], rb_hash_new());
	    }
	}
    }
}

static void
link_stmt(STMT *q, DBC *p)
{
    q->dbcp = p;
    list_add(&q->link, &p->stmts);
}

static void
unlink_stmt(STMT *q)
{
    if (q == NULL) {
	return;
    }
    q->dbc = Qnil;
    if (q->dbcp != NULL) {
	DBC *p = q->dbcp;

	list_del(&q->link);
	if (p->self == Qnil) {
	    free_dbc(p);
	}
	q->dbcp = NULL;
    }
}

static void
free_stmt(STMT *q)
{
    VALUE qself = q->self;

    q->self = q->dbc = Qnil;
    free_stmt_sub(q, 1);
    tracemsg(2, fprintf(stderr, "ObjFree: STMT %p\n", q););
    if (q->hstmt != SQL_NULL_HSTMT) {
	/* Issue warning message. */
	fprintf(stderr,	"WARNING: #<ODBC::Statement:0x%lx> was not dropped"
		" before garbage collection.\n", (long) qself);
	callsql(SQL_NULL_HENV, SQL_NULL_HDBC, q->hstmt,
		SQLFreeStmt(q->hstmt, SQL_DROP), "SQLFreeStmt(SQL_DROP)");
	q->hstmt = SQL_NULL_HSTMT;
    }
    unlink_stmt(q);
    xfree(q);
}

static void
start_gc()
{
    rb_funcall(rb_mGC, IDstart, 0, NULL);
}

static void
mark_dbc(DBC *p)
{
    if (p->env != Qnil) {
	rb_gc_mark(p->env);
    }
}

static void
mark_stmt(STMT *q)
{
    if (q->dbc != Qnil) {
	rb_gc_mark(q->dbc);
    }
}

/*
 *----------------------------------------------------------------------
 *
 *      Set internal error (or warning) message.
 *
 *----------------------------------------------------------------------
 */

static char *
set_err(const char *msg, int warn)
{
    VALUE a, v = rb_str_new2("INTERN (0) [RubyODBC]");

    v = rb_str_cat2(v, msg);
#ifdef USE_RB_ENC
    rb_enc_associate(v, rb_enc);
#endif
    a = rb_ary_new2(1);
    rb_ary_push(a, rb_obj_taint(v));
    CVAR_SET(Cobj, warn ? IDatatinfo : IDataterror, a);
    return STR2CSTR(v);
}

/*
 *----------------------------------------------------------------------
 *
 *      Functions to retrieve last SQL error or warning.
 *
 *----------------------------------------------------------------------
 */

static char *
get_err_or_info(SQLHENV henv, SQLHDBC hdbc, SQLHSTMT hstmt, int isinfo)
{
#ifdef UNICODE
    SQLWCHAR msg[SQL_MAX_MESSAGE_LENGTH], state[6 + 1];
#else
    SQLCHAR msg[SQL_MAX_MESSAGE_LENGTH], state[6 + 1];
#endif
    char buf[32], tmp[SQL_MAX_MESSAGE_LENGTH];
    SQLRETURN err;
    SQLINTEGER nativeerr;
    SQLSMALLINT len;
    VALUE v0 = Qnil, a = Qnil, v;
    int done = 0;

    while (!done) {
	v = Qnil;
	err = tracesql(henv, hdbc, hstmt,
		       SQLError(henv, hdbc, hstmt, state, &nativeerr, msg,
		       SQL_MAX_MESSAGE_LENGTH - 1, &len),
		       "SQLError");
	state[6] = '\0';
	msg[SQL_MAX_MESSAGE_LENGTH - 1] = '\0';
	switch (err) {
	case SQL_SUCCESS:
#ifdef UNICODE
	    v = uc_str_new2(state);
#else
	    v = rb_str_new2((char *) state);
#endif
	    sprintf(buf, " (%d) ", (int) nativeerr);
	    v = rb_str_cat2(v, buf);
#ifdef UNICODE
	    v = uc_str_cat(v, msg, len);
#else
	    v = rb_str_cat(v, (char *) msg, len);
#endif
	    break;
	case SQL_NO_DATA:
	    if ((v0 == Qnil) && (!isinfo)) {
		v = rb_str_new2("INTERN (0) [RubyODBC]No data found");
	    } else {
		v = Qnil;
	    }
	    done = 1;
	    break;
	case SQL_INVALID_HANDLE:
	    v = rb_str_new2("INTERN (0) [RubyODBC]Invalid handle");
	    done = 1;
	    break;
	case SQL_ERROR:
	    v = rb_str_new2("INTERN (0) [RubyODBC]Error reading error message");
	    done = 1;
	    break;
	default:
	    sprintf(tmp, "INTERN (0) [RubyODBC]Unknown error %d", (int) err);
	    v = rb_str_new2(tmp);
	    done = 1;
	    break;
	}
	if (v != Qnil) {
	    if (v0 == Qnil) {
		v0 = v;
		a = rb_ary_new();
	    }
	    rb_ary_push(a, rb_obj_taint(v));
	    tracemsg(1, fprintf(stderr, "  | %s\n", STR2CSTR(v)););
	}
    }
    CVAR_SET(Cobj, isinfo ? IDatatinfo : IDataterror, a);
    if (isinfo) {
	return NULL;
    }
    return (v0 == Qnil) ? NULL : STR2CSTR(v0);
}

#if defined(HAVE_SQLINSTALLERERROR) || (defined(UNICODE) && defined(HAVE_SQLINSTALLERERRORW))
static char *
get_installer_err()
{
#ifdef UNICODE
#ifdef HAVE_SQLINSTALLERERRORW
    int have_w = 1;
#else
    int have_w = 0;
#endif
    SQLWCHAR msg[SQL_MAX_MESSAGE_LENGTH];
#else
    char msg[SQL_MAX_MESSAGE_LENGTH];
#endif
    char buf[128];
    SQLRETURN err;
    VALUE v0 = Qnil, a = Qnil, v;
    int done = 0;
    WORD i, len;
    DWORD insterrcode;

    for (i = 1; (!done) && (i <= 8); i++) {
	v = Qnil;
#ifdef UNICODE
#ifdef USE_DLOPEN_FOR_ODBC_LIBS
	have_w = ruby_odbc_have_func("SQLInstallerErrorW", SQLInstallerErrorW);
#endif
	if (have_w) {
	    err = tracesql(SQL_NULL_HENV, SQL_NULL_HDBC, SQL_NULL_HSTMT,
			   SQLInstallerErrorW(i, &insterrcode, msg,
			   SQL_MAX_MESSAGE_LENGTH, &len),
			   "SQLInstallerErrorW");
	    msg[SQL_MAX_MESSAGE_LENGTH - 1] = 0;
	} else {
	    err = tracesql(SQL_NULL_HENV, SQL_NULL_HDBC, SQL_NULL_HSTMT,
			   SQLInstallerError(i, &insterrcode, (char *) msg,
			   SQL_MAX_MESSAGE_LENGTH, &len),
			   "SQLInstallerErrorW");
	    ((char *) msg)[SQL_MAX_MESSAGE_LENGTH - 1] = '\0';
	}
#else
	err = tracesql(SQL_NULL_HENV, SQL_NULL_HDBC, SQL_NULL_HSTMT,
		       SQLInstallerError(i, &insterrcode, msg,
					 SQL_MAX_MESSAGE_LENGTH, &len),
		       "SQLInstallerError");
	msg[SQL_MAX_MESSAGE_LENGTH - 1] = '\0';
#endif
	switch (err) {
	case SQL_SUCCESS:
	case SQL_SUCCESS_WITH_INFO:
	    sprintf(buf, "INSTALLER (%d) ", (int) insterrcode);
	    v = rb_str_new2(buf);
#ifdef UNICODE
	    if (have_w) {
#ifdef USE_RB_ENC
		rb_enc_associate(v, rb_enc);
#endif
		v = uc_str_cat(v, msg, len);
	    } else {
		v = rb_str_cat(v, (char *) msg, len);
	    }
#else
	    v = rb_str_cat(v, msg, len);
#endif
	    break;
	case SQL_NO_DATA:
	    done = 1;
	    break;
	case SQL_ERROR:
	    v = rb_str_new2("INTERN (0) [RubyODBC]");
	    v = rb_str_cat2(v, "Error reading installer error message");
	    done = 1;
	    break;
	default:
	    v = rb_str_new2("INTERN (0) [RubyODBC]");
	    sprintf(buf, "Unknown installer error %d", (int) err);
	    v = rb_str_cat2(v, buf);
	    done = 1;
	    break;
	}
	if (v != Qnil) {
	    if (v0 == Qnil) {
		v0 = v;
		a = rb_ary_new();
	    }
	    rb_ary_push(a, rb_obj_taint(v));
	    tracemsg(1, fprintf(stderr, "  | %s\n", STR2CSTR(v)););
	}
    }
    CVAR_SET(Cobj, IDataterror, a);
    return (v0 == Qnil) ? NULL : STR2CSTR(v0);
}
#endif

static char *
get_err(SQLHENV henv, SQLHDBC hdbc, SQLHSTMT hstmt)
{
    return get_err_or_info(henv, hdbc, hstmt, 0);
}

#ifdef TRACING
static void
trace_sql_ret(SQLRETURN ret)
{
    char msg[32];
    const char *p;

    switch (ret) {
    case SQL_SUCCESS:
	p = "SQL_SUCCESS";
	break;
    case SQL_SUCCESS_WITH_INFO:
	p = "SQL_SUCCESS_WITH_INFO";
	break;
    case SQL_NO_DATA:
	p = "SQL_NO_DATA";
	break;
    case SQL_ERROR:
	p = "SQL_ERROR";
	break;
    case SQL_INVALID_HANDLE:
	p = "SQL_INVALID_HANDLE";
	break;
    default:
	sprintf(msg, "SQL_RETURN=%d", (int) ret);
	p = msg;
	break;
    }
    fprintf(stderr, "  < %s\n", p);
}

static SQLRETURN
tracesql(SQLHENV henv, SQLHDBC hdbc, SQLHSTMT hstmt, SQLRETURN ret,
	 const char *m)
{
    if (tracing & 1) {
	fprintf(stderr, "SQLCall: %s", m);
	fprintf(stderr, "\n  > HENV=0x%lx, HDBC=0x%lx, HSTMT=0x%lx\n",
		(long) henv, (long) hdbc, (long) hstmt);
	trace_sql_ret(ret);
    }
    return ret;
}
#endif

static SQLRETURN
callsql(SQLHENV henv, SQLHDBC hdbc, SQLHSTMT hstmt, SQLRETURN ret,
	const char *m)
{
    SQLRETURN err;

    err = tracesql(henv, hdbc, hstmt, ret, m);
    if (err != SQL_SUCCESS) {
#ifdef UNICODE
	SQLWCHAR msg[SQL_MAX_MESSAGE_LENGTH], state[6 + 1];
#else
	SQLCHAR msg[SQL_MAX_MESSAGE_LENGTH], state[6 + 1];
#endif
	SQLINTEGER nativeerr;
	SQLSMALLINT len;
	int done = 0;

	while (!done) {
	    err = tracesql(henv, hdbc, hstmt,
			   SQLError(henv, hdbc, hstmt, state, &nativeerr, msg,
				    SQL_MAX_MESSAGE_LENGTH - 1, &len),
			   "SQLError");
	    switch (err) {
	    case SQL_SUCCESS:
		break;
	    case SQL_NO_DATA:
	    case SQL_INVALID_HANDLE:
	    case SQL_ERROR:
	    default:
		done = 1;
		break;
	    }
	}
    }
    return ret;
}

static int
succeeded_common(SQLHENV henv, SQLHDBC hdbc, SQLHSTMT hstmt, SQLRETURN ret,
		 char **msgp)
{
    if (!SQL_SUCCEEDED(ret)) {
	char *dummy;

	if (msgp == NULL) {
	    msgp = &dummy;
	}
	*msgp = get_err_or_info(henv, hdbc, hstmt, 0);
	return 0;
    }
    if (ret == SQL_SUCCESS_WITH_INFO) {
	get_err_or_info(henv, hdbc, hstmt, 1);
    } else {
	CVAR_SET(Cobj, IDatatinfo, Qnil);
    }
    return 1;
}

static int
succeeded(SQLHENV henv, SQLHDBC hdbc, SQLHSTMT hstmt, SQLRETURN ret,
	  char **msgp, const char *m, ...)
{
#ifdef TRACING
    va_list args;

    if (tracing & 1) {
	va_start(args, m);
	fprintf(stderr, "SQLCall: ");
	vfprintf(stderr, m, args);
	va_end(args);
	fprintf(stderr, "\n  > HENV=0x%lx, HDBC=0x%lx, HSTMT=0x%lx\n",
		(long) henv, (long) hdbc, (long) hstmt);
	trace_sql_ret(ret);
    }
#endif
    return succeeded_common(henv, hdbc, hstmt, ret, msgp);
}

static int
succeeded_nodata(SQLHENV henv, SQLHDBC hdbc, SQLHSTMT hstmt, SQLRETURN ret,
		 char **msgp, const char *m, ...)
{
#ifdef TRACING
    va_list args;

    if (tracing & 1) {
	va_start(args, m);
	fprintf(stderr, "SQLCall: ");
	vfprintf(stderr, m, args);
	va_end(args);
	fprintf(stderr, "\n  > HENV=0x%lx, HDBC=0x%lx, HSTMT=0x%lx\n",
		(long) henv, (long) hdbc, (long) hstmt);
	trace_sql_ret(ret);
    }
#endif
    if (ret == SQL_NO_DATA) {
	CVAR_SET(Cobj, IDatatinfo, Qnil);
	return 1;
    }
    return succeeded_common(henv, hdbc, hstmt, ret, msgp);
}

/*
 *----------------------------------------------------------------------
 *
 *      Return ENV from VALUE.
 *
 *----------------------------------------------------------------------
 */

static VALUE
env_of(VALUE self)
{
    if (rb_obj_is_kind_of(self, Cstmt) == Qtrue) {
	STMT *q;

	Data_Get_Struct(self, STMT, q);
	self = q->dbc;
	if (self == Qnil) {
	    rb_raise(Cerror, "%s", set_err("Stale ODBC::Statement", 0));
	}
    }
    if (rb_obj_is_kind_of(self, Cdbc) == Qtrue) {
	DBC *p;

	Data_Get_Struct(self, DBC, p);
	self = p->env;
	if (self == Qnil) {
	    rb_raise(Cerror, "%s", set_err("Stale ODBC::Database", 0));
	}
    }
    return self;
}

static ENV *
get_env(VALUE self)
{
    ENV *e;

    Data_Get_Struct(env_of(self), ENV, e);
    return e;
}

/*
 *----------------------------------------------------------------------
 *
 *      Return DBC from VALUE.
 *
 *----------------------------------------------------------------------
 */

static DBC *
get_dbc(VALUE self)
{
    DBC *p;

    if (rb_obj_is_kind_of(self, Cstmt) == Qtrue) {
	STMT *q;

	Data_Get_Struct(self, STMT, q);
	self = q->dbc;
	if (self == Qnil) {
	    rb_raise(Cerror, "%s", set_err("Stale ODBC::Statement", 0));
	}
    }
    Data_Get_Struct(self, DBC, p);
    return p;
}

/*
 *----------------------------------------------------------------------
 *
 *      Raise ODBC error from Ruby.
 *
 *----------------------------------------------------------------------
 */

static VALUE
dbc_raise(VALUE self, VALUE msg)
{
    VALUE a, v;
    char buf[SQL_MAX_MESSAGE_LENGTH + 1], *p;

    if (TYPE(msg) == T_STRING) {
	v = msg;
    } else {
	v = rb_any_to_s(msg);
    }
    strcpy(buf, "INTERN (1) [RubyODBC]");
    p = STR2CSTR(v);
    strncat(buf, p, SQL_MAX_MESSAGE_LENGTH - strlen(buf));
    buf[SQL_MAX_MESSAGE_LENGTH] = '\0';
    v = rb_str_new2(buf);
    a = rb_ary_new2(1);
    rb_ary_push(a, rb_obj_taint(v));
    CVAR_SET(Cobj, IDataterror, a);
    rb_raise(Cerror, "%s", buf);
    return Qnil;
}

/*
 *----------------------------------------------------------------------
 *
 *      Obtain an ENV.
 *
 *----------------------------------------------------------------------
 */

static VALUE
env_new(VALUE self)
{
    ENV *e;
    SQLHENV henv = SQL_NULL_HENV;
    VALUE obj;

    if (TYPE(self) == T_MODULE) {
	self = Cobj;
    }
    if (self == Cobj) {
	self = Cenv;
    }
    if ((!SQL_SUCCEEDED(SQLAllocEnv(&henv))) || (henv == SQL_NULL_HENV)) {
	rb_raise(Cerror, "%s", set_err("Cannot allocate SQLHENV", 0));
    }
    obj = Data_Make_Struct(self, ENV, NULL, free_env, e);
    tracemsg(2, fprintf(stderr, "ObjAlloc: ENV %p\n", e););
    e->self = obj;
    e->henv = henv;
    list_init(&e->dbcs, offsetof(DBC, link));
#if defined(UNICODE) && defined(SQL_OV_ODBC3)
    callsql(henv, SQL_NULL_HDBC, SQL_NULL_HSTMT,
	    SQLSetEnvAttr(henv, SQL_ATTR_ODBC_VERSION,
			  (SQLPOINTER) SQL_OV_ODBC3, 0),
	    "SQLSetEnvAttr(SQL_OV_ODBC3)");
#endif
    return obj;
}

/*
 *----------------------------------------------------------------------
 *
 *      Obtain array of known DSNs.
 *
 *----------------------------------------------------------------------
 */

static VALUE
dbc_dsns(VALUE self)
{
#ifdef UNICODE
    SQLWCHAR dsn[SQL_MAX_DSN_LENGTH], descr[SQL_MAX_MESSAGE_LENGTH * 2];
#else
    char dsn[SQL_MAX_DSN_LENGTH], descr[SQL_MAX_MESSAGE_LENGTH * 2];
#endif
    SQLSMALLINT dsnLen = 0, descrLen = 0;
    int first = 1;
    VALUE env, aret;
    ENV *e;

    env = env_new(Cenv);
    Data_Get_Struct(env, ENV, e);
    aret = rb_ary_new();
    while (succeeded(e->henv, SQL_NULL_HDBC, SQL_NULL_HSTMT,
		     SQLDataSources(e->henv, (SQLUSMALLINT) (first ?
				    SQL_FETCH_FIRST : SQL_FETCH_NEXT),
				    (SQLTCHAR *) dsn,
				    (SQLSMALLINT) sizeof (dsn), &dsnLen,
				    (SQLTCHAR *) descr,
				    (SQLSMALLINT) sizeof (descr),
				    &descrLen),
		     NULL, "SQLDataSources")) {
	VALUE odsn = rb_obj_alloc(Cdsn);

#ifdef UNICODE
	dsnLen = (dsnLen == 0) ? (SQLSMALLINT) uc_strlen(dsn) :
	    (SQLSMALLINT) (dsnLen / sizeof (SQLWCHAR));
	descrLen = (descrLen == 0) ?
	    (SQLSMALLINT) uc_strlen(descr) :
	    (SQLSMALLINT) (descrLen / sizeof (SQLWCHAR));
	rb_iv_set(odsn, "@name", uc_tainted_str_new(dsn, dsnLen));
	rb_iv_set(odsn, "@descr", uc_tainted_str_new(descr, descrLen));
#else
	dsnLen = (dsnLen == 0) ? (SQLSMALLINT) strlen(dsn) : dsnLen;
	descrLen = (descrLen == 0) ? (SQLSMALLINT) strlen(descr) : descrLen;
	rb_iv_set(odsn, "@name", rb_tainted_str_new(dsn, dsnLen));
	rb_iv_set(odsn, "@descr", rb_tainted_str_new(descr, descrLen));
#endif
	rb_ary_push(aret, odsn);
	first = dsnLen = descrLen = 0;
    }
    return aret;
}

/*
 *----------------------------------------------------------------------
 *
 *      Obtain array of known drivers.
 *
 *----------------------------------------------------------------------
 */

static VALUE
dbc_drivers(VALUE self)
{
#ifdef UNICODE
    SQLWCHAR driver[SQL_MAX_MESSAGE_LENGTH], attrs[SQL_MAX_MESSAGE_LENGTH * 2];
    SQLWCHAR *attr;
#else
    char driver[SQL_MAX_MESSAGE_LENGTH], attrs[SQL_MAX_MESSAGE_LENGTH * 2];
    char *attr;
#endif
    SQLSMALLINT driverLen = 0, attrsLen = 0;
    int first = 1;
    VALUE env, aret;
    ENV *e;

    env = env_new(Cenv);
    Data_Get_Struct(env, ENV, e);
    aret = rb_ary_new();
    while (succeeded(e->henv, SQL_NULL_HDBC, SQL_NULL_HSTMT,
		     SQLDrivers(e->henv, (SQLUSMALLINT) (first ?
				SQL_FETCH_FIRST : SQL_FETCH_NEXT),
				(SQLTCHAR *) driver,
				(SQLSMALLINT) sizeof (driver), &driverLen,
				(SQLTCHAR *) attrs,
				(SQLSMALLINT) sizeof (attrs), &attrsLen),
		     NULL, "SQLDrivers")) {
	VALUE odrv = rb_obj_alloc(Cdrv);
	VALUE h = rb_hash_new();
	int count = 0;

#ifdef UNICODE
	driverLen = (driverLen == 0) ?
	    (SQLSMALLINT) uc_strlen(driver) :
	    (SQLSMALLINT) (driverLen / sizeof (SQLWCHAR));
	rb_iv_set(odrv, "@name", uc_tainted_str_new(driver, driverLen));
	for (attr = attrs; *attr; attr += uc_strlen(attr) + 1) {
	    SQLWCHAR *p = uc_strchr(attr, (SQLWCHAR) '=');

	    if ((p != NULL) && (p != attr)) {
		rb_hash_aset(h,
			     uc_tainted_str_new(attr, (p - attr) /
						 sizeof (SQLWCHAR)),
			     uc_tainted_str_new2(p + 1));
		count++;
	    }
	}
#else
	driverLen = (driverLen == 0) ? (SQLSMALLINT) strlen(driver) : driverLen;
	rb_iv_set(odrv, "@name", rb_tainted_str_new(driver, driverLen));
	for (attr = attrs; *attr; attr += strlen(attr) + 1) {
	    char *p = strchr(attr, '=');

	    if ((p != NULL) && (p != attr)) {
		rb_hash_aset(h, rb_tainted_str_new(attr, p - attr),
			     rb_tainted_str_new2(p + 1));
		count++;
	    }
	}
#endif
	if (count > 0) {
	    rb_iv_set(odrv, "@attrs", h);
	}
	rb_ary_push(aret, odrv);
	first = driverLen = attrsLen = 0;
    }
    return aret;
}

/*
 *----------------------------------------------------------------------
 *
 *      Management methods.
 *
 *----------------------------------------------------------------------
 */

#ifdef HAVE_ODBCINST_H
static VALUE
conf_dsn(int argc, VALUE *argv, VALUE self, int op)
{
    VALUE drv, attr, issys, astr;
#ifdef UNICODE
#ifdef HAVE_SQLCONFIGDATASOURCEW
    int have_w = 1;
#else
    int have_w = 0;
#endif
    SQLWCHAR *sdrv, *sastr;
#else
    char *sdrv, *sastr;
#endif

    rb_scan_args(argc, argv, "12", &drv, &attr, &issys);
    if (rb_obj_is_kind_of(drv, Cdrv) == Qtrue) {
	VALUE a, x;

	if (argc > 2) {
	    rb_raise(rb_eArgError, "wrong # of arguments");
	}
	x = rb_iv_get(drv, "@name");
	a = rb_iv_get(drv, "@attrs");
	issys = attr;
	drv = x;
	attr = a;
    }
    Check_Type(drv, T_STRING);
    if (RTEST(issys)) {
	switch (op) {
	case ODBC_ADD_DSN:	op = ODBC_ADD_SYS_DSN; break;
	case ODBC_CONFIG_DSN:	op = ODBC_CONFIG_SYS_DSN; break;
	case ODBC_REMOVE_DSN:	op = ODBC_REMOVE_SYS_DSN; break;
	}
    }
    astr = rb_str_new2("");
    if (rb_obj_is_kind_of(attr, rb_cHash) == Qtrue) {
	VALUE a, x;

	a = rb_funcall(attr, IDkeys, 0, NULL);
	while ((x = rb_ary_shift(a)) != Qnil) {
	    VALUE v = rb_hash_aref(attr, x);

	    astr = rb_str_concat(astr, x);
	    astr = rb_str_cat2(astr, "=");
	    astr = rb_str_concat(astr, v);
	    astr = rb_str_cat(astr, "", 1);
	}
    }
    astr = rb_str_cat(astr, "", 1);
#ifdef UNICODE
#ifdef USE_DLOPEN_FOR_ODBC_LIBS
    have_w = ruby_odbc_have_func("SQLConfigDataSourceW", SQLConfigDataSourceW);
#endif
    if (have_w) {
#ifdef USE_RB_ENC
	drv = rb_funcall(drv, IDencode, 1, rb_encv);
	astr = rb_funcall(astr, IDencode, 1, rb_encv);
#endif
	sdrv = uc_from_utf((unsigned char *) STR2CSTR(drv), -1);
	sastr = uc_from_utf((unsigned char *) STR2CSTR(astr), -1);
	if ((sdrv == NULL) || (sastr == NULL)) {
	    uc_free(sdrv);
	    uc_free(sastr);
	    rb_raise(Cerror, "%s", set_err("Out of memory", 0));
	}
	if (SQLConfigDataSourceW(NULL, (WORD) op,
				 (LPWSTR) sdrv, (LPWSTR) sastr)) {
	    uc_free(sdrv);
	    uc_free(sastr);
	    return Qnil;
	}
	uc_free(sdrv);
	uc_free(sastr);
    } else {
	sdrv = (SQLWCHAR *) STR2CSTR(drv);
	sastr = (SQLWCHAR *) STR2CSTR(astr);
	if (SQLConfigDataSource(NULL, (WORD) op,
				(LPCSTR) sdrv, (LPCSTR) sastr)) {
	    return Qnil;
	}
    }
#else
    sdrv = STR2CSTR(drv);
    sastr = STR2CSTR(astr);
    if (SQLConfigDataSource(NULL, (WORD) op, sdrv, sastr)) {
	return Qnil;
    }
#endif
#if defined(HAVE_SQLINSTALLERERROR) || (defined(UNICODE) && defined(HAVE_SQLINSTALLERERRORW))
    rb_raise(Cerror, "%s", set_err(get_installer_err(), 0));
#else
    rb_raise(Cerror, "%s", set_err("DSN configuration error", 0));
#endif
    return Qnil;
}
#endif

static VALUE
dbc_adddsn(int argc, VALUE *argv, VALUE self)
{
#ifdef HAVE_ODBCINST_H
    return conf_dsn(argc, argv, self, ODBC_ADD_DSN);
#else
    rb_raise(Cerror, "%s", set_err("ODBC::add_dsn not supported", 0));
    return Qnil;
#endif
}

static VALUE
dbc_confdsn(int argc, VALUE *argv, VALUE self)
{
#ifdef HAVE_ODBCINST_H
    return conf_dsn(argc, argv, self, ODBC_CONFIG_DSN);
#else
    rb_raise(Cerror, "%s", set_err("ODBC::config_dsn not supported", 0));
    return Qnil;
#endif
}

static VALUE
dbc_deldsn(int argc, VALUE *argv, VALUE self)
{
#ifdef HAVE_ODBCINST_H
    return conf_dsn(argc, argv, self, ODBC_REMOVE_DSN);
#else
    rb_raise(Cerror, "%s", set_err("ODBC::del_dsn not supported", 0));
    return Qnil;
#endif
}

static VALUE
dbc_wfdsn(int argc, VALUE *argv, VALUE self)
{
#ifdef HAVE_ODBCINST_H
    VALUE fname, aname, kname, val;
#ifdef UNICODE
#ifdef HAVE_SQLWRITEFILEDSNW
    int have_w = 1;
#else
    int have_w = 0;
#endif
    SQLWCHAR *sfname, *saname, *skname, *sval = NULL;
#else
    char *sfname, *saname, *skname, *sval = NULL;
#endif

    rb_scan_args(argc, argv, "31", &fname, &aname, &kname, &val);
    Check_Type(fname, T_STRING);
    Check_Type(aname, T_STRING);
    Check_Type(kname, T_STRING);
    if (val != Qnil) {
	Check_Type(val, T_STRING);
    }
#ifdef UNICODE
#ifdef USE_DLOPEN_FOR_ODBC_LIBS
    have_w = ruby_odbc_have_func("SQLWriteFileDSNW", SQLWriteFileDSNW);
#endif
    if (have_w) {
	BOOL rc;

#ifdef USE_RB_ENC
	fname = rb_funcall(fname, IDencode, 1, rb_encv);
	aname = rb_funcall(aname, IDencode, 1, rb_encv);
	kname = rb_funcall(kname, IDencode, 1, rb_encv);
	if (val != Qnil) {
	    val = rb_funcall(val, IDencode, 1, rb_encv);
	}
#endif
	sfname = uc_from_utf((unsigned char *) STR2CSTR(fname), -1);
	saname = uc_from_utf((unsigned char *) STR2CSTR(aname), -1);
	skname = uc_from_utf((unsigned char *) STR2CSTR(kname), -1);
	if ((sfname == NULL) || (saname == NULL) || (skname == NULL)) {
nomem:
	    uc_free(sfname);
	    uc_free(saname);
	    uc_free(skname);
	    rb_raise(Cerror, "%s", set_err("Out of memory", 0));
	}
	if (val != Qnil) {
	    sval = uc_from_utf((unsigned char *) STR2CSTR(val), -1);
	    if (sval == NULL) {
		goto nomem;
	    }
	}
	rc = SQLWriteFileDSNW(sfname, saname, skname, sval);
	uc_free(sfname);
	uc_free(saname);
	uc_free(skname);
	uc_free(sval);
	if (rc) {
	    return Qnil;
	}
    } else {
	sfname = (SQLWCHAR *) STR2CSTR(fname);
	saname = (SQLWCHAR *) STR2CSTR(aname);
	skname = (SQLWCHAR *) STR2CSTR(kname);
	if (val != Qnil) {
	    sval = (SQLWCHAR *) STR2CSTR(val);
	}
	if (SQLWriteFileDSN((LPCSTR) sfname, (LPCSTR) saname,
			    (LPCSTR) skname, (LPCSTR) sval)) {
	    return Qnil;
	}
    }
#else
    sfname = STR2CSTR(fname);
    saname = STR2CSTR(aname);
    skname = STR2CSTR(kname);
    if (val != Qnil) {
	sval = STR2CSTR(val);
    }
    if (SQLWriteFileDSN(sfname, saname, skname, sval)) {
	return Qnil;
    }
#endif
#if defined(HAVE_SQLINSTALLERERROR) || (defined(UNICODE) && defined(HAVE_SQLINSTALLERERRORW))
    rb_raise(Cerror, "%s", set_err(get_installer_err(), 0));
#else
    rb_raise(Cerror, "%s", set_err("File DSN configuration error", 0));
#endif
#else
    rb_raise(Cerror, "%s", set_err("ODBC::write_file_dsn not supported", 0));
#endif
    return Qnil;
}

static VALUE
dbc_rfdsn(int argc, VALUE *argv, VALUE self)
{
#ifdef HAVE_ODBCINST_H
    VALUE fname, aname, kname;
#ifdef UNICODE
#ifdef HAVE_SQLREADFILEDSNW
    int have_w = 1;
#else
    int have_w = 0;
#endif
    SQLWCHAR *sfname, *saname, *skname, valbuf[SQL_MAX_MESSAGE_LENGTH];
#else
    char *sfname, *saname, *skname, valbuf[SQL_MAX_MESSAGE_LENGTH];
#endif

    rb_scan_args(argc, argv, "30", &fname, &aname, &kname);
    Check_Type(fname, T_STRING);
    Check_Type(aname, T_STRING);
    Check_Type(kname, T_STRING);
#ifdef UNICODE
#ifdef USE_DLOPEN_FOR_ODBC_LIBS
    have_w = ruby_odbc_have_func("SQLReadFileDSNW", SQLReadFileDSNW);
#endif
    if (have_w) {
	BOOL rc;

#ifdef USE_RB_ENC
	fname = rb_funcall(fname, IDencode, 1, rb_encv);
	aname = rb_funcall(aname, IDencode, 1, rb_encv);
	kname = rb_funcall(kname, IDencode, 1, rb_encv);
#endif
	sfname = uc_from_utf((unsigned char *) STR2CSTR(fname), -1);
	saname = uc_from_utf((unsigned char *) STR2CSTR(aname), -1);
	skname = uc_from_utf((unsigned char *) STR2CSTR(kname), -1);
	valbuf[0] = 0;
	if ((sfname == NULL) || (saname == NULL) || (skname == NULL)) {
	    uc_free(sfname);
	    uc_free(saname);
	    uc_free(skname);
	    rb_raise(Cerror, "%s", set_err("Out of memory", 0));
	}
	rc = SQLReadFileDSNW(sfname, saname, skname, valbuf,
			     sizeof (valbuf), NULL);
	uc_free(sfname);
	uc_free(saname);
	uc_free(skname);
	if (rc) {
	    return uc_tainted_str_new2(valbuf);
	}
    } else {
	sfname = (SQLWCHAR *) STR2CSTR(fname);
	saname = (SQLWCHAR *) STR2CSTR(aname);
	skname = (SQLWCHAR *) STR2CSTR(kname);
	valbuf[0] = '\0';
	if (SQLReadFileDSN((LPCSTR) sfname, (LPCSTR) saname,
			   (LPCSTR) skname, (LPSTR) valbuf,
			   sizeof (valbuf), NULL)) {
	    return rb_tainted_str_new2((char *) valbuf);
	}
    }
#else
    sfname = STR2CSTR(fname);
    saname = STR2CSTR(aname);
    skname = STR2CSTR(kname);
    valbuf[0] = '\0';
    if (SQLReadFileDSN(sfname, saname, skname, valbuf,
		       sizeof (valbuf), NULL)) {
	return rb_tainted_str_new2(valbuf);
    }
#endif
#if defined(HAVE_SQLINSTALLERERROR) || (defined(UNICODE) && defined(HAVE_SQLINSTALLERERRORW))
    rb_raise(Cerror, "%s", set_err(get_installer_err(), 0));
#else
    rb_raise(Cerror, "%s", set_err("File DSN configuration error", 0));
#endif
#else
    rb_raise(Cerror, "%s", set_err("ODBC::read_file_dsn not supported", 0));
    return Qnil;
#endif
}

/*
 *----------------------------------------------------------------------
 *
 *      Return last ODBC error or warning.
 *
 *----------------------------------------------------------------------
 */

static VALUE
dbc_error(VALUE self)
{
    return rb_cvar_get(Cobj, IDataterror);
}

static VALUE
dbc_warn(VALUE self)
{
    return rb_cvar_get(Cobj, IDatatinfo);
}

static VALUE
dbc_clrerror(VALUE self)
{
    CVAR_SET(Cobj, IDataterror, Qnil);
    CVAR_SET(Cobj, IDatatinfo, Qnil);
    return Qnil;
}

/*
 *----------------------------------------------------------------------
 *
 *      Connection instance initializer.
 *
 *----------------------------------------------------------------------
 */

#ifdef HAVE_RB_DEFINE_ALLOC_FUNC
static VALUE
dbc_alloc(VALUE self)
{
    DBC *p;
    VALUE obj = Data_Make_Struct(self, DBC, mark_dbc, free_dbc, p);

    tracemsg(2, fprintf(stderr, "ObjAlloc: DBC %p\n", p););
    list_init(&p->link, offsetof(DBC, link));
    p->self = obj;
    p->env = Qnil;
    p->envp = NULL;
    list_init(&p->stmts, offsetof(STMT, link));
    p->hdbc = SQL_NULL_HDBC;
    p->rbtime = Qfalse;
    p->gmtime = Qfalse;
    return obj;
}
#endif

static VALUE
dbc_new(int argc, VALUE *argv, VALUE self)
{
    DBC *p;
    VALUE obj, env = Qnil;

    if (TYPE(self) == T_MODULE) {
	self = Cobj;
    }
    if (self == Cobj) {
	self = Cdbc;
    }
    if (rb_obj_is_kind_of(self, Cenv) == Qtrue) {
	env = env_of(self);
	self = Cdbc;
    }
#ifdef HAVE_RB_DEFINE_ALLOC_FUNC
    obj = rb_obj_alloc(Cdbc);
    Data_Get_Struct(obj, DBC, p);
    p->env = env;
#else
    obj = Data_Make_Struct(self, DBC, mark_dbc, free_dbc, p);
    tracemsg(2, fprintf(stderr, "ObjAlloc: DBC %p\n", p););
    list_init(&p->link, offsetof(DBC, link));
    p->self = obj;
    p->env = env;
    p->envp = NULL;
    list_init(&p->stmts, offsetof(STMT, link));
    p->hdbc = SQL_NULL_HDBC;
    p->upc = 0;
#endif
    if (env != Qnil) {
	ENV *e;

	Data_Get_Struct(env, ENV, e);
	link_dbc(p, e);
    }
    if (argc > 0) {
	rb_obj_call_init(obj, argc, argv);
    }
    return obj;
}

/*
 *----------------------------------------------------------------------
 *
 *      Connect to data source.
 *
 *----------------------------------------------------------------------
 */

static VALUE
dbc_connect(int argc, VALUE *argv, VALUE self)
{
    ENV *e;
    DBC *p;
    VALUE dsn, user, passwd;
#ifdef UNICODE
    SQLWCHAR *sdsn = NULL, *suser = NULL, *spasswd = NULL;
#else
    char *sdsn, *suser = NULL, *spasswd = NULL;
#endif
    char *msg;
    SQLHDBC dbc;

    rb_scan_args(argc, argv, "03", &dsn, &user, &passwd);
    if (dsn != Qnil) {
	if (rb_obj_is_kind_of(dsn, Cdsn) == Qtrue) {
	    dsn = rb_iv_get(dsn, "@name");
	}
	Check_Type(dsn, T_STRING);
    }
    if (user != Qnil) {
	Check_Type(user, T_STRING);
    }
    if (passwd != Qnil) {
	Check_Type(passwd, T_STRING);
    }
    p = get_dbc(self);
    if (p->hdbc != SQL_NULL_HDBC) {
	rb_raise(Cerror, "%s", set_err("Already connected", 0));
    }
    if (p->env == Qnil) {
	p->env = env_new(Cenv);
	e = get_env(p->env);
	link_dbc(p, e);
    } else {
	e = get_env(p->env);
    }
    if (dsn == Qnil) {
	return self;
    }
#ifdef UNICODE
    if (user != Qnil) {
#ifdef USE_RB_ENC
	user = rb_funcall(user, IDencode, 1, rb_encv);
#endif
	suser = uc_from_utf((unsigned char *) STR2CSTR(user), -1);
    }
    if (passwd != Qnil) {
#ifdef USE_RB_ENC
	passwd = rb_funcall(passwd, IDencode, 1, rb_encv);
#endif
	spasswd = uc_from_utf((unsigned char *) STR2CSTR(passwd), -1);
    }
#ifdef USE_RB_ENC
    dsn = rb_funcall(dsn, IDencode, 1, rb_encv);
#endif
    sdsn = uc_from_utf((unsigned char *) STR2CSTR(dsn), -1);
    if (((suser == NULL) && (user != Qnil)) ||
	((spasswd == NULL) && (passwd != Qnil)) ||
	(sdsn == NULL)) {
	uc_free(sdsn);
	uc_free(suser);
	uc_free(spasswd);
	rb_raise(Cerror, "%s", set_err("Out of memory", 0));
    }
#else
    if (user != Qnil) {
	suser = STR2CSTR(user);
    }
    if (passwd != Qnil) {
	spasswd = STR2CSTR(passwd);
    }
    sdsn = STR2CSTR(dsn);
#endif
    if (!succeeded(e->henv, SQL_NULL_HDBC, SQL_NULL_HSTMT,
		   SQLAllocConnect(e->henv, &dbc), &msg, "SQLAllocConnect")) {
#ifdef UNICODE
	uc_free(sdsn);
	uc_free(suser);
	uc_free(spasswd);
#endif
	rb_raise(Cerror, "%s", msg);
    }
    if (!succeeded(SQL_NULL_HENV, dbc, SQL_NULL_HSTMT,
		   SQLConnect(dbc, (SQLTCHAR *) sdsn, SQL_NTS,
			      (SQLTCHAR *) suser,
			      (SQLSMALLINT) (suser ? SQL_NTS : 0),
			      (SQLTCHAR *) spasswd,
			      (SQLSMALLINT) (spasswd ? SQL_NTS : 0)),
		   &msg,
		   "SQLConnect('%s')", sdsn)) {
#ifdef UNICODE
	uc_free(sdsn);
	uc_free(suser);
	uc_free(spasswd);
#endif
	callsql(SQL_NULL_HENV, dbc, SQL_NULL_HSTMT,
		SQLFreeConnect(dbc), "SQLFreeConnect");
	rb_raise(Cerror, "%s", msg);
    }
#ifdef UNICODE
    uc_free(sdsn);
    uc_free(suser);
    uc_free(spasswd);
#endif
    p->hdbc = dbc;
    return self;
}

static VALUE
dbc_drvconnect(VALUE self, VALUE drv)
{
    ENV *e;
    DBC *p;
#ifdef UNICODE
    SQLWCHAR *sdrv;
#else
    char *sdrv;
#endif
    char *msg;
    SQLHDBC dbc;

    if (rb_obj_is_kind_of(drv, Cdrv) == Qtrue) {
	VALUE d, a, x;

	d = rb_str_new2("");
	a = rb_funcall(rb_iv_get(drv, "@attrs"), IDkeys, 0, NULL);
	while ((x = rb_ary_shift(a)) != Qnil) {
	    VALUE v = rb_hash_aref(rb_iv_get(drv, "@attrs"), x);

	    d = rb_str_concat(d, x);
	    d = rb_str_cat2(d, "=");
	    d = rb_str_concat(d, v);
	    d = rb_str_cat2(d, ";");
	}
	drv = d;
    }
    Check_Type(drv, T_STRING);
    p = get_dbc(self);
    if (p->hdbc != SQL_NULL_HDBC) {
	rb_raise(Cerror, "%s", set_err("Already connected", 0));
    }
    if (p->env == Qnil) {
	p->env = env_new(Cenv);
	e = get_env(p->env);
	link_dbc(p, e);
    } else {
	e = get_env(p->env);
    }
#ifdef UNICODE
#ifdef USE_RB_ENC
    drv = rb_funcall(drv, IDencode, 1, rb_encv);
#endif
    sdrv = uc_from_utf((unsigned char *) STR2CSTR(drv), -1);
    if (sdrv == NULL) {
	rb_raise(Cerror, "%s", set_err("Out of memory", 0));
    }
#else
    sdrv = STR2CSTR(drv);
#endif
    if (!succeeded(e->henv, SQL_NULL_HDBC, SQL_NULL_HSTMT,
		   SQLAllocConnect(e->henv, &dbc), &msg, "SQLAllocConnect")) {
#ifdef UNICODE
	uc_free(sdrv);
#endif
	rb_raise(Cerror, "%s", msg);
    }
    if (!succeeded(e->henv, dbc, SQL_NULL_HSTMT,
		   SQLDriverConnect(dbc, NULL, (SQLTCHAR *) sdrv, SQL_NTS,
				    NULL, 0, NULL, SQL_DRIVER_NOPROMPT),
		   &msg, "SQLDriverConnect")) {
#ifdef UNICODE
	uc_free(sdrv);
#endif
	callsql(SQL_NULL_HENV, dbc, SQL_NULL_HSTMT,
		SQLFreeConnect(dbc), "SQLFreeConnect");
	rb_raise(Cerror, "%s", msg);
    }
#ifdef UNICODE
    uc_free(sdrv);
#endif
    p->hdbc = dbc;
    return self;
}

static VALUE
dbc_connected(VALUE self)
{
    DBC *p = get_dbc(self);

    return (p->hdbc == SQL_NULL_HDBC) ? Qfalse : Qtrue;
}

static VALUE
dbc_timefmt(int argc, VALUE *argv, VALUE self)
{
    DBC *p = get_dbc(self);
    VALUE val;

    if (argc > 0) {
	rb_scan_args(argc, argv, "1", &val);
	p->rbtime = (val != Qnil && val != Qfalse) ? Qtrue : Qfalse;
    }
    return p->rbtime;
}

static VALUE
dbc_timeutc(int argc, VALUE *argv, VALUE self)
{
    DBC *p = get_dbc(self);
    VALUE val;

    if (argc > 0) {
	rb_scan_args(argc, argv, "1", &val);
	p->gmtime = (val != Qnil && val != Qfalse) ? Qtrue : Qfalse;
    }
    return p->gmtime;
}

/*
 *----------------------------------------------------------------------
 *
 *      Drop all active statements from data source.
 *
 *----------------------------------------------------------------------
 */

static VALUE
dbc_dropall(VALUE self)
{
    DBC *p = get_dbc(self);

    while (!list_empty(&p->stmts)) {
	STMT *q = list_first(&p->stmts);

	if (q->self == Qnil) {
	    rb_fatal("RubyODBC: invalid stmt in dropall");
	}
	stmt_drop(q->self);
    }
    return self;
}

/*
 *----------------------------------------------------------------------
 *
 *      Disconnect from data source.
 *
 *----------------------------------------------------------------------
 */

static VALUE
dbc_disconnect(int argc, VALUE *argv, VALUE self)
{
    DBC *p = get_dbc(self);
    VALUE nodrop = Qfalse;
    char *msg;

    rb_scan_args(argc, argv, "01", &nodrop);
    if (!RTEST(nodrop)) {
	dbc_dropall(self);
    }
    if (p->hdbc == SQL_NULL_HDBC) {
	return Qtrue;
    }
    if (list_empty(&p->stmts)) {
	callsql(SQL_NULL_HENV, p->hdbc, SQL_NULL_HSTMT,
		SQLDisconnect(p->hdbc), "SQLDisconnect");
	if (!succeeded(SQL_NULL_HENV, p->hdbc, SQL_NULL_HSTMT,
		       SQLFreeConnect(p->hdbc), &msg, "SQLFreeConnect")) {
	    rb_raise(Cerror, "%s", msg);
	}
	p->hdbc = SQL_NULL_HDBC;
	unlink_dbc(p);
	start_gc();
	return Qtrue;
    }
    return Qfalse;
}

/*
 *----------------------------------------------------------------------
 *
 *      Database meta data via SQLGetInfo()
 *
 *----------------------------------------------------------------------
 */

#define GI_CONST_SINT(x) { #x, x, SQL_C_SHORT }
#define GI_CONST_INT(x)  { #x, x, SQL_C_LONG }
#define GI_CONST_BITS(x) { #x, x, SQL_C_LONG }
#define GI_CONST_STR(x)  { #x, x, SQL_C_CHAR }
#define GI_CONST_END     { NULL, -1, -1 }
static struct {
    const char *name;
    int info;
    int maptype;
} get_info_map[] = {

    /* yielding ints */
    GI_CONST_SINT(SQL_ACTIVE_ENVIRONMENTS),
    GI_CONST_SINT(SQL_ACTIVE_CONNECTIONS),
    GI_CONST_SINT(SQL_ACTIVE_STATEMENTS),
    GI_CONST_INT(SQL_ASYNC_MODE),
    GI_CONST_SINT(SQL_CATALOG_LOCATION),
    GI_CONST_SINT(SQL_CONCAT_NULL_BEHAVIOR),
    GI_CONST_SINT(SQL_CORRELATION_NAME),
    GI_CONST_SINT(SQL_CURSOR_COMMIT_BEHAVIOR),
    GI_CONST_SINT(SQL_CURSOR_ROLLBACK_BEHAVIOR),
    GI_CONST_INT(SQL_CURSOR_SENSITIVITY),
    GI_CONST_INT(SQL_DDL_INDEX),
    GI_CONST_INT(SQL_DEFAULT_TXN_ISOLATION),
    GI_CONST_INT(SQL_DRIVER_HDBC),
    GI_CONST_INT(SQL_DRIVER_HENV),
    GI_CONST_INT(SQL_DRIVER_HDESC),
    GI_CONST_INT(SQL_DRIVER_HLIB),
    GI_CONST_INT(SQL_DRIVER_HSTMT),
    GI_CONST_SINT(SQL_FILE_USAGE),
    GI_CONST_SINT(SQL_GROUP_BY),
    GI_CONST_SINT(SQL_IDENTIFIER_CASE),
    GI_CONST_INT(SQL_MAX_ASYNC_CONCURRENT_STATEMENTS),
    GI_CONST_INT(SQL_MAX_BINARY_LITERAL_LEN),
    GI_CONST_SINT(SQL_MAX_CATALOG_NAME_LEN),
    GI_CONST_INT(SQL_MAX_CHAR_LITERAL_LEN),
    GI_CONST_SINT(SQL_MAX_COLUMN_NAME_LEN),
    GI_CONST_SINT(SQL_MAX_COLUMNS_IN_GROUP_BY),
    GI_CONST_SINT(SQL_MAX_COLUMNS_IN_INDEX),
    GI_CONST_SINT(SQL_MAX_COLUMNS_IN_ORDER_BY),
    GI_CONST_SINT(SQL_MAX_COLUMNS_IN_SELECT),
    GI_CONST_SINT(SQL_MAX_COLUMNS_IN_TABLE),
    GI_CONST_SINT(SQL_MAX_CONCURRENT_ACTIVITIES),
    GI_CONST_SINT(SQL_MAX_CURSOR_NAME_LEN),
    GI_CONST_SINT(SQL_MAX_DRIVER_CONNECTIONS),
    GI_CONST_SINT(SQL_MAX_IDENTIFIER_LEN),
    GI_CONST_INT(SQL_MAX_INDEX_SIZE),
    GI_CONST_SINT(SQL_MAX_OWNER_NAME_LEN),
    GI_CONST_SINT(SQL_MAX_PROCEDURE_NAME_LEN),
    GI_CONST_SINT(SQL_MAX_QUALIFIER_NAME_LEN),
    GI_CONST_INT(SQL_MAX_ROW_SIZE),
    GI_CONST_SINT(SQL_MAX_SCHEMA_NAME_LEN),
    GI_CONST_INT(SQL_MAX_STATEMENT_LEN),
    GI_CONST_SINT(SQL_MAX_TABLE_NAME_LEN),
    GI_CONST_SINT(SQL_MAX_TABLES_IN_SELECT),
    GI_CONST_SINT(SQL_MAX_USER_NAME_LEN),
    GI_CONST_SINT(SQL_NON_NULLABLE_COLUMNS),
    GI_CONST_SINT(SQL_NULL_COLLATION),
    GI_CONST_SINT(SQL_ODBC_API_CONFORMANCE),
    GI_CONST_INT(SQL_ODBC_INTERFACE_CONFORMANCE),
    GI_CONST_SINT(SQL_ODBC_SAG_CLI_CONFORMANCE),
    GI_CONST_SINT(SQL_ODBC_SQL_CONFORMANCE),
    GI_CONST_INT(SQL_PARAM_ARRAY_ROW_COUNTS),
    GI_CONST_INT(SQL_PARAM_ARRAY_SELECTS),
    GI_CONST_SINT(SQL_QUALIFIER_LOCATION),
    GI_CONST_SINT(SQL_QUOTED_IDENTIFIER_CASE),
    GI_CONST_INT(SQL_SQL_CONFORMANCE),
    GI_CONST_SINT(SQL_TXN_CAPABLE),

    /* yielding ints (but bitmasks) */
    GI_CONST_BITS(SQL_AGGREGATE_FUNCTIONS),
    GI_CONST_BITS(SQL_ALTER_DOMAIN),
    GI_CONST_BITS(SQL_ALTER_TABLE),
    GI_CONST_BITS(SQL_BATCH_ROW_COUNT),
    GI_CONST_BITS(SQL_BATCH_SUPPORT),
    GI_CONST_BITS(SQL_BOOKMARK_PERSISTENCE),
    GI_CONST_BITS(SQL_CATALOG_USAGE),
    GI_CONST_BITS(SQL_CONVERT_BINARY),
    GI_CONST_BITS(SQL_CONVERT_BIT),
    GI_CONST_BITS(SQL_CONVERT_CHAR),
#ifdef SQL_CONVERT_GUID
    GI_CONST_BITS(SQL_CONVERT_GUID),
#endif
    GI_CONST_BITS(SQL_CONVERT_DATE),
    GI_CONST_BITS(SQL_CONVERT_DECIMAL),
    GI_CONST_BITS(SQL_CONVERT_DOUBLE),
    GI_CONST_BITS(SQL_CONVERT_FLOAT),
    GI_CONST_BITS(SQL_CONVERT_FUNCTIONS),
    GI_CONST_BITS(SQL_CONVERT_INTEGER),
    GI_CONST_BITS(SQL_CONVERT_INTERVAL_YEAR_MONTH),
    GI_CONST_BITS(SQL_CONVERT_INTERVAL_DAY_TIME),
    GI_CONST_BITS(SQL_CONVERT_LONGVARBINARY),
    GI_CONST_BITS(SQL_CONVERT_LONGVARCHAR),
    GI_CONST_BITS(SQL_CONVERT_NUMERIC),
    GI_CONST_BITS(SQL_CONVERT_REAL),
    GI_CONST_BITS(SQL_CONVERT_SMALLINT),
    GI_CONST_BITS(SQL_CONVERT_TIME),
    GI_CONST_BITS(SQL_CONVERT_TIMESTAMP),
    GI_CONST_BITS(SQL_CONVERT_TINYINT),
    GI_CONST_BITS(SQL_CONVERT_VARBINARY),
    GI_CONST_BITS(SQL_CONVERT_VARCHAR),
    GI_CONST_BITS(SQL_CONVERT_WCHAR),
    GI_CONST_BITS(SQL_CONVERT_WLONGVARCHAR),
    GI_CONST_BITS(SQL_CONVERT_WVARCHAR),
    GI_CONST_BITS(SQL_CREATE_ASSERTION),
    GI_CONST_BITS(SQL_CREATE_CHARACTER_SET),
    GI_CONST_BITS(SQL_CREATE_COLLATION),
    GI_CONST_BITS(SQL_CREATE_DOMAIN),
    GI_CONST_BITS(SQL_CREATE_SCHEMA),
    GI_CONST_BITS(SQL_CREATE_TABLE),
    GI_CONST_BITS(SQL_CREATE_TRANSLATION),
    GI_CONST_BITS(SQL_CREATE_VIEW),
    GI_CONST_BITS(SQL_DATETIME_LITERALS),
    GI_CONST_BITS(SQL_DROP_ASSERTION),
    GI_CONST_BITS(SQL_DROP_CHARACTER_SET),
    GI_CONST_BITS(SQL_DROP_COLLATION),
    GI_CONST_BITS(SQL_DROP_DOMAIN),
    GI_CONST_BITS(SQL_DROP_SCHEMA),
    GI_CONST_BITS(SQL_DROP_TABLE),
    GI_CONST_BITS(SQL_DROP_TRANSLATION),
    GI_CONST_BITS(SQL_DROP_VIEW),
    GI_CONST_BITS(SQL_DTC_TRANSITION_COST),
    GI_CONST_BITS(SQL_DYNAMIC_CURSOR_ATTRIBUTES1),
    GI_CONST_BITS(SQL_DYNAMIC_CURSOR_ATTRIBUTES2),
    GI_CONST_BITS(SQL_FETCH_DIRECTION),
    GI_CONST_BITS(SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES1),
    GI_CONST_BITS(SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES2),
    GI_CONST_BITS(SQL_GETDATA_EXTENSIONS),
    GI_CONST_BITS(SQL_KEYSET_CURSOR_ATTRIBUTES1),
    GI_CONST_BITS(SQL_KEYSET_CURSOR_ATTRIBUTES2),
    GI_CONST_BITS(SQL_INDEX_KEYWORDS),
    GI_CONST_BITS(SQL_INFO_SCHEMA_VIEWS),
    GI_CONST_BITS(SQL_INSERT_STATEMENT),
    GI_CONST_BITS(SQL_LOCK_TYPES),
    GI_CONST_BITS(SQL_NUMERIC_FUNCTIONS),
    GI_CONST_BITS(SQL_OJ_CAPABILITIES),
    GI_CONST_BITS(SQL_OWNER_USAGE),
    GI_CONST_BITS(SQL_POS_OPERATIONS),
    GI_CONST_BITS(SQL_POSITIONED_STATEMENTS),
    GI_CONST_BITS(SQL_QUALIFIER_USAGE),
    GI_CONST_BITS(SQL_SCHEMA_USAGE),
    GI_CONST_BITS(SQL_SCROLL_CONCURRENCY),
    GI_CONST_BITS(SQL_SCROLL_OPTIONS),
    GI_CONST_BITS(SQL_SQL92_DATETIME_FUNCTIONS),
    GI_CONST_BITS(SQL_SQL92_FOREIGN_KEY_DELETE_RULE),
    GI_CONST_BITS(SQL_SQL92_FOREIGN_KEY_UPDATE_RULE),
    GI_CONST_BITS(SQL_SQL92_GRANT),
    GI_CONST_BITS(SQL_SQL92_NUMERIC_VALUE_FUNCTIONS),
    GI_CONST_BITS(SQL_SQL92_PREDICATES),
    GI_CONST_BITS(SQL_SQL92_RELATIONAL_JOIN_OPERATORS),
    GI_CONST_BITS(SQL_SQL92_REVOKE),
    GI_CONST_BITS(SQL_SQL92_ROW_VALUE_CONSTRUCTOR),
    GI_CONST_BITS(SQL_SQL92_STRING_FUNCTIONS),
    GI_CONST_BITS(SQL_SQL92_VALUE_EXPRESSIONS),
    GI_CONST_BITS(SQL_STANDARD_CLI_CONFORMANCE),
    GI_CONST_BITS(SQL_STATIC_CURSOR_ATTRIBUTES1),
    GI_CONST_BITS(SQL_STATIC_CURSOR_ATTRIBUTES2),
    GI_CONST_BITS(SQL_STATIC_SENSITIVITY),
    GI_CONST_BITS(SQL_STRING_FUNCTIONS),
    GI_CONST_BITS(SQL_SUBQUERIES),
    GI_CONST_BITS(SQL_SYSTEM_FUNCTIONS),
    GI_CONST_BITS(SQL_TIMEDATE_ADD_INTERVALS),
    GI_CONST_BITS(SQL_TIMEDATE_DIFF_INTERVALS),
    GI_CONST_BITS(SQL_TIMEDATE_FUNCTIONS),
    GI_CONST_BITS(SQL_TXN_ISOLATION_OPTION),
    GI_CONST_BITS(SQL_UNION),

    /* yielding strings */
    GI_CONST_STR(SQL_ACCESSIBLE_PROCEDURES),
    GI_CONST_STR(SQL_ACCESSIBLE_TABLES),
    GI_CONST_STR(SQL_CATALOG_NAME),
    GI_CONST_STR(SQL_CATALOG_NAME_SEPARATOR),
    GI_CONST_STR(SQL_CATALOG_TERM),
    GI_CONST_STR(SQL_COLLATION_SEQ),
    GI_CONST_STR(SQL_COLUMN_ALIAS),
    GI_CONST_STR(SQL_DATA_SOURCE_NAME),
    GI_CONST_STR(SQL_DATA_SOURCE_READ_ONLY),
    GI_CONST_STR(SQL_DATABASE_NAME),
    GI_CONST_STR(SQL_DBMS_NAME),
    GI_CONST_STR(SQL_DBMS_VER),
    GI_CONST_STR(SQL_DESCRIBE_PARAMETER),
    GI_CONST_STR(SQL_DM_VER),
    GI_CONST_STR(SQL_DRIVER_NAME),
    GI_CONST_STR(SQL_DRIVER_ODBC_VER),
    GI_CONST_STR(SQL_DRIVER_VER),
    GI_CONST_STR(SQL_EXPRESSIONS_IN_ORDERBY),
    GI_CONST_STR(SQL_IDENTIFIER_QUOTE_CHAR),
    GI_CONST_STR(SQL_INTEGRITY),
    GI_CONST_STR(SQL_KEYWORDS),
    GI_CONST_STR(SQL_LIKE_ESCAPE_CLAUSE),
    GI_CONST_STR(SQL_MAX_ROW_SIZE_INCLUDES_LONG),
    GI_CONST_STR(SQL_MULT_RESULT_SETS),
    GI_CONST_STR(SQL_MULTIPLE_ACTIVE_TXN),
    GI_CONST_STR(SQL_NEED_LONG_DATA_LEN),
    GI_CONST_STR(SQL_ODBC_SQL_OPT_IEF),
    GI_CONST_STR(SQL_ODBC_VER),
    GI_CONST_STR(SQL_ORDER_BY_COLUMNS_IN_SELECT),
    GI_CONST_STR(SQL_OUTER_JOINS),
    GI_CONST_STR(SQL_OWNER_TERM),
    GI_CONST_STR(SQL_PROCEDURE_TERM),
    GI_CONST_STR(SQL_PROCEDURES),
    GI_CONST_STR(SQL_QUALIFIER_NAME_SEPARATOR),
    GI_CONST_STR(SQL_QUALIFIER_TERM),
    GI_CONST_STR(SQL_ROW_UPDATES),
    GI_CONST_STR(SQL_SCHEMA_TERM),
    GI_CONST_STR(SQL_SEARCH_PATTERN_ESCAPE),
    GI_CONST_STR(SQL_SERVER_NAME),
    GI_CONST_STR(SQL_SPECIAL_CHARACTERS),
    GI_CONST_STR(SQL_TABLE_TERM),
    GI_CONST_STR(SQL_USER_NAME),
    GI_CONST_STR(SQL_XOPEN_CLI_YEAR),

    /* end of table */
    GI_CONST_END
};

#define GI_CONST_BITMAP(x)  { #x, x }
#define GI_CONST_BITMAP_END { NULL, 0 }
static struct {
    const char *name;
    int bits;
} get_info_bitmap[] = {
    GI_CONST_BITMAP(SQL_AD_ADD_CONSTRAINT_DEFERRABLE),
    GI_CONST_BITMAP(SQL_AD_ADD_CONSTRAINT_INITIALLY_DEFERRED),
    GI_CONST_BITMAP(SQL_AD_ADD_CONSTRAINT_INITIALLY_IMMEDIATE),
    GI_CONST_BITMAP(SQL_AD_ADD_CONSTRAINT_NON_DEFERRABLE),
    GI_CONST_BITMAP(SQL_AD_ADD_DOMAIN_CONSTRAINT),
    GI_CONST_BITMAP(SQL_AD_ADD_DOMAIN_DEFAULT),
    GI_CONST_BITMAP(SQL_AD_CONSTRAINT_NAME_DEFINITION),
    GI_CONST_BITMAP(SQL_AD_DROP_DOMAIN_CONSTRAINT),
    GI_CONST_BITMAP(SQL_AD_DROP_DOMAIN_DEFAULT),
    GI_CONST_BITMAP(SQL_AF_ALL),
    GI_CONST_BITMAP(SQL_AF_AVG),
    GI_CONST_BITMAP(SQL_AF_COUNT),
    GI_CONST_BITMAP(SQL_AF_DISTINCT),
    GI_CONST_BITMAP(SQL_AF_MAX),
    GI_CONST_BITMAP(SQL_AF_MIN),
    GI_CONST_BITMAP(SQL_AF_SUM),
    GI_CONST_BITMAP(SQL_AM_CONNECTION),
    GI_CONST_BITMAP(SQL_AM_NONE),
    GI_CONST_BITMAP(SQL_AM_STATEMENT),
    GI_CONST_BITMAP(SQL_AT_ADD_COLUMN),
    GI_CONST_BITMAP(SQL_AT_ADD_COLUMN_COLLATION),
    GI_CONST_BITMAP(SQL_AT_ADD_COLUMN_DEFAULT),
    GI_CONST_BITMAP(SQL_AT_ADD_COLUMN_SINGLE),
    GI_CONST_BITMAP(SQL_AT_ADD_CONSTRAINT),
    GI_CONST_BITMAP(SQL_AT_ADD_TABLE_CONSTRAINT),
#ifdef SQL_AT_COLUMN_SINGLE
    GI_CONST_BITMAP(SQL_AT_COLUMN_SINGLE),
#endif
    GI_CONST_BITMAP(SQL_AT_CONSTRAINT_DEFERRABLE),
    GI_CONST_BITMAP(SQL_AT_CONSTRAINT_INITIALLY_DEFERRED),
    GI_CONST_BITMAP(SQL_AT_CONSTRAINT_INITIALLY_IMMEDIATE),
    GI_CONST_BITMAP(SQL_AT_CONSTRAINT_NAME_DEFINITION),
    GI_CONST_BITMAP(SQL_AT_CONSTRAINT_NON_DEFERRABLE),
    GI_CONST_BITMAP(SQL_AT_DROP_COLUMN),
    GI_CONST_BITMAP(SQL_AT_DROP_COLUMN_CASCADE),
    GI_CONST_BITMAP(SQL_AT_DROP_COLUMN_DEFAULT),
    GI_CONST_BITMAP(SQL_AT_DROP_COLUMN_RESTRICT),
    GI_CONST_BITMAP(SQL_AT_DROP_TABLE_CONSTRAINT_CASCADE),
    GI_CONST_BITMAP(SQL_AT_DROP_TABLE_CONSTRAINT_RESTRICT),
    GI_CONST_BITMAP(SQL_AT_SET_COLUMN_DEFAULT),
    GI_CONST_BITMAP(SQL_BP_CLOSE),
    GI_CONST_BITMAP(SQL_BP_DELETE),
    GI_CONST_BITMAP(SQL_BP_DROP),
    GI_CONST_BITMAP(SQL_BP_OTHER_HSTMT),
    GI_CONST_BITMAP(SQL_BP_SCROLL),
    GI_CONST_BITMAP(SQL_BP_TRANSACTION),
    GI_CONST_BITMAP(SQL_BP_UPDATE),
    GI_CONST_BITMAP(SQL_BRC_EXPLICIT),
    GI_CONST_BITMAP(SQL_BRC_PROCEDURES),
    GI_CONST_BITMAP(SQL_BRC_ROLLED_UP),
    GI_CONST_BITMAP(SQL_BS_ROW_COUNT_EXPLICIT),
    GI_CONST_BITMAP(SQL_BS_ROW_COUNT_PROC),
    GI_CONST_BITMAP(SQL_BS_SELECT_EXPLICIT),
    GI_CONST_BITMAP(SQL_BS_SELECT_PROC),
    GI_CONST_BITMAP(SQL_CA1_ABSOLUTE),
    GI_CONST_BITMAP(SQL_CA1_BOOKMARK),
    GI_CONST_BITMAP(SQL_CA1_BULK_ADD),
    GI_CONST_BITMAP(SQL_CA1_BULK_DELETE_BY_BOOKMARK),
    GI_CONST_BITMAP(SQL_CA1_BULK_FETCH_BY_BOOKMARK),
    GI_CONST_BITMAP(SQL_CA1_BULK_UPDATE_BY_BOOKMARK),
    GI_CONST_BITMAP(SQL_CA1_LOCK_EXCLUSIVE),
    GI_CONST_BITMAP(SQL_CA1_LOCK_NO_CHANGE),
    GI_CONST_BITMAP(SQL_CA1_LOCK_UNLOCK),
    GI_CONST_BITMAP(SQL_CA1_NEXT),
    GI_CONST_BITMAP(SQL_CA1_POS_DELETE),
    GI_CONST_BITMAP(SQL_CA1_POSITIONED_DELETE),
    GI_CONST_BITMAP(SQL_CA1_POSITIONED_UPDATE),
    GI_CONST_BITMAP(SQL_CA1_POS_POSITION),
    GI_CONST_BITMAP(SQL_CA1_POS_REFRESH),
    GI_CONST_BITMAP(SQL_CA1_POS_UPDATE),
    GI_CONST_BITMAP(SQL_CA1_RELATIVE),
    GI_CONST_BITMAP(SQL_CA1_SELECT_FOR_UPDATE),
    GI_CONST_BITMAP(SQL_CA2_CRC_APPROXIMATE),
    GI_CONST_BITMAP(SQL_CA2_CRC_EXACT),
    GI_CONST_BITMAP(SQL_CA2_LOCK_CONCURRENCY),
    GI_CONST_BITMAP(SQL_CA2_MAX_ROWS_AFFECTS_ALL),
    GI_CONST_BITMAP(SQL_CA2_MAX_ROWS_CATALOG),
    GI_CONST_BITMAP(SQL_CA2_MAX_ROWS_DELETE),
    GI_CONST_BITMAP(SQL_CA2_MAX_ROWS_INSERT),
    GI_CONST_BITMAP(SQL_CA2_MAX_ROWS_SELECT),
    GI_CONST_BITMAP(SQL_CA2_MAX_ROWS_UPDATE),
    GI_CONST_BITMAP(SQL_CA2_OPT_ROWVER_CONCURRENCY),
    GI_CONST_BITMAP(SQL_CA2_OPT_VALUES_CONCURRENCY),
    GI_CONST_BITMAP(SQL_CA2_READ_ONLY_CONCURRENCY),
    GI_CONST_BITMAP(SQL_CA2_SENSITIVITY_ADDITIONS),
    GI_CONST_BITMAP(SQL_CA2_SENSITIVITY_DELETIONS),
    GI_CONST_BITMAP(SQL_CA2_SENSITIVITY_UPDATES),
    GI_CONST_BITMAP(SQL_CA2_SIMULATE_NON_UNIQUE),
    GI_CONST_BITMAP(SQL_CA2_SIMULATE_TRY_UNIQUE),
    GI_CONST_BITMAP(SQL_CA2_SIMULATE_UNIQUE),
    GI_CONST_BITMAP(SQL_CA_CONSTRAINT_DEFERRABLE),
    GI_CONST_BITMAP(SQL_CA_CONSTRAINT_INITIALLY_DEFERRED),
    GI_CONST_BITMAP(SQL_CA_CONSTRAINT_INITIALLY_IMMEDIATE),
    GI_CONST_BITMAP(SQL_CA_CONSTRAINT_NON_DEFERRABLE),
    GI_CONST_BITMAP(SQL_CA_CREATE_ASSERTION),
    GI_CONST_BITMAP(SQL_CB_CLOSE),
    GI_CONST_BITMAP(SQL_CB_DELETE),
    GI_CONST_BITMAP(SQL_CB_NON_NULL),
    GI_CONST_BITMAP(SQL_CB_NULL),
    GI_CONST_BITMAP(SQL_CB_PRESERVE),
    GI_CONST_BITMAP(SQL_CC_CLOSE),
    GI_CONST_BITMAP(SQL_CC_DELETE),
    GI_CONST_BITMAP(SQL_CCOL_CREATE_COLLATION),
    GI_CONST_BITMAP(SQL_CC_PRESERVE),
    GI_CONST_BITMAP(SQL_CCS_COLLATE_CLAUSE),
    GI_CONST_BITMAP(SQL_CCS_CREATE_CHARACTER_SET),
    GI_CONST_BITMAP(SQL_CCS_LIMITED_COLLATION),
    GI_CONST_BITMAP(SQL_CDO_COLLATION),
    GI_CONST_BITMAP(SQL_CDO_CONSTRAINT),
    GI_CONST_BITMAP(SQL_CDO_CONSTRAINT_DEFERRABLE),
    GI_CONST_BITMAP(SQL_CDO_CONSTRAINT_INITIALLY_DEFERRED),
    GI_CONST_BITMAP(SQL_CDO_CONSTRAINT_INITIALLY_IMMEDIATE),
    GI_CONST_BITMAP(SQL_CDO_CONSTRAINT_NAME_DEFINITION),
    GI_CONST_BITMAP(SQL_CDO_CONSTRAINT_NON_DEFERRABLE),
    GI_CONST_BITMAP(SQL_CDO_CREATE_DOMAIN),
    GI_CONST_BITMAP(SQL_CDO_DEFAULT),
    GI_CONST_BITMAP(SQL_CL_END),
    GI_CONST_BITMAP(SQL_CL_START),
    GI_CONST_BITMAP(SQL_CN_ANY),
    GI_CONST_BITMAP(SQL_CN_DIFFERENT),
    GI_CONST_BITMAP(SQL_CN_NONE),
    GI_CONST_BITMAP(SQL_CONCUR_TIMESTAMP),
    GI_CONST_BITMAP(SQL_CR_CLOSE),
    GI_CONST_BITMAP(SQL_CR_DELETE),
    GI_CONST_BITMAP(SQL_CR_PRESERVE),
    GI_CONST_BITMAP(SQL_CS_AUTHORIZATION),
    GI_CONST_BITMAP(SQL_CS_CREATE_SCHEMA),
    GI_CONST_BITMAP(SQL_CS_DEFAULT_CHARACTER_SET),
    GI_CONST_BITMAP(SQL_CT_COLUMN_COLLATION),
    GI_CONST_BITMAP(SQL_CT_COLUMN_CONSTRAINT),
    GI_CONST_BITMAP(SQL_CT_COLUMN_DEFAULT),
    GI_CONST_BITMAP(SQL_CT_COMMIT_DELETE),
    GI_CONST_BITMAP(SQL_CT_COMMIT_PRESERVE),
    GI_CONST_BITMAP(SQL_CT_CONSTRAINT_DEFERRABLE),
    GI_CONST_BITMAP(SQL_CT_CONSTRAINT_INITIALLY_DEFERRED),
    GI_CONST_BITMAP(SQL_CT_CONSTRAINT_INITIALLY_IMMEDIATE),
    GI_CONST_BITMAP(SQL_CT_CONSTRAINT_NAME_DEFINITION),
    GI_CONST_BITMAP(SQL_CT_CONSTRAINT_NON_DEFERRABLE),
    GI_CONST_BITMAP(SQL_CT_CREATE_TABLE),
    GI_CONST_BITMAP(SQL_CT_GLOBAL_TEMPORARY),
    GI_CONST_BITMAP(SQL_CT_LOCAL_TEMPORARY),
    GI_CONST_BITMAP(SQL_CTR_CREATE_TRANSLATION),
    GI_CONST_BITMAP(SQL_CT_TABLE_CONSTRAINT),
    GI_CONST_BITMAP(SQL_CU_DML_STATEMENTS),
    GI_CONST_BITMAP(SQL_CU_INDEX_DEFINITION),
    GI_CONST_BITMAP(SQL_CU_PRIVILEGE_DEFINITION),
    GI_CONST_BITMAP(SQL_CU_PROCEDURE_INVOCATION),
    GI_CONST_BITMAP(SQL_CU_TABLE_DEFINITION),
    GI_CONST_BITMAP(SQL_CV_CASCADED),
    GI_CONST_BITMAP(SQL_CV_CHECK_OPTION),
    GI_CONST_BITMAP(SQL_CV_CREATE_VIEW),
    GI_CONST_BITMAP(SQL_CV_LOCAL),
    GI_CONST_BITMAP(SQL_CVT_BIGINT),
    GI_CONST_BITMAP(SQL_CVT_BINARY),
    GI_CONST_BITMAP(SQL_CVT_BIT),
    GI_CONST_BITMAP(SQL_CVT_CHAR),
    GI_CONST_BITMAP(SQL_CVT_DATE),
    GI_CONST_BITMAP(SQL_CVT_DECIMAL),
    GI_CONST_BITMAP(SQL_CVT_DOUBLE),
    GI_CONST_BITMAP(SQL_CVT_FLOAT),
    GI_CONST_BITMAP(SQL_CVT_INTEGER),
    GI_CONST_BITMAP(SQL_CVT_INTERVAL_DAY_TIME),
    GI_CONST_BITMAP(SQL_CVT_INTERVAL_YEAR_MONTH),
    GI_CONST_BITMAP(SQL_CVT_LONGVARBINARY),
    GI_CONST_BITMAP(SQL_CVT_LONGVARCHAR),
    GI_CONST_BITMAP(SQL_CVT_NUMERIC),
    GI_CONST_BITMAP(SQL_CVT_REAL),
    GI_CONST_BITMAP(SQL_CVT_SMALLINT),
    GI_CONST_BITMAP(SQL_CVT_TIME),
    GI_CONST_BITMAP(SQL_CVT_TIMESTAMP),
    GI_CONST_BITMAP(SQL_CVT_TINYINT),
    GI_CONST_BITMAP(SQL_CVT_VARBINARY),
    GI_CONST_BITMAP(SQL_CVT_VARCHAR),
    GI_CONST_BITMAP(SQL_CVT_WCHAR),
    GI_CONST_BITMAP(SQL_CVT_WLONGVARCHAR),
    GI_CONST_BITMAP(SQL_CVT_WVARCHAR),
    GI_CONST_BITMAP(SQL_DA_DROP_ASSERTION),
    GI_CONST_BITMAP(SQL_DC_DROP_COLLATION),
    GI_CONST_BITMAP(SQL_DCS_DROP_CHARACTER_SET),
    GI_CONST_BITMAP(SQL_DD_CASCADE),
    GI_CONST_BITMAP(SQL_DD_DROP_DOMAIN),
    GI_CONST_BITMAP(SQL_DD_RESTRICT),
    GI_CONST_BITMAP(SQL_DI_CREATE_INDEX),
    GI_CONST_BITMAP(SQL_DI_DROP_INDEX),
    GI_CONST_BITMAP(SQL_DL_SQL92_DATE),
    GI_CONST_BITMAP(SQL_DL_SQL92_INTERVAL_DAY),
    GI_CONST_BITMAP(SQL_DL_SQL92_INTERVAL_DAY_TO_HOUR),
    GI_CONST_BITMAP(SQL_DL_SQL92_INTERVAL_DAY_TO_MINUTE),
    GI_CONST_BITMAP(SQL_DL_SQL92_INTERVAL_DAY_TO_SECOND),
    GI_CONST_BITMAP(SQL_DL_SQL92_INTERVAL_HOUR),
    GI_CONST_BITMAP(SQL_DL_SQL92_INTERVAL_HOUR_TO_MINUTE),
    GI_CONST_BITMAP(SQL_DL_SQL92_INTERVAL_HOUR_TO_SECOND),
    GI_CONST_BITMAP(SQL_DL_SQL92_INTERVAL_MINUTE),
    GI_CONST_BITMAP(SQL_DL_SQL92_INTERVAL_MINUTE_TO_SECOND),
    GI_CONST_BITMAP(SQL_DL_SQL92_INTERVAL_MONTH),
    GI_CONST_BITMAP(SQL_DL_SQL92_INTERVAL_SECOND),
    GI_CONST_BITMAP(SQL_DL_SQL92_INTERVAL_YEAR),
    GI_CONST_BITMAP(SQL_DL_SQL92_INTERVAL_YEAR_TO_MONTH),
    GI_CONST_BITMAP(SQL_DL_SQL92_TIME),
    GI_CONST_BITMAP(SQL_DL_SQL92_TIMESTAMP),
    GI_CONST_BITMAP(SQL_DS_CASCADE),
    GI_CONST_BITMAP(SQL_DS_DROP_SCHEMA),
    GI_CONST_BITMAP(SQL_DS_RESTRICT),
    GI_CONST_BITMAP(SQL_DT_CASCADE),
    GI_CONST_BITMAP(SQL_DTC_ENLIST_EXPENSIVE),
    GI_CONST_BITMAP(SQL_DTC_UNENLIST_EXPENSIVE),
    GI_CONST_BITMAP(SQL_DT_DROP_TABLE),
    GI_CONST_BITMAP(SQL_DTR_DROP_TRANSLATION),
    GI_CONST_BITMAP(SQL_DT_RESTRICT),
    GI_CONST_BITMAP(SQL_DV_CASCADE),
    GI_CONST_BITMAP(SQL_DV_DROP_VIEW),
    GI_CONST_BITMAP(SQL_DV_RESTRICT),
    GI_CONST_BITMAP(SQL_FD_FETCH_ABSOLUTE),
    GI_CONST_BITMAP(SQL_FD_FETCH_BOOKMARK),
    GI_CONST_BITMAP(SQL_FD_FETCH_FIRST),
    GI_CONST_BITMAP(SQL_FD_FETCH_LAST),
    GI_CONST_BITMAP(SQL_FD_FETCH_NEXT),
    GI_CONST_BITMAP(SQL_FD_FETCH_PRIOR),
    GI_CONST_BITMAP(SQL_FD_FETCH_RELATIVE),
#ifdef SQL_FD_FETCH_RESUME
    GI_CONST_BITMAP(SQL_FD_FETCH_RESUME),
#endif
    GI_CONST_BITMAP(SQL_FILE_CATALOG),
    GI_CONST_BITMAP(SQL_FILE_NOT_SUPPORTED),
    GI_CONST_BITMAP(SQL_FILE_QUALIFIER),
    GI_CONST_BITMAP(SQL_FILE_TABLE),
    GI_CONST_BITMAP(SQL_FN_CVT_CAST),
    GI_CONST_BITMAP(SQL_FN_CVT_CONVERT),
    GI_CONST_BITMAP(SQL_FN_NUM_ABS),
    GI_CONST_BITMAP(SQL_FN_NUM_ACOS),
    GI_CONST_BITMAP(SQL_FN_NUM_ASIN),
    GI_CONST_BITMAP(SQL_FN_NUM_ATAN),
    GI_CONST_BITMAP(SQL_FN_NUM_ATAN2),
    GI_CONST_BITMAP(SQL_FN_NUM_CEILING),
    GI_CONST_BITMAP(SQL_FN_NUM_COS),
    GI_CONST_BITMAP(SQL_FN_NUM_COT),
    GI_CONST_BITMAP(SQL_FN_NUM_DEGREES),
    GI_CONST_BITMAP(SQL_FN_NUM_EXP),
    GI_CONST_BITMAP(SQL_FN_NUM_FLOOR),
    GI_CONST_BITMAP(SQL_FN_NUM_LOG),
    GI_CONST_BITMAP(SQL_FN_NUM_LOG10),
    GI_CONST_BITMAP(SQL_FN_NUM_MOD),
    GI_CONST_BITMAP(SQL_FN_NUM_PI),
    GI_CONST_BITMAP(SQL_FN_NUM_POWER),
    GI_CONST_BITMAP(SQL_FN_NUM_RADIANS),
    GI_CONST_BITMAP(SQL_FN_NUM_RAND),
    GI_CONST_BITMAP(SQL_FN_NUM_ROUND),
    GI_CONST_BITMAP(SQL_FN_NUM_SIGN),
    GI_CONST_BITMAP(SQL_FN_NUM_SIN),
    GI_CONST_BITMAP(SQL_FN_NUM_SQRT),
    GI_CONST_BITMAP(SQL_FN_NUM_TAN),
    GI_CONST_BITMAP(SQL_FN_NUM_TRUNCATE),
    GI_CONST_BITMAP(SQL_FN_STR_ASCII),
    GI_CONST_BITMAP(SQL_FN_STR_BIT_LENGTH),
    GI_CONST_BITMAP(SQL_FN_STR_CHAR),
    GI_CONST_BITMAP(SQL_FN_STR_CHARACTER_LENGTH),
    GI_CONST_BITMAP(SQL_FN_STR_CHAR_LENGTH),
    GI_CONST_BITMAP(SQL_FN_STR_CONCAT),
    GI_CONST_BITMAP(SQL_FN_STR_DIFFERENCE),
    GI_CONST_BITMAP(SQL_FN_STR_INSERT),
    GI_CONST_BITMAP(SQL_FN_STR_LCASE),
    GI_CONST_BITMAP(SQL_FN_STR_LEFT),
    GI_CONST_BITMAP(SQL_FN_STR_LENGTH),
    GI_CONST_BITMAP(SQL_FN_STR_LOCATE),
    GI_CONST_BITMAP(SQL_FN_STR_LOCATE_2),
    GI_CONST_BITMAP(SQL_FN_STR_LTRIM),
    GI_CONST_BITMAP(SQL_FN_STR_OCTET_LENGTH),
    GI_CONST_BITMAP(SQL_FN_STR_POSITION),
    GI_CONST_BITMAP(SQL_FN_STR_REPEAT),
    GI_CONST_BITMAP(SQL_FN_STR_REPLACE),
    GI_CONST_BITMAP(SQL_FN_STR_RIGHT),
    GI_CONST_BITMAP(SQL_FN_STR_RTRIM),
    GI_CONST_BITMAP(SQL_FN_STR_SOUNDEX),
    GI_CONST_BITMAP(SQL_FN_STR_SPACE),
    GI_CONST_BITMAP(SQL_FN_STR_SUBSTRING),
    GI_CONST_BITMAP(SQL_FN_STR_UCASE),
    GI_CONST_BITMAP(SQL_FN_SYS_DBNAME),
    GI_CONST_BITMAP(SQL_FN_SYS_IFNULL),
    GI_CONST_BITMAP(SQL_FN_SYS_USERNAME),
    GI_CONST_BITMAP(SQL_FN_TD_CURDATE),
    GI_CONST_BITMAP(SQL_FN_TD_CURRENT_DATE),
    GI_CONST_BITMAP(SQL_FN_TD_CURRENT_TIME),
    GI_CONST_BITMAP(SQL_FN_TD_CURRENT_TIMESTAMP),
    GI_CONST_BITMAP(SQL_FN_TD_CURTIME),
    GI_CONST_BITMAP(SQL_FN_TD_DAYNAME),
    GI_CONST_BITMAP(SQL_FN_TD_DAYOFMONTH),
    GI_CONST_BITMAP(SQL_FN_TD_DAYOFWEEK),
    GI_CONST_BITMAP(SQL_FN_TD_DAYOFYEAR),
    GI_CONST_BITMAP(SQL_FN_TD_EXTRACT),
    GI_CONST_BITMAP(SQL_FN_TD_HOUR),
    GI_CONST_BITMAP(SQL_FN_TD_MINUTE),
    GI_CONST_BITMAP(SQL_FN_TD_MONTH),
    GI_CONST_BITMAP(SQL_FN_TD_MONTHNAME),
    GI_CONST_BITMAP(SQL_FN_TD_NOW),
    GI_CONST_BITMAP(SQL_FN_TD_QUARTER),
    GI_CONST_BITMAP(SQL_FN_TD_SECOND),
    GI_CONST_BITMAP(SQL_FN_TD_TIMESTAMPADD),
    GI_CONST_BITMAP(SQL_FN_TD_TIMESTAMPDIFF),
    GI_CONST_BITMAP(SQL_FN_TD_WEEK),
    GI_CONST_BITMAP(SQL_FN_TD_YEAR),
    GI_CONST_BITMAP(SQL_FN_TSI_DAY),
    GI_CONST_BITMAP(SQL_FN_TSI_FRAC_SECOND),
    GI_CONST_BITMAP(SQL_FN_TSI_HOUR),
    GI_CONST_BITMAP(SQL_FN_TSI_MINUTE),
    GI_CONST_BITMAP(SQL_FN_TSI_MONTH),
    GI_CONST_BITMAP(SQL_FN_TSI_QUARTER),
    GI_CONST_BITMAP(SQL_FN_TSI_SECOND),
    GI_CONST_BITMAP(SQL_FN_TSI_WEEK),
    GI_CONST_BITMAP(SQL_FN_TSI_YEAR),
    GI_CONST_BITMAP(SQL_GB_COLLATE),
    GI_CONST_BITMAP(SQL_GB_GROUP_BY_CONTAINS_SELECT),
    GI_CONST_BITMAP(SQL_GB_GROUP_BY_EQUALS_SELECT),
    GI_CONST_BITMAP(SQL_GB_NO_RELATION),
    GI_CONST_BITMAP(SQL_GB_NOT_SUPPORTED),
    GI_CONST_BITMAP(SQL_GD_ANY_COLUMN),
    GI_CONST_BITMAP(SQL_GD_ANY_ORDER),
    GI_CONST_BITMAP(SQL_GD_BLOCK),
    GI_CONST_BITMAP(SQL_GD_BOUND),
    GI_CONST_BITMAP(SQL_IC_LOWER),
    GI_CONST_BITMAP(SQL_IC_MIXED),
    GI_CONST_BITMAP(SQL_IC_SENSITIVE),
    GI_CONST_BITMAP(SQL_IC_UPPER),
    GI_CONST_BITMAP(SQL_IK_ALL),
    GI_CONST_BITMAP(SQL_IK_ASC),
    GI_CONST_BITMAP(SQL_IK_DESC),
    GI_CONST_BITMAP(SQL_IK_NONE),
    GI_CONST_BITMAP(SQL_IS_INSERT_LITERALS),
    GI_CONST_BITMAP(SQL_IS_INSERT_SEARCHED),
    GI_CONST_BITMAP(SQL_IS_SELECT_INTO),
    GI_CONST_BITMAP(SQL_ISV_ASSERTIONS),
    GI_CONST_BITMAP(SQL_ISV_CHARACTER_SETS),
    GI_CONST_BITMAP(SQL_ISV_CHECK_CONSTRAINTS),
    GI_CONST_BITMAP(SQL_ISV_COLLATIONS),
    GI_CONST_BITMAP(SQL_ISV_COLUMN_DOMAIN_USAGE),
    GI_CONST_BITMAP(SQL_ISV_COLUMN_PRIVILEGES),
    GI_CONST_BITMAP(SQL_ISV_COLUMNS),
    GI_CONST_BITMAP(SQL_ISV_CONSTRAINT_COLUMN_USAGE),
    GI_CONST_BITMAP(SQL_ISV_CONSTRAINT_TABLE_USAGE),
    GI_CONST_BITMAP(SQL_ISV_DOMAIN_CONSTRAINTS),
    GI_CONST_BITMAP(SQL_ISV_DOMAINS),
    GI_CONST_BITMAP(SQL_ISV_KEY_COLUMN_USAGE),
    GI_CONST_BITMAP(SQL_ISV_REFERENTIAL_CONSTRAINTS),
    GI_CONST_BITMAP(SQL_ISV_SCHEMATA),
    GI_CONST_BITMAP(SQL_ISV_SQL_LANGUAGES),
    GI_CONST_BITMAP(SQL_ISV_TABLE_CONSTRAINTS),
    GI_CONST_BITMAP(SQL_ISV_TABLE_PRIVILEGES),
    GI_CONST_BITMAP(SQL_ISV_TABLES),
    GI_CONST_BITMAP(SQL_ISV_TRANSLATIONS),
    GI_CONST_BITMAP(SQL_ISV_USAGE_PRIVILEGES),
    GI_CONST_BITMAP(SQL_ISV_VIEW_COLUMN_USAGE),
    GI_CONST_BITMAP(SQL_ISV_VIEWS),
    GI_CONST_BITMAP(SQL_ISV_VIEW_TABLE_USAGE),
    GI_CONST_BITMAP(SQL_LCK_EXCLUSIVE),
    GI_CONST_BITMAP(SQL_LCK_NO_CHANGE),
    GI_CONST_BITMAP(SQL_LCK_UNLOCK),
    GI_CONST_BITMAP(SQL_NC_END),
    GI_CONST_BITMAP(SQL_NC_HIGH),
    GI_CONST_BITMAP(SQL_NC_LOW),
    GI_CONST_BITMAP(SQL_NC_START),
    GI_CONST_BITMAP(SQL_NNC_NON_NULL),
    GI_CONST_BITMAP(SQL_NNC_NULL),
    GI_CONST_BITMAP(SQL_OAC_LEVEL1),
    GI_CONST_BITMAP(SQL_OAC_LEVEL2),
    GI_CONST_BITMAP(SQL_OAC_NONE),
    GI_CONST_BITMAP(SQL_OIC_CORE),
    GI_CONST_BITMAP(SQL_OIC_LEVEL1),
    GI_CONST_BITMAP(SQL_OIC_LEVEL2),
    GI_CONST_BITMAP(SQL_OJ_ALL_COMPARISON_OPS),
    GI_CONST_BITMAP(SQL_OJ_FULL),
    GI_CONST_BITMAP(SQL_OJ_INNER),
    GI_CONST_BITMAP(SQL_OJ_LEFT),
    GI_CONST_BITMAP(SQL_OJ_NESTED),
    GI_CONST_BITMAP(SQL_OJ_NOT_ORDERED),
    GI_CONST_BITMAP(SQL_OJ_RIGHT),
    GI_CONST_BITMAP(SQL_OSCC_COMPLIANT),
    GI_CONST_BITMAP(SQL_OSCC_NOT_COMPLIANT),
    GI_CONST_BITMAP(SQL_OSC_CORE),
    GI_CONST_BITMAP(SQL_OSC_EXTENDED),
    GI_CONST_BITMAP(SQL_OSC_MINIMUM),
    GI_CONST_BITMAP(SQL_OU_DML_STATEMENTS),
    GI_CONST_BITMAP(SQL_OU_INDEX_DEFINITION),
    GI_CONST_BITMAP(SQL_OU_PRIVILEGE_DEFINITION),
    GI_CONST_BITMAP(SQL_OU_PROCEDURE_INVOCATION),
    GI_CONST_BITMAP(SQL_OU_TABLE_DEFINITION),
    GI_CONST_BITMAP(SQL_PARC_BATCH),
    GI_CONST_BITMAP(SQL_PARC_NO_BATCH),
    GI_CONST_BITMAP(SQL_PAS_BATCH),
    GI_CONST_BITMAP(SQL_PAS_NO_BATCH),
    GI_CONST_BITMAP(SQL_PAS_NO_SELECT),
    GI_CONST_BITMAP(SQL_POS_ADD),
    GI_CONST_BITMAP(SQL_POS_DELETE),
    GI_CONST_BITMAP(SQL_POS_POSITION),
    GI_CONST_BITMAP(SQL_POS_REFRESH),
    GI_CONST_BITMAP(SQL_POS_UPDATE),
    GI_CONST_BITMAP(SQL_PS_POSITIONED_DELETE),
    GI_CONST_BITMAP(SQL_PS_POSITIONED_UPDATE),
    GI_CONST_BITMAP(SQL_PS_SELECT_FOR_UPDATE),
    GI_CONST_BITMAP(SQL_QL_END),
    GI_CONST_BITMAP(SQL_QL_START),
    GI_CONST_BITMAP(SQL_QU_DML_STATEMENTS),
    GI_CONST_BITMAP(SQL_QU_INDEX_DEFINITION),
    GI_CONST_BITMAP(SQL_QU_PRIVILEGE_DEFINITION),
    GI_CONST_BITMAP(SQL_QU_PROCEDURE_INVOCATION),
    GI_CONST_BITMAP(SQL_QU_TABLE_DEFINITION),
    GI_CONST_BITMAP(SQL_SCC_ISO92_CLI),
    GI_CONST_BITMAP(SQL_SCCO_LOCK),
    GI_CONST_BITMAP(SQL_SCCO_OPT_ROWVER),
    GI_CONST_BITMAP(SQL_SCCO_OPT_TIMESTAMP),
    GI_CONST_BITMAP(SQL_SCCO_OPT_VALUES),
    GI_CONST_BITMAP(SQL_SCCO_READ_ONLY),
    GI_CONST_BITMAP(SQL_SCC_XOPEN_CLI_VERSION1),
    GI_CONST_BITMAP(SQL_SC_FIPS127_2_TRANSITIONAL),
    GI_CONST_BITMAP(SQL_SC_NON_UNIQUE),
    GI_CONST_BITMAP(SQL_SC_SQL92_ENTRY),
    GI_CONST_BITMAP(SQL_SC_SQL92_FULL),
    GI_CONST_BITMAP(SQL_SC_SQL92_INTERMEDIATE),
    GI_CONST_BITMAP(SQL_SC_TRY_UNIQUE),
    GI_CONST_BITMAP(SQL_SC_UNIQUE),
    GI_CONST_BITMAP(SQL_SDF_CURRENT_DATE),
    GI_CONST_BITMAP(SQL_SDF_CURRENT_TIME),
    GI_CONST_BITMAP(SQL_SDF_CURRENT_TIMESTAMP),
    GI_CONST_BITMAP(SQL_SFKD_CASCADE),
    GI_CONST_BITMAP(SQL_SFKD_NO_ACTION),
    GI_CONST_BITMAP(SQL_SFKD_SET_DEFAULT),
    GI_CONST_BITMAP(SQL_SFKD_SET_NULL),
    GI_CONST_BITMAP(SQL_SFKU_CASCADE),
    GI_CONST_BITMAP(SQL_SFKU_NO_ACTION),
    GI_CONST_BITMAP(SQL_SFKU_SET_DEFAULT),
    GI_CONST_BITMAP(SQL_SFKU_SET_NULL),
    GI_CONST_BITMAP(SQL_SG_DELETE_TABLE),
    GI_CONST_BITMAP(SQL_SG_INSERT_COLUMN),
    GI_CONST_BITMAP(SQL_SG_INSERT_TABLE),
    GI_CONST_BITMAP(SQL_SG_REFERENCES_COLUMN),
    GI_CONST_BITMAP(SQL_SG_REFERENCES_TABLE),
    GI_CONST_BITMAP(SQL_SG_SELECT_TABLE),
    GI_CONST_BITMAP(SQL_SG_UPDATE_COLUMN),
    GI_CONST_BITMAP(SQL_SG_UPDATE_TABLE),
    GI_CONST_BITMAP(SQL_SG_USAGE_ON_CHARACTER_SET),
    GI_CONST_BITMAP(SQL_SG_USAGE_ON_COLLATION),
    GI_CONST_BITMAP(SQL_SG_USAGE_ON_DOMAIN),
    GI_CONST_BITMAP(SQL_SG_USAGE_ON_TRANSLATION),
    GI_CONST_BITMAP(SQL_SG_WITH_GRANT_OPTION),
    GI_CONST_BITMAP(SQL_SNVF_BIT_LENGTH),
    GI_CONST_BITMAP(SQL_SNVF_CHARACTER_LENGTH),
    GI_CONST_BITMAP(SQL_SNVF_CHAR_LENGTH),
    GI_CONST_BITMAP(SQL_SNVF_EXTRACT),
    GI_CONST_BITMAP(SQL_SNVF_OCTET_LENGTH),
    GI_CONST_BITMAP(SQL_SNVF_POSITION),
    GI_CONST_BITMAP(SQL_SO_DYNAMIC),
    GI_CONST_BITMAP(SQL_SO_FORWARD_ONLY),
    GI_CONST_BITMAP(SQL_SO_KEYSET_DRIVEN),
    GI_CONST_BITMAP(SQL_SO_MIXED),
    GI_CONST_BITMAP(SQL_SO_STATIC),
    GI_CONST_BITMAP(SQL_SP_BETWEEN),
    GI_CONST_BITMAP(SQL_SP_COMPARISON),
    GI_CONST_BITMAP(SQL_SP_EXISTS),
    GI_CONST_BITMAP(SQL_SP_IN),
    GI_CONST_BITMAP(SQL_SP_ISNOTNULL),
    GI_CONST_BITMAP(SQL_SP_ISNULL),
    GI_CONST_BITMAP(SQL_SP_LIKE),
    GI_CONST_BITMAP(SQL_SP_MATCH_FULL),
    GI_CONST_BITMAP(SQL_SP_MATCH_PARTIAL),
    GI_CONST_BITMAP(SQL_SP_MATCH_UNIQUE_FULL),
    GI_CONST_BITMAP(SQL_SP_MATCH_UNIQUE_PARTIAL),
    GI_CONST_BITMAP(SQL_SP_OVERLAPS),
    GI_CONST_BITMAP(SQL_SP_QUANTIFIED_COMPARISON),
    GI_CONST_BITMAP(SQL_SP_UNIQUE),
    GI_CONST_BITMAP(SQL_SQ_COMPARISON),
    GI_CONST_BITMAP(SQL_SQ_CORRELATED_SUBQUERIES),
    GI_CONST_BITMAP(SQL_SQ_EXISTS),
    GI_CONST_BITMAP(SQL_SQ_IN),
    GI_CONST_BITMAP(SQL_SQ_QUANTIFIED),
    GI_CONST_BITMAP(SQL_SR_CASCADE),
    GI_CONST_BITMAP(SQL_SR_DELETE_TABLE),
    GI_CONST_BITMAP(SQL_SR_GRANT_OPTION_FOR),
    GI_CONST_BITMAP(SQL_SR_INSERT_COLUMN),
    GI_CONST_BITMAP(SQL_SR_INSERT_TABLE),
    GI_CONST_BITMAP(SQL_SRJO_CORRESPONDING_CLAUSE),
    GI_CONST_BITMAP(SQL_SRJO_CROSS_JOIN),
    GI_CONST_BITMAP(SQL_SRJO_EXCEPT_JOIN),
    GI_CONST_BITMAP(SQL_SRJO_FULL_OUTER_JOIN),
    GI_CONST_BITMAP(SQL_SRJO_INNER_JOIN),
    GI_CONST_BITMAP(SQL_SRJO_INTERSECT_JOIN),
    GI_CONST_BITMAP(SQL_SRJO_LEFT_OUTER_JOIN),
    GI_CONST_BITMAP(SQL_SRJO_NATURAL_JOIN),
    GI_CONST_BITMAP(SQL_SRJO_RIGHT_OUTER_JOIN),
    GI_CONST_BITMAP(SQL_SRJO_UNION_JOIN),
    GI_CONST_BITMAP(SQL_SR_REFERENCES_COLUMN),
    GI_CONST_BITMAP(SQL_SR_REFERENCES_TABLE),
    GI_CONST_BITMAP(SQL_SR_RESTRICT),
    GI_CONST_BITMAP(SQL_SR_SELECT_TABLE),
    GI_CONST_BITMAP(SQL_SR_UPDATE_COLUMN),
    GI_CONST_BITMAP(SQL_SR_UPDATE_TABLE),
    GI_CONST_BITMAP(SQL_SR_USAGE_ON_CHARACTER_SET),
    GI_CONST_BITMAP(SQL_SR_USAGE_ON_COLLATION),
    GI_CONST_BITMAP(SQL_SR_USAGE_ON_DOMAIN),
    GI_CONST_BITMAP(SQL_SR_USAGE_ON_TRANSLATION),
    GI_CONST_BITMAP(SQL_SRVC_DEFAULT),
    GI_CONST_BITMAP(SQL_SRVC_NULL),
    GI_CONST_BITMAP(SQL_SRVC_ROW_SUBQUERY),
    GI_CONST_BITMAP(SQL_SRVC_VALUE_EXPRESSION),
    GI_CONST_BITMAP(SQL_SS_ADDITIONS),
    GI_CONST_BITMAP(SQL_SS_DELETIONS),
    GI_CONST_BITMAP(SQL_SSF_CONVERT),
    GI_CONST_BITMAP(SQL_SSF_LOWER),
    GI_CONST_BITMAP(SQL_SSF_SUBSTRING),
    GI_CONST_BITMAP(SQL_SSF_TRANSLATE),
    GI_CONST_BITMAP(SQL_SSF_TRIM_BOTH),
    GI_CONST_BITMAP(SQL_SSF_TRIM_LEADING),
    GI_CONST_BITMAP(SQL_SSF_TRIM_TRAILING),
    GI_CONST_BITMAP(SQL_SSF_UPPER),
    GI_CONST_BITMAP(SQL_SS_UPDATES),
    GI_CONST_BITMAP(SQL_SU_DML_STATEMENTS),
    GI_CONST_BITMAP(SQL_SU_INDEX_DEFINITION),
    GI_CONST_BITMAP(SQL_SU_PRIVILEGE_DEFINITION),
    GI_CONST_BITMAP(SQL_SU_PROCEDURE_INVOCATION),
    GI_CONST_BITMAP(SQL_SU_TABLE_DEFINITION),
    GI_CONST_BITMAP(SQL_SVE_CASE),
    GI_CONST_BITMAP(SQL_SVE_CAST),
    GI_CONST_BITMAP(SQL_SVE_COALESCE),
    GI_CONST_BITMAP(SQL_SVE_NULLIF),
    GI_CONST_BITMAP(SQL_TC_ALL),
    GI_CONST_BITMAP(SQL_TC_DDL_COMMIT),
    GI_CONST_BITMAP(SQL_TC_DDL_IGNORE),
    GI_CONST_BITMAP(SQL_TC_DML),
    GI_CONST_BITMAP(SQL_TC_NONE),
    GI_CONST_BITMAP(SQL_TRANSACTION_READ_COMMITTED),
    GI_CONST_BITMAP(SQL_TRANSACTION_READ_UNCOMMITTED),
    GI_CONST_BITMAP(SQL_TRANSACTION_REPEATABLE_READ),
    GI_CONST_BITMAP(SQL_TRANSACTION_SERIALIZABLE),
    GI_CONST_BITMAP(SQL_TXN_READ_COMMITTED),
    GI_CONST_BITMAP(SQL_TXN_READ_UNCOMMITTED),
    GI_CONST_BITMAP(SQL_TXN_REPEATABLE_READ),
    GI_CONST_BITMAP(SQL_TXN_SERIALIZABLE),
#ifdef SQL_TXN_VERSIONING
    GI_CONST_BITMAP(SQL_TXN_VERSIONING),
#endif
    GI_CONST_BITMAP(SQL_US_UNION),
    GI_CONST_BITMAP(SQL_US_UNION_ALL),
    GI_CONST_BITMAP(SQL_U_UNION),
    GI_CONST_BITMAP(SQL_U_UNION_ALL),

    /* end of table */
    GI_CONST_BITMAP_END
};

static VALUE
dbc_getinfo(int argc, VALUE *argv, VALUE self)
{
    DBC *p = get_dbc(self);
    VALUE which, vtype, vstr;
    SQLRETURN ret;
    int i, k, info = -1, maptype = -1, info_found = 0;
    SQLUSMALLINT sbuffer;
    SQLUINTEGER lbuffer;
    SQLSMALLINT len_in, len_out;
    char *string = NULL, buffer[513];

    rb_scan_args(argc, argv, "11", &which, &vtype);
    switch (TYPE(which)) {
    default:
	vstr = rb_any_to_s(which);
	string = STR2CSTR(vstr);
	goto doString;
    case T_STRING:
	string = STR2CSTR(which);
    doString:
	for (i = 0; get_info_map[i].name != NULL; i++) {
	    if (strcmp(string, get_info_map[i].name) == 0) {
		info = get_info_map[i].info;
		maptype = get_info_map[i].maptype;
		info_found = 2;
		break;
	    }
	}
	break;
    case T_FLOAT:
    case T_BIGNUM:
	k = (int) NUM2DBL(which);
	goto doInt;
    case T_FIXNUM:
	k = FIX2INT(which);
    doInt:
	info = k;
	info_found = 1;
	for (i = 0; get_info_map[i].name != NULL; i++) {
	    if (k == get_info_map[i].info) {
		info = get_info_map[i].info;
		maptype = get_info_map[i].maptype;
		info_found = 3;
		break;
	    }
	}
	break;
    }
    switch (info_found) {
    case 0:
	rb_raise(Cerror, "%s",
		 set_err("Invalid info type for ODBC::Connection.get_info",
			 0));
	return Qnil;
    case 1:
	sprintf(buffer, "Unknown info type %d for ODBC::Connection.get_info",
		info);
	set_err(buffer, 1);
	break;
    }
    if (vtype != Qnil) {
	switch (TYPE(vtype)) {
	case T_FIXNUM:
	    maptype = FIX2INT(vtype);
	    break;
	case T_BIGNUM:
	case T_FLOAT:
	    maptype = (int) NUM2DBL(vtype);
	    break;
	default:
	    rb_raise(rb_eTypeError, "need number for sql_type");
	    return Qnil;
	}
	switch (maptype) {
	case SQL_NUMERIC:
	case SQL_DECIMAL:
	case SQL_INTEGER:
	case SQL_FLOAT:
	case SQL_REAL:
	case SQL_DOUBLE:
	case SQL_C_ULONG:
#ifdef SQL_BIGINT
	case SQL_BIGINT:
#endif
#ifdef SQL_C_UBIGINT
	case SQL_C_UBIGINT:
#endif
	    maptype = SQL_C_LONG;
	    break;
	case SQL_SMALLINT:
	case SQL_TINYINT:
	case SQL_C_USHORT:
	case SQL_C_UTINYINT:
	    maptype = SQL_C_SHORT;
	    break;
	default:
	    maptype = SQL_C_CHAR;
	    break;
	}
    }
    switch (maptype) {
    case SQL_C_SHORT:
	len_in = sizeof (sbuffer);
	sbuffer = 0;
	ret = SQLGetInfo(p->hdbc, (SQLUSMALLINT) info,
			 (SQLPOINTER) &sbuffer, len_in, &len_out);
	break;
    case SQL_C_LONG:
	len_in = sizeof (lbuffer);
	lbuffer = 0;
	ret = SQLGetInfo(p->hdbc, (SQLUSMALLINT) info,
			 (SQLPOINTER) &lbuffer, len_in, &len_out);
	break;
    default:
    case SQL_C_CHAR:
	len_in = sizeof (buffer) - 1;
	memset(buffer, 0, sizeof (buffer));
	ret = SQLGetInfo(p->hdbc, (SQLUSMALLINT) info,
			 (SQLPOINTER) buffer, len_in, &len_out);
	break;
    }
    if (!SQL_SUCCEEDED(ret)) {
	rb_raise(Cerror, "%s",
		 get_err(SQL_NULL_HENV, p->hdbc, SQL_NULL_HSTMT));
    }
    switch (maptype) {
    case SQL_C_SHORT:
	return INT2NUM(sbuffer);
    case SQL_C_LONG:
	return INT2NUM(lbuffer);
    default:
    case SQL_C_CHAR:
	return rb_str_new(buffer, len_out);
    }
    return Qnil;
}

/*
 *----------------------------------------------------------------------
 *
 *      Fill column type array for statement.
 *
 *----------------------------------------------------------------------
 */

static COLTYPE *
make_coltypes(SQLHSTMT hstmt, int ncols, char **msgp)
{
    int i;
    COLTYPE *ret = NULL;
    SQLLEN type, size;

    for (i = 0; i < ncols; i++) {
	SQLUSMALLINT ic = i + 1;

	if (!succeeded(SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
		       SQLColAttributes(hstmt, ic,
					SQL_COLUMN_TYPE, NULL, 0, NULL,
					&type),
		       msgp, "SQLColAttributes(SQL_COLUMN_TYPE)")) {
	    return ret;
	}
	if (!succeeded(SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
		       SQLColAttributes(hstmt, ic,
					SQL_COLUMN_DISPLAY_SIZE,
					NULL, 0, NULL, &size),
		       msgp, "SQLColAttributes(SQL_COLUMN_DISPLAY_SIZE)")) {
	    return ret;
	}
    }
    ret = ALLOC_N(COLTYPE, ncols);
    if (ret == NULL) {
	if (msgp != NULL) {
	    *msgp = set_err("Out of memory", 0);
	}
	return NULL;
    }
    for (i = 0; i < ncols; i++) {
	SQLUSMALLINT ic = i + 1;

	callsql(SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
		SQLColAttributes(hstmt, ic,
				 SQL_COLUMN_TYPE,
				 NULL, 0, NULL, &type),
		"SQLColAttributes(SQL_COLUMN_TYPE)");
	callsql(SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
		SQLColAttributes(hstmt, ic,
				 SQL_COLUMN_DISPLAY_SIZE,
				 NULL, 0, NULL, &size),
		"SQLColAttributes(SQL_COLUMN_DISPLAY_SIZE)");
	switch (type) {
#ifdef SQL_BIT
	case SQL_BIT:
#endif
#ifdef SQL_TINYINT
	case SQL_TINYINT:
#endif
	case SQL_SMALLINT:
	case SQL_INTEGER:
	    type = SQL_C_LONG;
	    size = sizeof (SQLINTEGER);
	    break;
	case SQL_FLOAT:
	case SQL_DOUBLE:
	case SQL_REAL:
	    type = SQL_C_DOUBLE;
	    size = sizeof (double);
	    break;
	case SQL_DATE:
#ifdef SQL_TYPE_DATE
	case SQL_TYPE_DATE:
#endif
	    type = SQL_C_DATE;
	    size = sizeof (DATE_STRUCT);
	    break;
	case SQL_TIME:
#ifdef SQL_TYPE_TIME
	case SQL_TYPE_TIME:
#endif
	    type = SQL_C_TIME;
	    size = sizeof (TIME_STRUCT);
	    break;
	case SQL_TIMESTAMP:
#ifdef SQL_TYPE_TIMESTAMP
	case SQL_TYPE_TIMESTAMP:
#endif
	    type = SQL_C_TIMESTAMP;
	    size = sizeof (TIMESTAMP_STRUCT);
	    break;
	case SQL_LONGVARBINARY:
	    type = SQL_C_BINARY;
	    size = SQL_NO_TOTAL;
	    break;
	case SQL_LONGVARCHAR:
#ifdef UNICODE
	case SQL_WLONGVARCHAR:
	    type = SQL_C_WCHAR;
#else
	    type = SQL_C_CHAR;
#endif
	    size = SQL_NO_TOTAL;
	    break;
#ifdef SQL_BIGINT
	case SQL_BIGINT:
	    if (sizeof (SQLBIGINT) == sizeof (SQLINTEGER)) {
		type = SQL_C_LONG;
		size = sizeof (SQLINTEGER);
	    } else {
		type = SQL_C_SBIGINT;
		size = sizeof (SQLBIGINT);
	    }
	    break;
#endif
#ifdef SQL_UBIGINT
	case SQL_UBIGINT:
	    if (sizeof (SQLBIGINT) == sizeof (SQLINTEGER)) {
		type = SQL_C_LONG;
		size = sizeof (SQLINTEGER);
	    } else {
		type = SQL_C_UBIGINT;
		size = sizeof (SQLBIGINT);
	    }
	    break;
#endif
	default:
	    if ((size == 0) || (size > SEGSIZE)) {
		size = SQL_NO_TOTAL;
	    }
#ifdef UNICODE
	    type = SQL_C_WCHAR;
	    if (size != SQL_NO_TOTAL) {
		size *= sizeof (SQLWCHAR);
		size += sizeof (SQLWCHAR);
	    }
#else
	    type = SQL_C_CHAR;
	    if (size != SQL_NO_TOTAL) {
		size += 1;
	    }
#endif
	    break;
	}
	ret[i].type = type;
	ret[i].size = size;
    }
    return ret;
}

/*
 *----------------------------------------------------------------------
 *
 *      Fill parameter info array for statement.
 *
 *----------------------------------------------------------------------
 */

static PARAMINFO *
make_paraminfo(SQLHSTMT hstmt, int nump, char **msgp)
{
    int i;
    PARAMINFO *paraminfo = NULL;

    paraminfo = ALLOC_N(PARAMINFO, nump);
    if (paraminfo == NULL) {
	if (msgp != NULL) {
	    *msgp = set_err("Out of memory", 0);
	}
	return NULL;
    }
    for (i = 0; i < nump; i++) {
	paraminfo[i].iotype = SQL_PARAM_INPUT;
	paraminfo[i].outsize = 0;
	paraminfo[i].outbuf = NULL;
	paraminfo[i].rlen = SQL_NULL_DATA;
	paraminfo[i].ctype = SQL_C_CHAR;
#ifdef UNICODE
	paraminfo[i].outtype = SQL_WCHAR;
#else
	paraminfo[i].outtype = SQL_WCHAR;
#endif
	paraminfo[i].coldef_max = 0;
	if (!succeeded(SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
		       SQLDescribeParam(hstmt, (SQLUSMALLINT) (i + 1),
					&paraminfo[i].type,
					&paraminfo[i].coldef,
					&paraminfo[i].scale,
					&paraminfo[i].nullable),
		       NULL, "SQLDescribeParam")) {
#ifdef UNICODE
	    paraminfo[i].type = SQL_WVARCHAR;
#else
	    paraminfo[i].type = SQL_VARCHAR;
#endif
	    paraminfo[i].coldef = 0;
	    paraminfo[i].scale = 0;
	    paraminfo[i].nullable = SQL_NULLABLE_UNKNOWN;
	    paraminfo[i].override = 0;
	}
    }
    return paraminfo;
}

static void
retain_paraminfo_override(STMT *q, int nump, PARAMINFO *paraminfo)
{
    if ((q->paraminfo != NULL) && (q->nump == nump)) {
	int i;

	for (i = 0; i < nump; i++) {
	    paraminfo[i].iotype = q->paraminfo[i].iotype;
	    paraminfo[i].rlen = q->paraminfo[i].rlen;
	    paraminfo[i].ctype = q->paraminfo[i].ctype;
	    paraminfo[i].outtype = q->paraminfo[i].outtype;
	    paraminfo[i].outsize = q->paraminfo[i].outsize;
	    if (q->paraminfo[i].outbuf != NULL) {
		paraminfo[i].outbuf = q->paraminfo[i].outbuf;
		q->paraminfo[i].outbuf = NULL;
	    }
	    if (q->paraminfo[i].override) {
		paraminfo[i].override = q->paraminfo[i].override;
		paraminfo[i].type = q->paraminfo[i].type;
		paraminfo[i].coldef = q->paraminfo[i].coldef;
		paraminfo[i].scale = q->paraminfo[i].scale;
	    }
	}
    }
}

/*
 *----------------------------------------------------------------------
 *
 *      Wrap SQLHSTMT into struct/VALUE.
 *
 *----------------------------------------------------------------------
 */

static VALUE
wrap_stmt(VALUE dbc, DBC *p, SQLHSTMT hstmt, STMT **qp)
{
    VALUE stmt = Qnil;
    STMT *q;
    int i;

    stmt = Data_Make_Struct(Cstmt, STMT, mark_stmt, free_stmt, q);
    tracemsg(2, fprintf(stderr, "ObjAlloc: STMT %p\n", q););
    list_init(&q->link, offsetof(STMT, link));
    q->self = stmt;
    q->hstmt = hstmt;
    q->dbc = dbc;
    q->dbcp = NULL;
    q->paraminfo = NULL;
    q->coltypes = NULL;
    q->colnames = q->dbufs = NULL;
    q->colvals = NULL;
    q->fetchc = 0;
    q->upc = p->upc;
    q->usef = 0;
    rb_iv_set(q->self, "@_a", rb_ary_new());
    rb_iv_set(q->self, "@_h", rb_hash_new());
    for (i = 0; i < 4; i++) {
	rb_iv_set(q->self, colnamebuf[i], rb_hash_new());
    }
    if (hstmt != SQL_NULL_HSTMT) {
	link_stmt(q, p);
    } else {
	q->dbc = Qnil;
    }
    if (qp != NULL) {
	*qp = q;
    }
    return stmt;
}

/*
 *----------------------------------------------------------------------
 *
 *      Create statement with result.
 *
 *----------------------------------------------------------------------
 */

static VALUE
make_result(VALUE dbc, SQLHSTMT hstmt, VALUE result, int mode)
{
    DBC *p;
    STMT *q;
    SQLSMALLINT cols = 0, nump;
    COLTYPE *coltypes = NULL;
    PARAMINFO *paraminfo = NULL;
    char *msg = NULL;

    Data_Get_Struct(dbc, DBC, p);
    if ((hstmt == SQL_NULL_HSTMT) ||
	!succeeded(SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
		   SQLNumParams(hstmt, &nump), NULL, "SQLNumParams")) {
	nump = 0;
    }
    if (nump > 0) {
	paraminfo = make_paraminfo(hstmt, nump, &msg);
	if (paraminfo == NULL) {
	    goto error;
	}
    }
    if ((mode & MAKERES_PREPARE) ||
	(hstmt == SQL_NULL_HSTMT) ||
	(!succeeded(SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
		   SQLNumResultCols(hstmt, &cols), NULL,
		   "SQLNumResultCols"))) {
	cols = 0;
    }
    if (cols > 0) {
	coltypes = make_coltypes(hstmt, cols, &msg);
	if (coltypes == NULL) {
	    goto error;
	}
    }
    if (result == Qnil) {
	result = wrap_stmt(dbc, p, hstmt, &q);
    } else {
	Data_Get_Struct(result, STMT, q);
	retain_paraminfo_override(q, nump, paraminfo);
	free_stmt_sub(q, 1);
	if (q->dbc != dbc) {
	    unlink_stmt(q);
	    q->dbc = dbc;
	    if (hstmt != SQL_NULL_HSTMT) {
		link_stmt(q, p);
	    }
	}
	q->hstmt = hstmt;
    }
    q->nump = nump;
    q->paraminfo = paraminfo;
    q->ncols = cols;
    q->coltypes = coltypes;
    if ((mode & MAKERES_BLOCK) && rb_block_given_p()) {
	if (mode & MAKERES_NOCLOSE) {
	    return rb_yield(result);
	}
	return rb_ensure(rb_yield, result, stmt_close, result);
    }
    return result;
error:
    callsql(SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
	    SQLFreeStmt(hstmt, SQL_DROP), "SQLFreeStmt(SQL_DROP)");
    if (result != Qnil) {
	Data_Get_Struct(result, STMT, q);
	if (q->hstmt == hstmt) {
	    q->hstmt = SQL_NULL_HSTMT;
	    unlink_stmt(q);
	}
    }
    if (paraminfo != NULL) {
	xfree(paraminfo);
    }
    if (coltypes != NULL) {
	xfree(coltypes);
    }
    rb_raise(Cerror, "%s", msg);
    return Qnil;
}

static char *
upcase_if(char *string, int upc)
{
    if (upc && (string != NULL)) {
	unsigned char *p = (unsigned char *) string;

	while (*p != '\0') {
#ifdef UNICODE
	    if ((*p < 0x80) && ISLOWER(*p))
#else
	    if (ISLOWER(*p))
#endif
	    {
		*p = toupper(*p);
	    }
	    ++p;
	}
    }
    return string;
}

/*
 *----------------------------------------------------------------------
 *
 *      Constructor: make column from statement.
 *
 *----------------------------------------------------------------------
 */

static VALUE
make_column(SQLHSTMT hstmt, int i, int upc)
{
    VALUE obj, v;
    SQLUSMALLINT ic = i + 1;
    SQLLEN iv = 0;
#ifdef UNICODE
    SQLWCHAR name[SQL_MAX_MESSAGE_LENGTH];
#else
    char name[SQL_MAX_MESSAGE_LENGTH];
#endif
    char *msg;
    SQLSMALLINT name_len;

    name[0] = 0;
    if (!succeeded(SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
		   SQLColAttributes(hstmt, ic, SQL_COLUMN_LABEL, name,
				    (SQLSMALLINT) sizeof (name),
				    &name_len, NULL),
		   &msg, "SQLColAttributes(SQL_COLUMN_LABEL)")) {
	rb_raise(Cerror, "%s", msg);
    }
    obj = rb_obj_alloc(Ccolumn);
    if (name_len >= (SQLSMALLINT) sizeof (name)) {
	name_len = sizeof (name) - 1;
    }
    if (name_len > 0) {
	name[name_len / sizeof (name[0])] = 0;
    }
#ifdef UNICODE
    if (upc) {
	int len = uc_strlen(name);
	char tmpbuf[1];
	char *tmp = xmalloc(len);
	VALUE v;

	if (tmp == NULL) {
	    tmp = tmpbuf;
	    len = 0;
	}
	mkutf(tmp, name, len);
	v = rb_tainted_str_new2(upcase_if(tmp, 1));
#ifdef USE_RB_ENC
	rb_enc_associate(v, rb_enc);
#endif
	rb_iv_set(obj, "@name", v);
	if ((tmp != NULL) && (tmp != tmpbuf)) {
	    xfree(tmp);
	}
    } else {
	rb_iv_set(obj, "@name", uc_tainted_str_new2(name));
    }
#else
    rb_iv_set(obj, "@name", rb_tainted_str_new2(upcase_if(name, upc)));
#endif
    v = Qnil;
    name[0] = 0;
    if (succeeded(SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
		  SQLColAttributes(hstmt, ic, SQL_COLUMN_TABLE_NAME, name,
				   (SQLSMALLINT) sizeof (name),
				   &name_len, NULL),
		  NULL, "SQLColAttributes(SQL_COLUMN_TABLE_NAME)")) {
	if (name_len > (SQLSMALLINT) sizeof (name)) {
	    name_len = sizeof (name) - 1;
	}
	if (name_len > 0) {
	    name[name_len / sizeof (name[0])] = 0;
	}
#ifdef UNICODE
	v = uc_tainted_str_new2(name);
#else
	v = rb_tainted_str_new2(name);
#endif
    }
    rb_iv_set(obj, "@table", v);
    if (succeeded(SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
		  SQLColAttributes(hstmt, ic, SQL_COLUMN_TYPE, NULL,
				   0, NULL, &iv),
		  NULL, "SQLColAttributes(SQL_COLUMN_TYPE)")) {
	v = INT2NUM(iv);
    } else {
	v = INT2NUM(SQL_UNKNOWN_TYPE);
    }
    rb_iv_set(obj, "@type", v);
    v = Qnil;
    if (succeeded(SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
		  SQLColAttributes(hstmt, ic,
#if (ODBCVER >= 0x0300)
				   SQL_DESC_LENGTH,
#else
				   SQL_COLUMN_LENGTH,
#endif
				   NULL, 0, NULL, &iv),
		  NULL,
#if (ODBCVER >= 0x0300)
		  "SQLColAttributes(SQL_DESC_LENGTH)"
#else
		  "SQLColAttributes(SQL_COLUMN_LENGTH)"
#endif
	)) {
	v = INT2NUM(iv);
    } else if (succeeded(SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
			 SQLColAttributes(hstmt, ic,
					  SQL_COLUMN_DISPLAY_SIZE, NULL,
					  0, NULL, &iv),
			 NULL, "SQLColAttributes(SQL_COLUMN_DISPLAY_SIZE)")) {
	v = INT2NUM(iv);
    }
    rb_iv_set(obj, "@length", v);
    v = Qnil;
    if (succeeded(SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
		  SQLColAttributes(hstmt, ic, SQL_COLUMN_NULLABLE, NULL,
				   0, NULL, &iv),
		  NULL, "SQLColAttributes(SQL_COLUMN_NULLABLE)")) {
	v = (iv == SQL_NO_NULLS) ? Qfalse : Qtrue;
    }
    rb_iv_set(obj, "@nullable", v);
    v = Qnil;
    if (succeeded(SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
		  SQLColAttributes(hstmt, ic, SQL_COLUMN_SCALE, NULL,
				   0, NULL, &iv),
		  NULL, "SQLColAttributes(SQL_COLUMN_SCALE)")) {
	v = INT2NUM(iv);
    }
    rb_iv_set(obj, "@scale", v);
    v = Qnil;
    if (succeeded(SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
		  SQLColAttributes(hstmt, ic, SQL_COLUMN_PRECISION, NULL,
				   0, NULL, &iv),
		  NULL, "SQLColAttributes(SQL_COLUMN_PRECISION)")) {
	v = INT2NUM(iv);
    }
    rb_iv_set(obj, "@precision", v);
    v = Qnil;
    if (succeeded(SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
		  SQLColAttributes(hstmt, ic, SQL_COLUMN_SEARCHABLE, NULL,
				   0, NULL, &iv),
		  NULL, "SQLColAttributes(SQL_COLUMN_SEARCHABLE)")) {
	v = (iv == SQL_NO_NULLS) ? Qfalse : Qtrue;
    }
    rb_iv_set(obj, "@searchable", v);
    v = Qnil;
    if (succeeded(SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
		  SQLColAttributes(hstmt, ic, SQL_COLUMN_UNSIGNED, NULL,
				   0, NULL, &iv),
		  NULL, "SQLColAttributes(SQL_COLUMN_UNSIGNED)")) {
	v = (iv == SQL_NO_NULLS) ? Qfalse : Qtrue;
    }
    rb_iv_set(obj, "@unsigned", v);
    v = Qnil;
#ifdef SQL_COLUMN_AUTO_INCREMENT
    if (succeeded(SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
		  SQLColAttributes(hstmt, ic, SQL_COLUMN_AUTO_INCREMENT, NULL,
				   0, NULL, &iv),
		  NULL, "SQLColAttributes(SQL_COLUMN_AUTO_INCREMENT)")) {
	v = (iv == SQL_FALSE) ? Qfalse : Qtrue;
    }
#endif
    rb_iv_set(obj, "@autoincrement", v);
    return obj;
}

/*
 *----------------------------------------------------------------------
 *
 *      Constructor: make parameter from statement.
 *
 *----------------------------------------------------------------------
 */

static VALUE
make_param(STMT *q, int i)
{
    VALUE obj;
    int v;

    obj = rb_obj_alloc(Cparam);
#ifdef UNICODE
    v = q->paraminfo ? q->paraminfo[i].type : SQL_VARCHAR;
#else
    v = q->paraminfo ? q->paraminfo[i].type : SQL_WVARCHAR;
#endif
    rb_iv_set(obj, "@type", INT2NUM(v));
    v = q->paraminfo ? q->paraminfo[i].coldef : 0;
    rb_iv_set(obj, "@precision", INT2NUM(v));
    v = q->paraminfo ? q->paraminfo[i].scale : 0;
    rb_iv_set(obj, "@scale", INT2NUM(v));
    v = q->paraminfo ? q->paraminfo[i].nullable : SQL_NULLABLE_UNKNOWN;
    rb_iv_set(obj, "@nullable", INT2NUM(v));
    v = q->paraminfo ? q->paraminfo[i].iotype : SQL_PARAM_INPUT;
    rb_iv_set(obj, "@iotype", INT2NUM(v));
    v = q->paraminfo ? q->paraminfo[i].outsize : 0;
    rb_iv_set(obj, "@output_size", INT2NUM(v));
#ifdef UNICODE
    v = q->paraminfo ? q->paraminfo[i].outtype : SQL_WCHAR;
#else
    v = q->paraminfo ? q->paraminfo[i].outtype : SQL_CHAR;
#endif
    rb_iv_set(obj, "@output_type", INT2NUM(v));
    return obj;
}

/*
 *----------------------------------------------------------------------
 *
 *      Query tables/columns/keys/indexes/types of data source.
 *
 *----------------------------------------------------------------------
 */

static VALUE
dbc_info(int argc, VALUE *argv, VALUE self, int mode)
{
    DBC *p = get_dbc(self);
    VALUE which = Qnil, which2 = Qnil, which3 = Qnil;
#ifdef UNICODE
    SQLWCHAR *swhich = NULL, *swhich2 = NULL;
#else
    SQLCHAR *swhich = NULL, *swhich2 = NULL;
#endif
    char *msg;
    const char *argspec = NULL;
    SQLHSTMT hstmt;
    int needstr = 1, itype = SQL_ALL_TYPES;
    int iid = SQL_BEST_ROWID, iscope = SQL_SCOPE_CURROW;

    if (p->hdbc == SQL_NULL_HDBC) {
	rb_raise(Cerror, "%s", set_err("No connection", 0));
    }
    switch (mode) {
    case INFO_TYPES:
	needstr = 0;
	/* FALL THRU */
    case INFO_TABLES:
    case INFO_PRIMKEYS:
    case INFO_TPRIV:
    case INFO_PROCS:
	argspec = "01";
	break;
    case INFO_COLUMNS:
	argspec = "02";
	break;
    case INFO_INDEXES:
    case INFO_FORKEYS:
    case INFO_PROCCOLS:
	argspec = "11";
	break;
    case INFO_SPECCOLS:
	argspec = "12";
	break;
    default:
	rb_raise(Cerror, "%s", set_err("Invalid info mode", 0));
	break;
    }
    rb_scan_args(argc, argv, argspec, &which, &which2, &which3);
    if (which != Qnil) {
	if (needstr) {
	    Check_Type(which, T_STRING);
#ifdef UNICODE
	    swhich = (SQLWCHAR *) which;
#else
	    swhich = (SQLCHAR *) STR2CSTR(which);
#endif
	} else {
	    itype = NUM2INT(which);
	}
    }
    if (which2 != Qnil) {
	if (mode == INFO_SPECCOLS) {
	    iid = NUM2INT(which2);
	} else if (mode != INFO_INDEXES) {
	    Check_Type(which2, T_STRING);
#ifdef UNICODE
	    swhich2 = (SQLWCHAR *) which2;
#else
	    swhich2 = (SQLCHAR *) STR2CSTR(which2);
#endif
	}
    }
#ifdef UNICODE
    if (swhich != NULL) {
	VALUE val = (VALUE) swhich;

#ifdef USE_RB_ENC
	val = rb_funcall(val, IDencode, 1, rb_encv);
#endif
	swhich = uc_from_utf((unsigned char *) STR2CSTR(val), -1);
	if (swhich == NULL) {
	    rb_raise(Cerror, "%s", set_err("Out of memory", 0));
	}
    }
    if (swhich2 != NULL) {
	VALUE val = (VALUE) swhich2;

#ifdef USE_RB_ENC
	val = rb_funcall(val, IDencode, 1, rb_encv);
#endif
	swhich2 = uc_from_utf((unsigned char *) STR2CSTR(val), -1);
	if (swhich2 == NULL) {
	    uc_free(swhich);
	    rb_raise(Cerror, "%s", set_err("Out of memory", 0));
	}
    }
#endif
    if (which3 != Qnil) {
	iscope = NUM2INT(which3);
    }
    if (!succeeded(SQL_NULL_HENV, p->hdbc, SQL_NULL_HSTMT,
		   SQLAllocStmt(p->hdbc, &hstmt), &msg, "SQLAllocStmt")) {
#ifdef UNICODE
	uc_free(swhich);
	uc_free(swhich2);
#endif
	rb_raise(Cerror, "%s", msg);
    }
    switch (mode) {
    case INFO_TABLES:
	if (!succeeded(SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
		       SQLTables(hstmt, NULL, 0, NULL, 0,
				 swhich, (swhich == NULL) ? 0 : SQL_NTS,
				 NULL, 0),
		       &msg, "SQLTables")) {
	    goto error;
	}
	break;
    case INFO_COLUMNS:
	if (!succeeded(SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
		       SQLColumns(hstmt, NULL, 0, NULL, 0,
				  swhich, (swhich == NULL) ? 0 : SQL_NTS,
				  swhich2, (swhich2 == NULL) ? 0 : SQL_NTS),
		       &msg, "SQLColumns")) {
	    goto error;
	}
	break;
    case INFO_PRIMKEYS:
	if (!succeeded(SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
		       SQLPrimaryKeys(hstmt, NULL, 0, NULL, 0,
				      swhich, (swhich == NULL) ? 0 : SQL_NTS),
		       &msg, "SQLPrimaryKeys")) {
	    goto error;
	}
	break;
    case INFO_INDEXES:
	if (!succeeded(SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
		       SQLStatistics(hstmt, NULL, 0, NULL, 0,
				     swhich, (swhich == NULL) ? 0 : SQL_NTS,
				     (SQLUSMALLINT) (RTEST(which2) ?
				      SQL_INDEX_UNIQUE : SQL_INDEX_ALL),
				     SQL_ENSURE),
		       &msg, "SQLStatistics")) {
	    goto error;
	}
	break;
    case INFO_TYPES:
	if (!succeeded(SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
		       SQLGetTypeInfo(hstmt, (SQLSMALLINT) itype),
		       &msg, "SQLGetTypeInfo")) {
	    goto error;
	}
	break;
    case INFO_FORKEYS:
	if (!succeeded(SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
		       SQLForeignKeys(hstmt, NULL, 0, NULL, 0,
				      swhich, (swhich == NULL) ? 0 : SQL_NTS,
				      NULL, 0, NULL, 0,
				      swhich2, (swhich2 == NULL) ? 0 : SQL_NTS),
		       &msg, "SQLForeignKeys")) {
	    goto error;
	}
	break;
    case INFO_TPRIV:
	if (!succeeded(SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
		       SQLTablePrivileges(hstmt, NULL, 0, NULL, 0, swhich,
					  (swhich == NULL) ? 0 : SQL_NTS),
		       &msg, "SQLTablePrivileges")) {
	    goto error;
	}
	break;
    case INFO_PROCS:
	if (!succeeded(SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
		       SQLProcedures(hstmt, NULL, 0, NULL, 0,
				     swhich, (swhich == NULL) ? 0 : SQL_NTS),
		       &msg, "SQLProcedures")) {
	    goto error;
	}
	break;
    case INFO_PROCCOLS:
	if (!succeeded(SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
		       SQLProcedureColumns(hstmt, NULL, 0, NULL, 0,
					   swhich,
					   (swhich == NULL) ? 0 : SQL_NTS,
					   swhich2,
					   (swhich2 == NULL) ? 0 : SQL_NTS),
		       &msg, "SQLProcedureColumns")) {
	    goto error;
	}
	break;
    case INFO_SPECCOLS:
	if (!succeeded(SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
		       SQLSpecialColumns(hstmt, (SQLUSMALLINT) iid,
					 NULL, 0, NULL, 0,
					 swhich,
					 (swhich == NULL) ? 0 : SQL_NTS,
					 (SQLUSMALLINT) iscope,
					 SQL_NULLABLE),
		       &msg, "SQLSpecialColumns")) {
	    goto error;
	}
	break;
    }
#ifdef UNICODE
    uc_free(swhich);
    uc_free(swhich2);
#endif
    return make_result(self, hstmt, Qnil, MAKERES_BLOCK);
error:
#ifdef UNICODE
    uc_free(swhich);
    uc_free(swhich2);
#endif
    callsql(SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
	    SQLFreeStmt(hstmt, SQL_DROP), "SQLFreeStmt(SQL_DROP)");
    rb_raise(Cerror, "%s", msg);
    return Qnil;
}

static VALUE
dbc_tables(int argc, VALUE *argv, VALUE self)
{
    return dbc_info(argc, argv, self, INFO_TABLES);
}

static VALUE
dbc_columns(int argc, VALUE *argv, VALUE self)
{
    return dbc_info(argc, argv, self, INFO_COLUMNS);
}

static VALUE
dbc_primkeys(int argc, VALUE *argv, VALUE self)
{
    return dbc_info(argc, argv, self, INFO_PRIMKEYS);
}

static VALUE
dbc_indexes(int argc, VALUE *argv, VALUE self)
{
    return dbc_info(argc, argv, self, INFO_INDEXES);
}

static VALUE
dbc_types(int argc, VALUE *argv, VALUE self)
{
    return dbc_info(argc, argv, self, INFO_TYPES);
}

static VALUE
dbc_forkeys(int argc, VALUE *argv, VALUE self)
{
    return dbc_info(argc, argv, self, INFO_FORKEYS);
}

static VALUE
dbc_tpriv(int argc, VALUE *argv, VALUE self)
{
    return dbc_info(argc, argv, self, INFO_TPRIV);
}

static VALUE
dbc_procs(int argc, VALUE *argv, VALUE self)
{
    return dbc_info(argc, argv, self, INFO_PROCS);
}

static VALUE
dbc_proccols(int argc, VALUE *argv, VALUE self)
{
    return dbc_info(argc, argv, self, INFO_PROCCOLS);
}

static VALUE
dbc_speccols(int argc, VALUE *argv, VALUE self)
{
    return dbc_info(argc, argv, self, INFO_SPECCOLS);
}

/*
 *----------------------------------------------------------------------
 *
 *      Transaction stuff.
 *
 *----------------------------------------------------------------------
 */

static VALUE
dbc_trans(VALUE self, int what)
{
    ENV *e;
    SQLHDBC dbc = SQL_NULL_HDBC;
    char *msg;

    e = get_env(self);
    if (rb_obj_is_kind_of(self, Cdbc) == Qtrue) {
	DBC *d;

	d = get_dbc(self);
	dbc = d->hdbc;
    }
    if (!succeeded(e->henv, dbc, SQL_NULL_HSTMT,
#if (ODBCVER >= 0x0300)
		   SQLEndTran((SQLSMALLINT)
			      ((dbc == SQL_NULL_HDBC) ? SQL_HANDLE_ENV :
							SQL_HANDLE_DBC),
			      (dbc == SQL_NULL_HDBC) ? e->henv : dbc,
			      (SQLSMALLINT) what),
		   &msg, "SQLEndTran"
#else
		   SQLTransact(e->henv, dbc, (SQLUSMALLINT) what),
		   &msg, "SQLTransact"
#endif
       )) {
	rb_raise(Cerror, "%s", msg);
    }
    return Qnil;
}

static VALUE
dbc_commit(VALUE self)
{
    return dbc_trans(self, SQL_COMMIT);
}

static VALUE
dbc_rollback(VALUE self)
{
    return dbc_trans(self, SQL_ROLLBACK);
}

static VALUE
dbc_nop(VALUE self)
{
    return Qnil;
}

static VALUE
dbc_transbody(VALUE self)
{
    return rb_yield(rb_ary_entry(self, 0));
}

static VALUE
dbc_transfail(VALUE self, VALUE err)
{
    rb_ary_store(self, 1, err);
    dbc_rollback(rb_ary_entry(self, 0));
    return Qundef;
}

static VALUE
dbc_transaction(VALUE self)
{
    VALUE a, ret;

    if (!rb_block_given_p()) {
	rb_raise(rb_eArgError, "block required");
    }
    rb_ensure(dbc_commit, self, dbc_nop, self);
    a = rb_ary_new2(2);
    rb_ary_store(a, 0, self);
    rb_ary_store(a, 1, Qnil);
    if ((ret = rb_rescue2(dbc_transbody, a, dbc_transfail, a,
			  rb_eException, (VALUE) 0)) != Qundef) {
	dbc_commit(self);
	return ret;
    }
    ret = rb_ary_entry(a, 1);
    rb_exc_raise(rb_exc_new3(rb_obj_class(ret),
			     rb_funcall(ret, IDto_s, 0, 0)));
    return Qnil;
}

/*
 *----------------------------------------------------------------------
 *
 *      Environment attribute handling.
 *
 *----------------------------------------------------------------------
 */

#if (ODBCVER >= 0x0300)
static VALUE
do_attr(int argc, VALUE *argv, VALUE self, int op)
{
    SQLHENV henv = SQL_NULL_HENV;
    VALUE val;
    SQLLEN v = 0;
    SQLPOINTER vp;
    SQLINTEGER l;
    char *msg;

    if (self != Modbc) {
	henv = get_env(self)->henv;
    }
    rb_scan_args(argc, argv, "01", &val);
    if (val == Qnil) {
	if (!succeeded(henv, SQL_NULL_HDBC, SQL_NULL_HSTMT,
		       SQLGetEnvAttr(henv, (SQLINTEGER) op,
				     (SQLPOINTER) &v, sizeof (v), &l),
		       &msg, "SQLGetEnvAttr(%d)", op)) {
	    rb_raise(Cerror, "%s", msg);
	}
	return rb_int2inum(v);
    }
    v = NUM2INT(val);
    vp = (SQLPOINTER) v;
    if (!succeeded(henv, SQL_NULL_HDBC, SQL_NULL_HSTMT,
		   SQLSetEnvAttr(henv, (SQLINTEGER) op, vp, SQL_IS_INTEGER),
		   &msg, "SQLSetEnvAttr(%d)", op)) {
	rb_raise(Cerror, "%s", msg);
    }
    return Qnil;
}
#endif

static VALUE
env_cpooling(int argc, VALUE *argv, VALUE self)
{
#if (ODBCVER >= 0x0300)
    return do_attr(argc, argv, self, SQL_ATTR_CONNECTION_POOLING);
#else
    rb_raise(Cerror, "%s", set_err("Unsupported in ODBC < 3.0", 0));
    return Qnil;
#endif
}

static VALUE
env_cpmatch(int argc, VALUE *argv, VALUE self)
{
#if (ODBCVER >= 0x0300)
    return do_attr(argc, argv, self, SQL_ATTR_CP_MATCH);
#else
    rb_raise(Cerror, "%s", set_err("Unsupported in ODBC < 3.0", 0));
    return Qnil;
#endif
}

static VALUE
env_odbcver(int argc, VALUE *argv, VALUE self)
{
#if (ODBCVER >= 0x0300)
    return do_attr(argc, argv, self, SQL_ATTR_ODBC_VERSION);
#else
    VALUE val;

    rb_scan_args(argc, argv, "01", &val);
    if (val == Qnil) {
	return rb_int2inum(ODBCVER >> 8);
    }
    rb_raise(Cerror, "%s", set_err("Unsupported in ODBC < 3.0", 0));
#endif
}

/*
 *----------------------------------------------------------------------
 *
 *      Connection/statement option handling.
 *
 *      Note:
 *      ODBC 2 allows statement options to be set using SQLSetConnectOption,
 *      establishing the statement option as a default for any hstmts
 *      later allocated for that hdbc. This feature was deprecated in
 *      ODBC 3.x and may not work with ODBC 3.x drivers.
 *
 *      Although the Database class includes attribute accessors for
 *      statement-level options, a safer alternative, if using an ODBC 3
 *      driver, is to set the option directly on the Statement instance.
 *
 *----------------------------------------------------------------------
 */

#define OPT_LEVEL_STMT         1
#define OPT_LEVEL_DBC          2
#define OPT_LEVEL_BOTH         (OPT_LEVEL_STMT | OPT_LEVEL_DBC)

#define OPT_CONST_INT(x, level) { #x, x, level }
#define OPT_CONST_END    { NULL, -1 }
static struct {
    const char *name;
    int option;
    int level;
} option_map[] = {

    /* yielding ints */
    OPT_CONST_INT(SQL_AUTOCOMMIT, OPT_LEVEL_DBC),
    OPT_CONST_INT(SQL_NOSCAN, OPT_LEVEL_BOTH),
    OPT_CONST_INT(SQL_CONCURRENCY, OPT_LEVEL_BOTH),
    OPT_CONST_INT(SQL_QUERY_TIMEOUT, OPT_LEVEL_BOTH),
    OPT_CONST_INT(SQL_MAX_ROWS, OPT_LEVEL_BOTH),
    OPT_CONST_INT(SQL_MAX_LENGTH, OPT_LEVEL_BOTH),
    OPT_CONST_INT(SQL_ROWSET_SIZE, OPT_LEVEL_BOTH),
    OPT_CONST_INT(SQL_CURSOR_TYPE, OPT_LEVEL_BOTH),

    /* end of table */
    OPT_CONST_END
};

static VALUE
do_option(int argc, VALUE *argv, VALUE self, int isstmt, int op)
{
    DBC *p = NULL;
    STMT *q = NULL;
    VALUE val, val2, vstr;
    SQLINTEGER v;
    char *msg;
    int level = isstmt ? OPT_LEVEL_STMT : OPT_LEVEL_DBC;

    rb_scan_args(argc, argv, (op == -1) ? "11" : "01", &val, &val2);
    if (isstmt) {
	Data_Get_Struct(self, STMT, q);
	if (q->dbc == Qnil) {
	    rb_raise(Cerror, "%s", set_err("Stale ODBC::Statement", 0));
	}
	if (q->hstmt == SQL_NULL_HSTMT) {
	    rb_raise(Cerror, "%s", set_err("No statement", 0));
	}
    } else {
	p = get_dbc(self);
	if (p->hdbc == SQL_NULL_HDBC) {
	    rb_raise(Cerror, "%s", set_err("No connection", 0));
	}
    }
    if (op == -1) {
	char *string;
	int i, op_found = 0;

	switch (TYPE(val)) {
	default:
	    vstr = rb_any_to_s(val);
	    string = STR2CSTR(vstr);
	    goto doString;
	case T_STRING:
	    string = STR2CSTR(val);
	doString:
	    for (i = 0; option_map[i].name != NULL; i++) {
		if (strcmp(string, option_map[i].name) == 0) {
		    op = option_map[i].option;
		    level = option_map[i].level;
		    op_found = 3;
		    break;
		}
	    }
	    break;
	case T_FLOAT:
	case T_BIGNUM:
	    op = (int) NUM2DBL(val);
	    goto doInt;
	case T_FIXNUM:
	    op = FIX2INT(val);
	doInt:
	    op_found = 1;
	    for (i = 0; option_map[i].name != NULL; i++) {
		if (op == option_map[i].option) {
		    level = option_map[i].level;
		    op_found = 2;
		    break;
		}
	    }
	    break;
	}
	if (!op_found) {
	    rb_raise(Cerror, "%s", set_err("Unknown option", 0));
	    return Qnil;
	}
	val = val2;
    }
    if ((isstmt && (!(level & OPT_LEVEL_STMT))) ||
	(!isstmt && (!(level & OPT_LEVEL_DBC)))) {
	rb_raise(Cerror, "%s",
		 set_err("Invalid option type for this level", 0));
	return Qnil;
    }
    if (val == Qnil) {
	if (p != NULL) {
	    if (!succeeded(SQL_NULL_HENV, p->hdbc, SQL_NULL_HSTMT,
			   SQLGetConnectOption(p->hdbc, (SQLUSMALLINT) op,
					       (SQLPOINTER) &v),
			   &msg, "SQLGetConnectOption(%d)", op)) {
		rb_raise(Cerror, "%s", msg);
	    }
	} else {
	    if (!succeeded(SQL_NULL_HENV, SQL_NULL_HSTMT, q->hstmt,
			   SQLGetStmtOption(q->hstmt, (SQLUSMALLINT) op,
					    (SQLPOINTER) &v),
			   &msg, "SQLGetStmtOption(%d)", op)) {
		rb_raise(Cerror, "%s", msg);
	    }
	}
    }
    switch (op) {
    case SQL_AUTOCOMMIT:
	if (val == Qnil) {
	    return v ? Qtrue : Qfalse;
	}
	v = (TYPE(val) == T_FIXNUM) ?  
	    (FIX2INT(val) ? SQL_AUTOCOMMIT_ON : SQL_AUTOCOMMIT_OFF) : 
	    (RTEST(val) ? SQL_AUTOCOMMIT_ON : SQL_AUTOCOMMIT_OFF);
	break;

    case SQL_NOSCAN:
	if (val == Qnil) {
	    return v ? Qtrue : Qfalse;
	}
	v = (TYPE(val) == T_FIXNUM) ?  
	    (FIX2INT(val) ? SQL_NOSCAN_ON : SQL_NOSCAN_OFF) : 
	    (RTEST(val) ? SQL_NOSCAN_ON : SQL_NOSCAN_OFF);
	break;

    case SQL_CONCURRENCY:
    case SQL_QUERY_TIMEOUT:
    case SQL_MAX_ROWS:
    case SQL_MAX_LENGTH:
    case SQL_ROWSET_SIZE:
    case SQL_CURSOR_TYPE:
    default:
	if (val == Qnil) {
	    return rb_int2inum(v);
	}
	Check_Type(val, T_FIXNUM);
	v = FIX2INT(val);
	if (op == SQL_ROWSET_SIZE) {
	    rb_raise(Cerror, "%s", set_err("Read only attribute", 0));
	}
	break;
    }
    if (p != NULL) {
	if (!succeeded(SQL_NULL_HENV, p->hdbc, SQL_NULL_HSTMT,
		       SQLSetConnectOption(p->hdbc, (SQLUSMALLINT) op,
					   (SQLUINTEGER) v),
		       &msg, "SQLSetConnectOption(%d)", op)) {
	    rb_raise(Cerror, "%s", msg);
	}
    } else {
	if (!succeeded(SQL_NULL_HENV, SQL_NULL_HDBC, q->hstmt, 
		       SQLSetStmtOption(q->hstmt, (SQLUSMALLINT) op,
					(SQLUINTEGER) v),
		       &msg, "SQLSetStmtOption(%d)", op)) {
	    rb_raise(Cerror, "%s", msg);
	}
    }
    return Qnil;
}

static VALUE
dbc_autocommit(int argc, VALUE *argv, VALUE self)
{
    return do_option(argc, argv, self, 0, SQL_AUTOCOMMIT);
}

static VALUE
dbc_concurrency(int argc, VALUE *argv, VALUE self)
{
    return do_option(argc, argv, self, 0, SQL_CONCURRENCY);
}

static VALUE
dbc_maxrows(int argc, VALUE *argv, VALUE self)
{
    return do_option(argc, argv, self, 0, SQL_MAX_ROWS);
}

static VALUE
dbc_timeout(int argc, VALUE *argv, VALUE self)
{
    return do_option(argc, argv, self, 0, SQL_QUERY_TIMEOUT);
}

static VALUE
dbc_maxlength(int argc, VALUE *argv, VALUE self)
{
    return do_option(argc, argv, self, 0, SQL_MAX_LENGTH);
}

static VALUE
dbc_rowsetsize(int argc, VALUE *argv, VALUE self)
{
    return do_option(argc, argv, self, 0, SQL_ROWSET_SIZE);
}

static VALUE
dbc_cursortype(int argc, VALUE *argv, VALUE self)
{
    return do_option(argc, argv, self, 0, SQL_CURSOR_TYPE);
}

static VALUE
dbc_noscan(int argc, VALUE *argv, VALUE self)
{
    return do_option(argc, argv, self, 0, SQL_NOSCAN);
}

static VALUE
dbc_getsetoption(int argc, VALUE *argv, VALUE self)
{
    return do_option(argc, argv, self, 0, -1);
}

static VALUE
stmt_concurrency(int argc, VALUE *argv, VALUE self)
{
    return do_option(argc, argv, self, 1, SQL_CONCURRENCY);
}

static VALUE
stmt_maxrows(int argc, VALUE *argv, VALUE self)
{
    return do_option(argc, argv, self, 1, SQL_MAX_ROWS);
}

static VALUE
stmt_timeout(int argc, VALUE *argv, VALUE self)
{
    return do_option(argc, argv, self, 1, SQL_QUERY_TIMEOUT);
}

static VALUE
stmt_maxlength(int argc, VALUE *argv, VALUE self)
{
    return do_option(argc, argv, self, 1, SQL_MAX_LENGTH);
}

static VALUE
stmt_rowsetsize(int argc, VALUE *argv, VALUE self)
{
    return do_option(argc, argv, self, 1, SQL_ROWSET_SIZE);
}

static VALUE
stmt_cursortype(int argc, VALUE *argv, VALUE self)
{
    return do_option(argc, argv, self, 1, SQL_CURSOR_TYPE);
}

static VALUE
stmt_noscan(int argc, VALUE *argv, VALUE self)
{
    return do_option(argc, argv, self, 1, SQL_NOSCAN);
}

static VALUE
stmt_getsetoption(int argc, VALUE *argv, VALUE self)
{
    return do_option(argc, argv, self, 1, -1);
}

/*
 *----------------------------------------------------------------------
 *
 *      Scan literal date/time/timestamp to TIMESTAMP_STRUCT.
 *
 *----------------------------------------------------------------------
 */

static int
scan_dtts(VALUE str, int do_d, int do_t, TIMESTAMP_STRUCT *ts)
{
    int yy = 0, mm = 0, dd = 0, hh = 0, mmm = 0, ss = 0, ff = 0, i;
    char c, *cstr = STR2CSTR(str);

    memset(ts, 0, sizeof (TIMESTAMP_STRUCT));
    if (((sscanf(cstr, "{ts '%d-%d-%d %d:%d:%d.%d' %c",
		 &yy, &mm, &dd, &hh, &mmm, &ss, &ff, &c) == 8) ||
	 (sscanf(cstr, "{ts '%d-%d-%d %d:%d:%d' %c",
		 &yy, &mm, &dd, &hh, &mmm, &ss, &c) == 7)) &&
	(c == '}')) {
	ts->year = yy;
	ts->month = mm;
	ts->day = dd;
	ts->hour = hh;
	ts->minute = mmm;
	ts->second = ss;
	ts->fraction = ff;
	return 1;
    }
    if (do_d &&
	(sscanf(cstr, "{d '%d-%d-%d' %c", &yy, &mm, &dd, &c) == 4) &&
	(c == '}')) {
	ts->year = yy;
	ts->month = mm;
	ts->day = dd;
	return 1;
    }
    if (do_t && 
	(sscanf(cstr, "{t '%d:%d:%d' %c", &hh, &mmm, &ss, &c) == 4) &&
	(c == '}')) {
	ts->hour = yy;
	ts->minute = mmm;
	ts->second = ss;
	return 1;
    }
    ff = ss = 0;
    i = sscanf(cstr, "%d-%d-%d %d:%d:%d%c%d",
	       &yy, &mm, &dd, &hh, &mmm, &ss, &c, &ff);
    if (i >= 5) {
	if ((i > 6) && (c != 0) && (strchr(". \t", c) == NULL)) {
	    goto next;
	}
	ts->year = yy;
	ts->month = mm;
	ts->day = dd;
	ts->hour = hh;
	ts->minute = mmm;
	ts->second = ss;
	ts->fraction = ff;
	return 1;
    }
next:
    ff = ss = 0;
    if (do_d && (sscanf(cstr, "%d-%d-%d", &yy, &mm, &dd) == 3)) {
	ts->year = yy;
	ts->month = mm;
	ts->day = dd;
	return 1;
    }
    if (do_t && (sscanf(cstr, "%d:%d:%d", &hh, &mmm, &ss) == 3)) {
	ts->hour = hh;
	ts->minute = mmm;
	ts->second = ss;
	return 1;
    }
    return 0;
}

/*
 *----------------------------------------------------------------------
 *
 *      Date methods.
 *
 *----------------------------------------------------------------------
 */

#ifdef HAVE_RB_DEFINE_ALLOC_FUNC
static VALUE
date_alloc(VALUE self)
{
    DATE_STRUCT *date;
    VALUE obj = Data_Make_Struct(self, DATE_STRUCT, 0, xfree, date);

    memset(date, 0, sizeof (*date));
    return obj;
}
#else
static VALUE
date_new(int argc, VALUE *argv, VALUE self)
{
    DATE_STRUCT *date;
    VALUE obj = Data_Make_Struct(self, DATE_STRUCT, 0, xfree, date);
    
    rb_obj_call_init(obj, argc, argv);
    return obj;
}
#endif

static VALUE
date_load1(VALUE self, VALUE str, int load)
{
    TIMESTAMP_STRUCT tss;

    if (scan_dtts(str, 1, 0, &tss)) {
	DATE_STRUCT *date;
	VALUE obj;

	if (load) {
	    obj = Data_Make_Struct(self, DATE_STRUCT, 0, xfree, date);
	} else {
	    obj = self;
	    Data_Get_Struct(self, DATE_STRUCT, date);
	}
	date->year = tss.year;
	date->month = tss.month;
	date->day = tss.day;
	return obj;
    }
    if (load > 0) {
	rb_raise(rb_eTypeError, "marshaled ODBC::Date format error");
    }
    return Qnil;
}

static VALUE
date_load(VALUE self, VALUE str)
{
    return date_load1(self, str, 1);
}

static VALUE
date_init(int argc, VALUE *argv, VALUE self)
{
    DATE_STRUCT *date;
    VALUE d, m, y;

    rb_scan_args(argc, argv, "03", &y, &m, &d);
    if (rb_obj_is_kind_of(y, Cdate) == Qtrue) {
	DATE_STRUCT *date2;

	if (argc > 1) {
	    rb_raise(rb_eArgError, "wrong # arguments");
	}
	Data_Get_Struct(self, DATE_STRUCT, date);
	Data_Get_Struct(y, DATE_STRUCT, date2);
	*date = *date2;
	return self;
    }
    if (rb_obj_is_kind_of(y, Ctimestamp) == Qtrue) {
	TIMESTAMP_STRUCT *ts;

	if (argc > 1) {
	    rb_raise(rb_eArgError, "wrong # arguments");
	}
	Data_Get_Struct(self, DATE_STRUCT, date);
	Data_Get_Struct(y, TIMESTAMP_STRUCT, ts);
	date->year  = ts->year;
	date->month = ts->month;
	date->day   = ts->day;
	return self;
    }
    if (rb_obj_is_kind_of(y, rb_cTime) == Qtrue) {
	if (argc > 1) {
	    rb_raise(rb_eArgError, "wrong # arguments");
	}
	d = rb_funcall(y, IDday, 0, NULL);
	m = rb_funcall(y, IDmonth, 0, NULL);
	y = rb_funcall(y, IDyear, 0, NULL);
    } else if (rb_obj_is_kind_of(y, rb_cDate) == Qtrue) {
	if (argc > 1) {
	    rb_raise(rb_eArgError, "wrong # arguments");
	}
	d = rb_funcall(y, IDmday, 0, NULL);
	m = rb_funcall(y, IDmonth, 0, NULL);
	y = rb_funcall(y, IDyear, 0, NULL);
    } else if ((argc == 1) && (rb_obj_is_kind_of(y, rb_cString) == Qtrue)) {
	if (date_load1(self, y, 0) != Qnil) {
	    return self;
	}
    }
    Data_Get_Struct(self, DATE_STRUCT, date);
    date->year  = (y == Qnil) ? 0 : NUM2INT(y);
    date->month = (m == Qnil) ? 0 : NUM2INT(m);
    date->day   = (d == Qnil) ? 0 : NUM2INT(d);
    return self;
}

static VALUE
date_clone(VALUE self)
{
#ifdef HAVE_RB_DEFINE_ALLOC_FUNC
    VALUE obj = rb_obj_alloc(CLASS_OF(self));
    DATE_STRUCT *date1, *date2;

    Data_Get_Struct(self, DATE_STRUCT, date1);
    Data_Get_Struct(obj, DATE_STRUCT, date2);
    *date2 = *date1;
    return obj;
#else
    return date_new(1, &self, CLASS_OF(self));
#endif
}

static VALUE
date_to_s(VALUE self)
{
    DATE_STRUCT *date;
    char buf[128];

    Data_Get_Struct(self, DATE_STRUCT, date);
    sprintf(buf, "%04d-%02d-%02d", date->year, date->month, date->day);
    return rb_str_new2(buf);
}

static VALUE
date_dump(VALUE self, VALUE depth)
{
    return date_to_s(self);
}

static VALUE
date_inspect(VALUE self)
{
    VALUE s = rb_str_new2("#<ODBC::Date: ");

    s = rb_str_append(s, date_to_s(self));
    return rb_str_append(s, rb_str_new2(">"));
}

static VALUE
date_year(int argc, VALUE *argv, VALUE self)
{
    DATE_STRUCT *date;
    VALUE v;

    rb_scan_args(argc, argv, "01", &v);
    Data_Get_Struct(self, DATE_STRUCT, date);
    if (v == Qnil) {
	return INT2NUM(date->year);
    }
    date->year = NUM2INT(v);
    return self;
}

static VALUE
date_month(int argc, VALUE *argv, VALUE self)
{
    DATE_STRUCT *date;
    VALUE v;

    rb_scan_args(argc, argv, "01", &v);
    Data_Get_Struct(self, DATE_STRUCT, date);
    if (v == Qnil) {
	return INT2NUM(date->month);
    }
    date->month = NUM2INT(v);
    return self;
}

static VALUE
date_day(int argc, VALUE *argv, VALUE self)
{
    DATE_STRUCT *date;
    VALUE v;

    rb_scan_args(argc, argv, "01", &v);
    Data_Get_Struct(self, DATE_STRUCT, date);
    if (v == Qnil) {
	return INT2NUM(date->day);
    }
    date->day = NUM2INT(v);
    return self;
}

static VALUE
date_cmp(VALUE self, VALUE date)
{
    DATE_STRUCT *date1, *date2;

    if (rb_obj_is_kind_of(date, Cdate) != Qtrue) {
	rb_raise(rb_eTypeError, "need ODBC::Date as argument");
    }
    Data_Get_Struct(self, DATE_STRUCT, date1);
    Data_Get_Struct(date, DATE_STRUCT, date2);
    if (date1->year < date2->year) {
	return INT2FIX(-1);
    }
    if (date1->year == date2->year) {
	if (date1->month < date2->month) {
	    return INT2FIX(-1);
	}
	if (date1->month == date2->month) {
	    if (date1->day < date2->day) {
		return INT2FIX(-1);
	    }
	    if (date1->day == date2->day) {
		return INT2FIX(0);
	    }
	}
    }
    return INT2FIX(1);
}

/*
 *----------------------------------------------------------------------
 *
 *      Time methods.
 *
 *----------------------------------------------------------------------
 */

#ifdef HAVE_RB_DEFINE_ALLOC_FUNC
static VALUE
time_alloc(VALUE self)
{
    TIME_STRUCT *time;
    VALUE obj = Data_Make_Struct(self, TIME_STRUCT, 0, xfree, time);

    memset(time, 0, sizeof (*time));
    return obj;
}
#else
static VALUE
time_new(int argc, VALUE *argv, VALUE self)
{
    TIME_STRUCT *time;
    VALUE obj = Data_Make_Struct(self, TIME_STRUCT, 0, xfree, time);
    
    rb_obj_call_init(obj, argc, argv);
    return obj;
}
#endif

static VALUE
time_load1(VALUE self, VALUE str, int load)
{
    TIMESTAMP_STRUCT tss;

    if (scan_dtts(str, 0, 1, &tss)) {
	TIME_STRUCT *time;
	VALUE obj;
       
	if (load) {
	    obj = Data_Make_Struct(self, TIME_STRUCT, 0, xfree, time);
	} else {
	    obj = self;
	    Data_Get_Struct(self, TIME_STRUCT, time);
	}
	time->hour = tss.hour;
	time->minute = tss.minute;
	time->second = tss.second;
	return obj;
    }
    if (load > 0) {
	rb_raise(rb_eTypeError, "marshaled ODBC::Time format error");
    }
    return Qnil;
}

static VALUE
time_load(VALUE self, VALUE str)
{
    return time_load1(self, str, 1);
}

static VALUE
time_init(int argc, VALUE *argv, VALUE self)
{
    TIME_STRUCT *time;
    VALUE h, m, s;

    rb_scan_args(argc, argv, "03", &h, &m, &s);
    if (rb_obj_is_kind_of(h, Ctime) == Qtrue) {
	TIME_STRUCT *time2;

	if (argc > 1) {
	    rb_raise(rb_eArgError, "wrong # arguments");
	}
	Data_Get_Struct(self, TIME_STRUCT, time);
	Data_Get_Struct(h, TIME_STRUCT, time2);
	*time = *time2;
	return self;
    }
    if (rb_obj_is_kind_of(h, Ctimestamp) == Qtrue) {
	TIMESTAMP_STRUCT *ts;

	if (argc > 1) {
	    rb_raise(rb_eArgError, "wrong # arguments");
	}
	Data_Get_Struct(self, TIME_STRUCT, time);
	Data_Get_Struct(h, TIMESTAMP_STRUCT, ts);
	time->hour   = ts->hour;
	time->minute = ts->minute;
	time->second = ts->second;
	return self;
    }
    if (rb_obj_is_kind_of(h, rb_cTime) == Qtrue) {
	if (argc > 1) {
	    rb_raise(rb_eArgError, "wrong # arguments");
	}
	s = rb_funcall(h, IDsec, 0, NULL);
	m = rb_funcall(h, IDmin, 0, NULL);
	h = rb_funcall(h, IDhour, 0, NULL);
    } else if ((argc == 1) && (rb_obj_is_kind_of(h, rb_cString) == Qtrue)) {
	if (time_load1(self, h, 0) != Qnil) {
	    return self;
	}
    }
    Data_Get_Struct(self, TIME_STRUCT, time);
    time->hour   = (h == Qnil) ? 0 : NUM2INT(h);
    time->minute = (m == Qnil) ? 0 : NUM2INT(m);
    time->second = (s == Qnil) ? 0 : NUM2INT(s);
    return self;
}

static VALUE
time_clone(VALUE self)
{
#ifdef HAVE_RB_DEFINE_ALLOC_FUNC
    VALUE obj = rb_obj_alloc(CLASS_OF(self));
    TIME_STRUCT *time1, *time2;

    Data_Get_Struct(self, TIME_STRUCT, time1);
    Data_Get_Struct(obj, TIME_STRUCT, time2);
    *time2 = *time1;
    return obj;
#else
    return time_new(1, &self, CLASS_OF(self));
#endif
}

static VALUE
time_to_s(VALUE self)
{
    TIME_STRUCT *time;
    char buf[128];

    Data_Get_Struct(self, TIME_STRUCT, time);
    sprintf(buf, "%02d:%02d:%02d", time->hour, time->minute, time->second);
    return rb_str_new2(buf);
}

static VALUE
time_dump(VALUE self, VALUE depth)
{
    return time_to_s(self);
}

static VALUE
time_inspect(VALUE self)
{
    VALUE s = rb_str_new2("#<ODBC::Time: ");

    s = rb_str_append(s, time_to_s(self));
    return rb_str_append(s, rb_str_new2(">"));
}

static VALUE
time_hour(int argc, VALUE *argv, VALUE self)
{
    TIME_STRUCT *time;
    VALUE v;

    rb_scan_args(argc, argv, "01", &v);
    Data_Get_Struct(self, TIME_STRUCT, time);
    if (v == Qnil) {
	return INT2NUM(time->hour);
    }
    time->hour = NUM2INT(v);
    return self;
}

static VALUE
time_min(int argc, VALUE *argv, VALUE self)
{
    TIME_STRUCT *time;
    VALUE v;

    rb_scan_args(argc, argv, "01", &v);
    Data_Get_Struct(self, TIME_STRUCT, time);
    if (v == Qnil) {
	return INT2NUM(time->minute);
    }
    time->minute = NUM2INT(v);
    return self;
}

static VALUE
time_sec(int argc, VALUE *argv, VALUE self)
{
    TIME_STRUCT *time;
    VALUE v;

    rb_scan_args(argc, argv, "01", &v);
    Data_Get_Struct(self, TIME_STRUCT, time);
    if (v == Qnil) {
	return INT2NUM(time->second);
    }
    time->second = NUM2INT(v);
    return self;
}

static VALUE
time_cmp(VALUE self, VALUE time)
{
    TIME_STRUCT *time1, *time2;

    if (rb_obj_is_kind_of(time, Ctime) != Qtrue) {
	rb_raise(rb_eTypeError, "need ODBC::Time as argument");
    }
    Data_Get_Struct(self, TIME_STRUCT, time1);
    Data_Get_Struct(time, TIME_STRUCT, time2);
    if (time1->hour < time2->hour) {
	return INT2FIX(-1);
    }
    if (time1->hour == time2->hour) {
	if (time1->minute < time2->minute) {
	    return INT2FIX(-1);
	}
	if (time1->minute == time2->minute) {
	    if (time1->second < time2->second) {
		return INT2FIX(-1);
	    }
	    if (time1->second == time2->second) {
		return INT2FIX(0);
	    }
	}
    }
    return INT2FIX(1);
}

/*
 *----------------------------------------------------------------------
 *
 *      TimeStamp methods.
 *
 *----------------------------------------------------------------------
 */

#ifdef HAVE_RB_DEFINE_ALLOC_FUNC
static VALUE
timestamp_alloc(VALUE self)
{
    TIMESTAMP_STRUCT *ts;
    VALUE obj = Data_Make_Struct(self, TIMESTAMP_STRUCT, 0, xfree, ts);

    memset(ts, 0, sizeof (*ts));
    return obj;
}
#else
static VALUE
timestamp_new(int argc, VALUE *argv, VALUE self)
{
    TIMESTAMP_STRUCT *ts;
    VALUE obj = Data_Make_Struct(self, TIMESTAMP_STRUCT, 0, xfree, ts);

    rb_obj_call_init(obj, argc, argv);
    return obj;
}
#endif

static VALUE
timestamp_load1(VALUE self, VALUE str, int load)
{
    TIMESTAMP_STRUCT tss;

    if (scan_dtts(str, !load, !load, &tss)) {
	TIMESTAMP_STRUCT *ts;
	VALUE obj;

	if (load) {
	    obj = Data_Make_Struct(self, TIMESTAMP_STRUCT, 0, xfree, ts);
	} else {
	    obj = self;
	    Data_Get_Struct(self, TIMESTAMP_STRUCT, ts);
	}
	*ts = tss;
	return obj;
    }
    if (load > 0) {
	rb_raise(rb_eTypeError, "marshaled ODBC::TimeStamp format error");
    }
    return Qnil;
}

static VALUE
timestamp_load(VALUE self, VALUE str)
{
    return timestamp_load1(self, str, 1);
}

static VALUE
timestamp_init(int argc, VALUE *argv, VALUE self)
{
    TIMESTAMP_STRUCT *ts;
    VALUE d, m, y, hh, mm, ss, f;

    rb_scan_args(argc, argv, "07", &y, &m, &d, &hh, &mm, &ss, &f);
    if (rb_obj_is_kind_of(y, Ctimestamp) == Qtrue) {
	TIMESTAMP_STRUCT *ts2;

	if (argc > 1) {
	    rb_raise(rb_eArgError, "wrong # arguments");
	}
	Data_Get_Struct(self, TIMESTAMP_STRUCT, ts);
	Data_Get_Struct(y, TIMESTAMP_STRUCT, ts2);
	*ts = *ts2;
	return self;
    }
    if (rb_obj_is_kind_of(y, Cdate) == Qtrue) {
	DATE_STRUCT *date;

	if (argc > 1) {
	    if (argc > 2) {
		rb_raise(rb_eArgError, "wrong # arguments");
	    }
	    if (rb_obj_is_kind_of(m, Ctime) == Qtrue) {
		TIME_STRUCT *time;

		Data_Get_Struct(self, TIMESTAMP_STRUCT, ts);
		Data_Get_Struct(m, TIME_STRUCT, time);
		ts->hour   = time->hour;
		ts->minute = time->minute;
		ts->second = time->second;
	    } else {
		rb_raise(rb_eArgError, "need ODBC::Time argument");
	    }
	}
	Data_Get_Struct(self, TIMESTAMP_STRUCT, ts);
	Data_Get_Struct(y, DATE_STRUCT, date);
	ts->year = date->year;
	ts->year = date->year;
	ts->year = date->year;
	ts->fraction = 0;
	return self;
    }
    if (rb_obj_is_kind_of(y, rb_cTime) == Qtrue) {
	if (argc > 1) {
	    rb_raise(rb_eArgError, "wrong # arguments");
	}
	f  = rb_funcall(y, IDusec, 0, NULL);
	ss = rb_funcall(y, IDsec, 0, NULL);
	mm = rb_funcall(y, IDmin, 0, NULL);
	hh = rb_funcall(y, IDhour, 0, NULL);
	d  = rb_funcall(y, IDday, 0, NULL);
	m  = rb_funcall(y, IDmonth, 0, NULL);
	y  = rb_funcall(y, IDyear, 0, NULL);
	f = INT2NUM(NUM2INT(f) * 1000);
    } else if (rb_obj_is_kind_of(y, rb_cDate) == Qtrue) {
	if (argc > 1) {
	    rb_raise(rb_eArgError, "wrong # arguments");
	}
	f  = INT2FIX(0);
	ss = INT2FIX(0);
	mm = INT2FIX(0);
	hh = INT2FIX(0);
	d  = rb_funcall(y, IDmday, 0, NULL);
	m  = rb_funcall(y, IDmonth, 0, NULL);
	y  = rb_funcall(y, IDyear, 0, NULL);
    } else if ((argc == 1) && (rb_obj_is_kind_of(y, rb_cString) == Qtrue)) {
	if (timestamp_load1(self, y, 0) != Qnil) {
	    return self;
	}
    }
    Data_Get_Struct(self, TIMESTAMP_STRUCT, ts);
    ts->year     = (y  == Qnil) ? 0 : NUM2INT(y);
    ts->month    = (m  == Qnil) ? 0 : NUM2INT(m);
    ts->day      = (d  == Qnil) ? 0 : NUM2INT(d);
    ts->hour     = (hh == Qnil) ? 0 : NUM2INT(hh);
    ts->minute   = (mm == Qnil) ? 0 : NUM2INT(mm);
    ts->second   = (ss == Qnil) ? 0 : NUM2INT(ss);
    ts->fraction = (f  == Qnil) ? 0 : NUM2INT(f);
    return self;
}

static VALUE
timestamp_clone(VALUE self)
{
#ifdef HAVE_RB_DEFINE_ALLOC_FUNC
    VALUE obj = rb_obj_alloc(CLASS_OF(self));
    TIMESTAMP_STRUCT *ts1, *ts2;

    Data_Get_Struct(self, TIMESTAMP_STRUCT, ts1);
    Data_Get_Struct(obj, TIMESTAMP_STRUCT, ts2);
    *ts2 = *ts1;
    return obj;
#else
    return timestamp_new(1, &self, CLASS_OF(self));
#endif
}

static VALUE
timestamp_to_s(VALUE self)
{
    TIMESTAMP_STRUCT *ts;
    char buf[256];

    Data_Get_Struct(self, TIMESTAMP_STRUCT, ts);
    sprintf(buf, "%04d-%02d-%02d %02d:%02d:%02d %u",
	    ts->year, ts->month, ts->day,
	    ts->hour, ts->minute, ts->second,
	    (unsigned int) ts->fraction);
    return rb_str_new2(buf);
}

static VALUE
timestamp_dump(VALUE self, VALUE depth)
{
    return timestamp_to_s(self);
}

static VALUE
timestamp_inspect(VALUE self)
{
    VALUE s = rb_str_new2("#<ODBC::TimeStamp: \"");

    s = rb_str_append(s, timestamp_to_s(self));
    return rb_str_append(s, rb_str_new2("\">"));
}

static VALUE
timestamp_year(int argc, VALUE *argv, VALUE self)
{
    TIMESTAMP_STRUCT *ts;
    VALUE v;

    rb_scan_args(argc, argv, "01", &v);
    Data_Get_Struct(self, TIMESTAMP_STRUCT, ts);
    if (v == Qnil) {
	return INT2NUM(ts->year);
    }
    ts->year = NUM2INT(v);
    return self;
}

static VALUE
timestamp_month(int argc, VALUE *argv, VALUE self)
{
    TIMESTAMP_STRUCT *ts;
    VALUE v;

    rb_scan_args(argc, argv, "01", &v);
    Data_Get_Struct(self, TIMESTAMP_STRUCT, ts);
    if (v == Qnil) {
	return INT2NUM(ts->month);
    }
    ts->month = NUM2INT(v);
    return self;
}

static VALUE
timestamp_day(int argc, VALUE *argv, VALUE self)
{
    TIMESTAMP_STRUCT *ts;
    VALUE v;

    rb_scan_args(argc, argv, "01", &v);
    Data_Get_Struct(self, TIMESTAMP_STRUCT, ts);
    if (v == Qnil) {
	return INT2NUM(ts->day);
    }
    ts->day = NUM2INT(v);
    return self;
}

static VALUE
timestamp_hour(int argc, VALUE *argv, VALUE self)
{
    TIMESTAMP_STRUCT *ts;
    VALUE v;

    rb_scan_args(argc, argv, "01", &v);
    Data_Get_Struct(self, TIMESTAMP_STRUCT, ts);
    if (v == Qnil) {
	return INT2NUM(ts->hour);
    }
    ts->hour = NUM2INT(v);
    return self;
}

static VALUE
timestamp_min(int argc, VALUE *argv, VALUE self)
{
    TIMESTAMP_STRUCT *ts;
    VALUE v;

    rb_scan_args(argc, argv, "01", &v);
    Data_Get_Struct(self, TIMESTAMP_STRUCT, ts);
    if (v == Qnil) {
	return INT2NUM(ts->minute);
    }
    ts->minute = NUM2INT(v);
    return self;
}

static VALUE
timestamp_sec(int argc, VALUE *argv, VALUE self)
{
    TIMESTAMP_STRUCT *ts;
    VALUE v;

    rb_scan_args(argc, argv, "01", &v);
    Data_Get_Struct(self, TIMESTAMP_STRUCT, ts);
    if (v == Qnil) {
	return INT2NUM(ts->second);
    }
    ts->second = NUM2INT(v);
    return self;
}

static VALUE
timestamp_fraction(int argc, VALUE *argv, VALUE self)
{
    TIMESTAMP_STRUCT *ts;
    VALUE v;

    rb_scan_args(argc, argv, "01", &v);
    Data_Get_Struct(self, TIMESTAMP_STRUCT, ts);
    if (v == Qnil) {
	return INT2NUM(ts->fraction);
    }
    ts->fraction = NUM2INT(v);
    return self;
}

static VALUE
timestamp_cmp(VALUE self, VALUE timestamp)
{
    TIMESTAMP_STRUCT *ts1, *ts2;

    if (rb_obj_is_kind_of(timestamp, Ctimestamp) != Qtrue) {
	rb_raise(rb_eTypeError, "need ODBC::TimeStamp as argument");
    }
    Data_Get_Struct(self, TIMESTAMP_STRUCT, ts1);
    Data_Get_Struct(timestamp, TIMESTAMP_STRUCT, ts2);
    if (ts1->year < ts2->year) {
	return INT2FIX(-1);
    }
    if (ts1->year == ts2->year) {
	if (ts1->month < ts2->month) {
	    return INT2FIX(-1);
	}
	if (ts1->month == ts2->month) {
	    if (ts1->day < ts2->day) {
		return INT2FIX(-1);
	    }
	    if (ts1->day == ts2->day) {
		if (ts1->hour < ts2->hour) {
		    return INT2FIX(-1);
		}
		if (ts1->hour == ts2->hour) {
		    if (ts1->minute < ts2->minute) {
			return INT2FIX(-1);
		    }
		    if (ts1->minute == ts2->minute) {
			if (ts1->second < ts2->second) {
			    return INT2FIX(-1);
			}
			if (ts1->second == ts2->second) {
			    if (ts1->fraction < ts2->fraction) {
				return INT2FIX(-1);
			    }
			    if (ts1->fraction == ts2->fraction) {
				return INT2FIX(0);
			    }
			}
		    }
		}
	    }
	}
    }
    return INT2FIX(1);
}

/*
 *----------------------------------------------------------------------
 *
 *      Statement methods.
 *
 *----------------------------------------------------------------------
 */

static VALUE
stmt_drop(VALUE self)
{
    STMT *q;

    Data_Get_Struct(self, STMT, q);
    if (q->hstmt != SQL_NULL_HSTMT) {
	callsql(SQL_NULL_HENV, SQL_NULL_HDBC, q->hstmt,
		SQLFreeStmt(q->hstmt, SQL_DROP), "SQLFreeStmt(SQL_DROP)");
	q->hstmt = SQL_NULL_HSTMT;
	unlink_stmt(q);
    }
    free_stmt_sub(q, 1);
    return self;
}

static VALUE
stmt_close(VALUE self)
{
    STMT *q;

    Data_Get_Struct(self, STMT, q);
    if (q->hstmt != SQL_NULL_HSTMT) {
	callsql(SQL_NULL_HENV, SQL_NULL_HDBC, q->hstmt,
		SQLFreeStmt(q->hstmt, SQL_CLOSE), "SQLFreeStmt(SQL_CLOSE)");
    }
    free_stmt_sub(q, 1);
    return self;
}

static VALUE
stmt_cancel(VALUE self)
{
    STMT *q;
    char *msg;

    Data_Get_Struct(self, STMT, q);
    if (q->hstmt != SQL_NULL_HSTMT) {
	if (!succeeded(SQL_NULL_HENV, SQL_NULL_HDBC, q->hstmt,
		       SQLCancel(q->hstmt), &msg, "SQLCancel")) {
	    rb_raise(Cerror, "%s", msg);
	}
    }
    return self;
}

static void
check_ncols(STMT *q)
{
    if ((q->hstmt != SQL_NULL_HSTMT) && (q->ncols <= 0) &&
	(q->coltypes == NULL)) {
	COLTYPE *coltypes = NULL;
	SQLSMALLINT cols = 0;
	
	if (succeeded(SQL_NULL_HENV, SQL_NULL_HDBC, q->hstmt,
		      SQLNumResultCols(q->hstmt, &cols), NULL,
		      "SQLNumResultCols")
	    && (cols > 0)) {
	    coltypes = make_coltypes(q->hstmt, cols, NULL);
	    if (coltypes != NULL) {
		q->ncols = cols;
		q->coltypes = coltypes;
	    }
	}
    }
}

static VALUE
stmt_ncols(VALUE self)
{
    STMT *q;

    Data_Get_Struct(self, STMT, q);
    check_ncols(q);
    return INT2FIX(q->ncols);
}

static VALUE
stmt_nrows(VALUE self)
{
    STMT *q;
    SQLLEN rows = -1;
    char *msg;

    Data_Get_Struct(self, STMT, q);
    if ((q->hstmt != SQL_NULL_HSTMT) &&
	(!succeeded(SQL_NULL_HENV, SQL_NULL_HDBC, q->hstmt,
		    SQLRowCount(q->hstmt, &rows), &msg, "SQLRowCount"))) {
	rb_raise(Cerror, "%s", msg);
    }
    return INT2NUM(rows);
}

static VALUE
stmt_nparams(VALUE self)
{
    STMT *q;

    Data_Get_Struct(self, STMT, q);
    return INT2FIX(q->nump);
}

static int
param_num_check(STMT *q, VALUE pnum, int mkparaminfo, int needout)
{
    int vnum;

    Check_Type(pnum, T_FIXNUM);
    vnum = NUM2INT(pnum);
    if (mkparaminfo && (q->paraminfo == NULL)) {
	char *msg = NULL;
	SQLSMALLINT nump = 0;

	if (!succeeded(SQL_NULL_HENV, SQL_NULL_HDBC, q->hstmt,
		       SQLNumParams(q->hstmt, &nump), NULL, "SQLNumParams")) {
	    nump = 0;
	}
	if (nump > 0) {
	    PARAMINFO *paraminfo = make_paraminfo(q->hstmt, nump, &msg);

	    if (paraminfo == NULL) {
		rb_raise(Cerror, "%s", msg);
	    }
	    q->paraminfo = paraminfo;
	    if (q->paraminfo != NULL) {
		q->nump = nump;
	    }
	}
    }
    if ((q->paraminfo == NULL) || (vnum < 0) || (vnum >= q->nump)) {
	rb_raise(rb_eArgError, "parameter number out of bounds");
    }
    if (needout) {
	if ((q->paraminfo[vnum].iotype != SQL_PARAM_OUTPUT) &&
	    (q->paraminfo[vnum].iotype != SQL_PARAM_INPUT_OUTPUT)) {
	    rb_raise(Cerror, "not an output parameter");
	}
    }
    return vnum;
}

static VALUE
stmt_param_type(int argc, VALUE *argv, VALUE self)
{
    VALUE pnum, ptype, pcoldef, pscale;
    int vnum;
    STMT *q;

    rb_scan_args(argc, argv, "13", &pnum, &ptype, &pcoldef, &pscale);
    Data_Get_Struct(self, STMT, q);
    vnum = param_num_check(q, pnum, 1, 0);
    if (argc > 1) {
	int vtype, vcoldef, vscale;

	Check_Type(ptype, T_FIXNUM);
	vtype = NUM2INT(ptype);
	if (argc > 2) {
	    Check_Type(pcoldef, T_FIXNUM);
	    vcoldef = NUM2INT(pcoldef);
	    if (argc > 3) {
		Check_Type(pscale, T_FIXNUM);
		vscale = NUM2INT(pscale);
		q->paraminfo[vnum].scale = vscale;
	    }
	    q->paraminfo[vnum].coldef = vcoldef;
	}
	q->paraminfo[vnum].type = vtype;
	q->paraminfo[vnum].override = 1;
	return Qnil;
    }
    return INT2NUM(q->paraminfo[vnum].type);
}

static VALUE
stmt_param_iotype(int argc, VALUE *argv, VALUE self)
{
    VALUE pnum, piotype;
    int vnum, viotype;
    STMT *q;

    rb_scan_args(argc, argv, "11", &pnum, &piotype);
    Data_Get_Struct(self, STMT, q);
    vnum = param_num_check(q, pnum, 1, 0);
    if (argc > 1) {
	Check_Type(piotype, T_FIXNUM);
	viotype = NUM2INT(piotype);
	switch (viotype) {
	case SQL_PARAM_INPUT:
	case SQL_PARAM_INPUT_OUTPUT:
	case SQL_PARAM_OUTPUT:
	    q->paraminfo[vnum].iotype = viotype;
	    break;
	}
    }
    return INT2NUM(q->paraminfo[vnum].iotype);
}

static VALUE
stmt_param_output_value(int argc, VALUE *argv, VALUE self)
{
    VALUE pnum, v;
    int vnum;
    STMT *q;

    rb_scan_args(argc, argv, "10", &pnum);
    Data_Get_Struct(self, STMT, q);
    vnum = param_num_check(q, pnum, 0, 1);
    v = Qnil;
    if (q->paraminfo[vnum].rlen == SQL_NULL_DATA) {
	return v;
    }
    if (q->paraminfo[vnum].outbuf == NULL) {
	rb_raise(Cerror, "no output value available");
    }
    switch (q->paraminfo[vnum].ctype) {
    case SQL_C_LONG:
	v = INT2NUM(*((SQLINTEGER *) q->paraminfo[vnum].outbuf));
	break;
    case SQL_C_DOUBLE:
	v = rb_float_new(*((double *) q->paraminfo[vnum].outbuf));
	break;
    case SQL_C_DATE:
	{
	    DATE_STRUCT *date;

	    if (q->dbcp != NULL && q->dbcp->rbtime == Qtrue) {
		const char *p;
		char buffer[128];
		VALUE d;

		date = (DATE_STRUCT *) q->paraminfo[vnum].outbuf;
		p = (q->dbcp->gmtime == Qtrue) ? "+00:00" : "";
		sprintf(buffer, "%d-%d-%dT00:00:00%s",
			date->year, date->month, date->day, p);
		d = rb_str_new2(buffer);
		v = rb_funcall(rb_cDate, IDparse, 1, d);
	    } else {
		v = Data_Make_Struct(Cdate, DATE_STRUCT, 0, xfree, date);
		*date = *((DATE_STRUCT *) q->paraminfo[vnum].outbuf);
	    }
	}
	break;
    case SQL_C_TIME:
	{
	    TIME_STRUCT *time;

	    if (q->dbcp != NULL && q->dbcp->rbtime == Qtrue) {
		VALUE now, frac;

		time = (TIME_STRUCT *) q->paraminfo[vnum].outbuf;
		frac = rb_float_new(0.0);
		now = rb_funcall(rb_cTime, IDnow, 0, NULL);
		v = rb_funcall(rb_cTime,
			       (q->dbcp->gmtime == Qtrue) ? IDutc : IDlocal,
			       7,
			       rb_funcall(now, IDyear, 0, NULL),
			       rb_funcall(now, IDmonth, 0, NULL),
			       rb_funcall(now, IDday, 0, NULL),
			       INT2NUM(time->hour),
			       INT2NUM(time->minute),
			       INT2NUM(time->second),
			       frac);
	    } else {
		v = Data_Make_Struct(Ctime, TIME_STRUCT, 0, xfree, time);
		*time = *((TIME_STRUCT *) q->paraminfo[vnum].outbuf);
	    }
	}
	break;
    case SQL_C_TIMESTAMP:
	{
	    TIMESTAMP_STRUCT *ts;

	    if (q->dbcp != NULL && q->dbcp->rbtime == Qtrue) {
		VALUE frac;

		ts = (TIMESTAMP_STRUCT *) q->paraminfo[vnum].outbuf;
		frac = rb_float_new((double) 1.0e-3 * ts->fraction);
		v = rb_funcall(rb_cTime,
			       (q->dbcp->gmtime == Qtrue) ? IDutc : IDlocal,
			       7,
			       INT2NUM(ts->year),
			       INT2NUM(ts->month),
			       INT2NUM(ts->day),
			       INT2NUM(ts->hour),
			       INT2NUM(ts->minute),
			       INT2NUM(ts->second),
			       frac);
	    } else {
		v = Data_Make_Struct(Ctimestamp, TIMESTAMP_STRUCT,
				     0, xfree, ts);
		*ts = *((TIMESTAMP_STRUCT *) q->paraminfo[vnum].outbuf);
	    }
	}
	break;
#ifdef UNICODE
    case SQL_C_WCHAR:
	v = uc_tainted_str_new((SQLWCHAR *) q->paraminfo[vnum].outbuf,
			       q->paraminfo[vnum].rlen / sizeof (SQLWCHAR));
	break;
#endif
    case SQL_C_CHAR:
	v = rb_tainted_str_new(q->paraminfo[vnum].outbuf,
			       q->paraminfo[vnum].rlen);
	break;
    }
    return v;
}

static VALUE
stmt_param_output_size(int argc, VALUE *argv, VALUE self)
{
    VALUE pnum, psize;
    int vnum, vsize;
    STMT *q;

    rb_scan_args(argc, argv, "11", &pnum, &psize);
    Data_Get_Struct(self, STMT, q);
    vnum = param_num_check(q, pnum, 0, 1);
    if (argc > 1) {
	Check_Type(psize, T_FIXNUM);
	vsize = NUM2INT(psize);
	if ((vsize > 0) && (vsize < (int) (4 * sizeof (double)))) {
	    vsize = 4 * sizeof (double);
	}
	q->paraminfo[vnum].outsize = (vsize > 0) ? vsize : 0;
    }
    return INT2NUM(q->paraminfo[vnum].outsize);
}

static VALUE
stmt_param_output_type(int argc, VALUE *argv, VALUE self)
{
    VALUE pnum, ptype;
    int vnum, vtype;
    STMT *q;

    rb_scan_args(argc, argv, "11", &pnum, &ptype);
    Data_Get_Struct(self, STMT, q);
    vnum = param_num_check(q, pnum, 0, 1);
    if (argc > 1) {
	Check_Type(ptype, T_FIXNUM);
	vtype = NUM2INT(ptype);
	q->paraminfo[vnum].outtype = vtype;
    }
    return INT2NUM(q->paraminfo[vnum].outtype);
}

static VALUE
stmt_cursorname(int argc, VALUE *argv, VALUE self)
{
    VALUE cn = Qnil;
    STMT *q;
#ifdef UNICODE
    SQLWCHAR cname[SQL_MAX_MESSAGE_LENGTH];
    SQLWCHAR *cp;
#else
    SQLCHAR cname[SQL_MAX_MESSAGE_LENGTH];
    SQLCHAR *cp;
#endif
    char *msg;
    SQLSMALLINT cnLen = 0;

    rb_scan_args(argc, argv, "01", &cn);
    Data_Get_Struct(self, STMT, q);
    if (cn == Qnil) {
	if (!succeeded(SQL_NULL_HENV, SQL_NULL_HDBC, q->hstmt,
		       SQLGetCursorName(q->hstmt, (SQLTCHAR *) cname,
					(SQLSMALLINT) sizeof (cname), &cnLen),
		       &msg, "SQLGetCursorName")) {
	    rb_raise(Cerror, "%s", msg);
	}
#ifdef UNICODE
	cnLen = (cnLen == 0) ? (SQLSMALLINT) uc_strlen(cname) :
	    (SQLSMALLINT) (cnLen / sizeof (SQLWCHAR));
	return uc_tainted_str_new(cname, cnLen);
#else
	cnLen = (cnLen == 0) ? (SQLSMALLINT) strlen((char *) cname) : cnLen;
	return rb_tainted_str_new((char *) cname, cnLen);
#endif
    }
    if (TYPE(cn) != T_STRING) {
	cn = rb_any_to_s(cn);
    }
#ifdef UNICODE
#ifdef USE_RB_ENC
    cn = rb_funcall(cn, IDencode, 1, rb_encv);
#endif
    cp = uc_from_utf((unsigned char *) STR2CSTR(cn), -1);
    if (cp == NULL) {
	rb_raise(Cerror, "%s", set_err("Out of memory", 0));
    }
#else
    cp = (SQLCHAR *) STR2CSTR(cn);
#endif
    if (!succeeded(SQL_NULL_HENV, SQL_NULL_HDBC, q->hstmt,
		   SQLSetCursorName(q->hstmt, cp, SQL_NTS),
		   &msg, "SQLSetCursorName")) {
#ifdef UNICODE
	uc_free(cp);
#endif
	rb_raise(Cerror, "%s", msg);
    }
#ifdef UNICODE
    uc_free(cp);
#endif
    return cn;
}

static VALUE
stmt_column(int argc, VALUE *argv, VALUE self)
{
    STMT *q;
    VALUE col;

    rb_scan_args(argc, argv, "1", &col);
    Check_Type(col, T_FIXNUM);
    Data_Get_Struct(self, STMT, q);
    check_ncols(q);
    return make_column(q->hstmt, FIX2INT(col), q->upc);
}

static VALUE
stmt_columns(int argc, VALUE *argv, VALUE self)
{
    STMT *q;
    int i;
    VALUE res, as_ary = Qfalse;

    rb_scan_args(argc, argv, "01", &as_ary);
    Data_Get_Struct(self, STMT, q);
    check_ncols(q);
    if (rb_block_given_p()) {
	for (i = 0; i < q->ncols; i++) {
	    rb_yield(make_column(q->hstmt, i, q->upc));
	}
	return self;
    }
    if (RTEST(as_ary)) {
	res = rb_ary_new2(q->ncols);
    } else {
	res = rb_hash_new();
    }
    for (i = 0; i < q->ncols; i++) {
	VALUE obj;

	obj = make_column(q->hstmt, i, q->upc);
	if (RTEST(as_ary)) {
	    rb_ary_store(res, i, obj);
	} else {
	    VALUE name = rb_iv_get(obj, "@name");

	    if (rb_funcall(res, IDkeyp, 1, name) == Qtrue) {
		char buf[32];

		sprintf(buf, "#%d", i);
		name = rb_str_dup(name);
		name = rb_obj_taint(rb_str_cat2(name, buf));
	    }
	    rb_hash_aset(res, name, obj);
	}
    }
    return res;
}

static VALUE
stmt_param(int argc, VALUE *argv, VALUE self)
{
    STMT *q;
    VALUE par;
    int i;

    rb_scan_args(argc, argv, "1", &par);
    Check_Type(par, T_FIXNUM);
    Data_Get_Struct(self, STMT, q);
    i = FIX2INT(par);
    if ((i < 0) || (i >= q->nump)) {
	rb_raise(Cerror, "%s", set_err("Parameter out of bounds", 0));
    }
    return make_param(q, i);
}

static VALUE
stmt_params(VALUE self)
{
    STMT *q;
    int i;
    VALUE res;

    Data_Get_Struct(self, STMT, q);
    if (rb_block_given_p()) {
	for (i = 0; i < q->nump; i++) {
	    rb_yield(make_param(q, i));
	}
	return self;
    }
    res = rb_ary_new2(q->nump);
    for (i = 0; i < q->nump; i++) {
	VALUE obj;

	obj = make_param(q, i);
	rb_ary_store(res, i, obj);
    }
    return res;
}

static VALUE
do_fetch(STMT *q, int mode)
{
    int i, offc;
    char **bufs, *msg;
    VALUE res;

    if (q->ncols <= 0) {
	rb_raise(Cerror, "%s", set_err("No columns in result set", 0));
    }
    if (++q->fetchc >= 500) {
	q->fetchc = 0;
	start_gc();
    }
    bufs = q->dbufs;
    if (bufs == NULL) {
	int need = sizeof (char *) * q->ncols, needp;
	char *p;

	need = LEN_ALIGN(need);
	needp = need;
	for (i = 0; i < q->ncols; i++) {
	    if (q->coltypes[i].size != SQL_NO_TOTAL) {
		need += LEN_ALIGN(q->coltypes[i].size);
	    }
	}
	p = ALLOC_N(char, need);
	if (p == NULL) {
	    rb_raise(Cerror, "%s", set_err("Out of memory", 0));
	}
	q->dbufs = bufs = (char **) p;
	p += needp;
	for (i = 0; i < q->ncols; i++) {
	    int len = q->coltypes[i].size;

	    if (len == SQL_NO_TOTAL) {
		bufs[i] = NULL;
	    } else {
		bufs[i] = p;
		p += LEN_ALIGN(len);
	    }
	}
    }
    switch (mode & DOFETCH_MODES) {
    case DOFETCH_HASH:
    case DOFETCH_HASH2:
    case DOFETCH_HASHK:
    case DOFETCH_HASHK2:
	if (q->colnames == NULL) {
	    int need = sizeof (char *) * 4 * q->ncols + sizeof (char *);
	    int max_len[2] = { 0, 0 };
	    char **na, *p;
#ifdef UNICODE
	    SQLWCHAR name[SQL_MAX_MESSAGE_LENGTH];
#else
	    char name[SQL_MAX_MESSAGE_LENGTH];
#endif
	    SQLSMALLINT name_len;

	    for (i = 0; i < q->ncols; i++) {
		int need_len;

		name[0] = 0;
		if (!succeeded(SQL_NULL_HENV, SQL_NULL_HDBC, q->hstmt,
			       SQLColAttributes(q->hstmt,
						(SQLUSMALLINT) (i + 1),
						SQL_COLUMN_TABLE_NAME,
						name,
						sizeof (name),
						&name_len, NULL),
			       &msg,
			       "SQLColAttributes(SQL_COLUMN_TABLE_NAME)")) {
		    rb_raise(Cerror, "%s", msg);
		}
		if (name_len >= (SQLSMALLINT) sizeof (name)) {
		    name_len = sizeof (name) - 1;
		}
		if (name_len > 0) {
		    name[name_len / sizeof (name[0])] = 0;
		}
#ifdef UNICODE
		need_len = 6 * (uc_strlen(name) + 1);
#else
		need_len = 2 * (strlen(name) + 1);
#endif
		need += need_len;
		if (max_len[0] < need_len) {
		    max_len[0] = need_len;
		}
		name[0] = 0;
		if (!succeeded(SQL_NULL_HENV, SQL_NULL_HDBC, q->hstmt,
			       SQLColAttributes(q->hstmt,
						(SQLUSMALLINT) (i + 1),
						SQL_COLUMN_LABEL, name,
						sizeof (name),
						&name_len, NULL),
			       &msg, "SQLColAttributes(SQL_COLUMN_LABEL)")) {
		    rb_raise(Cerror, "%s", msg);
		}
		if (name_len >= (SQLSMALLINT) sizeof (name)) {
		    name_len = sizeof (name) - 1;
		}
		if (name_len > 0) {
		    name[name_len / sizeof (name[0])] = 0;
		}
#ifdef UNICODE
		need_len = 6 * 2 * (uc_strlen(name) + 1);
#else
		need_len = 2 * (strlen(name) + 1);
#endif
		need += need_len;
		if (max_len[1] < need_len) {
		    max_len[1] = need_len;
		}
	    }
	    need += max_len[0] + max_len[1] + 32;
	    p = ALLOC_N(char, need);
	    if (p == NULL) {
		rb_raise(Cerror, "%s", set_err("Out of memory", 0));
	    }
	    na = (char **) p;
	    p += sizeof (char *) * 4 * q->ncols + sizeof (char *);
	    for (i = 0; i < q->ncols; i++) {
		char *p0;

		name[0] = 0;
		callsql(SQL_NULL_HENV, SQL_NULL_HDBC, q->hstmt,
			SQLColAttributes(q->hstmt, (SQLUSMALLINT) (i + 1),
					 SQL_COLUMN_TABLE_NAME, name,
					 sizeof (name), &name_len, NULL),
			"SQLColAttributes(SQL_COLUMN_TABLE_NAME)");
		if (name_len >= (SQLSMALLINT) sizeof (name)) {
		    name_len = sizeof (name) - 1;
		}
		if (name_len > 0) {
		    name[name_len / sizeof (name[0])] = 0;
		}
		na[i + q->ncols] = p;
#ifdef UNICODE
		p += mkutf(p, name, uc_strlen(name));
#else
		strcpy(p, name);
#endif
		strcat(p, ".");
		p += strlen(p);
		p0 = p;
		name[0] = 0;
		callsql(SQL_NULL_HENV, SQL_NULL_HDBC, q->hstmt,
			SQLColAttributes(q->hstmt, (SQLUSMALLINT) (i + 1),
					 SQL_COLUMN_LABEL, name,
					 sizeof (name), &name_len, NULL),
			"SQLColAttributes(SQL_COLUMN_LABEL)");
		if (name_len >= (SQLSMALLINT) sizeof (name)) {
		    name_len = sizeof (name) - 1;
		}
		if (name_len > 0) {
		    name[name_len / sizeof (name[0])] = 0;
		}
		na[i] = p;
#ifdef UNICODE
		p += mkutf(p, name, uc_strlen(name)) + 1;
#else
		strcpy(p, name);
		p += strlen(p) + 1;
#endif
		na[i + 3 * q->ncols] = p;
		strcpy(p, na[i + q->ncols]);
		p += p0 - na[i + q->ncols];
		na[i + 2 * q->ncols] = upcase_if(p, 1);
		p += strlen(p) + 1;
	    }
	    /* reserved space for later adjustments */
	    na[4 * q->ncols] = p;
	    q->colnames = na;
	    if (q->colvals == NULL) {
		q->colvals = ALLOC_N(VALUE, 4 * q->ncols);
		if (q->colvals != NULL) {
		    VALUE cname;
		    VALUE colbuf[4];

		    for (i = 0; i < 4 * q->ncols; i++) {
			q->colvals[i] = Qnil;
		    }
		    for (i = 0; i < 4; i++) {
			colbuf[i] = rb_iv_get(q->self, colnamebuf[i]);
			if (colbuf[i] == Qnil) {
			    res = rb_hash_new();
			    rb_iv_set(q->self, colnamebuf[i], res);
			}
		    }
		    for (i = 0; i < 4 * q->ncols; i++) {
			res = colbuf[i / q->ncols];
			cname = rb_tainted_str_new2(q->colnames[i]);
#ifdef USE_RB_ENC
			rb_enc_associate(cname, rb_enc);
#endif
			q->colvals[i] = cname;
			if (rb_funcall(res, IDkeyp, 1, cname) == Qtrue) {
			    char *p;

			    cname = rb_tainted_str_new2(q->colnames[i]);
#ifdef USE_RB_ENC
			    rb_enc_associate(cname, rb_enc);
#endif
			    p = q->colnames[4 * q->ncols];
			    sprintf(p, "#%d", i);
			    cname = rb_str_cat2(cname, p);
			    q->colvals[i] = cname;
			}
			rb_obj_freeze(cname);
			rb_hash_aset(res, cname, Qtrue);
		    }
		}
	    }
	}
	/* FALL THRU */
    case DOFETCH_HASHN:
	if (mode & DOFETCH_BANG) {
	    res = rb_iv_get(q->self, "@_h");
	    if (res == Qnil) {
		res = rb_hash_new();
		rb_iv_set(q->self, "@_h", res);
	    }
	} else {
	    res = rb_hash_new();
	}
	break;
    default:
	if (mode & DOFETCH_BANG) {
	    res = rb_iv_get(q->self, "@_a");
	    if (res == Qnil) {
		res = rb_ary_new2(q->ncols);
		rb_iv_set(q->self, "@_a", res);
	    } else {
		rb_ary_clear(res);
	    }
	} else {
	    res = rb_ary_new2(q->ncols);
	}
    }
    offc = q->upc ? (2 * q->ncols) : 0;
    switch (mode & DOFETCH_MODES) {
    case DOFETCH_HASHK2:
    case DOFETCH_HASH2:
	offc += q->ncols;
	break;
    }
    for (i = 0; i < q->ncols; i++) {
	SQLLEN totlen;
	SQLLEN curlen = q->coltypes[i].size;
	SQLSMALLINT type = q->coltypes[i].type;
	VALUE v, name;
	char *valp, *freep = NULL;

	if (curlen == SQL_NO_TOTAL) {
	    SQLLEN chunksize = SEGSIZE;

	    totlen = 0;
#ifdef UNICODE
	    valp = ALLOC_N(char, chunksize + sizeof (SQLWCHAR));
#else
	    valp = ALLOC_N(char, chunksize + 1);
#endif
	    freep = valp;
	    while ((curlen == SQL_NO_TOTAL) || (curlen > chunksize)) {
		SQLRETURN rc;
		int ret;

		rc = SQLGetData(q->hstmt, (SQLUSMALLINT) (i + 1),
				type, (SQLPOINTER) (valp + totlen),
#ifdef UNICODE
				((type == SQL_C_CHAR) || (type == SQL_C_WCHAR)) ?
				(chunksize + (int) sizeof (SQLWCHAR)) :
				chunksize,
#else
				(type == SQL_C_CHAR) ?
				(chunksize + 1) : chunksize,
#endif
				&curlen);
		if (rc == SQL_NO_DATA) {
		    if (curlen == SQL_NO_TOTAL) {
			curlen = totlen;
		    }
		    break;
		}
		ret = succeeded(SQL_NULL_HENV, SQL_NULL_HDBC, q->hstmt,
				rc, &msg, "SQLGetData");
		if (!ret) {
		    xfree(valp);
		    rb_raise(Cerror, "%s", msg);
		}
		if (curlen == SQL_NULL_DATA) {
		    break;
		}
		if (curlen == SQL_NO_TOTAL) {
		    totlen += chunksize;
		} else if (curlen > chunksize) {
		    totlen += chunksize;
		    chunksize = curlen - chunksize;
		} else {
		    totlen += curlen;
		    break;
		}
#ifdef UNICODE
		REALLOC_N(valp, char, totlen + chunksize + sizeof (SQLWCHAR));
#else
		REALLOC_N(valp, char, totlen + chunksize + 1);
#endif
		if (valp == NULL) {
		    if (freep != NULL) {
			xfree(freep);
		    }
		    rb_raise(Cerror, "%s", set_err("Out of memory", 0));
		}
		freep = valp;
	    }
	    if (totlen > 0) {
		curlen = totlen;
	    }
	} else {
	    totlen = curlen;
	    valp = bufs[i];
	    if (!succeeded(SQL_NULL_HENV, SQL_NULL_HDBC, q->hstmt,
			   SQLGetData(q->hstmt, (SQLUSMALLINT) (i + 1), type,
				      (SQLPOINTER) valp, totlen, &curlen),
			   &msg, "SQLGetData")) {
		rb_raise(Cerror, "%s", msg);
	    }
	}
	if (curlen == SQL_NULL_DATA) {
	    v = Qnil;
	} else {
	    switch (type) {
	    case SQL_C_LONG:
		v = INT2NUM(*((SQLINTEGER *) valp));
		break;
	    case SQL_C_DOUBLE:
		v = rb_float_new(*((double *) valp));
		break;
#ifdef SQL_C_SBIGINT
	    case SQL_C_SBIGINT:
#ifdef LL2NUM
		v = LL2NUM(*((SQLBIGINT *) valp));
#else
		v = INT2NUM(*((SQLBIGINT *) valp));
#endif
		break;
#endif
#ifdef SQL_C_UBIGINT
	    case SQL_C_UBIGINT:
#ifdef LL2NUM
		v = ULL2NUM(*((SQLBIGINT *) valp));
#else
		v = UINT2NUM(*((SQLBIGINT *) valp));
#endif
		break;
#endif
	    case SQL_C_DATE:
		{
		    DATE_STRUCT *date;

		    if (q->dbcp != NULL && q->dbcp->rbtime == Qtrue) {
			const char *p;
			char buffer[128];
			VALUE d;

			date = (DATE_STRUCT *) valp;
			p = (q->dbcp->gmtime == Qtrue) ? "+00:00" : "";
			sprintf(buffer, "%d-%d-%dT00:00:00%s",
				date->year, date->month, date->day, p);
			d = rb_str_new2(buffer);
			v = rb_funcall(rb_cDate, IDparse, 1, d);
		    } else {
			v = Data_Make_Struct(Cdate, DATE_STRUCT, 0, xfree,
					     date);
			*date = *(DATE_STRUCT *) valp;
		    }
		}
		break;
	    case SQL_C_TIME:
		{
		    TIME_STRUCT *time;

		    if (q->dbcp != NULL && q->dbcp->rbtime == Qtrue) {
			VALUE now, frac;

			time = (TIME_STRUCT *) valp;
			frac = rb_float_new(0.0);
			now = rb_funcall(rb_cTime, IDnow, 0, NULL);
			v = rb_funcall(rb_cTime,
				       (q->dbcp->gmtime == Qtrue) ?
				       IDutc : IDlocal,
				       7,
				       rb_funcall(now, IDyear, 0, NULL),
				       rb_funcall(now, IDmonth, 0, NULL),
				       rb_funcall(now, IDday, 0, NULL),
				       INT2NUM(time->hour),
				       INT2NUM(time->minute),
				       INT2NUM(time->second),
				       frac);
		    } else {
			v = Data_Make_Struct(Ctime, TIME_STRUCT, 0, xfree,
					     time);
			*time = *(TIME_STRUCT *) valp;
		    }
		}
		break;
	    case SQL_C_TIMESTAMP:
		{
		    TIMESTAMP_STRUCT *ts;

		    if (q->dbcp != NULL && q->dbcp->rbtime == Qtrue) {
			VALUE frac;

			ts = (TIMESTAMP_STRUCT *) valp;
			frac = rb_float_new((double) 1.0e-3 * ts->fraction);
			v = rb_funcall(rb_cTime,
				       (q->dbcp->gmtime == Qtrue) ?
				       IDutc : IDlocal,
				       7,
				       INT2NUM(ts->year),
				       INT2NUM(ts->month),
				       INT2NUM(ts->day),
				       INT2NUM(ts->hour),
				       INT2NUM(ts->minute),
				       INT2NUM(ts->second),
				       frac);
		    } else {
			v = Data_Make_Struct(Ctimestamp, TIMESTAMP_STRUCT,
					     0, xfree, ts);
			*ts = *(TIMESTAMP_STRUCT *) valp;
		    }
		}
		break;
#ifdef UNICODE
	    case SQL_C_WCHAR:
		v = uc_tainted_str_new((SQLWCHAR *) valp,
				       curlen / sizeof (SQLWCHAR));
		break;
#endif
	    default:
		v = rb_tainted_str_new(valp, curlen);
		break;
	    }
	}
	if (freep != NULL) {
	    xfree(freep);
	}
	switch (mode & DOFETCH_MODES) {
	case DOFETCH_HASH:
	case DOFETCH_HASH2:
	    valp = q->colnames[i + offc];
	    name = (q->colvals == NULL) ? Qnil : q->colvals[i + offc];
	    if (name == Qnil) {
		name = rb_tainted_str_new2(valp);
#ifdef USE_RB_ENC
		rb_enc_associate(name, rb_enc);
#endif
		if (rb_funcall(res, IDkeyp, 1, name) == Qtrue) {
		    char *p;

		    name = rb_tainted_str_new2(valp);
#ifdef USE_RB_ENC
		    rb_enc_associate(name, rb_enc);
#endif
		    p = q->colnames[4 * q->ncols];
		    sprintf(p, "#%d", i);
		    name = rb_str_cat2(name, p);
		}
	    }
	    rb_hash_aset(res, name, v);
	    break;
	case DOFETCH_HASHK:
	case DOFETCH_HASHK2:
	    valp = q->colnames[i + offc];
#ifdef USE_RB_ENC
	    name = ID2SYM(rb_intern3(valp, strlen(valp), rb_enc));
#else
	    name = ID2SYM(rb_intern(valp));
#endif
	    if (rb_funcall(res, IDkeyp, 1, name) == Qtrue) {
		char *p;

		p = q->colnames[4 * q->ncols];
		sprintf(p, "%s#%d", valp, i);
#ifdef USE_RB_ENC
		name = ID2SYM(rb_intern3(p, strlen(p), rb_enc));
#else
		name = ID2SYM(rb_intern(p));
#endif
	    }
	    rb_hash_aset(res, name, v);
	    break;
	case DOFETCH_HASHN:
	    name = INT2NUM(i);
	    rb_hash_aset(res, name, v);
	    break;
	default:
	    rb_ary_push(res, v);
	}
    }
    return res;
}

static VALUE
stmt_fetch1(VALUE self, int bang)
{
    STMT *q;
    SQLRETURN ret;
    const char *msg;
    char *err;
#if (ODBCVER < 0x0300)
    SQLUINTEGER nRows;
    SQLUSMALLINT rowStat[1];
#endif

    Data_Get_Struct(self, STMT, q);
    if (q->ncols <= 0) {
	return Qnil;
    }
    if (q->usef) {
	goto usef;
    }
#if (ODBCVER < 0x0300)
    msg = "SQLExtendedFetch(SQL_FETCH_NEXT)";
    ret = SQLExtendedFetch(q->hstmt, SQL_FETCH_NEXT, 0, &nRows, rowStat);
#else
    msg = "SQLFetchScroll(SQL_FETCH_NEXT)";
    ret = SQLFetchScroll(q->hstmt, SQL_FETCH_NEXT, 0);
#endif
    if (ret == SQL_NO_DATA) {
	(void) tracesql(SQL_NULL_HENV, SQL_NULL_HDBC, q->hstmt, ret, msg);
	return Qnil;
    }
    if (succeeded(SQL_NULL_HENV, SQL_NULL_HDBC, q->hstmt, ret, &err, msg)) {
	return do_fetch(q, DOFETCH_ARY | (bang ? DOFETCH_BANG : 0));
    }
    if ((err != NULL) &&
	((strncmp(err, "IM001", 5) == 0) ||
	 (strncmp(err, "HYC00", 5) == 0))) {
usef:
	/* Fallback to SQLFetch() when others not implemented */
	msg = "SQLFetch";
	q->usef = 1;
	ret = SQLFetch(q->hstmt);
	if (ret == SQL_NO_DATA) {
	    (void) tracesql(SQL_NULL_HENV, SQL_NULL_HDBC, q->hstmt, ret, msg);
	    return Qnil;
	}
	if (succeeded(SQL_NULL_HENV, SQL_NULL_HDBC, q->hstmt, ret,
		      &err, msg)) {
	    return do_fetch(q, DOFETCH_ARY | (bang ? DOFETCH_BANG : 0));
	}
    }
    rb_raise(Cerror, "%s", err);
    return Qnil;
}

static VALUE
stmt_fetch(VALUE self)
{
    if (rb_block_given_p()) {
	return stmt_each(self);
    }
    return stmt_fetch1(self, 0);
}

static VALUE
stmt_fetch_bang(VALUE self)
{
    if (rb_block_given_p()) {
	return stmt_each(self);
    }
    return stmt_fetch1(self, 1);
}

static VALUE
stmt_fetch_first1(VALUE self, int bang, int nopos)
{
    STMT *q;
    SQLRETURN ret;
    const char *msg;
    char *err;
#if (ODBCVER < 0x0300)
    SQLUINTEGER nRows;
    SQLUSMALLINT rowStat[1];
#endif

    Data_Get_Struct(self, STMT, q);
    if (q->ncols <= 0) {
	return Qnil;
    }
    if (nopos) {
	goto dofetch;
    }
#if (ODBCVER < 0x0300)
    msg = "SQLExtendedFetch(SQL_FETCH_FIRST)";
    ret = SQLExtendedFetch(q->hstmt, SQL_FETCH_FIRST, 0, &nRows, rowStat);
#else
    msg = "SQLFetchScroll(SQL_FETCH_FIRST)";
    ret = SQLFetchScroll(q->hstmt, SQL_FETCH_FIRST, 0);
#endif
    if (ret == SQL_NO_DATA) {
	(void) tracesql(SQL_NULL_HENV, SQL_NULL_HDBC, q->hstmt, ret, msg);
	return Qnil;
    }
    if (succeeded(SQL_NULL_HENV, SQL_NULL_HDBC, q->hstmt, ret, &err, msg)) {
dofetch:
	return do_fetch(q, DOFETCH_ARY | (bang ? DOFETCH_BANG : 0));
    }
    rb_raise(Cerror, "%s", err);
    return Qnil;
}

static VALUE
stmt_fetch_first(VALUE self)
{
    return stmt_fetch_first1(self, 0, 0);
}

static VALUE
stmt_fetch_first_bang(VALUE self)
{
    return stmt_fetch_first1(self, 1, 0);
}

static VALUE
stmt_fetch_scroll1(int argc, VALUE *argv, VALUE self, int bang)
{
    STMT *q;
    VALUE dir, offs;
    SQLRETURN ret;
    int idir, ioffs = 1;
    char msg[128], *err;
#if (ODBCVER < 0x0300)
    SQLUINTEGER nRows;
    SQLUSMALLINT rowStat[1];
#endif

    rb_scan_args(argc, argv, "11", &dir, &offs);
    idir = NUM2INT(dir);
    if (offs != Qnil) {
	ioffs = NUM2INT(offs);
    }
    Data_Get_Struct(self, STMT, q);
    if (q->ncols <= 0) {
	return Qnil;
    }
#if (ODBCVER < 0x0300)
    sprintf(msg, "SQLExtendedFetch(%d)", idir);
    ret = SQLExtendedFetch(q->hstmt, (SQLSMALLINT) idir, (SQLINTEGER) ioffs,
			   &nRows, rowStat);
#else
    sprintf(msg, "SQLFetchScroll(%d)", idir);
    ret = SQLFetchScroll(q->hstmt, (SQLSMALLINT) idir, (SQLINTEGER) ioffs);
#endif
    if (ret == SQL_NO_DATA) {
	(void) tracesql(SQL_NULL_HENV, SQL_NULL_HDBC, q->hstmt, ret, msg);
	return Qnil;
    }
    if (succeeded(SQL_NULL_HENV, SQL_NULL_HDBC, q->hstmt, ret, &err, msg)) {
	return do_fetch(q, DOFETCH_ARY | (bang ? DOFETCH_BANG : 0));
    }
    rb_raise(Cerror, "%s", err);
    return Qnil;
}

static VALUE
stmt_fetch_scroll(int argc, VALUE *argv, VALUE self)
{
    return stmt_fetch_scroll1(argc, argv, self, 0);
}

static VALUE
stmt_fetch_scroll_bang(int argc, VALUE *argv, VALUE self)
{
    return stmt_fetch_scroll1(argc, argv, self, 1);
}

static VALUE
stmt_fetch_many(VALUE self, VALUE arg)
{
    int i, max = 0, all = arg == Qnil;
    VALUE res;

    if (!all) {
	max = NUM2INT(arg);
    }
    res = rb_ary_new();
    for (i = 0; all || (i < max); i++) {
	VALUE v = stmt_fetch1(self, 0);

	if (v == Qnil) {
	    break;
	}
	rb_ary_push(res, v);
    }
    return (i == 0) ? Qnil : res;
}

static VALUE
stmt_fetch_all(VALUE self)
{
    return stmt_fetch_many(self, Qnil);
}

static int
stmt_hash_mode(int argc, VALUE *argv, VALUE self)
{
    VALUE withtab = Qnil, usesym = Qnil;
    int mode = DOFETCH_HASH;

    rb_scan_args(argc, argv, "02", &withtab, &usesym);
    if ((withtab != Qtrue) && (withtab != Qfalse) && (withtab != Modbc) &&
	(rb_obj_is_kind_of(withtab, rb_cHash) == Qtrue)) {
	VALUE v;

	v = rb_hash_aref(withtab, ID2SYM(IDkey));
	if (v == ID2SYM(IDSymbol)) {
	    mode = DOFETCH_HASHK;
	} else if (v == ID2SYM(IDString)) {
	    mode = DOFETCH_HASH;
	} else if (v == ID2SYM(IDFixnum)) {
	    mode = DOFETCH_HASHN;
	} else {
	    rb_raise(Cerror, "Unsupported key mode");
	}
	if (mode != DOFETCH_HASHN) {
	    v = rb_hash_aref(withtab, ID2SYM(IDtable_names));
	    if (RTEST(v)) {
		mode = (mode == DOFETCH_HASHK)
		     ? DOFETCH_HASHK2 : DOFETCH_HASH2;
	    }
	}
	return mode;
    }
    if (withtab == Modbc) {
	return DOFETCH_HASHN;
    }
    mode = RTEST(withtab) ? DOFETCH_HASH2 : DOFETCH_HASH;
    if (RTEST(usesym)) {
	mode = (mode == DOFETCH_HASH2) ? DOFETCH_HASHK2 : DOFETCH_HASHK;
    }
    return mode;
}

static VALUE
stmt_fetch_hash1(int argc, VALUE *argv, VALUE self, int bang)
{
    STMT *q;
    SQLRETURN ret;
    int mode = stmt_hash_mode(argc, argv, self);
    const char *msg;
    char *err;
#if (ODBCVER < 0x0300)
    SQLUINTEGER nRows;
    SQLUSMALLINT rowStat[1];
#endif

    Data_Get_Struct(self, STMT, q);
    if (q->ncols <= 0) {
	return Qnil;
    }
    if (q->usef) {
	goto usef;
    }
#if (ODBCVER < 0x0300)
    msg = "SQLExtendedFetch(SQL_FETCH_NEXT)";
    ret = SQLExtendedFetch(q->hstmt, SQL_FETCH_NEXT, 0, &nRows, rowStat);
#else
    msg = "SQLFetchScroll(SQL_FETCH_NEXT)";
    ret = SQLFetchScroll(q->hstmt, SQL_FETCH_NEXT, 0);
#endif
    if (ret == SQL_NO_DATA) {
	(void) tracesql(SQL_NULL_HENV, SQL_NULL_HDBC, q->hstmt, ret, msg);
	return Qnil;
    }
    if (succeeded(SQL_NULL_HENV, SQL_NULL_HDBC, q->hstmt, ret, &err, msg)) {
	return do_fetch(q, mode | (bang ? DOFETCH_BANG : 0));
    }
    if ((err != NULL) &&
	((strncmp(err, "IM001", 5) == 0) ||
	 (strncmp(err, "HYC00", 5) == 0))) {
usef:
	/* Fallback to SQLFetch() when others not implemented */
	msg = "SQLFetch";
	q->usef = 1;
	ret = SQLFetch(q->hstmt);
	if (ret == SQL_NO_DATA) {
	    (void) tracesql(SQL_NULL_HENV, SQL_NULL_HDBC, q->hstmt, ret, msg);
	    return Qnil;
	}
	if (succeeded(SQL_NULL_HENV, SQL_NULL_HDBC, q->hstmt, ret,
		      &err, msg)) {
	    return do_fetch(q, mode | (bang ? DOFETCH_BANG : 0));
	}
    }
    rb_raise(Cerror, "%s", err);
    return Qnil;
}

static VALUE
stmt_fetch_hash(int argc, VALUE *argv, VALUE self)
{
    if (rb_block_given_p()) {
	return stmt_each_hash(argc, argv, self);
    }
    return stmt_fetch_hash1(argc, argv, self, 0);
}

static VALUE
stmt_fetch_hash_bang(int argc, VALUE *argv, VALUE self)
{
    if (rb_block_given_p()) {
	return stmt_each_hash(argc, argv, self);
    }
    return stmt_fetch_hash1(argc, argv, self, 1);
}

static VALUE
stmt_fetch_first_hash1(int argc, VALUE *argv, VALUE self, int bang, int nopos)
{
    STMT *q;
    SQLRETURN ret;
    int mode = stmt_hash_mode(argc, argv, self);
    const char *msg;
    char *err;
#if (ODBCVER < 0x0300)
    SQLUINTEGER nRows;
    SQLUSMALLINT rowStat[1];
#endif

    Data_Get_Struct(self, STMT, q);
    if (q->ncols <= 0) {
	return Qnil;
    }
    if (nopos) {
	goto dofetch;
    }
#if (ODBCVER < 0x0300)
    msg = "SQLExtendedFetch(SQL_FETCH_FIRST)";
    ret = SQLExtendedFetch(q->hstmt, SQL_FETCH_FIRST, 0, &nRows, rowStat);
#else
    msg = "SQLFetchScroll(SQL_FETCH_FIRST)";
    ret = SQLFetchScroll(q->hstmt, SQL_FETCH_FIRST, 0);
#endif
    if (ret == SQL_NO_DATA) {
	(void) tracesql(SQL_NULL_HENV, SQL_NULL_HDBC, q->hstmt, ret, msg);
	return Qnil;
    }
    if (succeeded(SQL_NULL_HENV, SQL_NULL_HDBC, q->hstmt, ret, &err, msg)) {
dofetch:
	return do_fetch(q, mode | (bang ? DOFETCH_BANG : 0));
    }
    rb_raise(Cerror, "%s", err);
    return Qnil;
}

static VALUE
stmt_fetch_first_hash(int argc, VALUE *argv, VALUE self)
{
    return stmt_fetch_first_hash1(argc, argv, self, 0, 0);
}

static VALUE
stmt_each(VALUE self)
{
    VALUE row, res = Qnil;
    STMT *q;
#if (ODBCVER < 0x0300)
    SQLUINTEGER nRows;
    SQLUSMALLINT rowStat[1];
#endif

    Data_Get_Struct(self, STMT, q);
#if (ODBCVER < 0x0300)
    switch (callsql(SQL_NULL_HENV, SQL_NULL_HDBC, q->hstmt,
		    SQLExtendedFetch(q->hstmt, SQL_FETCH_FIRST, 0, &nRows,
				     rowStat),
		    "SQLExtendedFetch(SQL_FETCH_FIRST)"))
#else
    switch (callsql(SQL_NULL_HENV, SQL_NULL_HDBC, q->hstmt,
		    SQLFetchScroll(q->hstmt, SQL_FETCH_FIRST, 0),
		    "SQLFetchScroll(SQL_FETCH_FIRST)"))
#endif
    {
    case SQL_NO_DATA:
	row = Qnil;
	break;
    case SQL_SUCCESS:
    case SQL_SUCCESS_WITH_INFO:
	row = stmt_fetch_first1(self, 0, 1);
	break;
    default:
	row = stmt_fetch1(self, 0);
    }
    if (rb_block_given_p()) {
	while (row != Qnil) {
	    rb_yield(row);
	    row = stmt_fetch1(self, 0);
	}
	return self;
    }
    if (row != Qnil) {
	res = rb_ary_new();
	while (row != Qnil) {
	    rb_ary_push(res, row);
	    row = stmt_fetch1(self, 0);
	}
    }
    return res;
}

static VALUE
stmt_each_hash(int argc, VALUE *argv, VALUE self)
{
    VALUE row, res = Qnil, withtab[2];
    STMT *q;
    int mode = stmt_hash_mode(argc, argv, self);
#if (ODBCVER < 0x0300)
    SQLUINTEGER nRows;
    SQLUSMALLINT rowStat[1];
#endif

    if (mode == DOFETCH_HASHN) {
	withtab[0] = Modbc;
	withtab[1] = Qfalse;
    } else {
	withtab[0] = ((mode == DOFETCH_HASH2) || (mode == DOFETCH_HASHK2))
		   ? Qtrue : Qfalse;
	withtab[1] = ((mode == DOFETCH_HASHK) || (mode == DOFETCH_HASHK2))
		   ? Qtrue : Qfalse;
    }
    Data_Get_Struct(self, STMT, q);
#if (ODBCVER < 0x0300)
    switch (callsql(SQL_NULL_HENV, SQL_NULL_HDBC, q->hstmt,
		    SQLExtendedFetch(q->hstmt, SQL_FETCH_FIRST, 0, &nRows,
				     rowStat),
		    "SQLExtendedFetch(SQL_FETCH_FIRST)"))
#else
    switch (callsql(SQL_NULL_HENV, SQL_NULL_HDBC, q->hstmt,
		    SQLFetchScroll(q->hstmt, SQL_FETCH_FIRST, 0),
		    "SQLFetchScroll(SQL_FETCH_FIRST)"))
#endif
    {
    case SQL_NO_DATA:
	row = Qnil;
	break;
    case SQL_SUCCESS:
    case SQL_SUCCESS_WITH_INFO:
	row = stmt_fetch_first_hash1(2, withtab, self, 0, 1);
	break;
    default:
	row = stmt_fetch_hash1(2, withtab, self, 0);
    }
    if (rb_block_given_p()) {
	while (row != Qnil) {
	    rb_yield(row);
	    row = stmt_fetch_hash1(2, withtab, self, 0);
	}
	return self;
    }
    if (row != Qnil) {
	res = rb_ary_new();
	while (row != Qnil) {
	    rb_ary_push(res, row);
	    row = stmt_fetch_hash1(2, withtab, self, 0);
	}
    }
    return res;
}

static VALUE
stmt_more_results(VALUE self)
{
    STMT *q;

    if (rb_block_given_p()) {
	rb_raise(rb_eArgError, "block not allowed");
    }
    Data_Get_Struct(self, STMT, q);
    if (q->hstmt == SQL_NULL_HSTMT) {
	return Qfalse;
    }
    switch (tracesql(SQL_NULL_HENV, SQL_NULL_HDBC, q->hstmt,
		     SQLMoreResults(q->hstmt), "SQLMoreResults")) {
    case SQL_NO_DATA:
	return Qfalse;
    case SQL_SUCCESS:
    case SQL_SUCCESS_WITH_INFO:
	free_stmt_sub(q, 0);
	make_result(q->dbc, q->hstmt, self, 0);
	break;
    default:
	rb_raise(Cerror, "%s",
		 get_err(SQL_NULL_HENV, SQL_NULL_HDBC, q->hstmt));
    }
    return Qtrue;
}

static VALUE
stmt_prep_int(int argc, VALUE *argv, VALUE self, int mode)
{
    DBC *p = get_dbc(self);
    STMT *q = NULL;
    VALUE sql, dbc, stmt;
    SQLHSTMT hstmt;
#ifdef UNICODE
    SQLWCHAR *ssql = NULL;
#else
    SQLCHAR *ssql = NULL;
#endif
    char *csql = NULL, *msg = NULL;

    if (rb_obj_is_kind_of(self, Cstmt) == Qtrue) {
	Data_Get_Struct(self, STMT, q);
	free_stmt_sub(q, 0);
	if (q->hstmt == SQL_NULL_HSTMT) {
	    if (!succeeded(SQL_NULL_HENV, p->hdbc, q->hstmt,
			   SQLAllocStmt(p->hdbc, &q->hstmt),
			   &msg, "SQLAllocStmt")) {
		rb_raise(Cerror, "%s", msg);
	    }
	} else if (!succeeded(SQL_NULL_HENV, SQL_NULL_HDBC, q->hstmt,
			      SQLFreeStmt(q->hstmt, SQL_CLOSE),
			      &msg, "SQLFreeStmt(SQL_CLOSE)")) {
	    rb_raise(Cerror, "%s", msg);
	}
	hstmt = q->hstmt;
	stmt = self;
	dbc = q->dbc;
    } else {
	if (!succeeded(SQL_NULL_HENV, p->hdbc, SQL_NULL_HSTMT,
		       SQLAllocStmt(p->hdbc, &hstmt),
		       &msg, "SQLAllocStmt")) {
	    rb_raise(Cerror, "%s", msg);
	}
	stmt = Qnil;
	dbc = self;
    }
    rb_scan_args(argc, argv, "1", &sql);
    Check_Type(sql, T_STRING);
#ifdef UNICODE
#ifdef USE_RB_ENC
    sql = rb_funcall(sql, IDencode, 1, rb_encv);
#endif
    csql = STR2CSTR(sql);
    ssql = uc_from_utf((unsigned char *) csql, -1);
    if (ssql == NULL) {
	rb_raise(Cerror, "%s", set_err("Out of memory", 0));
    }
#else
    csql = STR2CSTR(sql);
    ssql = (SQLCHAR *) csql;
#endif
    if ((mode & MAKERES_EXECD)) {
	SQLRETURN ret;

	if (!succeeded_nodata(SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
			      (ret = SQLExecDirect(hstmt, ssql, SQL_NTS)),
			      &msg, "SQLExecDirect('%s')", csql)) {
	    goto sqlerr;
	}
	if (ret == SQL_NO_DATA) {
	    callsql(SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
		    SQLFreeStmt(hstmt, SQL_CLOSE), "SQLFreeStmt(SQL_DROP)");
	    if (q != NULL) {
		q->hstmt = SQL_NULL_HSTMT;
		unlink_stmt(q);
	    }
	    hstmt = SQL_NULL_HSTMT;
	}
    } else if (!succeeded(SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
			  SQLPrepare(hstmt, ssql, SQL_NTS),
			  &msg, "SQLPrepare('%s')", csql)) {
sqlerr:
#ifdef UNICODE
	uc_free(ssql);
#endif
	callsql(SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
		SQLFreeStmt(hstmt, SQL_DROP), "SQLFreeStmt(SQL_DROP)");
	if (q != NULL) {
	    q->hstmt = SQL_NULL_HSTMT;
	    unlink_stmt(q);
	}
	rb_raise(Cerror, "%s", msg);
    } else {
	mode |= MAKERES_PREPARE;
    }
#ifdef UNICODE
    uc_free(ssql);
#endif
    return make_result(dbc, hstmt, stmt, mode);
}

static VALUE
stmt_prep(int argc, VALUE *argv, VALUE self)
{
    return stmt_prep_int(argc, argv, self, MAKERES_BLOCK);
}

static int
bind_one_param(int pnum, VALUE arg, STMT *q, char **msgp, int *outpp)
{
    SQLPOINTER valp = (SQLPOINTER) &q->paraminfo[pnum].buffer;
    SQLSMALLINT ctype, stype;
    SQLINTEGER vlen, rlen;
    SQLUINTEGER coldef;
#ifdef NO_RB_STR2CSTR
    VALUE val;
#endif
    long llen;
    int retry = 1;
#ifdef UNICODE
    SQLWCHAR *up;

    q->paraminfo[pnum].tofree = NULL;
#endif
    switch (TYPE(arg)) {
    case T_STRING:
#ifdef UNICODE
	ctype = SQL_C_WCHAR;
#ifdef USE_RB_ENC
	arg = rb_funcall(arg, IDencode, 1, rb_encv);
#endif
#ifndef NO_RB_STR2CSTR
	up = (SQLWCHAR *) rb_str2cstr(arg, &llen);
	if (llen != (long) strlen((char *) up)) {
	    ctype = SQL_C_BINARY;
	    valp = (SQLPOINTER) up;
	    rlen = llen;
	    vlen = rlen + 1;
	    break;
	}
#else
	val = rb_string_value(&arg);
	up = (SQLWCHAR *) RSTRING_PTR(val);
	llen = RSTRING_LEN(val);
	if (up == NULL) {
	    goto oom;
	}
	if (memchr((char *) up, 0, llen)) {
	    ctype = SQL_C_BINARY;
	    valp = (SQLPOINTER) up;
	    rlen = llen;
	    vlen = rlen + 1;
	    break;
	}
	up = (SQLWCHAR *) rb_string_value_cstr(&arg);
#endif
	up = uc_from_utf((unsigned char *) up, llen);
	if (up == NULL) {
	    goto oom;
	}
	*(SQLWCHAR **) valp = up;
	rlen = uc_strlen(up) * sizeof (SQLWCHAR);
	vlen = rlen + sizeof (SQLWCHAR);
	q->paraminfo[pnum].tofree = up;
#else
	ctype = SQL_C_CHAR;
#ifndef NO_RB_STR2CSTR
	valp = (SQLPOINTER) rb_str2cstr(arg, &llen);
	rlen = llen;
	if (rlen != (SQLINTEGER) strlen((char *) valp)) {
	    ctype = SQL_C_BINARY;
	}
	vlen = rlen + 1;
#else
	val = rb_string_value(&arg);
	valp = (SQLPOINTER) RSTRING_PTR(val);
	llen = RSTRING_LEN(val);
	if (valp == NULL) {
	    goto oom;
	}
	rlen = llen;
	vlen = rlen + 1;
	if (memchr((char *) valp, 0, llen)) {
	    ctype = SQL_C_BINARY;
	    break;
	}
	valp = (SQLPOINTER) rb_string_value_cstr(&arg);
#endif
#endif
	break;
    case T_FIXNUM:
	ctype = SQL_C_LONG;
	*(SQLINTEGER *) valp = FIX2INT(arg);
	rlen = 1;
	vlen = sizeof (SQLINTEGER);
	break;
    case T_FLOAT:
	ctype = SQL_C_DOUBLE;
	*(double *) valp = NUM2DBL(arg);
	rlen = 1;
	vlen = sizeof (double);
	break;
    case T_NIL:
	ctype = SQL_C_CHAR;
	valp = NULL;
	rlen = SQL_NULL_DATA;
	vlen = 0;
	break;
    case T_SYMBOL:
	ctype = SQL_C_CHAR;
	valp = NULL;
	vlen = 0;
	if (arg == ID2SYM(IDNULL)) {
	    rlen = SQL_NULL_DATA;
	} else if (arg == ID2SYM(IDdefault)) {
	    rlen = SQL_DEFAULT_PARAM;
	}
	/* fall through */
    default:
	if (rb_obj_is_kind_of(arg, Cdate) == Qtrue) {
	    DATE_STRUCT *date;

	    ctype = SQL_C_DATE;
	    Data_Get_Struct(arg, DATE_STRUCT, date);
	    valp = (SQLPOINTER) date;
	    rlen = 1;
	    vlen = sizeof (DATE_STRUCT);
	    break;
	}
	if (rb_obj_is_kind_of(arg, Ctime) == Qtrue) {
	    TIME_STRUCT *time;

	    ctype = SQL_C_TIME;
	    Data_Get_Struct(arg, TIME_STRUCT, time);
	    valp = (SQLPOINTER) time;
	    rlen = 1;
	    vlen = sizeof (TIME_STRUCT);
	    break;
	}
	if (rb_obj_is_kind_of(arg, Ctimestamp) == Qtrue) {
	    TIMESTAMP_STRUCT *ts;

	    ctype = SQL_C_TIMESTAMP;
	    Data_Get_Struct(arg, TIMESTAMP_STRUCT, ts);
	    valp = (SQLPOINTER) ts;
	    rlen = 1;
	    vlen = sizeof (TIMESTAMP_STRUCT);
	    break;
	}
	if (rb_obj_is_kind_of(arg, rb_cTime) == Qtrue) {
	    if (q->paraminfo[pnum].type == SQL_TIME) {
		TIME_STRUCT *time;

		ctype = SQL_C_TIME;
		time = (TIME_STRUCT *) valp;
		memset(time, 0, sizeof (TIME_STRUCT));
		time->hour   = rb_funcall(arg, IDhour, 0, NULL);
		time->minute = rb_funcall(arg, IDmin, 0, NULL);
		time->second = rb_funcall(arg, IDsec, 0, NULL);
		rlen = 1;
		vlen = sizeof (TIME_STRUCT);
	    } else if (q->paraminfo[pnum].type == SQL_DATE) {
		DATE_STRUCT *date;

		ctype = SQL_C_DATE;
		date = (DATE_STRUCT *) valp;
		memset(date, 0, sizeof (DATE_STRUCT));
		date->year  = rb_funcall(arg, IDyear, 0, NULL);
		date->month = rb_funcall(arg, IDmonth, 0, NULL);
		date->day   = rb_funcall(arg, IDday, 0, NULL);
		rlen = 1;
		vlen = sizeof (TIMESTAMP_STRUCT);
	    } else {
		TIMESTAMP_STRUCT *ts;

		ctype = SQL_C_TIMESTAMP;
		ts = (TIMESTAMP_STRUCT *) valp;
		memset(ts, 0, sizeof (TIMESTAMP_STRUCT));
		ts->year     = rb_funcall(arg, IDyear, 0, NULL);
		ts->month    = rb_funcall(arg, IDmonth, 0, NULL);
		ts->day      = rb_funcall(arg, IDday, 0, NULL);
		ts->hour     = rb_funcall(arg, IDhour, 0, NULL);
		ts->minute   = rb_funcall(arg, IDmin, 0, NULL);
		ts->second   = rb_funcall(arg, IDsec, 0, NULL);
#ifdef TIME_USE_USEC
		ts->fraction = rb_funcall(arg, IDusec, 0, NULL) * 1000;
#else
		ts->fraction = rb_funcall(arg, IDnsec, 0, NULL);
#endif
		rlen = 1;
		vlen = sizeof (TIMESTAMP_STRUCT);
	    }
	    break;
	}
	if (rb_obj_is_kind_of(arg, rb_cDate) == Qtrue) {
	    DATE_STRUCT *date;

	    ctype = SQL_C_DATE;
	    date = (DATE_STRUCT *) valp;
	    memset(date, 0, sizeof (DATE_STRUCT));
	    date->year  = rb_funcall(arg, IDyear, 0, NULL);
	    date->month = rb_funcall(arg, IDmonth, 0, NULL);
	    date->day   = rb_funcall(arg, IDmday, 0, NULL);
	    rlen = 1;
	    vlen = sizeof (DATE_STRUCT);
	    break;
	}
	ctype = SQL_C_CHAR;
#ifndef NO_RB_STR2CSTR
	valp = (SQLPOINTER *) rb_str2cstr(rb_str_to_str(arg), &llen);
	rlen = llen;
	if (rlen != (SQLINTEGER) strlen((char *) valp)) {
	    ctype = SQL_C_BINARY;
	}
	vlen = rlen + 1;
#else
	val = rb_string_value(&arg);
	valp = (SQLPOINTER) RSTRING_PTR(val);
	llen = RSTRING_LEN(val);
	if (valp == NULL) {
	    goto oom;
	}
	rlen = llen;
	vlen = rlen + 1;
	if (memchr((char *) valp, 0, llen)) {
	    ctype = SQL_C_BINARY;
	    break;
	}
	valp = (SQLPOINTER) rb_string_value_cstr(&arg);
#endif
	break;
    }
    stype = q->paraminfo[pnum].type;
    coldef = q->paraminfo[pnum].coldef;
    q->paraminfo[pnum].rlen = rlen;
    q->paraminfo[pnum].ctype = ctype;
    if (coldef == 0) {
	switch (ctype) {
	case SQL_C_LONG:
	    coldef = 10;
	    break;
	case SQL_C_DOUBLE:
	    coldef = 15;
	    if (stype == SQL_VARCHAR) {
		stype = SQL_DOUBLE;
	    }
	    break;
	case SQL_C_DATE:
	    coldef = 10;
	    break;
	case SQL_C_TIME:
	    coldef = 8;
	    break;
	case SQL_C_TIMESTAMP:
	    coldef = 19;
	    break;
	default:
	    /*
	     * Patch adopted from the Perl DBD::ODBC module ...
	     * per patch from Paul G. Weiss, who was experiencing re-preparing
	     * of queries when the size of the bound string's were increasing
	     * for example select * from tabtest where name = ?
	     * then executing with 'paul' and then 'thomas' would cause
	     * SQLServer to prepare the query twice, but if we ran 'thomas'
	     * then 'paul', it would not re-prepare the query.  The key seems
	     * to be allocating enough space for the largest parameter.
	     * TBD: the default for this should be a tunable parameter.
	     */
	    if ((stype == SQL_VARCHAR) &&
		(q->paraminfo[pnum].iotype != SQL_PARAM_INPUT_OUTPUT) &&
		(q->paraminfo[pnum].iotype != SQL_PARAM_OUTPUT)) {
		if (q->paraminfo[pnum].coldef_max == 0) {
		    q->paraminfo[pnum].coldef_max = (vlen > 128) ? vlen : 128;
		} else {
		    /* bump up max, if needed */
		    if (vlen > (SQLINTEGER) q->paraminfo[pnum].coldef_max) {
			q->paraminfo[pnum].coldef_max = vlen;
		    }
		}
		coldef = q->paraminfo[pnum].coldef_max;
	    } else {
		coldef = vlen;
	    }
	    break;
	}
    }
    if ((q->paraminfo[pnum].iotype == SQL_PARAM_INPUT_OUTPUT) ||
	(q->paraminfo[pnum].iotype == SQL_PARAM_OUTPUT)) {
	if (valp == NULL) {
	    if (q->paraminfo[pnum].outsize > 0) {
		if (q->paraminfo[pnum].outbuf != NULL) {
		    xfree(q->paraminfo[pnum].outbuf);
		}
		q->paraminfo[pnum].outbuf = xmalloc(q->paraminfo[pnum].outsize);
		if (q->paraminfo[pnum].outbuf == NULL) {
		    goto oom;
		}
		ctype = q->paraminfo[pnum].ctype = q->paraminfo[pnum].outtype;
		outpp[0]++;
		valp = q->paraminfo[pnum].outbuf;
		vlen = q->paraminfo[pnum].outsize;
	    }
	} else {
	    if (q->paraminfo[pnum].outbuf != NULL) {
		xfree(q->paraminfo[pnum].outbuf);
	    }
	    q->paraminfo[pnum].outbuf = xmalloc(vlen);
	    if (q->paraminfo[pnum].outbuf == NULL) {
oom:
#ifdef UNICODE
		if (q->paraminfo[pnum].tofree != NULL) {
		    uc_free(q->paraminfo[pnum].tofree);
		    q->paraminfo[pnum].tofree = NULL;
		}
#endif
		*msgp = set_err("Out of memory", 0);
		return -1;
	    }
#ifdef UNICODE
	    if (ctype == SQL_C_WCHAR) {
		memcpy(q->paraminfo[pnum].outbuf, *(SQLWCHAR **) valp, vlen);
	    } else
#endif
	    memcpy(q->paraminfo[pnum].outbuf, valp, vlen);
#ifdef UNICODE
	    if (ctype == SQL_C_WCHAR) {
		*(SQLWCHAR **) valp = (SQLWCHAR *) q->paraminfo[pnum].outbuf;
	    } else
#endif
	    valp = q->paraminfo[pnum].outbuf;
	    outpp[0]++;
	}
    }
retry:
#ifdef UNICODE
    if (!succeeded(SQL_NULL_HENV, SQL_NULL_HDBC, q->hstmt,
		   SQLBindParameter(q->hstmt, (SQLUSMALLINT) (pnum + 1),
				    q->paraminfo[pnum].iotype,
				    ctype, stype, coldef,
				    q->paraminfo[pnum].scale,
				    (ctype == SQL_C_WCHAR) ?
				    *(SQLWCHAR **) valp : valp,
				    vlen, &q->paraminfo[pnum].rlen),
		   msgp, "SQLBindParameter(%d)", pnum + 1))
#else
    if (!succeeded(SQL_NULL_HENV, SQL_NULL_HDBC, q->hstmt,
		   SQLBindParameter(q->hstmt, (SQLUSMALLINT) (pnum + 1),
				    q->paraminfo[pnum].iotype,
				    ctype, stype, coldef,
				    q->paraminfo[pnum].scale,
				    valp, vlen, &q->paraminfo[pnum].rlen),
		   msgp, "SQLBindParameter(%d)", pnum + 1))
#endif
    {
	if (retry) {
	    retry = 0;
	    if (stype == SQL_VARCHAR) {
		/* maybe MS Jet memo field */
		stype = SQL_LONGVARCHAR;
		goto retry;
	    }
#ifdef UNICODE
	    if (stype == SQL_WVARCHAR) {
		stype = SQL_WLONGVARCHAR;
		goto retry;
	    }
#endif
	}
	return -1;
    }
    return 0;
}

static VALUE
stmt_exec_int(int argc, VALUE *argv, VALUE self, int mode)
{
    STMT *q;
    int i, argnum, has_out_parms = 0;
    char *msg = NULL;
    SQLRETURN ret;

    Data_Get_Struct(self, STMT, q);
    if (argc > q->nump - ((EXEC_PARMXOUT(mode) < 0) ? 0 : 1)) {
	rb_raise(Cerror, "%s", set_err("Too much parameters", 0));
    }
    if (q->hstmt == SQL_NULL_HSTMT) {
	rb_raise(Cerror, "%s", set_err("Stale ODBC::Statement", 0));
    }
    if (!succeeded(SQL_NULL_HENV, SQL_NULL_HDBC, q->hstmt,
		   SQLFreeStmt(q->hstmt, SQL_CLOSE),
		   &msg, "SQLFreeStmt(SQL_CLOSE)")) {
	goto error;
    }
    callsql(SQL_NULL_HENV, SQL_NULL_HDBC, q->hstmt,
	    SQLFreeStmt(q->hstmt, SQL_RESET_PARAMS),
	    "SQLFreeStmt(SQL_RESET_PARMS)");
    for (i = argnum = 0; i < q->nump; i++) {
	VALUE arg;

	if (i == EXEC_PARMXOUT(mode)) {
	    if (bind_one_param(i, Qnil, q, &msg, &has_out_parms) < 0) {
		goto error;
	    }
	    continue;
	}
	arg = (argnum < argc) ? argv[argnum++] : Qnil;
	if (bind_one_param(i, arg, q, &msg, &has_out_parms) < 0) {
	    goto error;
	}
    }
    if (!succeeded_nodata(SQL_NULL_HENV, SQL_NULL_HDBC, q->hstmt,
			  (ret = SQLExecute(q->hstmt)),
			  &msg, "SQLExecute")) {
error:
#ifdef UNICODE
	for (i = 0; i < q->nump; i++) {
	    if (q->paraminfo[i].tofree != NULL) {
		uc_free(q->paraminfo[i].tofree);
		q->paraminfo[i].tofree = NULL;
	    }
	}
#endif
	callsql(SQL_NULL_HENV, SQL_NULL_HDBC, q->hstmt,
		SQLFreeStmt(q->hstmt, SQL_DROP), "SQLFreeStmt(SQL_DROP)");
	q->hstmt = SQL_NULL_HSTMT;
	unlink_stmt(q);
	rb_raise(Cerror, "%s", msg);
    }
#ifdef UNICODE
    for (i = 0; i < q->nump; i++) {
	if (q->paraminfo[i].tofree != NULL) {
	    uc_free(q->paraminfo[i].tofree);
	    q->paraminfo[i].tofree = NULL;
	}
    }
#endif
    if (!has_out_parms) {
	callsql(SQL_NULL_HENV, SQL_NULL_HDBC, q->hstmt,
		SQLFreeStmt(q->hstmt, SQL_RESET_PARAMS),
		"SQLFreeStmt(SQL_RESET_PARAMS)");
    }
    if (ret == SQL_NO_DATA) {
	return Qnil;
    }
    return make_result(q->dbc, q->hstmt, self, mode);
}

static VALUE
stmt_exec(int argc, VALUE *argv, VALUE self)
{
    return stmt_exec_int(argc, argv, self, MAKERES_BLOCK);
}

static VALUE
stmt_run(int argc, VALUE *argv, VALUE self)
{
    if (argc < 1) {
	rb_raise(rb_eArgError, "wrong # of arguments");
    }
    if (argc == 1) {
	return stmt_prep_int(1, argv, self,
			     MAKERES_EXECD | MAKERES_BLOCK);
    }
    return stmt_exec(argc - 1, argv + 1, stmt_prep_int(1, argv, self, 0));
}

static VALUE
stmt_do(int argc, VALUE *argv, VALUE self)
{
    VALUE stmt;
    
    if (argc < 1) {
	rb_raise(rb_eArgError, "wrong # of arguments");
    }
    if (argc == 1) {
	stmt = stmt_prep_int(1, argv, self,
			     MAKERES_EXECD | MAKERES_BLOCK | MAKERES_NOCLOSE);
    } else {
	stmt = stmt_prep_int(1, argv, self, 0);
	stmt_exec_int(argc - 1, argv + 1, stmt,
		      MAKERES_BLOCK | MAKERES_NOCLOSE);
    }
    return rb_ensure(stmt_nrows, stmt, stmt_drop, stmt);
}

static VALUE
stmt_ignorecase(int argc, VALUE *argv, VALUE self)
{
    VALUE onoff = Qnil;
    int *flag = NULL;

    rb_scan_args(argc, argv, "01", &onoff);
    if (rb_obj_is_kind_of(self, Cstmt) == Qtrue) {
	STMT *q;

	Data_Get_Struct(self, STMT, q);
	flag = &q->upc;
    } else if (rb_obj_is_kind_of(self, Cdbc) == Qtrue) {
	DBC *p;

	Data_Get_Struct(self, DBC, p);
	flag = &p->upc;
    } else {
	rb_raise(rb_eTypeError, "ODBC::Statement or ODBC::Database expected");
	return Qnil;
    }
    if (argc > 0) {
	*flag = RTEST(onoff);
    }
    return *flag ? Qtrue : Qfalse;
}

/*
 *----------------------------------------------------------------------
 *
 *      Create statement without implicit SQL prepare or execute.
 *
 *----------------------------------------------------------------------
 */
static VALUE
stmt_new(VALUE self)
{
    DBC *p;
    SQLHSTMT hstmt;
    char *msg = NULL;

    Data_Get_Struct(self, DBC, p);
    if (!succeeded(SQL_NULL_HENV, p->hdbc, SQL_NULL_HSTMT,
		   SQLAllocStmt(p->hdbc, &hstmt),
		   &msg, "SQLAllocStmt")) {
	rb_raise(Cerror, "%s", msg);
    }
    return wrap_stmt(self, p, hstmt, NULL);
}

/*
 *----------------------------------------------------------------------
 *
 *      Procedures with statements.
 *
 *----------------------------------------------------------------------
 */

static VALUE
stmt_proc_init(int argc, VALUE *argv, VALUE self)
{
    VALUE stmt = (argc > 0) ? argv[0] : Qnil;

    if (rb_obj_is_kind_of(stmt, Cstmt) == Qtrue) {
	rb_iv_set(self, "@statement", stmt);
	rb_iv_set(self, "@return_output_param", (argc > 1) ? argv[1] : Qnil);
	return self;
    }
    rb_raise(rb_eTypeError, "need ODBC::Statement as argument");
    return Qnil;
}

static VALUE
stmt_proc_call(int argc, VALUE *argv, VALUE self)
{
    VALUE stmt, val;

    stmt = rb_iv_get(self, "@statement");
    val = rb_iv_get(self, "@return_output_param");
    if (RTEST(val)) {
	int parnum = NUM2INT(val);

	stmt_exec_int(argc, argv, stmt, EXEC_PARMXNULL(parnum));
	rb_call_super(1, &stmt);
	return stmt_param_output_value(1, &val, stmt);
    }
    stmt_exec_int(argc, argv, stmt, 0);
    return rb_call_super(1, &stmt);
}

static VALUE
stmt_proc(int argc, VALUE *argv, VALUE self)
{
    VALUE sql, ptype, psize, pnum = Qnil, stmt, args[2];
    int parnum = 0;

    rb_scan_args(argc, argv, "13", &sql, &ptype, &psize, &pnum);
    if (!rb_block_given_p()) {
	rb_raise(rb_eArgError, "block required");
    }
    stmt = stmt_prep_int(1, &sql, self, 0);
    if (argc == 1) {
	return rb_funcall(Cproc, IDnew, 1, stmt);
    }
    if ((argc < 4) || (pnum == Qnil)) {
	pnum = INT2NUM(parnum);
    } else {
	parnum = NUM2INT(pnum);
    }
    args[0] = pnum;
    args[1] = INT2NUM(SQL_PARAM_OUTPUT);
    stmt_param_iotype(2, args, stmt);
    args[1] = ptype;
    stmt_param_output_type(2, args, stmt);
    if (argc > 2) {
	args[1] = psize;
    } else {
	args[1] = INT2NUM(256);
    }
    stmt_param_output_size(2, args, stmt);
    return rb_funcall(Cproc, IDnew, 2, stmt, pnum);
}

static VALUE
stmt_procwrap(int argc, VALUE *argv, VALUE self)
{
    VALUE arg0 = Qnil, arg1 = Qnil;

    rb_scan_args(argc, argv, "02", &arg0, &arg1);
    if (rb_obj_is_kind_of(self, Cstmt) == Qtrue) {
	if (arg1 != Qnil) {
	    rb_raise(rb_eArgError, "wrong # arguments");
	}
	arg1 = arg0;
	arg0 = self;
    } else if (rb_obj_is_kind_of(arg0, Cstmt) != Qtrue) {
	rb_raise(rb_eTypeError, "need ODBC::Statement as 1st argument");
    }
    return rb_funcall(Cproc, IDnew, 2, arg0, arg1);
}

/*
 *----------------------------------------------------------------------
 *
 *      Module functions.
 *
 *----------------------------------------------------------------------
 */

static VALUE
mod_dbcdisc(VALUE dbc)
{
    return dbc_disconnect(0, NULL, dbc);
}

static VALUE
mod_connect(int argc, VALUE *argv, VALUE self)
{
    VALUE dbc = dbc_new(argc, argv, self);

    if (rb_block_given_p()) {
	return rb_ensure(rb_yield, dbc, mod_dbcdisc, dbc);
    }
    return dbc;
}

static VALUE
mod_2time(int argc, VALUE *argv, VALUE self)
{
    VALUE a1, a2;
    VALUE y, m, d, hh, mm, ss, us;
    int once = 0;

    rb_scan_args(argc, argv, "11", &a1, &a2);
again:
    if (rb_obj_is_kind_of(a1, Ctimestamp) == Qtrue) {
	TIMESTAMP_STRUCT *ts;

	if (argc > 1) {
	    rb_raise(rb_eArgError, "wrong # arguments(2 for 1)");
	}
	Data_Get_Struct(a1, TIMESTAMP_STRUCT, ts);
	y = INT2NUM(ts->year);
	m = INT2NUM(ts->month);
	d = INT2NUM(ts->day);
	hh = INT2NUM(ts->hour);
	mm = INT2NUM(ts->minute);
	ss = INT2NUM(ts->second);
	us = INT2NUM(ts->fraction / 1000);
	goto mktime;
    }
    if (rb_obj_is_kind_of(a1, Cdate) == Qtrue) {
	DATE_STRUCT *date;

	if (a2 != Qnil) {
	    if (rb_obj_is_kind_of(a2, Ctime) == Qtrue) {
		TIME_STRUCT *time;

		Data_Get_Struct(a2, TIME_STRUCT, time);
		hh = INT2NUM(time->hour);
		mm = INT2NUM(time->minute);
		ss = INT2NUM(time->second);
	    } else {
		rb_raise(rb_eTypeError, "expecting ODBC::Time");
	    }
	} else {
	    hh = INT2FIX(0);
	    mm = INT2FIX(0);
	    ss = INT2FIX(0);
	}
	Data_Get_Struct(a1, DATE_STRUCT, date);
	y = INT2NUM(date->year);
	m = INT2NUM(date->month);
	d = INT2NUM(date->day);
	us = INT2FIX(0);
    }
    if (rb_obj_is_kind_of(a1, Ctime) == Qtrue) {
	TIME_STRUCT *time;

	if (a2 != Qnil) {
	    if (rb_obj_is_kind_of(a2, Cdate) == Qtrue) {
		DATE_STRUCT *date;

		Data_Get_Struct(a2, DATE_STRUCT, date);
		y = INT2NUM(date->year);
		m = INT2NUM(date->month);
		d = INT2NUM(date->day);
	    } else {
		rb_raise(rb_eTypeError, "expecting ODBC::Date");
	    }
	} else {
	    VALUE now = rb_funcall(rb_cTime, IDnow, 0, NULL);

	    y = rb_funcall(rb_cTime, IDyear, 1, now);
	    m = rb_funcall(rb_cTime, IDmonth, 1, now);
	    d = rb_funcall(rb_cTime, IDday, 1, now);
	}
	Data_Get_Struct(a1, TIME_STRUCT, time);
	hh = INT2NUM(time->hour);
	mm = INT2NUM(time->minute);
	ss = INT2NUM(time->second);
	us = INT2FIX(0);
mktime:
	return rb_funcall(rb_cTime, IDlocal, 7, y, m, d, hh, mm, ss, us);
    }
    if ((!once) && ((m = timestamp_load1(Ctimestamp, a1, -1)) != Qnil)) {
	a1 = m;
	once++;
	goto again;
    }
    if ((!once) && ((m = date_load1(Cdate, a1, -1)) != Qnil)) {
	a1 = m;
	if ((argc > 1) && ((m = time_load1(Ctime, a2, -1)) != Qnil)) {
	    a2 = m;
	}
	once++;
	goto again;
    }
    if ((!once) && ((m = time_load1(Ctime, a1, -1)) != Qnil)) {
	a1 = m;
	if ((argc > 1) && ((m = date_load1(Cdate, a2, -1)) != Qnil)) {
	    a2 = m;
	}
	once++;
	goto again;
    }
    rb_raise(rb_eTypeError,
	     "expecting ODBC::TimeStamp or ODBC::Date/Time or String");
    return Qnil;
}

static VALUE
mod_2date(VALUE self, VALUE arg)
{
    VALUE y, m, d;
    int once = 0;

again:
    if (rb_obj_is_kind_of(arg, Cdate) == Qtrue) {
	DATE_STRUCT *date;

	Data_Get_Struct(arg, DATE_STRUCT, date);
	y = INT2NUM(date->year);
	m = INT2NUM(date->month);
	d = INT2NUM(date->day);
	goto mkdate;
    }
    if (rb_obj_is_kind_of(arg, Ctimestamp) == Qtrue){
	TIMESTAMP_STRUCT *ts;

	Data_Get_Struct(arg, TIMESTAMP_STRUCT, ts);
	y = INT2NUM(ts->year);
	m = INT2NUM(ts->month);
	d = INT2NUM(ts->day);
mkdate:
	return rb_funcall(rb_cDate, IDnew, 3, y, m, d);
    }
    if ((!once) &&
	(((m = date_load1(Cdate, arg, -1)) != Qnil) ||
	 ((m = timestamp_load1(Ctimestamp, arg, -1)) != Qnil))) {
	arg = m;
	once++;
	goto again;
    }
    rb_raise(rb_eTypeError, "expecting ODBC::Date/Timestamp or String");
    return Qnil;
}

static VALUE
mod_trace(int argc, VALUE *argv, VALUE self)
{
    VALUE v = Qnil;

    rb_scan_args(argc, argv, "01", &v);
#ifdef TRACING
    if (argc > 0) {
	tracing = NUM2INT(v);
    }
    return INT2NUM(tracing);
#else
    return INT2NUM(0);
#endif
}

/*
 *----------------------------------------------------------------------
 *
 *      Table of constants and intern'ed string mappings.
 *
 *----------------------------------------------------------------------
 */

#define O_CONST(x)    { #x, x }
#define O_CONSTU(x)   { #x, SQL_UNKNOWN_TYPE }
#define O_CONST2(x,y) { #x, y }
#define O_CONST_END   { NULL, -1 }

static struct {
    const char *name;
    int value;
} o_const[] = {
    O_CONST(SQL_CURSOR_FORWARD_ONLY),
    O_CONST(SQL_CURSOR_KEYSET_DRIVEN),
    O_CONST(SQL_CURSOR_DYNAMIC),
    O_CONST(SQL_CURSOR_STATIC),
    O_CONST(SQL_CONCUR_READ_ONLY),
    O_CONST(SQL_CONCUR_LOCK),
    O_CONST(SQL_CONCUR_ROWVER),
    O_CONST(SQL_CONCUR_VALUES),
    O_CONST(SQL_FETCH_NEXT),
    O_CONST(SQL_FETCH_FIRST),
    O_CONST(SQL_FETCH_LAST),
    O_CONST(SQL_FETCH_PRIOR),
    O_CONST(SQL_FETCH_ABSOLUTE),
    O_CONST(SQL_FETCH_RELATIVE),
    O_CONST(SQL_UNKNOWN_TYPE),
    O_CONST(SQL_CHAR),
    O_CONST(SQL_NUMERIC),
    O_CONST(SQL_DECIMAL),
    O_CONST(SQL_INTEGER),
    O_CONST(SQL_SMALLINT),
    O_CONST(SQL_FLOAT),
    O_CONST(SQL_REAL),
    O_CONST(SQL_DOUBLE),
    O_CONST(SQL_VARCHAR),
#ifdef SQL_DATETIME
    O_CONST(SQL_DATETIME),
#else
    O_CONSTU(SQL_DATETIME),
#endif
#ifdef SQL_DATE
    O_CONST(SQL_DATE),
#else
    O_CONSTU(SQL_DATE),
#endif
#ifdef SQL_TYPE_DATE
    O_CONST(SQL_TYPE_DATE),
#else
    O_CONSTU(SQL_TYPE_DATE),
#endif
#ifdef SQL_TIME
    O_CONST(SQL_TIME),
#else
    O_CONSTU(SQL_TIME),
#endif
#ifdef SQL_TYPE_TIME
    O_CONST(SQL_TYPE_TIME),
#else
    O_CONSTU(SQL_TYPE_TIME),
#endif
#ifdef SQL_TIMESTAMP
    O_CONST(SQL_TIMESTAMP),
#else
    O_CONSTU(SQL_TIMESTAMP),
#endif
#ifdef SQL_TYPE_TIMESTAMP
    O_CONST(SQL_TYPE_TIMESTAMP),
#else
    O_CONSTU(SQL_TYPE_TIMESTAMP),
#endif
#ifdef SQL_LONGVARCHAR
    O_CONST(SQL_LONGVARCHAR),
#else
    O_CONSTU(SQL_LONGVARCHAR),
#endif
#ifdef SQL_BINARY
    O_CONST(SQL_BINARY),
#else
    O_CONSTU(SQL_BINARY),
#endif
#ifdef SQL_VARBINARY
    O_CONST(SQL_VARBINARY),
#else
    O_CONSTU(SQL_VARBINARY),
#endif
#ifdef SQL_LONGVARBINARY
    O_CONST(SQL_LONGVARBINARY),
#else
    O_CONSTU(SQL_LONGVARBINARY),
#endif
#ifdef SQL_BIGINT
    O_CONST(SQL_BIGINT),
#else
    O_CONSTU(SQL_BIGINT),
#endif
#ifdef SQL_TINYINT
    O_CONST(SQL_TINYINT),
#else
    O_CONSTU(SQL_TINYINT),
#endif
#ifdef SQL_BIT
    O_CONST(SQL_BIT),
#else
    O_CONSTU(SQL_BIT),
#endif
#ifdef SQL_GUID
    O_CONST(SQL_GUID),
#else
    O_CONSTU(SQL_GUID),
#endif
#ifdef SQL_WCHAR
    O_CONST(SQL_WCHAR),
#else
    O_CONSTU(SQL_WCHAR),
#endif
#ifdef SQL_WVARCHAR
    O_CONST(SQL_WVARCHAR),
#else
    O_CONSTU(SQL_WVARCHAR),
#endif
#ifdef SQL_WLONGVARCHAR
    O_CONST(SQL_WLONGVARCHAR),
#else
    O_CONSTU(SQL_WLONGVARCHAR),
#endif
#ifdef SQL_ATTR_ODBC_VERSION
    O_CONST(SQL_OV_ODBC2),
    O_CONST(SQL_OV_ODBC3),
#else
    O_CONST2(SQL_OV_ODBC2, 2),
    O_CONST2(SQL_OV_ODBC3, 3),
#endif
#ifdef SQL_ATTR_CONNECTION_POOLING
    O_CONST(SQL_CP_OFF),
    O_CONST(SQL_CP_ONE_PER_DRIVER),
    O_CONST(SQL_CP_ONE_PER_HENV),
    O_CONST(SQL_CP_DEFAULT),
#else
    O_CONST2(SQL_CP_OFF, 0),
    O_CONST2(SQL_CP_ONE_PER_DRIVER, 0),
    O_CONST2(SQL_CP_ONE_PER_HENV, 0),
    O_CONST2(SQL_CP_DEFAULT, 0),
#endif
#ifdef SQL_ATTR_CP_MATCH
    O_CONST(SQL_CP_STRICT_MATCH),
    O_CONST(SQL_CP_RELAXED_MATCH),
    O_CONST(SQL_CP_MATCH_DEFAULT),
#else
    O_CONST2(SQL_CP_STRICT_MATCH, 0),
    O_CONST2(SQL_CP_RELAXED_MATCH, 0),
    O_CONST2(SQL_CP_MATCH_DEFAULT, 0),
#endif
#ifdef SQL_SCOPE_CURROW
    O_CONST(SQL_SCOPE_CURROW),
#else
    O_CONST2(SQL_SCOPE_CURROW, 0),
#endif
#ifdef SQL_SCOPE_TRANSACTION
    O_CONST(SQL_SCOPE_TRANSACTION),
#else
    O_CONST2(SQL_SCOPE_TRANSACTION, 0),
#endif
#ifdef SQL_SCOPE_SESSION
    O_CONST(SQL_SCOPE_SESSION),
#else
    O_CONST2(SQL_SCOPE_SESSION, 0),
#endif
#ifdef SQL_BEST_ROWID
    O_CONST(SQL_BEST_ROWID),
#else
    O_CONST2(SQL_BEST_ROWID, 0),
#endif
#ifdef SQL_ROWVER
    O_CONST(SQL_ROWVER),
#else
    O_CONST2(SQL_ROWVER, 0),
#endif
    O_CONST(SQL_PARAM_TYPE_UNKNOWN),
    O_CONST(SQL_PARAM_INPUT),
    O_CONST(SQL_PARAM_OUTPUT),
    O_CONST(SQL_PARAM_INPUT_OUTPUT),
    O_CONST(SQL_DEFAULT_PARAM),
    O_CONST(SQL_RETURN_VALUE),
    O_CONST(SQL_RESULT_COL),
    O_CONST(SQL_PT_UNKNOWN),
    O_CONST(SQL_PT_PROCEDURE),
    O_CONST(SQL_PT_FUNCTION),

    /* end of table */
    O_CONST_END
};

static struct {
    ID *idp;
    const char *str;
} ids[] = {
    { &IDstart, "start" },
    { &IDatatinfo, "@@info" },
    { &IDataterror, "@@error" },
    { &IDkeys, "keys" },
    { &IDatattrs, "@attrs" },
    { &IDday, "day" },
    { &IDmonth, "month" },
    { &IDyear, "year" },
    { &IDmday, "mday" },
    { &IDnsec, "nsec" },
    { &IDusec, "usec" },
    { &IDsec, "sec" },
    { &IDmin, "min" },
    { &IDhour, "hour" },
    { &IDusec, "usec" },
    { &IDkeyp, "key?" },
    { &IDkey, "key" },
    { &IDSymbol, "Symbol" },
    { &IDString, "String" },
    { &IDFixnum, "Fixnum" },
    { &IDtable_names, "table_names" },
    { &IDnew, "new" },
    { &IDnow, "now" },
    { &IDlocal, "local" },
    { &IDname, "name" },
    { &IDtable, "table" },
    { &IDtype, "type" },
    { &IDlength, "length" },
    { &IDnullable, "nullable" },
    { &IDscale, "scale" },
    { &IDprecision, "precision" },
    { &IDsearchable, "searchable" },
    { &IDunsigned, "unsigned" },
    { &IDiotype, "iotype" },
    { &IDoutput_size, "output_size" },
    { &IDoutput_type, "output_type" },
    { &IDdescr, "descr" },
    { &IDstatement, "statement" },
    { &IDreturn_output_param, "return_output_param" },
    { &IDattrs, "attrs" },
    { &IDNULL, "NULL" },
    { &IDdefault, "default" },
#ifdef USE_RB_ENC
    { &IDencode, "encode" },
#endif
    { &IDparse, "parse" },
    { &IDutc, "utc" },
    { &IDlocal, "local" },
    { &IDto_s, "to_s" }
};

/*
 *----------------------------------------------------------------------
 *
 *      Module initializer.
 *
 *----------------------------------------------------------------------
 */

void
#ifdef UNICODE
Init_odbc_utf8()
#else
Init_odbc()
#endif
{
    int i;
    const char *modname = "ODBC";
    ID modid = rb_intern(modname);
    VALUE v = Qnil;

    rb_require("date");
    rb_cDate = rb_eval_string("Date");

    if (rb_const_defined(rb_cObject, modid)) {
	v = rb_const_get(rb_cObject, modid); 
	if (TYPE(v) != T_MODULE) {
	    rb_raise(rb_eTypeError, "%s already defined", modname);
	}
    }
    if (v != Qnil) {
#ifdef UNICODE
	modname = "ODBC_UTF8";
#else
	modname = "ODBC_NONE";
#endif
    }

    for (i = 0; i < (int) (sizeof (ids) / sizeof (ids[0])); i++) {
	*(ids[i].idp) = rb_intern(ids[i].str);
    }

    Modbc = rb_define_module(modname);
    Cobj = rb_define_class_under(Modbc, "Object", rb_cObject);
    rb_define_class_variable(Cobj, "@@error", Qnil);
    rb_define_class_variable(Cobj, "@@info", Qnil);

    Cenv = rb_define_class_under(Modbc, "Environment", Cobj);
    Cdbc = rb_define_class_under(Modbc, "Database", Cenv);
    Cstmt = rb_define_class_under(Modbc, "Statement", Cdbc);
    rb_include_module(Cstmt, rb_mEnumerable);

    Ccolumn = rb_define_class_under(Modbc, "Column", Cobj);
    rb_attr(Ccolumn, IDname, 1, 0, Qfalse);
    rb_attr(Ccolumn, IDtable, 1, 0, Qfalse);
    rb_attr(Ccolumn, IDtype, 1, 0, Qfalse);
    rb_attr(Ccolumn, IDlength, 1, 0, Qfalse);
    rb_attr(Ccolumn, IDnullable, 1, 0, Qfalse);
    rb_attr(Ccolumn, IDscale, 1, 0, Qfalse);
    rb_attr(Ccolumn, IDprecision, 1, 0, Qfalse);
    rb_attr(Ccolumn, IDsearchable, 1, 0, Qfalse);
    rb_attr(Ccolumn, IDunsigned, 1, 0, Qfalse);

    Cparam = rb_define_class_under(Modbc, "Parameter", Cobj);
    rb_attr(Cparam, IDtype, 1, 0, Qfalse);
    rb_attr(Cparam, IDprecision, 1, 0, Qfalse);
    rb_attr(Cparam, IDscale, 1, 0, Qfalse);
    rb_attr(Cparam, IDnullable, 1, 0, Qfalse);
    rb_attr(Cparam, IDiotype, 1, 0, Qfalse);
    rb_attr(Cparam, IDoutput_size, 1, 0, Qfalse);
    rb_attr(Cparam, IDoutput_type, 1, 0, Qfalse);

    Cdsn = rb_define_class_under(Modbc, "DSN", Cobj);
    rb_attr(Cdsn, IDname, 1, 1, Qfalse);
    rb_attr(Cdsn, IDdescr, 1, 1, Qfalse);

    Cdrv = rb_define_class_under(Modbc, "Driver", Cobj);
    rb_attr(Cdrv, IDname, 1, 1, Qfalse);
    rb_attr(Cdrv, IDattrs, 1, 1, Qfalse);

    Cerror = rb_define_class_under(Modbc, "Error", rb_eStandardError);

    Cproc = rb_define_class("ODBCProc", rb_cProc);

    Cdate = rb_define_class_under(Modbc, "Date", Cobj);
    rb_include_module(Cdate, rb_mComparable);
    Ctime = rb_define_class_under(Modbc, "Time", Cobj);
    rb_include_module(Ctime, rb_mComparable);
    Ctimestamp = rb_define_class_under(Modbc, "TimeStamp", Cobj);
    rb_include_module(Ctimestamp, rb_mComparable);

    /* module functions */
    rb_define_module_function(Modbc, "trace", mod_trace, -1);
    rb_define_module_function(Modbc, "trace=", mod_trace, -1);
    rb_define_module_function(Modbc, "connect", mod_connect, -1);
    rb_define_module_function(Modbc, "datasources", dbc_dsns, 0);
    rb_define_module_function(Modbc, "drivers", dbc_drivers, 0);
    rb_define_module_function(Modbc, "error", dbc_error, 0);
    rb_define_module_function(Modbc, "info", dbc_warn, 0);
    rb_define_module_function(Modbc, "clear_error", dbc_clrerror, 0);
    rb_define_module_function(Modbc, "newenv", env_new, 0);
    rb_define_module_function(Modbc, "to_time", mod_2time, -1);
    rb_define_module_function(Modbc, "to_date", mod_2date, 1);
    rb_define_module_function(Modbc, "connection_pooling", env_cpooling, -1);
    rb_define_module_function(Modbc, "connection_pooling=", env_cpooling, -1);
    rb_define_module_function(Modbc, "raise", dbc_raise, 1);

    /* singleton methods and constructors */
    rb_define_singleton_method(Cobj, "error", dbc_error, 0);
    rb_define_singleton_method(Cobj, "info", dbc_warn, 0);
    rb_define_singleton_method(Cobj, "clear_error", dbc_clrerror, 0);
    rb_define_singleton_method(Cobj, "raise", dbc_raise, 1);
    rb_define_alloc_func(Cenv, env_new);
    rb_define_singleton_method(Cenv, "connect", dbc_new, -1);
#ifdef HAVE_RB_DEFINE_ALLOC_FUNC
    rb_define_alloc_func(Cdbc, dbc_alloc);
#else
    rb_define_alloc_func(Cdbc, dbc_new);
    rb_define_alloc_func(Cdsn, dsn_new);
    rb_define_alloc_func(Cdrv, drv_new);
#endif
    rb_define_method(Cdsn, "initialize", dsn_init, 0);
    rb_define_method(Cdrv, "initialize", drv_init, 0);
    rb_define_method(Cdbc, "newstmt", stmt_new, 0);

    /* common (Cobj) methods */
    rb_define_method(Cobj, "error", dbc_error, 0);
    rb_define_method(Cobj, "info", dbc_warn, 0);
    rb_define_method(Cobj, "clear_error", dbc_clrerror, 0);
    rb_define_method(Cobj, "raise", dbc_raise, 1);

    /* common (Cenv) methods */
    rb_define_method(Cenv, "connect", dbc_new, -1);
    rb_define_method(Cenv, "environment", env_of, 0);
    rb_define_method(Cenv, "transaction", dbc_transaction, 0);
    rb_define_method(Cenv, "commit", dbc_commit, 0);
    rb_define_method(Cenv, "rollback", dbc_rollback, 0);
    rb_define_method(Cenv, "connection_pooling", env_cpooling, -1);
    rb_define_method(Cenv, "connection_pooling=", env_cpooling, -1);
    rb_define_method(Cenv, "cp_match", env_cpmatch, -1);
    rb_define_method(Cenv, "cp_match=", env_cpmatch, -1);
    rb_define_method(Cenv, "odbc_version", env_odbcver, -1);
    rb_define_method(Cenv, "odbc_version=", env_odbcver, -1);

    /* management things (odbcinst.h) */
    rb_define_module_function(Modbc, "add_dsn", dbc_adddsn, -1);
    rb_define_module_function(Modbc, "config_dsn", dbc_confdsn, -1);
    rb_define_module_function(Modbc, "del_dsn", dbc_deldsn, -1);
    rb_define_module_function(Modbc, "write_file_dsn", dbc_wfdsn, -1);
    rb_define_module_function(Modbc, "read_file_dsn", dbc_rfdsn, -1);

    /* connection (database) methods */
    rb_define_method(Cdbc, "initialize", dbc_connect, -1);
    rb_define_method(Cdbc, "connect", dbc_connect, -1);
    rb_define_method(Cdbc, "connected?", dbc_connected, 0);
    rb_define_method(Cdbc, "drvconnect", dbc_drvconnect, 1);
    rb_define_method(Cdbc, "drop_all", dbc_dropall, 0);
    rb_define_method(Cdbc, "disconnect", dbc_disconnect, -1);
    rb_define_method(Cdbc, "tables", dbc_tables, -1);
    rb_define_method(Cdbc, "columns", dbc_columns, -1);
    rb_define_method(Cdbc, "primary_keys", dbc_primkeys, -1);
    rb_define_method(Cdbc, "indexes", dbc_indexes, -1);
    rb_define_method(Cdbc, "types", dbc_types, -1);
    rb_define_method(Cdbc, "foreign_keys", dbc_forkeys, -1);
    rb_define_method(Cdbc, "table_privileges", dbc_tpriv, -1);
    rb_define_method(Cdbc, "procedures", dbc_procs, -1);
    rb_define_method(Cdbc, "procedure_columns", dbc_proccols, -1);
    rb_define_method(Cdbc, "special_columns", dbc_speccols, -1);
    rb_define_method(Cdbc, "get_info", dbc_getinfo, -1);
    rb_define_method(Cdbc, "prepare", stmt_prep, -1);
    rb_define_method(Cdbc, "run", stmt_run, -1);
    rb_define_method(Cdbc, "do", stmt_do, -1);
    rb_define_method(Cdbc, "proc", stmt_proc, -1);
    rb_define_method(Cdbc, "use_time", dbc_timefmt, -1);
    rb_define_method(Cdbc, "use_time=", dbc_timefmt, -1);
    rb_define_method(Cdbc, "use_utc", dbc_timeutc, -1);
    rb_define_method(Cdbc, "use_utc=", dbc_timeutc, -1);

    /* connection options */
    rb_define_method(Cdbc, "get_option", dbc_getsetoption, -1);
    rb_define_method(Cdbc, "set_option", dbc_getsetoption, -1);
    rb_define_method(Cdbc, "autocommit", dbc_autocommit, -1);
    rb_define_method(Cdbc, "autocommit=", dbc_autocommit, -1);
    rb_define_method(Cdbc, "concurrency", dbc_concurrency, -1);
    rb_define_method(Cdbc, "concurrency=", dbc_concurrency, -1);
    rb_define_method(Cdbc, "maxrows", dbc_maxrows, -1);
    rb_define_method(Cdbc, "maxrows=", dbc_maxrows, -1);
    rb_define_method(Cdbc, "timeout", dbc_timeout, -1);
    rb_define_method(Cdbc, "timeout=", dbc_timeout, -1);
    rb_define_method(Cdbc, "maxlength", dbc_maxlength, -1);
    rb_define_method(Cdbc, "maxlength=", dbc_maxlength, -1);
    rb_define_method(Cdbc, "rowsetsize", dbc_rowsetsize, -1);
    rb_define_method(Cdbc, "cursortype", dbc_cursortype, -1);
    rb_define_method(Cdbc, "cursortype=", dbc_cursortype, -1);
    rb_define_method(Cdbc, "noscan", dbc_noscan, -1);
    rb_define_method(Cdbc, "noscan=", dbc_noscan, -1);
    rb_define_method(Cdbc, "ignorecase", stmt_ignorecase, -1);
    rb_define_method(Cdbc, "ignorecase=", stmt_ignorecase, -1);

    /* statement methods */
    rb_define_method(Cstmt, "drop", stmt_drop, 0);
    rb_define_method(Cstmt, "close", stmt_close, 0);
    rb_define_method(Cstmt, "cancel", stmt_cancel, 0);
    rb_define_method(Cstmt, "column", stmt_column, -1);
    rb_define_method(Cstmt, "columns", stmt_columns, -1);
    rb_define_method(Cstmt, "parameter", stmt_param, -1);
    rb_define_method(Cstmt, "parameters", stmt_params, 0);
    rb_define_method(Cstmt, "param_type", stmt_param_type, -1);
    rb_define_method(Cstmt, "param_iotype", stmt_param_iotype, -1);
    rb_define_method(Cstmt, "param_output_size", stmt_param_output_size, -1);
    rb_define_method(Cstmt, "param_output_type", stmt_param_output_type, -1);
    rb_define_method(Cstmt, "param_output_value", stmt_param_output_value, -1);
    rb_define_method(Cstmt, "ncols", stmt_ncols, 0);
    rb_define_method(Cstmt, "nrows", stmt_nrows, 0);
    rb_define_method(Cstmt, "nparams", stmt_nparams, 0);
    rb_define_method(Cstmt, "cursorname", stmt_cursorname, -1);
    rb_define_method(Cstmt, "cursorname=", stmt_cursorname, -1);
    rb_define_method(Cstmt, "fetch", stmt_fetch, 0);
    rb_define_method(Cstmt, "fetch!", stmt_fetch_bang, 0);
    rb_define_method(Cstmt, "fetch_first", stmt_fetch_first, 0);
    rb_define_method(Cstmt, "fetch_first!", stmt_fetch_first_bang, 0);
    rb_define_method(Cstmt, "fetch_scroll", stmt_fetch_scroll, -1);
    rb_define_method(Cstmt, "fetch_scroll!", stmt_fetch_scroll_bang, -1);
    rb_define_method(Cstmt, "fetch_hash", stmt_fetch_hash, -1);
    rb_define_method(Cstmt, "fetch_hash!", stmt_fetch_hash_bang, -1);
    rb_define_method(Cstmt, "fetch_first_hash", stmt_fetch_first_hash, 0);
    rb_define_method(Cstmt, "fetch_many", stmt_fetch_many, 1);
    rb_define_method(Cstmt, "fetch_all", stmt_fetch_all, 0);
    rb_define_method(Cstmt, "each", stmt_each, 0);
    rb_define_method(Cstmt, "each_hash", stmt_each_hash, -1);
    rb_define_method(Cstmt, "execute", stmt_exec, -1);
    rb_define_method(Cstmt, "make_proc", stmt_procwrap, -1);
    rb_define_method(Cstmt, "more_results", stmt_more_results, 0);
    rb_define_method(Cstmt, "prepare", stmt_prep, -1);
    rb_define_method(Cstmt, "run", stmt_run, -1);
    rb_define_singleton_method(Cstmt, "make_proc", stmt_procwrap, -1);

    /* statement options */
    rb_define_method(Cstmt, "get_option", stmt_getsetoption, -1);
    rb_define_method(Cstmt, "set_option", stmt_getsetoption, -1);
    rb_define_method(Cstmt, "concurrency", stmt_concurrency, -1);
    rb_define_method(Cstmt, "concurrency=", stmt_concurrency, -1);
    rb_define_method(Cstmt, "maxrows", stmt_maxrows, -1);
    rb_define_method(Cstmt, "maxrows=", stmt_maxrows, -1);
    rb_define_method(Cstmt, "timeout", stmt_timeout, -1);
    rb_define_method(Cstmt, "timeout=", stmt_timeout, -1);
    rb_define_method(Cstmt, "maxlength", stmt_maxlength, -1);
    rb_define_method(Cstmt, "maxlength=", stmt_maxlength, -1);
    rb_define_method(Cstmt, "cursortype", stmt_cursortype, -1);
    rb_define_method(Cstmt, "cursortype=", stmt_cursortype, -1);
    rb_define_method(Cstmt, "noscan", stmt_noscan, -1);
    rb_define_method(Cstmt, "noscan=", stmt_noscan, -1);
    rb_define_method(Cstmt, "rowsetsize", stmt_rowsetsize, -1);

    /* data type methods */
#ifdef HAVE_RB_DEFINE_ALLOC_FUNC
    rb_define_alloc_func(Cdate, date_alloc);
#else
    rb_define_singleton_method(Cdate, "new", date_new, -1);
#endif
    rb_define_singleton_method(Cdate, "_load", date_load, 1);
    rb_define_method(Cdate, "initialize", date_init, -1);
    rb_define_method(Cdate, "clone", date_clone, 0);
    rb_define_method(Cdate, "to_s", date_to_s, 0);
    rb_define_method(Cdate, "_dump", date_dump, 1);
    rb_define_method(Cdate, "inspect", date_inspect, 0);
    rb_define_method(Cdate, "year", date_year, -1);
    rb_define_method(Cdate, "month", date_month, -1);
    rb_define_method(Cdate, "day", date_day, -1);
    rb_define_method(Cdate, "year=", date_year, -1);
    rb_define_method(Cdate, "month=", date_month, -1);
    rb_define_method(Cdate, "day=", date_day, -1);
    rb_define_method(Cdate, "<=>", date_cmp, 1);

#ifdef HAVE_RB_DEFINE_ALLOC_FUNC
    rb_define_alloc_func(Ctime, time_alloc);
#else
    rb_define_singleton_method(Ctime, "new", time_new, -1);
#endif
    rb_define_singleton_method(Ctime, "_load", time_load, 1);
    rb_define_method(Ctime, "initialize", time_init, -1);
    rb_define_method(Ctime, "clone", time_clone, 0);
    rb_define_method(Ctime, "to_s", time_to_s, 0);
    rb_define_method(Ctime, "_dump", time_dump, 1);
    rb_define_method(Ctime, "inspect", time_inspect, 0);
    rb_define_method(Ctime, "hour", time_hour, -1);
    rb_define_method(Ctime, "minute", time_min, -1);
    rb_define_method(Ctime, "second", time_sec, -1);
    rb_define_method(Ctime, "hour=", time_hour, -1);
    rb_define_method(Ctime, "minute=", time_min, -1);
    rb_define_method(Ctime, "second=", time_sec, -1);
    rb_define_method(Ctime, "<=>", time_cmp, 1);

#ifdef HAVE_RB_DEFINE_ALLOC_FUNC
    rb_define_alloc_func(Ctimestamp, timestamp_alloc);
#else
    rb_define_singleton_method(Ctimestamp, "new", timestamp_new, -1);
#endif
    rb_define_singleton_method(Ctimestamp, "_load", timestamp_load, 1);
    rb_define_method(Ctimestamp, "initialize", timestamp_init, -1);
    rb_define_method(Ctimestamp, "clone", timestamp_clone, 0);
    rb_define_method(Ctimestamp, "to_s", timestamp_to_s, 0);
    rb_define_method(Ctimestamp, "_dump", timestamp_dump, 1);
    rb_define_method(Ctimestamp, "inspect", timestamp_inspect, 0);
    rb_define_method(Ctimestamp, "year", timestamp_year, -1);
    rb_define_method(Ctimestamp, "month", timestamp_month, -1);
    rb_define_method(Ctimestamp, "day", timestamp_day, -1);
    rb_define_method(Ctimestamp, "hour", timestamp_hour, -1);
    rb_define_method(Ctimestamp, "minute", timestamp_min, -1);
    rb_define_method(Ctimestamp, "second", timestamp_sec, -1);
    rb_define_method(Ctimestamp, "fraction", timestamp_fraction, -1);
    rb_define_method(Ctimestamp, "year=", timestamp_year, -1);
    rb_define_method(Ctimestamp, "month=", timestamp_month, -1);
    rb_define_method(Ctimestamp, "day=", timestamp_day, -1);
    rb_define_method(Ctimestamp, "hour=", timestamp_hour, -1);
    rb_define_method(Ctimestamp, "minute=", timestamp_min, -1);
    rb_define_method(Ctimestamp, "second=", timestamp_sec, -1);
    rb_define_method(Ctimestamp, "fraction=", timestamp_fraction, -1);
    rb_define_method(Ctimestamp, "<=>", timestamp_cmp, 1);

    /* procedure methods */
    rb_define_method(Cproc, "initialize", stmt_proc_init, -1);
    rb_define_method(Cproc, "call", stmt_proc_call, -1);
    rb_define_method(Cproc, "[]", stmt_proc_call, -1);
    rb_attr(Cproc, IDstatement, 1, 0, Qfalse);
    rb_attr(Cproc, IDreturn_output_param, 1, 0, Qfalse);
#ifndef HAVE_RB_DEFINE_ALLOC_FUNC
    rb_enable_super(Cproc, "call");
    rb_enable_super(Cproc, "[]");
#endif

    /* constants */
    for (i = 0; o_const[i].name != NULL; i++) {
	rb_define_const(Modbc, o_const[i].name,
			INT2NUM(o_const[i].value));
    }
    for (i = 0; get_info_map[i].name != NULL; i++) {
	rb_define_const(Modbc, get_info_map[i].name,
			INT2NUM(get_info_map[i].info));
    }
    for (i = 0; get_info_bitmap[i].name != NULL; i++) {
	rb_define_const(Modbc, get_info_bitmap[i].name,
			INT2NUM(get_info_bitmap[i].bits));
    }
    for (i = 0; option_map[i].name != NULL; i++) {
	rb_define_const(Modbc, option_map[i].name,
			INT2NUM(option_map[i].option));
    }

#ifdef UNICODE
    rb_define_const(Modbc, "UTF8", Qtrue);
#ifdef USE_RB_ENC
    rb_enc = rb_utf8_encoding();
    rb_encv = rb_enc_from_encoding(rb_enc);
#endif
#else
    rb_define_const(Modbc, "UTF8", Qfalse);
#endif

#ifdef TRACING
    if (ruby_verbose) {
	tracing = -1;
    }
#endif
}
