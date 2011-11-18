#!/usr/bin/env ruby
#
# cqgen.rb - Poor man's C code generator to automatically
#            make access wrappers for ODBC datasources.
#
# -------------------------------------------------------------------
# This file is part of "ODBC-Ruby binding"
#
# Copyright (c) 2006 Christian Werner <chw@ch-werner.de>
#
# See the file "COPYING" for information on usage
# and redistribution of this file and for a
# DISCLAIMER OF ALL WARRANTIES.
# -------------------------------------------------------------------
#
# Pre-requisites:
#
#   * Ruby 1.6 or 1.8
#   * Ruby/ODBC >= 0.996
#   * Acesss to a ODBC datasource with properly
#     loaded schema for the wrappers to generate
#
# -------------------------------------------------------------------
#
# Functions in this module
#
#  cqgen DSName, UID, PWD, CFName, SQL, [ParmTypes, ParmNames]
#
#    DSName:    name of datasource to connect
#    UID:       user name on datasource
#    PWD:       password on datasource
#    CFName:    name for C function
#    SQL:       SQL text of one query yielding
#               one result set
#    ParmTypes: (optional, array) types for
#               query parameters
#    ParmNames: (optional, array) names for
#               query parameters
#
#    use this function to make C code for
#    SELECT/UPDATE/DELETE/INSERT SQL statements.
#    The "ParmTypes" is useful since many ODBC drivers
#    cannot return the proper types in SQLDescribeParam().
#
#  cqgen_test
#    append test functions and skeleton test main()
#    to end of C file
#
#
#  cqgen_output c_name, h_name
#    write C source code, 'c_name' for functions,
#    'h_name' for optional header file
#
# -------------------------------------------------------------------
#
# Specification file sample
#
# ---BOF---
#  require 'cqgen'
#  # simple query
#  cqgen 'bla', '', '', "GetResults", %q{
#    select * from results
#  }
#  # query with parameters using last DSN
#  cqgen nil, nil, nil, "GetResultsGt", %q{
#    select * from results where funfactor > ?
#  }
#  # update with parameters using other DSN
#  cqgen 'bla2', '', '', "IncrFunFactor", %q{
#    update results set funfactor = ? where funfactor < ?
#  }, [ ODBC::SQL_INTEGER, ODBC::SQL_INTEGER ],
#     [ "newfunfactor", "maxfunfactor" ]
#  cqgen_test
#  cqgen_output 'bla_wrap.c'
# ---EOF---
#
# Run this file with a command line like
#
#  $ ruby sample-file
#  $ cc -c bla_wrap.c
#  or
#  $ cc -o bla_wrap_test bla_wrap.c -DTEST_cqgen_all -lodbc
#
# Depending on the SQL text, cqgen writes one or more
# C functions and zero or one structure typedefs.
# The structure typedefs are merged when possible,
# thus you should write the least specific queries
# on top of the specification file.
#
# In the sample:
#
#  * function GetResults_init to initiate the query
#  * function GetResults to retrieve one result row
#  * function GetResults_deinit to release resources
#    associated with the query
#  * struct RESULT_GetResults for representation of
#    one result row
#
#  * function GetResultsGt_init with one parameter par1
#  * macro GetResultsGt aliased to GetResults
#    (function reused from 1st query)
#  * macro GetResultsGt_deinit aliased to GetResults_deinit
#    (function reused from 1st query)
#  * struct RESULT_GetResults for representation of
#    one result row (struct name reused from 1st query)
#
#  * function IncrFunFactor with two parameters named
#    "newfunfactor" and "maxfunfactor" and an SQLINTEGER
#    reference for the number of affected rows
#
# -------------------------------------------------------------------

require 'odbc'

$cqgen_dsn=nil
$cqgen_uid=nil
$cqgen_pwd=nil
$cqgen_simple_fns=Hash.new
$cqgen_stypes=Hash.new
$cqgen_h1=''
$cqgen_h2=''
$cqgen_c=''
$cqgen_ct1=''
$cqgen_ct2=''

def cqgen(dsn, uid, pwd, name, sql, parmtypes=nil, parmnames=nil)
  dsn=$cqgen_dsn if dsn.nil?
  uid=$cqgen_uid if uid.nil?
  pwd=$cqgen_pwd if pwd.nil?
  begin
    conn=ODBC::connect(dsn, uid, pwd)
    $cqgen_dsn=dsn
    $cqgen_uid=uid
    $cqgen_pwd=pwd
  rescue Exception => err
    $stderr.puts("connect for #{name} failed:\n")
    $stderr.puts("\t#{err}\n")
    return
  end
  sql = sql.strip
  sql = sql.gsub(/[\r\n]+/, " ")
  if sql.downcase =~ /where/
    sql0=sql + " and 1=0"
  else
    sql0=sql + " where 1=0"
  end
  begin
    stmt=conn.prepare(sql0)
    if stmt.ncols < 1
      stmt.execute
    end
  rescue Exception => err
    $stderr.puts("SQL error for #{name}:\n")
    $stderr.puts("\t#{err}\n")
    return
  end
  if stmt.ncols > 0
    fhead="SQLRETURN\n" + name + "_init(SQLHDBC hdbc, SQLHSTMT *hstmtRet"
    fhead2="SQLRETURN\n" + name + "(SQLHSTMT hstmt"
    flvars2="{\n    SQLRETURN ret;\n    INTERNAL_STMT *istmt;\n"
    fbody2="\n    istmt = (INTERNAL_STMT *) hstmt;\n"
    fhead3="SQLRETURN\n" + name + "_deinit(SQLHSTMT hstmt)\n"
    flvars3="{\n    INTERNAL_STMT *istmt;\n"
    fbody3="\n    istmt = (INTERNAL_STMT *) hstmt;\n"
    fbody3+="    if (istmt != NULL) {\n"
    fbody3+="\tSQLFreeStmt(istmt->hstmt, SQL_DROP);\n"
    fbody3+="\tfree(istmt);\n"
    fbody3+="    }\n    return SQL_SUCCESS;\n}\n\n"
  else
    fhead="SQLRETURN\n" + name + "(SQLHDBC hdbc"
    fhead2=""
    flvars2=""
    fbody2=""
    fhead3=""
    flvars3=""
    fbody3=""
  end
  flvars="{\n    SQLRETURN ret;\n"
  if stmt.ncols < 1
    flvars+="    SQLHSTMT hstmt;\n"
  end
  fbody=""
  flvarst="{\n    SQLRETURN ret;\n"
  fheadt="SQLRETURN\n" + name + "_test(SQLHDBC hdbc"
  fbodyt=""
  if stmt.ncols > 0
    flvars+="    INTERNAL_STMT *istmt;\n    SQLHSTMT hstmt;\n"
    fbody+="    *hstmtRet = hstmt = SQL_NULL_HSTMT;\n"
    fbody+="    ret = SQLAllocStmt(hdbc, &hstmt);\n"
    fbody+="    if (!SQL_SUCCEEDED(ret)) {\n\treturn ret;\n    }\n"
    fbody+="    istmt = malloc(sizeof (INTERNAL_STMT));\n"
    fbody+="    if (istmt == NULL) {\n\tSQLFreeStmt(hstmt, SQL_DROP);\n"
    fbody+="\treturn SQL_ERROR;\n    }\n"
    fbody+="    istmt->hstmt = hstmt;\n"
    fbody+="    istmt->result = NULL;\n"
    fbody+="    *hstmtRet = (SQLHSTMT) istmt;\n"
    flvarst+="    SQLHSTMT hstmt;\n"
    fbodyt+="    ret = #{name}_init(hdbc, &hstmt"
  else
    fbodyt+="    ret = #{name}(hdbc"
  end
  pnum=1
  stmt.parameters.each do |par|
    if !parmtypes.nil? 
      ptype=parmtypes[pnum-1]
    end
    if ptype.nil?
      ptype = par.type
    end
    if !parmnames.nil?
      pname=parmnames[pnum-1]
    end
    if pname.nil?
      pname="par#{pnum}"
    end
    case ptype
    when ODBC::SQL_INTEGER
      pdef=", SQLINTEGER *#{pname}"
      fbody+="    #{pname}_len = sizeof (SQLINTEGER);\n";
      fbody+="    ret = SQLBindParameter(hstmt, #{pnum}, SQL_PARAM_INPUT,"
      fbody+=" SQL_C_LONG, SQL_INTEGER, #{par.precision}, #{par.scale},"
    when ODBC::SQL_SMALLINT
      pdef=", SQLSMALLINT *#{pname}"
      fbody+="    #{pname}_len = sizeof (SQLSMALLINT);\n";
      fbody+="    ret = SQLBindParameter(hstmt, #{pnum}, SQL_PARAM_INPUT,"
      fbody+=" SQL_C_SHORT, SQL_SMALLINT, #{par.precision}, #{par.scale},"
    when ODBC::SQL_FLOAT
      pdef=", SQLDOUBLE *#{pname}"
      fbody+="    #{pname}_len = sizeof (SQLDOUBLE);\n";
      fbody+="    ret = SQLBindParameter(hstmt, #{pnum}, SQL_PARAM_INPUT,"
      fbody+=" SQL_C_DOUBLE, SQL_FLOAT, #{par.precision}, #{par.scale},"
    when ODBC::SQL_DOUBLE
      pdef=", SQLDOUBLE *#{pname}"
      fbody+="    #{pname}_len = sizeof (SQLDOUBLE);\n";
      fbody+="    ret = SQLBindParameter(hstmt, #{pnum}, SQL_PARAM_INPUT,"
      fbody+=" SQL_C_DOUBLE, SQL_DOUBLE, #{par.precision}, #{par.scale},"
    else
      pdef=", SQLCHAR *#{pname}"
      fbody+="    #{pname}_len = #{pname} ? strlen(#{pname}) : SQL_NULL_DATA;\n";
      fbody+="    ret = SQLBindParameter(hstmt, #{pnum}, SQL_PARAM_INPUT,"
      fbody+=" SQL_C_CHAR, SQL_CHAR, #{par.precision}, #{par.scale},"
    end
    fbody+=" #{pname}, #{pname}_len, &#{pname}_len);\n"
    fhead+=pdef
    fheadt+=pdef
    fbodyt+=", #{pname}"
    flvars+="    SQLINTEGER #{pname}_len;\n"
    if stmt.ncols > 0
      fbody+="    if (!SQL_SUCCEEDED(ret)) {\n\treturn ret;\n    }\n"
    else
      fbody+="    if (!SQL_SUCCEEDED(ret)) {\n\tgoto done;\n    }\n"
    end
    pnum+=1
  end
  fheadt+=")\n"
  if stmt.ncols < 1
    fbodyt+= ", &rowCount"
  end
  fbodyt+=");\n"
  if stmt.ncols > 0
    fbodyt+="    if (!SQL_SUCCEEDED(ret)) {\n"
    fbodyt+="\t#{name}_deinit(hstmt);\n\treturn ret;\n    }\n"
  end
  struc=""
  istruc=""
  if stmt.ncols > 0
    if pnum<=1
      $cqgen_simple_fns["#{name}_test"]=1
    end
    fbody+="    ret = SQLPrepare(hstmt, \"#{sql}\", SQL_NTS);\n"
    fbody+="    if (!SQL_SUCCEEDED(ret)) {\n\treturn ret;\n    }\n"
    fbody+="    ret = SQLExecute(hstmt);\n"
    fbody+="    return ret;\n}\n\n"
    struc+="typedef struct {\n"
    fbody2+="    hstmt = istmt->hstmt;\n"
    fbody2+="    if (result == istmt->result) {\n"
    fbody2+="\tgoto doexec;\n    }\n"
    fbody2+="    istmt->result = NULL;\n"
    fbodyt+="    do {\n"
    fbodyt+="\tret = #{name}(hstmt, &result);\n"
    fbodyt+="\tif (!SQL_SUCCEEDED(ret)) {\n\t    break;\n\t}\n"
    cols = stmt.columns(1)
    coltable = Hash.new
    cols.each do |col|
      coltable[col.table => 1]
    end
    cnum=1
    cols.each do |col|
      if coltable.size > 1
         cname=col.table+"_"+col.name
      else
         cname=col.name
      end
      case col.type
      when ODBC::SQL_INTEGER
        istruc+="    SQLLEN #{cname}_len;\n"
        istruc+="    SQLINTEGER #{cname};\n"
        fbody2+="    ret = SQLBindCol(hstmt, #{cnum}, SQL_C_LONG,"
        fbody2+=" &result->#{cname}, sizeof (SQL_C_LONG),"
        fbody2+=" &result->#{cname}_len);\n"
      when ODBC::SQL_SMALLINT
        istruc+="    SQLLEN #{cname}_len;\n"
        istruc+="    SQLSMALLINT #{cname};\n"
        fbody2+="    ret = SQLBindCol(hstmt, #{cnum}, SQL_C_SMALLINT,"
        fbody2+=" &result->#{cname}, sizeof (SQL_C_SMALLINT),"
        fbody2+=" &result->#{cname}_len);\n"
      when ODBC::SQL_FLOAT
        istruc+="    SQLLEN #{cname}_len;\n"
        istruc+="    SQLDOUBLE #{cname};\n"
        fbody2+="    ret = SQLBindCol(hstmt, #{cnum}, SQL_C_DOUBLE,"
        fbody2+=" &result->#{cname}, sizeof (SQLDOUBLE),"
        fbody2+=" &result->#{cname}_len);\n"
      when ODBC::SQL_DOUBLE
        istruc+="    SQLLEN #{cname}_len;\n"
        istruc+="    SQLDOUBLE #{cname};\n"
        fbody2+="    ret = SQLBindCol(hstmt, #{cnum}, SQL_C_DOUBLE,"
        fbody2+=" &result->#{cname}, sizeof (SQL_C_DOUBLE),"
        fbody2+=" &result->#{cname}_len);\n"
      else
        istruc+="    SQLLEN #{cname}_len;\n"
        istruc+="    SQLCHAR #{cname}[#{col.length}];\n"
        fbody2+="    ret = SQLBindCol(hstmt, #{cnum}, SQL_C_CHAR,"
        fbody2+=" result->#{cname}, #{col.length},"
        fbody2+=" &result->#{cname}_len);\n"
      end
      fbody2+="    if (!SQL_SUCCEEDED(ret)) {\n\treturn ret;\n    }\n"
      cnum+=1;
    end
    sname=$cqgen_stypes[istruc]
    if sname.nil?
      sname="RESULT_#{name}"
      $cqgen_stypes[istruc]=sname
      addstruct=true
    else
      addstruct=false
    end
    struc+="#{istruc}} #{sname};\n\n"
    flvarst+="    #{sname} result;\n\n"
    if addstruct
      fhead2+=", #{sname} *result"
    else
      fbody2=""
      flvars2=""
      fbody3=""
      flvars3=""
      fname=sname.gsub(/^RESULT_/, "")
      fhead2="#define\t\t#{name}\t#{fname}\n"
      fhead3="#define\t\t#{name}_deinit\t#{fname}_deinit\n"
    end
    cnum=1
    cols.each do |col|
      if coltable.size > 1
        cname=col.table+"_"+col.name
      else
        cname=col.name
      end
      case col.type
      when ODBC::SQL_INTEGER
        fmt='%ld'
      when ODBC::SQL_SMALLINT
        fmt='%d'
      when ODBC::SQL_FLOAT
        fmt='%g'
      when ODBC::SQL_DOUBLE
        fmt='%g'
      else
        fmt='%s'
      end
      fbodyt+="\tif (result.#{cname}_len != SQL_NULL_DATA) {\n"
      fbodyt+="\t    printf(\"#{sname}.#{cname}=#{fmt}\\n\",\n"
      fbodyt+="\t\t   result.#{cname});\n"
      fbodyt+="\t} else {\n"
      fbodyt+="\t    printf(\"#{sname}.#{cname}=NULL\\n\");\n"
      fbodyt+="\t}\n"
      cnum+=1;
    end
    fbodyt+="    } while (SQL_SUCCEEDED(ret));\n"
    fbodyt+="    #{name}_deinit(hstmt);\n"
  else
    fhead+=", SQLINTEGER *rowCountRet"
    fbody="    if (!SQL_SUCCEEDED(ret)) {\n\tgoto done;\n    }\n" + fbody
    fbody="    ret = SQLAllocStmt(hdbc, &hstmt);\n" + fbody
    fbody="    *rowCountRet = 0;\n" + fbody
    fbody+="    ret = SQLExecDirect(hstmt, "
    fbody+='"'+sql+'"'
    fbody+=", SQL_NTS);\n"
    fbody+="    if (!SQL_SUCCEEDED(ret)) {\n\tgoto done;\n    }\n"
    fbody+="    ret = SQLRowCount(hstmt, rowCountRet);\ndone:\n"
    fbody+="    SQLFreeStmt(hstmt, SQL_DROP);\n    return ret;\n}\n\n"
    flvarst+="    SQLINTEGER rowCount;\n\n"
    fbodyt+="    printf(\"#{name}_test rowCount=%ld\\n\", rowCount);\n"
  end
  if fbodyt.length > 0
    fbodyt+="    return ret;\n"
    fbodyt+="}\n"
  end
  fhead+=")\n"
  if fbody2.length > 0
    fhead2+=")\n"
    fbody2+="doexec:\n"
    fbody2+="    ret = SQLFetch(hstmt);\n"
    fbody2+="    if (!SQL_SUCCEEDED(ret)) {\n\treturn ret;\n    }\n"
    fbody2+="    istmt->result = result;\n"
    fbody2+="    return ret;\n}\n\n"
  end
  flvars+="\n"
  stmt.drop
  conn.disconnect
  sql = sql.gsub(/[ \t]+/, " ")
  sql = sql.gsub(/\/\*/, "")
  sql = sql.gsub(/\*\//, "")
  $cqgen_h1+="/* #{sql} */\n" if addstruct
  $cqgen_h1+=struc if addstruct
  $cqgen_h2+="/* #{sql} */\n"
  tmp=fhead.gsub(/SQLRETURN\n/, "SQLRETURN\t");
  $cqgen_h2+=tmp.gsub(/\)\n/, ");\n")
  if fhead2.length > 0
    tmp=fhead2.gsub(/SQLRETURN\n/, "SQLRETURN\t");
    $cqgen_h2+=tmp.gsub(/\)\n/, ");\n")
  end
  if fhead3.length > 0
    tmp=fhead3.gsub(/SQLRETURN\n/, "SQLRETURN\t");
    $cqgen_h2+=tmp.gsub(/\)\n/, ");\n")
  end
  $cqgen_h2+="\n"
  $cqgen_c+=fhead
  $cqgen_c+=flvars
  $cqgen_c+=fbody
  $cqgen_c+=fhead2 unless fhead2 =~ /^#define/
  $cqgen_c+=flvars2
  $cqgen_c+=fbody2
  $cqgen_c+=fhead3 unless fhead3 =~ /^#define/
  $cqgen_c+=flvars3
  $cqgen_c+=fbody3
  $cqgen_ct1+="#if defined(TEST_#{name}) || defined(TEST_cqgen_all)\n"
  $cqgen_ct1+=fheadt
  $cqgen_ct1+=flvarst
  $cqgen_ct1+=fbodyt
  $cqgen_ct1+="#endif\n\n"
end

def cqgen_test
  $cggen_dsn="" if $cqgen_dsn.nil?
  $cggen_uid="" if $cqgen_uid.nil?
  $cggen_pwd="" if $cqgen_pwd.nil?
  test_calls=""
  $cqgen_simple_fns.each_key do |name|
    test_calls+="    printf(\"=== #{name} ===\\n\");\n"
    test_calls+="    #{name}(hdbc);\n"
  end
  $cqgen_ct2=$cqgen_ct1
  $cqgen_ct2+=%Q{
#ifdef TEST_cqgen_all
int
main(int argc, char **argv)
{
    SQLHENV henv;
    SQLHDBC hdbc;
    SQLRETURN ret;

    ret = SQLAllocEnv(&henv);
    if (!SQL_SUCCEEDED(ret)) {
	exit(1);
    }
    ret = SQLAllocConnect(henv, &hdbc);
    if (!SQL_SUCCEEDED(ret)) {
	exit(2);
    }
    ret = SQLConnect(hdbc,
		     \"#{$cqgen_dsn}\", SQL_NTS,
		     \"#{$cqgen_uid}\", SQL_NTS,
		     \"#{$cqgen_pwd}\", SQL_NTS);
    if (!SQL_SUCCEEDED(ret)) {
	exit(3);
    }

    /* ADD YOUR TEST CALLS HERE */
#{test_calls}
    SQLDisconnect(hdbc);
    SQLFreeConnect(hdbc);
    SQLFreeEnv(henv);
    return 0;
}
#endif

}
end

def cqgen_output(c_name=nil, h_name=nil)
  h=''
  h+="#include <stdio.h>\n"
  h+="#include <stdlib.h>\n"
  h+="#include <string.h>\n"
  h+="#include <sqltypes.h>\n"
  h+="#include <sql.h>\n"
  h+="#include <sqlext.h>\n\n"
  h+=$cqgen_h1
  h+="\n"
  h+=$cqgen_h2
  if c_name.nil?
    cf=$stdout
    ctoclose=false
  else
    cf=File::open(c_name, 'w')
    ctoclose=true
  end
  if h_name.nil?
    hf=cf
    htoclose=false
  else
    hf=File::open(h_name, 'w')
    htoclose=true
    hb=File::basename(h_name).gsub(/\W/, "_")
    h="#ifndef _#{hb}\n#define _#{hb}\n\n#{h}\n#endif /* _#{hb} */\n"
  end
  hf.print(h)
  c=''
  if !h_name.nil? && (hf != cf)
    c+="#include <stdio.h>\n"
    c+="#include <stdlib.h>\n"
    c+="#include <string.h>\n"
    c+="#include <sqltypes.h>\n"
    c+="#include <sql.h>\n"
    c+="#include <sqlext.h>\n\n"
    c+="#include \"#{h_name}\"\n\n"
  end
  c+=%Q{
typedef struct {
    SQLHSTMT hstmt;
    void *result;
} INTERNAL_STMT;


}
  c+=$cqgen_c
  c+=$cqgen_ct2
  cf.print(c)
  hf.close if htoclose
  cf.close if ctoclose
  $cqgen_h1=''
  $cqgen_h2=''
  $cqgen_c=''
  $cqgen_ct1=''
  $cqgen_ct2=''
  $cqgen_simple_fns.clear
  $cqgen_stypes.clear
end

# Run this code only when the file is the main program
if $0 == __FILE__
  files=ARGV.flatten
  ARGV.clear
  files.each do |f|
    begin
      load f
    rescue Exception => err
      $stderr.puts("in file #{f}:\n")
      $stderr.puts("\t#{err}\n")
    end
  end
end
