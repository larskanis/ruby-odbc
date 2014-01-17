require 'mkmf'

if ! defined? PLATFORM
  PLATFORM = RUBY_PLATFORM
end

def have_library_ex(lib, func="main", headers=nil)
  checking_for "#{func}() in -l#{lib}" do
    libs = append_library($libs, lib)
    if !func.nil? && !func.empty? && COMMON_LIBS.include?(lib)
      true
    elsif try_func(func, libs, headers)
      $libs = libs
      true
    else
      false
    end
  end
end
 
dir_config("odbc")
have_header("version.h")
have_header("sql.h") || begin
  puts "ERROR: sql.h not found"
  exit 1
end
have_header("sqlext.h") || begin
  puts "ERROR: sqlext.h not found"
  exit 1
end
testdlopen = enable_config("dlopen", false)
begin
  if PLATFORM !~ /(mingw|cygwin)/ then
    header = "sqltypes.h"
  else
    header = ["windows.h", "sqltypes.h"]
  end
  if defined? have_type
    have_type("SQLTCHAR", header)
  else
    throw
  end
rescue
  puts "WARNING: please check sqltypes.h for SQLTCHAR manually,"
  puts "WARNING: if defined, modify CFLAGS in Makefile to contain"
  puts "WARNING: the option -DHAVE_TYPE_SQLTCHAR"
end
begin
  if PLATFORM !~ /(mingw|cygwin)/ then
    header = "sqltypes.h"
  else
    header = ["windows.h", "sqltypes.h"]
  end
  if defined? have_type
    have_type("SQLLEN", header)
  else
    throw
  end
rescue
  puts "WARNING: please check sqltypes.h for SQLLEN manually,"
  puts "WARNING: if defined, modify CFLAGS in Makefile to contain"
  puts "WARNING: the option -DHAVE_TYPE_SQLLEN"
end
begin
  if PLATFORM !~ /(mingw|cygwin)/ then
    header = "sqltypes.h"
  else
    header = ["windows.h", "sqltypes.h"]
  end
  if defined? have_type
    have_type("SQLULEN", header)
  else
    throw
  end
rescue
  puts "WARNING: please check sqltypes.h for SQLULEN manually,"
  puts "WARNING: if defined, modify CFLAGS in Makefile to contain"
  puts "WARNING: the option -DHAVE_TYPE_SQLULEN"
end
$have_odbcinst_h = have_header("odbcinst.h")

if PLATFORM =~ /mswin32/ then
  if !have_library_ex("odbc32", "SQLAllocConnect", "sql.h") ||
     !have_library_ex("odbccp32", "SQLConfigDataSource", "odbcinst.h") ||
     !have_library_ex("odbccp32", "SQLInstallerError", "odbcinst.h") ||
     !have_library("user32", "CharUpper") then
    puts "Can not locate odbc libraries"
    exit 1
  end
  have_func("SQLInstallerError", "odbcinst.h")
# mingw untested !!!
elsif PLATFORM =~ /(mingw|cygwin)/ then
  have_library("odbc32", "")
  have_library("odbccp32", "")
  have_library("user32", "")
elsif (testdlopen && PLATFORM !~ /(macos|darwin)/ && CONFIG["CC"] =~ /gcc/ && have_func("dlopen", "dlfcn.h") && have_library("dl", "dlopen")) then
  $LDFLAGS+=" -Wl,-init -Wl,ruby_odbc_init -Wl,-fini -Wl,ruby_odbc_fini"
  $CPPFLAGS+=" -DHAVE_SQLCONFIGDATASOURCE"
  $CPPFLAGS+=" -DHAVE_SQLINSTALLERERROR"
  $CPPFLAGS+=" -DUSE_DLOPEN_FOR_ODBC_LIBS"
  if defined? have_type then
    if have_type("SQLBIGINT", "sqltypes.h", "-DHAVE_LONG_LONG") then
      $CPPFLAGS+=" -DHAVE_LONG_LONG"
    end
  end
else
  have_library("odbc", "SQLAllocConnect") ||
    have_library("iodbc", "SQLAllocConnect")
  ($have_odbcinst_h &&
    have_library("odbcinst", "SQLConfigDataSource")) ||
  ($have_odbcinst_h &&
    have_library("iodbcinst", "SQLConfigDataSource"))
  $have_odbcinst_h &&
    have_func("SQLInstallerError", "odbcinst.h")
  if defined? have_type then
    if have_type("SQLBIGINT", "sqltypes.h", "-DHAVE_LONG_LONG") then
      $CPPFLAGS+=" -DHAVE_LONG_LONG"
    end
  end
end

create_makefile("odbc")
