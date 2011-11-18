# $Id: test.rb,v 1.7 2010/02/18 12:30:43 chw Exp chw $
#
# Execute in ruby-odbc top directory.
#
#  ruby test.rb "mysql-DSN" "mysql-user" "mysql-password"
#
# Test creates and deletes table "test" in that DSN.

require 'odbc'

$dsn = ARGV.shift
$uid = ARGV.shift
$pwd = ARGV.shift

begin
  Dir.glob("test/[0-9]*.rb").sort.each do |f|
    f =~ /^test\/\d+(.*)\.rb$/
    print $1 + "."*(20-$1.length)
    $stdout.flush
    load f
    print "ok\n"
  end
ensure
  begin
    $c.drop_all unless $c.class != ODBC::Database
  rescue
  end
  begin
    ODBC.connect($dsn, $uid, $pwd).do("drop table test")
  rescue
  end
end
