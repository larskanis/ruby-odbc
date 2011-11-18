$q = $c.run("insert into test (id, str) values (1, 'foo')")
$q.run("insert into test (id, str) values (2, 'bar')")

$p = $c.proc("insert into test (id, str) values (?, ?)") {}
$p.call(3, "FOO")
$p[4, "BAR"]
