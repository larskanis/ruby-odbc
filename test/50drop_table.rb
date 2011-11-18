# drop all statements, otherwise table might be locked for drop.
$c.drop_all
$c.do("drop table test")
