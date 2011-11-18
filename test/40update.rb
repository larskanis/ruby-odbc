$q = $c.run("update test set id=0, str='hoge'")
if $q.nrows != 4 then
  $stderr.print "update row count: expected 4, got ", $q.nrows, "\n"
end
