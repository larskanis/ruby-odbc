#!/usr/bin/env rake

require 'rubygems'
require 'hoe'
require 'rake/extensiontask'

hoe = Hoe.spec 'ruby-odbc' do |ext|
  developer('Christian Werner', 'chw @nospam@ ch-werner.de')

  self.readme_file = 'README.rdoc'
  self.history_file = 'ChangeLog'
  self.extra_rdoc_files << self.readme_file
  self.extra_rdoc_files += %w[ ext/init.c ext/odbc.c ]
  self.local_rdoc_dir = 'generated_docs'
  spec_extras[:extensions] = %w[ ext/extconf.rb ext/utf8/extconf.rb ]
end

ENV['RUBY_CC_VERSION'] ||= '1.8.7:1.9.2'

Rake::ExtensionTask.new('odbc_ext', hoe.spec) do |ext|
  ext.ext_dir = 'ext'
  ext.cross_compile = true
  ext.cross_platform = 'i386-mingw32'
  ext.cross_config_options << '--enable-win32-cross-compilation'
end

Rake::ExtensionTask.new('odbc_utf8_ext', hoe.spec) do |ext|
  ext.ext_dir = 'ext/utf8'
  ext.cross_compile = true
  ext.cross_platform = 'i386-mingw32'
  ext.cross_config_options << '--enable-win32-cross-compilation'
end
