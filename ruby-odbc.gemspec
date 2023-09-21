# frozen_string_literal: true

require_relative 'lib/ruby-odbc/version'

Gem::Specification.new do |spec|
  spec.name                  = 'ruby-odbc'
  spec.version               = RubyOdbc::VERSION
  spec.platform              = Gem::Platform::RUBY
  spec.author                = 'AppFolio'
  spec.email                 = 'dev@appfolio.com'
  spec.description           = 'ODBC binding for Ruby.'
  spec.summary               = spec.description
  spec.homepage              = 'https://github.com/appfolio/ruby-odbc'
  spec.license               = 'Nonstandard'
  spec.files                 = Dir['**/*'].select { |f| f[%r{^(lib/|ext/|.*gemspec)}] }
  spec.require_paths         = ['lib']
  spec.extensions            = ['ext/extconf.rb']
  spec.required_ruby_version = Gem::Requirement.new('>= 3.0')

  spec.metadata['allowed_push_host'] = 'https://rubygems.pkg.github.com/appfolio'
end
