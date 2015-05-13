require 'bundler/setup'
require 'sinatra'

class UnfuddleSlackWeb < Sinatra::Base; end

run UnfuddleSlackWeb
