use strict;
use warnings FATAL => 'all';
use Test::Nginx::Socket::Lua;
do "./t/Util.pm";

plan tests => repeat_each() * (blocks() * 3);

run_tests();

__DATA__

=== TEST 1: kong.trace.new()
--- http_config eval: $t::Util::HttpConfig
--- config
    location /t {
        content_by_lua_block {
            local PDK = require "kong.pdk"
            local pdk = PDK.new()

            local ok, err = pcall(pdk.trace.new)
            if not ok then
                ngx.say(err)
            end
        }
    }
--- request
GET /t
--- response_body
namespace must be a string
--- no_error_log
[error]