-- Copyright (C) Kong Inc.
local policies = require "kong.plugins.rate-limiting.policies"


local kong = kong
local ngx = ngx
local max = math.max
local time = ngx.time
local pairs = pairs
local tostring = tostring


local EMPTY = {}
local RATELIMIT_LIMIT = "X-RateLimit-Limit"
local RATELIMIT_REMAINING = "X-RateLimit-Remaining"

local RateLimitingHandler = {}


RateLimitingHandler.PRIORITY = 901
RateLimitingHandler.VERSION = "2.0.0"


local function get_identifier(conf)
  local identifier

  if conf.limit_by == "consumer" then
    identifier = (kong.client.get_consumer() or
                  kong.client.get_credential() or
                  EMPTY).id

  elseif conf.limit_by == "credential" then
    identifier = (kong.client.get_credential() or
                  EMPTY).id
  end

  return identifier or kong.client.get_forwarded_ip()
end


local function get_usage(conf, identifier, current_timestamp, limits)
  local usage = {}
  local stop

  for period, limit in pairs(limits) do
    local current_usage, err = policies[conf.policy].usage(conf, identifier, period, current_timestamp)
    if err then
      return nil, nil, err
    end

    -- What is the current usage for the configured limit name?
    local remaining = limit - current_usage

    -- Recording usage
    usage[period] = {
      limit = limit,
      remaining = remaining,
    }

    if remaining <= 0 then
      stop = period
    end
  end

  return usage, stop
end


function RateLimitingHandler:access(conf)
  local current_timestamp = time() * 1000

  -- Consumer is identified by ip address or authenticated_credential id
  local identifier = get_identifier(conf)
  local fault_tolerant = conf.fault_tolerant

  -- Load current metric for configured period
  local limits = {
    second = conf.second,
    minute = conf.minute,
    hour = conf.hour,
    day = conf.day,
    month = conf.month,
    year = conf.year,
  }

  local usage, stop, err = get_usage(conf, identifier, current_timestamp, limits)
  if err then
    if fault_tolerant then
      kong.log.err("failed to get usage: ", tostring(err))
    else
      kong.log.err(err)
      return kong.response.exit(500, { message = "An unexpected error occurred" })
    end
  end

  if usage then
    -- Adding headers
    local headers
    if not conf.hide_client_headers then
      headers = {}
      for k, v in pairs(usage) do
        if stop == nil or stop == k then
          v.remaining = v.remaining - 1
        end

        headers[RATELIMIT_LIMIT .. "-" .. k] = v.limit
        headers[RATELIMIT_REMAINING .. "-" .. k] = max(0, v.remaining)
      end
    end

    -- If limit is exceeded, terminate the request
    if stop then
      return kong.response.exit(429, { message = "API rate limit exceeded" }, headers)
    end

    if headers then
      kong.response.set_headers(headers)
    end
  end

  local _, err = policies[conf.policy].increment(conf, limits, identifier, current_timestamp, 1)
  if err then
    kong.log.notice("incrementing rate-limits failed: ", err)
  end
end


return RateLimitingHandler
