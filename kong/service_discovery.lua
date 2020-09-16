local cjson = require "cjson"
local utils = require "kong.tools.utils"
local http = require "resty.http"
local socket_url = require "socket.url"


local service_discovery = {}


local formats_running = {}
local consul_index = nil


local parsed_urls_cache = {}


-- Parse host url.
-- @param `url` host url
-- @return `parsed_url` a table with host details:
-- scheme, host, port, path, query, userinfo
local function parse_url(host_url)
  local parsed_url = parsed_urls_cache[host_url]

  if parsed_url then
    return parsed_url
  end

  parsed_url = socket_url.parse(host_url)
  if not parsed_url.port then
    if parsed_url.scheme == "http" then
      parsed_url.port = 80
    elseif parsed_url.scheme == "https" then
      parsed_url.port = 443
    end
  end
  if not parsed_url.path then
    parsed_url.path = "/"
  end

  parsed_urls_cache[host_url] = parsed_url

  return parsed_url
end


local function http_get(http_endpoint, timeout)
  local parsed_url = parse_url(http_endpoint)
  local host = parsed_url.host
  local port = tonumber(parsed_url.port)

  local httpc = http.new()
  httpc:set_timeout(timeout)
  local ok, err = httpc:connect(host, port)
  if not ok then
    return nil, "failed to connect to " .. host .. ":" .. tostring(port) .. ": " .. err
  end

  if parsed_url.scheme == "https" then
    local _, err = httpc:ssl_handshake(true, host, false)
    if err then
      return nil, "failed to do SSL handshake with " ..
                  host .. ":" .. tostring(port) .. ": " .. err
    end
  end

  local res, err = httpc:request({
    method = "GET",
    path = parsed_url.path,
    query = parsed_url.query,
    headers = {
      ["Host"] = parsed_url.host,
    },
  })
  if not res then
    return nil, "failed request to " .. host .. ":" .. tostring(port) .. ": " .. err
  end

  -- always read response body, even if we discard it without using it on success
  local response_body = res:read_body()
  local success = res.status < 400
  local err_msg

  if not success then
    err_msg = "request to " .. host .. ":" .. tostring(port) ..
              " returned status code " .. tostring(res.status) .. " and body " ..
              response_body
  end

  ok, err = httpc:set_keepalive()
  if not ok then
    -- the batch might already be processed at this point, so not being able to set the keepalive
    -- will not return false (the batch might not need to be reprocessed)
    kong.log.err("failed keepalive for ", host, ":", tostring(port), ": ", err)
  end

  return success, response_body or err_msg, res.headers
end


local function ensure_running(format, url)
  if ngx.worker.id() ~= 0 then -- only run on one worker
    return
  end

  if formats_running[format] then
    return
  end

  formats_running[format] = true

  if format == "consul" then
    -- This assumes that all upstreams using Consul are pointing
    -- to the same Consul host, so any URL received works to
    -- initialize the blocking-query watcher
    local parsed_url = parse_url(url)
    local services_url = parsed_url.scheme .. "://" ..
                         parsed_url.host .. ":" .. parsed_url.port ..
                         "/v1/catalog/services"

    ngx.timer.at(0, function(premature)
      if premature then
        return
      end

      while formats_running[format] do
        local req_url = services_url
        if consul_index then
          req_url = req_url .. "?index=" .. consul_index
        end
        local ok, body, headers = http_get(req_url, 60000)
        if not formats_running[format] then
          consul_index = nil
          break
        end

        if headers then
          local new_consul_index = headers["X-Consul-Index"]

          -- This assumes that any change in the X-Consul-Index
          -- value means that a relevant change has happened in
          -- the targets.
          --
          -- If this is not true and the index keeps
          -- changing due to unrelated operations in Consul,
          -- this loop will spin a lot.
          if consul_index and consul_index ~= new_consul_index then

            -- TODO the order of operations here is not ideal:
            -- we are flushing the cache before we get the new
            -- data. Instead, we should query Consul about
            -- all relevant upstreams here and replace their
            -- target cache data before sending the "reset" signal.
            -- With the cache pre-filled, the "reset" signal would
            -- then just update the state of the balancer objects.
            local ok, err = kong.worker_events.post("balancer", "targets", {
              operation = "reset",
              entity = { id = "all", name = "all" }
            })
            if not ok then
              kong.log.err(err)
            end
          end

          consul_index = new_consul_index
        end
      end
    end)
  end
end


------------------------------------------------------------------------------
-- Resets the service discovery background operations
function service_discovery.init()
  -- this will cause the background watcher to eventually stop.
  formats_running = {}
end


------------------------------------------------------------------------------
-- Loads a list of targets via service discovery
-- @param upstream Upstream entity object
-- @return The target array, with target entity tables.
function service_discovery.load_targets(upstream)
  if upstream.service_discovery.format == "consul" then
    ensure_running("consul", upstream.service_discovery.url)

    local ok, resp = http_get(upstream.service_discovery.url,
                              upstream.service_discovery.timeout)
    if not ok then
      kong.log.err("failed querying Consul: ", resp)
      return {}
    end

    local data = cjson.decode(resp)
    if not data then
      kong.log.err("failed parsing JSON in Consul response: ", resp)
      return {}
    end

    local now = ngx.now()

    local targets = {}
    for _, item in ipairs(data) do
      local host = item.ServiceAddress
      if host == "" then
        host = item.Address
      end

      local port = item.ServicePort
      local weight = item.ServiceMeta
                     and item.ServiceMeta.weight
                     or  100

      table.insert(targets, {
        id = utils.uuid(),
        created_at = now,
        upstream = { id = upstream.id },
        target = host .. ":" .. port,
        name = host,
        port = port,
        weight = weight,
        tags = item.ServiceTags,
      })
    end

    return targets
  end

  return {}
end


------------------------------------------------------------------------------
-- Loads a list of targets via service discovery
-- @param operation "create", "delete", "update", "upsert"
-- @param upstream Upstream entity object; if "delete", contains only the id
-- @return The target array, with target entity tables.
function service_discovery.on_upstream_event(operation, upstream)
end


return service_discovery
