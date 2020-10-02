local cjson = require "cjson"
local utils = require "kong.tools.utils"
local http = require "resty.http"


local service_discovery = {}


local formats_running = {}
local consul_index = nil


local function http_get(scheme, host, port, path, query, timeout)
  local httpc = http.new()
  httpc:set_timeout(timeout)

  local ok, err = httpc:connect(host, port)
  if not ok then
    return nil, "failed to connect to "
      .. host .. ":" .. tostring(port) .. ": " .. err
  end

  if scheme == "https" then
    local _, err = httpc:ssl_handshake(true, host, false)
    if err then
      return nil, "failed to do SSL handshake with " ..
                  host .. ":" .. tostring(port) .. ": " .. err
    end
  end

  local res, err = httpc:request({
    method = "GET",
    path = path,
    query = query or "",
    headers = {
      ["Host"] = host,
    },
  })
  if not res then
    return nil, "failed request to "
      .. host .. ":" .. tostring(port) .. ": " .. err
  end

  -- always read response body, even if we discard it without using
  -- it on success
  local response_body = res:read_body()
  local success = res.status < 400
  local err_msg

  if not success then
    err_msg = "request to " .. host .. ":" .. tostring(port) ..
              " returned status code " .. tostring(res.status) ..
              " and body " .. response_body
  end

  ok, err = httpc:set_keepalive()
  if not ok then
    -- the batch might already be processed at this point, so not being
    -- able to set the keepalive will not return false (the batch might
    -- not need to be reprocessed)
    kong.log.err("failed keepalive for ", host, ":",
      tostring(port), ": ", err)
  end

  return success, response_body or err_msg, res.headers
end


local function invalidate_consul_upstreams()
  local balancer = require "kong.runloop.balancer"

  for _, upstream_id in pairs(balancer.get_all_upstreams()) do
    local upstream = balancer.get_upstream_by_id(upstream_id)
    if upstream.service_discovery then
      print("INVALIDATING UPSTREAM" .. upstream.name)

      -- only invalidate if updated targets list can be obtained
      local targets = service_discovery.load_targets(upstream)
      if not targets then
        return nil, "failed fetching updated targets"
      end

      local target_data = { upstream = upstream }
      balancer.on_target_event("update", target_data, target_data)
    end
  end

  return true
end


local function init_watcher_consul()
  ngx.timer.at(0, function(premature)
    if premature then
      return
    end

    while formats_running["consul"] do
      print"RUNNING CONSUL WATCHER"
      print(ngx.worker.id())
      print(ngx.worker.pid())

      local ok, body, headers = http_get(kong.configuration.consul_scheme,
                                         kong.configuration.consul_host,
                                         kong.configuration.consul_port,
                                         "/v1/catalog/services",
                                         { index = consul_index },
                                         3000)
      if not ok and not body:match("timeout") then
        kong.log.err("failed querying Consul services: ", body)
        goto continue
      end

      if not formats_running["consul"] then
        consul_index = nil
      end

      if headers then
        -- This assumes that any change in the X-Consul-Index
        -- value means that a relevant change has happened in
        -- the targets.
        --
        -- If this is not true and the index keeps
        -- changing due to unrelated operations in Consul,
        -- this loop will spin a lot.
        local new_consul_index = headers["X-Consul-Index"]
        consul_index = consul_index or new_consul_index

        if consul_index and consul_index ~= new_consul_index then
          print "CONSUL WATCHER: NEW INDEX"
          print("worker = " .. ngx.worker.id())

          -- only update index if invalidation / reload was successful
          if invalidate_consul_upstreams() then
            consul_index = new_consul_index
          end
        end
      end

      ::continue::
    end
  end)

  return true
end


-- Background Watcher
-- Detects changes on services and invalidates Kong state
-- Only runs on worker 0
local function init_watcher(format)
  if formats_running[format] then
    return true
  end

  if format == "consul" then
    assert(init_watcher_consul())
  end

  formats_running[format] = true

  return true
end


------------------------------------------------------------------------------
-- Resets the service discovery background operations
function service_discovery.init(opts)
  -- this will cause the background watcher to eventually stop
  formats_running = {}

  for _, format in ipairs(opts.formats) do
    assert(init_watcher(format))
  end

  return true
end


------------------------------------------------------------------------------
-- Loads a list of targets via service discovery
-- @param upstream Upstream entity object
-- @return The target array, with target entity tables.
function service_discovery.load_targets(upstream)
  print("LOADING TARGETS FOR ", upstream.name)

  if upstream.service_discovery.format == "consul" then

    local ok, resp = http_get(kong.configuration.consul_scheme,
                              kong.configuration.consul_host,
                              kong.configuration.consul_port,
                              "/v1/catalog/service/" .. upstream.name,
                              { index = consul_index },
                              kong.configuration.consul_timeout)
    if not ok then
      kong.log.err("failed querying Consul: ", resp)
      return {}
    end

    local data, err = cjson.decode(resp)
    if not data then
      kong.log.err("failed parsing JSON in Consul response: ", err)
      return {}
    end

    local now = ngx.now()

    local targets = {}
    for _, item in ipairs(data) do
      local host = item.ServiceAddress
      if not host or host == "" then
        host = item.Address
      end

      local port = item.ServicePort

      -- TODO what other fields are relevant?
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
end


return service_discovery
