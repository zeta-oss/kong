---
-- Vault module
--
-- This module can be used to resolve vault references.
--
-- @module kong.vault


local tostring = tostring
local byte = string.byte
local fmt = string.format
local sub = string.sub
local type = type
local url_parse = require "socket.url".parse


local IS_CLI = ngx.IS_CLI
local BRACE_START = byte("{")
local BRACE_END = byte("}")
local COLON = byte(":")
local SLASH = byte("/")


---
-- TODO: docs
-- @function kong.vault.is_reference
local function is_reference(reference)
  return type(reference)      == "string"
     and byte(reference, 1)   == BRACE_START
     and byte(reference, -1)  == BRACE_END
     and byte(reference, 7)   == COLON
     and byte(reference, 8)   == SLASH
     and byte(reference, 9)   == SLASH
     and sub(reference, 2, 6) == "vault"
     and url_parse(sub(reference, 2, -2)) ~= nil
end


---
-- TODO: docs
-- @function kong.vault.get
local function get(reference)
  if type(reference)      ~= "string"
  or byte(reference, 1)   ~= BRACE_START
  or byte(reference, -1)  ~= BRACE_END
  or byte(reference, 7)   ~= COLON
  or byte(reference, 8)   ~= SLASH
  or byte(reference, 9)   ~= SLASH
  or sub(reference, 2, 6) ~= "vault"
  then
    return nil, fmt("not a reference [%s]", tostring(reference))
  end

  local url, err = url_parse(sub(reference, 2, -2))
  if not url then
    return nil, fmt("not a reference (%s) [%s]", err, tostring(reference))
  end

  local prefix = url.host
  local strategy
  local vault
  if IS_CLI then
    -- process reference
    -- vault =
    vault = { name = prefix } -- TODO: other configs from ENV

  else
    -- config reference
    local vaults = kong.db.vaults
    local cache_key = vaults:cache_key(prefix)
    vault, err = kong.core_cache:get(cache_key, nil, vaults.select_by_prefix, vaults, prefix)
    if not vault then
      if err then
        return nil, fmt("unable to load vault (%s): %s [%s]", prefix, err, tostring(reference))
      end

      return nil, fmt("vault not found (%s) [%s]", prefix, tostring(reference))
    end

    strategy = vaults.strategies[vault.name]
  end

  if not strategy then
    return nil, fmt("vault not installed (%s) [%s]", vault.name, tostring(reference))
  end

  local resource = sub(url.path, 2)
  local value, err = strategy.get(vault.config, resource)
  if not value then
    return nil, fmt("unable load value (%s) from vault (%s): %s [%s]", resource, vault.name, err, tostring(reference))
  end

  return tostring(value)
end


local function new(_)
  return {
    get = get,
    is_reference = is_reference,
  }
end


return {
  new = new,
}
