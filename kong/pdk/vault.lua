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
  local resource = sub(url.path, 2)
  -- local config = url.query
  local strategy
  local vault
  if IS_CLI then
    -- process reference
    -- TODO: parse config options from URL
    vault = { name = prefix, config = {}}
    local v_mod = "kong.vaults." .. vault.name
    local ok, mod = pcall(require, v_mod)
    -- print("ok = " .. require("inspect")(ok))
    -- print("mod = " .. require("inspect")(mod))
    if not ok then
      print("ok = " .. require("inspect")(ok))
      return nil, fmt("could not find vault %s", vault.name)
    end
    -- TODO: is this required? Shouldn't .init() be loaded implicitly
    mod.init()

    local deref, deref_err = mod.get(vault.config, resource)
    if not deref then
      return nil, fmt("unable loto ad value (%s): %s [%s]", resource, deref_err, tostring(reference))
    end

    return tostring(deref)

  else
    -- config reference
    if not kong.db then
      return nil, fmt("kong.db not yet loaded")
    end
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
