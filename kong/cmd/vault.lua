local fmt = string.format
local kong_global = require "kong.global"
local DB = require "kong.db"
local conf_loader = require "kong.conf_loader"
local pl_path = require "pl.path"
local log = require "kong.cmd.utils.log"

local function init_db(args)
  -- retrieve default prefix or use given one
  log.disable()
  local conf = assert(conf_loader(args.conf, {
    prefix = args.prefix
  }))
  log.enable()

  if pl_path.exists(conf.kong_env) then
    -- load <PREFIX>/kong.conf containing running node's config
    conf = assert(conf_loader(conf.kong_env))
  end

  package.path = conf.lua_package_path .. ";" .. package.path

  _G.kong = kong_global.new()
  kong_global.init_pdk(_G.kong, conf, nil) -- nil: latest PDK

  local db = assert(DB.new(conf))
  assert(db:init_connector())
  assert(db:connect())
  assert(db.vaults:load_vault_schemas(conf.loaded_vaults))

  _G.kong.db = db
  return db

end

local function get(args)
  if args.command == "get" then
    local vault = args[1]
    local key = args[2]
    if not key or not vault then
      print("the 'get' command needs a <key> and a <vault> argument")
      print("kong vault get <vault> <key>")
      os.exit(1)
    end

    local db = init_db(args)

    local vaults = db.vaults:select_by_prefix(vault)
    if not vaults then
      print(fmt("vault with prefix (%s) could not be found.", vault))
      os.exit(1)
    end

    local v_mod = db.vaults.strategies[vaults.name]
    if not v_mod then
      print(fmt("vault not installed (%s)", v_mod))
      os.exit(1)
    end
    local deref_value = v_mod:get(key)
    if deref_value then
      print(fmt("value: %s", deref_value))
      os.exit(0)
    end
    print(fmt("value for key %s could not be found", key))
  end
end

local function list(args)
  if args.command == "list" then
    local db = init_db(args)
    local strats = db.vaults
    -- TODO: select all vaults and print prefix
    for k, _ in pairs(strats) do
      print(fmt("* %s", k))
    end
    os.exit(0)
  end
end

local function list_vaults(args)
  -- TODO: nameing
  if args.command == "strategies" then
    local db = init_db(args)
    local v_mod = db.vaults.strategies
    for k, _ in pairs(v_mod) do
      print(fmt("* %s", k))
    end
  end
end




local function execute(args)
  -- TODO: the conditionals should not be required.
  -- args.command = args.command:gsub("%-", "_")
  -- this is handled via the cmd.init mod
  if args.command == "" then
    os.exit(0)
  end
  if args.command == "get" then
    get(args)
  end
  if args.command == "list" then
    list(args)
  end
  if args.command == "strategies" then
    list_vaults(args)
  end
end


local lapp = [[
Usage: kong vault COMMAND [vault] [OPTIONS]

Use declarative configuration files with Kong.

The available commands are:
  get <vault> <key>                Retrieves a value for <key> from <vault>
  list                             Lists configured vaults
  strategies                       Lists available vault strategies

Options:
  <placeholder for potential filtering options>
]]

return {
  lapp = lapp,
  execute = execute,
  sub_commands = {
    get = true,
    list = true,
    strategies = true,
  },
}

