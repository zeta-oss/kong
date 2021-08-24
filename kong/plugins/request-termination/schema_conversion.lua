-- Dynamically downgrade plugin configs to older versions

local version = require "version"

return {
  -- array of versions in which the schema changed. Each version has its own
  -- conversion function to donwgrade to the version prior to this one
  { version = version "2.1.0",
    convert = function(config)
      -- convert from version "2.1.0" to the previous version
      if config.echo == true then
        config.message = "[request-termination] echo option not available before version 2.1.0"
      end
      config.echo = nil
      config.trigger = nil
    end
  }, {
    version = version "2.0.0",
    convert = function(config)
      -- convert from version "2.0.0" to the previous version
    end
  },
  -- etc...


  -- a single method to 'downgrade' the config table in-place to the target
  -- version.
  convert = function(self, config, target_plugin_version)
    if type(target_plugin_version) == "string" then
      target_plugin_version = version(target_plugin_version)
    end

    for _, version_entry in ipairs(self) do
      if target_plugin_version < version_entry.version then
        version_entry.convert(config)
      else
        return
      end
    end
  end
}
