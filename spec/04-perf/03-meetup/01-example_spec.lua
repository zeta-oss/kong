local perf = require("spec.helpers.perf")

perf.set_log_level(ngx.DEBUG)

perf.use_driver("docker")

describe("perf test for Kong 2.5.0", function()
  lazy_setup(function()
    local helpers = perf.setup()

    local bp = helpers.get_db_utils("postgres", {
      "routes",
      "services",
    })

    local upstream_uri = perf.start_upstream([[
    location = /test {
      return 200;
    }
    ]])

    local service = bp.services:insert {
      url = upstream_uri .. "/test",
    }

    bp.routes:insert {
      paths = { "/route1" },
      service = service,
      strip_path = true,
    }

    local route = bp.routes:insert {
      paths = { "/route2" },
      service = service,
      strip_path = true,
    }

    bp.plugins:insert {
      name = "correlation-id",
      route = route,
    }

  end)

  before_each(function()
    perf.start_kong("2.5.0", {
      --kong configs
    })
  end)

  after_each(function()
    perf.stop_kong()
  end)

  lazy_teardown(function()
    perf.teardown()
  end)

  it("#withtout_plugin", function()
    perf.start_load({
      path = "/route1",
      connections = 1000,
      threads = 5,
      duration = 30,
    })

    local result = assert(perf.wait_result())

    print(("### Result for without plugin:\n%s"):format(result))

    perf.save_error_log("without-plugin.log")
  end)

  it("#with_plugin", function()
    perf.start_load({
      path = "/route2",
      connections = 1000,
      threads = 5,
      duration = 30,
    })

    local result = assert(perf.wait_result())

    print(("### Result for with plugin:\n%s"):format(result))

    perf.save_error_log("with-plugin.log")
  end)
end)