local floor = math.floor
local math_random = math.random
local ngx_now = ngx.now

-- ngx.now in microseconds
local function ngx_now_mu()
  return ngx_now() * 1000000
end

local span_methods = {}
local span_mt = {
  __index = span_methods,
}


local baggage_mt = {
  __newindex = function()
    error("attempt to set immutable baggage")
  end,
}


-- Default Id generator
local function generate_span_id()
  return rand_bytes(8)
end


-- Build-in simple sampler
local function simple_sampler(sample_ratio)
  return math_random() < sample_ratio
end


-- internal new_span, the data structures may not fit the OpenTelemetry spec,
-- but it's suitable for OpenResty
local function _new_span(kind, name, start_timestamp_mu,
                   should_sample, trace_id,
                   span_id, parent_id, baggage)
  assert(kind == "SERVER" or kind == "CLIENT", "invalid span kind")
  assert(type(name) == "string" and name ~= "", "invalid span name")
  assert(type(start_timestamp_mu) == "number" and start_timestamp_mu >= 0,
         "invalid span start_timestamp")
  assert(type(trace_id) == "string", "invalid trace id")

  if span_id == nil then
    span_id = generate_span_id()
  else
    assert(type(span_id) == "string", "invalid span id")
  end

  if parent_id ~= nil then
    assert(type(parent_id) == "string", "invalid parent id")
  end

  if baggage then
    setmetatable(baggage, baggage_mt)
  end

  return setmetatable({
    kind = kind,
    trace_id = trace_id,
    span_id = span_id,
    parent_id = parent_id,
    name = name,
    timestamp = floor(start_timestamp_mu),
    should_sample = should_sample,
    baggage = baggage,
    n_logs = 0,
    is_recording = true,
  }, span_mt)
end

-- Noop Span
local noop_span = setmetatable({}, { __index = function(self, key)
  return {}
end})

-- Create a child span of this one
function span_methods:new_child_span(name, kind, start_timestamp_mu)
  return _new_span(
    kind,
    name,
    start_timestamp_mu,
    self.should_sample,
    self.trace_id,
    generate_span_id(),
    self.span_id,
    self.baggage
  )
end


-- Ends a Span
function span_methods:finish(finish_timestamp_mu)
  assert(self.duration == nil, "span already finished")
  assert(type(finish_timestamp_mu) == "number" and finish_timestamp_mu >= 0,
         "invalid span finish timestamp")
  local duration = finish_timestamp_mu - self.timestamp
  assert(duration >= 0, "invalid span duration")
  self.duration = floor(duration)
  self.is_recording = false
  return true
end


-- Set a tag to a Span
function span_methods:set_tag(key, value)
  assert(type(key) == "string", "invalid tag key")
  if value ~= nil then -- Validate value
    local vt = type(value)
    assert(vt == "string" or vt == "number" or vt == "boolean",
      "invalid tag value (expected string, number, boolean or nil)")
  end
  local tags = self.tags
  if tags then
    tags[key] = value
  elseif value ~= nil then
    tags = {
      [key] = value
    }
    self.tags = tags
  end
  return true
end


function span_methods:each_tag()
  local tags = self.tags
  if tags == nil then return function() end end
  return next, tags
end


-- Adds an annotation to a Span
function span_methods:annotate(value, timestamp_mu)
  assert(type(value) == "string", "invalid annotation value")
  assert(type(timestamp_mu) == "number" and timestamp_mu >= 0, "invalid annotation timestamp")

  local annotation = {
    value = value,
    timestamp = floor(timestamp_mu),
  }

  local annotations = self.annotations
  if annotations then
    annotations[#annotations + 1] = annotation
  else
    self.annotations = { annotation }
  end
  return true
end


function span_methods:each_baggage_item()
  local baggage = self.baggage
  if baggage == nil then return function() end end
  return next, baggage
end


local tracer_mt = {}
tracer_mt.__index = tracer_mt

-- Creates a new child Span or root Span
-- if start_timestamp_mu is ignored, current timestamp will be used.
function tracer_mt:start_span(name, parent_span, kind, start_timestamp_mu)
  -- noop Tracer
  if self.noop then return noop_span end

  local timestamp_mu = start_timestamp_mu or ngx_now_mu()

  local span
  if not parent_span then
    -- if no parent Span, the new Span should be a root Span.
    span = _new_span(kind, name, timestamp_mu,
                    simple_sampler(self.config.sample_ratio))
  else
    -- create new Span from parent Span
    span = parent_span:new_child_span(name, kind, timestamp_mu)
  end

  return span
end

-- Set tracer config, used in dynamic load config in different OpenResty phases
-- usually called in init_worker and rewrite phases.
function tracer_mt:set_config(conf)
  self.config = conf
  self.noop = false
end

-- Create new TracerProvider instance
-- see: https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/trace/api.md#tracerprovider
local function new()
  local _TP = {}
  local tracers = {}

  function _TP.get_tracer(name)
    return tracers[name] or
      -- indicates whether a tracer is noop tracer
      setmetatable({ noop = true }, tracer_mt)
  end

  return _TP
end

return {
  new = new,
}