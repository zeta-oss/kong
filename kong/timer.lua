local timer_at = ngx.timer.at
local timer_every = ngx.timer.every
local sleep = ngx.sleep
local exiting = ngx.worker.exiting

local now = ngx.now
local update_timer = ngx.update_time

local unpack = table.unpack

local debug_getinfo = debug.getinfo

local max = math.max
local min = math.min
local floor = math.floor
local ceil = math.ceil
local pow = math.pow
local huge = math.huge


local log = ngx.log

local ERR = ngx.ERR
local DEBUG = ngx.DEBUG

local wait = ngx.thread.wait
local kill = ngx.thread.kill
local spawn = ngx.thread.spawn

local FOCUS_UPDATE_TIME = true

local DEFAULT_THREADS = 1000
local DEFAULT_MAX_EXPIRE = 24 * 60 * 60
local DEFAULT_RECREATE_INTERVAL = 50
local DEFAULT_REAL_TIMER = 10

local _M = {}


local function print_wheel(self, _wheel)
    local wheel

    log(ERR, "======== BEGIN MSEC ========")
    wheel = _wheel.msec
    log(ERR, "pointer = " .. wheel.pointer)
    log(ERR, "nelt = " .. wheel.nelt)
    for i, v in ipairs(wheel.array) do
        for _, value in pairs(v) do
            log(ERR, "timer: " .. _wheel.real_timer .. ", index = " .. i .. ", name = " .. value.name .. 
                ", offset.second = " .. value.next_pointer.second ..
                ", offset.minute = " .. value.next_pointer.minute ..
                ", offset.hour = " .. value.next_pointer.hour)
        end
    end
    log(ERR, "========= END SECOND =========")


    log(ERR, "======== BEGIN SECOND ========")
    wheel = _wheel.sec
    log(ERR, "pointer = " .. wheel.pointer)
    log(ERR, "nelt = " .. wheel.nelt)
    for i, v in ipairs(wheel.array) do
        for _, value in pairs(v) do
            log(ERR, "timer: " .. _wheel.real_timer .. ", index = " .. i .. ", name = " .. value.name .. 
                ", offset.second = " .. value.next_pointer.second ..
                ", offset.minute = " .. value.next_pointer.minute ..
                ", offset.hour = " .. value.next_pointer.hour)
        end
    end
    log(ERR, "========= END SECOND =========")


    log(ERR, "======== BEGIN MINUTE ========")
    wheel = _wheel.min
    log(ERR, "pointer = " .. wheel.pointer)
    log(ERR, "nelt = " .. wheel.nelt)
    for i, v in ipairs(wheel.array) do
        for _, value in pairs(v) do
            log(ERR, "timer: " .. _wheel.real_timer .. ", index = " .. i .. ", name = " .. value.name  .. 
                ", offset.second = " .. value.next_pointer.second ..
                ", offset.minute = " .. value.next_pointer.minute ..
                ", offset.hour = " .. value.next_pointer.hour)
        end
    end
    log(ERR, "========= END MINUTE =========")


    log(ERR, "======== BEGIN HOUR ========")
    wheel = _wheel.hour
    log(ERR, "pointer = " .. wheel.pointer)
    log(ERR, "nelt = " .. wheel.nelt)
    for i, v in ipairs(wheel.array) do
        for _, value in pairs(v) do
            log(ERR, "timer: " .. _wheel.real_timer .. ", index = " .. i .. ", name = " .. value.name .. 
                ", offset.second = " .. value.next_pointer.second ..
                ", offset.minute = " .. value.next_pointer.minute ..
                ", offset.hour = " .. value.next_pointer.hour)
        end
    end
    log(ERR, "========= END HOUR =========")
end


local function round(value, digits)
    local x = 10 * digits
    return floor(value * x + 0.5) / x
end


local function get_avg(cur_value, cur_count, old_avg)
    return old_avg + ((cur_value - old_avg) / cur_count)
end


local function get_variance(cur_value, cur_count, old_variance, old_avg)
    return (((cur_count - 1) / pow(cur_count, 2)) * pow(cur_value - old_avg, 2)) + (((cur_count - 1) / cur_count) * old_variance)
end


local function job_wrapper(job, wheel)
    local stats = job.stats
    local latency = stats.latency
    stats.runs = stats.runs + 1
    local start = now()

    job.callback(false, unpack(job.args))

    local finish = stats.finish + 1
    stats.finish = finish

    if FOCUS_UPDATE_TIME then
        update_timer()
    end

    local spend = now() - start

    latency.max = max(latency.max, spend)
    latency.min = min(latency.min, spend)

    local old_avg = latency.avg
    latency.avg = get_avg(spend, finish, old_avg)

    local old_variance = latency.variance
    latency.variance = get_variance(spend, finish, old_variance, old_avg)

    log(ERR, job)

end


local function wheel_init(nelt)
    local ret = {
        pointer = 0,
        nelt = nelt,
        array = {}
    }

    for i = 1, ret.nelt do
        ret.array[i] = setmetatable({ }, { __mode = "v" })
    end

    return ret
end


local function wheel_cal_pointer(wheel, pointer, offset)
    local nelt = wheel.nelt
    local p = pointer
    
    p = (p + offset) % (nelt + 1)

    if p == 0 then
        return 1, true
    end

    return p, false
end


local function wheel_get_cur_pointer(wheel)
    return wheel.pointer
end


local function wheel_insert(wheel, pointer, job)
    assert(wheel)
    assert(wheel.array)
    assert(pointer > 0)

    if not wheel.array[pointer][job.name] then
        wheel.array[pointer][job.name] = job

    else
        return nil, "already exists job"
    end

    return true, nil
end


local function wheel_insert_by_delay(wheel, delay, job)
    assert(wheel)
    assert(delay >= 0)

    local pointer, is_pointer_back_to_start = wheel_cal_pointer(wheel, wheel.pointer, delay)

    if not wheel.array[pointer][job.name] then
        wheel.array[pointer][job.name] = job
    else
        return nil, "already exists job"
    end

    return true, nil
end


local function wheel_move_to_next(wheel)
    assert(wheel)

    local pointer, is_pointer_back_to_start = wheel_cal_pointer(wheel, wheel.pointer, 1)
    wheel.pointer = pointer

    return wheel.array[wheel.pointer], is_pointer_back_to_start
end


local function job_re_cal_next_pointer(job, wheel)
    local delay_hour = job.delay.hour
    local delay_minute = job.delay.minute
    local delay_second = job.delay.second
    local delay_msec = job.delay.msec

    local hour_wheel = wheel.hour
    local minute_wheel = wheel.min
    local second_wheel = wheel.sec
    local msec_wheel = wheel.msec

    local cur_hour_pointer = wheel_get_cur_pointer(hour_wheel)
    local cur_minute_pointer = wheel_get_cur_pointer(minute_wheel)
    local cur_second_pointer = wheel_get_cur_pointer(second_wheel)
    local cur_msec_pointer = wheel_get_cur_pointer(msec_wheel)
    

    local next_hour_pointer = 0
    local next_minute_pointer = 0
    local next_second_pointer = 0
    local next_msec_pointer = wheel_cal_pointer(msec_wheel, cur_msec_pointer, delay_msec)

    if delay_hour ~= 0 then
        next_hour_pointer = wheel_cal_pointer(minute_wheel, cur_hour_pointer, delay_hour)
    end

    if delay_minute ~= 0 then
        next_minute_pointer = wheel_cal_pointer(minute_wheel, cur_minute_pointer, delay_minute)
    end

    if delay_second ~= 0 then
        next_second_pointer = wheel_cal_pointer(second_wheel, cur_second_pointer, delay_second)
    end

    job.next_pointer.hour = next_hour_pointer
    job.next_pointer.minute = next_minute_pointer
    job.next_pointer.second = next_second_pointer
    job.next_pointer.msec = next_msec_pointer
end


local function job_tostring(job)
    local str = ""
    
    local stats = job.stats
    local latency = stats.latency
    local meta = job.meta

    str = str .. "name = " .. job.name
    str = str .. ", latency.max = " .. latency.max
    str = str .. ", latency.min = " .. latency.min
    str = str .. ", latency.avg = " .. latency.avg
    str = str .. ", latency.variance = " .. latency.variance
    str = str .. ", meta.name = " .. meta.name

    return str
end


local function job_create_meta(job)
    local meta = job.meta
    local callstack = meta.callstack
    local base = 3

    for i = 1, 3 do
        local info = debug_getinfo(i + base, "nSl")

        if not info or info.short_src == "[C]" then
            break
        end

        callstack[i] = {
            line = info.currentline,
            func = info.name or info.what,
            source = info.short_src,
            where = info.namewhat
        }
    end

    local top_stack = callstack[1]

    if top_stack then
        meta.name = top_stack.source .. ":" .. top_stack.line .. ":" .. top_stack.func .. "()"
    end
end


local function job_create(wheel, name, callback, delay, once, args)
    local _, delay_msec = math.modf(delay)
    delay_msec = max(delay_msec * 100, 100)
    delay_msec = ceil(delay_msec)
    delay_msec = ceil(delay_msec / 100)

    log(ERR, delay_msec)

    delay, _ = math.modf(delay)

    local delay_hour = math.modf(delay / 60 / 60)
    delay = delay % (60 * 60)
    local delay_minute = math.modf(delay / 60)
    local delay_second = delay % 60

    local ret = {
        enable = true,
        name = name,
        callback = callback,
        delay = {
            hour = delay_hour,
            minute = delay_minute,
            second = delay_second,
            msec = delay_msec
        },
        next_pointer = {
            hour = 0,
            minute = 0,
            second = 0,
            msec = 0
        },
        once = once,
        args = args,
        stats = {
            latency = {
                avg = 0,
                max = -1,
                min = huge,
                variance = 0
            },

            runs = 0,
            finish = 0
        },
        meta = {
            name = "",
            callstack = {}
        }
    }

    job_create_meta(ret)

    job_re_cal_next_pointer(ret, wheel)

    setmetatable(ret, {
        __tostring = job_tostring
    })

    log(ERR, ret)

    return ret
end


local function insert_job_to_wheel(wheel, job)
    local ok, err

    local hour_wheel = wheel.hour
    local minute_wheel = wheel.min
    local second_wheel = wheel.sec
    local msec_wheel = wheel.msec
    

    if job.next_pointer.hour ~= 0 then
        ok, err = wheel_insert(hour_wheel, job.next_pointer.hour, job)
    
    elseif job.next_pointer.minute ~= 0 then
        ok, err = wheel_insert(minute_wheel, job.next_pointer.minute, job)
    
    elseif job.next_pointer.second ~= 0 then
        ok, err = wheel_insert(second_wheel, job.next_pointer.second, job)

    else
        ok, err = wheel_insert(msec_wheel, job.next_pointer.msec, job)
    end

    if not ok then
        return nil, err
    end

    return true, nil
end


local function worker_timer_callback(premature, self, wheel)
    while not exiting() do
        if premature then
            return
        end

        local hour_wheel = wheel.hour
        local minute_wheel = wheel.min
        local second_wheel = wheel.sec
        local msec_wheel = wheel.msec

        local threads = {
            count = 0,
            array = {}
        }


        local real_timer = wheel.real_timer

        local callbacks, continue = wheel_move_to_next(msec_wheel)

        for name, job in pairs(callbacks) do
            if job.enable then
                threads.count = threads.count + 1 
                threads.array[threads.count] = spawn(job_wrapper, job, wheel)
            end

            if not job.once then
                job_re_cal_next_pointer(job, wheel)
                insert_job_to_wheel(wheel, job)
            end

            callbacks[name] = nil
        end

        if continue then
            callbacks, continue = wheel_move_to_next(second_wheel)

            for name, job in pairs(callbacks) do
                wheel_insert(msec_wheel, job.next_pointer.msec, job)
                callbacks[name] = nil
            end

            if continue then
                callbacks, continue = wheel_move_to_next(minute_wheel)
    
                for name, job in pairs(callbacks) do
                    wheel_insert(second_wheel, job.next_pointer.second, job)
                    callbacks[name] = nil
                end

                callbacks, continue = wheel_move_to_next(hour_wheel)

                if continue then
                    for name, job in pairs(callbacks) do
                        wheel_insert(minute_wheel, job.next_pointer.minute, job)
                        callbacks[name] = nil
                    end
                end
            end
        end
    
        -- print_wheel(self, timer_index)
        -- log(ERR, "")

        wait(unpack(threads.array))
        
        real_timer.alive = true
        real_timer.counter.trigger = real_timer.counter.trigger + 1
    
        if real_timer.counter.trigger % self.opt.recreate_interval == 0 then
            timer_at(1, worker_timer_callback, self, wheel)
            break
        end
    
        sleep(0.1)
    end
end


local function master_timer_callback(premature, self)
    local init = true
    local sleep_count = 0

    while not exiting() do
        if premature then
            return
        end

        local real_timers = self.real_timers
        -- local wheels = self.wheels
        local opt_real_timer = self.opt.real_timer

        local wheels = self.wheels

        if init then
            init = false

            for i = 1, opt_real_timer do
                timer_at(0.1, worker_timer_callback, self, wheels[i])
                real_timers[i].alive = true
            end
        
        else
            for i = 1, opt_real_timer do
                real_timers[i].alive = false
            end

            if sleep_count < 5 then
                sleep(1)
            
            else
                for i = 1, opt_real_timer do
                    if not real_timers[i].alive then
                        timer_at(1, worker_timer_callback, self, wheels[i])
                    end
                end

            end

        end

    end
end

-- create a virtual timer
-- name: name of timer
-- once: is it run once
local function create(self ,name, callback, delay, once, args)
    -- like round-robin
    self.cur_real_timer_index = self.cur_real_timer_index == self.opt.real_timer and 1 or self.cur_real_timer_index + 1
    local cur = self.cur_real_timer_index
    local wheel = self.wheels[cur]

    if not name then
        name = tostring(math.random())
    end

    local job = job_create(wheel, name, callback, delay, once, args)

    self.jobs[name] = job

    return insert_job_to_wheel(wheel, job)
end


function _M:configure(options)
    ngx.eeeee[1] = 1

    math.randomseed(os.time())

    local opt = {
        -- max_expire = options and options.max_expire or DEFAULT_MAX_EXPIRE,
        max_expire = 24 * 60 * 60,

        -- restart a timer after a certain number of this timer triggers
        recreate_interval = options and options.recreate_interval or DEFAULT_RECREATE_INTERVAL,

        -- number of timer will be created by OpenResty API
        real_timer = options and options.real_timer or DEFAULT_REAL_TIMER
    }

    self.opt = opt

    -- enbale/diable entire timing system
    self.enable = false

    -- each real timer has it own wheel
    self.wheels = {}

    self.real_timers = {}

    self.jobs = {}

    -- the timer of the last job that was added
    -- see function create
    self.cur_real_timer_index = 1

    self.default_name = 1

    for i = 1, self.opt.real_timer do
        self.real_timers[i] = {
            index = i,
            alive = false,
            counter = {
                trigger = 0,
                delay = 0,
                fault = 0,
                recreate = 0,
            }
        }

        self.wheels[i] = {
            real_timer = self.real_timers[i],
            msec = wheel_init(10),
            sec = wheel_init(60),
            min = wheel_init(60),
            hour = wheel_init(24),
        }
    end
end


function _M:start()
    self.enable = true

    -- start the master timer
    -- the task of the master timer is to check the status of each worker timer,
    -- and if a woker timer crash, it will recreate a new one.
    timer_at(0, master_timer_callback, self)
end


function _M:stop()
    self.enable = false
end



function _M:once(name, callback, delay, ...)
    if delay == 0 then
        return timer_at(delay, callback, unpack({ ... }))
    end

    return create(self, name, callback, delay, true, { ... })
end


function _M:every(name, callback, interval, ...)
    if interval == 0 then
        return timer_every(interval, callback, unpack({ ... }))
    end

    return create(self, name, callback, interval, false, { ... })
end


function _M:run(name)
    local jobs = self.jobs
    local job = jobs[name]

    if job then
        
        if not job.enable then
            job.enable = true

        else
            return false, "running"
        end
    
    else
        return false, "timer not found"
    end

    return true, nil
end


function _M:pause(name)
    local jobs = self.jobs
    local job = jobs[name]

    if job then

        if not job.enable then
            job.enable = false

        else
            return false, "already paused"
        end
        
    
    else
        return false, "timer not found"
    end

    return true, nil
end


function _M:cancel(name)
    local jobs = self.jobs
    local job = jobs[name]

    if job then
        job.enable = false
    
    else
        return false, "timer not found"
    end

    jobs[name] = nil

    return true, nil
end


function _M:stats()
    local sys_latency = {
        max = -1,
        min = huge,
        avg = 0,
        variance = 0,
    }

    local jobs_latency = {}

    local fault = 0

    local count = 0

    for k, v in pairs(self.jobs) do
        count = count + 1
        local stats = v.stats
        local latency = stats.latency
        local old_sys_avg = sys_latency.avg
        sys_latency.max = max(latency.max)
        sys_latency.min = min(latency.min)
        sys_latency.avg = get_avg(latency.avg, count, old_sys_avg)
        sys_latency.variance = get_variance(sys_latency.avg, count, sys_latency.variance, old_sys_avg)
        fault = fault + stats.finish - stats.runs
    end

    return {
        count = count,
        fault = fault,
        sys_latency = sys_latency,
        jobs = self.jobs
    }
end


return _M