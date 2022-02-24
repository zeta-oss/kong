local timer_at = ngx.timer.at
local timer_every = ngx.timer.every
local sleep = ngx.sleep
local exiting = ngx.worker.exiting

local unpack = table.unpack

local log = ngx.log

local ERR = ngx.ERR
local DEBUG = ngx.DEBUG

local wait = ngx.thread.wait
local kill = ngx.thread.kill
local spawn = ngx.thread.spawn

local DEFAULT_THREADS = 1000
local DEFAULT_MAX_EXPIRE = 24 * 60 * 60
local DEFAULT_RECREATE_INTERVAL = 50
local DEFAULT_REAL_TIMER = 10

local _M = {}


local function print_wheel(self, timer_index)
    local wheel
    log(ERR, "======== BEGIN SECOND ========")
    wheel = self.wheels_for_each_real_timer[timer_index].second_wheel
    log(ERR, "pointer = " .. wheel.pointer)
    log(ERR, "nelt = " .. wheel.nelt)
    for i, v in ipairs(wheel.array) do
        for _, value in pairs(v) do
            log(ERR, "timer: " .. timer_index .. ", index = " .. i .. ", name = " .. value.name .. 
                ", offset.second = " .. value.next_pointer.second ..
                ", offset.minute = " .. value.next_pointer.minute ..
                ", offset.hour = " .. value.next_pointer.hour)
        end
    end
    log(ERR, "========= END SECOND =========")


    log(ERR, "======== BEGIN MINUTE ========")
    wheel = self.wheels_for_each_real_timer[timer_index].minute_wheel
    log(ERR, "pointer = " .. wheel.pointer)
    log(ERR, "nelt = " .. wheel.nelt)
    for i, v in ipairs(wheel.array) do
        for _, value in pairs(v) do
            log(ERR, "timer: " .. timer_index .. ", index = " .. i .. ", name = " .. value.name  .. 
                ", offset.second = " .. value.next_pointer.second ..
                ", offset.minute = " .. value.next_pointer.minute ..
                ", offset.hour = " .. value.next_pointer.hour)
        end
    end
    log(ERR, "========= END MINUTE =========")


    log(ERR, "======== BEGIN HOUR ========")
    wheel = self.wheels_for_each_real_timer[timer_index].hour_wheel
    log(ERR, "pointer = " .. wheel.pointer)
    log(ERR, "nelt = " .. wheel.nelt)
    for i, v in ipairs(wheel.array) do
        for _, value in pairs(v) do
            log(ERR, "timer: " .. timer_index .. ", index = " .. i .. ", name = " .. value.name .. 
                ", offset.second = " .. value.next_pointer.second ..
                ", offset.minute = " .. value.next_pointer.minute ..
                ", offset.hour = " .. value.next_pointer.hour)
        end
    end
    log(ERR, "========= END HOUR =========")
end


local function wheel_init(nelt)
    local ret = {
        pointer = 0,
        nelt = nelt,
        array = {}
    }

    for i = 1, ret.nelt do
        ret.array[i] = {}
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
    assert(pointer > 0)

    if not wheel.array[pointer][job.name] then
        wheel.array[pointer][job.name] = job
    else
        return nil, "already exists job"
    end

    return true, nil
end


local function wheel_insert_with_delay(wheel, delay, job)
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


local function wheel_move_to_next(self, timer_index, wheel, callback)
    assert(wheel)
    assert(callback)

    local pointer, is_pointer_back_to_start = wheel_cal_pointer(wheel, wheel.pointer, 1)
    
    wheel.pointer = pointer

    local jobs = wheel.array[wheel.pointer]

    for k, v in pairs(jobs) do
        callback(self, timer_index, v)
        jobs[k] = nil
    end

    return is_pointer_back_to_start
end


local function job_re_cal_next_pointer(self, timer_index, job)
    local delay_hour = job.delay.hour
    local delay_minute = job.delay.minute
    local delay_second = job.delay.second

    local second_wheel = self.wheels_for_each_real_timer[timer_index].second_wheel
    local minute_wheel = self.wheels_for_each_real_timer[timer_index].minute_wheel
    local hour_wheel = self.wheels_for_each_real_timer[timer_index].hour_wheel

    local cur_second_pointer = wheel_get_cur_pointer(second_wheel)
    local cur_minute_pointer = wheel_get_cur_pointer(minute_wheel)
    local cur_hour_pointer = wheel_get_cur_pointer(hour_wheel)

    local next_hour_pointer = 0
    local next_minute_pointer = 0
    local next_second_pointer = wheel_cal_pointer(second_wheel, cur_second_pointer, delay_second)

    if delay_minute ~= 0 then
        next_minute_pointer = wheel_cal_pointer(minute_wheel, cur_minute_pointer, delay_minute)
    end

    if delay_hour ~= 0 then
        next_hour_pointer = wheel_cal_pointer(minute_wheel, cur_hour_pointer, delay_hour)
    end

    job.next_pointer.hour = next_hour_pointer
    job.next_pointer.minute = next_minute_pointer
    job.next_pointer.second = next_second_pointer
end

local function job_create(self, timer_index, name, callback, delay, once, args)
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
            second = delay_second
        },
        next_pointer = {
            hour = 0,
            minute = 0,
            second = 0,
        },
        once = once,
        args = args
    }

    job_re_cal_next_pointer(self, timer_index, ret)

    return ret
end


local function insert_job_to_wheel(self, timer_index, job)
    local ok, err

    local second_wheel = self.wheels_for_each_real_timer[timer_index].second_wheel
    local minute_wheel = self.wheels_for_each_real_timer[timer_index].minute_wheel
    local hour_wheel = self.wheels_for_each_real_timer[timer_index].hour_wheel

    if job.next_pointer.hour ~= 0 then
        ok, err = wheel_insert(hour_wheel, job.next_pointer.hour, job)
    
    elseif job.next_pointer.minute ~= 0 then
        ok, err = wheel_insert(minute_wheel, job.next_pointer.minute, job)
    
    else
        ok, err = wheel_insert(second_wheel, job.next_pointer.second, job)
    end

    if not ok then
        return nil, err
    end

    return true, nil
end


local function second_wheel_callback(self, timer_index, job)
    if not job.enable then
        insert_job_to_wheel(self, timer_index, job)
    end

    if self.enable then
        spawn(job.callback, false, unpack(job.args))
    end


    if not job.once then
        job_re_cal_next_pointer(self, timer_index, job)
        insert_job_to_wheel(self, timer_index, job)
    end
    
end


local function minute_wheel_callback(self, timer_index, job)
    if not job.enable then
        insert_job_to_wheel(self, job)
    end

    local second_wheel = self.wheels_for_each_real_timer[timer_index].second_wheel

    wheel_insert(second_wheel, job.next_pointer.second, job)
end


local function hour_wheel_callback(self, timer_index, job)
    if not job.enable then
        insert_job_to_wheel(self, job)
    end

    local minute_wheel = self.wheels_for_each_real_timer[timer_index].minute_wheel

    wheel_insert(minute_wheel, job.next_pointer.minute, job)

end


local function worker_timer_callback(premature, self, timer_index)
    while not exiting() do
        if premature then
            return
        end

        local second_wheel = self.wheels_for_each_real_timer[timer_index].second_wheel
        local minute_wheel = self.wheels_for_each_real_timer[timer_index].minute_wheel
        local hour_wheel = self.wheels_for_each_real_timer[timer_index].hour_wheel
    
        if wheel_move_to_next(self, timer_index, second_wheel, second_wheel_callback) then
            if wheel_move_to_next(self, timer_index, minute_wheel, minute_wheel_callback) then
                wheel_move_to_next(self, timer_index, hour_wheel, hour_wheel_callback)
            end
        end
    
        -- print_wheel(self, timer_index)
        -- log(ERR, "")
        
        self.timer_alive_flag[timer_index] = true
        self.timer_run_count[timer_index] = self.timer_run_count[timer_index] + 1
    
        if self.timer_run_count[timer_index] > self.opt.recreate_interval then
            timer_at(1, worker_timer_callback, self, timer_index)
            break
        end
    
        sleep(1)
    end
end


local function master_timer_callback(premature, self)
    local init = true
    local sleep_count = 0

    while not exiting() do
        if premature then
            return
        end

        if init then
            init = false
            for i = 1, self.opt.real_timer do
                timer_at(1, worker_timer_callback, self, i)
            end
        
        else
            for i = 1, self.opt.real_timer do
                self.timer_alive_flag[i] = false
            end

            if sleep_count < 5 then
                sleep(1)
            
            else
                for i = 1, self.opt.real_timer do
                    if not self.timer_alive_flag[i] then
                        timer_at(1, worker_timer_callback, self, i)
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
    local old = self.cur_real_timer_index
    self.cur_real_timer_index = self.cur_real_timer_index == self.opt.real_timer and 1 or self.cur_real_timer_index + 1

    local job = job_create(self, old, name, callback, delay, once, args)
    return insert_job_to_wheel(self, old, job)
end


function _M:configure(options)
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

    -- number of times each timer is triggered
    self.timer_run_count = {}

    -- the worker timer will be set to true on every trigger,
    -- and the master timer will be set to false on every trigger
    self.timer_alive_flag = {}

    -- each real timer has it own wheel
    self.wheels_for_each_real_timer = {}

    -- the timer of the last job that was added
    -- see function create
    self.cur_real_timer_index = 1

    self.default_name = 1

    for i = 1, self.opt.real_timer do
        self.timer_run_count[i] = 0
        self.timer_alive_flag[i] = true

        self.wheels_for_each_real_timer[i] = {}
        self.wheels_for_each_real_timer[i].second_wheel = wheel_init(60)
        self.wheels_for_each_real_timer[i].minute_wheel = wheel_init(60)
        self.wheels_for_each_real_timer[i].hour_wheel = wheel_init(math.modf(self.opt.max_expire / 60 / 60))
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


 
function _M:create_once(name, callback, delay, ...)
    if delay < 1 then
        return timer_at(delay, callback, unpack({ ... }))
    end

    if not name then
        name = tostring(math.random())
    end
    return create(self, name, callback, delay, true, { ... })
end


function _M:create_every(name, callback, interval, ...)
    if interval < 1 then
        return timer_every(interval, callback, unpack({ ... }))
    end

    if not name then
        name = tostring(math.random())
    end
    return create(self, name, callback, interval, false, { ... })
end


return _M