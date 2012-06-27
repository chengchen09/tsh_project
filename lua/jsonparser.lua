--[[
--Author: Chen Cheng
--Description:
--]]

local ESCAPE_STR = ' \n\t\r'
local QUOTE_STR = '"\\/bfnrtu'
local ERR_MSG = 'json parse error'
local parse_array
local parse_object

local function null()
    return null
end

local function get(ss, index)
    return string.sub(ss, index, index)
end

local function parse_error(msg, ss, index)
    local line = 0
    local col = 0
    local ts = string.sub(ss, 1, index)
    _, line = string.gsub(ts, '\n', '')
    line = line + 1
    if line == 1 then
        col = index
    else
        local rts = string.reverse(ts)
        local pos = 0
        pos = string.find(rts, '\n')
        col = pos - 1
    end
    print(string.format('%s: line %d colunm %d char %s', msg, line, col, get(ss, index)))
    assert(nil)
end

local function escape(ss, index)
    while index <= string.len(ss) and 
        string.find(ESCAPE_STR, get(ss, index), nil, true) do
        index = index + 1
    end
    return index
end

local function parse_number(ss, index)
    local b = index
    local e = index
    end_str = ESCAPE_STR .. ',}]'
    while string.find(end_str, get(ss, e), nil, true) == nil do
        e = e + 1
    end
    local ns = string.sub(ss, b, e - 1)
    n = tonumber(ns)
    index = e
    if n == nil then
        parse_error(ERR_MSG, ss, index)
    end
    return n, index
end

local function parse_string(ss, index)
    local b = index
    local e = index
    while get(ss, e) ~= '"' do
        if get(ss, e) == '\\' then
            e = e + 1
            if string.find(QUOTE_STR, get(ss, e), nil, true) == nil then
                parse_error(ERR_MSG, ss, index)
            end
        end
        e = e + 1
    end
    index = e + 1
    return string.sub(ss, b, e - 1), index
end

local function parse_value(ss, index)
    local v = nil
    local c = get(ss, index)
    if c == '{' then
        index = index + 1
        v, index = parse_object(ss, index)
    elseif c == '[' then
        index = index + 1
        v, index = parse_array(ss, index)
    elseif c == '"' then
        index = index + 1
        v, index = parse_string(ss, index)
    elseif c == 'n' and string.sub(ss, index, index + 3) == 'null' then
        index = index + 4
        v = null
    elseif c == 't' and string.sub(ss, index, index + 3) == 'true' then
        index = index + 4
        v = true
    elseif c == 'f' and string.sub(ss, index, index + 4) == 'false' then
        index = index + 5
        v = false
    else
        v, index = parse_number(ss, index)
    end
    return v, index
end

parse_array = function (ss, index)
    local ll = {}
    ll[0] = 'array'
    index = escape(ss, index)
    if get(ss, index) == ']' then
        index = index + 1
        return ll, index
    end
    local pos = 1
    while true do
        ll[pos], index = parse_value(ss, index)
        pos = pos + 1
        index = escape(ss, index)
        if get(ss, index) == ',' then
            index = index + 1
            index = escape(ss, index)
        elseif get(ss, index) == ']' then
            index = index + 1
            break
        else
            parse_error(ERR_MSG, ss, index)
        end
    end
    return ll, index
end

parse_object = function (ss, index)
    local tt = {}
    tt[0] = 'object'
    index = escape(ss, index)
    if get(ss, index) == '}' then
        index = index + 1
        return tt, index
    elseif get(ss, index) ~= '"' then
        parse_error(ERR_MSG, ss, index)
    end
    index = index + 1
    local key = ''
    while true do
        key, index = parse_string(ss, index)
        index = escape(ss, index)
        if get(ss, index) ~= ':' then
            parse_error(ERR_MSG, ss, index)
        end
        index = index + 1
        index = escape(ss, index)
        tt[key], index = parse_value(ss, index)
        index = escape(ss, index)
        if get(ss, index) == '}' then
            index = index + 1
            break
        elseif get(ss, index) == ',' then
            index = index + 1
            index = escape(ss, index)
        end

        if get(ss, index) ~= '"' then
            parse_error(ERR_MSG, ss, index)
        end
        index = index + 1
    end
    return tt, index
end

local function parse(ss)
    local index = 1
    local tt = {}
    index = escape(ss, index)
    local c = get(ss, index)
    index = index + 1
    if c == '{' then
        tt, index = parse_object(ss, index)
    elseif c == '[' then
        tt, index = parse_array(ss, index)
    end 
    index = escape(ss, index)
    if index <= string.len(ss) then
        parse_error(ERR_MSG .. '--extra data', ss, index)
    end
    return tt
end

local jsonparser = {
    parse = parse,
    null = null
}

return jsonparser
