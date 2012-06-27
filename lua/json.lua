--[[
    Author: Chen Cheng
    Description:
]]

jsonparser = require('jsonparser')

local function jv_to_str(jv)
    local ss = ''
    if type(jv) == type('') then
        ss = '"' .. jv .. '"'
    elseif type(jv) == type({}) then
        if jv[0] == 'object' then
            local key_flag = false
            ss = '{'
            for k, v in pairs(jv) do
                if type(k) == type('') then
                    ss = ss .. '"' .. k .. '":' .. jv_to_str(v) .. ','
                end
            end
            if string.sub(ss, -1, -1) == ',' then
                ss = string.sub(ss, 1, -2)
            end
            ss = ss .. '}'
        elseif jv[0] == 'array' then
            ss = '['
            for i, v in ipairs(jv) do
                ss = ss .. jv_to_str(v) .. ','
            end
            if string.sub(ss, -1, -1) == ',' then
                ss = string.sub(ss, 1, -2)
            end
            ss = ss .. ']'
        else
            assert(nil)
        end
    elseif jv == true then
        ss = 'true'
    elseif jv == false then
        ss = 'false'
    elseif jv == jsonparser.null then
        ss = 'null'
    else
        ss = ss .. jv
    end
    return ss
end

local function jv_to_formatstr(jv, space)
    if space == nil then
        space = ''
    end
    local ss = ''
    if type(jv) == type('') then
        ss = '"' .. jv .. '"'
    elseif type(jv) == type({}) then
        if jv[0] == 'object' then
            local key_flag = false
            ss = '{\n'
            space = space .. '  '
            for k, v in pairs(jv) do
                if type(k) == type('') then
                    ss = ss .. space .. '"' .. k .. '": ' .. jv_to_formatstr(v, space) .. ',\n'
                end
            end
            space = string.sub(space, 1, -3)
            if string.sub(ss, -2, -1) == ',\n' then
                ss = string.sub(ss, 1, -3) .. '\n' .. space .. '}'
            else
                ss = string.sub(ss, 1, -2) .. '}'
            end
        elseif jv[0] == 'array' then
            ss = '[\n'
            space = space .. '  '
            for i, v in ipairs(jv) do
                ss = ss .. space .. jv_to_formatstr(v, space) .. ',\n'
            end
            space = string.sub(space, 1, -3)
            if string.sub(ss, -2, -1) == ',\n' then
                ss = string.sub(ss, 1, -3) .. '\n' .. space .. ']'
            else
                ss = string.sub(ss, 1, -2) .. ']'
            end
        else
            assert(nil)
        end
    elseif jv == true then
        ss = 'true'
    elseif jv == false then
        ss = 'false'
    elseif jv == jsonparser.null then
        ss = 'null'
    else
        ss = ss .. jv
    end
    return ss
end

local function str(tt)
    local ss = ''
    if tt == nil then
        ss = nil
    else
        ss = jv_to_str(tt)
    end
    return ss
end

local function formatstr(tt)
    local ss = ''
    if tt == nil then
        ss = nil
    else
        ss = jv_to_formatstr(tt)
    end
    return ss
end

local function load(f)
    local tt = nil
    ss = f:read('*all')
    tt = jsonparser.parse(ss)
    return tt
end

local function loads(ss)
    return jsonparser.parse(ss)
end

local function load_lua(f)
    local tt = nil
    ss = f:read('*all')
    tt = jsonparser.parse(ss)
    return tt
end

local function dump(tt, f)
    local ss = formatstr(tt)
    f:write(ss)
end

local function dumps(tt)
    return formatstr(tt)
end

local function dump_lua(tt, f)
    local ss = str(tt)
    f:write(ss)
end

local json = {
    load = load,
    loads = loads,
    load_lua = load_lua,
    dump = dump,
    dumps = dumps,
    dump_lua = dump_lua
}

return json
