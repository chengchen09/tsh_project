json = require('json')
JSONS = require('test_pass1')

local function compare_table(t1,t2)
    if t1==t2 then 
        return true 
    end
    if (type(t1) ~= "table") then 
        return false 
    end
    local mt1 = getmetatable(t1)
    local mt2 = getmetatable(t2)
    if (not compare_table(mt1,mt2)) then 
        return false 
    end

    for k1,v1 in pairs(t1) do
        local v2 = t2[k1]
        if (not compare_table(v1,v2)) then 
            return false 
        end
    end
    for k2,v2 in pairs(t2) do
        local v1 = t1[k2]
        if (not compare_table(v1,v2)) then 
            return false 
        end
    end
    return true  
end

fn = './test_pass'

for i, v in ipairs(JSONS) do
    local tt = json.loads(v)

    -- json format
    local jfile = fn .. i .. '.json'
    local f = io.open(jfile, 'w')
    json.dump(tt, f)
    f:close()

    f = io.open(jfile, 'r')
    local jtt = json.load(f)
    f:close()
    if not compare_table(tt, jtt) then
        print('tt and jtt is not equal')
    end
    -- lua format
    local lfile = fn .. i .. '.lj'
    f = io.open(lfile, 'w')
    json.dump_lua(tt, f)
    f:close()

    f = io.open(lfile, 'r')
    local ltt = json.load_lua(f)
    f:close()
    if not compare_table(tt, ltt) then
        print('tt and ltt is not equal')
    end
end
print('test dump load operation passed')
