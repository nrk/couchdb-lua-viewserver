require 'luarocks.require'
local json = require 'json'

local funs, map_results, handlers = {}, {}, {}


local function slice(list, offset, length)
    local slice  = {}
    local length = length or (#list - offset + 1)
    local to = offset + length - 1
    for i = offset, to do slice[#slice + 1] = list[i] end
    return slice
end

local function output(data)
    io.stdout:write(json.encode(data), "\n")
    io.flush()
end

local function cdb_error(id, message)
    return {
        error = {
            id     = id,
            reason = message
        }
    }
end

local function eval(src)
    return loadstring("return " .. src)()
end

local function compile(source)
    local successful, result = pcall(eval, source)

    if successful and type(result) == 'function' then 
        return result 
    elseif not successful then
        raise('compilation_error', result)
    else
        raise('compilation_error', string.format('expression does not eval to a function (%s)', source))
    end
end

local function exec(fun, ...)
    local successful, result = pcall(fun, unpack(arg))

    if not successful then 
        if result ~= nil and type(result.error) ~= "function" then 
            if result.error.id == 'fatal_error' then
                raise('map_runtime_error', 'function raised fatal exception')
            else
                --log('function raised exception (' .. result.error.reason .. ') with doc._id ' .. doc._id)
                log('function raised exception (' .. result.error.reason .. ')')
            end
        else
            log('function raised exception (' .. (result or 'unknown') .. ')')
        end
    end

    return successful, result
end

local function handle_command(cmd)
    local cmd_handler = handlers[cmd[1]]

    if type(cmd_handler) == "function" then 
        local _, retval = pcall(cmd_handler, unpack(slice(cmd, 2)))
        return retval
    else
        return cdb_error('query_server_error', 'unknown command ' .. cmd[1])
    end
end


-- ********* functions accessible from views ********* -


function raise(id, message)
    error(cdb_error(id, message))
end

function raise_fatal(message)
    raise("fatal_error", message)
end

function log(message)
    output({ log = message })
end

function emit(key, value)
    map_results = { key, value }
end

function sum(values)
    local sum = 0
    for _, v in pairs(values) do sum = sum + v end
    return sum
end


-- ********* couchdb handlers ********* --


local function reset()
    funs = {}
    return true
end

local function add_fun(fun_source)
    table.insert(funs, compile(fun_source))
    return true
end

local function map_doc(doc)
    local results = {}

    for _, fun in pairs(funs) do
        map_results = {}
        local successful = exec(fun, doc)
        if successful then table.insert(results, map_results) else map_results = {} end
    end

    return { results }
end

local function reduce(reduce_funs, arguments, rereduce)
    local keys, values, reductions = {}, {}, {}

    if not rereduce then
        for _, kv in ipairs(arguments) do
            table.insert(keys, kv[1])
            table.insert(values, kv[2])
        end
    else
        keys, values = nil, arguments
    end

    for i, reduce_fun_src in ipairs(reduce_funs) do 
        local successful, result = exec(compile(reduce_fun_src), keys, values, rereduce)
        if not successful then result = nil end
        table.insert(reductions, i, result) 
    end

    return { true, reductions }
end

local function rereduce(reduce_funs, arguments)
    return reduce(reduce_funs, arguments, true)
end

local function validate(fun_source, new_doc, old_doc, user_ctx)
    compile(fun_source)(new_doc, old_doc, user_ctx)
    return 1
end


handlers = { 
    reset    = reset, 
    add_fun  = add_fun, 
    map_doc  = map_doc, 
    reduce   = reduce, 
    rereduce = rereduce,
    validate = validate,
}


-- ************************************ --


local function main_loop()
    while true do
        local line_from_stdin = io.stdin:read('*line')
        if not line_from_stdin then break end

        local successful, result = pcall(json.decode, line_from_stdin)
        if successful then output(handle_command(result)) else output(cdb_error('unknown_error', result)) end
    end
end

main_loop()
