local table = require 'ext.table'
local class = require 'ext.class'
local assert = require 'ext.assert'
local SQLField = require 'sql.field'

local SQLTable = class()

SQLTable.__name = 'SQLTable'

function SQLTable:init(args)
	self.name = assert(args.name)
	self.fields = table((assert(args.fields)))
	self.constraints = args.constraints

	for i=1,#self.fields do
		local field = SQLField(self.fields[i])
		self.fields[i] = field
		-- TODO separate table for this? meh?
		self.fields[field.name] = field
	end
end


-- generate a drop cmd
function SQLTable:dropcmd()
	return 'drop table '..self.name..';'
end

-- generate create
function SQLTable:createcmd()
	local ss = self.fields:mapi(function(field) return field:addcmd() end)
	if self.constraints then
		for _,constraint in ipairs(self.constraints) do
			ss:insert(constraint)
		end
	end
	return 'create table '..self.name..' ('..ss:concat', '..');'
end

--[[
generate insert
obj = keys and values to be inserted
	value sql formatting is done by matching the keys with the sql fields and formatting according to field type
--]]
function SQLTable:insertcmd(obj)
	local keys = table()
	local values = table()
	for k,v in pairs(obj) do
		keys:insert(k)
		local field = assert.index(self.fields, k, "tried to insert a column that isn't in the table")
		values:insert((field:serializeValue(v)))
	end
	return 'insert into '..self.name
		..' ('..keys:concat', '..') values ('
		..values:concat', '..');'
end

-- Make table:cmd functions to interact with whatever API is available first

local function wrap_resty(field)
	local fieldcmd = field..'cmd'
	SQLTable[field] = function(self, db, ...)
		local cmd = self[fieldcmd](self, ...)
ngx.log(ngx.ERR, '> '..cmd)
		return db:query(cmd)
	end
end

-- based on luasql-mysql (since it's run CLI):
local function wrap_luasql(field)
	local fieldcmd = field..'cmd'
	SQLTable[field] = function(self, conn, ...)
		local cmd = self[fieldcmd](self, ...)
print('> '..cmd)
		return conn:execute(cmd)
	end
end

local usingResty = pcall(require, 'resty.mysql')
for _,field in ipairs{'drop', 'create', 'insert'} do
	if usingResty then
		wrap_resty(field)
	else
		wrap_luasql(field)
	end
end

return SQLTable
