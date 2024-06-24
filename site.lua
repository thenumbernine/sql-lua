local class = require 'ext.class'
local table = require 'ext.table'
local os = require 'ext.os'
local template = require 'template'
local SQLTable = require 'sql.table'

local SQLSite = class()

--[[
args:
	dbname
	username
	password
	tables = table of SQLTable's
--]]
function SQLSite:init(args)
	self.dbname = assertindex(args, 'dbname')
	self.username = assertindex(args, 'username')
	self.password = assertindex(args, 'password')
	self.tables = table.mapi(assertindex(args, 'tables'), function(t)
		return (assert(SQLTable:isa(t)))
	end)
	self.tableForName = self.tables:mapi(function(t) return t, t.name end):setmetatable(nil)
end

-- runs mysql cli to create the db and username
-- I'd script this but idk how to, since luasql-mysql requires you to be connected to a database in the first place
function SQLSite:createDB()
	local s = template([[
create database <?=dbname?>;
create user "<?=username?>"@"localhost" identified by "<?=password?>";
use <?=dbname?>;
grant all privileges on <?=dbname?> to "<?=username?>"@"localhost";
flush privileges;
]], self):gsub('\s*\n\s*', ' ')
	assert(not s:find("'"))	-- or escape them either way

	os.exec("mysql -u "..self.username.." -p -e '"..s.."'")
end

--[[
this requires luasql-mysql to be installed

it also requires :createDB() to be run first

--]]
function SQLSite:rebuildTables()
	local luasql = require 'luasql.mysql'
	local sqlEscape = require 'sql.escape'
	local SQLTable = require 'sql.table'

	local env = assert(luasql.mysql())

	local dbname = self.dbname
	local user = self.username
	local pass =  self.password

	-- in luasql I have to connect to at least one database at all times? weird.
	local conn = assert(env:connect(dbname, user, pass))
	local echo = function(cmd)o
		print('> '..cmd)
		return conn:execute(cmd)
	end

	local tableSize = function(name)
		-- boy, luasql select is a much bigger pain in the ass compared to resty mysql
		local cursor = assert(echo('select count(*) from '..name..';'))
		--print('col names:', table.concat(cursor:getcolnames(), ', '))
		local row = cursor:fetch({}, 'a')
		-- why is count(*) lowercase when COLUMN_NAMES is uppercase?
		local count = assert(tonumber(row['count(*)']))
		--print('row', type(count), count)
		return count
	end

	local tableExists = function(name)
		-- boy, luasql select is a much bigger pain in the ass compared to resty mysql
		local cursor = assert(echo("select count(*) from information_schema.tables where table_schema="..sqlEscape(dbname).." and table_name="..sqlEscape(name)..";"))
		--print('col names:', table.concat(cursor:getcolnames(), ', '))
		local row = cursor:fetch({}, 'a')
		local count = assert(tonumber(row['count(*)']))
		--print('row', type(count), count)
		return count > 0
	end

	local sqldesc = require 'sqldesc'

	for _,t in ipairs(sqldesc) do
		--[[ remake all tables and wipe everything
		t:drop(conn)	-- don't assert in case it's not there
		assert(t:create(conn))
		--]]
		
		--[[ alter them until they match ... ?
		assert(echo('create table if not exists '..t.name..';'))
		for _,field in ipairs(t.fields) do
			assert(echo('alter table '..t.name..' add '..field:addcmd()..';'))
		end
		-- TODO query table for its fields, drop the ones not present
		-- 'select * from information_schema.columns where table_name='users' and table_schema='..sqlEscape(dbname)..';'
		-- but ... there's so many properties ... how to ensure they all match ...
		-- sql why ...
		error'not finished'
		--]]

		--[[ migrate from old db to new .. .doesn't work because mysql doesn't like renaming databases
		assert(echo('insert into '..dbdst..'.'..t.name..' select * from '..dbsrc..'.'..t.name..';'))
		--]]

		-- [[ make a staging table, copy over, then rename original to _old and rename staging to original
		local tmp = SQLTable(table(t, {
			name = 'new_'..t.name,
		}))
		assert(tmp:create(conn))
		
		-- only copy over if the old table exists
		if tableExists(t.name) 
		and tableSize(t.name) > 0
		then
			local cursor = assert(echo('select column_name from information_schema.columns where table_schema='..sqlEscape(dbname)..' and table_name='..sqlEscape(t.name)..';'))
print('col names:', table.concat(cursor:getcolnames(), ', '))
			local oldFields = {}
			local row = cursor:fetch({}, 'a')
			while row do
				-- 'count(*)' is preserved lowercase, but COLUMN_NAME is preserved uppercase .... hmmmmmmm
				oldFields[tostring(row.COLUMN_NAME)] = true
				
				row = cursor:fetch(row, 'a')
			end
print('got old fields ', table.keys(oldFields):concat', ')
			if not next(oldFields) then
				error('somehow there are no table columns to migrate')
			end
			assert(echo('insert into '..tmp.name
				..' select '
				..tmp.fields
					:mapi(function(field) return field.name end)
					:filter(function(fieldName) return oldFields[fieldName] end)	-- filter out fields missing from the original table
					:concat', '
				..' from '..t.name..';'))
		end
		--]]
	end

	-- [[ TODO here if all is well then rename (/drop old?) the tables
	-- NOTICE THAT IF I USED FOREIGN KEY RELATIONSHIPS THEN THEY'D ALL GET FUCKED UP.  TOO BAD I CAN'T JUST RELATE VIA TABLE NAMES AND SWAP OUT DATABASES.  THAT'D JUST MAKE TOO MUCH FUCKING SENSE.
	-- move old tables to old_
	-- TODO search for old#_ prefix availble and use that one
	for _,t in ipairs(sqldesc) do
		if tableExists(t.name) then
			assert(echo('rename table '..t.name..' to old_'..t.name..';'))
		end
	end
	-- move new_ to tables
	for _,t in ipairs(sqldesc) do
		assert(echo('rename table new_'..t.name..' to '..t.name..';'))
	end
	--]]

	conn:close()
	env:close()
end

return SQLSite
