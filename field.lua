local class = require 'ext.class'
local sqlEscape = require 'sql.escape'

local SQLField = class()

SQLField.__name = 'SQLField'

function SQLField:init(args)
	for k,v in pairs(args) do
		self[k] = v
	end
end

function SQLField:addcmd()
	local s = self.name
		.. ' ' .. self.type
	if self.attr then
		s = s .. ' ' .. self.attr
	end
	return s
end

function SQLField:serializeValue(v)
	if self.type == 'text'
	or self.type:match'^varchar'
	then
		return sqlEscape(v)
	end

	if self.type == 'timestamp' then
		-- TODO decimals?
		--local t = os.date('*t', v)
		--return os.date("!'%Y-%m-%d %H:%M:%S'", v)
		-- or do I assume it's a date string?
		return sqlEscape(v)
	end

	return v
end

return SQLField
