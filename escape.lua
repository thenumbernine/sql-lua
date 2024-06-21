-- TODO put this in sql.table or sql.escape or somewhere

local function sqlEscape(s)
	s = "'"..s:gsub("'", "''").."'"
	return s
end

return sqlEscape
