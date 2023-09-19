-- $RCSfile$
--
-- $Revision$
--
-- $Id$
--
-- Copyright (c) 2014, 2016 Alcatel-Lucent. All rights reserved.
-- Please read the associated COPYRIGHTS file for more details.
--

-- To enable debug logging, add a line to aerospike.conf in the logging file
-- declaration:
--   context query debug

-- Op codes in the filter expression; see ASCriteria.OpCode.
local EQ = 0
local BETWEEN = 1
local AND = 2
local OR = 3
local NOT = 4
local WITHIN = 5
local EXISTS = 6

-- These are used to determine the type of collection so we can iterate
-- properly on a wildcard path component.
local LIST_META = getmetatable(list())
local MAP_META = getmetatable(map())

-- Reads the next token from the expression table.
--
-- in:
--   aInExpression - the expression token table
--
-- return:
--   the next token value
local function readNext(aInExpression)
    local i = aInExpression.index
    debug("Reading index " .. i .. " of " .. aInExpression.length)
    if i > aInExpression.length then
        return nil
    end
    aInExpression.index = i + 1
    return aInExpression[i]
end

-- Read an array from the expression.  An array consists of two entries, the
-- first being the number of elements, the second being the array.  This is
-- necessary as we can't extract the number of elements from the array itself.
--
-- in:
--   aInExpression - the expression token table
--
-- return:
--   the array object, containing two properties:
--     length - the array length
--     array  - the actual array, with indices from 1 to length
local function readArray(aInExpression)
    local lNumComponents = readNext(aInExpression)
    local lArray = readNext(aInExpression)

    return {length = lNumComponents,
            array = lArray}
end

-- Add an object to the end of the list.
--
-- in:
--   aInObject     - the object to add to the list
--   aInOutObjects - the list to add it to
local function addObject(aInObject, aInOutObjects)
    local lIndex = aInOutObjects.length + 1
    aInOutObjects.length = lIndex
    aInOutObjects[lIndex] = aInObject
    if aInObject == nil then
        debug('Added nil to list')
    elseif getmetatable(aInObject) == LIST_META then
        debug("Added list value to list")
    elseif getmetatable(aInObject) == MAP_META then
        debug("Added map value to list");
    else
        debug('Added value ' .. aInObject .. ' to list')
    end
end

-- Retrieve the object values that match the path.
--
-- in:
--   aInObject     - the object to retrieve the values from
--   aInPath       - the path components to traverse
--   aInIndex      - the current index of path to match against
--   aInOutObjects - the list of objects to add matching values to
--   aInExists     - check for existence only?
local function getObjects(aInObject, aInPath, aInIndex, aInOutObjects, aInExists)
    if aInObject == nil then
        return
    end
    if aInIndex > aInPath.length then
        -- End of the line - add it.
        addObject(aInObject, aInOutObjects)
        return
    end
    local lKey = aInPath.array[aInIndex]
    if lKey == nil then
        debug('Iterating over all values of object...')
        if (getmetatable(aInObject) == LIST_META) then
            local lIndex = 1
            for lValue in list.iterator(aInObject) do
                debug('Traversing down at list index ' .. lIndex)
                getObjects(lValue, aInPath, aInIndex + 1, aInOutObjects,
                           aInExists)
                if (aInExists and aInOutObjects.length > 0) then
                    break
                end
                lIndex = lIndex + 1
            end
        elseif (getmetatable(aInObject) == MAP_META) then
            for lIndex, lValue in map.pairs(aInObject) do
                debug('Traversing down at map key ' .. lIndex)
                getObjects(lValue, aInPath, aInIndex + 1, aInOutObjects,
                           aInExists)
                if (aInExists and aInOutObjects.length > 0) then
                    break
                end
            end
        end
    else
        debug('Traversing down at specified key ' .. lKey)
        local lObject = nil
        if (getmetatable(aInObject) == LIST_META) then
            -- List indices are 1-based in lua, so offset here
            lObject = aInObject[tonumber(lKey) + 1]
        elseif (getmetatable(aInObject) == MAP_META) then
            lObject = aInObject[lKey]
            if lObject == nil then
                -- No value, so try it as a numeric key
                lKey = tonumber(lKey)
                if lKey ~= nil then
                    lObject = aInObject[lKey]
                end
            end
        else
            -- have to assume record
            lObject = aInObject[lKey]
        end
        getObjects(lObject, aInPath, aInIndex + 1, aInOutObjects, aInExists)
    end
end

-- Based on the attribute/key/index path given, find the values in the record.
--
-- in:
--   aInRecord - the record to find the value within
--   aInPath   - the attribute/key/index path
--   aInExists     - check for existence only?
--
-- return:
--   the value(s)
local function getPath(aInRecord, aInPath, aInExists)
    local lObjects = {length = 0, array = {}}
    getObjects(aInRecord, aInPath, 1, lObjects, aInExists)
    return lObjects
end

-- Applies the next operation in the expression token table.  It is assumed
-- that the next available token will be an operation code.
--
-- in:
--   aInRecord     - the record to apply the operation to
--   aInApply      - does expression evaluation actually need to be performed,
--                   or does it suffice to just consume the tokens
--   aInExpression - the expression token table
--
-- return:
--   true if the filter expression evaluates to true, false if not, and nil if
--   apply is false
local function applyNext(aInRecord, aInApply, aInExpression)
    local lOpCode = readNext(aInExpression)

    local lApply
    if aInApply then
        lApply = "true"
    else
        lApply = "false"
    end
    debug("Executing opcode " .. lOpCode .. "; apply=" .. lApply)

    if lOpCode == EQ then
        local lPath = readArray(aInExpression)
        local lExpectedValues = readArray(aInExpression)
        if aInApply then
            local lValues = getPath(aInRecord, lPath, false)
            for i = 1, lExpectedValues.length do
                local lExpValue = lExpectedValues.array[i]
                for lIndex, lValue in ipairs(lValues) do
                    debug("Comparing " .. lExpValue .. " to " .. lValue)
                    if lExpValue == lValue then
                        return true
                    end
                end
            end
            return false
        else
            return nil
        end

    elseif lOpCode == BETWEEN then
        local lPath = readArray(aInExpression)
        local lLower = readNext(aInExpression)
        local lUpper = readNext(aInExpression)
        if aInApply then
            local lValues = getPath(aInRecord, lPath, false)
            for lIndex, lValue in ipairs(lValues) do
                if (lLower == nil or lValue >= lLower) and
                   (lUpper == nil or lValue <= lUpper) then
                    return true
                end
            end
            return false
        else
            return nil
        end

    elseif lOpCode == AND then
        local n = readNext(aInExpression)
        debug("ANDing " .. n .. " operands")
        local lResult = true
        for i = 1, n do
            local lNextResult = applyNext(aInRecord, aInApply, aInExpression)
            if aInApply and not lNextResult then
                lResult = false
                aInApply = false
            end
        end
        return lResult

    elseif lOpCode == OR then
        local n = readNext(aInExpression)
        debug("ORing " .. n .. " operands")
        local lResult = false
        for i = 1, n do
            local lNextResult = applyNext(aInRecord, aInApply, aInExpression)
            if aInApply and lNextResult then
                lResult = true
                aInApply = false
            end
        end
        return lResult

    elseif lOpCode == NOT then
        local lNextResult = applyNext(aInRecord, aInApply, aInExpression)
        if aInApply then
            return not lNextResult
        else
            return nil
        end

    elseif lOpCode == WITHIN then
        local lPath = readArray(aInExpression)
        -- save the index
        local lSavedIndex = aInExpression.index
        -- have to do a dummy applyNext to advance the expression index in case
        -- we don't actually iterate below
        applyNext(nil, false, aInExpression)
        if aInApply then
            local lPathRoots = getPath(aInRecord, lPath, false)
            for lPathIndex, lPathRoot in ipairs(lPathRoots) do
                -- reset the index for subsequent iterations
                aInExpression.index = lSavedIndex;
                local lNextResult = applyNext(lPathRoot,
                                              aInApply,
                                              aInExpression)
                if lNextResult then
                    return true
                end
            end
            return false
        end
        return nil

    elseif lOpCode == EXISTS then
        local lPath = readArray(aInExpression)
        if aInApply then
            local lValues = getPath(aInRecord, lPath, true)
            return lValues.length > 0
        else
            return nil
        end
    end
end

-- Filters the record based on the expression tokens given.
--
-- in:
--   aInRecord     - the record to filter
--   aInExpression - the filter expression token table
--
-- return:
--   true if the record is selected, false otherwise
local function filterRecord(aInRecord, aInExpression)
    local lResult = true
    if aInExpression.length ~= 0 then
        -- index has to be reset for every record
        aInExpression.index = 1

        lResult = applyNext(aInRecord, true, aInExpression)
    end
    if lResult then
        debug("Record matches")
    else
        debug("Record does not match")
    end
    return lResult
end

-- Perform a generic query based on the expression tokens.
--
-- in:
--   aInStream - the stream to filter
--   ...       - the filter expression tokens
--
-- return:
--   the filtering stream
function genericQuery(aInStream, ...)
    -- index is the next token to be read
    -- length is the number of tokens in the expression
    local lExpression = {index = 1,
                         length = select('#', ...),
                         ...}

    -- Determine whether the record matches the filter expression.
    --
    -- in:
    --   aInRecord - the record to filter
    --
    -- return:
    --   true if the record matches the filter, false otherwise
    local function filterValues(aInRecord)
        return filterRecord(aInRecord, lExpression)
    end

    -- Convert the record to a map.
    --
    -- in:
    --   aInRecord - the record to convert
    --
    -- return:
    --   a map containing the record bin values, plus an entry ['$generation']
    --   for the record generation and one ['$ttl'] for the current TTL
    local function mapRecord(aInRecord)
        local lMap = map()
        lMap['$generation'] = record.gen(aInRecord)
        lMap['$ttl'] = record.ttl(aInRecord)
        for i, lBinName in ipairs(record.bin_names(aInRecord)) do
            lMap[lBinName] = aInRecord[lBinName]
        end
        return lMap
    end

    return aInStream : filter(filterValues) : map(mapRecord)
end

-- Perform a generic count based on the expression tokens.
--
-- in:
--   aInStream - the stream to filter
--   ...       - the filter expression tokens
--
-- return:
--   the filtering/mapping/aggregating stream
function genericCount(aInStream, ...)
    -- index is the next token to be read
    -- length is the number of tokens in the expression
    local lExpression = {index = 1,
                         length = select('#', ...),
                         ...}
    local function filterValues(aInRecord)
        return filterRecord(aInRecord, lExpression)
    end

    local function one(aInRecord)
        return 1
    end

    local function add(aInLeft, aInRight)
        return aInLeft + aInRight
    end

    return aInStream : filter(filterValues) : map(one) : reduce(add)
end

-- Perform a generic sum based on the expression tokens.
--
-- in:
--   aInStream - the stream to filter
--   aInPath   - the path components to traverse
--   ...       - the filter expression tokens
--
-- return:
--   the filtering/mapping/aggregating stream
function genericSum(aInStream, aInPath, ...)
    -- index is the next token to be read
    -- length is the number of tokens in the expression
    local lExpression = {index = 1,
                         length = select('#', ...),
                         ...}

    local function filterValues(aInRecord)
        return filterRecord(aInRecord, lExpression)
    end

    local function getValue(aInRecord)
        local lTotal = 0

        local lPath = {length = list.size(aInPath),
                       array = aInPath}
        local lValues = getPath(aInRecord, lPath)
        if lValues.length > 0 then
            for lIndex, lValue in ipairs(lValues) do
                local lNumber = tonumber(lValue)
                if lNumber ~= nil then
                    lTotal = lTotal + lNumber
                end
            end
        end

        return lTotal
    end

    local function add(aInLeft, aInRight)
        return aInLeft + aInRight
    end

    return aInStream : filter(filterValues) : map(getValue) : reduce(add)
end

-- Perform a generic delete based on the expression tokens.
--
-- in:
--   aInRecord  - the record to delete if it matches the expression
--   aInKeyBin  - the name of the bin containing the id
--   ...        - the filter expression tokens
function genericDelete(aInRecord, aInKeyBin, ...)
    -- index is the next token to be read
    -- length is the number of tokens in the expression
    local lExpression = {index = 1,
                         length = select('#', ...),
                         ... }

    if filterRecord(aInRecord, lExpression) then
        debug('Removing record ' .. record.setname(aInRecord) .. ':' ..
              aInRecord[aInKeyBin])

        aerospike:remove(aInRecord)
    end
end

-- UDF to sum list items based on list indexes.
-- The bin is expected to hold a list of integers.
-- The function returns a map[index:sum]
--
-- in:
--   aInStream - the record stream
--   aInBinName - the bin containing the list of integers
--
-- return:
--   a Map containing sum of list values by index
function listSum(aInStream, aInBinName, ...)
    if aInBinName == nil then
        return map()
    end

    debug('Summing up the list items of bin'..tostring(aInBinName))

    -- index is the next token to be read
    -- length is the number of tokens in the expression
    local lExpression = {index = 1,
                         length = select('#', ...),
                         ...}

    local function filterValues(aInRecord)
        return filterRecord(aInRecord, lExpression)
    end

    local function getListValueMap(aInRecord)
        local lCountMap=map()
        local lListSize = 0
        if aInRecord[aInBinName] ~= nil then
            lListSize = list.size(aInRecord[aInBinName])
        else
            return lCountMap
        end

        if lListSize ~= 0 then
    	      for index = 1,lListSize,1 do
              lCountMap[index] = tonumber(aInRecord[aInBinName][index])
            end
        end
        return lCountMap
    end

    local function add(aInLeft, aInRight)
        return aInLeft + aInRight
    end

    local function sumMap(aInThis, aInThat)
        return map.merge(aInThis, aInThat, add)
    end

    return aInStream : filter(filterValues) : map(getListValueMap) : reduce(sumMap)
end