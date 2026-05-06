local addonName = ...
local LFRaider = _G.LFRaider or {}
_G.LFRaider = LFRaider

local ADDON_PREFIX = "|cff33ff99LFRaider|r"
local DEFAULT_REALM = "Dreamscythe"
local MINIMAP_RADIUS = 80
local SUMMARY_COLOR = "|cff33ff99"
local RESET_COLOR = "|r"
local LFG_TOOLTIP_MIN_WIDTH = 270

local DEFAULT_DB = {
    tooltips = true,
    lfg = true,
    who = true,
    whoChat = true,
    showWCL = true,
    minimapButton = {
        show = true,
        position = 220,
    },
}

local SCORE_COLORS = {
    { min = 99, color = "|cffff66cc" },
    { min = 95, color = "|cffff8000" },
    { min = 75, color = "|cffa335ee" },
    { min = 50, color = "|cff0070ff" },
    { min = 25, color = "|cff1eff00" },
    { min = 0, color = "|cff9d9d9d" },
}

local minimapButton = nil
local minimapMenu = nil

local function Atan2(y, x)
    if math.atan2 then
        return math.atan2(y, x)
    end

    if x > 0 then
        return math.atan(y / x)
    elseif x < 0 and y >= 0 then
        return math.atan(y / x) + math.pi
    elseif x < 0 and y < 0 then
        return math.atan(y / x) - math.pi
    elseif x == 0 and y > 0 then
        return math.pi / 2
    elseif x == 0 and y < 0 then
        return -math.pi / 2
    end

    return 0
end

local function CopyDefaults(source, target)
    for key, value in pairs(source) do
        if type(value) == "table" then
            if type(target[key]) ~= "table" then
                target[key] = {}
            end
            CopyDefaults(value, target[key])
        elseif target[key] == nil then
            target[key] = value
        end
    end
end

local function EnsureSavedVariables()
    if type(_G.LFRaiderDB) ~= "table" then
        _G.LFRaiderDB = {}
    end

    CopyDefaults(DEFAULT_DB, _G.LFRaiderDB)
    return _G.LFRaiderDB
end

local function GetMinimapSettings()
    local db = EnsureSavedVariables()
    if type(db.minimapButton) ~= "table" then
        db.minimapButton = {}
    end

    CopyDefaults(DEFAULT_DB.minimapButton, db.minimapButton)
    return db.minimapButton
end

local function Trim(value)
    if type(value) ~= "string" then
        return nil
    end

    return (value:gsub("^%s+", ""):gsub("%s+$", ""))
end

local function NormalizeName(name)
    name = Trim(name)
    if not name or name == "" then
        return nil
    end

    return string.lower(name)
end

local function NormalizeRealm(realm)
    realm = Trim(realm)
    if not realm or realm == "" then
        return nil
    end

    return string.lower(realm:gsub("[%s%-']", ""))
end

local function SplitNameRealm(value, fallbackRealm)
    value = Trim(value)
    if not value or value == "" then
        return nil, fallbackRealm
    end

    local name, realm = value:match("^([^%-]+)%-(.+)$")
    if name and realm then
        return Trim(name), Trim(realm)
    end

    return value, fallbackRealm
end

local function GetCurrentRealm()
    if type(GetRealmName) == "function" then
        local realm = GetRealmName()
        if realm and realm ~= "" then
            return realm
        end
    end

    return DEFAULT_REALM
end

local function GetDataset()
    if type(_G.LFRaiderData) == "table" then
        return _G.LFRaiderData
    end

    return {}
end

local function GetScoreColor(score)
    score = tonumber(score)
    if not score then
        return "|cffffffff"
    end

    for _, bracket in ipairs(SCORE_COLORS) do
        if score >= bracket.min then
            return bracket.color
        end
    end

    return "|cffffffff"
end

local function FormatScore(score)
    score = tonumber(score)
    if not score then
        return "n/a"
    end

    return string.format("%.1f", score)
end

local function ColorScore(score)
    return GetScoreColor(score) .. FormatScore(score) .. RESET_COLOR
end

local function WrapColor(color, text)
    if not text or text == "" then
        return nil
    end

    return color .. text .. RESET_COLOR
end

local function Print(message)
    if _G.DEFAULT_CHAT_FRAME and type(_G.DEFAULT_CHAT_FRAME.AddMessage) == "function" then
        _G.DEFAULT_CHAT_FRAME:AddMessage(ADDON_PREFIX .. ": " .. tostring(message))
    end
end

local function GetRawField(rawEntry, ...)
    if type(rawEntry) ~= "table" then
        return nil
    end

    for i = 1, select("#", ...) do
        local key = select(i, ...)
        local value = rawEntry[key]
        if value ~= nil then
            return value
        end
    end

    return nil
end

local function BuildCharacterRecord(name, realm, rawEntry)
    if rawEntry == nil then
        return nil
    end

    local dataset = GetDataset()
    local scoreScale = tonumber(dataset.scoreScale) or 10
    if scoreScale <= 0 then
        scoreScale = 10
    end

    local rawWCL
    if type(rawEntry) == "number" then
        rawWCL = rawEntry
    elseif type(rawEntry) == "table" then
        rawWCL = GetRawField(rawEntry, 1, "wcl", "wclOverall", "score", "ranking")
    end

    rawWCL = tonumber(rawWCL)

    if rawWCL == nil then
        return nil
    end

    local realmKey = NormalizeRealm(realm or GetCurrentRealm())
    local realmDisplay = realm
    if realmKey and type(dataset.realmNames) == "table" then
        realmDisplay = dataset.realmNames[realmKey] or realmDisplay
    end

    return {
        name = name,
        realm = realmDisplay or GetCurrentRealm(),
        wclOverall = rawWCL and (rawWCL / scoreScale) or nil,
        rawWCL = rawWCL,
    }
end

local function GetCharacterRecord(name, realm)
    local dataset = GetDataset()
    local realms = dataset.realms
    if type(realms) ~= "table" then
        return nil
    end

    local resolvedName, resolvedRealm = SplitNameRealm(name, realm or GetCurrentRealm())
    local nameKey = NormalizeName(resolvedName)
    local realmKey = NormalizeRealm(resolvedRealm)
    if not nameKey or not realmKey then
        return nil
    end

    local realmScores = realms[realmKey]
    if type(realmScores) ~= "table" then
        return nil
    end

    return BuildCharacterRecord(resolvedName, resolvedRealm, realmScores[nameKey])
end

local function GetScore(name, realm)
    local record = GetCharacterRecord(name, realm)
    if not record then
        return nil
    end

    return record.wclOverall, record.rawWCL
end

local function HasAnyEnabledValue(record)
    local db = EnsureSavedVariables()
    return db.showWCL and record.wclOverall ~= nil
end

local function BuildCompactSummary(record)
    if not record or not HasAnyEnabledValue(record) then
        return nil
    end

    local db = EnsureSavedVariables()
    local parts = {}
    if db.showWCL and record.wclOverall ~= nil then
        parts[#parts + 1] = FormatScore(record.wclOverall) .. "%"
    end

    if #parts == 0 then
        return nil
    end

    return table.concat(parts, " / ")
end

local BuildCompactLFGSummary

local function BuildColoredSummary(record)
    local summary = BuildCompactLFGSummary(record)
    if not summary then
        return nil
    end

    return SUMMARY_COLOR .. "[" .. RESET_COLOR .. summary .. SUMMARY_COLOR .. "]" .. RESET_COLOR
end

BuildCompactLFGSummary = function(record)
    if not record or not HasAnyEnabledValue(record) then
        return nil
    end

    local db = EnsureSavedVariables()
    local parts = {}
    if db.showWCL and record.wclOverall ~= nil then
        parts[#parts + 1] = WrapColor(GetScoreColor(record.wclOverall), FormatScore(record.wclOverall) .. "%")
    end

    if #parts == 0 then
        return nil
    end

    return table.concat(parts, " ")
end

local function BuildSlashSummary(record)
    if not record or not HasAnyEnabledValue(record) then
        return "no bundled score"
    end

    local db = EnsureSavedVariables()
    local parts = {}
    if db.showWCL and record.wclOverall ~= nil then
        parts[#parts + 1] = "WCL " .. ColorScore(record.wclOverall)
    end

    return table.concat(parts, ", ")
end

local function AppendSummaryToFontString(fontString, name, realm, allowExpand)
    if not fontString or type(fontString.GetText) ~= "function" or type(fontString.SetText) ~= "function" then
        return false
    end

    local record = GetCharacterRecord(name, realm)
    local summary = BuildColoredSummary(record)
    if not summary then
        return false
    end

    local text = fontString:GetText() or ""
    if fontString.LFRaiderLastSummary and string.sub(text, -string.len(fontString.LFRaiderLastSummary)) == fontString.LFRaiderLastSummary then
        text = string.sub(text, 1, string.len(text) - string.len(fontString.LFRaiderLastSummary))
    end

    local appended = " " .. summary
    fontString.LFRaiderLastSummary = appended
    fontString:SetText(text .. appended)
    if allowExpand and type(fontString.SetWidth) == "function" then
        fontString:SetWidth(0)
    end
    return true
end

local function ResolveUnit(unit)
    if not unit or type(UnitExists) ~= "function" or not UnitExists(unit) then
        return nil, nil
    end

    if type(UnitIsPlayer) == "function" and not UnitIsPlayer(unit) then
        return nil, nil
    end

    local name
    local realm
    if type(UnitFullName) == "function" then
        name, realm = UnitFullName(unit)
    end

    if not name and type(UnitName) == "function" then
        name, realm = UnitName(unit)
    end

    if not realm or realm == "" then
        realm = GetCurrentRealm()
    end

    return name, realm
end

local function LookupUnit(unit)
    local name, realm = ResolveUnit(unit)
    if not name then
        return nil
    end

    local record = GetCharacterRecord(name, realm)
    return {
        name = name,
        realm = realm,
        score = record and record.wclOverall or nil,
        rawScore = record and record.rawWCL or nil,
        record = record,
    }
end

local function PrintLookup(name, realm)
    if not name then
        Print("No character selected.")
        return
    end

    local record = GetCharacterRecord(name, realm)
    local resolvedName, resolvedRealm = SplitNameRealm(name, realm or GetCurrentRealm())
    Print(string.format("%s-%s: %s", resolvedName, resolvedRealm or GetCurrentRealm(), BuildSlashSummary(record)))
end

local function PrintUnitLookup(unit)
    local lookup = LookupUnit(unit)
    if not lookup then
        Print("No player unit found.")
        return
    end

    PrintLookup(lookup.name, lookup.realm)
end

local function PrintStats()
    local dataset = GetDataset()
    local generatedAt = dataset.generatedAt or "unknown"
    local totalCharacters = tonumber(dataset.totalCharacters) or 0
    local source = dataset.source or "unknown"
    Print(string.format("%d bundled characters, generated %s, source %s", totalCharacters, generatedAt, source))
end

local function PrintHelp()
    Print("/lfr target - look up your target")
    Print("/lfr self - look up yourself")
    Print("/lfr Name-Realm - look up a character")
    Print("/lfr stats - show bundled dataset info")
    Print("/lfr wcl on|off - toggle Warcraft Logs overall")
    Print("/lfr lfg on|off - toggle LFG pane annotations")
    Print("/lfr who on|off - toggle Who pane annotations")
    Print("/lfr whochat on|off - toggle /who chat annotations")
    Print("/lfr tooltip on|off - toggle tooltip lines")
    Print("/lfr minimap - toggle minimap button")
end

local function SetBooleanOption(key, enabled, label)
    EnsureSavedVariables()[key] = not not enabled
    Print(label .. " " .. (enabled and "enabled" or "disabled") .. ".")
end

local function ToggleBooleanOption(key, label)
    local db = EnsureSavedVariables()
    SetBooleanOption(key, not db[key], label)
end

local function ParseToggle(value)
    value = string.lower(Trim(value) or "")
    if value == "on" or value == "1" or value == "true" or value == "yes" then
        return true
    end
    if value == "off" or value == "0" or value == "false" or value == "no" then
        return false
    end
    return nil
end

local function AreTooltipsEnabled()
    local db = EnsureSavedVariables()
    return db.tooltips ~= false
end

local function AddRecordToTooltip(tooltip, record, compact)
    if not record or not HasAnyEnabledValue(record) then
        return false
    end

    if not tooltip or type(tooltip.AddLine) ~= "function" then
        return false
    end

    if compact then
        local summary = BuildCompactLFGSummary(record)
        if summary then
            tooltip:AddLine("LFRaider: " .. summary, 1, 1, 1)
        end
    else
        local db = EnsureSavedVariables()
        if db.showWCL and record.wclOverall ~= nil then
            tooltip:AddLine("Warcraft Logs: " .. ColorScore(record.wclOverall))
        end
    end

    if type(tooltip.Show) == "function" then
        tooltip:Show()
    end
    return true
end

local function AddScoreToTooltip(tooltip, unit)
    if not AreTooltipsEnabled() then
        return
    end

    local lookup = LookupUnit(unit)
    if not lookup or not lookup.record then
        return
    end

    local tooltipKey = lookup.name .. "-" .. lookup.realm .. ":" .. tostring(lookup.rawScore)
    if tooltip and tooltip.LFRaiderTooltipKey == tooltipKey then
        return
    end

    if tooltip then
        tooltip.LFRaiderTooltipKey = tooltipKey
    end
    AddRecordToTooltip(tooltip, lookup.record, false)
end

local function AddNameToTooltip(tooltip, name, realm, compact)
    if not AreTooltipsEnabled() then
        return
    end

    AddRecordToTooltip(tooltip, GetCharacterRecord(name, realm), compact)
end

local function IsRegionShown(region)
    if not region then
        return false
    end

    if type(region.IsShown) == "function" then
        return region:IsShown()
    end

    return true
end

local function EnsureLFGMetricFontString(entry, key, width)
    if entry[key] then
        return entry[key]
    end

    if not entry or type(entry.CreateFontString) ~= "function" then
        return nil
    end

    local fontString = entry:CreateFontString(nil, "ARTWORK", "GameFontHighlightSmall")
    if not fontString then
        return nil
    end

    if type(fontString.SetJustifyH) == "function" then
        fontString:SetJustifyH("RIGHT")
    end
    if width and type(fontString.SetWidth) == "function" then
        fontString:SetWidth(width)
    end

    entry[key] = fontString
    return fontString
end

local function SetMetricFontText(fontString, text)
    if not fontString or type(fontString.SetText) ~= "function" then
        return
    end

    fontString:SetText(text or "")
    if text and text ~= "" then
        if type(fontString.Show) == "function" then
            fontString:Show()
        end
    elseif type(fontString.Hide) == "function" then
        fontString:Hide()
    end
end

local function GetLFGMetricLeftAnchor(entry)
    if entry and entry.NewPlayerFriendlyIcon and IsRegionShown(entry.NewPlayerFriendlyIcon) then
        return entry.NewPlayerFriendlyIcon, "RIGHT", 4
    end

    if entry and entry.ClassIcon and IsRegionShown(entry.ClassIcon) then
        return entry.ClassIcon, "RIGHT", 4
    end

    if entry and entry.Level and IsRegionShown(entry.Level) then
        return entry.Level, "RIGHT", 4
    end

    if entry and entry.VoiceChat and IsRegionShown(entry.VoiceChat) then
        return entry.VoiceChat, "RIGHT", 4
    end

    if entry and entry.Name then
        return entry.Name, "RIGHT", 6
    end

    return entry, "LEFT", 10
end

local function GetLFGMetricRightAnchor(entry)
    if entry and entry.DataDisplay and IsRegionShown(entry.DataDisplay) then
        return entry.DataDisplay, "LEFT", -8
    end

    if entry and entry.PendingLabel and IsRegionShown(entry.PendingLabel) then
        return entry.PendingLabel, "LEFT", -6
    end

    if entry and entry.ExpirationTime and IsRegionShown(entry.ExpirationTime) then
        return entry.ExpirationTime, "LEFT", -6
    end

    return entry, "RIGHT", -10
end

local function LayoutLFGMetricText(entry, fontString)
    if not fontString then
        return
    end

    local leftAnchor, leftPoint, leftOffset = GetLFGMetricLeftAnchor(entry)
    local rightAnchor, rightPoint, rightOffset = GetLFGMetricRightAnchor(entry)
    if not leftAnchor or not rightAnchor then
        return
    end

    if type(fontString.ClearAllPoints) == "function" then
        fontString:ClearAllPoints()
    end
    if type(fontString.SetPoint) == "function" then
        fontString:SetPoint("LEFT", leftAnchor, leftPoint, leftOffset, -1)
        fontString:SetPoint("RIGHT", rightAnchor, rightPoint, rightOffset, -1)
    end
end

local function SetLFGEntryMetrics(entry, name, realm)
    if not entry then
        return false
    end

    local record = GetCharacterRecord(name, realm)
    local summary = BuildCompactLFGSummary(record)
    local fontString = EnsureLFGMetricFontString(entry, "LFRaiderMetricText")
    if not fontString then
        return false
    end

    if entry.LFRaiderWCLText then
        SetMetricFontText(entry.LFRaiderWCLText, nil)
    end
    if entry.LFRaiderItemText then
        SetMetricFontText(entry.LFRaiderItemText, nil)
    end

    SetMetricFontText(fontString, summary)
    LayoutLFGMetricText(entry, fontString)
    return summary ~= nil
end

local function AnnotateLFGBrowseTooltipMember(frame, realm)
    if not AreTooltipsEnabled() or not frame or not frame.Name or not frame.Level then
        return false
    end

    if type(frame.Name.GetText) ~= "function" or type(frame.Level.GetText) ~= "function" or type(frame.Level.SetText) ~= "function" then
        return false
    end

    local name = frame.Name:GetText()
    local baseLevel = frame.Level:GetText() or ""
    local summary = BuildCompactLFGSummary(GetCharacterRecord(name, realm))
    if summary then
        frame.Level:SetText(summary .. " " .. baseLevel)
        return true
    end

    frame.Level:SetText(baseLevel)
    return false
end

local function AnnotateLFGBrowseSearchEntryTooltip(tooltip, resultID)
    if not AreTooltipsEnabled() or not tooltip or not resultID then
        return
    end

    local realm = GetCurrentRealm()
    local addedSummary = false

    if tooltip.Leader then
        addedSummary = AnnotateLFGBrowseTooltipMember(tooltip.Leader, realm) or addedSummary
    end

    if tooltip.memberPool and type(tooltip.memberPool.EnumerateActive) == "function" then
        for frame in tooltip.memberPool:EnumerateActive() do
            addedSummary = AnnotateLFGBrowseTooltipMember(frame, realm) or addedSummary
        end
    end

    if addedSummary and type(tooltip.SetWidth) == "function" then
        local width = 0
        if type(tooltip.GetWidth) == "function" then
            width = tooltip:GetWidth() or 0
        end

        if width < LFG_TOOLTIP_MIN_WIDTH then
            tooltip:SetWidth(LFG_TOOLTIP_MIN_WIDTH)
        end
    end
end

local function HookTooltips()
    if LFRaider.tooltipsHooked then
        return
    end

    LFRaider.tooltipsHooked = true

    if _G.GameTooltip and type(_G.GameTooltip.HookScript) == "function" then
        _G.GameTooltip:HookScript("OnTooltipCleared", function(tooltip)
            tooltip.LFRaiderTooltipKey = nil
        end)

        _G.GameTooltip:HookScript("OnTooltipSetUnit", function(tooltip)
            if type(tooltip.GetUnit) ~= "function" then
                return
            end

            local _, unit = tooltip:GetUnit()
            AddScoreToTooltip(tooltip, unit)
        end)
    end

    if _G.TooltipDataProcessor
        and _G.Enum
        and _G.Enum.TooltipDataType
        and _G.Enum.TooltipDataType.Unit
        and type(_G.TooltipDataProcessor.AddTooltipPostCall) == "function"
    then
        _G.TooltipDataProcessor.AddTooltipPostCall(_G.Enum.TooltipDataType.Unit, function(tooltip)
            if not tooltip or type(tooltip.GetUnit) ~= "function" then
                return
            end

            local _, unit = tooltip:GetUnit()
            AddScoreToTooltip(tooltip, unit)
        end)
    end
end

local function AnnotateLFGSearchEntry(entry)
    local db = EnsureSavedVariables()
    if not db.lfg or not entry or not entry.resultID or not _G.C_LFGList or type(_G.C_LFGList.GetSearchResultInfo) ~= "function" then
        return
    end

    local info = _G.C_LFGList.GetSearchResultInfo(entry.resultID)
    if not info then
        return
    end

    local leaderName = info.leaderName
    if (not leaderName or leaderName == "") and type(_G.C_LFGList.GetSearchResultLeaderInfo) == "function" then
        local leaderInfo = _G.C_LFGList.GetSearchResultLeaderInfo(entry.resultID)
        leaderName = leaderInfo and leaderInfo.name
    end

    if not leaderName then
        return
    end

    SetLFGEntryMetrics(entry, leaderName, GetCurrentRealm())
end

local function AnnotateLFGApplicantMember(member, appID, memberIdx)
    local db = EnsureSavedVariables()
    if not db.lfg or not member or not member.Name or not _G.C_LFGList or type(_G.C_LFGList.GetApplicantMemberInfo) ~= "function" then
        return
    end

    local name = _G.C_LFGList.GetApplicantMemberInfo(appID, memberIdx)
    if name then
        AppendSummaryToFontString(member.Name, name, GetCurrentRealm(), true)
    end
end

local function AnnotateWhoList()
    local db = EnsureSavedVariables()
    if not db.who or not _G.C_FriendList or type(_G.C_FriendList.GetWhoInfo) ~= "function" then
        return
    end

    local count = _G.WHOS_TO_DISPLAY or 17
    for i = 1, count do
        local button = _G["WhoFrameButton" .. i]
        local nameText = _G["WhoFrameButton" .. i .. "Name"]
        if button and nameText and button.whoIndex then
            local info = _G.C_FriendList.GetWhoInfo(button.whoIndex)
            if info and info.fullName then
                AppendSummaryToFontString(nameText, info.fullName, GetCurrentRealm())
            end
        end
    end
end

local function StripChatCodes(value)
    if type(value) ~= "string" then
        return nil
    end

    return value:gsub("|c%x%x%x%x%x%x%x%x", ""):gsub("|r", "")
end

local function CleanWhoResultName(value)
    value = Trim(StripChatCodes(value))
    if not value or value == "" then
        return nil
    end

    local bracketed = value:match("^%[([^%]]+)%]$")
    if bracketed then
        value = Trim(bracketed)
    end

    if not value or value == "" or value:find("%s") then
        return nil
    end

    return value
end

local function LooksLikeWhoResultDetails(details)
    details = Trim(StripChatCodes(details))
    if not details or details == "" then
        return false
    end

    return details:sub(1, 24):find("%d") ~= nil
end

local function GetWhoResultMessageName(message)
    local plainMessage = StripChatCodes(message)
    local linkName, linkDetails = plainMessage:match("|Hplayer:([^:|]+)[^|]*|h.-|h:%s*(.+)$")
    if linkName and LooksLikeWhoResultDetails(linkDetails) then
        return CleanWhoResultName(linkName)
    end

    local bracketName, bracketDetails = plainMessage:match("^%s*%[([^%]]+)%]:%s*(.+)$")
    if bracketName and LooksLikeWhoResultDetails(bracketDetails) then
        return CleanWhoResultName(bracketName)
    end

    local plainName, plainDetails = plainMessage:match("^%s*([^:]-):%s*(.+)$")
    plainName = CleanWhoResultName(plainName)
    if plainName and LooksLikeWhoResultDetails(plainDetails) then
        return plainName
    end

    return nil
end

local function BuildMessageSummaries(message)
    local db = EnsureSavedVariables()
    if not db.whoChat or type(message) ~= "string" or string.find(message, SUMMARY_COLOR .. "[", 1, true) then
        return nil
    end

    -- Only annotate /who result lines. Classic chat output is generated by
    -- C_FriendList.SendWho(..., Enum.SocialWhoOrigin.Chat), and its details
    -- are not guaranteed to start with the English word "Level".
    local whoName = GetWhoResultMessageName(message)
    if not whoName then
        return nil
    end

    -- Skip dice roll messages: "Vocoder rolls 26 (1-100)"
    if message:find(" rolls %d+ %(") then
        return nil
    end

    local summaries = {}
    local seen = {}
    for token in string.gmatch(whoName, "[%a][%a'%-]+") do
        if not seen[token] then
            local record = GetCharacterRecord(token, GetCurrentRealm())
            local summary = BuildCompactLFGSummary(record)
            if summary then
                summaries[#summaries + 1] = {
                    name = token,
                    summary = summary,
                }
                seen[token] = true
            end
        end
        if #summaries >= 3 then
            break
        end
    end

    if #summaries == 0 then
        return nil
    end

    if #summaries == 1 then
        return SUMMARY_COLOR .. "[" .. RESET_COLOR .. summaries[1].summary .. SUMMARY_COLOR .. "]" .. RESET_COLOR
    end

    local parts = {}
    for _, summary in ipairs(summaries) do
        parts[#parts + 1] = summary.name .. ": " .. summary.summary
    end

    return SUMMARY_COLOR .. "[" .. RESET_COLOR .. table.concat(parts, "; ") .. SUMMARY_COLOR .. "]" .. RESET_COLOR
end

local function ChatSystemMessageFilter(_, _, message, ...)
    local summary = BuildMessageSummaries(message)
    if summary then
        return false, message .. " " .. summary, ...
    end

    return false, message, ...
end

local function HookGlobalFunctionOnce(functionName, callback)
    if type(hooksecurefunc) ~= "function" or type(_G[functionName]) ~= "function" then
        return
    end

    LFRaider.hookedFunctions = LFRaider.hookedFunctions or {}
    if LFRaider.hookedFunctions[functionName] then
        return
    end

    hooksecurefunc(functionName, callback)
    LFRaider.hookedFunctions[functionName] = true
end

local function HookWhoAndChat()
    HookGlobalFunctionOnce("WhoList_Update", AnnotateWhoList)
    HookGlobalFunctionOnce("LFGListSearchEntry_Update", AnnotateLFGSearchEntry)
    HookGlobalFunctionOnce("LFGBrowseSearchEntry_Update", AnnotateLFGSearchEntry)
    HookGlobalFunctionOnce("LFGListApplicationViewer_UpdateApplicantMember", AnnotateLFGApplicantMember)
    HookGlobalFunctionOnce("LFGListApplicantMember_OnEnter", function(member)
        if member and type(member.GetParent) == "function" then
            local parent = member:GetParent()
            if parent and parent.applicantID and member.memberIdx and _G.C_LFGList and type(_G.C_LFGList.GetApplicantMemberInfo) == "function" then
                local name = _G.C_LFGList.GetApplicantMemberInfo(parent.applicantID, member.memberIdx)
                AddNameToTooltip(_G.GameTooltip, name, GetCurrentRealm(), true)
            end
        end
    end)
    HookGlobalFunctionOnce("LFGListUtil_SetSearchEntryTooltip", function(tooltip, resultID)
        if _G.C_LFGList and type(_G.C_LFGList.GetSearchResultInfo) == "function" then
            local info = _G.C_LFGList.GetSearchResultInfo(resultID)
            AddNameToTooltip(tooltip, info and info.leaderName, GetCurrentRealm(), true)
        end
    end)
    HookGlobalFunctionOnce("LFGBrowseSearchEntryTooltip_UpdateAndShow", AnnotateLFGBrowseSearchEntryTooltip)

    if not LFRaider.chatFilterHooked then
        if type(ChatFrame_AddMessageEventFilter) == "function" then
            ChatFrame_AddMessageEventFilter("CHAT_MSG_SYSTEM", ChatSystemMessageFilter)
            LFRaider.chatFilterHooked = true
        elseif _G.ChatFrameUtil and type(_G.ChatFrameUtil.AddMessageEventFilter) == "function" then
            _G.ChatFrameUtil.AddMessageEventFilter("CHAT_MSG_SYSTEM", ChatSystemMessageFilter)
            LFRaider.chatFilterHooked = true
        end
    end
end

local function UpdateMinimapButtonPosition()
    if not minimapButton or not _G.Minimap then
        return
    end

    local settings = GetMinimapSettings()
    local angle = math.rad(settings.position or 220)
    local x = math.cos(angle) * MINIMAP_RADIUS
    local y = math.sin(angle) * MINIMAP_RADIUS
    minimapButton:ClearAllPoints()
    minimapButton:SetPoint("CENTER", _G.Minimap, "CENTER", x, y)
end

local function AddMenuButton(text, checked, func)
    local info = UIDropDownMenu_CreateInfo()
    info.text = text
    info.checked = checked
    info.isNotRadio = true
    info.keepShownOnClick = true
    info.func = func
    UIDropDownMenu_AddButton(info)
end

local function AddMenuPlainButton(text, func)
    local info = UIDropDownMenu_CreateInfo()
    info.text = text
    info.notCheckable = true
    info.func = func
    UIDropDownMenu_AddButton(info)
end

local function OpenMinimapMenu(anchor)
    if type(CreateFrame) ~= "function"
        or type(UIDropDownMenu_CreateInfo) ~= "function"
        or type(UIDropDownMenu_AddButton) ~= "function"
        or type(UIDropDownMenu_Initialize) ~= "function"
        or type(ToggleDropDownMenu) ~= "function"
    then
        PrintStats()
        PrintHelp()
        return
    end

    if not minimapMenu then
        minimapMenu = CreateFrame("Frame", "LFRaiderMinimapMenu", _G.UIParent, "UIDropDownMenuTemplate")
    end

    UIDropDownMenu_Initialize(minimapMenu, function()
        local db = EnsureSavedVariables()

        local title = UIDropDownMenu_CreateInfo()
        title.text = "LFRaider"
        title.isTitle = true
        title.notCheckable = true
        UIDropDownMenu_AddButton(title)

        AddMenuButton("Show Warcraft Logs overall", db.showWCL, function()
            ToggleBooleanOption("showWCL", "Warcraft Logs overall")
        end)
        AddMenuButton("Annotate LFG pane", db.lfg, function()
            ToggleBooleanOption("lfg", "LFG pane annotations")
        end)
        AddMenuButton("Annotate Who pane", db.who, function()
            ToggleBooleanOption("who", "Who pane annotations")
        end)
        AddMenuButton("Annotate /who chat", db.whoChat, function()
            ToggleBooleanOption("whoChat", "/who chat annotations")
        end)
        AddMenuButton("Tooltip lines", db.tooltips, function()
            ToggleBooleanOption("tooltips", "Tooltip scores")
        end)
        AddMenuPlainButton("Print dataset stats", PrintStats)
        AddMenuPlainButton("Hide minimap button", function()
            GetMinimapSettings().show = false
            if minimapButton then
                minimapButton:Hide()
            end
            Print("Minimap button hidden. Use /lfr minimap to show it again.")
        end)
    end, "MENU")

    ToggleDropDownMenu(1, nil, minimapMenu, anchor or minimapButton, 0, 0)
end

local function CreateMinimapButton()
    if minimapButton or type(CreateFrame) ~= "function" or not _G.Minimap then
        return
    end

    minimapButton = CreateFrame("Button", "LFRaiderMinimapButton", _G.Minimap)
    minimapButton:SetSize(32, 32)
    minimapButton:SetFrameStrata("MEDIUM")
    minimapButton:SetFrameLevel(8)
    minimapButton:SetHighlightTexture("Interface\\Minimap\\UI-Minimap-ZoomButton-Highlight")

    local icon = minimapButton:CreateTexture(nil, "BACKGROUND")
    icon:SetTexture("Interface\\Icons\\INV_Misc_GroupLooking")
    icon:SetSize(20, 20)
    icon:SetPoint("CENTER", minimapButton, "CENTER", 0, 0)
    icon:SetTexCoord(0.08, 0.92, 0.08, 0.92)
    minimapButton.icon = icon

    local border = minimapButton:CreateTexture(nil, "OVERLAY")
    border:SetTexture("Interface\\Minimap\\MiniMap-TrackingBorder")
    border:SetSize(56, 56)
    border:SetPoint("TOPLEFT", minimapButton, "TOPLEFT", 0, 0)

    minimapButton:SetScript("OnEnter", function(self)
        if _G.GameTooltip then
            _G.GameTooltip:SetOwner(self, "ANCHOR_LEFT")
            _G.GameTooltip:SetText(SUMMARY_COLOR .. "LFRaider" .. RESET_COLOR)
            _G.GameTooltip:AddLine("Left-click for display toggles", 1, 1, 1)
            _G.GameTooltip:AddLine("Right-click to look up your target", 1, 1, 1)
            _G.GameTooltip:Show()
        end
    end)

    minimapButton:SetScript("OnLeave", function()
        if _G.GameTooltip and type(_G.GameTooltip.Hide) == "function" then
            _G.GameTooltip:Hide()
        end
    end)

    minimapButton:RegisterForClicks("LeftButtonUp", "RightButtonUp")
    minimapButton:SetScript("OnClick", function(self, button)
        if button == "RightButton" then
            PrintUnitLookup("target")
        else
            OpenMinimapMenu(self)
        end
    end)

    minimapButton:RegisterForDrag("LeftButton")
    minimapButton:SetScript("OnDragStart", function(self)
        self.dragging = true
    end)
    minimapButton:SetScript("OnDragStop", function(self)
        self.dragging = false
        if not _G.Minimap or type(GetCursorPosition) ~= "function" then
            return
        end

        local mx, my = _G.Minimap:GetCenter()
        local px, py = GetCursorPosition()
        local scale = _G.Minimap:GetEffectiveScale()
        px, py = px / scale, py / scale
        GetMinimapSettings().position = math.deg(Atan2(py - my, px - mx))
        UpdateMinimapButtonPosition()
    end)
    minimapButton:SetScript("OnUpdate", function(self)
        if not self.dragging or not _G.Minimap or type(GetCursorPosition) ~= "function" then
            return
        end

        local mx, my = _G.Minimap:GetCenter()
        local px, py = GetCursorPosition()
        local scale = _G.Minimap:GetEffectiveScale()
        px, py = px / scale, py / scale
        GetMinimapSettings().position = math.deg(Atan2(py - my, px - mx))
        UpdateMinimapButtonPosition()
    end)

    UpdateMinimapButtonPosition()
end

local function ToggleMinimapButton()
    local settings = GetMinimapSettings()
    settings.show = not settings.show

    if settings.show then
        CreateMinimapButton()
        if minimapButton then
            minimapButton:Show()
            UpdateMinimapButtonPosition()
        end
        Print("Minimap button shown.")
    else
        if minimapButton then
            minimapButton:Hide()
        end
        Print("Minimap button hidden. Use /lfr minimap to show it again.")
    end
end

local function HandleOptionCommand(key, label, rest)
    local toggle = ParseToggle(rest)
    if toggle == nil then
        ToggleBooleanOption(key, label)
    else
        SetBooleanOption(key, toggle, label)
    end
end

local function HandleSlash(input)
    input = Trim(input) or ""
    local command, rest = input:match("^(%S+)%s*(.-)$")
    command = command and string.lower(command) or ""

    if command == "" then
        if type(UnitExists) == "function" and UnitExists("target") then
            PrintUnitLookup("target")
        else
            PrintUnitLookup("player")
        end
        return
    end

    if command == "help" then
        PrintHelp()
        return
    end

    if command == "stats" then
        PrintStats()
        return
    end

    if command == "target" then
        PrintUnitLookup("target")
        return
    end

    if command == "self" or command == "me" or command == "player" then
        PrintUnitLookup("player")
        return
    end

    if command == "minimap" or command == "mm" then
        ToggleMinimapButton()
        return
    end

    if command == "menu" then
        OpenMinimapMenu(minimapButton)
        return
    end

    if command == "tooltip" or command == "tooltips" then
        HandleOptionCommand("tooltips", "Tooltip scores", rest)
        return
    end

    if command == "lfg" then
        HandleOptionCommand("lfg", "LFG pane annotations", rest)
        return
    end

    if command == "who" then
        HandleOptionCommand("who", "Who pane annotations", rest)
        return
    end

    if command == "whochat" then
        HandleOptionCommand("whoChat", "/who chat annotations", rest)
        return
    end

    if command == "wcl" or command == "logs" then
        HandleOptionCommand("showWCL", "Warcraft Logs overall", rest)
        return
    end

    local name, realm = SplitNameRealm(input, GetCurrentRealm())
    PrintLookup(name, realm)
end

local function RegisterSlashCommands()
    _G.SLASH_LFRAIDER1 = "/lfraider"
    _G.SLASH_LFRAIDER2 = "/lfr"
    _G.SlashCmdList = _G.SlashCmdList or {}
    _G.SlashCmdList.LFRAIDER = HandleSlash
end

local function OnAddonLoaded()
    EnsureSavedVariables()
    HookTooltips()
    HookWhoAndChat()
    RegisterSlashCommands()
    if GetMinimapSettings().show then
        CreateMinimapButton()
    end
end

local frame = CreateFrame("Frame")
frame:RegisterEvent("ADDON_LOADED")
frame:RegisterEvent("PLAYER_LOGIN")
frame:RegisterEvent("WHO_LIST_UPDATE")
frame:RegisterEvent("LFG_LIST_SEARCH_RESULTS_RECEIVED")
frame:RegisterEvent("LFG_LIST_SEARCH_RESULT_UPDATED")
frame:RegisterEvent("LFG_LIST_APPLICANT_UPDATED")
frame:RegisterEvent("LFG_LIST_APPLICANT_LIST_UPDATED")
frame:SetScript("OnEvent", function(_, event, loadedAddonName)
    if event == "ADDON_LOADED" and loadedAddonName == addonName then
        OnAddonLoaded()
    elseif event == "ADDON_LOADED" then
        HookWhoAndChat()
    elseif event == "PLAYER_LOGIN" then
        HookWhoAndChat()
    elseif event == "WHO_LIST_UPDATE" then
        AnnotateWhoList()
    elseif string.sub(event, 1, 9) == "LFG_LIST" then
        HookWhoAndChat()
    end
end)

LFRaider.NormalizeName = NormalizeName
LFRaider.NormalizeRealm = NormalizeRealm
LFRaider.SplitNameRealm = SplitNameRealm
LFRaider.GetScore = GetScore
LFRaider.GetCharacterRecord = GetCharacterRecord
LFRaider.FormatScore = FormatScore
LFRaider.ColorScore = ColorScore
LFRaider.LookupUnit = LookupUnit
LFRaider.AddScoreToTooltip = AddScoreToTooltip
LFRaider.AppendSummaryToFontString = AppendSummaryToFontString
LFRaider.BuildCompactLFGSummary = BuildCompactLFGSummary
LFRaider.AnnotateLFGSearchEntry = AnnotateLFGSearchEntry
LFRaider.AnnotateLFGApplicantMember = AnnotateLFGApplicantMember
LFRaider.AnnotateLFGBrowseSearchEntryTooltip = AnnotateLFGBrowseSearchEntryTooltip
LFRaider.AnnotateWhoList = AnnotateWhoList
LFRaider.BuildMessageSummaries = BuildMessageSummaries
LFRaider.ChatSystemMessageFilter = ChatSystemMessageFilter
LFRaider.HandleSlash = HandleSlash
LFRaider.PrintStats = PrintStats
LFRaider.ToggleMinimapButton = ToggleMinimapButton
LFRaider.OpenMinimapMenu = OpenMinimapMenu
LFRaider.UpdateMinimapButtonPosition = UpdateMinimapButtonPosition
