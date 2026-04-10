local function assert_equal(actual, expected, message)
    if actual ~= expected then
        error((message or "assert_equal failed") .. string.format(" (expected=%s, actual=%s)", tostring(expected), tostring(actual)))
    end
end

local function assert_true(value, message)
    if not value then
        error(message or "assert_true failed")
    end
end

local function assert_nil(value, message)
    if value ~= nil then
        error((message or "assert_nil failed") .. string.format(" (actual=%s)", tostring(value)))
    end
end

local function new_font_string(text)
    local font = {
        text = text or "",
        width = 0,
    }

    function font:SetText(value)
        self.text = value
    end

    function font:GetText()
        return self.text
    end

    function font:SetTextColor() end
    function font:SetPoint() end
    function font:SetWidth(width) self.width = width end
    function font:GetWidth() return #tostring(self.text or "") * 6 end
    function font:IsTruncated() return false end
    function font:Show() self.shown = true end
    function font:Hide() self.shown = false end

    return font
end

local function install_fixture_data()
    _G.LFRaiderData = {
        generatedAt = "test-fixture",
        source = "tests",
        scoreScale = 10,
        itemScoreScale = 1,
        totalCharacters = 1,
        fields = {
            wclOverall = 1,
            itemScore = 2,
        },
        realms = {
            dreamscythe = {
                vocoder = { 747, 126 },
            },
        },
        realmNames = {
            dreamscythe = "Dreamscythe",
        },
    }
end

local function setup_env(opts)
    opts = opts or {}

    local state = {
        chat = {},
        chat_filters = {},
        frames = {},
        hooks = {},
        menu_entries = {},
        tooltip_lines = {},
        units = opts.units or {
            player = {
                exists = true,
                is_player = true,
                name = "Vocoder",
                realm = "Dreamscythe",
            },
            target = {
                exists = true,
                is_player = true,
                name = "Vocoder",
                realm = "Dreamscythe",
            },
            mouseover = {
                exists = true,
                is_player = true,
                name = "Vocoder",
                realm = "Dreamscythe",
            },
        },
    }

    _G.LFRaider = nil
    _G.LFRaiderDB = nil
    _G.LFRaiderData = nil
    _G.SlashCmdList = {}
    _G.SLASH_LFRAIDER1 = nil
    _G.SLASH_LFRAIDER2 = nil
    _G.WHOS_TO_DISPLAY = 17
    _G.LEVEL_ABBR = "Lvl"
    _G.UIParent = nil
    _G.Minimap = nil

    local function new_texture()
        local texture = {}
        function texture:SetTexture(value) self.texture = value end
        function texture:SetSize(width, height) self.width = width; self.height = height end
        function texture:SetPoint(...) self.point = { ... } end
        function texture:SetTexCoord(...) self.texcoord = { ... } end
        function texture:SetDesaturated(value) self.desaturated = value end
        return texture
    end

    local function new_frame(frame_type, name, parent, template)
        local frame = {
            frame_type = frame_type,
            name = name,
            parent = parent,
            template = template,
            scripts = {},
            events = {},
            shown = true,
        }

        function frame:RegisterEvent(event) self.events[event] = true end
        function frame:SetScript(scriptName, handler) self.scripts[scriptName] = handler end
        function frame:SetSize(width, height) self.width = width; self.height = height end
        function frame:SetFrameStrata(value) self.strata = value end
        function frame:SetFrameLevel(value) self.level = value end
        function frame:SetHighlightTexture(value) self.highlight = value end
        function frame:CreateTexture() return new_texture() end
        function frame:CreateFontString() return new_font_string() end
        function frame:RegisterForClicks(...) self.clicks = { ... } end
        function frame:RegisterForDrag(...) self.drags = { ... } end
        function frame:ClearAllPoints() self.point = nil end
        function frame:SetPoint(...) self.point = { ... } end
        function frame:GetCenter() return 0, 0 end
        function frame:GetEffectiveScale() return 1 end
        function frame:Show() self.shown = true end
        function frame:Hide() self.shown = false end
        function frame:IsShown() return self.shown end
        function frame:GetParent() return self.parent end

        state.frames[#state.frames + 1] = frame
        if name then
            _G[name] = frame
        end
        return frame
    end

    _G.CreateFrame = function(frame_type, name, parent, template)
        return new_frame(frame_type, name, parent, template)
    end

    _G.UIParent = new_frame("Frame", "UIParent", nil, nil)
    _G.Minimap = new_frame("Frame", "Minimap", _G.UIParent, nil)

    _G.DEFAULT_CHAT_FRAME = {
        AddMessage = function(_, message)
            state.chat[#state.chat + 1] = message
        end,
    }

    _G.GameTooltip = {
        hooks = {},
        AddLine = function(self, line)
            state.tooltip_lines[#state.tooltip_lines + 1] = line
            self.lines = state.tooltip_lines
        end,
        SetOwner = function() end,
        SetPoint = function() end,
        SetText = function(self, text)
            self.text = text
        end,
        Show = function(self) self.shown = true end,
        Hide = function(self) self.shown = false end,
        HookScript = function(self, scriptName, handler)
            self.hooks[scriptName] = handler
        end,
        GetUnit = function()
            return "Vocoder", "mouseover"
        end,
    }

    _G.GetRealmName = function()
        return "Dreamscythe"
    end

    _G.GetCursorPosition = function()
        return 10, 10
    end

    _G.UnitExists = function(unit)
        local unitInfo = state.units[unit]
        return unitInfo and unitInfo.exists ~= false or false
    end

    _G.UnitIsPlayer = function(unit)
        local unitInfo = state.units[unit]
        return unitInfo and unitInfo.is_player ~= false or false
    end

    _G.UnitFullName = function(unit)
        local unitInfo = state.units[unit]
        if not unitInfo then
            return nil, nil
        end

        return unitInfo.name, unitInfo.realm
    end

    _G.UnitName = function(unit)
        local unitInfo = state.units[unit]
        return unitInfo and unitInfo.name or nil
    end

    _G.TooltipDataProcessor = nil
    _G.Enum = nil

    _G.RAID_CLASS_COLORS = {
        MAGE = { r = 0.25, g = 0.78, b = 0.92 },
    }
    _G.NORMAL_FONT_COLOR = { r = 1, g = 1, b = 1 }
    _G.GRAY_FONT_COLOR = { r = 0.5, g = 0.5, b = 0.5 }

    _G.C_LFGList = {
        GetSearchResultInfo = function(resultID)
            if resultID == 1 then
                return {
                    leaderName = "Vocoder",
                    name = "Heroic Slave Pens",
                    activityIDs = { 1 },
                    numMembers = 1,
                    isDelisted = false,
                }
            end
            return nil
        end,
        GetSearchResultLeaderInfo = function()
            return {
                name = "Vocoder",
                classFilename = "MAGE",
            }
        end,
        GetApplicantMemberInfo = function()
            return "Vocoder", "MAGE", "Mage", 70, 126
        end,
    }

    _G.C_FriendList = {
        GetWhoInfo = function(index)
            if index == 1 then
                return {
                    fullName = "Vocoder",
                    level = 70,
                }
            end
            return nil
        end,
    }

    _G.WhoList_Update = function() end
    _G.LFGListSearchEntry_Update = function() end
    _G.LFGBrowseSearchEntry_Update = function() end
    _G.LFGListApplicationViewer_UpdateApplicantMember = function() end
    _G.LFGListApplicantMember_OnEnter = function() end
    _G.LFGListUtil_SetSearchEntryTooltip = function() end

    _G.hooksecurefunc = function(functionName, callback)
        state.hooks[functionName] = callback
    end

    _G.ChatFrame_AddMessageEventFilter = function(event, filter)
        state.chat_filters[event] = filter
    end

    _G.UIDropDownMenu_CreateInfo = function()
        return {}
    end

    _G.UIDropDownMenu_AddButton = function(info)
        state.menu_entries[#state.menu_entries + 1] = info
    end

    _G.UIDropDownMenu_Initialize = function(frame, initializer)
        frame.initializer = initializer
        state.menu_entries = {}
        initializer()
    end

    _G.ToggleDropDownMenu = function(level, value, menu)
        state.last_menu = {
            level = level,
            value = value,
            menu = menu,
        }
    end

    assert(loadfile("LFRaider_Data.lua"))()
    install_fixture_data()
    assert(loadfile("LFRaider.lua"))("LFRaider")

    local frame = state.frames[#state.frames]
    assert_true(frame.events.ADDON_LOADED, "addon frame should register ADDON_LOADED")
    frame.scripts.OnEvent(frame, "ADDON_LOADED", "LFRaider")

    return state
end

local function test_lookup_score_and_item_score()
    setup_env()

    local score = _G.LFRaider.GetScore("Vocoder", "Dreamscythe")
    local item_score = _G.LFRaider.GetItemScore("Vocoder", "Dreamscythe")
    assert_equal(_G.LFRaider.FormatScore(score), "74.7")
    assert_equal(_G.LFRaider.FormatItemScore(item_score), "126")
    assert_nil(_G.LFRaider.GetScore("Unknown", "Dreamscythe"))
end

local function test_normalizes_realm_display_names()
    setup_env()

    assert_equal(_G.LFRaider.NormalizeRealm("Dream-scythe"), "dreamscythe")
    assert_equal(_G.LFRaider.NormalizeRealm("Dream scythe"), "dreamscythe")
end

local function test_slash_lookup_prints_both_scores()
    local state = setup_env()

    _G.SlashCmdList.LFRAIDER("Vocoder-Dreamscythe")
    assert_true(string.find(state.chat[#state.chat], "74.7", 1, true), "slash lookup should print WCL score")
    assert_true(string.find(state.chat[#state.chat], "iScore 126", 1, true), "slash lookup should print item score")
end

local function test_stats_print_dataset_info()
    local state = setup_env()

    _G.SlashCmdList.LFRAIDER("stats")
    assert_true(string.find(state.chat[#state.chat], "1 bundled characters", 1, true), "stats should print character count")
end

local function test_tooltip_adds_enabled_scores_once()
    local state = setup_env()

    _G.LFRaider.AddScoreToTooltip(_G.GameTooltip, "mouseover")
    _G.LFRaider.AddScoreToTooltip(_G.GameTooltip, "mouseover")

    assert_equal(#state.tooltip_lines, 2)
    assert_true(string.find(state.tooltip_lines[1], "74.7", 1, true), "tooltip should include WCL score")
    assert_true(string.find(state.tooltip_lines[2], "126", 1, true), "tooltip should include item score")
end

local function test_lfg_search_entry_annotation()
    setup_env()

    local entry = {
        resultID = 1,
        Name = new_font_string("Heroic Slave Pens"),
        ActivityName = new_font_string("Dungeon"),
    }

    _G.LFRaider.AnnotateLFGSearchEntry(entry)
    assert_true(string.find(entry.ActivityName:GetText(), "WCL 74.7", 1, true), "LFG activity row should include WCL summary")
    assert_true(string.find(entry.ActivityName:GetText(), "iScore 126", 1, true), "LFG activity row should include item summary")
end

local function test_lfg_applicant_annotation()
    setup_env()

    local member = {
        Name = new_font_string("Vocoder"),
    }

    _G.LFRaider.AnnotateLFGApplicantMember(member, 42, 1)
    assert_true(string.find(member.Name:GetText(), "WCL 74.7", 1, true), "LFG applicant should include WCL summary")
end

local function test_who_list_annotation()
    setup_env()

    _G.WhoFrameButton1 = { whoIndex = 1 }
    _G.WhoFrameButton1Name = new_font_string("Vocoder")

    _G.LFRaider.AnnotateWhoList()
    assert_true(string.find(_G.WhoFrameButton1Name:GetText(), "iScore 126", 1, true), "Who pane should include item summary")
end

local function test_who_chat_filter_appends_summary()
    setup_env()

    local _, message = _G.LFRaider.ChatSystemMessageFilter(nil, "CHAT_MSG_SYSTEM", "Vocoder: Level 70 Mage")
    assert_true(string.find(message, "WCL 74.7", 1, true), "system chat filter should append WCL summary")
end

local function test_minimap_button_opens_menu()
    local state = setup_env()

    local button = _G.LFRaiderMinimapButton
    assert_true(button ~= nil, "minimap button should be created")
    button.scripts.OnClick(button, "LeftButton")
    assert_true(#state.menu_entries >= 8, "minimap menu should include toggles")
end

local tests = {
    test_lookup_score_and_item_score,
    test_normalizes_realm_display_names,
    test_slash_lookup_prints_both_scores,
    test_stats_print_dataset_info,
    test_tooltip_adds_enabled_scores_once,
    test_lfg_search_entry_annotation,
    test_lfg_applicant_annotation,
    test_who_list_annotation,
    test_who_chat_filter_appends_summary,
    test_minimap_button_opens_menu,
}

for _, test in ipairs(tests) do
    test()
end

print(string.format("ok - %d tests passed", #tests))
