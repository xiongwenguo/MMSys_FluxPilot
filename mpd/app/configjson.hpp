
// Copyright (c) 2023. ByteDance Inc. All rights reserved.

#pragma once
#include "nlohmann/json.hpp"
using json = nlohmann::json;
struct UpNodeConfig
{
    std::string IP="127.0.0.1";
    uint16_t port=41111;
    std::string selfpeerID="0001020304050607080910111213141516171800"; //20 HEX number, 40 chars in total
};
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(UpNodeConfig,IP,port,selfpeerID)

struct DownNodeConfig
{
    DownNodeConfig()
    {
        UpNodeConfig up1;
        up1.IP = "10.0.0.2";
        up1.port = 41111;
        up1.selfpeerID = "0001020304050607080910111213141516171801";

        UpNodeConfig up2;
        up2.IP = "10.0.0.3";
        up2.port = 41111;
        up2.selfpeerID = "0001020304050607080910111213141516171802";

        upnodes.emplace_back(up2);
        upnodes.emplace_back(up2);
    }
    std::string IP="10.0.0.1";
    uint16_t port=8888;
    std::string selfpeerID="00010203040506070809101112131415161718FF"; //20 HEX number, 40 chars in total
    std::vector<UpNodeConfig> upnodes;
};
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(DownNodeConfig,IP,port,selfpeerID,upnodes)

