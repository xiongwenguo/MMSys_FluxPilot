// Copyright (c) 2023. ByteDance Inc. All rights reserved.

#pragma once


#include "mpd/download/transportcontroller/transportcontroller.hpp"

#include "configjson.hpp"

#include "playermetainfo.h"
#include <memory>

class PlayerEventHandler;
namespace MPD
{

    enum SDKLogLevel
    {
        ERROR =0,
        WARN,
        EVENT,
        INFO,
        DEBUG
    };


    bool ParseDownloadJson(std::string& jsonpath,DownNodeConfig& downNodeConfig);

    bool CreatePlayer();

    bool InitPlayer(PlayMetaConfig& playMetaConfig, SDKLogLevel sdkLogLevel=SDKLogLevel::WARN,
            std::shared_ptr<PlayerEventHandler> playerEventHandler = nullptr);

    bool SetTransportModuleSettings(std::shared_ptr<TransportModuleSettings> transMd);

    bool StartPlayer();

    bool StartDownload(uint32_t seconds, DownNodeConfig& downNodeConfig);

    bool StopPlayer();

    bool StartTesting();

    void WaitForFinished();
}// end of MPD namespace


