// Copyright (c) 2023. ByteDance Inc. All rights reserved.

#pragma once

#include <cstdint>

class PlayerEventHandler
{
public:
    virtual ~PlayerEventHandler() = default;
    virtual void OnPlayStart()
    {
        spdlog::info(" Play Start ");
    }

    virtual void OnPlayResume()
    {
        spdlog::info("Play Resume");
    }

    virtual void OnPlayComplete()
    {
        spdlog::info("Play Complete");
    }

    virtual void OnError()
    {
        spdlog::info("Play Error");
    }

    virtual void OnPlayStop()
    {
        spdlog::info("Play Stop");
    }

    virtual void OnPlayPosUpdate(uint64_t oldPlayPos, uint64_t newPlayPos)
    {
        spdlog::info("Play Pos {}--->{}",oldPlayPos,newPlayPos);
    }

    virtual void OnPlayCacheUpdate(uint64_t oldCachePos, uint64_t newCachePos)
    {
        spdlog::info("Player Cache {}--->{}",oldCachePos,newCachePos);
    }
};
