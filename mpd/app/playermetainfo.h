// Copyright (c) 2023. ByteDance Inc. All rights reserved.

#pragma once

#include <cstdint>

/// meta info for dummy player
struct PlayMetaConfig
{
    /** If the player is paused or stopped, it will start playing when the buffer seconds exceeds minplayseconds
     * */
    uint32_t minplayseconds = 5;

    /** Play task length in bytes, note that this value should be less or equals to the file length in data nodes
    * */
    uint64_t filelengthinbyte = 10 * 1024 * 1024;

    /** play speed in bps*/
    uint32_t bitrate = 1024 * 1024;

    /** play speed in bytes per seconds*/
    uint32_t byterate = bitrate / 8;
};