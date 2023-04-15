// Copyright (c) 2023. ByteDance Inc. All rights reserved.

#pragma once


struct PlayTaskInfo
{
    unsigned int cacheddatapos=0;/// current continue cache position in Byte
    unsigned int curplaypos=0;/// current play position in Byte
    unsigned int filelengthinbyte=0;/// total file length in Byte
    unsigned int byterate=0;/// bitrate in Byte
};

/**@brief this class is used for send play information to download module
 * transport layer may use this callback function get cache and play pos
 */
class DummyPlayerHandler
{
public:
    virtual void GetPlayTaskInfo(PlayTaskInfo&)=0;
};