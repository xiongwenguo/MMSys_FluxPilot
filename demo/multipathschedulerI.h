// Copyright (c) 2023. ByteDance Inc. All rights reserved.

#pragma once

#include <map>
#include "basefw/base/hash.h"
#include "basefw/base/shared_ptr.h"
#include "sessionstreamcontroller.hpp"

enum MultiPathSchedulerType
{
    MULTI_PATH_SCHEDULE_NONE = 0,
    MULTI_PATH_SCHEDULE_RR = 1,
};

class MultiPathSchedulerHandler
{
public:
    virtual bool OnGetCurrPlayPos(uint64_t& currplaypos) = 0; // curr play pos

    virtual bool OnGetCurrCachePos(uint64_t& currcachepos) = 0;

    virtual bool OnGetByteRate(uint32_t& playbyterate) = 0; // bytes per second

    virtual void OnRequestDownloadPieces(uint32_t maxsubpiececnt) = 0; // ask for more subpieces

    virtual ~MultiPathSchedulerHandler() = default;
};

class MultiPathSchedulerAlgo
{
public:
    explicit MultiPathSchedulerAlgo(const fw::ID& taskid,
            std::map<fw::ID, fw::shared_ptr<SessionStreamController>>& dlsessionmap,
            std::set<DataNumber>& downloadQueue, std::set<int32_t>& lostPiecesQueue)
            : m_taskid(taskid), m_dlsessionmap(dlsessionmap),
              m_downloadQueue(downloadQueue), m_lostPiecesQueue(lostPiecesQueue)
    {
    }

    virtual MultiPathSchedulerType SchedulerType()
    {
        return MultiPathSchedulerType::MULTI_PATH_SCHEDULE_NONE;
    }

    virtual int32_t StartMultiPathScheduler(fw::weak_ptr<MultiPathSchedulerHandler> mpsHandler) = 0;

    virtual bool StopMultiPathScheduler() = 0;

    virtual void OnSessionCreate(const fw::ID& sessionid) = 0;

    virtual void OnSessionDestory(const fw::ID& sessionid) = 0;

    virtual void OnResetDownload() = 0;

    virtual void DoMultiPathSchedule() = 0;

    virtual uint32_t DoSinglePathSchedule(const fw::ID& sessionid) = 0;

    virtual void OnTimedOut(const fw::ID& sessionid, const std::vector<int32_t>& spns) = 0;

    virtual void OnReceiveSubpieceData(const fw::ID& sessionid, SeqNumber seq, DataNumber pno, Timepoint recvtime) = 0;

    virtual void SortSession(std::multimap<Duration, fw::shared_ptr<SessionStreamController>>& sortmmap) = 0;

    virtual int32_t DoSendSessionSubTask(const fw::ID& sessionid) = 0;

    virtual ~MultiPathSchedulerAlgo() = default;

protected:
    fw::ID m_taskid;
    std::map<fw::ID, fw::shared_ptr<SessionStreamController>>& m_dlsessionmap;
    std::set<DataNumber>& m_downloadQueue; // main task queue
    std::set<DataNumber>& m_lostPiecesQueue;// the lost pieces queue, waiting to be retransmitted
};

