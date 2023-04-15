// Copyright (c) 2023. ByteDance Inc. All rights reserved.

#pragma once


#include <vector>
#include <set>
#include "basefw/base/log.h"
#include "multipathschedulerI.h"
#include <numeric>


/// min RTT Round Robin multipath scheduler
class RRMultiPathScheduler : public MultiPathSchedulerAlgo
{
public:
    MultiPathSchedulerType SchedulerType() override
    {
        return MultiPathSchedulerType::MULTI_PATH_SCHEDULE_RR;
    }

    explicit RRMultiPathScheduler(const fw::ID& taskid,
            std::map<fw::ID, fw::shared_ptr<SessionStreamController>>& dlsessionmap,
            std::set<DataNumber>& downloadQueue, std::set<int32_t>& lostPiecesQueue)
            : MultiPathSchedulerAlgo(taskid, dlsessionmap, downloadQueue, lostPiecesQueue)
    {
        SPDLOG_DEBUG("taskid :{}", taskid.ToLogStr());
    }

    ~RRMultiPathScheduler() override
    {
        SPDLOG_TRACE("");
    }

    int32_t StartMultiPathScheduler(fw::weak_ptr<MultiPathSchedulerHandler> mpsHandler) override
    {
        SPDLOG_DEBUG("");
        m_phandler = std::move(mpsHandler);
        return 0;
    }

    bool StopMultiPathScheduler() override
    {
        SPDLOG_DEBUG("");
        OnResetDownload();
        return true;
    }

    void OnSessionCreate(const fw::ID& sessionid) override
    {
        SPDLOG_DEBUG("session: {}", sessionid.ToLogStr());
        auto&& itor = m_session_needdownloadpieceQ.find(sessionid);
        if (itor != m_session_needdownloadpieceQ.end())
        {// found, clear the sending queue
            SPDLOG_WARN("Session: {} is already created", sessionid.ToLogStr());
            for (auto&& subpiecetask: itor->second)
            {
                m_downloadQueue.emplace(subpiecetask);
            }
        }
        m_session_needdownloadpieceQ[sessionid] = std::set<int32_t>();

    }

    void OnSessionDestory(const fw::ID& sessionid) override
    {
        SPDLOG_DEBUG("session: {}", sessionid.ToLogStr());
        // find the session's queue, clear the subpieces and add the subpieces to main downloading queue
        auto&& itor = m_session_needdownloadpieceQ.find(sessionid);
        if (itor == m_session_needdownloadpieceQ.end())
        {
            SPDLOG_WARN("Session: {} isn't in session queue", sessionid.ToLogStr());
            return;
        }
        for (auto&& subpiecetask: itor->second)
        {
            m_downloadQueue.emplace(subpiecetask);

        }
        m_session_needdownloadpieceQ.erase(itor);
    }

    void OnResetDownload() override
    {
        SPDLOG_DEBUG("");

        if (m_session_needdownloadpieceQ.empty())
        {
            return;
        }
        for (auto& it_sn: m_session_needdownloadpieceQ)
        {
            if (!it_sn.second.empty())
            {
                m_downloadQueue.insert(it_sn.second.begin(), it_sn.second.end());

                it_sn.second.clear();
            }
        }
        m_session_needdownloadpieceQ.clear();

    }

    void DoMultiPathSchedule() override
    {
        if (m_session_needdownloadpieceQ.empty())
        {
            SPDLOG_DEBUG("Empty session map");
            return;
        }
        // sort session first
        SPDLOG_TRACE("DoMultiPathSchedule");
        SortSession(m_sortmmap);
        // send pkt requests on each session based on ascend order;
        FillUpSessionTask();

    }


    uint32_t DoSinglePathSchedule(const fw::ID& sessionid) override
    {
        SPDLOG_DEBUG("session:{}", sessionid.ToLogStr());
        auto&& session_itor = m_dlsessionmap.find(sessionid);
        // if key doesn't map to a valid set, []operator should create an empty set for us
        if (session_itor==m_dlsessionmap.end())
        {
            SPDLOG_WARN("Unknown session: {}", sessionid.ToLogStr());
            return -1;
        }
        auto& session = session_itor->second;
        auto uni32DataReqCnt = session->CanRequestPktCnt();
        SPDLOG_DEBUG("Free Wnd : {}", uni32DataReqCnt);
        // try to find how many pieces of data we should fill in sub-task queue;
        if (uni32DataReqCnt == 0)
        {
            SPDLOG_WARN("Free Wnd equals to 0");
            return -1;
        }

        if (m_downloadQueue.size() < uni32DataReqCnt)
        {
            auto handler = m_phandler.lock();
            if (handler)
            {
                handler->OnRequestDownloadPieces(uni32DataReqCnt - m_downloadQueue.size());
            }
            else
            {
                SPDLOG_ERROR("handler = null");
            }
        }

        /// Add task to session task queue
        std::vector<int32_t> vecSubpieceNums;
        // eject uni32DataReqCnt number of subpieces from
        for (auto itr = m_downloadQueue.begin(); itr != m_downloadQueue.end() && uni32DataReqCnt > 0;)
        {
            vecSubpieceNums.push_back(*itr);
            m_downloadQueue.erase(itr++);
            --uni32DataReqCnt;
        }

        m_session_needdownloadpieceQ[sessionid].insert(vecSubpieceNums.begin(), vecSubpieceNums.end());

        ////////////////////////////////////DoSendRequest
        DoSendSessionSubTask(sessionid);
        return 0;
    }

    void OnTimedOut(const fw::ID& sessionid, const std::vector<int32_t>& pns) override
    {
        SPDLOG_DEBUG("session {},lost pieces {}", sessionid.ToLogStr(), pns);
        for (auto& pidx: pns)
        {
            auto&& itor_pair = m_lostPiecesQueue.emplace(pidx);
            if (!itor_pair.second)
            {
                SPDLOG_WARN(" pieceId {} already marked lost", pidx);
            }
        }
    }

    void OnReceiveSubpieceData(const fw::ID& sessionid, SeqNumber seq, DataNumber pno, Timepoint recvtime) override
    {
        SPDLOG_DEBUG("session:{}, seq:{}, pno:{}, recvtime:{}",
                sessionid.ToLogStr(), seq, pno, recvtime.ToDebuggingValue());
        /// rx and tx signal are forwarded directly from transport controller to session controller

        DoSinglePathSchedule(sessionid);
    }

    void SortSession(std::multimap<Duration, fw::shared_ptr<SessionStreamController>>& sortmmap) override
    {
        SPDLOG_TRACE("");
        sortmmap.clear();
        for (auto&& sessionItor: m_dlsessionmap)
        {
            auto score = sessionItor.second->GetRtt();
            sortmmap.emplace(score, sessionItor.second);
        }

    }

private:
    int32_t DoSendSessionSubTask(const fw::ID& sessionid) override
    {
        SPDLOG_TRACE("session id: {}", sessionid.ToLogStr());
        int32_t i32Result = -1;
        auto& setNeedDlSubpiece = m_session_needdownloadpieceQ[sessionid];
        if (setNeedDlSubpiece.empty())
        {
            SPDLOG_TRACE("empty sending queue");
            return i32Result;
        }

        auto& session = m_dlsessionmap[sessionid];
        uint32_t u32CanSendCnt = session->CanRequestPktCnt();
        std::vector<int32_t> vecSubpieces;
        for (auto itor = setNeedDlSubpiece.begin();
             itor != setNeedDlSubpiece.end() && vecSubpieces.size() < u32CanSendCnt;)
        {
            vecSubpieces.emplace_back(*itor);
            setNeedDlSubpiece.erase(itor++);
        }

        bool rt = m_dlsessionmap[sessionid]->DoRequestdata(sessionid, vecSubpieces);
        if (rt)
        {
            i32Result = 0;
            //succeed
        }
        else
        {
            // fail
            // return sending pieces to main download queue
            SPDLOG_DEBUG("Send failed, Given back");
            m_downloadQueue.insert(setNeedDlSubpiece.begin(), setNeedDlSubpiece.end());
        }

        return i32Result;
    }

    void FillUpSessionTask()
    {
        // 1. put lost packets back into main download queue
        SPDLOG_TRACE("");
        for (auto&& lostpiece: m_lostPiecesQueue)
        {
            auto&& itor_pair = m_downloadQueue.emplace(lostpiece);
            if (itor_pair.second)
            {
                SPDLOG_TRACE("lost piece {} inserts successfully", lostpiece);
            }
            else
            {
                SPDLOG_TRACE("lost piece {} already in task queue", lostpiece);
            }
        }
        m_lostPiecesQueue.clear();

        // 2. go through every session,find how many pieces we can request at one time

        std::map<basefw::ID, uint32_t> toSendinEachSession;
        for (auto&& itor: m_dlsessionmap)
        {
            auto& sessionId = itor.first;
            auto& sessStream = itor.second;
            auto sessCanSendCnt = sessStream->CanRequestPktCnt();
            toSendinEachSession.emplace(sessionId, sessCanSendCnt);
            if (sessCanSendCnt != 0)
            {
                SPDLOG_TRACE("session {} has {} free wnd", sessionId.ToLogStr(), sessCanSendCnt);
            }
        }
        uint32_t totalSubpieceCnt = std::accumulate(toSendinEachSession.begin(), toSendinEachSession.end(),
                0, [](size_t total,
                        std::pair<const basefw::ID, uint32_t>& session_task_itor)
                {
                    return total + session_task_itor.second;
                });

        // 3. try to request enough piece cnt from up layer, if necessary

        if (m_downloadQueue.size() < totalSubpieceCnt)
        {
            auto handler = m_phandler.lock();
            if (handler)
            {
                handler->OnRequestDownloadPieces(totalSubpieceCnt - m_downloadQueue.size());
            }
            else
            {
                SPDLOG_ERROR("handler = null");
            }
        }

        SPDLOG_TRACE(" download queue size: {}, need pieces cnt: {}", m_downloadQueue.size(), totalSubpieceCnt);

        // 4. fill up each session Queue, based on min RTT first order, and send
        std::vector<DataNumber> vecToSendpieceNums;
        for (auto&& rtt_sess: m_sortmmap)
        {
            auto& sessStream = rtt_sess.second;
            auto&& sessId = sessStream->GetSessionId();
            auto&& itor_id_ssQ = m_session_needdownloadpieceQ.find(sessId);
            if (itor_id_ssQ != m_session_needdownloadpieceQ.end())
            {
                auto&& id_sendcnt = toSendinEachSession.find(sessId);
                if (id_sendcnt != toSendinEachSession.end())
                {
                    auto uni32DataReqCnt = toSendinEachSession.at(sessId);
                    for (auto&& itr = m_downloadQueue.begin();
                         itr != m_downloadQueue.end() && uni32DataReqCnt > 0;)
                    {

                        vecToSendpieceNums.push_back(*itr);
                        m_downloadQueue.erase(itr++);
                        --uni32DataReqCnt;

                    }

                    m_session_needdownloadpieceQ[sessId].insert(vecToSendpieceNums.begin(),
                            vecToSendpieceNums.end());
                    vecToSendpieceNums.clear();
                }
                else
                {
                    SPDLOG_ERROR("Can't found session {} in toSendinEachSession", sessId.ToLogStr());
                }


            }
            else
            {
                SPDLOG_ERROR("Can't found Session:{} in session_needdownloadsubpiece", sessId.ToLogStr());
            }
        }

        // then send in each session
        for (auto&& it_sn = m_session_needdownloadpieceQ.begin();
             it_sn != m_session_needdownloadpieceQ.end(); ++it_sn)
        {
            auto& sessId = it_sn->first;
            auto& sessQueue = it_sn->second;
            SPDLOG_TRACE("session Id:{}, session queue:{}", sessId.ToLogStr(), sessQueue);
            DoSendSessionSubTask(it_sn->first);
        }


    }// end of FillUpSessionTask

    /// It's multipath scheduler's duty to maintain session_needdownloadsubpiece, and m_sortmmap
    std::map<fw::ID, std::set<DataNumber>> m_session_needdownloadpieceQ;// session task queues
    std::multimap<Duration, fw::shared_ptr<SessionStreamController>> m_sortmmap;
    fw::weak_ptr<MultiPathSchedulerHandler> m_phandler;

};

