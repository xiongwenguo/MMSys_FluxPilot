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

    explicit RRMultiPathScheduler(TransportDownloadTaskInfo & tansDlTkInfo, 
                                  std::map<fw::ID, fw::shared_ptr<SessionStreamController>> &dlsessionmap,
                                  std::set<std::pair<SeqNumber, DataNumber>> &downloadQueue, 
                                  std::set<std::pair<SeqNumber, DataNumber>> &priorityQueue, 
                                  std::set<std::pair<SeqNumber, DataNumber>> &lostPiecesQueue,
                                  std::set<std::pair<SeqNumber, DataNumber>> &alreadyDlQueue)
        : MultiPathSchedulerAlgo(tansDlTkInfo.m_rid, dlsessionmap, downloadQueue, priorityQueue, lostPiecesQueue, alreadyDlQueue),
          m_taskStartTp(Clock::GetClock()->Now()),m_tansDlTkInfoRR(tansDlTkInfo)
    {
        
        SPDLOG_DEBUG("taskid :{}", tansDlTkInfo.m_rid.ToLogStr());
    }

    ~RRMultiPathScheduler() override
    {
        SPDLOG_TRACE("");
    }

    int32_t StartMultiPathScheduler(fw::weak_ptr<MultiPathSchedulerHandler> mpsHandler) override
    {
        SPDLOG_DEBUG("");
        m_phandler = std::move(mpsHandler);
        m_continuousPktSeq = -1;
        m_maxSeqAckPktSeqNum = -1;
        m_taskStartTp = Clock::GetClock()->Now();
        return 0;
    }

    bool StopMultiPathScheduler() override
    {
        SPDLOG_DEBUG("");
        OnResetDownload();
        m_maxSeqAckPktSeqNum = -1;
        m_continuousPktSeq = -1;
        return true;
    }

    void OnSessionCreate(const fw::ID &sessionid) override
    {
        SPDLOG_DEBUG("session: {}", sessionid.ToLogStr());
        auto &&itor = m_session_needdownloadpieceQ.find(sessionid);
        if (itor != m_session_needdownloadpieceQ.end())
        { // found, clear the sending queue
            SPDLOG_WARN("Session: {} is already created", sessionid.ToLogStr());
            for (auto &&subpiecetask : itor->second)
            {
                m_downloadQueue.emplace(subpiecetask);
            }
        }
        m_session_needdownloadpieceQ[sessionid] = std::set<std::pair<SeqNumber, DataNumber>>();
    }

    void OnSessionDestory(const fw::ID &sessionid) override
    {
        SPDLOG_DEBUG("session: {}", sessionid.ToLogStr());
        // find the session's queue, clear the subpieces and add the subpieces to main downloading queue
        auto &&itor = m_session_needdownloadpieceQ.find(sessionid);
        if (itor == m_session_needdownloadpieceQ.end())
        {
            SPDLOG_WARN("Session: {} isn't in session queue", sessionid.ToLogStr());
            return;
        }
        for (auto &&subpiecetask : itor->second)
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
        for (auto &it_sn : m_session_needdownloadpieceQ)
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

    uint32_t DoSinglePathSchedule(const fw::ID &sessionid) override
    {
        SPDLOG_DEBUG("session:{}", sessionid.ToLogStr());
        // if key doesn't map to a valid set, []operator should create an empty set for us
        auto &session = m_dlsessionmap[sessionid];
        if (!session)
        {
            SPDLOG_WARN("Unknown session: {}", sessionid.ToLogStr());
            return -1;
        }

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
        std::vector<std::pair<SeqNumber, DataNumber>> vecSubpieceNums;
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

    /*void OnTimedOut(const fw::ID &sessionid, const std::vector<int32_t> &pns) override
    {
        SPDLOG_DEBUG("session {},lost pieces {}", sessionid.ToLogStr(), pns);
        for (auto &pidx : pns)
        {
            auto &&itor_pair = m_lostPiecesQueue.emplace(pidx);
            if (!itor_pair.second)
            {
                SPDLOG_WARN(" pieceId {} already marked lost", pidx);
            }
        }
    }*/
    // start by zpy
    void OnTimedOut(const fw::ID &sessionid, const std::vector<std::pair<uint32_t, int32_t>> &pns) override
    {
        SPDLOG_DEBUG("session {},lost pieces {}", sessionid.ToLogStr(), pns);
        for (auto &pidx : pns)
        {
            auto &&itor_pair = m_lostPiecesQueue.emplace(pidx);
            if (!itor_pair.second)
            {
                SPDLOG_WARN(" pieceId {} already marked lost", pidx.second);
            }
        }
    }
    // end

    // GXW Add:
    void UpdateSeqNum(const fw::ID &sessionid, SeqNumber seq, DataNumber pno, Timepoint recvtime)
    {
        if(seq > m_maxSeqAckPktSeqNum)
        {
            m_maxSeqAckPktSeqNum = seq;
        }
        m_alreadyDledQueue.insert(std::make_pair(seq,pno));
        for(auto it=m_alreadyDledQueue.begin();it!=m_alreadyDledQueue.end();)
        {
            if(m_continuousPktSeq+1==it->first)
            {
                m_continuousPktSeq = it->first;
                m_alreadyDledQueue.erase(it++);
            }
            else
            {
                break;
            }
        }
    }
    // GXW

    void ReInjectPkt()
    {
        Timepoint now_t = Clock::GetClock()->Now();
        Duration interval = now_t - m_taskStartTp;
        int64_t intervalInMicroSeconds = interval.ToMicroseconds();
        
        Duration maxRTT(Duration::FromSeconds(0));
        for (auto &&sessionItor : m_dlsessionmap)
        {
            auto RTT = sessionItor.second->GetRtt(); 
            if(RTT > maxRTT)
                maxRTT = RTT;           
        }
        int64_t bufferTimeIntervalInMicroSeconds = maxRTT.ToMicroseconds()*2;
        int pktCntNeeded =1.0*m_tansDlTkInfoRR.m_byterate*(bufferTimeIntervalInMicroSeconds+intervalInMicroSeconds)/1e6 + 1;
        std::map<SeqNumber,bool> reInjectMap;
        for(SeqNumber i=m_continuousPktSeq+1;i<pktCntNeeded;i++)
        {
            reInjectMap.insert(std::make_pair(i,false));
        }
        if(m_continuousPktSeq+1<pktCntNeeded)
        {
            std::vector<int32_t> vec;
            for(SeqNumber i=m_continuousPktSeq+1;i<pktCntNeeded;i++)
            {
                for(auto &&sessionItor : m_dlsessionmap)
                {
                    int32_t re = sessionItor.second->FindInFlightPktDataNum(i);
                    if(re!=-1)
                    {
                        vec.push_back(re);
                        reInjectMap[i]=true;
                        break;
                    }
                }
            }

            for(auto itor=reInjectMap.begin();itor!=reInjectMap.end();itor++)
            {
                if(itor->second==false)
                {
                    for(auto it=m_lostPiecesQueue.begin();it!=m_lostPiecesQueue.end();it++)
                    {
                        if(it->first==itor->first)
                        {
                            vec.push_back(it->second);
                            reInjectMap[itor->first] = true;
                            m_lostPiecesQueue.erase(it);
                            break;
                        }
                    }
                }   
            }

            SortSession(m_sortmmap);
            uint32_t requestCntPerSession = vec.size() / m_sortmmap.size() + 1;
            auto itor = vec.begin();
            for (auto&& rtt_sess: m_sortmmap)
            {
                std::vector<DataNumber> toSendPkts;
                for(int i=0;i<requestCntPerSession&&itor!=vec.end();i++)
                {
                    toSendPkts.push_back(*itor);
                    itor++;
                }
                bool rt = rtt_sess.second->DoRequestdata(rtt_sess.second->GetSessionId(),toSendPkts);
                if(rt)
                {

                }
                else
                {
                    for(int i=0;i<requestCntPerSession;i++)
                    {
                        itor--;
                    }
                }
            }
        }
        

        


    }
    void OnReceiveSubpieceData(const fw::ID &sessionid, SeqNumber seq, DataNumber pno, Timepoint recvtime) override
    {
        SPDLOG_DEBUG("session:{}, seq:{}, pno:{}, recvtime:{}",
                     sessionid.ToLogStr(), seq, pno, recvtime.ToDebuggingValue());
        /// rx and tx signal are forwarded directly from transport controller to session controller
        UpdateSeqNum(sessionid,seq,pno,recvtime);
        //ReInjectPkt();
        DoSinglePathSchedule(sessionid);
    }

    void SortSession(std::multimap<Duration, fw::shared_ptr<SessionStreamController>> &sortmmap) override
    {
        SPDLOG_TRACE("");
        sortmmap.clear();
        for (auto &&sessionItor : m_dlsessionmap)
        {
            auto expectionRTT = sessionItor.second->GetRtt() * (1 - sessionItor.second->GetLossRate());
            expectionRTT = expectionRTT + sessionItor.second->GetRtt() * (2 * sessionItor.second->GetLossRate());
            sortmmap.emplace(expectionRTT, sessionItor.second);
        }
    }
    void SortSessionLossRate(std::multimap<double, fw::shared_ptr<SessionStreamController>> &sortlrmap) override
    {
        sortlrmap.clear();
        for (auto &&sessionItor : m_dlsessionmap)
        {
            auto lossRate = sessionItor.second->GetLossRate();
            sortlrmap.emplace(lossRate, sessionItor.second);
        }
    }


private:
    int32_t DoSendSessionSubTask(const fw::ID &sessionid) override
    {
        SPDLOG_TRACE("session id: {}", sessionid.ToLogStr());
        int32_t i32Result = -1;
        auto &setNeedDlSubpiece = m_session_needdownloadpieceQ[sessionid];
        if (setNeedDlSubpiece.empty())
        {
            SPDLOG_TRACE("empty sending queue");
            return i32Result;
        }

        auto &session = m_dlsessionmap[sessionid];
        uint32_t u32CanSendCnt = session->CanRequestPktCnt();
        std::vector<int32_t> vecSubpieces;
        for (auto itor = setNeedDlSubpiece.begin();
             itor != setNeedDlSubpiece.end() && vecSubpieces.size() < u32CanSendCnt;itor++)
        {
            vecSubpieces.emplace_back(itor->second);  
        }

        bool rt = m_dlsessionmap[sessionid]->DoRequestdata(sessionid, vecSubpieces);
        if (rt)
        {
            i32Result = 0;
            int i=0;
            for (auto itor = setNeedDlSubpiece.begin();
             itor != setNeedDlSubpiece.end() && i < vecSubpieces.size();)
            {
                setNeedDlSubpiece.erase(itor++);
                i++;
            }
            // succeed
        }
        else
        {
            // fail
            // return sending pieces to main download queue
            SPDLOG_DEBUG("Send failed, Given back");
            m_priorityQueue.insert(setNeedDlSubpiece.begin(), setNeedDlSubpiece.end());
            setNeedDlSubpiece.clear();
        }

        return i32Result;
    }

    void FillUpSessionTask()
    {
        // 1. put lost packets and into priority download queue
        SPDLOG_TRACE("");
        for (auto &&lostpiece : m_lostPiecesQueue)
        {
            // auto &&itor_pair = m_downloadQueue.emplace(lostpiece);

            // start by zpy
            auto &&itor_pair = m_priorityQueue.emplace(lostpiece);
            // end

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
        for (auto &&itor : m_dlsessionmap)
        {
            auto &sessionId = itor.first;
            auto &sessStream = itor.second;
            auto sessCanSendCnt = sessStream->CanRequestPktCnt();
            toSendinEachSession.emplace(sessionId, sessCanSendCnt);
            if (sessCanSendCnt != 0)
            {
                SPDLOG_TRACE("session {} has {} free wnd", sessionId.ToLogStr(), sessCanSendCnt);
            }
        }
        uint32_t totalSubpieceCnt = std::accumulate(toSendinEachSession.begin(), toSendinEachSession.end(),
                                                    0, [](size_t total, std::pair<const basefw::ID, uint32_t> &session_task_itor)
                                                    { return total + session_task_itor.second; });

        // 3. try to request enough piece cnt from up layer, if necessary

        // GXW: 我又改了一波你的代码
        if(m_priorityQueue.size() + m_downloadQueue.size() < totalSubpieceCnt)
        {
            auto handler = m_phandler.lock();
            if (handler)
            {
                handler->OnRequestDownloadPieces(totalSubpieceCnt-m_priorityQueue.size()-m_downloadQueue.size());
            }
            else
            {
                SPDLOG_ERROR("handler = null");
            }
        }
        SPDLOG_TRACE(" download queue size: {}, need pieces cnt: {}", m_downloadQueue.size(), totalSubpieceCnt);

        // 如果所有可用session的CanRequestCnt之和小于需要发送的优先级队列的Pkt数目
        // 我们就暂且不顾拥塞控制
        if(m_priorityQueue.size() > totalSubpieceCnt)
        {
            uint32_t excessRequestPerSession = (m_priorityQueue.size()-totalSubpieceCnt) / m_sortmmap.size() + 1;
            std::vector<std::pair<SeqNumber, DataNumber>> vecToSendpieceNums0;
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
                        auto uni32DataReqCnt = toSendinEachSession.at(sessId) + excessRequestPerSession;  // CanSendCnt
                        for (auto&& itr = m_priorityQueue.begin();
                             itr != m_priorityQueue.end() && uni32DataReqCnt > 0;)
                        {

                            vecToSendpieceNums0.push_back(*itr);
                            m_priorityQueue.erase(itr++);
                            --uni32DataReqCnt;

                        }

                        m_session_needdownloadpieceQ[sessId].insert(vecToSendpieceNums0.begin(),
                                vecToSendpieceNums0.end());
                        vecToSendpieceNums0.clear();
                    }
                }
            }
            for (auto &&it_sn = m_session_needdownloadpieceQ.begin();
                 it_sn != m_session_needdownloadpieceQ.end(); ++it_sn)
            {
                auto &sessId = it_sn->first;
                auto &sessQueue = it_sn->second;
                SPDLOG_TRACE("session Id:{}, session queue:{}", sessId.ToLogStr(), sessQueue);
                DoSendSessionSubTask(it_sn->first);
            }
            return;
        }
        

        // 4. fill up each session Queue, based on min RTT first order, and send
        std::vector<std::pair<SeqNumber, DataNumber>> vecToSendpieceNums;
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
                    auto uni32DataReqCnt = toSendinEachSession.at(sessId);  // CanSendCnt
                    
                    if(m_priorityQueue.size()>=uni32DataReqCnt)
                    {
                        // 如果优先队列不空，就发送m_priorityQueue中的下载请求
                        for (auto&& itr = m_priorityQueue.begin();
                         itr != m_priorityQueue.end() && uni32DataReqCnt > 0;)
                        {

                            vecToSendpieceNums.push_back(*itr);
                            m_priorityQueue.erase(itr++);
                            --uni32DataReqCnt;
                        }

                        m_session_needdownloadpieceQ[sessId].insert(vecToSendpieceNums.begin(),
                                                                    vecToSendpieceNums.end());
                        vecToSendpieceNums.clear();
                    }
                    else if(m_priorityQueue.size()>0&&m_priorityQueue.size()<uni32DataReqCnt)
                    {
                        for (auto&& itr = m_priorityQueue.begin();
                         itr != m_priorityQueue.end() && uni32DataReqCnt > 0;)
                        {

                            vecToSendpieceNums.push_back(*itr);
                            m_priorityQueue.erase(itr++);
                            --uni32DataReqCnt;
                        }
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
                        // 如果优先队列为空，就发送m_downloadQueue中的下载请求
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
        for (auto &&it_sn = m_session_needdownloadpieceQ.begin();
             it_sn != m_session_needdownloadpieceQ.end(); ++it_sn)
        {
            auto &sessId = it_sn->first;
            auto &sessQueue = it_sn->second;
            SPDLOG_TRACE("session Id:{}, session queue:{}", sessId.ToLogStr(), sessQueue);
            DoSendSessionSubTask(it_sn->first);
        }

    } // end of FillUpSessionTask

    /// It's multipath scheduler's duty to maintain session_needdownloadsubpiece, and m_sortmmap
    std::map<fw::ID, std::set<std::pair<SeqNumber, DataNumber>>> m_session_needdownloadpieceQ; // session task queues
    std::multimap<Duration, fw::shared_ptr<SessionStreamController>> m_sortmmap;
    std::multimap<double, fw::shared_ptr<SessionStreamController>> m_sortlrmap;
    fw::weak_ptr<MultiPathSchedulerHandler> m_phandler;
    TransportDownloadTaskInfo &m_tansDlTkInfoRR; 
    uint32_t m_continuousPktSeq;
    uint32_t m_maxSeqAckPktSeqNum;
    Timepoint m_taskStartTp;
};
