// Copyright (c) 2023. ByteDance Inc. All rights reserved.

#include "demotransportcontroller.hpp"

DemoTransportCtlConfig::DemoTransportCtlConfig() : TransPortControllerConfig()
{
}


std::string DemoTransportCtlConfig::DebugInfo()
{
    std::stringstream ss;
    ss
            << "{"
            << "minWnd:" << minWnd << " maxWnd:" << maxWnd << " slowStartThreshold:" << slowStartThreshold
            << " }";
    return ss.str();
}


DemoTransportCtl::DemoTransportCtl(std::shared_ptr<TransPortControllerConfig> ctlConfig)
        : MPDTransportController(ctlConfig)
{
    //multipathscheduler = std::make_shared(RRMultiPathScheduler());
    m_transCtlConfig = std::dynamic_pointer_cast<DemoTransportCtlConfig>(ctlConfig);
    renoccConfig.minCwnd = m_transCtlConfig->minWnd;
    renoccConfig.maxCwnd = m_transCtlConfig->maxWnd;
    renoccConfig.ssThresh = m_transCtlConfig->slowStartThreshold;
    SPDLOG_DEBUG("config:{}", m_transCtlConfig->DebugInfo());
}

DemoTransportCtl::~DemoTransportCtl()
{
    SPDLOG_DEBUG("");
    if (isRunning)
    {
        SPDLOG_ERROR("Destruct before stop");
    }
}

/** @brief The Controller is started
 *  @param tansDlTkInfo  download task info
 *  @param transCtlHandler application layer callback
 **/
bool DemoTransportCtl::StartTransportController(TransportDownloadTaskInfo tansDlTkInfo,
        std::weak_ptr<MPDTransCtlHandler> transCtlHandler)
{
    if (isRunning)
    {
        //warn: already started
        SPDLOG_WARN("Already running");
        return true;
    }
    else
    {
        isRunning = true;
    }
    SPDLOG_DEBUG("taskid: {}", tansDlTkInfo.m_rid.ToLogStr());

    m_transctlHandler = transCtlHandler;

    m_multipathscheduler.reset(
            new RRMultiPathScheduler(m_tansDlTkInfo.m_rid, m_sessStreamCtlMap, m_downloadPieces, m_lostPiecesl));
    m_multipathscheduler->StartMultiPathScheduler(shared_from_this());
    return true;
}

/** @brief The controller should be fully stopped and prepare to be destructed.
 * */
void DemoTransportCtl::StopTransportController()
{
    SPDLOG_DEBUG("Stop Transport Controller isRunning = {}", isRunning);
    if (!isRunning)
    {
        return;
    }
    isRunning = false;
    // todo:
    if (m_multipathscheduler)
    {
        m_multipathscheduler->StopMultiPathScheduler();
    }

    if (!m_sessStreamCtlMap.empty())
    {
        for (auto&& id_sess: m_sessStreamCtlMap)
        {
            if (id_sess.second)
            {
                if (id_sess.second)
                {
                    id_sess.second->StopSessionStreamCtl();
                    id_sess.second.reset();
                }
            }
        }
    }
}

/**
 * @brief Session is ready to send data request
 * @param sessionid
 */
void DemoTransportCtl::OnSessionCreate(const fw::ID& sessionid)
{
    SPDLOG_TRACE("session = {}",sessionid.ToLogStr());
    if (!isRunning)
    {
        SPDLOG_WARN(" Start session before Start transport Module");
    }
    auto&& sessionItor = m_sessStreamCtlMap.find(sessionid);
    if (sessionItor == m_sessStreamCtlMap.end())
    {
        m_sessStreamCtlMap[sessionid] = std::make_shared<SessionStreamController>();
        m_sessStreamCtlMap[sessionid]->StartSessionStreamCtl(sessionid, renoccConfig, shared_from_this());
    }
    else
    {
        // session recreate again
        SPDLOG_WARN("session:{} already created", sessionid.ToLogStr());
    }
    // forward the message
    if (m_multipathscheduler)
    {
        m_multipathscheduler->OnSessionCreate(sessionid);
    }
    else
    {
        SPDLOG_ERROR("multipathscheduler = null");
    }
}

/**
 * @brief Session is no longer available
 * @param sessionid
 */
void DemoTransportCtl::OnSessionDestory(const fw::ID& sessionid)
{
    SPDLOG_TRACE("session = {}",sessionid.ToLogStr());
    if (!isRunning)
    {
        SPDLOG_WARN(" Stop session before Start transport Module");
    }
    // forward the message First
    if (m_multipathscheduler)
    {
        m_multipathscheduler->OnSessionDestory(sessionid);
    }
    else
    {
        SPDLOG_ERROR("multipathscheduler = null");
    }
    // do clean jobs
    auto&& sessionItor = m_sessStreamCtlMap.find(sessionid);
    if (sessionItor == m_sessStreamCtlMap.end())
    {
        // warn: try to destroy a session we don't know
        SPDLOG_WARN(" try to destroy a session we don't know");
    }
    else
    {
        m_sessStreamCtlMap[sessionid]->StopSessionStreamCtl();
        m_sessStreamCtlMap[sessionid].reset();
        m_sessStreamCtlMap.erase(sessionid);
    }

}

/**
 * @brief The application layer is adding tasks to transport layer.
 * @param datapiecesVec the piece numbers to be added
 */
void DemoTransportCtl::OnPieceTaskAdding(std::vector<int32_t>& datapiecesVec)
{
    for (auto&& dataPiece: datapiecesVec)
    {
        auto rt = m_downloadPieces.emplace(dataPiece);
        if (!rt.second)
        {
            // warning: already has
        }
    }
    // Do multipath schedule after new tasks added
    m_multipathscheduler->DoMultiPathSchedule();
}

/**@brief the download task is started now.
 * */
void DemoTransportCtl::OnDownloadTaskStart()
{
    if (isRunning)
    {
        if (m_multipathscheduler)
        {
            m_multipathscheduler->StartMultiPathScheduler(shared_from_this());
        }
    }

}

/**@brief the download task is stopped and this object may be destroyed at any time.
 * */
void DemoTransportCtl::OnDownloadTaskStop()
{
    StopTransportController();
}

/**@brief the download task has been reset, the task will be stopped soon.
 * */
void DemoTransportCtl::OnDownloadTaskReset()
{
    StopTransportController();
}

/** @brief Some data pieces have been received on session with sessionid
 * @param sessionid  the session
 * @param datapiece data pieces number, each packet carry exact one 1KB data piece
 * */
void DemoTransportCtl::OnDataPiecesReceived(const fw::ID& sessionid, uint32_t seq, int32_t datapiece, uint64_t tic_us)
{
    SPDLOG_TRACE("session = {}, seq ={},datapiece = {},tic_us = {}",sessionid.ToLogStr(),seq,datapiece,tic_us);
    Timepoint recvtic = Clock::GetClock()->CreateTimeFromMicroseconds(tic_us);
    // call session control firstly to change cwnd first
    auto&& sessionItor = m_sessStreamCtlMap.find(sessionid);
    if (sessionItor != m_sessStreamCtlMap.end())
    {
        sessionItor->second->OnDataPktReceived(seq, datapiece, recvtic);
    }
    else
    {
        //warn: received on an unknown session
        SPDLOG_WARN("received on an unknown session {}", sessionid.ToLogStr());
    }
    // inform multipath scheduler
    m_multipathscheduler->OnReceiveSubpieceData(sessionid, seq, datapiece, recvtic);
}

/**
 * @brief Signal that when a data request packet has been sent successfully
 * @note When we say SENT, it means the packet has been given to ASIO to sent. The packet itself may still inside OS
 * kernel space or hardware buffer.
 * @param sessionid remote upside session id
 * @param datapiecesvec the data piece number, each packet may carry 1 piece or 8 pieces
 * @param senttime_us the sent timepoint in us
 */
void DemoTransportCtl::OnDataSent(const fw::ID& sessionid, const std::vector<int32_t>& datapiecesvec,
        const std::vector<uint32_t>& seqvec,
        uint64_t senttime_us)
{
    SPDLOG_DEBUG("sessionid = {}, datapieces = {}, seq = {}, senttic = {}",
            sessionid.ToLogStr(),
            datapiecesvec,
            seqvec, senttime_us);
    auto&& sessStreamItor = m_sessStreamCtlMap.find(sessionid);
    if (sessStreamItor != m_sessStreamCtlMap.end())
    {
        sessStreamItor->second->OnDataRequestPktSent(
                seqvec,
                datapiecesvec, Clock::GetClock()->CreateTimeFromMicroseconds(senttime_us));
    }
    else
    {
        SPDLOG_WARN("sessionid = {} can't found in session map ", sessionid.ToLogStr());
    }
}

/**@brief Check timeout events periodically, the user defined timeout check operation may be called in here.
 * */
void DemoTransportCtl::OnLossDetectionAlarm()
{
    SPDLOG_TRACE("DemoTransportCtl::OnLossDetectionAlarm()");
    // Step 1: Check loss in each session
    for (auto&& sessStreamItor: m_sessStreamCtlMap)
    {
        sessStreamItor.second->OnLossDetectionAlarm();
    }
    // Step 2: Forward message to Multipath Scheduler
    m_multipathscheduler->DoMultiPathSchedule();
}

// session stream handler

void DemoTransportCtl::OnPiecePktTimeout(const basefw::ID& peerid, const std::vector<int32_t>& spns)
{
    if (!isRunning)
    {
        return;
    }

    m_multipathscheduler->OnTimedOut(peerid, spns);

}

bool DemoTransportCtl::DoSendDataRequest(const basefw::ID& peerid, const std::vector<int32_t>& spns)
{
    SPDLOG_TRACE("peerid = {}, spns= {}", peerid.ToLogStr(), spns);
    if (!isRunning)
    {
        SPDLOG_TRACE("isRunning = false");
        return false;
    }
    else
    {

    }

    auto handler = m_transctlHandler.lock();
    if (handler)
    {
        return handler->DoSendDataRequest(peerid, spns);
    }
    else
    {
        SPDLOG_WARN("handler == null");
        return false;
    }

}

//Multipath scheduler handlers

bool DemoTransportCtl::OnGetCurrPlayPos(uint64_t& currplaypos)
{
    // curr play pos in Byte
    return false;
}

bool DemoTransportCtl::OnGetCurrCachePos(uint64_t& currcachepos)
{
    return false;

}

bool DemoTransportCtl::OnGetByteRate(uint32_t& playbyterate)
{
    // bytes per second
    return false;
}

void DemoTransportCtl::OnRequestDownloadPieces(uint32_t maxpiececnt)
{
    if (!isRunning)
    {
        return;
    }
    // ask for more task pieces
    auto handler = m_transctlHandler.lock();
    if (handler)
    {
        handler->DoRequestDatapiecesTask(maxpiececnt);
    }
    else
    {
        SPDLOG_WARN("Handler = null");
    }
}

std::shared_ptr<MPDTransportController>
DemoTransportCtlFactory::MakeTransportController(std::shared_ptr<TransPortControllerConfig> ctlConfig)
{
    std::shared_ptr<MPDTransportController> transportControllerImpl(new DemoTransportCtl(ctlConfig));
    return transportControllerImpl;
}

