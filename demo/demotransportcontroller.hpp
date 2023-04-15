// Copyright (c) 2023. ByteDance Inc. All rights reserved.


#pragma once

#include "transportcontroller.hpp"
#include "multipathschedulerI.h"
#include "congestioncontrol.hpp"
#include "sessionstreamcontroller.hpp"
#include "rrmultipathscheduler.hpp"


struct DemoTransportCtlConfig : public TransPortControllerConfig
{
public:
    DemoTransportCtlConfig();

    ~DemoTransportCtlConfig() override = default;

    uint32_t maxWnd{ 64 };
    uint32_t minWnd{ 1 };
    uint32_t slowStartThreshold{ 32 };

    std::string DebugInfo();
};

/** @brief a demo transport algorithm class implementing the P2PTransportController interface
 * */
class DemoTransportCtl : public MPDTransportController,
                         public MultiPathSchedulerHandler,
                         public SessionStreamCtlHandler,
                         public std::enable_shared_from_this<DemoTransportCtl>
{
public:

    explicit DemoTransportCtl(std::shared_ptr<TransPortControllerConfig> ctlConfig);

    ~ DemoTransportCtl() override;

    /** @brief The Controller is started
     *  @param tansDlTkInfo  download task info
     *  @param transCtlHandler application layer callback
     **/
    bool StartTransportController(TransportDownloadTaskInfo tansDlTkInfo,
            std::weak_ptr<MPDTransCtlHandler> transCtlHandler) override;

    /** @brief The controller should be fully stopped and prepare to be destructed.
     * */
    void StopTransportController() override;

    /**
     * @brief Session is ready to send data request
     * @param sessionid
     */
    void OnSessionCreate(const fw::ID& sessionid) override;

    /**
     * @brief Session is no longer available
     * @param sessionid
     */
    void OnSessionDestory(const fw::ID& sessionid) override;

    /**
     * @brief The application layer is adding tasks to transport layer.
     * @param datapiecesVec the piece numbers to be added
     */
    void OnPieceTaskAdding(std::vector<int32_t>& datapiecesVec) override;

    /**@brief the download task is started now.
     * */
    void OnDownloadTaskStart() override;


    /**@brief the download task is stopped and this object may be destroyed at any time.
     * */
    void OnDownloadTaskStop() override;

    /**@brief the download task has been reset, the task will be stopped soon.
     * */
    void OnDownloadTaskReset() override;

    /** @brief Some data pieces have been received on session with sessionid
     * @param sessionid  the session
     * @param datapiece data pieces number, each packet carry exact one 1KB data piece
     * */
    void OnDataPiecesReceived(const fw::ID& sessionid, uint32_t seq, int32_t datapiece, uint64_t tic_us) override;


    /**
     * @brief Signal that when a data request packet has been sent successfully
     * @note When we say SENT, it means the packet has been given to ASIO to sent. The packet itself may still inside OS
     * kernel space or hardware buffer.
     * @param sessionid remote upside session id
     * @param datapiecesvec the data piece number, each packet may carry 1 piece or 8 pieces
     * @param senttime_us the sent timepoint in us
     */
    void OnDataSent(const fw::ID& sessionid, const std::vector<int32_t>& datapiecesvec,
            const std::vector<uint32_t>& seqvec, uint64_t senttime_us) override;

    /**@brief Check timeout events periodically, the user defined timeout check operation may be called in here.
     * */
    void OnLossDetectionAlarm() override;

    // session stream handler

    void OnPiecePktTimeout(const basefw::ID& peerid, const std::vector<int32_t>& spns) override;

    bool DoSendDataRequest(const basefw::ID& peerid, const std::vector<int32_t>& spns) override;

    //Multipath scheduler handlers

    bool OnGetCurrPlayPos(uint64_t& currplaypos) override;

    bool OnGetCurrCachePos(uint64_t& currcachepos) override;

    bool OnGetByteRate(uint32_t& playbyterate) override;

    void OnRequestDownloadPieces(uint32_t maxpiececnt) override;

private:

    bool isRunning{ false };
    TransportDownloadTaskInfo m_tansDlTkInfo;/// task info, rid,filelength, etc
    std::shared_ptr<DemoTransportCtlConfig> m_transCtlConfig;/// transport module config
    std::unique_ptr<MultiPathSchedulerAlgo> m_multipathscheduler;/// multipath scheduler
    std::weak_ptr<MPDTransCtlHandler> m_transctlHandler; // transport module call back
    std::set<DataNumber> m_downloadPieces;/// main task download queue
    std::set<DataNumber> m_lostPiecesl;/// lost packets will be stored here till retransmission
    RenoCongestionCtlConfig renoccConfig;/// congestion config file
    std::map<basefw::ID, std::shared_ptr<SessionStreamController>> m_sessStreamCtlMap;/// map session id to sessionstream
};

/** @class A demo TransportController used to create DemoTransportCtl
 * */
class DemoTransportCtlFactory : public TransportControllerFactory
{
public:
    ~ DemoTransportCtlFactory() override = default;

    std::shared_ptr<MPDTransportController>
    MakeTransportController(std::shared_ptr<TransPortControllerConfig> ctlConfig) override;
};