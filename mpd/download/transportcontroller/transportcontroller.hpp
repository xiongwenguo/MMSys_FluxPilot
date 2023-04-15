
// Copyright (c) 2023. ByteDance Inc. All rights reserved.

#pragma once


#include <set>
#include <vector>
#include <map>
#include <memory>
#include <limits>
#include "basefw/base/hash.h"


enum class TransportControllerType : uint8_t
{
    NONE = 0
};


/** /@brief the task info
 * */
struct TransportDownloadTaskInfo
{
    fw::ID m_rid;
    uint64_t m_filelength{ std::numeric_limits<uint64_t>::max() };
    uint32_t m_byterate{ std::numeric_limits<uint32_t>::max() };

};


/** @brief Transport controller config base struct. A sub class should be created for each distinct algorthm
 *
 * */
struct TransPortControllerConfig
{
    TransportControllerType tranCtlTypeId{ TransportControllerType::NONE };

    virtual ~TransPortControllerConfig() = default;
};

class MPDTransportController;

/**
 * A Factory class to produce the Transport Controller class before download task is started.
 * You should create a derived class, which can produce your transport controller,
 * then pass this factory class to Download SDK.
 * */
class TransportControllerFactory
{
public:
    TransportControllerFactory() = default;

    virtual ~TransportControllerFactory() = default;

    /**@brief P2PDownloadTask will call this function to create a transport controller for each download task.
     * */
    virtual std::shared_ptr<MPDTransportController> MakeTransportController(
            std::shared_ptr<TransPortControllerConfig> ctlConfig)
    {
        return nullptr;
    }
};

/** This struct will be passed to download module, you may put related information inside the derived class
 * */
struct TransportModuleSettings
{
    /** This Factory Class will be used to create a TransportController at proper time.
     * */
    std::shared_ptr<TransportControllerFactory> transportCtlFactory;

    /** The Config field that will be passed to P2PTransportController
     * You may put anything necessary here.
     * */
    std::shared_ptr<TransPortControllerConfig> transportCtlConfig;

    /**@brief The time interval when the system will check which packets have been timed out, default is 100ms.
    *        It is strongly suggested that set this value between 20~200ms. This function will only be called
    *        when the task is started
    *@return the time interval in ms. Default value is 100ms
    * */
    virtual uint32_t GetAlarmInterval()
    {
        return 100U;
    };

    virtual ~TransportModuleSettings() = default;

};


/** /@brief This class is used to communicate with upper layer, the downloadtask.
 * */
class MPDTransCtlHandler
{
public:
    /** @brief Send a DataRequest to request data pieces from session with session id
     * @param datapieces datapieces to request in THIS DataRequest,either 1 piece or 8 pieces.
     * The datapiece number should be in ascending order.
     * @param sessionid upload side session ID
     * @return True if the request leaves user space successfully
     */
    virtual bool DoSendDataRequest(const fw::ID& sessionid, const std::vector<int32_t>& datapieces) = 0;

    /** @brief Request the number of maxpiecesnum from application layer
     *  @param maxpiecesnum tell application layer the number of pieces numbers transport layer wants.
     *                      The application layer respond this request sync or async, and may
     *                      add more or less than maxpiecesnum pieces through AddTask()
     */
    virtual bool DoRequestDatapiecesTask(uint32_t piecesnum) = 0;

    virtual ~MPDTransCtlHandler() = default;
};

/// bind to MPD task

/**@brief This class is the interface of transport algorithm.
 *
 *        All the operation will be called in a single thread (See ASIO library's website), the same thread runs Network IO handlers.
 *        Please do not post any sync IO operation or any time consuming task inside the functions, which will slow down
 *        the IO handle speed. These functions are assumed return quickly. If you do have such heavy duty operations,
 *        consider create a new thread.
 *
 *        Each download task has ONE MPDTransportController
 * */
#include <type_traits>
#include <functional>

class MPDTransportController
{
public:

    explicit MPDTransportController(std::shared_ptr<TransPortControllerConfig> ctlConfig)
    {
    }

    virtual ~MPDTransportController() = default;

    /** @brief The Controller is started
     *  @param tansDlTkInfo  download task info
     *  @param transCtlHandler application layer callback
     **/
    virtual bool StartTransportController(TransportDownloadTaskInfo tansDlTkInfo,
            std::weak_ptr<MPDTransCtlHandler> transCtlHandler)
    {
        return false;
    };

    /** @brief The controller should be fully stopped and prepare to be destructed.
     * */
    virtual void StopTransportController()
    {
    };

    /**
     * @brief Session is ready to send data request
     * @param sessionid
     */
    virtual void OnSessionCreate(const fw::ID& sessionid)
    {
    };

    /**
     * @brief Session is no longer available
     * @param sessionid
     */
    virtual void OnSessionDestory(const fw::ID& sessionid)
    {
    };

    /**
     * @brief The application layer is adding tasks to transport layer.
     * @param datapieces the piece numbers to be added
     */
    virtual void OnPieceTaskAdding(std::vector<int32_t>& datapieces)
    {
    };

    /**@brief the download task is started now.
     * */
    virtual void OnDownloadTaskStart()
    {
    };

    /**@brief the download task is stopped and this object may be destroyed at any time.
     * */
    virtual void OnDownloadTaskStop()
    {
    };

    /**@brief the download task has been reset, the task may be stopped soon.
     * */
    virtual void OnDownloadTaskReset()
    {
    };

    /** @brief Some data pieces have been received on session with sessionid
     * @param sessionid  the session
     * @param datapiece data pieces number, each packet carry exact one 1KB data piece
     * @param tic_us receive time point in microseconds
     * */
    virtual void OnDataPiecesReceived(const fw::ID& sessionid, uint32_t seq, int32_t datapiece, uint64_t tic_us)
    {
    };

    /**
     * @brief Signal that when a data request packet has been sent successfully
     * @note When we say SENT, it means the packet has been given to ASIO to sent. The packet itself may still inside OS
     * kernel space or hardware buffer.
     * @param sessionid remote upside session id
     * @param datapiecesvec the data piece number, each packet may carry 1 piece or 8 pieces
     * @param seqvec sequence number
     * @param senttime_us the sent time point in us
     */
    virtual void
    OnDataSent(const fw::ID& sessionid, const std::vector<int32_t>& datapiecesvec,
            const std::vector<uint32_t>& seqvec, uint64_t senttime_us)
    {
    };

    /**@brief Check timeout events periodically, the user defined timeout check operation may be called in here.
     * */
    virtual void OnLossDetectionAlarm()
    {
    };

};

