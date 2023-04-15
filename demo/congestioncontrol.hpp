// Copyright (c) 2023. ByteDance Inc. All rights reserved.


#pragma once

#include <cstdint>
#include <chrono>
#include "utils/thirdparty/quiche/rtt_stats.h"
#include "basefw/base/log.h"
#include "utils/rttstats.h"
#include "utils/transporttime.h"
#include "utils/defaultclock.hpp"
#include "sessionstreamcontroller.hpp"
#include "packettype.h"

enum class CongestionCtlType : uint8_t
{
    none = 0,
    reno = 1
};

struct LossEvent
{
    bool valid{ false };
    // There may be multiple timeout events at one time
    std::vector<InflightPacket> lossPackets;
    Timepoint losttic{ Timepoint::Infinite() };

    std::string DebugInfo() const
    {
        std::stringstream ss;
        ss << "valid: " << valid << " "
           << "lossPackets:{";
        for (const auto& pkt: lossPackets)
        {
            ss << pkt;
        }

        ss << "} "
           << "losttic: " << losttic.ToDebuggingValue() << " ";
        return ss.str();
    }
};

struct AckEvent
{
    /** since we receive packets one by one, each packet carries only one data piece*/
    bool valid{ false };
    DataPacket ackPacket;
    Timepoint sendtic{ Timepoint::Infinite() };
    Timepoint losttic{ Timepoint::Infinite() };

    std::string DebugInfo() const
    {
        std::stringstream ss;
        ss << "valid: " << valid << " "
           << "ackpkt:{"
           << "seq: " << ackPacket.seq << " "
           << "dataid: " << ackPacket.pieceId << " "
           << "} "
           << "sendtic: " << sendtic.ToDebuggingValue() << " "
           << "losttic: " << losttic.ToDebuggingValue() << " ";
        return ss.str();
    }
};

/** This is a loss detection algorithm interface
 *  similar to the GeneralLossAlgorithm interface in Quiche project
 * */
class LossDetectionAlgo
{

public:
    /** @brief this function will be called when loss detection may happen, like timer alarmed or packet acked
     * input
     * @param downloadingmap all the packets inflight, sent but not acked or lost
     * @param eventtime timpoint that this function is called
     * @param ackEvent  ack event that trigger this function, if any
     * @param maxacked max sequence number that has acked
     * @param rttStats RTT statics module
     * output
     * @param losses loss event
     * */
    virtual void DetectLoss(const InFlightPacketMap& downloadingmap, Timepoint eventtime,
            const AckEvent& ackEvent, uint64_t maxacked, LossEvent& losses, RttStats& rttStats)
    {
    };

    virtual ~LossDetectionAlgo() = default;

};

class DefaultLossDetectionAlgo : public LossDetectionAlgo
{/// Check loss event based on RTO
public:
    void DetectLoss(const InFlightPacketMap& downloadingmap, Timepoint eventtime, const AckEvent& ackEvent,
            uint64_t maxacked, LossEvent& losses, RttStats& rttStats) override
    {
        SPDLOG_TRACE("inflight: {} eventtime: {} ackEvent:{} ", downloadingmap.DebugInfo(),
                eventtime.ToDebuggingValue(), ackEvent.DebugInfo());
        /** RFC 9002 Section 6
         * */
        Duration maxrtt = std::max(rttStats.previous_srtt(), rttStats.latest_rtt());
        if (maxrtt == Duration::Zero())
        {
            SPDLOG_DEBUG(" {}", maxrtt == Duration::Zero());
            maxrtt = rttStats.SmoothedOrInitialRtt();
        }
        Duration loss_delay = maxrtt + (maxrtt * (5.0 / 4.0));
        loss_delay = std::max(loss_delay, Duration::FromMicroseconds(1));
        SPDLOG_TRACE(" maxrtt: {}, loss_delay: {}", maxrtt.ToDebuggingValue(), loss_delay.ToDebuggingValue());
        for (const auto& pkt_itor: downloadingmap.inflightPktMap)
        {
            const auto& pkt = pkt_itor.second;
            if (Timepoint(pkt.sendtic + loss_delay) <= eventtime)
            {
                losses.lossPackets.emplace_back(pkt);
            }
        }
        if (!losses.lossPackets.empty())
        {
            losses.losttic = eventtime;
            losses.valid = true;
            SPDLOG_DEBUG("losses: {}", losses.DebugInfo());
        }
    }

    ~DefaultLossDetectionAlgo() override
    {
    }

private:
};


class CongestionCtlAlgo
{
public:

    virtual ~CongestionCtlAlgo() = default;

    virtual CongestionCtlType GetCCtype() = 0;

    /////  Event
    virtual void OnDataSent(const InflightPacket& sentpkt) = 0;

    virtual void OnDataAckOrLoss(const AckEvent& ackEvent, const LossEvent& lossEvent, RttStats& rttstats) = 0;

    /////
    virtual uint32_t GetCWND() = 0;

//    virtual uint32_t GetFreeCWND() = 0;

};

/// config or setting for specific cc algo
/// used for pass parameters to CongestionCtlAlgo
struct RenoCongestionCtlConfig
{
    uint32_t minCwnd{ 1 };
    uint32_t maxCwnd{ 64 };
    uint32_t ssThresh{ 32 };/** slow start threshold*/
};

class RenoCongestionContrl : public CongestionCtlAlgo
{
public:

    explicit RenoCongestionContrl(const RenoCongestionCtlConfig& ccConfig)
    {
        m_ssThresh = ccConfig.ssThresh;
        m_minCwnd = ccConfig.minCwnd;
        m_maxCwnd = ccConfig.maxCwnd;
        SPDLOG_DEBUG("m_ssThresh:{}, m_minCwnd:{}, m_maxCwnd:{} ", m_ssThresh, m_minCwnd, m_maxCwnd);
    }

    ~RenoCongestionContrl() override
    {
        SPDLOG_DEBUG("");
    }

    CongestionCtlType GetCCtype() override
    {
        return CongestionCtlType::reno;
    }

    void OnDataSent(const InflightPacket& sentpkt) override
    {
        SPDLOG_TRACE("");
    }

    void OnDataAckOrLoss(const AckEvent& ackEvent, const LossEvent& lossEvent, RttStats& rttstats) override
    {
        SPDLOG_TRACE("ackevent:{}, lossevent:{}", ackEvent.DebugInfo(), lossEvent.DebugInfo());
        if (lossEvent.valid)
        {
            OnDataLoss(lossEvent);
        }

        if (ackEvent.valid)
        {
            OnDataRecv(ackEvent);
        }

    }

    /////
    uint32_t GetCWND() override
    {
        SPDLOG_TRACE(" {}", m_cwnd);
        return m_cwnd;
    }

//    virtual uint32_t GetFreeCWND() = 0;

private:

    bool InSlowStart()
    {
        bool rt = false;
        if (m_cwnd < m_ssThresh)
        {
            rt = true;
        }
        else
        {
            rt = false;
        }
        SPDLOG_TRACE(" m_cwnd:{}, m_ssThresh:{}, InSlowStart:{}", m_cwnd, m_ssThresh, rt);
        return rt;
    }

    bool LostCheckRecovery(Timepoint largestLostSentTic)
    {
        SPDLOG_DEBUG("largestLostSentTic:{},lastLagestLossPktSentTic:{}",
                largestLostSentTic.ToDebuggingValue(), lastLagestLossPktSentTic.ToDebuggingValue());
        /** If the largest sent tic of this loss event,is bigger than the last sent tic of the last lost pkt
         * (plus a 10ms correction), this session is in Recovery phase.
         * */
        if (lastLagestLossPktSentTic.IsInitialized() &&
            (largestLostSentTic + Duration::FromMilliseconds(10) > lastLagestLossPktSentTic))
        {
            SPDLOG_DEBUG("In Recovery");
            return true;
        }
        else
        {
            // a new timelost
            lastLagestLossPktSentTic = largestLostSentTic;
            SPDLOG_DEBUG("new loss");
            return false;
        }

    }

    void ExitSlowStart()
    {
        SPDLOG_DEBUG("m_ssThresh:{}, m_cwnd:{}", m_ssThresh, m_cwnd);
        m_ssThresh = m_cwnd;
    }


    void OnDataRecv(const AckEvent& ackEvent)
    {
        SPDLOG_DEBUG("ackevent:{},m_cwnd:{}", ackEvent.DebugInfo(), m_cwnd);
        if (InSlowStart())
        {
            /// add 1 for each ack event
            m_cwnd += 1;

            if (m_cwnd >= m_ssThresh)
            {
                ExitSlowStart();
            }
            SPDLOG_DEBUG("new m_cwnd:{}", m_cwnd);
        }
        else
        {
            /// add cwnd for each RTT
            m_cwndCnt++;
            m_cwnd += m_cwndCnt / m_cwnd;
            if (m_cwndCnt == m_cwnd)
            {
                m_cwndCnt = 0;
            }
            SPDLOG_DEBUG("not in slow start state,new m_cwndCnt:{} new m_cwnd:{}",
                    m_cwndCnt, ackEvent.DebugInfo(), m_cwnd);

        }
        m_cwnd = BoundCwnd(m_cwnd);

        SPDLOG_DEBUG("after RX, m_cwnd={}", m_cwnd);
    }

    void OnDataLoss(const LossEvent& lossEvent)
    {
        SPDLOG_DEBUG("lossevent:{}", lossEvent.DebugInfo());
        Timepoint maxsentTic{ Timepoint::Zero() };

        for (const auto& lostpkt: lossEvent.lossPackets)
        {
            maxsentTic = std::max(maxsentTic, lostpkt.sendtic);
        }

        /** In Recovery phase, cwnd will decrease 1 pkt for each lost pkt
         *  Otherwise, cwnd will cut half.
         * */
        if (InSlowStart())
        {
            // loss in slow start, just cut half
            m_cwnd = m_cwnd / 2;
            m_cwnd = BoundCwnd(m_cwnd);

        }
        else //if (!LostCheckRecovery(maxsentTic))
        {
            // Not In slow start and not inside Recovery state
            // Cut half
            m_cwnd = m_cwnd / 2;
            m_cwnd = BoundCwnd(m_cwnd);
            m_ssThresh = m_cwnd;
            // enter Recovery state
        }
        SPDLOG_DEBUG("after Loss, m_cwnd={}", m_cwnd);
    }


    uint32_t BoundCwnd(uint32_t trySetCwnd)
    {
        return std::max(m_minCwnd, std::min(trySetCwnd, m_maxCwnd));
    }

    uint32_t m_cwnd{ 1 };
    uint32_t m_cwndCnt{ 0 }; /** in congestion avoid phase, used for counting ack packets*/
    Timepoint lastLagestLossPktSentTic{ Timepoint::Zero() };


    uint32_t m_minCwnd{ 1 };
    uint32_t m_maxCwnd{ 64 };
    uint32_t m_ssThresh{ 32 };/** slow start threshold*/
};
