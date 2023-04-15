// Copyright (c) 2023. ByteDance Inc. All rights reserved.

#pragma once

#include <deque>
#include <memory>
#include "congestioncontrol.hpp"
#include "basefw/base/log.h"
#include "packettype.h"

class SessionStreamCtlHandler
{
public:
    virtual void OnPiecePktTimeout(const basefw::ID& peerid, const std::vector<int32_t>& spns) = 0;

    virtual bool DoSendDataRequest(const basefw::ID& peerid, const std::vector<int32_t>& spns) = 0;
};

/// PacketSender is a simple traffic control module, in TCP or Quic, it is called Pacer.
/// Decide if we can send pkt at this time
class PacketSender
{
public:
    bool CanSend(uint32_t cwnd, uint32_t downloadingPktCnt)
    {

        auto rt = false;
        if (cwnd > downloadingPktCnt)
        {
            rt = true;
        }
        else
        {
            rt = false;
        }
        SPDLOG_TRACE("cwnd:{},downloadingPktCnt:{},rt: {}", cwnd, downloadingPktCnt, rt);
        return rt;
    }

    uint32_t MaySendPktCnt(uint32_t cwnd, uint32_t downloadingPktCnt)
    {
        SPDLOG_TRACE("cwnd:{},downloadingPktCnt:{}", cwnd, downloadingPktCnt);
        if (cwnd >= downloadingPktCnt)
        {
            return std::min(cwnd - downloadingPktCnt, 8U);
        }
        else
        {
            return 0U;
        }
    }

};

/// SessionStreamController is the single session delegate inside transport module.
/// This single session contains three part, congestion control module, loss detection module, traffic control module.
/// It may be used to send data request in its session and receive the notice when packets has been sent
class SessionStreamController
{
public:

    SessionStreamController()
    {
        SPDLOG_TRACE("");
    }

    ~SessionStreamController()
    {
        SPDLOG_TRACE("");
        StopSessionStreamCtl();
    }

    void StartSessionStreamCtl(const basefw::ID& sessionId, RenoCongestionCtlConfig& ccConfig,
            std::weak_ptr<SessionStreamCtlHandler> ssStreamHandler)
    {
        if (isRunning)
        {
            SPDLOG_WARN("isRunning = true");
            return;
        }
        isRunning = true;
        m_sessionId = sessionId;
        m_ssStreamHandler = ssStreamHandler;
        // cc
        m_ccConfig = ccConfig;
        m_congestionCtl.reset(new RenoCongestionContrl(m_ccConfig));

        // send control
        m_sendCtl.reset(new PacketSender());

        //loss detection
        m_lossDetect.reset(new DefaultLossDetectionAlgo());

        // set initial smothed rtt
        m_rttstats.set_initial_rtt(Duration::FromMilliseconds(200));

    }

    void StopSessionStreamCtl()
    {
        if (isRunning)
        {
            isRunning = false;
        }
        else
        {
            SPDLOG_WARN("isRunning = false");
        }
    }

    basefw::ID GetSessionId()
    {
        if (isRunning)
        {
            return m_sessionId;
        }
        else
        {
            return {};
        }
    }

    bool CanSend()
    {
        SPDLOG_TRACE("");
        if (!isRunning)
        {
            return false;
        }

        return m_sendCtl->CanSend(m_congestionCtl->GetCWND(), GetInFlightPktNum());
    }

    uint32_t CanRequestPktCnt()
    {
        SPDLOG_TRACE("");
        if (!isRunning)
        {
            return false;
        }
        return m_sendCtl->MaySendPktCnt(m_congestionCtl->GetCWND(), GetInFlightPktNum());
    };

    /// send ONE datarequest Pkt, requestting for the data pieces whose id are in spns
    bool DoRequestdata(const basefw::ID& peerid, const std::vector<int32_t>& spns)
    {
        SPDLOG_TRACE("peerid = {}, spns = {}", peerid.ToLogStr(), spns);
        if (!isRunning)
        {
            return false;
        }
        if (!CanSend())
        {
            SPDLOG_WARN("CanSend = false");
            return false;
        }

        if (spns.size() > CanRequestPktCnt())
        {
            SPDLOG_WARN("The number of request data pieces {} exceeds the freewnd {}", spns.size(), CanRequestPktCnt());
            return false;
        }
        auto handler = m_ssStreamHandler.lock();
        if (handler)
        {
            return handler->DoSendDataRequest(peerid, spns);
        }
        else
        {
            SPDLOG_WARN("SessionStreamHandler is null");
            return false;
        }

    }

    void OnDataRequestPktSent(const std::vector<SeqNumber>& seqs,
            const std::vector<DataNumber>& dataids, Timepoint sendtic)
    {
        SPDLOG_TRACE("seq = {}, dataid = {}, sendtic = {}",
                seqs,
                dataids, sendtic.ToDebuggingValue());
        if (!isRunning)
        {
            return;
        }
        auto seqidx = 0;
        for (auto datano: dataids)
        {
            DataPacket p;
            p.seq = seqs[seqidx];
            p.pieceId = datano;
            // add to downloading queue
            m_inflightpktmap.AddSentPacket(p, sendtic);

            // inform cc algo that a packet is sent
            InflightPacket sentpkt;
            sentpkt.seq = seqs[seqidx];
            sentpkt.pieceId = datano;
            sentpkt.sendtic = sendtic;
            m_congestionCtl->OnDataSent(sentpkt);
            seqidx++;
        }

    }

    void OnDataPktReceived(uint32_t seq, int32_t datapiece, Timepoint recvtic)
    {
        if (!isRunning)
        {
            return;
        }
        // find the sending record
        auto rtpair = m_inflightpktmap.PktIsInFlight(seq, datapiece);
        auto inFlight = rtpair.first;
        auto inflightPkt = rtpair.second;
        if (inFlight)
        {

            auto oldsrtt = m_rttstats.smoothed_rtt();
            // we don't have ack_delay in this simple implementation.
            auto pkt_rtt = recvtic - inflightPkt.sendtic;
            m_rttstats.UpdateRtt(pkt_rtt, Duration::Zero(), Clock::GetClock()->Now());
            auto newsrtt = m_rttstats.smoothed_rtt();

            auto oldcwnd = m_congestionCtl->GetCWND();

            AckEvent ackEvent;
            ackEvent.valid = true;
            ackEvent.ackPacket.seq = seq;
            ackEvent.ackPacket.pieceId = datapiece;
            ackEvent.sendtic = inflightPkt.sendtic;
            LossEvent lossEvent; // if we detect loss when ACK event, we may do loss check here.
            m_congestionCtl->OnDataAckOrLoss(ackEvent, lossEvent, m_rttstats);

            auto newcwnd = m_congestionCtl->GetCWND();
            // mark as received
            m_inflightpktmap.OnPacktReceived(inflightPkt, recvtic);
        }
        else
        {
            SPDLOG_WARN(" Recv an pkt with unknown seq:{}", seq);
        }

    }

    void OnLossDetectionAlarm()
    {
        DoAlarmTimeoutDetection();
    }

    void InformLossUp(LossEvent& loss)
    {
        if (!isRunning)
        {
            return;
        }
        auto handler = m_ssStreamHandler.lock();
        if (handler)
        {
            std::vector<int32_t> lossedPieces;
            for (auto&& pkt: loss.lossPackets)
            {
                lossedPieces.emplace_back(pkt.pieceId);
            }
            handler->OnPiecePktTimeout(m_sessionId, lossedPieces);
        }
    }

    void DoAlarmTimeoutDetection()
    {
        if (!isRunning)
        {
            return;
        }
        ///check timeout
        Timepoint now_t = Clock::GetClock()->Now();
        AckEvent ack;
        LossEvent loss;
        m_lossDetect->DetectLoss(m_inflightpktmap, now_t, ack, -1, loss, m_rttstats);
        if (loss.valid)
        {
            for (auto&& pkt: loss.lossPackets)
            {
                m_inflightpktmap.RemoveFromInFlight(pkt);
            }
            m_congestionCtl->OnDataAckOrLoss(ack, loss, m_rttstats);
            InformLossUp(loss);
        }
    }

    Duration GetRtt()
    {
        Duration rtt{ Duration::Zero() };
        if (isRunning)
        {
            rtt = m_rttstats.smoothed_rtt();
        }
        SPDLOG_TRACE("rtt = {}", rtt.ToDebuggingValue());
        return rtt;
    }

    uint32_t GetInFlightPktNum()
    {
        return m_inflightpktmap.InFlightPktNum();
    }


    basefw::ID GetSessionID()
    {
        return m_sessionId;
    }

private:
    bool isRunning{ false };

    basefw::ID m_sessionId;/** The remote peer id defines the session id*/
    basefw::ID m_taskid;/**The file id downloading*/
    RenoCongestionCtlConfig m_ccConfig;
    std::unique_ptr<CongestionCtlAlgo> m_congestionCtl;
    std::unique_ptr<LossDetectionAlgo> m_lossDetect;
    std::weak_ptr<SessionStreamCtlHandler> m_ssStreamHandler;
    InFlightPacketMap m_inflightpktmap;

    std::unique_ptr<PacketSender> m_sendCtl;
    RttStats m_rttstats;
};

