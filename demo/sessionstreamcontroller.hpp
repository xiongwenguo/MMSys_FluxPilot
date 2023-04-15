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
    virtual void OnPiecePktTimeout(const basefw::ID &peerid, const std::vector<std::pair<uint32_t, int32_t>> &spns) = 0;

    virtual bool DoSendDataRequest(const basefw::ID &peerid, const std::vector<int32_t> &spns) = 0;
};

/// PacketSender is a simple traffic control module, in TCP or Quic, it is called Pacer.
/// Decide if we can send pkt at this time
class PacketSender
{
public:
    // commented by GXW: 这个 downloadingPktCnt 应该由哪个 API 传参过来
    // commented by GXW: 没看懂机制 但是感觉可改
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
    // commented by GXW: 没看懂机制 但是感觉可改
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

    uint32_t MaySendPktCntTimingK(uint32_t cwnd, uint32_t downloadingPktCnt, uint32_t k)
    {
        SPDLOG_TRACE("cwnd:{},downloadingPktCnt:{}", cwnd, downloadingPktCnt);
        if (cwnd >= downloadingPktCnt)
        {
            return std::min(cwnd - downloadingPktCnt, (uint32_t)(k*8));
        }
        else
        {
            return 0U;
        }
    }
};

/// commented by GXW: 需要搞清楚 congestion control module, loss detection module, traffic control module 是怎么联动的
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

    void StartSessionStreamCtl(const basefw::ID &sessionId, RenoCongestionCtlConfig &ccConfig,
                               std::weak_ptr<SessionStreamCtlHandler> ssStreamHandler)
    {
        if (isRunning)
        {
            SPDLOG_WARN("isRunning = true");
            return;
        }
        m_sumSentPktCount = 0;
        m_sumLossPktCount = 0;
        isRunning = true;
        m_sessionId = sessionId;
        m_ssStreamHandler = ssStreamHandler;
        // cc
        m_ccConfig = ccConfig;
        m_congestionCtl.reset(new RenoCongestionContrl(m_ccConfig));

        // send control
        m_sendCtl.reset(new PacketSender());

        // loss detection
        m_lossDetect.reset(new DefaultLossDetectionAlgo());

        // set initial smoothed rtt
        m_rttstats.set_initial_rtt(Duration::FromMilliseconds(200));
    }

    void StopSessionStreamCtl()
    {
        if (isRunning)
        {
            isRunning = false;
            m_sumSentPktCount = 0;
            m_sumLossPktCount = 0;
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
        // commented by GXW: 没看懂机制: > OK; < not OK
        return m_sendCtl->CanSend(m_congestionCtl->GetCWND(), GetInFlightPktNum());
    }

    // commented by GXW: 感觉可改
    uint32_t CanRequestPktCnt()
    {
        SPDLOG_TRACE("");
        if (!isRunning)
        {
            return false;
        }
        return m_sendCtl->MaySendPktCnt(m_congestionCtl->GetCWND(), GetInFlightPktNum());
    };

    uint32_t CanRequestPktCntTimingK(uint32_t k)
    {
        SPDLOG_TRACE("");
        if (!isRunning)
        {
            return false;
        }
        return m_sendCtl->MaySendPktCntTimingK(m_congestionCtl->GetCWND(), GetInFlightPktNum(), k);
    };

    /// send ONE datarequest Pkt, requestting for the data pieces whose id are in spns
    bool DoRequestdata(const basefw::ID &peerid, const std::vector<int32_t> &spns)
    {
        SPDLOG_TRACE("peerid = {}, spns = {}", peerid.ToLogStr(), spns);
        if (!isRunning)
        {
            return false;
        }
        auto handler = m_ssStreamHandler.lock();
        if (handler)
        {
            // commented by GXW: handler->DoSendDataRequest(peerid, spns) 才是真正执行 sendRequest 的
            return handler->DoSendDataRequest(peerid, spns);
        }
        else
        {
            SPDLOG_WARN("SessionStreamHandler is null");
            return false;
        }
    }
    // 
    void OnDataRequestPktSent(const std::vector<SeqNumber> &seqs,
                              const std::vector<DataNumber> &dataids, Timepoint sendtic)
    {
        SPDLOG_TRACE("seq = {}, dataid = {}, sendtic = {}",
                     seqs,
                     dataids, sendtic.ToDebuggingValue());
        if (!isRunning)
        {
            return;
        }
        m_sumSentPktCount += seqs.size();
        auto seqidx = 0;
        for (auto datano : dataids)
        {
            DataPacket p;
            p.seq = seqs[seqidx];
            p.pieceId = datano;
            // commented by GXW: 如此看来，pieceId 和 DataNumber 是一回事
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
            // commented by GXW: 虽然第二个参数我不懂是什么意思 但是被传入的是 Zero
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

    void InformLossUp(LossEvent &loss)
    {
        if (!isRunning)
        {
            return;
        }
        auto handler = m_ssStreamHandler.lock();
        if (handler)
        {
            std::vector<std::pair<uint32_t, int32_t>> lossedPieces;
            for (auto &&pkt : loss.lossPackets)
            {
                lossedPieces.emplace_back(std::make_pair(pkt.seq, pkt.pieceId));
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
        /// check timeout
        Timepoint now_t = Clock::GetClock()->Now();
        AckEvent ack;
        LossEvent loss;
        // commented by GXW: 在DefaultLossDetectionAlgo::DetectLoss()之中 其实没动 AckEvent
        m_lossDetect->DetectLoss(m_inflightpktmap, now_t, ack, -1, loss, m_rttstats);
        if (loss.valid)
        {
            for (auto &&pkt : loss.lossPackets)
            {
                m_inflightpktmap.RemoveFromInFlight(pkt);
            }
            UpdateLossRate(loss.lossPackets.size());
            m_congestionCtl->OnDataAckOrLoss(ack, loss, m_rttstats);
            InformLossUp(loss);
        }
    }

    Duration GetRtt()
    {
        Duration rtt{Duration::Zero()};
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

    void UpdateLossRate(size_t lossPktCount)
    {
        m_sumLossPktCount += lossPktCount;
        m_lossRate = m_sumLossPktCount * 1.0 / (1.0 * (m_sumSentPktCount - GetInFlightPktNum()));
    }
    double GetLossRate()
    {
        return m_lossRate;
    }

    // GXW
    // 能找到则返回对应的DataNumber 否则返回 -1
    DataNumber FindInFlightPktDataNum(SeqNumber seq)
    {
        auto result = m_inflightpktmap.FindInFlightPktDataNum(seq);
        if(result.first==true)
            return result.second;
        else    
            return -1;
    }

private:
    bool isRunning{false};
    // commented by GXW: 注意区分 sessionId 和 taskId
    basefw::ID m_sessionId; /** The remote peer id defines the session id*/
    basefw::ID m_taskid;    /**The file id downloading*/
    RenoCongestionCtlConfig m_ccConfig;
    std::unique_ptr<CongestionCtlAlgo> m_congestionCtl;
    std::unique_ptr<LossDetectionAlgo> m_lossDetect;
    std::weak_ptr<SessionStreamCtlHandler> m_ssStreamHandler;
    InFlightPacketMap m_inflightpktmap;

    std::unique_ptr<PacketSender> m_sendCtl;
    RttStats m_rttstats;
    size_t m_sumSentPktCount;
    size_t m_sumLossPktCount;
    double m_lossRate;
};
