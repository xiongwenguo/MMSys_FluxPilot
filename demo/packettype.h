// Copyright (c) 2023. ByteDance Inc. All rights reserved.

#pragma once
using SeqNumber = uint32_t;
using DataNumber = int32_t;
#define MAX_SEQNUMBER std::numeric_limits<uint32_t>::max()
#define MAX_DATANUMBER std::numeric_limits<int32_t>::max()

#include <sstream>

struct DataPacket
{
    SeqNumber seq{ MAX_SEQNUMBER };
    DataNumber pieceId{ MAX_DATANUMBER };

    virtual ~DataPacket() = default;
};

struct InflightPacket : DataPacket
{
    Timepoint sendtic{ Timepoint::Zero() };

    friend std::ostream& operator<<(std::ostream& os, const InflightPacket& pkt)
    {
        os << "{ seq: " << pkt.seq << " piceId: " << pkt.pieceId << " sendtic: " << pkt.sendtic << " }";
        return os;
    }

    virtual std::string DebugInfo()
    {
        std::stringstream ss;
        ss << *this;
        return ss.str();
    }

    ~InflightPacket() override
    {
    }
};

struct AckedPacket : InflightPacket
{
    Timepoint recvtic{ Timepoint::Zero() };

    friend std::ostream& operator<<(std::ostream& os, const AckedPacket& pkt)
    {
        os << "{ seq: " << pkt.seq << " piceId: " << pkt.pieceId <<
           " sendtic: " << pkt.sendtic << "recvtic: " << " }";
        return os;
    }

    virtual std::string DebugInfo() override
    {
        std::stringstream ss;
        ss << *this;
        return ss.str();
    }

    ~AckedPacket() override
    {
    }
};

class InFlightPacketMap
{
public:
    void AddSentPacket(DataPacket& p, QuicTime sendtic)
    {
        InflightPacket inflightPacket;
        inflightPacket.seq = p.seq;
        inflightPacket.pieceId = p.pieceId;
        inflightPacket.sendtic = sendtic;
        auto&& itor_pair = inflightPktMap.emplace(std::make_pair(p.seq, p.pieceId), inflightPacket);
        if (!itor_pair.second)
        {
            SPDLOG_WARN("insert failed with seq = {},duplicate pkt?", p.seq);
        }
        else
        {
            if (p.seq > maxSeqInflightPkt.seq)
            {
                maxSeqInflightPkt = inflightPacket;
            }
            SPDLOG_DEBUG("Add pkt with seq = {}, max inflight {}, max acked {}", p.seq,
                    MaxSeqInflightPkt().DebugInfo(), MaxSeqAckedPkt().DebugInfo());
        }

        SPDLOG_TRACE("Sent seq:{}, map now:{}", p.seq, DebugInfo());
    }

    void OnPacktReceived(InflightPacket& p, QuicTime recvtic)
    {
        auto&& itor = inflightPktMap.find(std::make_pair(p.seq, p.pieceId));
        if (itor != inflightPktMap.end())
        {
            inflightPktMap.erase(std::make_pair(p.seq, p.pieceId));
            SPDLOG_DEBUG("recv pkt with seq = {}, max inflight {}, max acked {}", p.seq,
                    MaxSeqInflightPkt().DebugInfo(), MaxSeqAckedPkt().DebugInfo());
            if (p.seq > maxSeqAckPkt.seq)
            {
                AckedPacket ackpkt;
                ackpkt.seq = p.seq;
                ackpkt.pieceId = p.pieceId;
                ackpkt.sendtic = p.sendtic;
                ackpkt.recvtic = recvtic;
                maxSeqAckPkt = ackpkt;
            }
        }
        else
        {
            SPDLOG_WARN("Receive a pkt with unknown seq {}", p.seq);
        }


        SPDLOG_TRACE("Recv seq:{}, map now:{}", p.seq, DebugInfo());
    }

    // packet is marked as lost
    void RemoveFromInFlight(InflightPacket& p)
    {
        auto&& itor = inflightPktMap.find(std::make_pair(p.seq, p.pieceId));
        if (itor != inflightPktMap.end())
        {
            inflightPktMap.erase(std::make_pair(p.seq, p.pieceId));
            SPDLOG_DEBUG("remove pkt with seq = {}, max inflight {}, max acked {}", p.seq,
                    MaxSeqInflightPkt().DebugInfo(), MaxSeqAckedPkt().DebugInfo());
        }
        else
        {
            SPDLOG_WARN("Remove a pkt with unknown seq {}", p.seq);
        }
        SPDLOG_TRACE("remove seq:{}, map now:{}", p.seq, DebugInfo());
    }

    size_t InFlightPktNum() const
    {
        return inflightPktMap.size();
    }

    /// return Inflight packet with max sequence number

    InflightPacket MaxSeqInflightPkt()
    {
        return maxSeqInflightPkt;
    }

    /// return Acked packet with max sequence number
    AckedPacket MaxSeqAckedPkt()
    {
        return maxSeqAckPkt;
    }


    /// return <true,found pkt> or <false,InflightPacket()>
    std::pair<bool, InflightPacket> PktIsInFlight(SeqNumber seq, DataNumber dataid = MAX_DATANUMBER)
    {
        std::pair<bool, InflightPacket> rt = std::make_pair(false, InflightPacket());
        const auto& itor = inflightPktMap.find(std::make_pair(seq, dataid));
        if (itor != inflightPktMap.end())
        {
            if (dataid != MAX_DATANUMBER && dataid != itor->second.pieceId)
            {
                SPDLOG_WARN("seq {} found, but data id is different. Input dataid: {}, found dataid:{}",
                        seq, dataid, itor->second.pieceId);
            }
            rt.first = true;
            rt.second = itor->second;
        }
        SPDLOG_TRACE("seq:{}", seq);
        return rt;
    }

    std::string DebugInfo() const
    {
        std::stringstream ss;
        ss << " { ";
        for (const auto& itor: inflightPktMap)
        {
            ss << itor.second;
        }
        ss << " } ";
        return ss.str();
    }

    AckedPacket maxSeqAckPkt;
    InflightPacket maxSeqInflightPkt;
    using InFlKeyType = std::pair<SeqNumber, DataNumber>;
    std::map<InFlKeyType, InflightPacket> inflightPktMap;
};