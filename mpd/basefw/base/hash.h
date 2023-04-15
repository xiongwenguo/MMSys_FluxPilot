// Copyright (c) 2023. ByteDance Inc. All rights reserved.
/*******************************************************
*
* Author(s): Pxf
*
*******************************************************/
#ifndef _BASEFW_HASH_H
#define _BASEFW_HASH_H

#include "basefw/commdef.h"
#include "basefw/base/md5.h"

namespace basefw
{
    class ID
    {
    public:
        ID();
        explicit ID(uint8_t b[20]);
        explicit ID(const std::string& idstr);

        std::string ToStr() const;
        std::string ToLogStr() const;
        bool IsEmpty() const;
        void Reset();
        uint8_t* Getbuf();
        const uint8_t* Getbuf() const;
        ID(const basefw::ID& id);
        ID& operator = (const ID& id);
        bool operator == (const ID& id) const;
        bool operator != (const ID& id) const;
        bool operator < (const ID& id) const;

        static const ID EmptyID;

    private:
        bool Parse(const std::string& idstr);
        int32_t ctoi(char ch);
        static void BCDtoASCII(const uint8_t *str, int strlen, char *ascii) ;

    private:
        uint8_t ID_20_[20];
        static const uint32_t m_buffer_len = 20;
    };

    class ShortID {
    public:
        ShortID();
        explicit ShortID(const ID& id);

        bool operator==(const ShortID& id) const;
        bool operator!=(const ShortID& id) const;
        bool operator<(const ShortID& id) const;

        std::string ToStr() const;

        uint32_t GetInternalVal() const;

        friend class CPacket;

    private:
        uint32_t id_;
    };

    class CHash
    {
    public:
        CHash();
        ~CHash();

        void Add(uint8_t* data, uint32_t data_len);
        void Finish();

        ID GetHashID();

    private:
        CMD5 m_md5;
        uint8_t buf[20];
    };
}


#endif