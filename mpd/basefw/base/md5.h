/**
 *  The C++ implementation of the MD5 Message-Digest Algorithm
 */
/*
 * Copyright (C) 1991-2, RSA Data Security, Inc. Created 1991. All
 * rights reserved.
 * 
 * License to copy and use this software is granted provided that it
 * is identified as the "RSA Data Security, Inc. MD5 Message-Digest
 * Algorithm" in all material mentioning or referencing this software
 * or this function.
 * 
 * License is also granted to make and use derivative works provided
 * that such works are identified as "derived from the RSA Data
 * Security, Inc. MD5 Message-Digest Algorithm" in all material
 * mentioning or referencing the derived work.
 * 
 * RSA Data Security, Inc. makes no representations concerning either
 * the merchantability of this software or the suitability of this
 * software for any particular purpose. It is provided "as is"
 * without express or implied warranty of any kind.
 * 
 * These notices must be retained in any copies of any part of this
 * documentation and/or software.
 */
// Copyright (c) 2023. ByteDance Inc. All rights reserved.
#ifndef _BASEFW_MD5_H_
#define _BASEFW_MD5_H_

#include "basefw/commdef.h"

namespace basefw
{
    typedef uint8_t* POINTER;
    typedef uint32_t MD5_UINT4;

    class CMD5
    {
    public:
        /* MD5 context. */
        typedef struct
        {
            MD5_UINT4 state[4];               /* state (ABCD) */
            MD5_UINT4 count[2];               /* number of bits, modulo 2^64 (lsb first) */
            uint8_t buffer[64];               /* input buffer */
        } MD5_CTX;

    public:
        CMD5();
        ~CMD5();

        void Reset();
        void Add(uint8_t* data, uint32_t data_len);
        void Finish();

        void GetHash(uint8_t [16]);

    private:
        void MD5Transform(MD5_UINT4 [4], uint8_t [64]);
        void Encode(uint8_t *, MD5_UINT4 *, MD5_UINT4);
        void Decode(MD5_UINT4 *, uint8_t *, MD5_UINT4);

    private:
        MD5_CTX m_context;
        bool _finished;
    };
}

#endif