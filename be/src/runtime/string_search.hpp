// This is taken from the python search string function doing string search (substring)
// using an optimized boyer-moore-horspool algorithm.
// http://hg.python.org/cpython/file/6b6c79eba944/Objects/stringlib/fastsearch.h
//
// PYTHON SOFTWARE FOUNDATION LICENSE VERSION 2
// --------------------------------------------
//
// 1. This LICENSE AGREEMENT is between the Python Software Foundation
// ("PSF"), and the Individual or Organization ("Licensee") accessing and
// otherwise using this software ("Python") in source or binary form and
// its associated documentation.
//
// 2. Subject to the terms and conditions of this License Agreement, PSF
// hereby grants Licensee a nonexclusive, royalty-free, world-wide
// license to reproduce, analyze, test, perform and/or display publicly,
// prepare derivative works, distribute, and otherwise use Python
// alone or in any derivative version, provided, however, that PSF's
// License Agreement and PSF's notice of copyright, i.e., "Copyright (c)
// 2001, 2002, 2003, 2004, 2005, 2006 Python Software Foundation; All Rights
// Reserved" are retained in Python alone or in any derivative version
// prepared by Licensee.
//
// 3. In the event Licensee prepares a derivative work that is based on
// or incorporates Python or any part thereof, and wants to make
// the derivative work available to others as provided herein, then
// Licensee hereby agrees to include in any such work a brief summary of
// the changes made to Python.
//
// 4. PSF is making Python available to Licensee on an "AS IS"
// basis. PSF MAKES NO REPRESENTATIONS OR WARRANTIES, EXPRESS OR
// IMPLIED. BY WAY OF EXAMPLE, BUT NOT LIMITATION, PSF MAKES NO AND
// DISCLAIMS ANY REPRESENTATION OR WARRANTY OF MERCHANTABILITY OR FITNESS
// FOR ANY PARTICULAR PURPOSE OR THAT THE USE OF PYTHON WILL NOT
// INFRINGE ANY THIRD PARTY RIGHTS.
//
// 5. PSF SHALL NOT BE LIABLE TO LICENSEE OR ANY OTHER USERS OF PYTHON
// FOR ANY INCIDENTAL, SPECIAL, OR CONSEQUENTIAL DAMAGES OR LOSS AS
// A RESULT OF MODIFYING, DISTRIBUTING, OR OTHERWISE USING PYTHON,
// OR ANY DERIVATIVE THEREOF, EVEN IF ADVISED OF THE POSSIBILITY THEREOF.
//
// 6. This License Agreement will automatically terminate upon a material
// breach of its terms and conditions.
//
// 7. Nothing in this License Agreement shall be deemed to create any
// relationship of agency, partnership, or joint venture between PSF and
// Licensee. This License Agreement does not grant permission to use PSF
// trademarks or trade name in a trademark sense to endorse or promote
// products or services of Licensee, or any third party.
//
// 8. By copying, installing or otherwise using Python, Licensee
// agrees to be bound by the terms and conditions of this License
// Agreement.

#pragma once

#include <cstdint>
#include <cstring>
#include <vector>

#include "common/logging.h"
#include "util/slice.h"

namespace starrocks {

// TODO: This can be sped up with SIDD_CMP_EQUAL_ORDERED or at the very least rewritten
// from published algorithms.
class StringSearch {
public:
    virtual ~StringSearch() = default;
    StringSearch() = default;

    // Initialize/Precompute a StringSearch object from the pattern
    StringSearch(const Slice* pattern) : _pattern(pattern) {
        // Special cases
        if (_pattern->size <= 1) {
            return;
        }

        // Build compressed lookup table
        int mlast = _pattern->size - 1;
        _skip = mlast - 1;

        for (int i = 0; i < mlast; ++i) {
            bloom_add(_pattern->data[i]);

            if (_pattern->data[i] == _pattern->data[mlast]) {
                _skip = mlast - i - 1;
            }
        }

        bloom_add(_pattern->data[mlast]);
    }

    // search for this pattern in str.
    //   Returns the offset into str if the pattern exists
    //   Returns -1 if the pattern is not found
    int search(const Slice str) const {
        // Special cases
        if (!_pattern || _pattern->size == 0) {
            return -1;
        }

        int mlast = _pattern->size - 1;
        int w = str.size - _pattern->size;
        int n = str.size;
        int m = _pattern->size;
        const char* s = str.data;
        const char* p = _pattern->data;

        // Special case if pattern->len == 1
        if (m == 1) {
            const char* result = reinterpret_cast<const char*>(memchr(s, p[0], n));

            if (result != nullptr) {
                return result - s;
            }

            return -1;
        }

        // General case.
        int j;
        // TODO: the original code seems to have an off by one error. It is possible
        // to index at w + m which is the length of the input string. Checks have
        // been added to make sure that w + m < str->len.
        for (int i = 0; i <= w; i++) {
            // note: using mlast in the skip path slows things down on x86
            if (s[i + m - 1] == p[m - 1]) {
                // candidate match
                for (j = 0; j < mlast; j++)
                    if (s[i + j] != p[j]) {
                        break;
                    }

                if (j == mlast) {
                    return i;
                }

                // miss: check if next character is part of pattern
                if (i + m < n && !bloom_query(s[i + m])) {
                    i = i + m;
                } else {
                    i = i + _skip;
                }
            } else {
                // skip: check if next character is part of pattern
                if (i + m < n && !bloom_query(s[i + m])) {
                    i = i + m;
                }
            }
        }

        return -1;
    }

private:
    static const int BLOOM_WIDTH = 64;

    void bloom_add(char c) { _mask |= (1UL << (c & (BLOOM_WIDTH - 1))); }

    bool bloom_query(char c) const { return _mask & (1UL << (c & (BLOOM_WIDTH - 1))); }

    const Slice* _pattern;
    int64_t _mask{0};
    int64_t _skip = 0;
};

} // namespace starrocks
