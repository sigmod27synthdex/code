/******************************************************************************
 * Project:  synthdex
 * Purpose:  Adaptive TIR indexing
 * Author:   Anonymous Author(s)
 ******************************************************************************
 * Copyright (c) 2025 - 2026
 *
 * All rights reserved.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included
 * in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
 * OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
 * THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 ******************************************************************************/

#ifndef _BANDEDSYNTHDEX_H_
#define _BANDEDSYNTHDEX_H_

#include "../utils/global.h"
#include "../learning/stats.h"
#include "../containers/relations.h"
#include "../containers/hierarchical_index.h"
#include "../structure/idxschema.h"
#include "../structure/idxschemaserializer.h"
#include "../structure/synthdex.h"
#include "../structure/synthdexopt.h"

#include <vector>
#include <string>


struct BandInfo
{
    double start_rel;        // Relative band start in [0,1]
    double end_rel;          // Relative band end   in [0,1]
    Timestamp abs_start;     // Absolute band start timestamp
    Timestamp abs_end;       // Absolute band end   timestamp
    IRIndex* index;          // Per-band SynthDex or SynthDexOpt
};


class BandedSynthDex : public IRIndex
{
public:
    BandedSynthDex(
        const IRelation &O,
        const vector<IdxSchema> &band_schemas,
        const vector<OStats> &per_band_ostats);

    void query(const RangeIRQuery &q, RelationId &result) override;

    void update(const IRelation &R) override;

    void remove(const vector<bool> &idsToDelete) override;

    void softdelete(const vector<bool> &idsToDelete) override;

    size_t getSize() override;

    string str() const override;

    ~BandedSynthDex();

    static void parse_band_range(
        const string &band_str, double &start_rel, double &end_rel);

private:
    vector<BandInfo> bands;
    Timestamp domain;
};

#endif // _BANDEDSYNTHDEX_H_
