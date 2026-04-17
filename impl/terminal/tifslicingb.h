/******************************************************************************
 * Project:  synthdex
 * Purpose:  Adaptive TIR indexing
 * Author:   Anonymous Author(s)
 ******************************************************************************
 * Copyright (c) 2025 - 2026
 *
 *
 * Extending
 *
 * Project:  irhint
 * Purpose:  Fast indexing for termporal information retrieval
 * Author:   Anonymous Author(s)
 ******************************************************************************
 * Copyright (c) 2023 - 2024
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

#ifndef _tIFSLICINGb_H_
#define _tIFSLICINGb_H_

#include <vector>
#include <queue>
#include "../structure/framework.h"
#include "../containers/relations.h"
#include "../containers/inverted_file.h"
#include "../containers/1dgrid.h"
#include "../containers/hint_m.h"
#include "../utils/global.h"



class tIFSLICINGb : public Refinement_ElemFreq
{
private:
    uint partitions;
    double dynratio;
    double dynbase;
    Timestamp time_start;
    ElementId elem_min;
    ElementId elem_max;
    // Posting lists (vector indexed by elem - lists_offset for O(1) access)
    vector<OneDimensionalGrid*> lists;
    ElementId lists_offset;
    
public:
    // Constructing
    tIFSLICINGb();
    tIFSLICINGb(
        const Boundaries &boundaries,
        const uint &partitions,
        const double &dynbase,
        const double &dynratio,
        const IRelation &R);
    void getStats();
    size_t getSize();
    void print(char c);
    ~tIFSLICINGb();

    // Updating
    void update(const IRelation &R);

    // Deleting
    void remove(const vector<bool> &idsToDelete);

    // Soft-deleting (tombstone: replace id with -1)
    void softdelete(const vector<bool> &idsToDelete);

    // Querying
    bool moveOut_checkBoth(const RangeIRQuery &q, RelationId &candidates);
    bool intersectx(const RangeIRQuery &q, const unsigned int elemoff, RelationId &candidates);
    bool intersectAndOutput(const RangeIRQuery &q, const unsigned int off, RelationId &candidates, RelationId &result);
    
    // Querying
    void move_out(
        const RangeIRQuery &q,
        ElementId &elem_off,
        RelationId &candidates);

    void refine(
        const RangeIRQuery &q,
        ElementId &elem_off,
        RelationId &candidates);

    string str(const int level) const;
};


#endif
