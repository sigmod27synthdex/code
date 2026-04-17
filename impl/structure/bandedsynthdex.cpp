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

#include "bandedsynthdex.h"
#include <algorithm>
#include <omp.h>

using namespace std;


void BandedSynthDex::parse_band_range(
    const string &band_str, double &start_rel, double &end_rel)
{
    // Find separator dash that is not part of scientific notation (e- or E-)
    size_t pos = string::npos;
    for (size_t i = 1; i < band_str.size(); ++i)
    {
        if (band_str[i] == '-' && band_str[i - 1] != 'e' && band_str[i - 1] != 'E')
        {
            pos = i;
            break;
        }
    }
    if (pos == string::npos)
        throw runtime_error("Invalid band_range format: " + band_str);
    start_rel = stod(band_str.substr(0, pos));
    end_rel = stod(band_str.substr(pos + 1));
}


BandedSynthDex::BandedSynthDex(
    const IRelation &O,
    const vector<IdxSchema> &band_schemas,
    const vector<OStats> &per_band_ostats)
{
    this->domain = O.gend;
    size_t num_bands = band_schemas.size();

    bool use_optimized = Cfg::get<bool>("synthesis.use-templated-synthdex");

    // Parse band boundaries and prepare data structures
    this->bands.resize(num_bands);
    vector<IRelation> O_fragments(num_bands);

    for (size_t i = 0; i < num_bands; ++i)
    {
        double start_rel, end_rel;
        parse_band_range(band_schemas[i].band_range, start_rel, end_rel);

        Timestamp abs_start = static_cast<Timestamp>(start_rel * this->domain);
        Timestamp abs_end = static_cast<Timestamp>(end_rel * this->domain);

        this->bands[i].start_rel = start_rel;
        this->bands[i].end_rel = end_rel;
        this->bands[i].abs_start = abs_start;
        this->bands[i].abs_end = abs_end;
        this->bands[i].index = nullptr;

        // Reserve conservatively (records can appear in multiple bands)
        size_t est = static_cast<size_t>(
            O.size() * ((abs_end - abs_start)
                / static_cast<double>(this->domain)) * 1.5);
        O_fragments[i].reserve(est);
    }

    // Slice O: assign each record to ALL overlapping bands, clipped to local coords
    for (const auto &record : O)
    {
        for (size_t i = 0; i < num_bands; ++i)
        {
            if (record.start < this->bands[i].abs_end
                && record.end > this->bands[i].abs_start)
            {
                Timestamp local_start = max(record.start, this->bands[i].abs_start)
                    - this->bands[i].abs_start;
                Timestamp local_end = min(record.end, this->bands[i].abs_end)
                    - this->bands[i].abs_start;

                IRecord local_record(record.id, local_start, local_end);
                local_record.elements = record.elements;
                O_fragments[i].push_back(local_record);
            }
        }
    }

    // Finalize fragments and validate before parallel construction
    for (size_t i = 0; i < num_bands; ++i)
    {
        O_fragments[i].determineDomain();
        O_fragments[i].shrink_to_fit();

        if (O_fragments[i].empty())
            throw runtime_error("BandedSynthDex: band "
                + band_schemas[i].band_range + " has no records");
    }

    // Build per-band indexes in parallel
    #pragma omp parallel for schedule(dynamic)
    for (size_t i = 0; i < num_bands; ++i)
    {

        const OStats &ostats = (i < per_band_ostats.size())
            ? per_band_ostats[i]
            : per_band_ostats.back();

        if (use_optimized)
            this->bands[i].index = new SynthDexOpt(
                O_fragments[i], band_schemas[i], ostats);
        else
            this->bands[i].index = new SynthDex(
                O_fragments[i], band_schemas[i], ostats);
    }
}


void BandedSynthDex::query(const RangeIRQuery &q, RelationId &result)
{
    // Transform query bounds in-place to avoid per-query heap allocation
    // from copying q.elems. Callers pass elements from a mutable vector
    // via const ref, so the const_cast is safe -- we restore before return.
    auto &mq = const_cast<RangeIRQuery &>(q);
    Timestamp orig_start = mq.start;
    Timestamp orig_end = mq.end;

    if (this->bands.size() == 1)
    {
        // Single-band fast path: no overlap scan, no dedup
        const auto &band = this->bands[0];
        if (orig_end < band.abs_start || orig_start > band.abs_end) return;

        mq.start = max(orig_start, band.abs_start) - band.abs_start;
        mq.end = min(orig_end, band.abs_end) - band.abs_start;
        band.index->query(mq, result);
        mq.start = orig_start;
        mq.end = orig_end;
        return;
    }

    // Multi-band: find overlapping bands
    int overlap_count = 0;
    const BandInfo *single = nullptr;

    for (const auto &band : this->bands)
    {
        if (orig_end < band.abs_start || orig_start > band.abs_end) continue;
        overlap_count++;
        if (overlap_count == 1)
            single = &band;
        else
            break;
    }

    if (overlap_count == 1)
    {
        mq.start = max(orig_start, single->abs_start) - single->abs_start;
        mq.end = min(orig_end, single->abs_end) - single->abs_start;
        single->index->query(mq, result);
        mq.start = orig_start;
        mq.end = orig_end;
        return;
    }

    if (overlap_count > 1)
    {
        for (const auto &band : this->bands)
        {
            if (orig_end < band.abs_start || orig_start > band.abs_end) continue;

            mq.start = max(orig_start, band.abs_start) - band.abs_start;
            mq.end = min(orig_end, band.abs_end) - band.abs_start;

            RelationId band_result;
            band.index->query(mq, band_result);
            result.insert(result.end(), band_result.begin(), band_result.end());
        }

        mq.start = orig_start;
        mq.end = orig_end;

        // Deduplicate: sort + unique is more cache-friendly than set
        sort(result.begin(), result.end());
        result.erase(unique(result.begin(), result.end()), result.end());
    }
}


void BandedSynthDex::update(const IRelation &R)
{
    for (auto &band : this->bands)
        band.index->update(R);
}


void BandedSynthDex::remove(const vector<bool> &idsToDelete)
{
    for (auto &band : this->bands)
        band.index->remove(idsToDelete);
}


void BandedSynthDex::softdelete(const vector<bool> &idsToDelete)
{
    for (auto &band : this->bands)
        band.index->softdelete(idsToDelete);
}


size_t BandedSynthDex::getSize()
{
    size_t size = sizeof(*this);
    size += this->bands.size() * sizeof(BandInfo);
    for (const auto &band : this->bands)
        size += band.index->getSize();
    return size;
}


string BandedSynthDex::str() const
{
    ostringstream oss;
    oss << "BandedSynthDex (" << this->bands.size() << " bands)";
    for (size_t i = 0; i < this->bands.size(); ++i)
    {
        const auto &band = this->bands[i];

        // Indent sub-index lines under the band header
        string sub = band.index->str();
        string indented;
        bool first_line = true;
        for (size_t p = 0; p < sub.size(); )
        {
            size_t nl = sub.find('\n', p);
            if (nl == string::npos) nl = sub.size();
            if (!first_line)
                indented += "\n\t  ";
            indented += sub.substr(p, nl - p);
            first_line = false;
            p = nl + 1;
        }

        oss << "\n\t+ Band " << i << " [" << band.start_rel << "-" << band.end_rel
            << "] (time [" << band.abs_start << ".." << band.abs_end << ")): "
            << indented;
    }
    return oss.str();
}


BandedSynthDex::~BandedSynthDex()
{
    for (auto &band : this->bands)
    {
        delete band.index;
    }
}
