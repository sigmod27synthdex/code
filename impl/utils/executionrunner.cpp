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

#include "executionrunner.h"
#include <iostream>
#include <sstream>
#include <random>
#include <algorithm>
#include <fstream>
#include <filesystem>
#include <regex>

using namespace std;


ExecutionRunner::ExecutionRunner(
    const string& idxschema_str,
    bool do_query,
    bool do_predict,
    const string& file_O,
    const string& file_Q,
    const IRelation& O,
    const vector<tuple<string,vector<RangeIRQuery>>>& Q,
    const OStats& Ostats,
    StatsComp& statscomp,
    const vector<string>& groups)
    : idxschema_str(resolve_idxschema(idxschema_str)), do_query(do_query), do_predict(do_predict),
      file_O(file_O), file_Q(file_Q),
      O(O), Q(Q), Ostats(Ostats), statscomp(statscomp),
      groups(groups),
      idx(nullptr)
{
}


string ExecutionRunner::resolve_idxschema(const string& idxschema_str)
{
    if (idxschema_str.size() >= 4
        && idxschema_str.substr(idxschema_str.size() - 4) == ".idx")
    {
        if (!filesystem::exists(idxschema_str))
            throw runtime_error("Index schema file not found: " + idxschema_str);
        ifstream ifs(idxschema_str);
        string content((istreambuf_iterator<char>(ifs)),
                        istreambuf_iterator<char>());
        // Trim trailing whitespace/newlines
        while (!content.empty() && (content.back() == '\n' || content.back() == '\r' || content.back() == ' '))
            content.pop_back();
        return content;
    }
    return idxschema_str;
}


int ExecutionRunner::execute()
{
    if (this->idxschema_str == "!")
    {
        this->execute_synthetic();
        return (int)this->Q.size();
    }
    else
        return this->execute_userdefined();
}


double ExecutionRunner::construct(const IdxSchema &schema)
{
    Log::w(1, "Schema", IdxSchemaSerializer::to_json(schema));

    // Choose between SynthDex (runtime) and SynthDexOpt (template-based)
    bool use_optimized = Cfg::get<bool>("synthesis.use-templated-synthdex");
    Log::w(2, "Using", use_optimized ? "SynthDexOpt (template-based)" : "SynthDex (runtime)");

    Timer timer;
    timer.start();
    if (use_optimized)
        this->idx = new SynthDexOpt(this->O, schema, this->Ostats);
    else
        this->idx = new SynthDex(this->O, schema, this->Ostats);
    double time = timer.stop();

    Log::w(1, "Structure", this->idx->str());

    Log::w(1, "Indexing time [s]", time);
    Log::w(1, "Size [megabytes]", this->idx->getSize() / (1024.0 * 1024.0));

    return time;
}


double ExecutionRunner::construct_banded(const vector<IdxSchema> &band_schemas,
    const vector<OStats> &per_band_ostats)
{
    Log::w(1, "Banded schema (compact)",
        IdxSchemaSerializer::to_json_banded_line(band_schemas));

    Timer timer;
    timer.start();
    this->idx = new BandedSynthDex(this->O, band_schemas, per_band_ostats);
    double time = timer.stop();

    Log::w(1, "Structure", this->idx->str());
    Log::w(1, "Indexing time [s]", time);
    Log::w(1, "Size [megabytes]", this->idx->getSize() / (1024.0 * 1024.0));

    return time;
}


void ExecutionRunner::execute_synthetic()
{
    map<string, vector<IdxSchemaSuggestion>> suggestions;

    auto prep = SynthesisRunner::prepare_banding(
        this->O, this->file_O, this->Q, this->statscomp, this->groups);

    if (prep.has_banded_templates && !prep.batched_Q.empty())
    {
        SynthesisRunner synthesisrunner(
            prep.batched_Q, this->statscomp, prep.per_q_ostats,
            this->groups, prep.band_inner_templates);
        suggestions = synthesisrunner.run();
    }
    else if (!prep.has_banded_templates)
    {
        SynthesisRunner synthesisrunner(
            this->Q, this->statscomp, {}, this->groups);
        suggestions = synthesisrunner.run();
    }

    if (suggestions.empty()) return;

    // Check if suggestions contain temporal bands
    bool has_bands = any_of(suggestions.begin(), suggestions.end(),
        [](const auto& p) { return any_of(p.second.begin(), p.second.end(),
            [](const auto& s) { return !s.temporal_band.empty(); }); });

    if (has_bands)
    {
        auto br = SynthesisRunner::process_banded(
            suggestions, prep.batched_Q, prep.all_slice_bounds, this->O.gend);

        if (!br.schemas.empty())
        {
            Log::w(0, "Banded Index (" + to_string(br.schemas.size()) + " bands)");
            Log::w(1, "Banded schema",
                IdxSchemaSerializer::to_json_banded(br.schemas));

            auto per_band_ostats = SynthesisRunner::compute_per_band_ostats(
                br.schemas, this->O, this->statscomp);

            auto time = this->construct_banded(br.schemas, per_band_ostats);

            // Evaluate against all Q
            auto Q_single = this->Q;
            IndexEvaluator* evaluator = Cfg::get<bool>("check-results")
                ? static_cast<IndexEvaluator*>(
                    new ResultValidator(
                        this->O, Q_single, this->Ostats,
                        this->file_O.empty(), this->file_Q.empty()))
                : static_cast<IndexEvaluator*>(
                    new PerformanceAnalyzer(
                        this->O, Q_single, this->Ostats, true));

            evaluator->run(this->idx, this->Istats, time);

            delete this->idx;
            delete evaluator;
            return;
        }
    }

    // Non-banded path: original per-Q logic
    auto Q_num = 0;
    for (auto &Qx : this->Q)
    {
        Log::w(0, progress("Index", Q_num++, (int)this->Q.size()));

        auto Q_single = vector<tuple<string,vector<RangeIRQuery>>>{Qx};
        IndexEvaluator* evaluator = Cfg::get<bool>("check-results") 
        ? static_cast<IndexEvaluator*>(
            new ResultValidator(
                this->O, Q_single, this->Ostats,
                this->file_O.empty(), this->file_Q.empty()))
        : static_cast<IndexEvaluator*>(
            new PerformanceAnalyzer(
                this->O, Q_single, this->Ostats));

        auto Qname = get<0>(Qx);

        auto it = find_if(suggestions.begin(), suggestions.end(),
            [&](const auto& pair)
            { return pair.second.front().oq_file.find(Qname) != string::npos; });
        auto suggestion = it->second.front();

        Log::w(1, "Workload adaptation", strs(suggestion.workload, "\n"));
        Log::w(1, "Optimization", suggestion.variant);

        this->idxschema = suggestion.idxschema;
        this->Istats = this->statscomp.analyze_i(this->idxschema, "synthetic");

        auto time = this->construct(this->idxschema);

        evaluator->run(this->idx, this->Istats, time);

        delete this->idx;

        delete evaluator;
    }
}


int ExecutionRunner::execute_userdefined()
{
    // Determine index schemas to explore
    struct IndexTask { optional<string> pattern; string label; };
    vector<IndexTask> tasks;

    if (this->idxschema_str != "")
    {
        // Check if the user-provided string is a banded schema (JSON array)
        stringstream ss_check(this->idxschema_str);
        string first_part;
        getline(ss_check, first_part, '|');
        if (!first_part.empty() && IdxSchemaSerializer::is_banded(first_part))
        {
            // Banded schema path — single banded schema, no pipe splitting
            tasks.push_back({optional<string>(this->idxschema_str), "banded"});
        }
        else
        {
            // User-provided explicit schema(s) via pipe-separated string
            stringstream ss(this->idxschema_str);
            string part;
            while (getline(ss, part, '|'))
                if (!part.empty())
                    tasks.push_back({optional<string>(part), part});
        }
    }
    else
    {
        // Per-template exploration: for each active template, generate
        // the number of random configurations defined in
        // "i.gen.exploration-per-O" (falls back to 0 if not specified).
        auto resolved = IGen::resolve_active_templates(this->groups);
        for (const auto& [group, pattern_name] : resolved)
        {
            int count = 0;
            try {
                count = Cfg::get<int>(
                    "i.gen.exploration-per-O." + pattern_name);
            } catch (...) {
                // Template not listed in exploration-per-O, skip
                continue;
            }

            auto pattern_json = Cfg::get_json(
                "i.gen.design-space." + group + "." + pattern_name);

            for (int j = 0; j < count; j++)
                tasks.push_back({optional<string>(pattern_json),
                    pattern_name + " (" + to_string(j+1)
                    + "/" + to_string(count) + ")"});
        }
    }

    auto idx_num = (int)tasks.size();

    // Randomize task order
    random_device rd;
    mt19937 rng(rd());
    shuffle(tasks.begin(), tasks.end(), rng);

    // Create one evaluator per Q entry before the index loop so that
    // result_cnts/result_xors survive across index configs (cross-index
    // consistency checking is preserved).
    vector<vector<tuple<string,vector<RangeIRQuery>>>> single_Qs;
    single_Qs.reserve(this->Q.size());
    for (const auto& Qx : this->Q)
        single_Qs.push_back({Qx});

    bool check = Cfg::get<bool>("check-results");
    vector<IndexEvaluator*> evaluators;
    evaluators.reserve(single_Qs.size());
    for (const auto& sq : single_Qs)
    {
        evaluators.push_back(check
            ? static_cast<IndexEvaluator*>(new ResultValidator(
                this->O, sq, this->Ostats,
                this->file_O.empty(), this->file_Q.empty()))
            : static_cast<IndexEvaluator*>(new PerformanceAnalyzer(
                this->O, sq, this->Ostats)));
    }

    Timer eta_timer;
    eta_timer.start();

    for (int i = 0; i < idx_num; i++)
    {
        Log::w(0, progress("Index", i, idx_num));
        Log::w(1, "Template", tasks[i].label);

        // Check if this task is a banded schema
        bool is_banded_task = tasks[i].pattern.has_value()
            && IdxSchemaSerializer::is_banded(tasks[i].pattern.value());

        if (is_banded_task)
        {
            auto banded_schemas = IdxSchemaSerializer::from_json_banded(
                tasks[i].pattern.value());

            Log::w(1, "Banded schema (" + to_string(banded_schemas.size()) + " bands)");

            auto per_band_ostats = SynthesisRunner::compute_per_band_ostats(
                banded_schemas, this->O, this->statscomp);

            auto time = this->construct_banded(banded_schemas, per_band_ostats);

            for (const auto& sq : single_Qs)
            {
                PerformanceAnalyzer pa(this->O, sq, this->Ostats, true);
                pa.run(this->idx, this->Istats, time);
            }

            delete this->idx;
        }
        else
        {
        this->idxschema = IGen(this->groups).construct_I(tasks[i].pattern);

        auto idxschema_json_line
            = IdxSchemaSerializer::to_json_line(this->idxschema);

        this->Istats = this->statscomp.analyze_i(this->idxschema, "manual");

        if (this->do_predict || Cfg::get<bool>("prediction.predict-before-run"))
            this->predict();

        if (!this->do_query) continue;

        auto time = this->construct(this->idxschema);

        for (auto* evaluator : evaluators)
            evaluator->run(this->idx, this->Istats, time);
        
        delete this->idx;
        }

        if (idx_num > 1)
        {
            double elapsed = eta_timer.stop();
            double avg = elapsed / (i + 1);
            double remaining = avg * (idx_num - i - 1);
            int rem_min = (int)(remaining / 60);
            int rem_sec = (int)remaining % 60;
            Log::w(0, "ETA all Is for O", to_string(rem_min) + "m " + to_string(rem_sec) + "s"
                + " (" + to_string((int)elapsed) + "s elapsed, "
                + to_string(idx_num - i - 1) + " remaining)");
        }
    }

    for (auto* e : evaluators) delete e;

    return idx_num;
}


void ExecutionRunner::predict()
{
    ProcessExec::python_setup(false);

    Log::w(0, "Prediction");

    Log::w(1, "Schema", IdxSchemaSerializer::to_json(this->idxschema));

    auto lcm_regex = Cfg::get<string>("out.machine-prefix");
    Log::w(1, "LCM regex", lcm_regex);

    this->statscomp.Qstats.clear();

    vector<string> files_OQI;
    for (auto& Qx : this->Q)
    {
        auto name = Persistence::compose_name(this->Ostats, Qx);

        this->statscomp.analyze_Q(get<1>(Qx));

        auto file_OQI = Persistence::write_OQI_stats_csv(
            this->Ostats, this->statscomp.Qstats, this->Istats, name);

        this->statscomp.Qstats.clear();

        files_OQI.push_back(file_OQI);
    }

    auto files_OQI_str = accumulate(
        next(files_OQI.begin()), files_OQI.end(), "\"" + files_OQI[0] + "\"",
        [](const string &a, const string &b) { return a + " \"" + b + "\""; });

    ProcessExec::python_run("Inference",
        "./learning/prediction.py \"" + Cfg::config_path
        + "\" \"" + Cfg::get_out_dir() + "\" " + files_OQI_str, 1);
}


void ExecutionRunner::update(const string& file_O, const string& file_O2,
    const string& idxschema_str, StatsComp& statscomp)
{
    Log::w(0, "Constructing and Updating Index");

    // Load original objects
    auto O = Persistence::read_O_dat(file_O);
    Log::w(1, "Records count", to_string(O.size()));
    
    // Compute statistics for original objects
    auto Ostats = statscomp.analyze_O(O, file_O);

    // Load new objects for update
    auto O2 = Persistence::read_O_dat(file_O2);
    Log::w(1, "Records count", to_string(O2.size()));
    double ratio_org = (double)O.size() / (O.size() + O2.size()) * 100.0;
    double ratio_upd = (double)O2.size() / (O.size() + O2.size()) * 100.0;
    
    char ratio_str[100];
    snprintf(ratio_str, sizeof(ratio_str), "%.2f%% Construction + %.2f%% Update", ratio_org, ratio_upd);
    Log::w(1, "Records ratio", string(ratio_str));

    for (auto& r : O2) r.id += O.size();

    auto resolved = resolve_idxschema(idxschema_str);
    auto idxschema_parsed = IGen().construct_I(optional<string>(resolved));

    // Construct initial index with original objects
    Log::w(0, "Construction");

    Log::w(1, "Schema", IdxSchemaSerializer::to_json(idxschema_parsed));

    bool use_optimized = Cfg::get<bool>("synthesis.use-templated-synthdex");
    Log::w(2, "Using", use_optimized ? "SynthDexOpt (template-based)" : "SynthDex (runtime)");

    IRIndex* idx;
    Timer timer;
    timer.start();
    if (use_optimized)
        idx = new SynthDexOpt(O, idxschema_parsed, Ostats);
    else
        idx = new SynthDex(O, idxschema_parsed, Ostats);
    double construction_time = timer.stop();

    Log::w(1, "Structure", idx->str());
    Log::w(1, "Indexing time [s]", construction_time);
    Log::w(1, "Size [megabytes]", idx->getSize() / (1024.0 * 1024.0));

    // Update index with new objects
    Log::w(0, "Update");

    timer.start();

    idx->update(O2);

    double update_time = timer.stop();

    Log::w(1, "Updating time [s]", update_time);
    Log::w(1, "Size [megabytes]", idx->getSize() / (1024.0 * 1024.0));
    
    delete idx;
}


void ExecutionRunner::remove(const string& file_O, const string& file_O2,
    const string& idxschema_str, StatsComp& statscomp)
{
    Log::w(0, "Constructing Index and Deleting Records");

    // Load original objects
    auto O = Persistence::read_O_dat(file_O);
    Log::w(1, "Records count", to_string(O.size()));
    
    // Compute statistics for original objects
    auto Ostats = statscomp.analyze_O(O, file_O);

    // Load IDs to delete
    auto idsToDelete = Persistence::read_Oids_dat(file_O2);
    Log::w(1, "Records to delete", to_string(idsToDelete.size()));
    double ratio_org = (double)O.size() / (O.size() + idsToDelete.size()) * 100.0;
    double ratio_del = (double)idsToDelete.size() / (O.size() + idsToDelete.size()) * 100.0;
    
    char ratio_str[100];
    snprintf(ratio_str, sizeof(ratio_str), "%.2f%% Construction + %.2f%% Deletion", ratio_org, ratio_del);
    Log::w(1, "Records ratio", string(ratio_str));

    auto resolved = resolve_idxschema(idxschema_str);
    auto idxschema_parsed = IGen().construct_I(optional<string>(resolved));

    // Construct initial index with original objects
    Log::w(0, "Construction");

    Log::w(1, "Schema", IdxSchemaSerializer::to_json(idxschema_parsed));

    bool use_optimized = Cfg::get<bool>("synthesis.use-templated-synthdex");
    Log::w(2, "Using", use_optimized ? "SynthDexOpt (template-based)" : "SynthDex (runtime)");

    IRIndex* idx;
    Timer timer;
    timer.start();
    if (use_optimized)
        idx = new SynthDexOpt(O, idxschema_parsed, Ostats);
    else
        idx = new SynthDex(O, idxschema_parsed, Ostats);
    double construction_time = timer.stop();

    Log::w(1, "Structure", idx->str());
    Log::w(1, "Indexing time [s]", construction_time);
    Log::w(1, "Size [megabytes]", idx->getSize() / (1024.0 * 1024.0));

    // Delete objects from index
    Log::w(0, "Delete");

    timer.start();

    RecordId maxId = *max_element(idsToDelete.begin(), idsToDelete.end());
    vector<bool> idsBitmap(maxId + 1, false);
    for (auto id : idsToDelete) idsBitmap[id] = true;
    idx->remove(idsBitmap);

    double delete_time = timer.stop();

    Log::w(1, "Deletion time [s]", delete_time);
    Log::w(1, "Size [megabytes]", idx->getSize() / (1024.0 * 1024.0));
    
    delete idx;
}


void ExecutionRunner::softdelete(const string& file_O, const string& file_O2,
    const string& idxschema_str, StatsComp& statscomp)
{
    Log::w(0, "Constructing Index and Soft-Deleting Records");

    // Load original objects
    auto O = Persistence::read_O_dat(file_O);
    Log::w(1, "Records count", to_string(O.size()));
    
    // Compute statistics for original objects
    auto Ostats = statscomp.analyze_O(O, file_O);

    // Load IDs to soft-delete
    auto idsToDelete = Persistence::read_Oids_dat(file_O2);
    Log::w(1, "Records to soft-delete", to_string(idsToDelete.size()));
    double ratio_org = (double)O.size() / (O.size() + idsToDelete.size()) * 100.0;
    double ratio_del = (double)idsToDelete.size() / (O.size() + idsToDelete.size()) * 100.0;
    
    char ratio_str[100];
    snprintf(ratio_str, sizeof(ratio_str), "%.2f%% Construction + %.2f%% Soft-Deletion", ratio_org, ratio_del);
    Log::w(1, "Records ratio", string(ratio_str));

    auto resolved = resolve_idxschema(idxschema_str);
    auto idxschema_parsed = IGen().construct_I(optional<string>(resolved));

    // Construct initial index with original objects
    Log::w(0, "Construction");

    Log::w(1, "Schema", IdxSchemaSerializer::to_json(idxschema_parsed));

    bool use_optimized = Cfg::get<bool>("synthesis.use-templated-synthdex");
    Log::w(2, "Using", use_optimized ? "SynthDexOpt (template-based)" : "SynthDex (runtime)");

    IRIndex* idx;
    Timer timer;
    timer.start();
    if (use_optimized)
        idx = new SynthDexOpt(O, idxschema_parsed, Ostats);
    else
        idx = new SynthDex(O, idxschema_parsed, Ostats);
    double construction_time = timer.stop();

    Log::w(1, "Structure", idx->str());
    Log::w(1, "Indexing time [s]", construction_time);
    Log::w(1, "Size [megabytes]", idx->getSize() / (1024.0 * 1024.0));

    // Soft-delete objects from index (replace IDs with tombstone -1)
    Log::w(0, "Soft-Delete");

    timer.start();

    // Build bitmap once, pass by reference through the entire index tree
    RecordId maxId = *max_element(idsToDelete.begin(), idsToDelete.end());
    vector<bool> idsBitmap(maxId + 1, false);
    for (auto id : idsToDelete) idsBitmap[id] = true;
    idx->softdelete(idsBitmap);

    double softdelete_time = timer.stop();

    Log::w(1, "Soft-deletion time [s]", softdelete_time);
    Log::w(1, "Size [megabytes]", idx->getSize() / (1024.0 * 1024.0));
    
    delete idx;
}


void ExecutionRunner::synthesize(
    const vector<tuple<string,vector<RangeIRQuery>>>& Q,
    const OStats& Ostats, StatsComp& statscomp,
    const vector<string>& groups)
{
    SynthesisRunner synthesisrunner(Q, statscomp, {}, groups);
    auto result = synthesisrunner.run();

    for (const auto& [oq_file, suggestions] : result)
    {
        // Suggestions are already sorted by synthesisrunner (best first = smallest predicted_performance)
        
        // Get best values for percentage calculations
        double best_throughput = suggestions.empty() ? 0.0 : 
            1.0 / pow(10, suggestions[0].predicted_performance);
        double best_size_mb = suggestions.empty() ? 0.0 : 
            pow(10, suggestions[0].predicted_size) / (1024*1024);
        
        int top = 0;
        int top_limit = Cfg::get<int>("synthesis.top-k");
        double top_relative = Cfg::get<double>("synthesis.top-relative");
        double prev_throughput = 0.0;
        double prev_size_mb = 0.0;
        
        for (const auto& suggestion : suggestions)
        {
            // Convert from log10 space to linear space
            // predicted_performance is log10(s/q), so 10^predicted_performance gives s/q
            auto time_per_query_sq = pow(10, suggestion.predicted_performance);
            auto throughput_qps = 1.0 / time_per_query_sq;
            auto size_mb = pow(10, suggestion.predicted_size) / (1024*1024);
            
            // Skip if both throughput and size are only marginally different from previous
            if (top > 0 && prev_throughput > 0 && prev_size_mb > 0)
            {
                double throughput_diff_pct = abs((prev_throughput - throughput_qps) / prev_throughput) * 100.0;
                double size_diff_pct = abs((prev_size_mb - size_mb) / prev_size_mb) * 100.0;
                
                if (throughput_diff_pct < top_relative && size_diff_pct < top_relative)
                {
                    Log::w(2, "Skipping suggestion (only " + to_string(throughput_diff_pct) + 
                           "% throughput diff, " + to_string(size_diff_pct) + "% size diff)");
                    continue;
                }
            }
            
            // Calculate percentage scores relative to best (100% = best for throughput, smaller % = better for size)
            double throughput_pct = (best_throughput > 0) ? (throughput_qps / best_throughput) * 100.0 : 100.0;
            double size_pct = (best_size_mb > 0) ? (size_mb / best_size_mb) * 100.0 : 100.0;
            
            // Format percentages with one decimal place
            stringstream throughput_pct_str, size_pct_str;
            throughput_pct_str << fixed << setprecision(1) << throughput_pct;
            size_pct_str << fixed << setprecision(1) << size_pct;

            if (top >= top_limit)
            {
                Log::w(0, "Top-k limit reached, stopping further suggestions.");
                break;
            }
            
            Log::w(0, "Suggestion (" + to_string(top+1) + "/" + to_string(suggestions.size()) + ")");
            Log::w(2, "OQ statistics file", suggestion.oq_file);
            Log::w(1, "Optimization", suggestion.variant);
            Log::w(1, "Workload", str(suggestion.workload, "\n"));
            Log::w(1, "Synth ID", to_string(suggestion.synth_id));
            Log::w(2, "Avg time per query [s/q] (predicted)", to_string(time_per_query_sq));
            Log::w(1, "Throughput [q/s] (predicted)", to_string(throughput_qps));
            Log::w(1, "Size [megabytes] (predicted)", to_string(size_mb));
            Log::w(1, "Relative to winning suggestion", throughput_pct_str.str() + "% Throughput, " + size_pct_str.str() + "% Size");
            Log::w(1, "Schema", IdxSchemaSerializer::to_json(suggestion.idxschema));
            Log::w(1, "Schema (single line)", "'" + IdxSchemaSerializer::to_json_line(suggestion.idxschema) + "'");
            
            prev_throughput = throughput_qps;
            prev_size_mb = size_mb;
            top++;
        }
    }
}


void ExecutionRunner::synthesize(
    const vector<tuple<string,vector<RangeIRQuery>>>& Q,
    const vector<OStats>& per_q_ostats, StatsComp& statscomp,
    const vector<pair<Timestamp, Timestamp>>& all_slice_bounds,
    const vector<string>& groups,
    const vector<IdxSchema>& custom_templates)
{
    SynthesisRunner synthesisrunner(Q, statscomp, per_q_ostats, groups, custom_templates);
    auto result = synthesisrunner.run();

    // Group suggestions by temporal band
    map<string, vector<const IdxSchemaSuggestion*>> by_band;
    for (const auto& [oq_file, suggestions] : result)
        for (const auto& s : suggestions)
        {
            string band = s.temporal_band.empty() ? "(global)" : s.temporal_band;
            by_band[band].push_back(&s);
        }

    // Sort within each band by predicted performance (best first)
    for (auto& [band, band_suggestions] : by_band)
        sort(band_suggestions.begin(), band_suggestions.end(),
            [](const IdxSchemaSuggestion* a, const IdxSchemaSuggestion* b)
            { return a->predicted_performance < b->predicted_performance; });

    int top_limit = Cfg::get<int>("synthesis.top-k");
    double top_relative = Cfg::get<double>("synthesis.top-relative");

    for (const auto& [band, band_suggestions] : by_band)
    {
        Log::w(0, "Temporal Band: " + band);

        double best_throughput = band_suggestions.empty() ? 0.0 :
            1.0 / pow(10, band_suggestions[0]->predicted_performance);
        double best_size_mb = band_suggestions.empty() ? 0.0 :
            pow(10, band_suggestions[0]->predicted_size) / (1024*1024);

        int top = 0;
        double prev_throughput = 0.0;
        double prev_size_mb = 0.0;

        for (const auto* suggestion : band_suggestions)
        {
            auto time_per_query_sq = pow(10, suggestion->predicted_performance);
            auto throughput_qps = 1.0 / time_per_query_sq;
            auto size_mb = pow(10, suggestion->predicted_size) / (1024*1024);

            if (top > 0 && prev_throughput > 0 && prev_size_mb > 0)
            {
                double throughput_diff_pct = abs((prev_throughput - throughput_qps) / prev_throughput) * 100.0;
                double size_diff_pct = abs((prev_size_mb - size_mb) / prev_size_mb) * 100.0;

                if (throughput_diff_pct < top_relative && size_diff_pct < top_relative)
                {
                    Log::w(2, "Skipping suggestion (only " + to_string(throughput_diff_pct) +
                           "% throughput diff, " + to_string(size_diff_pct) + "% size diff)");
                    continue;
                }
            }

            double throughput_pct = (best_throughput > 0) ? (throughput_qps / best_throughput) * 100.0 : 100.0;
            double size_pct = (best_size_mb > 0) ? (size_mb / best_size_mb) * 100.0 : 100.0;

            stringstream throughput_pct_str, size_pct_str;
            throughput_pct_str << fixed << setprecision(1) << throughput_pct;
            size_pct_str << fixed << setprecision(1) << size_pct;

            if (top >= top_limit)
            {
                Log::w(0, "Top-k limit reached, stopping further suggestions.");
                break;
            }

            Log::w(0, "Suggestion (" + to_string(top+1) + "/" + to_string(band_suggestions.size()) + ")");
            Log::w(1, "Temporal band", band);
            Log::w(2, "OQ statistics file", suggestion->oq_file);
            Log::w(1, "Optimization", suggestion->variant);
            Log::w(1, "Workload", str(suggestion->workload, "\n"));
            Log::w(1, "Synth ID", to_string(suggestion->synth_id));
            Log::w(2, "Avg time per query [s/q] (predicted)", to_string(time_per_query_sq));
            Log::w(1, "Throughput [q/s] (predicted)", to_string(throughput_qps));
            Log::w(1, "Size [megabytes] (predicted)", to_string(size_mb));
            Log::w(1, "Relative to winning suggestion", throughput_pct_str.str() + "% Throughput, " + size_pct_str.str() + "% Size");
            Log::w(1, "Schema", IdxSchemaSerializer::to_json(suggestion->idxschema));
            Log::w(1, "Schema (single line)", "'" + IdxSchemaSerializer::to_json_line(suggestion->idxschema) + "'");

            prev_throughput = throughput_qps;
            prev_size_mb = size_mb;
            top++;
        }
    }

    // Emit merged banded schema if temporal bands are present
    bool has_bands = any_of(result.begin(), result.end(),
        [](const auto& p) { return any_of(p.second.begin(), p.second.end(),
            [](const auto& s) { return !s.temporal_band.empty(); }); });

    if (has_bands)
    {
        auto br = SynthesisRunner::process_banded(
            result, Q, all_slice_bounds);

        if (!br.schemas.empty())
        {
            Log::w(0, "Merged Banded Schema (" + to_string(br.schemas.size()) + " bands)");
            Log::w(1, "Schema (banded)", IdxSchemaSerializer::to_json_banded(br.schemas));
            Log::w(1, "Schema (banded, single line)",
                "'" + IdxSchemaSerializer::to_json_banded_line(br.schemas) + "'");
        }
    }
}
