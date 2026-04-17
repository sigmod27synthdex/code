# Adaptive Ensemble Indexing for Temporal Information Retrieval via Learned Cost Models

## Overview

Temporal Information Retrieval (TIR) extends classical IR by incorporating temporal dynamics. Given a collection of objects — such as documents or database records — each characterized by a time interval and a set of descriptive elements (e.g., keywords), the goal is to efficiently answer *time-travel IR queries*: retrieve all objects whose description contains the query elements and whose lifespan overlaps the query time range. This type of query is fundamental in web and document archives, versioning systems, temporal databases, and scientific data collections.

Prior work addresses this problem by extending inverted index structures with temporal information. Vertical partitioning (slicing) divides every posting list by time domain; horizontal partitioning (sharding) groups postings into temporal shards. More recently, reorganizing posting lists as HINT — the state-of-the-art in-memory interval index — has been shown to outperform these IR-first approaches, particularly as query time extents grow. However, no single method dominates across all workloads: performance varies substantially with query element selectivity, temporal distributions, and dataset characteristics.

This project presents a framework for automatically *synthesizing* workload-optimized index configurations. Rather than using a single structure for the entire dataset, a configuration may take the form of an *index ensemble* — a set of different indexing structures applied to different partitions of the input. Given a representative set of queries, the synthesis process searches the *design space* of all such configurations and finds an approximately optimal one. The key enabler of this process is a *Learned Cost Model (LCM)*: a neural network trained on workload statistics that predicts the query performance of any candidate configuration in microseconds, eliminating the need to actually build and benchmark each candidate. This makes exhaustive-style exploration of the vast design space tractable.

Key components of the framework:

- **Learned Cost Model (LCM)** — a neural network that predicts query throughput from compact workload statistics, enabling rapid evaluation of candidate configurations without building and testing each actual index.
- **Synthesis process** — explores the large design space of possible configurations, guided by the LCM, and iteratively refines the most promising designs to converge on a near-optimal solution.
- **Compositional design framework** — defines the design space of all possible index configurations. Configurations range from a single structure to an *index ensemble* that hierarchically combines existing structures (tIF, tIF+Slicing, tIF+Sharding, tIF+HINT, irHINT) with flexible partitioning strategies, serving as the building blocks for synthesis.


## Dependencies
- GCC 13.3.0
- Python 3.12.3
- PyTorch 2.8.0
- NumPy 2.3.2
- pandas 2.3.2
- scikit-learn 1.7.1


## Data
The datasets and workloads used in the experiments can be sourced from [https://seafile.rlp.net/d/0b8cc26c4b6f4ade9574/](https://seafile.rlp.net/d/0b8cc26c4b6f4ade9574/).
Put the unzipped files into directory ```exps/samples```. 


## Setup
Compile with
```bash
cd impl
make -j
```
Compilation uses flags `-O3 -mavx -march=native`.

Open `config/config.setup.json` and set your Python executable in the `setup` section, then install required Python libraries:
```bash
./nis setup
```
This installs: numpy, pandas, torch, scikit-learn, pyarrow, psutil.


## Usage
```
./nis <command> [--config:<dir>] [--stage:<stage>]
```

| Command | Description |
| ------- | ----------- |
| `help` | Open manual. |
| `query [<idx> [<obj> [<qry>]]]` | Run queries with a specified index, object file, and query file. Omit arguments to generate random data (to produce LCM training data). Use `?[:groups]` for `<idx>` to build a random index; use `![:groups]` for a workload-optimized index. Groups are comma-separated design-space groups (e.g. `?:m,d` or `!:dt`). Use `\|` as a separator to specify multiple indexes or query files. |
| `update <idx> <obj> <obj2>` | Update index `<idx>` (built on `<obj>`) by inserting the new objects in `<obj2>`. |
| `delete <idx> <obj> <obj2>` | Delete the objects in `<obj2>` from index `<idx>` built on `<obj>`. |
| `softdelete <idx> <obj> <obj2>` | Soft-delete (tombstone) the objects in `<obj2>` from index `<idx>` built on `<obj>`. |
| `analyze <obj> [<qry>]` | Analyze the statistical properties of an object file, and optionally of a query file. |
| `generate-O` | Generate a random object dataset according to the configured parameters. |
| `generate-Q <obj>` | Generate random queries (configured workloads) for the given object file. |
| `generate-OQ` | Generate both random objects and random queries (configured workloads) in one step. |
| `generate-O-stats` | Generate statistical summaries for random object data. |
| `convert` | Convert statistics to training format. |
| `learn` | Train the Learned Cost Model on previously collected index statistics. |
| `predict <idx> <obj> [<qry>]` | Use the Learned Cost Model to predict query performance for a given index and dataset without actually building it. |
| `synthesize ![:groups] <obj> [<qry>]` | Run the synthesis process to find an approximately optimal index configuration for the given object and query workload. Groups are comma-separated design-space groups. |
| `score <top>:[<obj>]\|[<qry>]` | Load all recorded scores, filter by `<top>` (`skyline` or `complete`), and display results. `<obj>` and `<qry>` are optional filters. |
| `accuracy [?[:groups]]` | Measure LCM prediction accuracy. Generates random O, Q, I combinations, runs actual queries and predictions. |
| `logs [<num>]` | Display the last `<num>` lines from all log files. |
| `config <part>` | Open a configuration file part in the default editor (e.g., `config core`). |
| `setup` | Install required Python libraries into the configured environment. |
| `clean [<artifacts>]` | Remove generated artifacts. Specify a comma-separated list of extensions (default: `.csv,.log,.dat,.qry`). |
| `sequence <flow>` | Run a predefined sequence of configured stages. |

Debug with ```valgrind -s --leak-check=full --show-leak-kinds=all ...``` and ```gdb --batch --ex run --ex bt --args ...```.


## Configuration

Settings are split across modular files in `config/`, assembled via `config.core.json`:

| File | Content |
| ---- | ------- |
| `config.core.json` | Master file; includes all other config files. |
| `config.setup.json` | General settings, output paths, Python environment. |
| `config.learning.json` | LCM training and data conversion. |
| `config.synthesis.json` | Synthesis, prediction, and accuracy settings. |
| `config.objects.json` | Object generation templates and parameters. |
| `config.queries.json` | Query workload patterns. |
| `config.indices.json` | Index design space definitions. |
| `config.sequences.json` | Workflow sequences and stages. |

The defaults reproduce the experimental setup from the paper; most users will not need to adjust the entries below.

**Output (`out`)** — in `config.setup.json`

| Key | Default | Description |
| --- | ------- | ----------- |
| `dir` | `../output` | Root directory for all generated output files. |
| `machine-prefix` | `"Venus"` | Prefix added to output filenames to identify the machine/run. |
| `max-file-size-megabytes` | `4000` | Maximum size per output file before rotation. |
| `detailed` | `false` | Emit detailed output. |
| `formatted` | `false` | Emit formatted output. |

**Environment (`setup`)** — in `config.setup.json`

Configures how the Python virtual environment is created and which interpreter is used by the LCM and synthesis modules.

| Key | Description |
| --- | ----------- |
| `create` | Shell command to create the virtual environment. |
| `interpreters` | List of candidate Python executable paths (first available is used). |
| `libraries` | List of packages installed by `./nis setup`. |

**Synthesis (`synthesis`)** — in `config.synthesis.json`

Controls the index configuration synthesis process.

| Key | Default | Description |
| --- | ------- | ----------- |
| `variant` | `["grad"]` | Search strategy: `"grid"` (discrete parameter sweep) or `"grad"` (gradient-based continuous optimization). |
| `top-k` | `3` | Number of top candidate configurations to retain per synthesis round. |
| `skyline` | `true` | Apply Pareto skyline filtering (throughput vs. size) to prune dominated candidates. |
| `skyline-tolerance` | `0.005` | Relative tolerance when comparing candidates on the skyline. |
| `sample-size` | `1000` | Number of random samples evaluated in the initial exploration phase. |
| `store-all-evaluations` | `true` | Persist all evaluated configurations to disk for later analysis. |
| `batch-evals-size` | `10` | Number of candidate evaluations per batch. |

The `grid` strategy performs a multi-resolution sweep defined by `step-resolution-and-top-k`: coarser resolutions explore broadly, finer resolutions refine the best candidates from the previous step. The `grad` strategy uses gradient-based continuous optimization with multi-start initialization.

**Prediction (`prediction`)** — in `config.synthesis.json`

| Key | Default | Description |
| --- | ------- | ----------- |
| `predict-before-run` | `false` | Run LCM prediction before actual query execution. |
| `top-k` | `0` | Number of top predictions to retain (0 = all). |

**Accuracy (`accuracy`)** — in `config.synthesis.json`

| Key | Default | Description |
| --- | ------- | ----------- |
| `num-object-collections` | `100` | Number of random object collections to generate. |
| `num-indices-per-OQ` | `10` | Number of random indices per object-query pair. |
| `emit-training-data` | `true` | Also emit results as LCM training data. |

**Learned Cost Model (`learning`)** — in `config.learning.json`

Controls training of the LCM neural network.

| Key | Default | Description |
| --- | ------- | ----------- |
| `split` | `[0.9, 0.1, 0.0]` | Train / validation / test split ratios. |
| `batch-size` | `64` | Mini-batch size for training. |
| `epochs` | `30` | Maximum number of training epochs. |
| `patience` | `5` | Early stopping patience (epochs without improvement). |
| `features-dim` | `57` | Dimensionality of the input feature vector. |
| `targets` | `["p_throughput_log", "p_size_log"]` | Prediction targets (log-scale throughput and index size). |
| `neural-network.hidden-layers` | `[128, 128, 32, 16]` | Hidden layer sizes of the neural network. |

**Object Generation (`o`)** — in `config.objects.json`

Controls the generation of synthetic object datasets. Object templates define the concrete boundary ranges (size, temporal extent, element frequency distribution) that the object generator samples from. The numeric parameters (e.g. bin counts, clamp values) govern how those template boundaries are interpreted and discretised.

**Query Generation (`q`)** — in `config.queries.json`

Defines a set of named query workload patterns (e.g., `LONG`, `SELECTIVE`, `HOT+COLD`) that cover a broad range of element selectivities, temporal extents, and skew characteristics. These patterns are used to generate representative training and evaluation workloads.

**Index Design Space (`i`)** — in `config.indices.json`

Defines the set of parameterized index configuration templates available to the synthesis process, including pure single-structure configurations and ensemble configurations with element-frequency or temporal partitioning.

**Sequences** — in `config.sequences.json`

Defines predefined workflow sequences (`flows`) and configuration stages (`stages`). A flow is a named list of commands executed in order; a stage applies configuration overrides before running.

**General** — in `config.setup.json`

| Key | Default | Description |
| --- | ------- | ----------- |
| `threads` | `-1` | Number of threads (`-1` = all available) for query generation and object statistics. |
| `log-level-output` | `1` | Verbosity of console output (0 = minimal, higher = more detail). |
| `check-results` | `false` | Validate query results for correctness (slower; useful for debugging). |


## Components

```
impl/                       Source code and build
├── main                    Entry point and CLI parsing
├── makefile                Compilation
├── config/                 Configuration (modular JSON files)
│   ├── config.core         Master file (includes all others)
│   ├── config.setup        General settings, output, environment
│   ├── config.learning     LCM training and conversion
│   ├── config.synthesis    Synthesis, prediction, accuracy
│   ├── config.objects      Object generation templates
│   ├── config.queries      Query workload patterns
│   ├── config.indices      Index design space definitions
│   └── config.sequences    Workflow sequences and stages
├── containers/             Core index data structures
│   ├── inverted_file       Plain inverted file (tIF)
│   ├── temporal_inverted_f Time-aware inverted file
│   ├── hint_m*             HINT interval index variants
│   ├── hierarchical_index  Hierarchical index wrapper
│   ├── 1dgrid              1-D grid structure
│   ├── offsets             Offset tables
│   └── relations           Object/interval relations
├── terminal/               Leaf-level index implementations
│   ├── tif                 Basic tIF
│   ├── tifslicing*         tIF + temporal slicing variants
│   ├── tifsharding         tIF + temporal sharding
│   ├── tifhintg            tIF + HINT
│   └── irhint              irHINT (time-first)
├── structure/              Index schema and configuration framework
│   ├── framework           Abstract index framework
│   ├── idxschema*          Schema definition, encoding, serialization
│   ├── fanout              Fanout / partitioning logic
│   └── synthdex*           Synthesized index construction
├── synthesis/              Synthesis algorithms
│   ├── synthesisbase       Shared synthesis infrastructure
│   ├── skyline             Pareto skyline filtering
│   ├── sy_grid             Grid-based search
│   └── sy_grad             Gradient-based search
├── generation/             Synthetic data generation
│   ├── ogen                Object generation
│   ├── qgen                Query generation
│   ├── igen                Index generation
│   ├── ostatsgen           Object statistics generation
│   └── randomgen           Random primitive generation
├── learning/               Learned Cost Model (LCM)
│   ├── dataloader          Training data loading
│   ├── learningbase        Shared training infrastructure
│   ├── trainer             Model training loop
│   ├── prediction          Inference / cost prediction
│   ├── statscomp           Statistics computation
│   └── statsserializer     Statistics serialization
└── utils/                  Shared utilities
    ├── cfg                 Configuration access
    ├── controller          Top-level command dispatcher
    ├── executionrunner     Query execution orchestration
    ├── indexevaluator      Index build and evaluation
    ├── synthesisrunner     Synthesis orchestration
    ├── performanceanalyzer Performance measurement
    ├── resultvalidator     Correctness checking
    ├── score               Score aggregation and display
    ├── persistence         File I/O
    ├── parsing             Input parsing
    ├── logging             Logging
    └── processexec         Subprocess execution (Python bridge)

output/                     Generated artifacts (created at runtime)
├── compilation/            Compiled object files
├── data/                   Generated object and query datasets
├── stats/                  Index statistics collected for LCM training
│   ├── O/                  Object-level statistics
│   ├── O-complete/         Full object statistics
│   ├── OQ/                 Object + query workload statistics
│   ├── OQI/                Object + query + index statistics
│   ├── OQIP/               Object + query + index + performance statistics (primary training data)
│   ├── OQIP-complete/      Full OQIP statistics
│   ├── OQIP-predictions/   LCM predictions for object + query + index
│   ├── I/                  Index-only statistics
│   ├── I-synthesis/        Index statistics generated during synthesis runs
│   ├── Q-complete/         Full query statistics
│   └── formatted/          Human-friendly statistics (JSON)
├── train/                  Prepared LCM training data (Parquet files, generated from OQIP statistics)
├── lcms/                   Saved LCM model checkpoints
├── score/                  Score CSV files from query runs
├── logs/                   Runtime log files
├── plots/                  Generated plot scripts and figures
└── tests/                  Test output

exps/                       Experiment driver and supporting files
├── start.py                Main experiment script (enables/disables datasets, workloads, configs)
├── idxcfg/                 Predefined index configuration JSON files used in experiments
├── samples/                Sample datasets and query workloads
└── scores/                 Collected score CSV results from experiment runs
```


## Experiments
Experiments were conducted on a dual Intel Xeon E5-2630 v4 @ 2.20 GHz with 512 GB RAM running AlmaLinux 8.5 (Kernel 4.18.0).

All experiments can be run with `misc/exps.start.py`, where datasets, workloads, and configurations can be individually enabled or disabled.
